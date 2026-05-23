use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use super::VideoFrameFormat;

fn dup_dma_fd(raw: std::os::unix::io::RawFd) -> Option<OwnedFd> {
    if raw < 0 {
        return None;
    }
    // SAFETY: `raw` was validated as non-negative and is only duplicated; ownership of the
    // original fd remains with its caller/GStreamer memory.
    let duped = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) };
    if duped < 0 {
        tracing::warn!(
            "[VIDEO] dup_dma_fd: F_DUPFD_CLOEXEC failed: {}",
            std::io::Error::last_os_error()
        );
        return None;
    }
    // SAFETY: `duped` is a new fd returned by `F_DUPFD_CLOEXEC`, so `OwnedFd` owns it exactly once.
    Some(unsafe { OwnedFd::from_raw_fd(duped) })
}

pub(super) fn dup_plane_fd_for_clone(fd: &OwnedFd) -> Option<OwnedFd> {
    match fd.try_clone() {
        Ok(cloned) => Some(cloned),
        Err(e) => {
            let duplicated = dup_dma_fd(fd.as_raw_fd());
            if duplicated.is_none() {
                tracing::warn!("[VIDEO] plane fd clone failed: {e}");
            }
            duplicated
        }
    }
}

/// Extract DMA-BUF file descriptors and plane info from an NV12 GStreamer buffer.
///
/// NV12 buffers may carry 1 or 2 memory blocks:
///   - 1 block: Y and UV packed into a single allocation (different offsets)
///   - 2 blocks: Y and UV in separate DMA-BUF allocations
///
/// Falls back to regular NV12 if fd extraction fails.
pub(super) fn extract_dmabuf_nv12(
    buffer: &gst::Buffer,
    strides: [i32; 4],
    offsets: [usize; 4],
) -> VideoFrameFormat {
    // Validate strides before casting to u32 (silently wraps if negative)
    if strides[0] < 0 || strides[1] < 0 {
        tracing::warn!(
            "[VIDEO] Negative strides detected ({}, {}), falling back to CPU path",
            strides[0],
            strides[1]
        );
        return VideoFrameFormat::Nv12 {
            y_stride: strides[0].max(0) as u32,
            uv_offset: offsets[1] as u32,
            uv_stride: strides[1].max(0) as u32,
        };
    }
    if buffer.n_memory() >= 2 {
        // Separate DMA-BUFs per plane
        let y_mem = buffer.peek_memory(0);
        let uv_mem = buffer.peek_memory(1);

        if let (Some(y_dmabuf), Some(uv_dmabuf)) = (
            y_mem.downcast_memory_ref::<gst_alloc::DmaBufMemory>(),
            uv_mem.downcast_memory_ref::<gst_alloc::DmaBufMemory>(),
        ) {
            match (dup_dma_fd(y_dmabuf.fd()), dup_dma_fd(uv_dmabuf.fd())) {
                (Some(y_fd), Some(uv_fd)) => {
                    return VideoFrameFormat::DmaBufNv12 {
                        y_fd,
                        y_stride: strides[0] as u32,
                        y_offset: offsets[0] as u32,
                        uv_fd,
                        uv_stride: strides[1] as u32,
                        uv_offset: offsets[1] as u32,
                    };
                }
                (Some(y_fd), None) => {
                    drop(y_fd);
                    tracing::warn!(
                        "[VIDEO] dup failed for dual-plane DMA-BUF, falling back to CPU path"
                    );
                }
                (None, Some(uv_fd)) => {
                    drop(uv_fd);
                    tracing::warn!(
                        "[VIDEO] dup failed for dual-plane DMA-BUF, falling back to CPU path"
                    );
                }
                (None, None) => {
                    tracing::warn!(
                        "[VIDEO] dup failed for dual-plane DMA-BUF, falling back to CPU path"
                    );
                }
            }
        }
    } else if buffer.n_memory() == 1 {
        // Single DMA-BUF with both planes at different offsets
        let mem = buffer.peek_memory(0);
        if let Some(dmabuf) = mem.downcast_memory_ref::<gst_alloc::DmaBufMemory>() {
            let Some(y_fd) = dup_dma_fd(dmabuf.fd()) else {
                tracing::warn!(
                    "[VIDEO] dup failed for single-plane DMA-BUF, falling back to CPU path"
                );
                return VideoFrameFormat::Nv12 {
                    y_stride: strides[0] as u32,
                    uv_offset: offsets[1] as u32,
                    uv_stride: strides[1] as u32,
                };
            };
            let uv_fd = match y_fd.try_clone() {
                Ok(fd) => fd,
                Err(_) => match dup_dma_fd(y_fd.as_raw_fd()) {
                    Some(fd) => fd,
                    None => {
                        tracing::warn!(
                            "[VIDEO] could not dup UV view of single DMA-BUF, falling back to CPU path"
                        );
                        return VideoFrameFormat::Nv12 {
                            y_stride: strides[0] as u32,
                            uv_offset: offsets[1] as u32,
                            uv_stride: strides[1] as u32,
                        };
                    }
                },
            };
            return VideoFrameFormat::DmaBufNv12 {
                y_fd,
                y_stride: strides[0] as u32,
                y_offset: offsets[0] as u32,
                uv_fd,
                uv_stride: strides[1] as u32,
                uv_offset: offsets[1] as u32,
            };
        }
    }

    // Fallback: treat as regular NV12 (CPU-accessible)
    if buffer.n_memory() == 0 {
        tracing::debug!("[VIDEO] No memory blocks on buffer, falling back to CPU path");
    } else {
        tracing::warn!(
            "[VIDEO] DMA-BUF memory detected but fd extraction failed, falling back to NV12 CPU path"
        );
    }
    VideoFrameFormat::Nv12 {
        y_stride: strides[0] as u32,
        uv_offset: offsets[1] as u32,
        uv_stride: strides[1] as u32,
    }
}
