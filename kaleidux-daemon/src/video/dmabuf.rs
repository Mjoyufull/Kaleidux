use anyhow::Context;
use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use gstreamer_video as gst_video;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::os::fd::{FromRawFd, OwnedFd};

use super::{NativeDmaBufNv12, NativeDmaBufObject, NativeDmaBufPlane, VideoFrameFormat};

const DESCRIPTOR_CACHE_LIMIT: usize = 24;
const DRM_FORMAT_MOD_LINEAR: u64 = 0;
const DRM_FORMAT_MOD_INVALID: u64 = u64::MAX;

const fn fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

pub(super) const DRM_FORMAT_NV12: u32 = fourcc(b'N', b'V', b'1', b'2');

fn dup_dma_fd(raw: std::os::fd::RawFd) -> anyhow::Result<OwnedFd> {
    anyhow::ensure!(raw >= 0, "invalid DMA-BUF fd");
    // SAFETY: raw is borrowed from a live GstDmaBufMemory. The returned
    // descriptor is independent and owned by the cached allocation layout.
    let duplicated = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(std::io::Error::last_os_error()).context("duplicating DMA-BUF fd");
    }
    // SAFETY: F_DUPFD_CLOEXEC returned a new descriptor owned exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AllocationKey {
    memories: Vec<usize>,
    memory_offsets: Vec<usize>,
    width: u32,
    height: u32,
    fourcc: u32,
    modifier: u64,
    strides: [i32; 2],
    offsets: [usize; 2],
}

#[derive(Debug, Clone)]
struct CachedDescriptor {
    key: AllocationKey,
    descriptor: NativeDmaBufNv12,
    /// Keep GstMemory identities alive for the cache generation. This prevents
    /// allocator pointer reuse from aliasing an older Vulkan import without a
    /// per-frame fstat/inode syscall.
    _allocation_owners: Vec<gst::Memory>,
}

/// Callback-local cache keyed by GstMemory allocation identity. A decoder pool
/// rotates a small stable set of memories, so the cold path duplicates object
/// FDs once and every subsequent sample only clones Arcs and retains its buffer.
#[derive(Debug, Default)]
pub(super) struct DmaBufDescriptorCache {
    entries: VecDeque<CachedDescriptor>,
}

impl DmaBufDescriptorCache {
    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(super) fn frame_format(
        &mut self,
        buffer: &gst::BufferRef,
        width: u32,
        height: u32,
        fourcc: u32,
        modifier: u64,
        strides: [i32; 4],
        offsets: [usize; 4],
    ) -> anyhow::Result<VideoFrameFormat> {
        validate_layout(buffer, width, height, fourcc, modifier, strides, offsets)?;
        let key = allocation_key(buffer, width, height, fourcc, modifier, strides, offsets)?;
        if let Some(index) = self.entries.iter().position(|entry| entry.key == key) {
            let entry = self
                .entries
                .remove(index)
                .expect("descriptor index came from this queue");
            let descriptor = entry.descriptor.clone();
            self.entries.push_back(entry);
            return Ok(VideoFrameFormat::DmaBufNv12 { frame: descriptor });
        }

        let descriptor = build_descriptor(buffer, &key, strides, offsets)?;
        let allocation_owners = (0..buffer.n_memory())
            .map(|index| buffer.peek_memory(index).to_owned())
            .collect();
        self.entries.push_back(CachedDescriptor {
            key,
            descriptor: descriptor.clone(),
            _allocation_owners: allocation_owners,
        });
        while self.entries.len() > DESCRIPTOR_CACHE_LIMIT {
            self.entries.pop_front();
        }
        Ok(VideoFrameFormat::DmaBufNv12 { frame: descriptor })
    }

    pub(super) fn synchronized_frame_format(
        &mut self,
        buffer: &gst::BufferRef,
        width: u32,
        height: u32,
        fourcc: u32,
        modifier: u64,
        strides: [i32; 4],
        offsets: [usize; 4],
    ) -> anyhow::Result<VideoFrameFormat> {
        let mut format =
            self.frame_format(buffer, width, height, fourcc, modifier, strides, offsets)?;
        let VideoFrameFormat::DmaBufNv12 { frame } = &mut format else {
            unreachable!("DMA-BUF descriptor cache returned a non-DMA-BUF frame")
        };
        frame.acquire_fence = Some(std::sync::Arc::new(export_implicit_read_fence(
            frame.objects.as_ref(),
        )?));
        Ok(format)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[repr(C)]
struct DmaBufExportSyncFile {
    flags: u32,
    fd: i32,
}

#[repr(C)]
struct SyncMergeData {
    name: [u8; 32],
    fd2: i32,
    fence: i32,
    flags: u32,
    pad: u32,
}

const IOC_WRITE: libc::c_ulong = 1;
const IOC_READ: libc::c_ulong = 2;
const IOC_TYPESHIFT: u32 = 8;
const IOC_SIZESHIFT: u32 = 16;
const IOC_DIRSHIFT: u32 = 30;
const DMA_BUF_IOCTL_EXPORT_SYNC_FILE: libc::c_ulong = ((IOC_READ | IOC_WRITE) << IOC_DIRSHIFT)
    | ((std::mem::size_of::<DmaBufExportSyncFile>() as libc::c_ulong) << IOC_SIZESHIFT)
    | ((b'b' as libc::c_ulong) << IOC_TYPESHIFT)
    | 2;
const SYNC_IOC_MERGE: libc::c_ulong = ((IOC_READ | IOC_WRITE) << IOC_DIRSHIFT)
    | ((std::mem::size_of::<SyncMergeData>() as libc::c_ulong) << IOC_SIZESHIFT)
    | ((b'>' as libc::c_ulong) << IOC_TYPESHIFT)
    | 3;

fn export_implicit_read_fence(objects: &[NativeDmaBufObject]) -> anyhow::Result<OwnedFd> {
    let mut fences = Vec::with_capacity(objects.len());
    for object in objects {
        let mut request = DmaBufExportSyncFile {
            // DMA_BUF_SYNC_READ: wait only for writers before Vulkan reads.
            flags: 1,
            fd: -1,
        };
        // SAFETY: request follows linux/dma-buf.h and object.fd stays live.
        let result = unsafe {
            libc::ioctl(
                std::os::fd::AsRawFd::as_raw_fd(&object.fd),
                DMA_BUF_IOCTL_EXPORT_SYNC_FILE,
                std::ptr::addr_of_mut!(request),
            )
        };
        if result != 0 || request.fd < 0 {
            return Err(std::io::Error::last_os_error())
                .context("exporting DMA-BUF producer fence");
        }
        // SAFETY: the ioctl returned a fresh sync_file descriptor.
        fences.push(unsafe { OwnedFd::from_raw_fd(request.fd) });
    }
    let mut merged = fences
        .pop()
        .ok_or_else(|| anyhow::anyhow!("DMA-BUF frame has no objects"))?;
    for fence in fences {
        let mut request = SyncMergeData {
            name: [0; 32],
            fd2: std::os::fd::AsRawFd::as_raw_fd(&fence),
            fence: -1,
            flags: 0,
            pad: 0,
        };
        request.name[..8].copy_from_slice(b"kaleidux");
        // SAFETY: both sync_file descriptors and the UAPI request stay live.
        let result = unsafe {
            libc::ioctl(
                std::os::fd::AsRawFd::as_raw_fd(&merged),
                SYNC_IOC_MERGE,
                std::ptr::addr_of_mut!(request),
            )
        };
        if result != 0 || request.fence < 0 {
            return Err(std::io::Error::last_os_error()).context("merging DMA-BUF producer fences");
        }
        // SAFETY: SYNC_IOC_MERGE returned a new sync_file descriptor.
        merged = unsafe { OwnedFd::from_raw_fd(request.fence) };
    }
    Ok(merged)
}

fn validate_layout(
    buffer: &gst::BufferRef,
    width: u32,
    height: u32,
    fourcc: u32,
    modifier: u64,
    strides: [i32; 4],
    offsets: [usize; 4],
) -> anyhow::Result<()> {
    anyhow::ensure!(fourcc == DRM_FORMAT_NV12, "DMA_DRM fourcc is not NV12");
    anyhow::ensure!(
        modifier != DRM_FORMAT_MOD_INVALID,
        "DMA_DRM caps omitted a usable modifier"
    );
    anyhow::ensure!(width > 0 && height > 0, "empty DMA-BUF frame");
    anyhow::ensure!(strides[0] > 0 && strides[1] > 0, "invalid DMA-BUF strides");
    anyhow::ensure!(
        strides[0] as u32 >= width && strides[1] as u32 >= width,
        "DMA-BUF pitch is smaller than the NV12 plane width"
    );
    anyhow::ensure!(buffer.n_memory() > 0, "DMA-BUF frame has no memory");
    if buffer.n_memory() == 1 && modifier == DRM_FORMAT_MOD_LINEAR {
        let y_end = offsets[0].saturating_add(strides[0] as usize * height as usize);
        let uv_end = offsets[1].saturating_add(strides[1] as usize * height.div_ceil(2) as usize);
        anyhow::ensure!(
            y_end <= buffer.size() && uv_end <= buffer.size(),
            "linear DMA-BUF plane exceeds buffer bounds"
        );
    }
    Ok(())
}

fn allocation_key(
    buffer: &gst::BufferRef,
    width: u32,
    height: u32,
    fourcc: u32,
    modifier: u64,
    strides: [i32; 4],
    offsets: [usize; 4],
) -> anyhow::Result<AllocationKey> {
    let mut memories = Vec::with_capacity(buffer.n_memory());
    let mut memory_offsets = Vec::with_capacity(buffer.n_memory());
    for index in 0..buffer.n_memory() {
        let memory = buffer.peek_memory(index);
        anyhow::ensure!(
            memory
                .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
                .is_some(),
            "mixed non-DMA-BUF memory in DMA_DRM frame"
        );
        memories.push(memory.as_ptr() as usize);
        memory_offsets.push(memory.offset());
    }
    Ok(AllocationKey {
        memories,
        memory_offsets,
        width,
        height,
        fourcc,
        modifier,
        strides: [strides[0], strides[1]],
        offsets: [offsets[0], offsets[1]],
    })
}

fn build_descriptor(
    buffer: &gst::BufferRef,
    key: &AllocationKey,
    strides: [i32; 4],
    offsets: [usize; 4],
) -> anyhow::Result<NativeDmaBufNv12> {
    let mut objects = Vec::with_capacity(buffer.n_memory());
    for index in 0..buffer.n_memory() {
        let memory = buffer.peek_memory(index);
        let dmabuf = memory
            .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
            .ok_or_else(|| anyhow::anyhow!("memory {index} is not DMA-BUF"))?;
        objects.push(NativeDmaBufObject {
            fd: dup_dma_fd(dmabuf.fd())?,
            size: memory.maxsize() as u64,
            modifier: key.modifier,
        });
    }

    let plane_objects = plane_object_indices(buffer, offsets)?;
    let planes = std::array::from_fn(|plane_index| NativeDmaBufPlane {
        layer_index: 0,
        object_index: plane_objects[plane_index],
        offset: buffer
            .peek_memory(plane_objects[plane_index])
            .offset()
            .saturating_add(offsets[plane_index]) as u64,
        pitch: strides[plane_index] as u64,
        drm_fourcc: key.fourcc,
    });

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    Ok(NativeDmaBufNv12 {
        surface_id: hasher.finish(),
        objects: objects.into(),
        planes,
        acquire_fence: None,
        drm_syncobj: None,
    })
}

fn plane_object_indices(
    buffer: &gst::BufferRef,
    offsets: [usize; 4],
) -> anyhow::Result<[usize; 2]> {
    if buffer.n_memory() >= 2 {
        // Hardware exporters conventionally expose one GstMemory per NV12
        // plane and VideoMeta offsets relative to each memory object.
        return Ok([0, 1]);
    }
    let y = buffer
        .find_memory(offsets[0]..offsets[0].saturating_add(1))
        .map(|(range, _)| range.start)
        .ok_or_else(|| anyhow::anyhow!("luma offset is outside the DMA-BUF"))?;
    let uv = buffer
        .find_memory(offsets[1]..offsets[1].saturating_add(1))
        .map(|(range, _)| range.start)
        .ok_or_else(|| anyhow::anyhow!("chroma offset is outside the DMA-BUF"))?;
    Ok([y, uv])
}

pub(super) fn linear_nv12_fourcc(format: gst_video::VideoFormat) -> Option<(u32, u64)> {
    (format == gst_video::VideoFormat::Nv12).then_some((DRM_FORMAT_NV12, DRM_FORMAT_MOD_LINEAR))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Once};

    fn init() {
        static INIT: Once = Once::new();
        INIT.call_once(|| gst::init().expect("GStreamer should initialize"));
    }

    fn dmabuf_buffer(sizes: &[usize]) -> gst::Buffer {
        init();
        let allocator = gst_alloc::DmaBufAllocator::new();
        let mut buffer = gst::Buffer::new();
        for size in sizes {
            let file = std::fs::File::open("/dev/zero").expect("/dev/zero should open");
            // SAFETY: ownership of this fresh File is transferred to GstMemory.
            let memory = unsafe { allocator.alloc(file, *size) }
                .expect("DMA-BUF allocator should wrap the descriptor");
            buffer
                .get_mut()
                .expect("new buffer is writable")
                .append_memory(memory);
        }
        buffer
    }

    #[test]
    fn linear_single_object_layout_is_cached_without_fd_duplication() {
        let buffer = dmabuf_buffer(&[12_288]);
        let mut cache = DmaBufDescriptorCache::default();
        let first = cache
            .frame_format(
                &buffer,
                64,
                64,
                DRM_FORMAT_NV12,
                DRM_FORMAT_MOD_LINEAR,
                [128, 128, 0, 0],
                [0, 8192, 0, 0],
            )
            .expect("linear layout should be valid");
        let second = cache
            .frame_format(
                &buffer,
                64,
                64,
                DRM_FORMAT_NV12,
                DRM_FORMAT_MOD_LINEAR,
                [128, 128, 0, 0],
                [0, 8192, 0, 0],
            )
            .expect("cached layout should be valid");
        let VideoFrameFormat::DmaBufNv12 { frame: first } = first else {
            panic!("expected DMA-BUF frame")
        };
        let VideoFrameFormat::DmaBufNv12 { frame: second } = second else {
            panic!("expected DMA-BUF frame")
        };
        assert!(Arc::ptr_eq(&first.objects, &second.objects));
        assert_eq!(cache.len(), 1);
        assert_eq!(first.planes[1].offset, 8192);
    }

    #[test]
    fn tiled_multi_object_layout_preserves_modifier_and_object_mapping() {
        const INTEL_X_TILED: u64 = 0x0100_0000_0000_0001;
        let buffer = dmabuf_buffer(&[8192, 4096]);
        let mut cache = DmaBufDescriptorCache::default();
        let format = cache
            .frame_format(
                &buffer,
                64,
                64,
                DRM_FORMAT_NV12,
                INTEL_X_TILED,
                [128, 128, 0, 0],
                [0, 0, 0, 0],
            )
            .expect("tiled multi-object layout should be preserved");
        let VideoFrameFormat::DmaBufNv12 { frame } = format else {
            panic!("expected DMA-BUF frame")
        };
        assert_eq!(frame.objects.len(), 2);
        assert!(
            frame
                .objects
                .iter()
                .all(|object| object.modifier == INTEL_X_TILED)
        );
        assert_eq!(frame.planes[0].object_index, 0);
        assert_eq!(frame.planes[1].object_index, 1);
        assert_eq!(frame.planes[0].drm_fourcc, DRM_FORMAT_NV12);
        assert_eq!(frame.planes[1].drm_fourcc, DRM_FORMAT_NV12);
    }

    #[test]
    fn invalid_modifier_is_rejected_before_import() {
        let buffer = dmabuf_buffer(&[12_288]);
        let error = DmaBufDescriptorCache::default()
            .frame_format(
                &buffer,
                64,
                64,
                DRM_FORMAT_NV12,
                DRM_FORMAT_MOD_INVALID,
                [128, 128, 0, 0],
                [0, 8192, 0, 0],
            )
            .expect_err("invalid modifier must not reach Vulkan");
        assert!(error.to_string().contains("modifier"));
    }
}
