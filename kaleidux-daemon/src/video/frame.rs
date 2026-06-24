use gstreamer as gst;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::os::unix::io::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::VideoBackendKind;

#[cfg(feature = "mpv-backend")]
#[derive(Clone, Debug)]
pub struct GlExternalFrame {
    inner: Arc<GlExternalFrameInner>,
}

#[cfg(feature = "mpv-backend")]
#[derive(Debug)]
struct GlExternalFrameInner {
    texture: Arc<wgpu::Texture>,
    slot_busy: Arc<AtomicBool>,
    release_scheduled: AtomicBool,
}

#[cfg(feature = "mpv-backend")]
impl GlExternalFrame {
    pub(crate) fn new(texture: Arc<wgpu::Texture>, slot_busy: Arc<AtomicBool>) -> Self {
        Self {
            inner: Arc::new(GlExternalFrameInner {
                texture,
                slot_busy,
                release_scheduled: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn texture(&self) -> &wgpu::Texture {
        self.inner.texture.as_ref()
    }

    pub(crate) fn release_after_submit(&self, queue: &wgpu::Queue) {
        if self.inner.release_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        let slot_busy = self.inner.slot_busy.clone();
        queue.on_submitted_work_done(move || slot_busy.store(false, Ordering::Release));
    }
}

#[cfg(feature = "mpv-backend")]
impl Drop for GlExternalFrameInner {
    fn drop(&mut self) {
        if !self.release_scheduled.load(Ordering::Acquire) {
            self.slot_busy.store(false, Ordering::Release);
        }
    }
}

#[derive(Debug)]
pub enum VideoFrameFormat {
    Rgba,
    #[cfg(feature = "mpv-backend")]
    /// OpenGL renders into memory shared with Vulkan; WGPU blits the GPU texture.
    GlExternalRgba {
        frame: GlExternalFrame,
    },
    Nv12 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
    /// DMA-BUF zero-copy: file descriptors for each plane, no CPU-side data.
    /// File descriptors are owned and will be closed when dropped.
    DmaBufNv12 {
        y_fd: OwnedFd,
        y_stride: u32,
        y_offset: u32,
        uv_fd: OwnedFd,
        uv_stride: u32,
        uv_offset: u32,
    },
    /// CUDA zero-copy: buffer stays in GPU memory, renderer uses CUDA-Vulkan interop.
    CudaNv12 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
    I420 {
        y_stride: u32,
        u_offset: u32,
        u_stride: u32,
        v_offset: u32,
        v_stride: u32,
    },
}

impl VideoFrameFormat {
    pub fn try_clone(&self) -> Option<Self> {
        match self {
            Self::Rgba => Some(Self::Rgba),
            #[cfg(feature = "mpv-backend")]
            Self::GlExternalRgba { frame } => Some(Self::GlExternalRgba {
                frame: frame.clone(),
            }),
            Self::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Some(Self::Nv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            }),
            Self::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Some(Self::CudaNv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            }),
            Self::I420 {
                y_stride,
                u_offset,
                u_stride,
                v_offset,
                v_stride,
            } => Some(Self::I420 {
                y_stride: *y_stride,
                u_offset: *u_offset,
                u_stride: *u_stride,
                v_offset: *v_offset,
                v_stride: *v_stride,
            }),
            Self::DmaBufNv12 {
                y_fd,
                y_stride,
                y_offset,
                uv_fd,
                uv_stride,
                uv_offset,
            } => {
                let y_fd = super::dmabuf::dup_plane_fd_for_clone(y_fd)?;
                let uv_fd = super::dmabuf::dup_plane_fd_for_clone(uv_fd)?;
                Some(Self::DmaBufNv12 {
                    y_fd,
                    y_stride: *y_stride,
                    y_offset: *y_offset,
                    uv_fd,
                    uv_stride: *uv_stride,
                    uv_offset: *uv_offset,
                })
            }
        }
    }
}

/// Video frame carrying pixel data in RGBA or planar YUV formats.
/// Uses gst::Buffer to avoid copying data.
pub struct VideoFrame {
    pub buffer: gst::Buffer,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub format: VideoFrameFormat,
    pub session_id: u64,
    pub pts_ns: Option<u64>,
    pub duration_ns: Option<u64>,
}

impl VideoFrame {
    pub fn trace_fingerprint(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.width.hash(&mut hasher);
        self.height.hash(&mut hasher);
        self.stride.hash(&mut hasher);
        self.session_id.hash(&mut hasher);
        self.pts_ns.hash(&mut hasher);
        self.duration_ns.hash(&mut hasher);
        self.buffer.size().hash(&mut hasher);
        self.buffer.n_memory().hash(&mut hasher);
        self.buffer.offset().hash(&mut hasher);
        self.buffer.offset_end().hash(&mut hasher);
        format!("{:?}", self.buffer.flags()).hash(&mut hasher);
        match &self.format {
            VideoFrameFormat::Rgba => "rgba".hash(&mut hasher),
            #[cfg(feature = "mpv-backend")]
            VideoFrameFormat::GlExternalRgba { .. } => "gl-external-rgba".hash(&mut hasher),
            VideoFrameFormat::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            }
            | VideoFrameFormat::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                y_stride.hash(&mut hasher);
                uv_offset.hash(&mut hasher);
                uv_stride.hash(&mut hasher);
            }
            VideoFrameFormat::DmaBufNv12 {
                y_stride,
                y_offset,
                uv_stride,
                uv_offset,
                ..
            } => {
                y_stride.hash(&mut hasher);
                y_offset.hash(&mut hasher);
                uv_stride.hash(&mut hasher);
                uv_offset.hash(&mut hasher);
            }
            VideoFrameFormat::I420 {
                y_stride,
                u_offset,
                u_stride,
                v_offset,
                v_stride,
            } => {
                y_stride.hash(&mut hasher);
                u_offset.hash(&mut hasher);
                u_stride.hash(&mut hasher);
                v_offset.hash(&mut hasher);
                v_stride.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    #[allow(dead_code)]
    pub fn try_clone(&self) -> Option<Self> {
        Some(Self {
            buffer: self.buffer.clone(),
            width: self.width,
            height: self.height,
            stride: self.stride,
            format: self.format.try_clone()?,
            session_id: self.session_id,
            pts_ns: self.pts_ns,
            duration_ns: self.duration_ns,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerEventKind {
    Error,
    Eos,
    FatalLifecycle,
}

#[derive(Debug, Clone)]
pub struct PlayerEvent {
    pub source_id: String,
    pub session_id: u64,
    pub backend_kind: VideoBackendKind,
    pub kind: PlayerEventKind,
    pub reason: String,
}

#[derive(Clone, Default)]
pub struct LatestFrameMailbox {
    frames: Arc<parking_lot::Mutex<HashMap<String, VideoFrame>>>,
    pending_notifications: Arc<parking_lot::Mutex<HashSet<String>>>,
    pending_since: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
    overwrite_count: Arc<AtomicU64>,
    signal_pending: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl LatestFrameMailbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish_frame(&self, source_id: &str, frame: VideoFrame) {
        let mut should_signal = false;
        {
            let mut frames = self.frames.lock();
            if frames.insert(source_id.to_string(), frame).is_some() {
                self.overwrite_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        {
            self.pending_since
                .lock()
                .insert(source_id.to_string(), Instant::now());
        }
        {
            let mut pending = self.pending_notifications.lock();
            if pending.insert(source_id.to_string()) {
                should_signal = true;
            }
        }

        if !should_signal {
            return;
        }

        self.signal_pending.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    pub fn take_frame(&self, source_id: &str) -> Option<VideoFrame> {
        let frame = self.frames.lock().remove(source_id);
        self.pending_notifications.lock().remove(source_id);
        self.pending_since.lock().remove(source_id);
        frame
    }

    pub fn defer_notification(&self, source_id: &str) {
        self.pending_notifications.lock().remove(source_id);
    }

    pub fn inspect_frame<R, F>(&self, source_id: &str, inspect: F) -> Option<R>
    where
        F: FnOnce(&VideoFrame) -> R,
    {
        let frames = self.frames.lock();
        frames.get(source_id).map(inspect)
    }

    pub fn has_pending_frame(&self, source_id: &str) -> bool {
        self.frames.lock().contains_key(source_id)
    }

    pub fn pending_frame_age(&self, source_id: &str) -> Option<Duration> {
        if !self.has_pending_frame(source_id) {
            return None;
        }
        self.pending_since
            .lock()
            .get(source_id)
            .map(Instant::elapsed)
    }

    pub fn clear_source(&self, source_id: &str) {
        self.frames.lock().remove(source_id);
        self.pending_notifications.lock().remove(source_id);
        self.pending_since.lock().remove(source_id);
    }

    pub fn take_overwrite_count(&self) -> u64 {
        self.overwrite_count.swap(0, Ordering::Relaxed)
    }

    pub fn has_signal_pending(&self) -> bool {
        self.signal_pending.load(Ordering::Acquire)
    }

    pub fn clear_signal_pending(&self) {
        self.signal_pending.store(false, Ordering::Release);
    }

    pub fn pending_sources(&self) -> Vec<String> {
        self.pending_notifications.lock().iter().cloned().collect()
    }

    pub fn notified(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.notify.notified()
    }

    pub fn occupancy(&self) -> usize {
        self.frames.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_frame(session_id: u64) -> VideoFrame {
        let _ = gst::init();
        let buffer = gst::Buffer::with_size(4).expect("buffer allocation should succeed");
        VideoFrame {
            buffer,
            width: 1,
            height: 1,
            stride: 4,
            format: VideoFrameFormat::Rgba,
            session_id,
            pts_ns: None,
            duration_ns: None,
        }
    }

    #[test]
    fn pending_frame_state_tracks_source_presence() {
        let mailbox = LatestFrameMailbox::new();
        assert!(!mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_frame_age("HDMI-A-1").is_none());

        mailbox.publish_frame("HDMI-A-1", test_frame(7));
        assert!(mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_frame_age("HDMI-A-1").is_some());
        assert!(!mailbox.has_pending_frame("DP-2"));

        let _ = mailbox.take_frame("HDMI-A-1");
        assert!(!mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_frame_age("HDMI-A-1").is_none());
    }

    #[test]
    fn deferred_notification_keeps_frame_and_allows_resignal() {
        let mailbox = LatestFrameMailbox::new();
        mailbox.publish_frame("HDMI-A-1", test_frame(7));
        mailbox.clear_signal_pending();

        mailbox.defer_notification("HDMI-A-1");

        assert!(mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_frame_age("HDMI-A-1").is_some());
        assert!(mailbox.pending_sources().is_empty());

        mailbox.publish_frame("HDMI-A-1", test_frame(8));

        assert!(mailbox.has_signal_pending());
        assert_eq!(mailbox.pending_sources(), vec!["HDMI-A-1".to_string()]);
        assert_eq!(
            mailbox.take_frame("HDMI-A-1").map(|frame| frame.session_id),
            Some(8)
        );
    }
}
