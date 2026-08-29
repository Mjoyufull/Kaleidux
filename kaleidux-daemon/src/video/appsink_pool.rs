use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use gstreamer_video as gst_video;

use super::dmabuf::DRM_FORMAT_NV12;

#[derive(Debug, Clone, Copy)]
struct PoolLayout {
    width: u32,
    height: u32,
}

mod imp {
    use super::*;
    use gst::subclass::prelude::*;

    pub struct ImportableDmaBufPool {
        pub(super) allocator: gst_alloc::DRMDumbAllocator,
        pub(super) layout: parking_lot::Mutex<Option<PoolLayout>>,
    }

    #[gst::glib::object_subclass]
    impl ObjectSubclass for ImportableDmaBufPool {
        const NAME: &'static str = "KaleiduxImportableDmaBufPool";
        type Type = super::ImportableDmaBufPool;
        type ParentType = gst::BufferPool;

        fn new() -> Self {
            // Construction only happens through ImportableDmaBufPool::new,
            // which installs a working allocator immediately afterward.
            let allocator = probe_drm_allocator()
                .expect("KaleiduxImportableDmaBufPool requires a DRM dumb allocator");
            Self {
                allocator,
                layout: parking_lot::Mutex::new(None),
            }
        }
    }

    impl ObjectImpl for ImportableDmaBufPool {}
    impl GstObjectImpl for ImportableDmaBufPool {}

    impl BufferPoolImpl for ImportableDmaBufPool {
        fn options() -> &'static [&'static str] {
            &["GstBufferPoolOptionVideoMeta"]
        }

        fn set_config(&self, config: &mut gst::BufferPoolConfigRef) -> bool {
            let Some((Some(caps), _, _, _)) = config.params() else {
                return false;
            };
            let Ok(info) = gst_video::VideoInfo::from_caps(&caps) else {
                return false;
            };
            if info.format() != gst_video::VideoFormat::Nv12 {
                return false;
            }
            *self.layout.lock() = Some(PoolLayout {
                width: info.width(),
                height: info.height(),
            });
            self.parent_set_config(config)
        }

        fn alloc_buffer(
            &self,
            _params: Option<&gst::BufferPoolAcquireParams>,
        ) -> Result<gst::Buffer, gst::FlowError> {
            let layout = self.layout.lock().ok_or(gst::FlowError::NotNegotiated)?;
            // SAFETY: the allocator owns a live DRM primary-node descriptor,
            // and the returned memory is immediately owned by GstMemory.
            let (memory, pitch) = unsafe {
                self.allocator
                    .alloc(DRM_FORMAT_NV12, layout.width, layout.height)
            }
            .map_err(|_| gst::FlowError::Error)?;
            let dumb = memory
                .downcast_memory::<gst_alloc::DRMDumbMemory>()
                .map_err(|_| gst::FlowError::Error)?;
            let dmabuf = dumb.export_dmabuf().map_err(|_| gst::FlowError::Error)?;
            let mut buffer = gst::Buffer::new();
            let buffer_ref = buffer.get_mut().ok_or(gst::FlowError::Error)?;
            buffer_ref.append_memory(dmabuf.upcast_memory::<gst::Memory>());
            let uv_offset = pitch as usize * layout.height as usize;
            gst_video::VideoMeta::add_full(
                buffer_ref,
                gst_video::VideoFrameFlags::empty(),
                gst_video::VideoFormat::Nv12,
                layout.width,
                layout.height,
                &[0, uv_offset],
                &[pitch as i32, pitch as i32],
            )
            .map_err(|_| gst::FlowError::Error)?;
            Ok(buffer)
        }
    }

    fn probe_drm_allocator() -> Option<gst_alloc::DRMDumbAllocator> {
        static ALLOCATOR: std::sync::OnceLock<Option<gst_alloc::DRMDumbAllocator>> =
            std::sync::OnceLock::new();
        ALLOCATOR
            .get_or_init(|| {
                (0..16)
                    .map(|index| std::path::PathBuf::from(format!("/dev/dri/card{index}")))
                    .filter(|path| path.exists())
                    .find_map(|path| {
                        gst_alloc::DRMDumbAllocator::with_device_path(path)
                            .ok()
                            .filter(gst_alloc::DRMDumbAllocator::has_prime_export)
                    })
            })
            .clone()
    }

    pub(super) fn available() -> bool {
        probe_drm_allocator().is_some()
    }
}

gst::glib::wrapper! {
    pub struct ImportableDmaBufPool(ObjectSubclass<imp::ImportableDmaBufPool>)
        @extends gst::BufferPool, gst::Object;
}

impl ImportableDmaBufPool {
    pub(super) fn try_new() -> Option<Self> {
        if !imp::available() {
            return None;
        }
        Some(gst::glib::Object::builder().build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gst::prelude::*;

    #[test]
    fn proposed_pool_allocates_bounded_linear_nv12_dmabufs_when_drm_is_available() {
        gst::init().expect("GStreamer should initialize");
        let Some(pool) = ImportableDmaBufPool::try_new() else {
            return;
        };
        let caps = gst::Caps::builder("video/x-raw")
            .features([gst_alloc::CAPS_FEATURE_MEMORY_DMABUF.as_str()])
            .field("format", "NV12")
            .field("width", 64i32)
            .field("height", 64i32)
            .field("framerate", gst::Fraction::new(30, 1))
            .build();
        let info = gst_video::VideoInfo::from_caps(&caps).expect("valid NV12 caps");
        let mut config = pool.config();
        config.set_params(Some(&caps), info.size() as u32, 2, 6);
        config.add_option(gst_video::BUFFER_POOL_OPTION_VIDEO_META);
        pool.set_config(config)
            .expect("pool should accept NV12 caps");
        pool.set_active(true)
            .expect("pool should preallocate buffers");
        let buffer = pool
            .acquire_buffer(None)
            .expect("pool should return a buffer");
        assert_eq!(buffer.n_memory(), 1);
        assert!(
            buffer
                .peek_memory(0)
                .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
                .is_some()
        );
        // SAFETY: the returned pointer is borrowed from this live buffer.
        let meta = unsafe {
            gst_video::ffi::gst_buffer_get_video_meta(buffer.as_ptr() as *mut gst::ffi::GstBuffer)
        };
        assert!(!meta.is_null());
        // SAFETY: meta belongs to this still-live buffer and contains fixed
        // four-element arrays copied immediately below.
        let (strides, offsets) = unsafe { ((*meta).stride, (*meta).offset) };
        let format = super::super::dmabuf::DmaBufDescriptorCache::default()
            .synchronized_frame_format(&buffer, 64, 64, DRM_FORMAT_NV12, 0, strides, offsets)
            .expect("pool DMA-BUF should export an implicit producer fence");
        let super::super::VideoFrameFormat::DmaBufNv12 { frame } = format else {
            panic!("expected DMA-BUF descriptor")
        };
        assert!(frame.acquire_fence.is_some());
        pool.set_active(false).expect("pool should stop cleanly");
    }
}
