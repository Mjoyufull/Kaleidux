//! Runs the process-wide DMA-BUF degradation contract in an isolated test process.

use super::*;
use crate::video::appsink::{AppsinkNegotiatedState, sample_to_video_frame};
use crate::video::capabilities::{VideoCapabilities, VideoMode, build_video_sink_caps};
use gstreamer as gst;
use gstreamer_video as gst_video;

#[test]
#[ignore = "mutates process-wide importer policy; CI runs this test alone"]
fn appsink_dma_import_failure_recovers_to_system_memory() {
    gst::init().unwrap();
    let info = gst_video::VideoInfo::builder(gst_video::VideoFormat::Nv12, 64, 64)
        .build()
        .unwrap();
    let caps = info.to_caps().unwrap();
    let path = std::env::temp_dir().join(format!("kld-dma-recovery-{}", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(info.size() as u64).unwrap();
    let allocator = gstreamer_allocators::DmaBufAllocator::new();
    // SAFETY: the allocator takes this live owned FD and its complete extent.
    // It is never submitted to a GPU; this test exercises buffer classification.
    let memory = unsafe { allocator.alloc(file, info.size()) }.unwrap();
    let mut gpu_buffer = gst::Buffer::new();
    gpu_buffer.get_mut().unwrap().append_memory(memory);
    let mut negotiated = AppsinkNegotiatedState::default();
    let mut descriptors = crate::video::dmabuf::DmaBufDescriptorCache::default();
    // The FD is write-only: retaining its allocation must not try to map/copy
    // pixels. MemoryRef::to_owned() would copy and panic on this fixture.
    let initial = descriptors
        .frame_format(
            &gpu_buffer,
            64,
            64,
            crate::video::dmabuf::DRM_FORMAT_NV12,
            0,
            [info.stride()[0], info.stride()[1], 0, 0],
            [info.offset()[0], info.offset()[1], 0, 0],
        )
        .unwrap();
    assert!(matches!(
        initial,
        crate::video::VideoFrameFormat::DmaBufNv12 { .. }
    ));
    drop(descriptors);
    let gpu_sample = gst::Sample::builder()
        .buffer(&gpu_buffer)
        .caps(&caps)
        .build();
    #[cfg(feature = "backend-appsink")]
    let prepared_gpu_player = {
        let (events, _) = tokio::sync::mpsc::channel(8);
        crate::video::VideoPlayer::new(
            path.to_str().unwrap(),
            std::sync::Arc::new("test".into()),
            10,
            0.0,
            crate::video::LatestFrameMailbox::new(),
            events,
            std::sync::Arc::new(crate::metrics::PerformanceMetrics::new()),
            crate::video::VideoBackendRequest::ForceAppsink,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap()
    };
    #[cfg(feature = "backend-appsink")]
    assert!(!prepared_gpu_player.needs_appsink_import_retry());
    assert!(report_appsink_import_failure());
    assert!(!report_appsink_import_failure());
    assert!(!can_import_dmabuf());
    #[cfg(feature = "backend-appsink")]
    {
        assert!(prepared_gpu_player.needs_appsink_import_retry());
        let (events, _) = tokio::sync::mpsc::channel(8);
        let prepared_cpu_player = crate::video::VideoPlayer::new(
            prepared_gpu_player.source_uri(),
            std::sync::Arc::new("test".into()),
            11,
            0.0,
            crate::video::LatestFrameMailbox::new(),
            events,
            std::sync::Arc::new(crate::metrics::PerformanceMetrics::new()),
            crate::video::VideoBackendRequest::ForceAppsink,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            prepared_cpu_player.source_uri(),
            prepared_gpu_player.source_uri()
        );
        assert!(
            !prepared_cpu_player.needs_appsink_import_retry(),
            "CPU recovery cannot request another import retry"
        );
    }
    let fallback_caps = build_video_sink_caps(VideoMode::Auto, &VideoCapabilities::default());
    assert!(!fallback_caps.to_string().contains("memory:DMABuf"));
    assert!(matches!(
        sample_to_video_frame("test", gpu_sample, 1, &mut negotiated),
        Err(gst::FlowError::NotNegotiated)
    ));
    let cpu_buffer = gst::Buffer::from_mut_slice(vec![0; info.size()]);
    let cpu_sample = gst::Sample::builder()
        .buffer(&cpu_buffer)
        .caps(&caps)
        .build();
    let fallback = sample_to_video_frame("test", cpu_sample, 2, &mut negotiated).unwrap();
    assert!(matches!(
        fallback.format,
        crate::video::VideoFrameFormat::Nv12 { .. }
    ));
    #[cfg(feature = "backend-ffmpeg")]
    assert!(
        crate::video::report_native_surface_import_failure(),
        "appsink must not disable FFmpeg imports"
    );
    std::fs::remove_file(path).unwrap();
}
