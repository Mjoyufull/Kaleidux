use super::super::appsink::should_warn_about_cpu_video_path;
use super::super::*;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Once;
use std::sync::atomic::AtomicU64;

fn init_gst_for_basic_video_tests() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        gst::init().expect("failed to initialize gstreamer for video tests");
    });
}

#[test]
fn zero_volume_disables_audio_pipeline() {
    assert!(!audio_enabled_for_volume(0.0));
    assert_eq!(playbin_flags_for_volume(0.0), "video");
}

#[test]
fn positive_volume_keeps_audio_pipeline() {
    assert!(audio_enabled_for_volume(0.01));
    assert_eq!(playbin_flags_for_volume(0.01), "video+audio");
}

#[test]
fn zero_volume_configures_decodebin_for_video_only_caps() {
    init_gst_for_basic_video_tests();
    let element = gst::ElementFactory::make("decodebin")
        .name("decodebin-test")
        .build()
        .expect("decodebin should be available");

    configure_pipeline_element("HDMI-A-1", false, &element);

    let expose_all = element.property::<bool>("expose-all-streams");
    let caps = element.property::<gst::Caps>("caps");
    assert!(!expose_all);
    assert_eq!(caps.to_string(), "video/x-raw(ANY)");
}

#[test]
fn local_video_paths_are_percent_encoded_in_file_uris() {
    let uri = build_video_uri("/tmp/video clip #1?.mp4").expect("uri should be built");
    assert_eq!(uri, "file:///tmp/video%20clip%20%231%3F.mp4");
}

#[test]
fn dmabuf_try_clone_preserves_dmabuf_format() {
    let y_file = std::fs::File::open("/dev/null").expect("should open /dev/null");
    let uv_file = std::fs::File::open("/dev/null").expect("should open /dev/null");
    let y_fd: OwnedFd = y_file.into();
    let uv_fd: OwnedFd = uv_file.into();
    let original_y = y_fd.as_raw_fd();
    let original_uv = uv_fd.as_raw_fd();

    let format = VideoFrameFormat::DmaBufNv12 {
        y_fd,
        y_stride: 64,
        y_offset: 0,
        uv_fd,
        uv_stride: 64,
        uv_offset: 128,
    };

    let cloned = format.try_clone().expect("dma-buf clone should succeed");
    match cloned {
        VideoFrameFormat::DmaBufNv12 { y_fd, uv_fd, .. } => {
            assert_ne!(y_fd.as_raw_fd(), original_y);
            assert_ne!(uv_fd.as_raw_fd(), original_uv);
        }
        other => panic!("expected DMA-BUF clone, got {:?}", other),
    }
}

#[test]
fn cpu_video_path_warning_triggers_for_auto_i420() {
    assert!(should_warn_about_cpu_video_path(
        VideoMode::Auto,
        &VideoFrameFormat::I420 {
            y_stride: 1,
            u_offset: 2,
            u_stride: 3,
            v_offset: 4,
            v_stride: 5,
        }
    ));
}

#[test]
fn cpu_video_path_warning_skips_for_forced_cpu_modes() {
    assert!(!should_warn_about_cpu_video_path(
        VideoMode::ForceCpu,
        &VideoFrameFormat::I420 {
            y_stride: 1,
            u_offset: 2,
            u_stride: 3,
            v_offset: 4,
            v_stride: 5,
        }
    ));
    assert!(!should_warn_about_cpu_video_path(
        VideoMode::ForceNv12,
        &VideoFrameFormat::Nv12 {
            y_stride: 1,
            uv_offset: 2,
            uv_stride: 3,
        }
    ));
    assert!(!should_warn_about_cpu_video_path(
        VideoMode::ForceRgba,
        &VideoFrameFormat::Rgba
    ));
}

#[test]
fn publish_interval_is_disabled_without_positive_limit() {
    assert_eq!(publish_interval_ns(None), None);
    assert_eq!(publish_interval_ns(Some(0)), None);
}

#[test]
fn publish_interval_converts_fps_to_nanoseconds() {
    assert_eq!(publish_interval_ns(Some(15)), Some(66_666_666));
    assert_eq!(publish_interval_ns(Some(30)), Some(33_333_333));
}

#[test]
fn publish_gate_throttles_until_interval_elapses() {
    let last_publish_ns = AtomicU64::new(NEVER_PUBLISHED_NS);

    assert!(should_publish_now(&last_publish_ns, Some(100), 0));
    assert!(!should_publish_now(&last_publish_ns, Some(100), 50));
    assert!(should_publish_now(&last_publish_ns, Some(100), 100));
}

#[test]
fn publish_gate_allows_every_frame_without_limit() {
    let last_publish_ns = AtomicU64::new(NEVER_PUBLISHED_NS);

    assert!(should_publish_now(&last_publish_ns, None, 10));
    assert!(should_publish_now(&last_publish_ns, None, 11));
}
