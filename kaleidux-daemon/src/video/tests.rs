use super::test_support::{remove_env_var, set_env_var, with_video_env_test_lock};
use super::*;
use std::sync::Once;

pub(crate) fn init_gst_for_tests() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        gst::init().expect("failed to initialize gstreamer for video tests");
    });
}

#[path = "tests/basic.rs"]
mod basic;

#[test]
fn appsink_pending_refresh_uses_slower_uncapped_default() {
    with_video_env_test_lock(|| {
        let old_value = std::env::var_os("KLD_APPSINK_PENDING_REFRESH_MS");
        remove_env_var("KLD_APPSINK_PENDING_REFRESH_MS");

        assert_eq!(
            super::appsink::appsink_pending_refresh_interval(None),
            Some(std::time::Duration::from_millis(75))
        );
        assert_eq!(
            super::appsink::appsink_pending_refresh_interval(Some(24)),
            Some(std::time::Duration::from_millis(32))
        );

        match old_value {
            Some(value) => set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", value),
            None => remove_env_var("KLD_APPSINK_PENDING_REFRESH_MS"),
        }
    });
}

#[test]
fn appsink_pending_refresh_honors_env_override() {
    with_video_env_test_lock(|| {
        let old_value = std::env::var_os("KLD_APPSINK_PENDING_REFRESH_MS");
        set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", "17");

        assert_eq!(
            super::appsink::appsink_pending_refresh_interval(None),
            Some(std::time::Duration::from_millis(17))
        );
        set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", "-1");
        assert_eq!(super::appsink::appsink_pending_refresh_interval(None), None);

        match old_value {
            Some(value) => set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", value),
            None => remove_env_var("KLD_APPSINK_PENDING_REFRESH_MS"),
        }
    });
}

#[test]
fn appsink_processing_continues_while_player_accepts_samples() {
    let accept_samples = AtomicBool::new(true);
    let callback_stop_logged = AtomicBool::new(false);

    assert!(!should_abort_appsink_sample(
        &accept_samples,
        &callback_stop_logged,
        "HDMI-A-1"
    ));
    assert!(!callback_stop_logged.load(Ordering::SeqCst));
}

#[test]
fn appsink_processing_aborts_once_player_stops_accepting_samples() {
    let accept_samples = AtomicBool::new(false);
    let callback_stop_logged = AtomicBool::new(false);

    assert!(should_abort_appsink_sample(
        &accept_samples,
        &callback_stop_logged,
        "HDMI-A-1"
    ));
    assert!(callback_stop_logged.load(Ordering::SeqCst));
}

#[test]
fn auto_caps_include_i420_and_rgba_cpu_fallbacks() {
    init_gst_for_tests();
    let caps = build_video_sink_caps(VideoMode::Auto, &VideoCapabilities::default());
    let caps_text = caps.to_string();

    assert!(caps_text.contains("format=(string)NV12"));
    assert!(caps_text.contains("format=(string)I420"));
    assert!(caps_text.contains("format=(string)RGBA"));
    assert!(!caps_text.contains("video/x-raw; video/x-raw"));
}

#[test]
fn auto_caps_include_zero_copy_preferences_when_cuda_path_is_present() {
    init_gst_for_tests();
    let capabilities = VideoCapabilities {
        has_nvidia_driver: true,
        nvcodec_decoders: vec!["nvh264dec"],
        vaapi_decoders: Vec::new(),
        cuda_elements: vec!["cudaconvert"],
    };
    let caps_text = build_video_sink_caps(VideoMode::Auto, &capabilities).to_string();

    assert!(caps_text.contains("memory:CUDAMemory"));
    assert!(caps_text.contains("memory:DMABuf"));
    assert!(caps_text.contains("format=(string)I420"));
}

#[test]
fn force_nv12_caps_remain_strict() {
    init_gst_for_tests();
    let caps_text =
        build_video_sink_caps(VideoMode::ForceNv12, &VideoCapabilities::default()).to_string();

    assert!(caps_text.contains("format=(string)NV12"));
    assert!(!caps_text.contains("I420"));
    assert!(!caps_text.contains("RGBA"));
}

#[test]
fn force_cpu_caps_exclude_zero_copy_formats() {
    init_gst_for_tests();
    let caps_text =
        build_video_sink_caps(VideoMode::ForceCpu, &VideoCapabilities::default()).to_string();

    assert!(caps_text.contains("format=(string)NV12"));
    assert!(caps_text.contains("format=(string)I420"));
    assert!(caps_text.contains("format=(string)RGBA"));
    assert!(!caps_text.contains("memory:CUDAMemory"));
    assert!(!caps_text.contains("memory:DMABuf"));
}

#[test]
fn auto_falls_back_when_cuda_path_is_unavailable() {
    init_gst_for_tests();
    let capabilities = VideoCapabilities {
        has_nvidia_driver: true,
        nvcodec_decoders: Vec::new(),
        vaapi_decoders: Vec::new(),
        cuda_elements: vec!["cudaconvert"],
    };
    let caps_text = build_video_sink_caps(VideoMode::Auto, &capabilities).to_string();

    assert!(!caps_text.contains("memory:CUDAMemory"));
    assert!(caps_text.contains("memory:DMABuf"));
    assert!(caps_text.contains("format=(string)RGBA"));
}

#[test]
fn strict_cuda_caps_remain_strict() {
    init_gst_for_tests();
    let caps_text =
        build_video_sink_caps(VideoMode::StrictCuda, &VideoCapabilities::default()).to_string();

    assert!(caps_text.contains("memory:CUDAMemory"));
    assert!(!caps_text.contains("memory:DMABuf"));
    assert!(!caps_text.contains("format=(string)RGBA"));
}

#[test]
fn appsink_sync_defaults_to_enabled() {
    with_video_env_test_lock(|| {
        let old_sync = std::env::var_os("KLD_APPSINK_SYNC");
        let old_unsync = std::env::var_os("KLD_APPSINK_UNSYNC");
        remove_env_var("KLD_APPSINK_SYNC");
        remove_env_var("KLD_APPSINK_UNSYNC");

        assert!(appsink_sync_enabled());

        match old_sync {
            Some(value) => set_env_var("KLD_APPSINK_SYNC", value),
            None => remove_env_var("KLD_APPSINK_SYNC"),
        }
        match old_unsync {
            Some(value) => set_env_var("KLD_APPSINK_UNSYNC", value),
            None => remove_env_var("KLD_APPSINK_UNSYNC"),
        }
    });
}

#[test]
fn appsink_unsync_flag_overrides_sync_defaults() {
    with_video_env_test_lock(|| {
        let old_sync = std::env::var_os("KLD_APPSINK_SYNC");
        let old_unsync = std::env::var_os("KLD_APPSINK_UNSYNC");
        set_env_var("KLD_APPSINK_SYNC", "1");
        set_env_var("KLD_APPSINK_UNSYNC", "1");

        assert!(!appsink_sync_enabled());

        match old_sync {
            Some(value) => set_env_var("KLD_APPSINK_SYNC", value),
            None => remove_env_var("KLD_APPSINK_SYNC"),
        }
        match old_unsync {
            Some(value) => set_env_var("KLD_APPSINK_UNSYNC", value),
            None => remove_env_var("KLD_APPSINK_UNSYNC"),
        }
    });
}

fn dummy_frame(session_id: u64) -> VideoFrame {
    init_gst_for_tests();
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
fn latest_frame_mailbox_coalesces_same_source_frames() {
    let mailbox = LatestFrameMailbox::new();

    mailbox.publish_frame("DP-2", dummy_frame(1));
    mailbox.publish_frame("DP-2", dummy_frame(2));

    assert!(mailbox.has_signal_pending());
    assert_eq!(mailbox.pending_sources(), vec!["DP-2".to_string()]);
    assert_eq!(mailbox.take_overwrite_count(), 1);
    assert_eq!(
        mailbox
            .take_frame("DP-2")
            .expect("latest frame should exist")
            .session_id,
        2
    );
}

#[test]
fn latest_frame_mailbox_clear_source_allows_resignal() {
    let mailbox = LatestFrameMailbox::new();

    mailbox.publish_frame("HDMI-A-1", dummy_frame(5));
    mailbox.clear_source("HDMI-A-1");
    assert!(mailbox.take_frame("HDMI-A-1").is_none());

    mailbox.publish_frame("HDMI-A-1", dummy_frame(6));

    assert!(mailbox.has_signal_pending());
    assert_eq!(mailbox.pending_sources(), vec!["HDMI-A-1".to_string()]);
    assert_eq!(
        mailbox
            .take_frame("HDMI-A-1")
            .expect("frame should be republished after clear")
            .session_id,
        6
    );
}

#[test]
fn appsink_timing_defaults_are_low_latency() {
    with_video_env_test_lock(|| {
        let old_deadline = std::env::var_os("KLD_APPSINK_PROCESSING_DEADLINE_MS");
        let old_lateness = std::env::var_os("KLD_APPSINK_MAX_LATENESS_MS");
        remove_env_var("KLD_APPSINK_PROCESSING_DEADLINE_MS");
        remove_env_var("KLD_APPSINK_MAX_LATENESS_MS");

        assert_eq!(appsink::appsink_processing_deadline_ms(), 20);
        assert_eq!(appsink::appsink_max_lateness_ms(), -1);

        match old_deadline {
            Some(value) => set_env_var("KLD_APPSINK_PROCESSING_DEADLINE_MS", value),
            None => remove_env_var("KLD_APPSINK_PROCESSING_DEADLINE_MS"),
        }
        match old_lateness {
            Some(value) => set_env_var("KLD_APPSINK_MAX_LATENESS_MS", value),
            None => remove_env_var("KLD_APPSINK_MAX_LATENESS_MS"),
        }
    });
}
#[test]
fn appsink_timing_accepts_env_overrides() {
    with_video_env_test_lock(|| {
        let old_deadline = std::env::var_os("KLD_APPSINK_PROCESSING_DEADLINE_MS");
        let old_lateness = std::env::var_os("KLD_APPSINK_MAX_LATENESS_MS");
        set_env_var("KLD_APPSINK_PROCESSING_DEADLINE_MS", "7");
        set_env_var("KLD_APPSINK_MAX_LATENESS_MS", "33");

        assert_eq!(appsink::appsink_processing_deadline_ms(), 7);
        assert_eq!(appsink::appsink_max_lateness_ms(), 33);

        match old_deadline {
            Some(value) => set_env_var("KLD_APPSINK_PROCESSING_DEADLINE_MS", value),
            None => remove_env_var("KLD_APPSINK_PROCESSING_DEADLINE_MS"),
        }
        match old_lateness {
            Some(value) => set_env_var("KLD_APPSINK_MAX_LATENESS_MS", value),
            None => remove_env_var("KLD_APPSINK_MAX_LATENESS_MS"),
        }
    });
}
