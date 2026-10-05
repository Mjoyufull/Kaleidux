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

#[cfg(feature = "backend-ffmpeg")]
#[test]
fn native_async_open_failure_is_reported_during_prebuffer() {
    let source = Arc::new("missing-native-input".to_string());
    let path = std::env::temp_dir().join(format!(
        "kaleidux-missing-video-{}-{}.mp4",
        std::process::id(),
        rand::random::<u64>()
    ));
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(32);
    let mut player = super::native_backend::NativePlayer::new(
        path.to_str().unwrap(),
        source,
        u64::MAX,
        0.0,
        LatestFrameMailbox::new(),
        event_tx,
        Arc::new(PerformanceMetrics::new()),
        None,
        None,
        std::time::Instant::now(),
    )
    .expect("worker creation precedes opening the input");
    let error = player
        .prebuffer(|| false)
        .err()
        .expect("failed async open must not commit the backend");
    assert!(error.to_string().contains("native decoder failed:"));
    player.stop().unwrap();
}

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
        storage: buffer.into(),
        width: 1,
        height: 1,
        stride: 4,
        format: VideoFrameFormat::Rgba,
        session_id,
        pts_ns: None,
        duration_ns: None,
        color: Default::default(),
        geometry: crate::video::VideoGeometry::for_dimensions(1, 1),
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
fn retired_session_cannot_clear_replacement_or_other_output() {
    let mailbox = LatestFrameMailbox::new();
    mailbox.publish_frame("DP-1", dummy_frame(2));
    mailbox.publish_frame("DP-2", dummy_frame(1));
    mailbox.clear_session("DP-1", 1);
    assert!(mailbox.pending_sources().contains(&"DP-1".to_owned()));
    assert!(mailbox.has_pending_frame("DP-1"));
    assert!(mailbox.pending_frame_age("DP-1").is_some());
    assert!(mailbox.has_pending_frame("DP-2"));
    mailbox.clear_session("DP-1", 2);
    assert!(!mailbox.has_pending_frame("DP-1"));
    assert!(mailbox.has_pending_frame("DP-2"));
    mailbox.publish_frame("DP-1", dummy_frame(3));
    assert!(mailbox.pending_sources().contains(&"DP-1".to_owned()));
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

#[test]
fn backend_explicitly_forced_logic() {
    with_video_env_test_lock(|| {
        set_video_mode(VideoMode::Auto);
        set_video_backend_request(VideoBackendRequest::Auto);

        assert!(!backend_is_explicitly_forced(VideoBackendRequest::Auto));
        assert!(backend_is_explicitly_forced(
            VideoBackendRequest::ForceFfmpeg
        ));
        assert!(backend_is_explicitly_forced(VideoBackendRequest::ForceMpv));
        assert!(backend_is_explicitly_forced(
            VideoBackendRequest::ForceAppsink
        ));

        set_video_backend_request(VideoBackendRequest::ForceFfmpeg);
        assert!(backend_is_explicitly_forced(VideoBackendRequest::Auto));
        set_video_backend_request(VideoBackendRequest::Auto);

        set_video_mode(VideoMode::StrictCuda);
        assert!(backend_is_explicitly_forced(VideoBackendRequest::Auto));
        set_video_mode(VideoMode::Auto);
    });
}

#[test]
fn candidate_video_backends_preserves_ladder_and_forced_choice() {
    with_video_env_test_lock(|| {
        set_video_mode(VideoMode::Auto);
        set_video_backend_request(VideoBackendRequest::Auto);

        let auto_candidates = candidate_video_backends(VideoBackendRequest::Auto);
        assert!(!auto_candidates.is_empty());
        if cfg!(feature = "backend-ffmpeg") {
            assert_eq!(auto_candidates[0], VideoBackendRequest::ForceFfmpeg);
        }

        let forced_ffmpeg = candidate_video_backends(VideoBackendRequest::ForceFfmpeg);
        assert_eq!(forced_ffmpeg, vec![VideoBackendRequest::ForceFfmpeg]);

        let forced_mpv = candidate_video_backends(VideoBackendRequest::ForceMpv);
        assert_eq!(forced_mpv, vec![VideoBackendRequest::ForceMpv]);

        let forced_appsink = candidate_video_backends(VideoBackendRequest::ForceAppsink);
        assert_eq!(forced_appsink, vec![VideoBackendRequest::ForceAppsink]);
    });
}
