use gst::prelude::*;
use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
use libc::uintptr_t;
use std::collections::{HashMap, HashSet};
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoMode {
    Auto,
    StrictCuda,
    ForceCpu,
    ForceDmaBuf,
    ForceNv12,
    ForceRgba,
}

static VIDEO_MODE: AtomicU8 = AtomicU8::new(0);
static VIDEO_CAPABILITIES: once_cell::sync::Lazy<parking_lot::Mutex<Option<VideoCapabilities>>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(None));
static CPU_VIDEO_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);
static CUDA_LAYOUT_LOG_SIGNATURES: once_cell::sync::Lazy<parking_lot::Mutex<HashMap<String, String>>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(HashMap::new()));

#[link(name = "gstwayland-1.0")]
unsafe extern "C" {
    fn gst_wl_display_handle_context_new(
        display: *mut std::ffi::c_void,
    ) -> *mut gst::ffi::GstContext;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoBackendKind {
    Appsink,
    WaylandDirect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoBackendRequest {
    Auto,
    ForceAppsink,
    ForceWaylandDirect,
}

#[derive(Debug, Clone)]
pub struct NativeWaylandVideoTarget {
    pub output_name: String,
    pub display_handle: uintptr_t,
    pub surface_handle: uintptr_t,
    pub width: i32,
    pub height: i32,
}

impl NativeWaylandVideoTarget {
    pub fn new(
        output_name: impl Into<String>,
        display_handle: *mut std::ffi::c_void,
        surface_handle: *mut std::ffi::c_void,
    ) -> Self {
        Self {
            output_name: output_name.into(),
            display_handle: display_handle as uintptr_t,
            surface_handle: surface_handle as uintptr_t,
            width: 1,
            height: 1,
        }
    }

    pub fn with_size(mut self, width: u32, height: u32) -> Self {
        self.width = width.max(1) as i32;
        self.height = height.max(1) as i32;
        self
    }
}

const NVCODEC_DECODER_FACTORIES: [&str; 5] =
    ["nvh264dec", "nvh265dec", "nvav1dec", "nvvp9dec", "nvvp8dec"];
const VAAPI_DECODER_FACTORIES: [&str; 5] =
    ["vah264dec", "vah265dec", "vaav1dec", "vavp9dec", "vavp8dec"];
const CUDA_SUPPORT_FACTORIES: [&str; 4] = [
    "cudaconvert",
    "cudaconvertscale",
    "cudadownload",
    "cudaupload",
];

#[derive(Debug, Clone, Default)]
pub struct VideoCapabilities {
    pub has_nvidia_driver: bool,
    pub nvcodec_decoders: Vec<&'static str>,
    pub vaapi_decoders: Vec<&'static str>,
    pub cuda_elements: Vec<&'static str>,
}

impl VideoCapabilities {
    pub fn has_cuda_path(&self) -> bool {
        self.has_nvidia_driver
            && !self.nvcodec_decoders.is_empty()
            && !self.cuda_elements.is_empty()
    }
}

impl VideoMode {
    pub fn cli_label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::StrictCuda => "cuda",
            Self::ForceCpu => "cpu",
            Self::ForceDmaBuf => "dmabuf",
            Self::ForceNv12 => "nv12",
            Self::ForceRgba => "rgba",
        }
    }
}

pub fn set_video_mode(mode: VideoMode) {
    let val = match mode {
        VideoMode::Auto => 0,
        VideoMode::StrictCuda => 1,
        VideoMode::ForceCpu => 2,
        VideoMode::ForceDmaBuf => 3,
        VideoMode::ForceNv12 => 4,
        VideoMode::ForceRgba => 5,
    };
    VIDEO_MODE.store(val, Ordering::SeqCst);
    info!("[VIDEO] Video mode set to {:?}", mode);
}

pub fn get_video_mode() -> VideoMode {
    match VIDEO_MODE.load(Ordering::SeqCst) {
        1 => VideoMode::StrictCuda,
        2 => VideoMode::ForceCpu,
        3 => VideoMode::ForceDmaBuf,
        4 => VideoMode::ForceNv12,
        5 => VideoMode::ForceRgba,
        _ => VideoMode::Auto,
    }
}

fn env_flag_enabled(key: &str) -> bool {
    std::env::var_os(key)
        .and_then(|value| value.into_string().ok())
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn cuda_layout_log_every_frame_enabled() -> bool {
    env_flag_enabled("KLD_TRACE_VIDEO_LAYOUT_EVERY_FRAME")
}

fn prefer_videoinfo_cuda_layout_enabled() -> bool {
    env_flag_enabled("KLD_CUDA_LAYOUT_PREFER_VIDEOINFO")
}

fn direct_aspect_mode_value() -> String {
    std::env::var("KLD_DIRECT_ASPECT_MODE")
        .unwrap_or_else(|_| "letterbox".to_string())
        .trim()
        .to_ascii_lowercase()
}

pub fn native_wayland_backend_enabled() -> bool {
    if env_flag_enabled("KLD_DISABLE_NATIVE_WAYLAND_VIDEO") {
        return false;
    }

    if env_flag_enabled("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO") {
        return true;
    }

    std::env::var_os("WAYLAND_DISPLAY").is_some()
}

fn resolve_backend_request(
    request: VideoBackendRequest,
    native_wayland_target: Option<&NativeWaylandVideoTarget>,
) -> VideoBackendKind {
    match request {
        VideoBackendRequest::ForceAppsink => VideoBackendKind::Appsink,
        VideoBackendRequest::ForceWaylandDirect => {
            if native_wayland_backend_enabled() && native_wayland_target.is_some() {
                VideoBackendKind::WaylandDirect
            } else {
                VideoBackendKind::Appsink
            }
        }
        VideoBackendRequest::Auto => {
            if native_wayland_backend_enabled() && native_wayland_target.is_some() {
                VideoBackendKind::WaylandDirect
            } else {
                VideoBackendKind::Appsink
            }
        }
    }
}

fn maybe_create_wayland_display_context(
    target: &NativeWaylandVideoTarget,
) -> anyhow::Result<gst::Context> {
    if target.display_handle == 0 {
        anyhow::bail!(
            "native Wayland target for {} has no display handle",
            target.output_name
        );
    }

    let context = unsafe {
        gst::glib::translate::from_glib_full(gst_wl_display_handle_context_new(
            target.display_handle as *mut std::ffi::c_void,
        ))
    };
    Ok(context)
}

fn apply_native_wayland_target(
    pipeline: &gst::Element,
    sink: &gst::Element,
    target: &NativeWaylandVideoTarget,
) -> anyhow::Result<()> {
    let context = maybe_create_wayland_display_context(target)?;
    pipeline.set_context(&context);
    sink.set_context(&context);

    let overlay = sink
        .clone()
        .dynamic_cast::<gst_video::VideoOverlay>()
        .map_err(|_| anyhow::anyhow!("waylandsink does not implement GstVideoOverlay"))?;

    unsafe {
        gst_video::prelude::VideoOverlayExtManual::set_window_handle(
            &overlay,
            target.surface_handle,
        );
    }
    gst_video::prelude::VideoOverlayExt::set_render_rectangle(
        &overlay,
        0,
        0,
        target.width,
        target.height,
    )?;
    debug!(
        "[VIDEO] {}: Applied direct render rectangle {}x{} on surface=0x{:x}",
        target.output_name,
        target.width.max(1),
        target.height.max(1),
        target.surface_handle
    );

    Ok(())
}

fn audio_enabled_for_volume(volume: f64) -> bool {
    volume > f64::EPSILON
}

fn playbin_flags_for_volume(volume: f64) -> &'static str {
    if audio_enabled_for_volume(volume) {
        "video+audio"
    } else {
        "video"
    }
}

fn build_video_uri(uri: &str) -> anyhow::Result<String> {
    if uri.contains("://") {
        return Ok(uri.to_string());
    }

    let path = std::path::Path::new(uri);
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    gst::glib::filename_to_uri(&abs_path, None)
        .map(|uri| uri.to_string())
        .map_err(anyhow::Error::from)
}

fn should_abort_appsink_sample(
    accept_samples: &AtomicBool,
    callback_stop_logged: &AtomicBool,
    source_id: &str,
) -> bool {
    if accept_samples.load(Ordering::SeqCst) {
        return false;
    }

    if !callback_stop_logged.swap(true, Ordering::SeqCst) {
        debug!(
            "[VIDEO] {}: Stopping appsink sample processing for superseded player",
            source_id
        );
    }

    true
}

fn rgba_caps() -> gst::Caps {
    gst::Caps::builder("video/x-raw")
        .field("format", "RGBA")
        .build()
}

fn i420_caps() -> gst::Caps {
    gst::Caps::builder("video/x-raw")
        .field("format", "I420")
        .build()
}

fn nv12_caps() -> gst::Caps {
    gst::Caps::builder("video/x-raw")
        .field("format", "NV12")
        .build()
}

fn dmabuf_nv12_caps() -> gst::Caps {
    gst::Caps::builder("video/x-raw")
        .features([gst_alloc::CAPS_FEATURE_MEMORY_DMABUF.as_str()])
        .field("format", "NV12")
        .build()
}

fn cuda_nv12_caps() -> gst::Caps {
    gst::Caps::builder("video/x-raw")
        .features(["memory:CUDAMemory"])
        .field("format", "NV12")
        .build()
}

fn chroma_plane_extent(width: u32, height: u32) -> (u32, u32) {
    (width.div_ceil(2), height.div_ceil(2))
}

fn element_factories_available(names: &[&'static str]) -> Vec<&'static str> {
    names
        .iter()
        .copied()
        .filter(|name| gst::ElementFactory::find(name).is_some())
        .collect()
}

pub fn detect_video_capabilities() -> VideoCapabilities {
    VideoCapabilities {
        has_nvidia_driver: std::fs::metadata("/proc/driver/nvidia/gpus").is_ok(),
        nvcodec_decoders: element_factories_available(&NVCODEC_DECODER_FACTORIES),
        vaapi_decoders: element_factories_available(&VAAPI_DECODER_FACTORIES),
        cuda_elements: element_factories_available(&CUDA_SUPPORT_FACTORIES),
    }
}

pub fn refresh_video_capabilities() -> VideoCapabilities {
    let capabilities = detect_video_capabilities();
    *VIDEO_CAPABILITIES.lock() = Some(capabilities.clone());
    capabilities
}

pub fn current_video_capabilities() -> VideoCapabilities {
    if let Some(capabilities) = VIDEO_CAPABILITIES.lock().clone() {
        capabilities
    } else {
        refresh_video_capabilities()
    }
}

fn caps_ladder_for_mode(mode: VideoMode, capabilities: &VideoCapabilities) -> Vec<gst::Caps> {
    match mode {
        VideoMode::ForceRgba => vec![rgba_caps()],
        VideoMode::ForceCpu => vec![nv12_caps(), i420_caps(), rgba_caps()],
        VideoMode::ForceNv12 => vec![nv12_caps()],
        VideoMode::ForceDmaBuf => vec![dmabuf_nv12_caps()],
        VideoMode::StrictCuda => vec![cuda_nv12_caps()],
        VideoMode::Auto => {
            let mut ladder = Vec::new();
            if capabilities.has_cuda_path() {
                ladder.push(cuda_nv12_caps());
            }
            ladder.push(dmabuf_nv12_caps());
            ladder.push(nv12_caps());
            ladder.push(i420_caps());
            ladder.push(rgba_caps());
            ladder
        }
    }
}

pub fn caps_ladder_labels(mode: VideoMode, capabilities: &VideoCapabilities) -> Vec<&'static str> {
    match mode {
        VideoMode::ForceRgba => vec!["RGBA"],
        VideoMode::ForceCpu => vec!["NV12", "I420", "RGBA"],
        VideoMode::ForceNv12 => vec!["NV12"],
        VideoMode::ForceDmaBuf => vec!["DMABuf NV12"],
        VideoMode::StrictCuda => vec!["CUDAMemory NV12"],
        VideoMode::Auto => {
            let mut labels = Vec::new();
            if capabilities.has_cuda_path() {
                labels.push("CUDAMemory NV12");
            }
            labels.push("DMABuf NV12");
            labels.push("NV12");
            labels.push("I420");
            labels.push("RGBA");
            labels
        }
    }
}

fn build_video_sink_caps(mode: VideoMode, capabilities: &VideoCapabilities) -> gst::Caps {
    let mut ladder = caps_ladder_for_mode(mode, capabilities).into_iter();
    let mut caps = ladder.next().unwrap_or_else(rgba_caps);
    for next_caps in ladder {
        caps.merge(next_caps);
    }
    caps
}

pub fn validate_selected_video_mode(mode: VideoMode) -> anyhow::Result<VideoCapabilities> {
    let capabilities = refresh_video_capabilities();
    let caps_ladder = caps_ladder_labels(mode, &capabilities);

    info!(
        "[VIDEO] Negotiation requested_mode={} nvidia_driver={} nvcodec={:?} cuda_elements={:?} caps_ladder={:?}",
        mode.cli_label(),
        capabilities.has_nvidia_driver,
        capabilities.nvcodec_decoders,
        capabilities.cuda_elements,
        caps_ladder
    );

    if matches!(mode, VideoMode::StrictCuda) && !capabilities.has_cuda_path() {
        anyhow::bail!(
            "--video-mode cuda requires a working GStreamer CUDA path (nvidia_driver={} nvcodec={:?} cuda_elements={:?})",
            capabilities.has_nvidia_driver,
            capabilities.nvcodec_decoders,
            capabilities.cuda_elements
        );
    }

    Ok(capabilities)
}

fn is_nvcodec_decoder_factory(factory_name: &str) -> bool {
    NVCODEC_DECODER_FACTORIES.contains(&factory_name)
}

fn configure_decoder_element(source_id: &str, element: &gst::Element) {
    let Some(factory_name) = element.factory().map(|factory| factory.name().to_string()) else {
        return;
    };

    if !is_nvcodec_decoder_factory(factory_name.as_str()) {
        return;
    }

    debug!(
        "[VIDEO] {}: Keeping decoder {} on default scheduling/presentation settings",
        source_id, factory_name
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    fn init_gst_for_tests() {
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

    fn with_video_env_test_lock<T>(test: impl FnOnce() -> T) -> T {
        static ENV_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
            once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
        let _guard = ENV_LOCK
            .lock()
            .expect("env test lock should not be poisoned");
        test()
    }

    fn set_env_var(key: &str, value: impl AsRef<std::ffi::OsStr>) {
        unsafe { std::env::set_var(key, value) }
    }

    fn remove_env_var(key: &str) {
        unsafe { std::env::remove_var(key) }
    }

    #[test]
    fn native_wayland_backend_is_enabled_by_default_on_wayland() {
        with_video_env_test_lock(|| {
            let old_disable = std::env::var_os("KLD_DISABLE_NATIVE_WAYLAND_VIDEO");
            let old_experimental = std::env::var_os("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO");
            let old_wayland_display = std::env::var_os("WAYLAND_DISPLAY");

            remove_env_var("KLD_DISABLE_NATIVE_WAYLAND_VIDEO");
            remove_env_var("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO");
            set_env_var("WAYLAND_DISPLAY", "wayland-test-0");

            assert!(native_wayland_backend_enabled());

            match old_disable {
                Some(value) => set_env_var("KLD_DISABLE_NATIVE_WAYLAND_VIDEO", value),
                None => remove_env_var("KLD_DISABLE_NATIVE_WAYLAND_VIDEO"),
            }
            match old_experimental {
                Some(value) => set_env_var("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO", value),
                None => remove_env_var("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO"),
            }
            match old_wayland_display {
                Some(value) => set_env_var("WAYLAND_DISPLAY", value),
                None => remove_env_var("WAYLAND_DISPLAY"),
            }
        });
    }

    #[test]
    fn disable_flag_overrides_default_wayland_direct_backend() {
        with_video_env_test_lock(|| {
            let old_disable = std::env::var_os("KLD_DISABLE_NATIVE_WAYLAND_VIDEO");
            let old_experimental = std::env::var_os("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO");
            let old_wayland_display = std::env::var_os("WAYLAND_DISPLAY");

            set_env_var("KLD_DISABLE_NATIVE_WAYLAND_VIDEO", "1");
            remove_env_var("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO");
            set_env_var("WAYLAND_DISPLAY", "wayland-test-0");

            assert!(!native_wayland_backend_enabled());

            match old_disable {
                Some(value) => set_env_var("KLD_DISABLE_NATIVE_WAYLAND_VIDEO", value),
                None => remove_env_var("KLD_DISABLE_NATIVE_WAYLAND_VIDEO"),
            }
            match old_experimental {
                Some(value) => set_env_var("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO", value),
                None => remove_env_var("KLD_EXPERIMENTAL_NATIVE_WAYLAND_VIDEO"),
            }
            match old_wayland_display {
                Some(value) => set_env_var("WAYLAND_DISPLAY", value),
                None => remove_env_var("WAYLAND_DISPLAY"),
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
}

/// Configure GStreamer decoder element ranks based on detected GPU vendor.
/// On NVIDIA: boost nvcodec decoders above VA-API so GStreamer picks native
/// NVDEC (which can export DMA-BUF via cudadownload) instead of the VA-API
/// shim (nvidia-vaapi-driver, which cannot export DMA-BUF).
pub fn configure_hw_decoders() {
    let capabilities = refresh_video_capabilities();
    let mode = get_video_mode();

    if matches!(mode, VideoMode::ForceCpu) {
        let mut demoted_nvcodec = Vec::new();
        for name in NVCODEC_DECODER_FACTORIES {
            if let Some(factory) = gst::ElementFactory::find(name) {
                factory.set_rank(gst::Rank::MARGINAL);
                demoted_nvcodec.push(name);
            }
        }

        let mut demoted_vaapi = Vec::new();
        for name in VAAPI_DECODER_FACTORIES {
            if let Some(factory) = gst::ElementFactory::find(name) {
                factory.set_rank(gst::Rank::MARGINAL);
                demoted_vaapi.push(name);
            }
        }

        info!(
            "[VIDEO] CPU mode requested: demoted hardware decoders nvcodec {:?}, vaapi {:?}; software decode/system-memory formats will be preferred when available",
            demoted_nvcodec, demoted_vaapi
        );
        return;
    }

    if capabilities.has_nvidia_driver {
        let mut boosted = Vec::new();
        for name in NVCODEC_DECODER_FACTORIES {
            if let Some(factory) = gst::ElementFactory::find(name) {
                factory.set_rank(gst::Rank::PRIMARY + 1);
                boosted.push(name);
            }
        }

        let mut demoted = Vec::new();
        for name in VAAPI_DECODER_FACTORIES {
            if let Some(factory) = gst::ElementFactory::find(name) {
                factory.set_rank(gst::Rank::MARGINAL);
                demoted.push(name);
            }
        }

        info!(
            "[VIDEO] NVIDIA detected: boosted nvcodec {:?}, demoted VA-API {:?}, cuda_elements {:?}, cuda_path={}",
            boosted,
            demoted,
            capabilities.cuda_elements,
            capabilities.has_cuda_path()
        );
    } else {
        info!(
            "[VIDEO] Non-NVIDIA GPU: VA-API decoders preferred for DMA-BUF zero-copy (vaapi={:?})",
            capabilities.vaapi_decoders
        );
    }
}

#[derive(Debug)]
pub enum VideoFrameFormat {
    Rgba,
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
                let y_fd = dup_plane_fd_for_clone(y_fd)?;
                let uv_fd = dup_plane_fd_for_clone(uv_fd)?;
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

#[derive(Debug, Clone, Copy, Default)]
pub struct DirectSinkStats {
    pub rendered: u64,
    pub dropped: u64,
    pub average_rate: f64,
}

#[derive(Clone, Default)]
pub struct LatestFrameMailbox {
    frames: Arc<parking_lot::Mutex<HashMap<String, VideoFrame>>>,
    pending_notifications: Arc<parking_lot::Mutex<HashSet<String>>>,
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
        frame
    }

    pub fn inspect_frame<R, F>(&self, source_id: &str, inspect: F) -> Option<R>
    where
        F: FnOnce(&VideoFrame) -> R,
    {
        let frames = self.frames.lock();
        frames.get(source_id).map(inspect)
    }

    pub fn clear_source(&self, source_id: &str) {
        self.frames.lock().remove(source_id);
        self.pending_notifications.lock().remove(source_id);
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

#[derive(Debug, Clone, Copy)]
pub struct VideoPrebufferProfile {
    pub set_state: std::time::Duration,
    pub state_wait: std::time::Duration,
    pub pull_preroll: std::time::Duration,
    pub set_state_result: &'static str,
    pub state_wait_settled: bool,
    pub current_state: gst::State,
    pub pending_state: gst::State,
}

pub struct VideoPrebufferResult {
    pub frame: Option<VideoFrame>,
    pub profile: VideoPrebufferProfile,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AppsinkQueueLevels {
    pub buffers: u64,
    pub bytes: u64,
    pub time_ns: u64,
}

struct BusWatchHandle {
    context: gst::glib::MainContext,
    source_id: gst::glib::SourceId,
}

impl BusWatchHandle {
    fn remove(self) {
        if let Some(source) = self.context.find_source_by_id(&self.source_id) {
            source.destroy();
        }
    }
}

struct BusDispatcher {
    context: gst::glib::MainContext,
    main_loop: gst::glib::MainLoop,
    thread: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
    finished_rx: parking_lot::Mutex<Option<mpsc::Receiver<()>>>,
    shutdown_started: AtomicBool,
}

impl BusDispatcher {
    fn new() -> Self {
        let context = gst::glib::MainContext::new();
        let main_loop = gst::glib::MainLoop::new(Some(&context), false);
        let context_for_thread = context.clone();
        let main_loop_for_thread = main_loop.clone();
        let (finished_tx, finished_rx) = mpsc::channel();

        let thread = std::thread::Builder::new()
            .name("kaleidux-gst-bus".to_string())
            .spawn(move || {
                context_for_thread
                    .with_thread_default(|| {
                        info!("[VIDEO] Shared GStreamer bus dispatcher started");
                        main_loop_for_thread.run();
                    })
                    .expect("failed to set GLib thread-default context for bus dispatcher");
                let _ = finished_tx.send(());
            })
            .expect("failed to spawn GStreamer bus dispatcher");

        Self {
            context,
            main_loop,
            thread: parking_lot::Mutex::new(Some(thread)),
            finished_rx: parking_lot::Mutex::new(Some(finished_rx)),
            shutdown_started: AtomicBool::new(false),
        }
    }

    fn attach(&self, source: gst::glib::Source) -> BusWatchHandle {
        let source_id = source.attach(Some(&self.context));
        BusWatchHandle {
            context: self.context.clone(),
            source_id,
        }
    }

    fn shutdown(&self, timeout: std::time::Duration) {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return;
        }

        self.main_loop.quit();

        if let Some(receiver) = self.finished_rx.lock().take()
            && receiver.recv_timeout(timeout).is_err()
        {
            warn!(
                "[VIDEO] Shared GStreamer bus dispatcher did not exit within {:.1}ms",
                timeout.as_secs_f64() * 1000.0
            );
            return;
        }

        if let Some(thread) = self.thread.lock().take() {
            let _ = thread.join();
        }
    }
}

static BUS_DISPATCHER: once_cell::sync::Lazy<BusDispatcher> =
    once_cell::sync::Lazy::new(BusDispatcher::new);

pub fn shutdown_bus_dispatcher(timeout: std::time::Duration) {
    BUS_DISPATCHER.shutdown(timeout);
}

fn dup_dma_fd(raw: std::os::unix::io::RawFd) -> Option<OwnedFd> {
    if raw < 0 {
        return None;
    }
    let duped = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) };
    if duped < 0 {
        tracing::warn!(
            "[VIDEO] dup_dma_fd: F_DUPFD_CLOEXEC failed: {}",
            std::io::Error::last_os_error()
        );
        return None;
    }
    Some(unsafe { OwnedFd::from_raw_fd(duped) })
}

fn dup_plane_fd_for_clone(fd: &OwnedFd) -> Option<OwnedFd> {
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
fn extract_dmabuf_nv12(
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

pub struct VideoPlayer {
    pub pipeline: gst::Element,
    appsink: Option<gst_app::AppSink>,
    direct_wayland_sink: Option<gst::Element>,
    backend_kind: VideoBackendKind,
    is_running: Arc<AtomicBool>,
    bus_watch: Option<BusWatchHandle>,
    frame_mailbox: LatestFrameMailbox,
    player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
    source_id: Arc<String>,
    session_id: u64,
    start_time: std::time::Instant,
    first_frame_logged: Arc<AtomicBool>,
    decode_path_logged: Arc<AtomicBool>,
    accept_samples: Arc<AtomicBool>,
    pending_start_position_ns: Option<u64>,
}

impl VideoPlayer {
    fn backend_label(&self) -> &'static str {
        match self.backend_kind {
            VideoBackendKind::Appsink => "appsink",
            VideoBackendKind::WaylandDirect => "wayland-direct",
        }
    }

    /// Create a new video player with a bounded channel for backpressure
    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
        backend_request: VideoBackendRequest,
        native_wayland_target: Option<NativeWaylandVideoTarget>,
    ) -> anyhow::Result<Self> {
        let creation_start = std::time::Instant::now();
        let pipeline_name = if gst::ElementFactory::find("playbin").is_some() {
            "playbin"
        } else if gst::ElementFactory::find("playbin3").is_some() {
            "playbin3"
        } else {
            anyhow::bail!("Neither playbin nor playbin3 is available");
        };
        let pipeline = gst::ElementFactory::make(pipeline_name)
            .name("playbin")
            .build()?;

        let full_uri = build_video_uri(uri)?;
        info!("Setting video URI: {}", full_uri);
        pipeline.set_property("uri", &full_uri);

        pipeline.set_property_from_str("flags", playbin_flags_for_volume(volume));
        pipeline.set_property("message-forward", true);
        if !audio_enabled_for_volume(volume) {
            pipeline.set_property("mute", true);
        }
        if pipeline_name == "playbin3" {
            pipeline.set_property("instant-uri", true);
        }
        let tune_source_id = source_id.clone();
        let _ = pipeline.connect("element-setup", false, move |values| {
            if let Ok(element) = values[1].get::<gst::Element>() {
                configure_decoder_element(tune_source_id.as_ref(), &element);
            }
            None
        });

        let first_frame_logged = Arc::new(AtomicBool::new(false));
        let decode_path_logged = Arc::new(AtomicBool::new(false));
        let accept_samples = Arc::new(AtomicBool::new(true));
        let mode = get_video_mode();
        let capabilities = current_video_capabilities();
        let caps_ladder = caps_ladder_labels(mode, &capabilities);
        let caps = build_video_sink_caps(mode, &capabilities);

        let requested_backend =
            resolve_backend_request(backend_request, native_wayland_target.as_ref());
        let (backend_kind, appsink, direct_wayland_sink) = match requested_backend {
            VideoBackendKind::WaylandDirect => {
                if let Some(target) = native_wayland_target.as_ref() {
                    let sink = Self::configure_native_wayland_sink(&pipeline, target)?;
                    info!(
                        "[VIDEO] {}: VideoPlayer created with playbin + waylandsink direct path on {}",
                        source_id, target.output_name
                    );
                    (VideoBackendKind::WaylandDirect, None, Some(sink))
                } else {
                    (
                        VideoBackendKind::Appsink,
                        Some(Self::configure_appsink(
                            &pipeline,
                            &source_id,
                            session_id,
                            &frame_mailbox,
                            creation_start,
                            &caps,
                            &caps_ladder,
                            first_frame_logged.clone(),
                            decode_path_logged.clone(),
                            accept_samples.clone(),
                        )?),
                        None,
                    )
                }
            }
            VideoBackendKind::Appsink => (
                VideoBackendKind::Appsink,
                Some(Self::configure_appsink(
                    &pipeline,
                    &source_id,
                    session_id,
                    &frame_mailbox,
                    creation_start,
                    &caps,
                    &caps_ladder,
                    first_frame_logged.clone(),
                    decode_path_logged.clone(),
                    accept_samples.clone(),
                )?),
                None,
            ),
        };

        Ok(Self {
            pipeline,
            appsink,
            direct_wayland_sink,
            backend_kind,
            is_running: Arc::new(AtomicBool::new(false)),
            bus_watch: None,
            frame_mailbox,
            player_event_tx,
            source_id,
            session_id,
            start_time: creation_start,
            first_frame_logged,
            decode_path_logged,
            accept_samples,
            pending_start_position_ns: None,
        })
    }

    fn configure_native_wayland_sink(
        pipeline: &gst::Element,
        target: &NativeWaylandVideoTarget,
    ) -> anyhow::Result<gst::Element> {
        let sink = gst::ElementFactory::make("waylandsink")
            .name("video-sink")
            .build()?;
        sink.set_property("sync", true);
        sink.set_property("qos", true);
        sink.set_property("enable-last-sample", false);
        sink.set_property("show-preroll-frame", true);
        let aspect_mode = direct_aspect_mode_value();
        let force_aspect_ratio = !matches!(aspect_mode.as_str(), "crop");
        sink.set_property("force-aspect-ratio", force_aspect_ratio);
        sink.set_property("processing-deadline", 0u64);
        sink.set_property("max-lateness", -1i64);
        if matches!(aspect_mode.as_str(), "crop") {
            debug!(
                "[VIDEO] {}: direct aspect mode=crop requested (force-aspect-ratio=false)",
                target.output_name
            );
        }
        pipeline.set_property("video-sink", &sink);
        apply_native_wayland_target(pipeline, &sink, target)?;
        Self::expose_direct_sink(&sink)?;
        Ok(sink)
    }

    #[allow(clippy::too_many_arguments)]
    fn configure_appsink(
        pipeline: &gst::Element,
        source_id: &Arc<String>,
        session_id: u64,
        frame_mailbox: &LatestFrameMailbox,
        creation_start: std::time::Instant,
        caps: &gst::Caps,
        caps_ladder: &[&str],
        first_frame_logged: Arc<AtomicBool>,
        decode_path_logged: Arc<AtomicBool>,
        accept_samples: Arc<AtomicBool>,
    ) -> anyhow::Result<gst_app::AppSink> {
        let appsink = gst::ElementFactory::make("appsink")
            .name("video-sink")
            .build()?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast to AppSink"))?;

        appsink.set_caps(Some(caps));
        appsink.set_sync(true);
        appsink.set_drop(true);
        appsink.set_max_buffers(1);
        appsink.set_property("enable-last-sample", false);
        appsink.set_property("wait-on-eos", false);
        appsink.set_property("qos", true);
        appsink.set_property_from_str("leaky-type", "downstream");

        let cb_source_id = source_id.clone();
        let frame_mailbox_clone = frame_mailbox.clone();
        let callback_first_frame_logged = first_frame_logged.clone();
        let callback_decode_path_logged = decode_path_logged.clone();
        let callback_accept_samples = accept_samples.clone();
        let callback_stop_logged = Arc::new(AtomicBool::new(false));
        let callback_stop_logged_clone = callback_stop_logged.clone();

        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let source_id = cb_source_id.clone();
                    let source_name = source_id.as_ref().as_str();

                    if should_abort_appsink_sample(
                        &callback_accept_samples,
                        &callback_stop_logged_clone,
                        source_name,
                    ) {
                        return Err(gst::FlowError::Flushing);
                    }

                    if !callback_first_frame_logged.load(Ordering::SeqCst) {
                        callback_first_frame_logged.store(true, Ordering::SeqCst);
                        let duration = creation_start.elapsed();
                        info!(
                            "[ASSET] {}: First video frame produced in {:.3}ms",
                            source_id,
                            duration.as_secs_f64() * 1000.0
                        );
                    }

                    let sample = match sink.pull_sample() {
                        Ok(sample) => sample,
                        Err(_) => return Err(gst::FlowError::Error),
                    };

                    if should_abort_appsink_sample(
                        &callback_accept_samples,
                        &callback_stop_logged_clone,
                        source_name,
                    ) {
                        return Err(gst::FlowError::Flushing);
                    }

                    let frame = sample_to_video_frame(source_name, sample, session_id)?;
                    maybe_log_decode_path(source_name, &frame, &callback_decode_path_logged);
                    frame_mailbox_clone.publish_frame(source_name, frame);

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        appsink.set_property("drop", true);
        appsink.set_property("max-buffers", 1u32);
        pipeline.set_property("video-sink", &appsink);

        info!(
            "[VIDEO] {}: VideoPlayer created with playbin + appsink (requested_mode={} caps_ladder={:?} caps={})",
            source_id,
            get_video_mode().cli_label(),
            caps_ladder,
            caps
        );

        Ok(appsink)
    }

    pub fn is_direct_surface_backend(&self) -> bool {
        self.backend_kind == VideoBackendKind::WaylandDirect
    }

    pub fn is_appsink_backend(&self) -> bool {
        self.backend_kind == VideoBackendKind::Appsink
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn current_position_ns(&self) -> Option<u64> {
        self.pipeline
            .query_position::<gst::ClockTime>()
            .map(gst::ClockTime::nseconds)
    }

    pub fn seek_to_position_ns(&self, position_ns: u64) -> anyhow::Result<()> {
        if position_ns == 0 {
            return Ok(());
        }

        self.pipeline.seek_simple(
            gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT | gst::SeekFlags::ACCURATE,
            gst::ClockTime::from_nseconds(position_ns),
        )?;
        Ok(())
    }

    pub fn set_start_position_ns(&mut self, position_ns: u64) {
        self.pending_start_position_ns = (position_ns > 0).then_some(position_ns);
    }

    fn expose_direct_sink(sink: &gst::Element) -> anyhow::Result<()> {
        let overlay = sink
            .clone()
            .dynamic_cast::<gst_video::VideoOverlay>()
            .map_err(|_| anyhow::anyhow!("waylandsink does not implement GstVideoOverlay"))?;
        gst_video::prelude::VideoOverlayExt::expose(&overlay);
        Ok(())
    }

    pub fn direct_sink_stats(&self) -> Option<DirectSinkStats> {
        let sink = self.direct_wayland_sink.as_ref()?;
        sink.find_property("stats")?;

        let structure = sink.property::<gst::Structure>("stats");
        Some(DirectSinkStats {
            rendered: structure.get::<u64>("rendered").ok().unwrap_or(0),
            dropped: structure.get::<u64>("dropped").ok().unwrap_or(0),
            average_rate: structure.get::<f64>("average-rate").ok().unwrap_or(0.0),
        })
    }

    pub fn update_direct_surface_size(&self, width: u32, height: u32) -> anyhow::Result<()> {
        let Some(sink) = self.direct_wayland_sink.as_ref() else {
            return Ok(());
        };

        let overlay = sink
            .clone()
            .dynamic_cast::<gst_video::VideoOverlay>()
            .map_err(|_| anyhow::anyhow!("waylandsink does not implement GstVideoOverlay"))?;
        gst_video::prelude::VideoOverlayExt::set_render_rectangle(
            &overlay,
            0,
            0,
            width.max(1) as i32,
            height.max(1) as i32,
        )?;
        debug!(
            "[VIDEO] {}: Updated direct render rectangle to {}x{}",
            self.source_id,
            width.max(1),
            height.max(1)
        );
        Self::expose_direct_sink(sink)?;
        Ok(())
    }

    /// Pre-buffer video by moving the pipeline to PAUSED and extracting the preroll sample.
    pub fn prebuffer<F>(&mut self, should_abort: F) -> anyhow::Result<VideoPrebufferResult>
    where
        F: Fn() -> bool,
    {
        debug!("[VIDEO] {}: Pre-buffering video pipeline", self.source_id);
        let set_state_start = std::time::Instant::now();
        let ret = self.pipeline.set_state(gst::State::Paused)?;
        let set_state_duration = set_state_start.elapsed();
        match ret {
            gst::StateChangeSuccess::Success => debug!(
                "[VIDEO] {}: Pipeline state -> Paused (pre-roll complete)",
                self.source_id
            ),
            gst::StateChangeSuccess::Async => debug!(
                "[VIDEO] {}: Pipeline state -> Paused (Async, pre-buffering)",
                self.source_id
            ),
            _ => {}
        };
        let set_state_result = match ret {
            gst::StateChangeSuccess::Success => "success",
            gst::StateChangeSuccess::Async => "async",
            gst::StateChangeSuccess::NoPreroll => "no-preroll",
        };

        let state_wait_start = std::time::Instant::now();
        let state_wait_budget = std::time::Duration::from_millis(1500);
        let state_wait_slice = std::time::Duration::from_millis(50);
        let mut state_settled = false;
        let mut current = gst::State::Null;
        let mut pending = gst::State::VoidPending;
        while state_wait_start.elapsed() < state_wait_budget {
            if should_abort() {
                anyhow::bail!("prebuffer aborted");
            }

            let remaining = state_wait_budget.saturating_sub(state_wait_start.elapsed());
            let wait_slice = remaining.min(state_wait_slice);
            let (state_result, current_state, pending_state) = self
                .pipeline
                .state(gst::ClockTime::from_mseconds(wait_slice.as_millis() as u64));
            current = current_state;
            pending = pending_state;
            if state_result.is_ok() {
                state_settled = true;
                break;
            }
        }
        let state_wait_duration = state_wait_start.elapsed();
        if state_settled {
            debug!(
                "[VIDEO] {}: Pre-buffer state settled at {:?} (pending {:?})",
                self.source_id, current, pending
            );
        } else {
            debug!(
                "[VIDEO] {}: Timed out waiting for pre-buffer state ({:?} -> {:?})",
                self.source_id, current, pending
            );
        }

        let pull_preroll_start = std::time::Instant::now();
        let preroll_budget = std::time::Duration::from_millis(250);
        let preroll_slice = std::time::Duration::from_millis(50);
        let mut preroll = None;
        if let Some(appsink) = self.appsink.as_ref() {
            while pull_preroll_start.elapsed() < preroll_budget {
                if should_abort() {
                    anyhow::bail!("prebuffer aborted");
                }

                let remaining = preroll_budget.saturating_sub(pull_preroll_start.elapsed());
                let wait_slice = remaining.min(preroll_slice);
                let Some(sample) = appsink
                    .try_pull_preroll(gst::ClockTime::from_mseconds(wait_slice.as_millis() as u64))
                else {
                    continue;
                };

                preroll = match sample_to_video_frame(self.source_id.as_ref(), sample, self.session_id) {
                    Ok(frame) => Some(frame),
                    Err(e) => {
                        debug!(
                            "[VIDEO] {}: Failed to decode preroll sample: {:?}",
                            self.source_id, e
                        );
                        None
                    }
                };
                break;
            }
        }
        let pull_preroll_duration = pull_preroll_start.elapsed();

        let profile = VideoPrebufferProfile {
            set_state: set_state_duration,
            state_wait: state_wait_duration,
            pull_preroll: pull_preroll_duration,
            set_state_result,
            state_wait_settled: state_settled,
            current_state: current,
            pending_state: pending,
        };

        debug!(
            "[VIDEO] {}: Pre-buffer timings set_state {:.1}ms ({}) + wait_state {:.1}ms settled={} current={:?} pending={:?} + pull_preroll {:.1}ms preroll_frame={}",
            self.source_id,
            profile.set_state.as_secs_f64() * 1000.0,
            profile.set_state_result,
            profile.state_wait.as_secs_f64() * 1000.0,
            profile.state_wait_settled,
            profile.current_state,
            profile.pending_state,
            profile.pull_preroll.as_secs_f64() * 1000.0,
            preroll.is_some()
        );

        if preroll.is_some() && !self.first_frame_logged.swap(true, Ordering::SeqCst) {
            let duration = self.start_time.elapsed();
            info!(
                "[ASSET] {}: First video frame produced in {:.3}ms (preroll)",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            );
        }
        if let Some(frame) = preroll.as_ref() {
            maybe_log_decode_path(self.source_id.as_ref(), frame, &self.decode_path_logged);
        } else if self.is_direct_surface_backend()
            && !self.first_frame_logged.swap(true, Ordering::SeqCst)
        {
            info!(
                "[ASSET] {}: Native Wayland video pipeline preroll completed in {:.3}ms",
                self.source_id,
                self.start_time.elapsed().as_secs_f64() * 1000.0
            );
        }

        Ok(VideoPrebufferResult {
            frame: preroll,
            profile,
        })
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        info!(
            "[VIDEO] {}: Starting playback for {}",
            self.source_id,
            self.pipeline.name()
        );

        // Start pipeline (or transition from Ready to Playing if pre-buffered)
        let ret = self.pipeline.set_state(gst::State::Playing)?;
        let duration = self.start_time.elapsed();
        match ret {
            gst::StateChangeSuccess::Success => info!(
                "[VIDEO] {}: Pipeline state -> Playing in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
            gst::StateChangeSuccess::Async => info!(
                "[VIDEO] {}: Pipeline state -> Playing (Async) in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
            gst::StateChangeSuccess::NoPreroll => info!(
                "[VIDEO] {}: Pipeline state -> Playing (Live) in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
        }

        self.install_bus_watch()?;
        if let Some(position_ns) = self.pending_start_position_ns.take()
            && let Err(e) = self.seek_to_position_ns(position_ns)
        {
            debug!(
                "[VIDEO] {}: Direct/startup seek to {:.1}ms was skipped after start: {}",
                self.source_id,
                position_ns as f64 / 1_000_000.0,
                e
            );
        }
        if let Some(sink) = self.direct_wayland_sink.as_ref()
            && let Err(e) = Self::expose_direct_sink(sink)
        {
            debug!(
                "[VIDEO] {}: Direct wayland expose after start failed: {}",
                self.source_id, e
            );
        }
        self.is_running.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn install_bus_watch(&mut self) -> anyhow::Result<()> {
        self.remove_bus_watch();
        let bus = self
            .pipeline
            .bus()
            .ok_or_else(|| anyhow::anyhow!("Pipeline has no bus"))?;
        let pipeline = self.pipeline.clone();
        let source_id = self.source_id.clone();
        let player_event_tx = self.player_event_tx.clone();
        let session_id = self.session_id;
        let backend_kind = self.backend_kind;

        let watch = bus.create_watch(
            Some(&format!("kaleidux-bus-{}", source_id)),
            gst::glib::Priority::DEFAULT,
            move |_bus, msg| {
                use gst::MessageView;

                match msg.view() {
                    MessageView::StateChanged(s)
                        if s.src()
                            .as_ref()
                            .map(|src| {
                                std::ptr::eq(
                                    src.as_ptr() as *const std::ffi::c_void,
                                    pipeline.as_ptr() as *const std::ffi::c_void,
                                )
                            })
                            .unwrap_or(false) =>
                    {
                        debug!(
                            "[VIDEO] {}: Pipeline state changed from {:?} to {:?}",
                            source_id,
                            s.old(),
                            s.current()
                        );
                    }
                    MessageView::Eos(..) => {
                        let _ = player_event_tx.send(PlayerEvent {
                            source_id: source_id.to_string(),
                            session_id,
                            backend_kind,
                            kind: PlayerEventKind::Eos,
                            reason: "eos".to_string(),
                        });
                        if pipeline
                            .seek_simple(
                                gst::SeekFlags::FLUSH | gst::SeekFlags::SEGMENT,
                                gst::ClockTime::ZERO,
                            )
                            .is_err()
                        {
                            let reason = "failed to seek to start after eos".to_string();
                            let _ = player_event_tx.send(PlayerEvent {
                                source_id: source_id.to_string(),
                                session_id,
                                backend_kind,
                                kind: PlayerEventKind::FatalLifecycle,
                                reason: reason.clone(),
                            });
                            tracing::error!("[VIDEO] {}: {}", source_id, reason);
                            return gst::glib::ControlFlow::Break;
                        }
                    }
                    MessageView::SegmentDone(..)
                        if pipeline
                            .seek_simple(gst::SeekFlags::SEGMENT, gst::ClockTime::ZERO)
                            .is_err() =>
                    {
                        let reason = "failed to restart segment loop".to_string();
                        let _ = player_event_tx.send(PlayerEvent {
                            source_id: source_id.to_string(),
                            session_id,
                            backend_kind,
                            kind: PlayerEventKind::FatalLifecycle,
                            reason: reason.clone(),
                        });
                        tracing::error!("[VIDEO] {}: {}", source_id, reason);
                        return gst::glib::ControlFlow::Break;
                    }
                    MessageView::SegmentDone(..) => {}
                    MessageView::Error(err) => {
                        let error_msg = format!(
                            "Error from {:?}: {} ({:?})",
                            err.src().map(|s| s.path_string()),
                            err.error(),
                            err.debug()
                        );
                        tracing::error!("[VIDEO] {}: {}", source_id, error_msg);
                        let _ = player_event_tx.send(PlayerEvent {
                            source_id: source_id.to_string(),
                            session_id,
                            backend_kind,
                            kind: PlayerEventKind::Error,
                            reason: error_msg,
                        });
                        return gst::glib::ControlFlow::Break;
                    }
                    _ => {}
                }

                gst::glib::ControlFlow::Continue
            },
        );

        self.bus_watch = Some(BUS_DISPATCHER.attach(watch));
        Ok(())
    }

    fn remove_bus_watch(&mut self) {
        if let Some(watch) = self.bus_watch.take() {
            watch.remove();
        }
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        self.request_stop()
    }

    pub fn appsink_queue_levels(&self) -> Option<AppsinkQueueLevels> {
        let appsink = self.appsink.as_ref()?;
        appsink.find_property("current-level-buffers")?;

        Some(AppsinkQueueLevels {
            buffers: appsink.property::<u64>("current-level-buffers"),
            bytes: appsink
                .find_property("current-level-bytes")
                .map(|_| appsink.property::<u64>("current-level-bytes"))
                .unwrap_or_default(),
            time_ns: appsink
                .find_property("current-level-time")
                .map(|_| appsink.property::<u64>("current-level-time"))
                .unwrap_or_default(),
        })
    }

    pub fn request_stop(&mut self) -> anyhow::Result<()> {
        let was_running = self.is_running.swap(false, Ordering::SeqCst);
        self.remove_bus_watch();
        self.accept_samples.store(false, Ordering::SeqCst);
        self.frame_mailbox.clear_source(self.source_id.as_ref());
        if let Some(bus) = self.pipeline.bus() {
            bus.unset_sync_handler();
        }
        if gst::version() >= (1, 16, 3, 0)
            && let Some(appsink) = self.appsink.as_ref()
        {
            appsink.set_callbacks(gst_app::AppSinkCallbacks::builder().build());
        }
        if was_running {
            info!(
                "[VIDEO] {}: Stopping video playback (session={} backend={})",
                self.source_id,
                self.session_id,
                self.backend_label()
            );
            // Fade audio before teardown to prevent clicks on audio-enabled pipelines.
            self.pipeline.set_property("volume", 0.0);
        }

        // Pause first (transition to Ready state first helps cleanup).
        let _ = self.pipeline.set_state(gst::State::Paused);

        // Always force Null, even for players that never fully entered Playing.
        // Prebuffered or failed-start pipelines can still hold decoder state.
        self.pipeline.set_state(gst::State::Null)?;
        Ok(())
    }

    pub fn set_volume(&mut self, volume: f64) {
        self.pipeline.set_property("volume", volume);
    }

    pub fn pause(&self) -> anyhow::Result<()> {
        self.pipeline.set_state(gst::State::Paused)?;
        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        self.pipeline.set_state(gst::State::Playing)?;
        Ok(())
    }
}

pub fn frame_decode_path_label(frame: &VideoFrame) -> &'static str {
    match frame.format {
        VideoFrameFormat::Rgba => "rgba",
        VideoFrameFormat::Nv12 { .. } => "nv12",
        VideoFrameFormat::DmaBufNv12 { .. } => "dmabuf-nv12",
        VideoFrameFormat::CudaNv12 { .. } => "cuda-nv12",
        VideoFrameFormat::I420 { .. } => "i420",
    }
}

fn should_warn_about_cpu_video_path(mode: VideoMode, format: &VideoFrameFormat) -> bool {
    matches!(
        (mode, format),
        (
            VideoMode::Auto | VideoMode::StrictCuda | VideoMode::ForceDmaBuf,
            VideoFrameFormat::Nv12 { .. } | VideoFrameFormat::I420 { .. } | VideoFrameFormat::Rgba
        )
    )
}

fn maybe_log_decode_path(source_id: &str, frame: &VideoFrame, logged: &AtomicBool) {
    if !logged.swap(true, Ordering::SeqCst) {
        let actual_path = frame_decode_path_label(frame);
        info!(
            "[VIDEO] {}: Actual decode path={} frame={}x{} session={}",
            source_id, actual_path, frame.width, frame.height, frame.session_id
        );

        let requested_mode = get_video_mode();
        if should_warn_about_cpu_video_path(requested_mode, &frame.format)
            && !CPU_VIDEO_FALLBACK_WARNED.swap(true, Ordering::SeqCst)
        {
            let capabilities = current_video_capabilities();
            warn!(
                "[VIDEO] {}: Falling back to CPU video path (actual={} requested_mode={} nvidia_driver={} vaapi={:?} nvcodec={:?} cuda_elements={:?}); this usually means hardware decode/zero-copy is unavailable and can cause high CPU usage",
                source_id,
                actual_path,
                requested_mode.cli_label(),
                capabilities.has_nvidia_driver,
                capabilities.vaapi_decoders,
                capabilities.nvcodec_decoders,
                capabilities.cuda_elements
            );
        }
    }
}

fn sample_to_video_frame(
    source_name: &str,
    sample: gst::Sample,
    session_id: u64,
) -> Result<VideoFrame, gst::FlowError> {
    let buffer = match sample.buffer() {
        Some(b) => b.to_owned(),
        None => return Err(gst::FlowError::Error),
    };

    let caps = match sample.caps() {
        Some(c) => c,
        None => return Err(gst::FlowError::Error),
    };

    let video_info = match gst_video::VideoInfo::from_caps(caps) {
        Ok(vi) => vi,
        Err(_) => return Err(gst::FlowError::Error),
    };

    let width = video_info.width();
    let height = video_info.height();

    // Prefer GstVideoMeta stride/offset (reflects actual memory layout from
    // hardware decoders), fall back to VideoInfo when the meta is absent.
    let (meta_strides, meta_offsets, has_meta) = unsafe {
        let raw_meta =
            gst_video::ffi::gst_buffer_get_video_meta(buffer.as_ptr() as *mut gst::ffi::GstBuffer);
        if !raw_meta.is_null() {
            let meta = &*raw_meta;
            (meta.stride, meta.offset, true)
        } else {
            ([0i32; 4], [0usize; 4], false)
        }
    };
    let vi_strides = video_info.stride();
    let vi_offsets = video_info.offset();
    let mut vi_s = [0i32; 4];
    let mut vi_o = [0usize; 4];
    let n_planes = (video_info.n_planes() as usize).min(4);
    vi_s[..n_planes].copy_from_slice(&vi_strides[..n_planes]);
    vi_o[..n_planes].copy_from_slice(&vi_offsets[..n_planes]);
    let buffer_size = buffer.size();
    let caps_str = caps.to_string();

    let is_cuda = caps
        .features(0)
        .is_some_and(|f| f.contains("memory:CUDAMemory"));
    let (strides, offsets) = if is_cuda && prefer_videoinfo_cuda_layout_enabled() {
        (vi_s, vi_o)
    } else if has_meta {
        (meta_strides, meta_offsets)
    } else {
        (vi_s, vi_o)
    };
    let y_stride = strides[0] as u32;

    let format = match video_info.format() {
        gst_video::VideoFormat::Nv12 => {
            let (uv_width, uv_height) = chroma_plane_extent(width, height);
            let min_y_bytes = y_stride as usize * height as usize;
            let uv_offset = offsets[1];
            let uv_stride = strides[1].max(0) as usize;
            let min_uv_bytes = uv_stride.saturating_mul(uv_height as usize);
            let nv12_layout_invalid = strides[0] <= 0
                || strides[1] <= 0
                || y_stride < width
                || uv_stride < (uv_width.saturating_mul(2)) as usize
                || uv_offset > buffer_size
                || uv_offset.saturating_add(min_uv_bytes) > buffer_size
                || min_y_bytes > buffer_size;
            if nv12_layout_invalid {
                tracing::warn!(
                    "[VIDEO] Invalid NV12 layout detected: size={} frame={}x{} y_stride={} uv_offset={} uv_stride={} caps={}",
                    buffer_size,
                    width,
                    height,
                    strides[0],
                    offsets[1],
                    strides[1],
                    caps_str
                );
            }
            if is_cuda {
                let signature = format!(
                    "{}x{}:{}:{}:{}:{}:{}:{}:{}",
                    width,
                    height,
                    strides[0],
                    offsets[1],
                    strides[1],
                    buffer_size,
                    has_meta as u8,
                    vi_s[0],
                    vi_s[1]
                );
                let key = format!("{source_name}:{session_id}");
                let mut should_log_layout = cuda_layout_log_every_frame_enabled();
                if !should_log_layout {
                    let mut signatures = CUDA_LAYOUT_LOG_SIGNATURES.lock();
                    if signatures.get(&key) != Some(&signature) {
                        signatures.insert(key, signature);
                        should_log_layout = true;
                    }
                }
                if should_log_layout {
                    tracing::debug!(
                        "[VIDEO] CUDA NV12 layout {}: y_stride={} uv_offset={} uv_stride={} frame={}x{} size={} has_meta={} vi_y_stride={} vi_uv_stride={} caps={}",
                        source_name,
                        strides[0],
                        offsets[1],
                        strides[1],
                        width,
                        height,
                        buffer_size,
                        has_meta,
                        vi_s[0],
                        vi_s[1],
                        caps_str
                    );
                }
                VideoFrameFormat::CudaNv12 {
                    y_stride,
                    uv_offset: offsets[1] as u32,
                    uv_stride: strides[1] as u32,
                }
            } else {
                let is_dmabuf = buffer.n_memory() > 0
                    && buffer
                        .peek_memory(0)
                        .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
                        .is_some();

                if is_dmabuf {
                    extract_dmabuf_nv12(&buffer, strides, offsets)
                } else {
                    VideoFrameFormat::Nv12 {
                        y_stride,
                        uv_offset: offsets[1] as u32,
                        uv_stride: strides[1] as u32,
                    }
                }
            }
        }
        gst_video::VideoFormat::I420 => VideoFrameFormat::I420 {
            y_stride,
            u_offset: offsets[1] as u32,
            u_stride: strides[1] as u32,
            v_offset: offsets[2] as u32,
            v_stride: strides[2] as u32,
        },
        gst_video::VideoFormat::Rgba => VideoFrameFormat::Rgba,
        other => {
            tracing::error!("[VIDEO] Unsupported format {:?}, negotiation failed", other);
            return Err(gst::FlowError::NotNegotiated);
        }
    };

    let pts_ns = buffer.pts().map(|pts| pts.nseconds());
    let duration_ns = buffer.duration().map(|duration| duration.nseconds());

    Ok(VideoFrame {
        buffer,
        width,
        height,
        stride: y_stride,
        format,
        session_id,
        pts_ns,
        duration_ns,
    })
}

impl Drop for VideoPlayer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
