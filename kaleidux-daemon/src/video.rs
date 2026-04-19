use gst::prelude::*;
use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
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
    ForceDmaBuf,
    ForceNv12,
    ForceRgba,
}

static VIDEO_MODE: AtomicU8 = AtomicU8::new(0);
static VIDEO_CAPABILITIES: once_cell::sync::Lazy<parking_lot::Mutex<Option<VideoCapabilities>>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(None));

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
        VideoMode::ForceDmaBuf => 3,
        VideoMode::ForceNv12 => 4,
        VideoMode::ForceRgba => 5,
    };
    VIDEO_MODE.store(val, Ordering::SeqCst);
    info!("[VIDEO] Video mode set to {:?}", mode);
}

pub fn get_video_mode() -> VideoMode {
    match VIDEO_MODE.load(Ordering::SeqCst) {
        1 | 2 => VideoMode::StrictCuda,
        3 => VideoMode::ForceDmaBuf,
        4 => VideoMode::ForceNv12,
        5 => VideoMode::ForceRgba,
        _ => VideoMode::Auto,
    }
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

fn set_optional_property<T>(
    element: &gst::Element,
    property_name: &str,
    value: T,
    applied: &mut Vec<&'static str>,
    applied_label: &'static str,
) where
    T: Clone + Send + Sync + 'static + gst::glib::value::ToValue,
{
    if element.find_property(property_name).is_none() {
        return;
    }

    element.set_property(property_name, &value);
    applied.push(applied_label);
}

fn configure_decoder_element(source_id: &str, element: &gst::Element) {
    let Some(factory_name) = element.factory().map(|factory| factory.name().to_string()) else {
        return;
    };

    if !is_nvcodec_decoder_factory(factory_name.as_str()) {
        return;
    }

    let mut applied = Vec::new();
    set_optional_property(
        element,
        "max-display-delay",
        0i32,
        &mut applied,
        "max-display-delay=0",
    );
    set_optional_property(
        element,
        "num-output-surfaces",
        1u32,
        &mut applied,
        "num-output-surfaces=1",
    );

    if applied.is_empty() {
        info!(
            "[VIDEO] {}: Decoder tuning {} -> no supported low-latency properties",
            source_id, factory_name
        );
    } else {
        info!(
            "[VIDEO] {}: Decoder tuning {} -> {}",
            source_id,
            factory_name,
            applied.join(", ")
        );
    }
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
        }
    }

    #[test]
    fn latest_frame_mailbox_coalesces_same_source_frames() {
        let mailbox = LatestFrameMailbox::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);

        mailbox.publish_frame("DP-2", dummy_frame(1), &tx);
        mailbox.publish_frame("DP-2", dummy_frame(2), &tx);

        assert!(matches!(
            rx.try_recv(),
            Ok(FrameSignal::Ready(source)) if source == "DP-2"
        ));
        assert!(rx.try_recv().is_err());
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);

        mailbox.publish_frame("HDMI-A-1", dummy_frame(5), &tx);
        mailbox.clear_source("HDMI-A-1");
        assert!(mailbox.take_frame("HDMI-A-1").is_none());

        mailbox.publish_frame("HDMI-A-1", dummy_frame(6), &tx);

        assert!(matches!(
            rx.try_recv(),
            Ok(FrameSignal::Ready(source)) if source == "HDMI-A-1"
        ));
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

impl Clone for VideoFrameFormat {
    fn clone(&self) -> Self {
        match self {
            Self::Rgba => Self::Rgba,
            Self::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Self::Nv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            },
            Self::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Self::CudaNv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            },
            Self::I420 {
                y_stride,
                u_offset,
                u_stride,
                v_offset,
                v_stride,
            } => Self::I420 {
                y_stride: *y_stride,
                u_offset: *u_offset,
                u_stride: *u_stride,
                v_offset: *v_offset,
                v_stride: *v_stride,
            },
            Self::DmaBufNv12 {
                y_fd,
                y_stride,
                y_offset,
                uv_fd,
                uv_stride,
                uv_offset,
            } => match (dup_plane_fd_for_clone(y_fd), dup_plane_fd_for_clone(uv_fd)) {
                (Some(y_fd), Some(uv_fd)) => Self::DmaBufNv12 {
                    y_fd,
                    y_stride: *y_stride,
                    y_offset: *y_offset,
                    uv_fd,
                    uv_stride: *uv_stride,
                    uv_offset: *uv_offset,
                },
                (Some(y_fd), None) => {
                    drop(y_fd);
                    tracing::warn!(
                        "[VIDEO] Falling back to CPU NV12 clone after UV DMA-BUF fd duplication failed"
                    );
                    Self::Nv12 {
                        y_stride: *y_stride,
                        uv_offset: *uv_offset,
                        uv_stride: *uv_stride,
                    }
                }
                (None, Some(uv_fd)) => {
                    drop(uv_fd);
                    tracing::warn!(
                        "[VIDEO] Falling back to CPU NV12 clone after Y DMA-BUF fd duplication failed"
                    );
                    Self::Nv12 {
                        y_stride: *y_stride,
                        uv_offset: *uv_offset,
                        uv_stride: *uv_stride,
                    }
                }
                (None, None) => {
                    tracing::warn!(
                        "[VIDEO] Falling back to CPU NV12 clone after DMA-BUF fd duplication failed"
                    );
                    Self::Nv12 {
                        y_stride: *y_stride,
                        uv_offset: *uv_offset,
                        uv_stride: *uv_stride,
                    }
                }
            },
        }
    }
}

/// Video frame carrying pixel data in RGBA or planar YUV formats.
/// Uses gst::Buffer to avoid copying data.
#[derive(Clone)]
pub struct VideoFrame {
    pub buffer: gst::Buffer,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub format: VideoFrameFormat,
    pub session_id: u64,
}

#[derive(Debug, Clone)]
pub enum FrameSignal {
    Ready(String),
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
    pub kind: PlayerEventKind,
    pub reason: String,
}

#[derive(Clone, Default)]
pub struct LatestFrameMailbox {
    frames: Arc<parking_lot::Mutex<HashMap<String, VideoFrame>>>,
    pending_notifications: Arc<parking_lot::Mutex<HashSet<String>>>,
    overwrite_count: Arc<AtomicU64>,
}

impl LatestFrameMailbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish_frame(
        &self,
        source_id: &str,
        frame: VideoFrame,
        signal_tx: &tokio::sync::mpsc::Sender<FrameSignal>,
    ) {
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

        match signal_tx.try_send(FrameSignal::Ready(source_id.to_string())) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.pending_notifications.lock().remove(source_id);
                debug!(
                    "[VIDEO] {}: Frame-ready signal channel full, will retry on next frame",
                    source_id
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.pending_notifications.lock().remove(source_id);
            }
        }
    }

    pub fn take_frame(&self, source_id: &str) -> Option<VideoFrame> {
        let frame = self.frames.lock().remove(source_id);
        self.pending_notifications.lock().remove(source_id);
        frame
    }

    pub fn clear_source(&self, source_id: &str) {
        self.frames.lock().remove(source_id);
        self.pending_notifications.lock().remove(source_id);
    }

    pub fn take_overwrite_count(&self) -> u64 {
        self.overwrite_count.swap(0, Ordering::Relaxed)
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

        if let Some(receiver) = self.finished_rx.lock().take() {
            if receiver.recv_timeout(timeout).is_err() {
                warn!(
                    "[VIDEO] Shared GStreamer bus dispatcher did not exit within {:.1}ms",
                    timeout.as_secs_f64() * 1000.0
                );
                return;
            }
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
    let duped = unsafe { libc::dup(raw) };
    if duped < 0 {
        tracing::warn!(
            "[VIDEO] dup_dma_fd: libc::dup failed: {}",
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
    pub appsink: gst_app::AppSink,
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
}

impl VideoPlayer {
    /// Create a new video player with a bounded channel for backpressure
    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_signal_tx: tokio::sync::mpsc::Sender<FrameSignal>,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
    ) -> anyhow::Result<Self> {
        let _video_start = std::time::Instant::now();
        let creation_start = std::time::Instant::now();
        // Prefer playbin3 on modern GStreamer for lower-latency URI changes and
        // more efficient internal graph management. Fall back to playbin if it
        // is unavailable on the host system.
        let pipeline_name = if gst::ElementFactory::find("playbin3").is_some() {
            "playbin3"
        } else {
            "playbin"
        };
        let pipeline = gst::ElementFactory::make(pipeline_name)
            .name("playbin")
            .build()?;

        // Set the URI
        let full_uri = if uri.contains("://") {
            uri.to_string()
        } else {
            // Convert local path to file:// URI
            let path = std::path::Path::new(uri);
            let abs_path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            };
            format!("file://{}", abs_path.display())
        };

        info!("Setting video URI: {}", full_uri);
        pipeline.set_property("uri", &full_uri);

        // Disable subtitles/buffering unconditionally. Also skip audio decoding entirely
        // when the configured volume is zero to avoid extra decoder/buffer state.
        pipeline.set_property_from_str("flags", playbin_flags_for_volume(volume));
        if !audio_enabled_for_volume(volume) {
            pipeline.set_property("mute", true);
        }
        if pipeline_name == "playbin3" {
            pipeline.set_property("instant-uri", true);
            let tune_source_id = source_id.clone();
            let _ = pipeline.connect("element-setup", false, move |values| {
                if let Ok(element) = values[1].get::<gst::Element>() {
                    configure_decoder_element(tune_source_id.as_ref(), &element);
                }
                None
            });
        }

        // Create appsink for video frames - configure like gSlapper does
        let appsink = gst::ElementFactory::make("appsink")
            .name("video-sink")
            .build()?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast to AppSink"))?;

        let mode = get_video_mode();
        let capabilities = current_video_capabilities();
        let caps_ladder = caps_ladder_labels(mode, &capabilities);
        let caps = build_video_sink_caps(mode, &capabilities);

        appsink.set_caps(Some(&caps));
        appsink.set_sync(true); // Sync to clock
        appsink.set_drop(true); // Drop frames if late - CRITICAL for preventing buffer accumulation
        appsink.set_max_buffers(1); // Match gSlapper: 1 buffer to minimize latency and memory
        appsink.set_property("enable-last-sample", false); // Don't retain a full decoded frame.
        appsink.set_property("wait-on-eos", false); // Tear down without waiting on queued buffers.
        appsink.set_property("qos", true); // Let upstream know we're latency-sensitive.
        appsink.set_property_from_str("leaky-type", "downstream");
        // CRITICAL: Enable emit-signals to get callbacks, but ensure we handle them quickly
        // The new_sample callback will be called for each frame

        // Keep source_id for closure
        let cb_source_id = source_id.clone();

        // Set up new-sample callback
        let frame_signal_tx_clone = frame_signal_tx.clone();
        let frame_mailbox_clone = frame_mailbox.clone();
        let first_frame_logged = Arc::new(AtomicBool::new(false));
        let callback_first_frame_logged = first_frame_logged.clone();
        let decode_path_logged = Arc::new(AtomicBool::new(false));
        let callback_decode_path_logged = decode_path_logged.clone();
        let accept_samples = Arc::new(AtomicBool::new(true));
        let callback_accept_samples = accept_samples.clone();
        let callback_stop_logged = Arc::new(AtomicBool::new(false));
        let callback_stop_logged_clone = callback_stop_logged.clone();
        let creation_time_ref = creation_start;

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
                        let duration = creation_time_ref.elapsed();
                        info!(
                            "[ASSET] {}: First video frame produced in {:.3}ms",
                            source_id,
                            duration.as_secs_f64() * 1000.0
                        );
                    }

                    let session_id = session_id;

                    let sample = match sink.pull_sample() {
                        Ok(s) => s,
                        Err(_) => return Err(gst::FlowError::Error),
                    };

                    if should_abort_appsink_sample(
                        &callback_accept_samples,
                        &callback_stop_logged_clone,
                        source_name,
                    ) {
                        return Err(gst::FlowError::Flushing);
                    }

                    let frame = sample_to_video_frame(sample, session_id)?;
                    maybe_log_decode_path(source_name, &frame, &callback_decode_path_logged);

                    // Send frame - if channel is full, drop frame immediately to release gst::Buffer
                    frame_mailbox_clone.publish_frame(source_name, frame, &frame_signal_tx_clone);

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        // Configure appsink
        appsink.set_property("drop", true);
        appsink.set_property("max-buffers", 1u32);

        // Set appsink as the video sink
        pipeline.set_property("video-sink", &appsink);

        info!(
            "[VIDEO] {}: VideoPlayer created with playbin + appsink (requested_mode={} audio={} caps_ladder={:?} caps={})",
            source_id,
            mode.cli_label(),
            audio_enabled_for_volume(volume),
            caps_ladder,
            caps
        );

        Ok(Self {
            pipeline,
            appsink,
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
        })
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
        while pull_preroll_start.elapsed() < preroll_budget {
            if should_abort() {
                anyhow::bail!("prebuffer aborted");
            }

            let remaining = preroll_budget.saturating_sub(pull_preroll_start.elapsed());
            let wait_slice = remaining.min(preroll_slice);
            let Some(sample) = self
                .appsink
                .try_pull_preroll(gst::ClockTime::from_mseconds(wait_slice.as_millis() as u64))
            else {
                continue;
            };

            preroll = match sample_to_video_frame(sample, self.session_id) {
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
                                kind: PlayerEventKind::FatalLifecycle,
                                reason: reason.clone(),
                            });
                            tracing::error!("[VIDEO] {}: {}", source_id, reason);
                            return gst::glib::ControlFlow::Break;
                        }
                    }
                    MessageView::SegmentDone(..) => {
                        if pipeline
                            .seek_simple(gst::SeekFlags::SEGMENT, gst::ClockTime::ZERO)
                            .is_err()
                        {
                            let reason = "failed to restart segment loop".to_string();
                            let _ = player_event_tx.send(PlayerEvent {
                                source_id: source_id.to_string(),
                                session_id,
                                kind: PlayerEventKind::FatalLifecycle,
                                reason: reason.clone(),
                            });
                            tracing::error!("[VIDEO] {}: {}", source_id, reason);
                            return gst::glib::ControlFlow::Break;
                        }
                    }
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
        if self
            .appsink
            .find_property("current-level-buffers")
            .is_none()
        {
            return None;
        }

        Some(AppsinkQueueLevels {
            buffers: self.appsink.property::<u64>("current-level-buffers"),
            bytes: self
                .appsink
                .find_property("current-level-bytes")
                .map(|_| self.appsink.property::<u64>("current-level-bytes"))
                .unwrap_or_default(),
            time_ns: self
                .appsink
                .find_property("current-level-time")
                .map(|_| self.appsink.property::<u64>("current-level-time"))
                .unwrap_or_default(),
        })
    }

    pub fn request_stop(&mut self) -> anyhow::Result<()> {
        let was_running = self.is_running.swap(false, Ordering::SeqCst);
        self.remove_bus_watch();
        self.accept_samples.store(false, Ordering::SeqCst);
        self.frame_mailbox.clear_source(self.source_id.as_ref());
        if gst::version() >= (1, 16, 3, 0) {
            self.appsink
                .set_callbacks(gst_app::AppSinkCallbacks::builder().build());
        }
        if was_running {
            info!("Stopping video playback...");
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

fn maybe_log_decode_path(source_id: &str, frame: &VideoFrame, logged: &AtomicBool) {
    if !logged.swap(true, Ordering::SeqCst) {
        info!(
            "[VIDEO] {}: Actual decode path={} frame={}x{} session={}",
            source_id,
            frame_decode_path_label(frame),
            frame.width,
            frame.height,
            frame.session_id
        );
    }
}

fn sample_to_video_frame(
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
    let (strides, offsets) = unsafe {
        let raw_meta =
            gst_video::ffi::gst_buffer_get_video_meta(buffer.as_ptr() as *mut gst::ffi::GstBuffer);
        if !raw_meta.is_null() {
            let meta = &*raw_meta;
            (meta.stride, meta.offset)
        } else {
            let vi_strides = video_info.stride();
            let vi_offsets = video_info.offset();
            let mut s = [0i32; 4];
            let mut o = [0usize; 4];
            let n_planes = (video_info.n_planes() as usize).min(4);
            for i in 0..n_planes {
                s[i] = vi_strides[i];
                o[i] = vi_offsets[i] as usize;
            }
            (s, o)
        }
    };
    let y_stride = strides[0] as u32;

    let is_cuda = caps
        .features(0)
        .is_some_and(|f| f.contains("memory:CUDAMemory"));

    let format = match video_info.format() {
        gst_video::VideoFormat::Nv12 => {
            if is_cuda {
                tracing::trace!(
                    "[VIDEO] CUDA NV12 layout: y_stride={}, uv_offset={}, uv_stride={} ({}x{})",
                    strides[0],
                    offsets[1],
                    strides[1],
                    width,
                    height
                );
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

    Ok(VideoFrame {
        buffer,
        width,
        height,
        stride: y_stride,
        format,
        session_id,
    })
}

impl Drop for VideoPlayer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
