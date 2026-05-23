use gst::prelude::*;
use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use tracing::info;

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
static VIDEO_BACKEND_REQUEST: AtomicU8 = AtomicU8::new(0);
static VIDEO_CAPABILITIES: once_cell::sync::Lazy<parking_lot::Mutex<Option<VideoCapabilities>>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(None));
pub(super) static CPU_VIDEO_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);
pub(super) static CUDA_LAYOUT_LOG_SIGNATURES: once_cell::sync::Lazy<
    parking_lot::Mutex<HashMap<String, String>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoBackendKind {
    Appsink,
    MpvExperimental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoBackendRequest {
    Auto,
    ForceAppsink,
    ForceMpvExperimental,
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

pub fn set_video_backend_request(request: VideoBackendRequest) {
    let value = match request {
        VideoBackendRequest::Auto => 0,
        VideoBackendRequest::ForceAppsink => 1,
        VideoBackendRequest::ForceMpvExperimental => 2,
    };
    VIDEO_BACKEND_REQUEST.store(value, Ordering::Relaxed);
}

pub fn get_video_backend_request() -> VideoBackendRequest {
    match VIDEO_BACKEND_REQUEST.load(Ordering::Relaxed) {
        1 => VideoBackendRequest::ForceAppsink,
        2 => VideoBackendRequest::ForceMpvExperimental,
        _ => VideoBackendRequest::Auto,
    }
}

pub fn resolve_video_backend_request(request: VideoBackendRequest) -> VideoBackendRequest {
    match request {
        VideoBackendRequest::Auto => get_video_backend_request(),
        forced => forced,
    }
}

pub fn validate_selected_video_backend(request: VideoBackendRequest) -> anyhow::Result<()> {
    if matches!(request, VideoBackendRequest::ForceMpvExperimental)
        && !cfg!(feature = "mpv-backend")
    {
        anyhow::bail!(
            "--video-backend mpv requires building kaleidux-daemon with the mpv-backend Cargo feature"
        );
    }
    Ok(())
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

pub(crate) fn chroma_plane_extent(width: u32, height: u32) -> (u32, u32) {
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

pub(super) fn build_video_sink_caps(
    mode: VideoMode,
    capabilities: &VideoCapabilities,
) -> gst::Caps {
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
pub(super) fn is_nvcodec_decoder_factory(factory_name: &str) -> bool {
    NVCODEC_DECODER_FACTORIES.contains(&factory_name)
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
