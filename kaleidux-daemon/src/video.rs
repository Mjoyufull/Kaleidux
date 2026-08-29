use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app as gst_app;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::metrics::PerformanceMetrics;

#[path = "video/capabilities.rs"]
mod capabilities;
pub(super) use capabilities::chroma_plane_extent;
pub use capabilities::{
    VideoBackendKind, VideoBackendRequest, VideoCapabilities, VideoMode, caps_ladder_labels,
    configure_hw_decoders, current_video_capabilities, detect_video_capabilities,
    enabled_video_backend_labels, get_video_backend_request, get_video_mode,
    mpv_backend_is_explicitly_forced, p010_sampling_supported, refresh_video_capabilities,
    resolve_video_backend_request, set_p010_sampling_supported, set_video_backend_request,
    set_video_mode, validate_selected_video_mode, video_backend_feature, video_backend_is_enabled,
};
use capabilities::{build_video_sink_caps, is_nvcodec_decoder_factory};

#[path = "video/bus.rs"]
mod bus;
use bus::BusWatchHandle;
pub use bus::shutdown_bus_dispatcher;
#[path = "video/appsink.rs"]
mod appsink;
#[path = "video/appsink_pool.rs"]
mod appsink_pool;
#[path = "video/dmabuf.rs"]
mod dmabuf;
#[path = "video/drm_syncobj.rs"]
pub(crate) mod drm_syncobj;
#[cfg(feature = "backend-mpv")]
#[path = "video/mpv_backend.rs"]
mod mpv_backend;
#[cfg(not(feature = "backend-mpv"))]
#[path = "video/mpv_backend_disabled.rs"]
mod mpv_backend;
#[cfg(feature = "backend-mpv")]
#[path = "video/mpv_native.rs"]
mod mpv_native;
#[cfg(not(feature = "backend-mpv"))]
#[path = "video/mpv_native_disabled.rs"]
mod mpv_native;
#[cfg(feature = "backend-ffmpeg")]
#[path = "video/native_backend.rs"]
mod native_backend;
#[cfg(not(feature = "backend-ffmpeg"))]
#[path = "video/native_backend_disabled.rs"]
mod native_backend;
pub use appsink::frame_decode_path_label;
pub(crate) use mpv_native::MpvRenderApiRequest;
pub use mpv_native::{MpvComposedVideoTarget, MpvNativeVideoTarget};
#[path = "video/lifecycle.rs"]
mod lifecycle;
pub use lifecycle::{AppsinkQueueLevels, VideoPrebufferProfile, VideoPrebufferResult};

#[path = "video/frame.rs"]
mod frame;
#[path = "video/frame_gl.rs"]
mod frame_gl;
#[path = "video/frame_mailbox.rs"]
mod frame_mailbox;
pub use frame::{
    DrmSyncobjFrame, NativeDmaBufNv12, NativeDmaBufObject, NativeDmaBufPlane, VideoChromaSiting,
    VideoColorMatrix, VideoColorMetadata, VideoColorPrimaries, VideoColorRange,
    VideoContentLightMetadata, VideoCropRect, VideoGeometry, VideoMasteringMetadata, VideoRotation,
    VideoTransfer,
};
pub use frame::{PlayerEvent, PlayerEventKind, VideoFrame, VideoFrameFormat, VideoFrameStorage};
pub(crate) use frame_gl::GlExternalFrame;
pub use frame_mailbox::LatestFrameMailbox;
pub(crate) use native_backend::report_native_surface_import_failure;
pub use native_backend::{NativeDecoderApi, NativePathTier};

pub(super) fn env_flag_enabled(key: &str) -> bool {
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
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_flag_enabled("KLD_TRACE_VIDEO_LAYOUT_EVERY_FRAME"))
}

fn prefer_videoinfo_cuda_layout_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env_flag_enabled("KLD_CUDA_LAYOUT_PREFER_VIDEOINFO"))
}

fn appsink_sync_enabled() -> bool {
    if env_flag_enabled("KLD_APPSINK_UNSYNC") {
        return false;
    }
    match std::env::var("KLD_APPSINK_SYNC") {
        Ok(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
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

const NEVER_PUBLISHED_NS: u64 = u64::MAX;

fn publish_interval_ns(max_publish_fps: Option<u32>) -> Option<u64> {
    let fps = max_publish_fps?;
    if fps == 0 {
        return None;
    }
    Some(1_000_000_000u64 / fps as u64)
}

fn should_publish_now(last_publish_ns: &AtomicU64, interval_ns: Option<u64>, now_ns: u64) -> bool {
    let Some(interval_ns) = interval_ns else {
        return true;
    };
    let previous = last_publish_ns.load(Ordering::Relaxed);
    if previous != NEVER_PUBLISHED_NS && now_ns.saturating_sub(previous) < interval_ns {
        return false;
    }
    last_publish_ns.store(now_ns, Ordering::Relaxed);
    true
}

#[path = "video/pipeline_config.rs"]
mod pipeline_config;
use pipeline_config::{build_publish_rate_filter, configure_pipeline_element};

#[cfg(test)]
#[path = "video/test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
#[path = "video/tests.rs"]
mod tests;

pub struct VideoPlayer {
    source_uri: String,
    pub pipeline: Option<gst::Element>,
    appsink: Option<gst_app::AppSink>,
    mpv: Option<mpv_backend::MpvPlayer>,
    native: Option<native_backend::NativePlayer>,
    backend_kind: VideoBackendKind,
    is_running: Arc<AtomicBool>,
    bus_watch: Option<BusWatchHandle>,
    frame_mailbox: LatestFrameMailbox,
    player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
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
            VideoBackendKind::Mpv => "mpv",
            VideoBackendKind::Ffmpeg => "ffmpeg",
        }
    }

    fn log_backend_snapshot(&self, phase: &str) {
        let sink_factory = self
            .appsink
            .as_ref()
            .map(|sink| sink.upcast_ref::<gst::Element>())
            .and_then(|sink| sink.factory())
            .map(|factory| factory.name().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        info!(
            "[VIDEO-BACKEND] {} phase={} session={} backend={} sink={} pipeline={}",
            self.source_id,
            phase,
            self.session_id,
            self.backend_label(),
            sink_factory,
            self.pipeline
                .as_ref()
                .map(|pipeline| pipeline.name().to_string())
                .unwrap_or_else(|| self.backend_label().to_string())
        );
    }

    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        backend_request: VideoBackendRequest,
        decode_group_id: Option<u64>,
        max_publish_fps: Option<u32>,
        render_size: Option<(u32, u32)>,
        mpv_native_target: Option<MpvNativeVideoTarget>,
        mpv_composed_target: Option<MpvComposedVideoTarget>,
    ) -> anyhow::Result<Self> {
        let creation_start = std::time::Instant::now();
        let mut resolved_backend_request = resolve_video_backend_request(backend_request);
        let backend_is_explicitly_forced = backend_request != VideoBackendRequest::Auto
            || get_video_backend_request() != VideoBackendRequest::Auto;
        if !video_backend_is_enabled(resolved_backend_request) {
            anyhow::bail!(
                "video backend '{}' is disabled in this build (required Cargo feature: {}; enabled backends: {:?})",
                match resolved_backend_request {
                    VideoBackendRequest::Auto => "auto",
                    VideoBackendRequest::ForceAppsink => "appsink",
                    VideoBackendRequest::ForceMpv => "mpv",
                    VideoBackendRequest::ForceFfmpeg => "ffmpeg",
                },
                video_backend_feature(resolved_backend_request),
                enabled_video_backend_labels(),
            );
        }
        if matches!(resolved_backend_request, VideoBackendRequest::ForceFfmpeg) {
            let native_result = Self::new_native(
                uri,
                source_id.clone(),
                session_id,
                volume,
                frame_mailbox.clone(),
                player_event_tx.clone(),
                metrics.clone(),
                decode_group_id,
                max_publish_fps,
                creation_start,
            );
            match native_result {
                Ok(player) => return Ok(player),
                Err(error) if !backend_is_explicitly_forced => {
                    if video_backend_is_enabled(VideoBackendRequest::ForceMpv) {
                        warn!(
                            "[VIDEO] {}: default FFmpeg backend unavailable ({error:#}); falling back to mpv",
                            source_id
                        );
                        resolved_backend_request = VideoBackendRequest::ForceMpv;
                    } else if video_backend_is_enabled(VideoBackendRequest::ForceAppsink) {
                        warn!(
                            "[VIDEO] {}: default FFmpeg backend unavailable ({error:#}); falling back to appsink",
                            source_id
                        );
                        resolved_backend_request = VideoBackendRequest::ForceAppsink;
                    } else {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if matches!(resolved_backend_request, VideoBackendRequest::ForceMpv) {
            let mpv_result = Self::new_mpv(
                uri,
                source_id.clone(),
                session_id,
                volume,
                frame_mailbox.clone(),
                player_event_tx.clone(),
                metrics.clone(),
                max_publish_fps,
                render_size,
                mpv_native_target.clone(),
                mpv_composed_target.clone(),
                creation_start,
            );
            match mpv_result {
                Ok(player) => return Ok(player),
                Err(error)
                    if !backend_is_explicitly_forced
                        && !mpv_backend_is_explicitly_forced()
                        && video_backend_is_enabled(VideoBackendRequest::ForceAppsink) =>
                {
                    warn!(
                        "[VIDEO] {}: default mpv backend unavailable ({error:#}); falling back to appsink",
                        source_id
                    )
                }
                Err(error) => return Err(error),
            }
        }

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

        let audio_enabled = audio_enabled_for_volume(volume);
        pipeline.set_property_from_str("flags", playbin_flags_for_volume(volume));
        pipeline.set_property("message-forward", true);
        if !audio_enabled {
            pipeline.set_property("mute", true);
            pipeline.set_property("volume", 0.0f64);
        }
        if pipeline_name == "playbin3" {
            pipeline.set_property("instant-uri", true);
        }
        if let Some(video_filter) = build_publish_rate_filter(source_id.as_ref(), max_publish_fps) {
            pipeline.set_property("video-filter", &video_filter);
        }

        let full_uri = build_video_uri(uri)?;
        info!("Setting video URI: {}", full_uri);
        pipeline.set_property("uri", &full_uri);
        let tune_source_id = source_id.clone();
        let _ = pipeline.connect("element-setup", false, move |values| {
            if let Ok(element) = values[1].get::<gst::Element>() {
                configure_pipeline_element(tune_source_id.as_ref(), audio_enabled, &element);
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

        let appsink = Self::configure_appsink(
            &pipeline,
            &source_id,
            session_id,
            &frame_mailbox,
            &metrics,
            creation_start,
            &caps,
            &caps_ladder,
            first_frame_logged.clone(),
            decode_path_logged.clone(),
            accept_samples.clone(),
            max_publish_fps,
        )?;
        let backend_kind = VideoBackendKind::Appsink;

        metrics.record_video_backend_session(backend_kind);
        if matches!(resolved_backend_request, VideoBackendRequest::ForceAppsink) {
            debug!(
                "[VIDEO] {}: using requested appsink/WGPU backend",
                source_id
            );
        }

        let player = Self {
            source_uri: uri.to_string(),
            pipeline: Some(pipeline),
            appsink: Some(appsink),
            mpv: None,
            native: None,
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
        };
        player.log_backend_snapshot("created");
        Ok(player)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_mpv(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        max_publish_fps: Option<u32>,
        render_size: Option<(u32, u32)>,
        mpv_native_target: Option<MpvNativeVideoTarget>,
        mpv_composed_target: Option<MpvComposedVideoTarget>,
        creation_start: std::time::Instant,
    ) -> anyhow::Result<Self> {
        let mpv = mpv_backend::MpvPlayer::new(
            uri,
            source_id.clone(),
            session_id,
            volume,
            frame_mailbox.clone(),
            player_event_tx.clone(),
            metrics.clone(),
            max_publish_fps,
            render_size,
            mpv_native_target,
            mpv_composed_target,
            creation_start,
        )?;
        let backend_kind = VideoBackendKind::Mpv;
        metrics.record_video_backend_session(backend_kind);
        let player = Self {
            source_uri: uri.to_string(),
            pipeline: None,
            appsink: None,
            mpv: Some(mpv),
            native: None,
            backend_kind,
            is_running: Arc::new(AtomicBool::new(false)),
            bus_watch: None,
            frame_mailbox,
            player_event_tx,
            source_id,
            session_id,
            start_time: creation_start,
            first_frame_logged: Arc::new(AtomicBool::new(false)),
            decode_path_logged: Arc::new(AtomicBool::new(false)),
            accept_samples: Arc::new(AtomicBool::new(true)),
            pending_start_position_ns: None,
        };
        player.log_backend_snapshot("created");
        Ok(player)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_native(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        decode_group_id: Option<u64>,
        max_publish_fps: Option<u32>,
        creation_start: std::time::Instant,
    ) -> anyhow::Result<Self> {
        let native = native_backend::NativePlayer::new(
            uri,
            source_id.clone(),
            session_id,
            volume,
            frame_mailbox.clone(),
            player_event_tx.clone(),
            metrics.clone(),
            decode_group_id,
            max_publish_fps,
            creation_start,
        )?;
        let backend_kind = VideoBackendKind::Ffmpeg;
        metrics.record_video_backend_session(backend_kind);
        let player = Self {
            source_uri: uri.to_string(),
            pipeline: None,
            appsink: None,
            mpv: None,
            native: Some(native),
            backend_kind,
            is_running: Arc::new(AtomicBool::new(false)),
            bus_watch: None,
            frame_mailbox,
            player_event_tx,
            source_id,
            session_id,
            start_time: creation_start,
            first_frame_logged: Arc::new(AtomicBool::new(false)),
            decode_path_logged: Arc::new(AtomicBool::new(false)),
            accept_samples: Arc::new(AtomicBool::new(true)),
            pending_start_position_ns: None,
        };
        player.log_backend_snapshot("created");
        Ok(player)
    }

    pub fn is_appsink_backend(&self) -> bool {
        self.backend_kind == VideoBackendKind::Appsink
    }

    pub fn is_native_experimental_backend(&self) -> bool {
        self.backend_kind == VideoBackendKind::Ffmpeg
    }

    pub fn request_video_frame(&self) {
        if let Some(native) = self.native.as_ref() {
            native.request_frame();
        }
    }

    pub fn source_uri(&self) -> &str {
        &self.source_uri
    }

    pub fn renders_natively(&self) -> bool {
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.renders_natively();
        }
        false
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn current_position_ns(&self) -> Option<u64> {
        if let Some(native) = self.native.as_ref() {
            return Some(native.current_position_ns());
        }
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.current_position_ns();
        }
        self.pipeline
            .as_ref()?
            .query_position::<gst::ClockTime>()
            .map(gst::ClockTime::nseconds)
    }

    pub fn seek_to_position_ns(&self, position_ns: u64) -> anyhow::Result<()> {
        if let Some(native) = self.native.as_ref() {
            native.seek_to_position_ns(position_ns);
            return Ok(());
        }
        if position_ns == 0 {
            return Ok(());
        }
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.seek_to_position_ns(position_ns);
        }
        self.pipeline
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("GStreamer pipeline missing for seek"))?
            .seek_simple(
                gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT | gst::SeekFlags::ACCURATE,
                gst::ClockTime::from_nseconds(position_ns),
            )?;
        Ok(())
    }

    pub fn set_start_position_ns(&mut self, position_ns: u64) {
        self.pending_start_position_ns = (position_ns > 0).then_some(position_ns);
    }
}
