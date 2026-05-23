use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app as gst_app;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, info};

use crate::metrics::PerformanceMetrics;

#[path = "video/capabilities.rs"]
mod capabilities;
pub(super) use capabilities::chroma_plane_extent;
pub use capabilities::{
    VideoBackendKind, VideoBackendRequest, VideoCapabilities, VideoMode, caps_ladder_labels,
    configure_hw_decoders, current_video_capabilities, detect_video_capabilities,
    get_video_backend_request, get_video_mode, refresh_video_capabilities,
    resolve_video_backend_request, set_video_backend_request, set_video_mode,
    validate_selected_video_backend, validate_selected_video_mode,
};
use capabilities::{build_video_sink_caps, is_nvcodec_decoder_factory};

#[path = "video/bus.rs"]
mod bus;
use bus::BusWatchHandle;
pub use bus::shutdown_bus_dispatcher;
#[path = "video/appsink.rs"]
mod appsink;
#[path = "video/dmabuf.rs"]
mod dmabuf;
#[cfg(feature = "mpv-backend")]
#[path = "video/mpv_backend.rs"]
mod mpv_backend;
pub use appsink::frame_decode_path_label;
#[path = "video/lifecycle.rs"]
mod lifecycle;
pub use lifecycle::{AppsinkQueueLevels, VideoPrebufferProfile, VideoPrebufferResult};

#[path = "video/frame.rs"]
mod frame;
pub use frame::{LatestFrameMailbox, PlayerEvent, PlayerEventKind, VideoFrame, VideoFrameFormat};

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
    env_flag_enabled("KLD_TRACE_VIDEO_LAYOUT_EVERY_FRAME")
}

fn prefer_videoinfo_cuda_layout_enabled() -> bool {
    env_flag_enabled("KLD_CUDA_LAYOUT_PREFER_VIDEOINFO")
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
use pipeline_config::configure_pipeline_element;

#[cfg(test)]
#[path = "video/test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
#[path = "video/tests.rs"]
mod tests;

pub struct VideoPlayer {
    pub pipeline: Option<gst::Element>,
    appsink: Option<gst_app::AppSink>,
    #[cfg(feature = "mpv-backend")]
    mpv: Option<mpv_backend::MpvPlayer>,
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
            VideoBackendKind::MpvExperimental => "mpv-experimental",
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
        player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        backend_request: VideoBackendRequest,
        max_publish_fps: Option<u32>,
    ) -> anyhow::Result<Self> {
        let creation_start = std::time::Instant::now();
        let resolved_backend_request = resolve_video_backend_request(backend_request);
        validate_selected_video_backend(resolved_backend_request)?;
        if matches!(
            resolved_backend_request,
            VideoBackendRequest::ForceMpvExperimental
        ) {
            return Self::new_mpv_experimental(
                uri,
                source_id,
                session_id,
                volume,
                frame_mailbox,
                player_event_tx,
                metrics,
                max_publish_fps,
                creation_start,
            );
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
            pipeline: Some(pipeline),
            appsink: Some(appsink),
            #[cfg(feature = "mpv-backend")]
            mpv: None,
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

    #[cfg(feature = "mpv-backend")]
    #[allow(clippy::too_many_arguments)]
    fn new_mpv_experimental(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        max_publish_fps: Option<u32>,
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
            creation_start,
        )?;
        let backend_kind = VideoBackendKind::MpvExperimental;
        metrics.record_video_backend_session(backend_kind);
        let player = Self {
            pipeline: None,
            appsink: None,
            mpv: Some(mpv),
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

    #[cfg(not(feature = "mpv-backend"))]
    #[allow(clippy::too_many_arguments)]
    fn new_mpv_experimental(
        _uri: &str,
        _source_id: Arc<String>,
        _session_id: u64,
        _volume: f64,
        _frame_mailbox: LatestFrameMailbox,
        _player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
        _metrics: Arc<PerformanceMetrics>,
        _max_publish_fps: Option<u32>,
        _creation_start: std::time::Instant,
    ) -> anyhow::Result<Self> {
        anyhow::bail!(
            "libmpv backend requested but kaleidux-daemon was built without the mpv-backend Cargo feature"
        )
    }

    pub fn is_appsink_backend(&self) -> bool {
        self.backend_kind == VideoBackendKind::Appsink
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn current_position_ns(&self) -> Option<u64> {
        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.current_position_ns();
        }
        self.pipeline
            .as_ref()?
            .query_position::<gst::ClockTime>()
            .map(gst::ClockTime::nseconds)
    }

    pub fn seek_to_position_ns(&self, position_ns: u64) -> anyhow::Result<()> {
        if position_ns == 0 {
            return Ok(());
        }

        #[cfg(feature = "mpv-backend")]
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
