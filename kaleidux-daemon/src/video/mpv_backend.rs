use libmpv2::{Format, Mpv, events};
use libmpv2_sys as sys;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{info, trace, warn};

use crate::metrics::PerformanceMetrics;
use crate::observability::video_backend::VideoBackendMetricKind;

use super::mpv_native::{
    MpvComposedRenderThreadConfig, MpvComposedVideoTarget, MpvNativeRenderThreadConfig,
    MpvNativeVideoTarget, run_composed_render_thread, run_native_render_thread,
};
use super::{
    LatestFrameMailbox, PlayerEvent, PlayerEventKind, VideoFrame, publish_interval_ns,
    should_publish_now,
};

#[path = "mpv_backend/config.rs"]
mod config;
#[path = "mpv_backend/software_render.rs"]
mod software_render;
use config::{
    apply_fast_gpu_options, capture_fps as mpv_capture_fps, hwdec_mode as mpv_hwdec_mode,
    normalized_render_bounds, render_api as mpv_render_api,
};
use software_render::{SoftwareRenderContext, capture_video_frame_with_context};

const MPV_FIRST_FRAME_TIMEOUT: Duration = Duration::from_millis(1500);
const MPV_FIRST_FRAME_POLL: Duration = Duration::from_millis(50);
const MPV_NOTHING_TO_PLAY_ERROR: i32 = sys::mpv_error_MPV_ERROR_NOTHING_TO_PLAY;

pub struct MpvPlayer {
    mpv: Arc<Mpv>,
    render_context: Option<SoftwareRenderContext>,
    native_target: Option<MpvNativeVideoTarget>,
    composed_target: Option<MpvComposedVideoTarget>,
    source_id: Arc<String>,
    session_id: u64,
    frame_mailbox: LatestFrameMailbox,
    player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
    metrics: Arc<PerformanceMetrics>,
    stop_requested: Arc<AtomicBool>,
    first_frame_logged: Arc<AtomicBool>,
    capture_interval: Duration,
    max_publish_fps: Option<u32>,
    render_size: Option<(u32, u32)>,
    event_thread: Option<JoinHandle<()>>,
    frame_thread: Option<JoinHandle<()>>,
    start_time: Instant,
}

impl MpvPlayer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_mailbox: LatestFrameMailbox,
        player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        max_publish_fps: Option<u32>,
        render_size: Option<(u32, u32)>,
        native_target: Option<MpvNativeVideoTarget>,
        composed_target: Option<MpvComposedVideoTarget>,
        start_time: Instant,
    ) -> anyhow::Result<Self> {
        let mpv = Mpv::with_initializer(|init| {
            init.set_option("vo", "libmpv")?;
            if volume > f64::EPSILON {
                init.set_option("audio", "yes")?;
            } else {
                init.set_option("audio", "no")?;
                init.set_option("ao", "null")?;
            }
            init.set_option("loop-file", "inf")?;
            init.set_option("keep-open", "yes")?;
            init.set_option("idle", true)?;
            init.set_option("pause", true)?;
            init.set_option("osd-level", 0i64)?;
            init.set_option("osc", false)?;
            init.set_option("terminal", false)?;
            init.set_option("msg-level", "all=warn")?;
            init.set_option("hwdec", mpv_hwdec_mode().as_str())?;
            apply_fast_gpu_options(&init);
            if let Err(error) = init.set_option("sws-fast", true) {
                warn!("[VIDEO] libmpv ignored sws-fast option: {}", error);
            }
            if let Err(error) = init.set_option("sws-scaler", "fast-bilinear") {
                warn!("[VIDEO] libmpv ignored sws-scaler option: {}", error);
            }
            if let Err(error) = init.set_option("sws-allow-zimg", false) {
                warn!("[VIDEO] libmpv ignored sws-allow-zimg option: {}", error);
            }
            if let Err(error) = init.set_option("sid", "no") {
                warn!("[VIDEO] libmpv ignored sid option: {}", error);
            }
            Ok(())
        })?;
        mpv.set_property("volume", (volume * 100.0).clamp(0.0, 100.0))?;
        mpv.disable_deprecated_events()?;
        mpv.enable_event(events::mpv_event_id::EndFile)?;
        mpv.enable_event(events::mpv_event_id::FileLoaded)?;
        mpv.enable_event(events::mpv_event_id::PlaybackRestart)?;
        mpv.enable_event(events::mpv_event_id::VideoReconfig)?;
        mpv.enable_event(events::mpv_event_id::Shutdown)?;
        mpv.observe_property("time-pos", Format::Double, 1)?;

        let render_api = mpv_render_api(native_target.as_ref(), composed_target.as_ref());
        let use_native_gl = render_api.is_native_gl();
        let use_composed_gl = render_api.is_composed_gl();
        let active_native_target = if use_native_gl { native_target } else { None };
        let active_composed_target = if use_composed_gl {
            composed_target
        } else {
            None
        };
        let render_context = if use_native_gl || use_composed_gl {
            None
        } else {
            Some(SoftwareRenderContext::new(&mpv)?)
        };
        let capture_fps = mpv_capture_fps(max_publish_fps);
        let normalized_render_size = normalized_render_bounds(render_size);
        info!(
            "[VIDEO] {}: VideoPlayer created with libmpv experimental backend (session={} render_api={} capture_fps={} max_publish_fps={:?} render_size={:?} native_target={:?} sw_format={} hwdec={} uri={})",
            source_id,
            session_id,
            if use_native_gl {
                "opengl-wayland-overlay-experimental"
            } else if use_composed_gl {
                "opengl-vulkan-shared-composed-experimental"
            } else {
                "software"
            },
            capture_fps,
            max_publish_fps,
            normalized_render_size,
            active_native_target,
            render_context
                .as_ref()
                .map(|context| context.format.to_string_lossy().into_owned())
                .unwrap_or_else(|| "none".to_string()),
            mpv_hwdec_mode(),
            uri
        );

        let player = Self {
            mpv: Arc::new(mpv),
            render_context,
            native_target: active_native_target,
            composed_target: active_composed_target,
            source_id,
            session_id,
            frame_mailbox,
            player_event_tx,
            metrics,
            stop_requested: Arc::new(AtomicBool::new(false)),
            first_frame_logged: Arc::new(AtomicBool::new(false)),
            capture_interval: Duration::from_nanos(1_000_000_000u64 / capture_fps as u64),
            max_publish_fps,
            render_size: normalized_render_size,
            event_thread: None,
            frame_thread: None,
            start_time,
        };
        player.load_file(uri)?;
        Ok(player)
    }

    pub fn prebuffer(
        &mut self,
        should_abort: impl Fn() -> bool,
    ) -> anyhow::Result<Option<VideoFrame>> {
        if should_abort() {
            anyhow::bail!("prebuffer aborted");
        }

        if self.renders_natively() || self.renders_composed_gl() {
            return Ok(None);
        }

        let wait_start = Instant::now();
        while wait_start.elapsed() < MPV_FIRST_FRAME_TIMEOUT {
            if should_abort() {
                anyhow::bail!("prebuffer aborted");
            }
            let Some(render_context) = self.render_context.as_ref() else {
                anyhow::bail!("mpv software render context is not available for prebuffer");
            };
            if let Some(frame) = capture_video_frame_with_context(
                &self.mpv,
                self.session_id,
                render_context,
                self.render_size,
                true,
            )? {
                self.log_first_frame("preroll");
                return Ok(Some(frame));
            }
            std::thread::sleep(MPV_FIRST_FRAME_POLL);
        }
        Ok(None)
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        self.stop_requested.store(false, Ordering::SeqCst);
        self.spawn_event_thread();
        if self.renders_natively() {
            self.spawn_native_render_thread();
        } else if self.renders_composed_gl() {
            self.spawn_composed_render_thread();
        } else {
            self.spawn_frame_thread();
        }
        self.mpv.set_property("pause", false)?;
        info!(
            "[VIDEO] {}: libmpv experimental backend started (session={})",
            self.source_id, self.session_id
        );
        Ok(())
    }

    pub fn renders_natively(&self) -> bool {
        self.native_target.is_some()
    }

    fn renders_composed_gl(&self) -> bool {
        self.composed_target.is_some()
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        self.stop_requested.store(true, Ordering::SeqCst);
        self.frame_mailbox.clear_source(self.source_id.as_ref());
        let _ = self.mpv.command("stop", &[]);
        let _ = self.mpv.command("quit", &[]);
        if let Some(handle) = self.frame_thread.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.event_thread.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    pub fn set_volume(&self, volume: f64) {
        let _ = self
            .mpv
            .set_property("volume", (volume * 100.0).clamp(0.0, 100.0));
    }

    pub fn pause(&self) -> anyhow::Result<()> {
        self.mpv.set_property("pause", true)?;
        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        self.mpv.set_property("pause", false)?;
        Ok(())
    }

    pub fn current_position_ns(&self) -> Option<u64> {
        self.mpv
            .get_property::<f64>("time-pos")
            .ok()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|seconds| (seconds * 1_000_000_000.0) as u64)
    }

    pub fn seek_to_position_ns(&self, position_ns: u64) -> anyhow::Result<()> {
        if position_ns == 0 {
            return Ok(());
        }
        let seconds = position_ns as f64 / 1_000_000_000.0;
        self.mpv
            .command("seek", &[&seconds.to_string(), "absolute+exact"])?;
        Ok(())
    }

    fn load_file(&self, uri: &str) -> anyhow::Result<()> {
        self.mpv.command("loadfile", &[uri, "replace"])?;
        Ok(())
    }

    fn spawn_event_thread(&mut self) {
        if self.event_thread.is_some() {
            return;
        }
        let event_handle = self.mpv.clone();
        let source_id = self.source_id.clone();
        let session_id = self.session_id;
        let player_event_tx = self.player_event_tx.clone();
        let stop_requested = self.stop_requested.clone();
        self.event_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-events-{}", source_id))
            .spawn(move || {
                while !stop_requested.load(Ordering::SeqCst) {
                    let Some(event) = event_handle.wait_event(0.25) else {
                        continue;
                    };
                    match event {
                        Ok(events::Event::EndFile(reason)) => {
                            trace!(
                                "[VIDEO] {}: libmpv EndFile event session={} reason={:?}",
                                source_id, session_id, reason
                            );
                        }
                        Ok(events::Event::PlaybackRestart) => {
                            trace!(
                                "[VIDEO] {}: libmpv PlaybackRestart session={}",
                                source_id, session_id
                            );
                        }
                        Ok(events::Event::VideoReconfig) => {
                            trace!(
                                "[VIDEO] {}: libmpv VideoReconfig session={}",
                                source_id, session_id
                            );
                        }
                        Ok(events::Event::Shutdown) => break,
                        Ok(_) => {}
                        Err(e) => {
                            if is_ignorable_event_error(&e) {
                                trace!(
                                    "[VIDEO] {}: ignoring libmpv non-fatal event error: {}",
                                    source_id, e
                                );
                                continue;
                            }
                            let reason = format!("libmpv event error: {}", e);
                            warn!("[VIDEO] {}: {}", source_id, reason);
                            let _ = player_event_tx.send(PlayerEvent {
                                source_id: source_id.to_string(),
                                session_id,
                                backend_kind: super::VideoBackendKind::MpvExperimental,
                                kind: PlayerEventKind::Error,
                                reason,
                            });
                        }
                    }
                }
            })
            .ok();
    }

    fn spawn_frame_thread(&mut self) {
        if self.frame_thread.is_some() {
            return;
        }
        let mpv = self.mpv.clone();
        let Some(render_context) = self.render_context.take() else {
            warn!(
                "[VIDEO] {}: libmpv frame thread not started; render context missing",
                self.source_id
            );
            return;
        };
        let source_id = self.source_id.clone();
        let session_id = self.session_id;
        let frame_mailbox = self.frame_mailbox.clone();
        let metrics = self.metrics.clone();
        let stop_requested = self.stop_requested.clone();
        let first_frame_logged = self.first_frame_logged.clone();
        let interval = self.capture_interval;
        let start_time = self.start_time;
        let publish_interval_ns = publish_interval_ns(self.max_publish_fps);
        let render_size = self.render_size;
        let last_publish_ns =
            Arc::new(std::sync::atomic::AtomicU64::new(super::NEVER_PUBLISHED_NS));
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-frames-{}", source_id))
            .spawn(move || {
                let render_context = render_context;
                while !stop_requested.load(Ordering::SeqCst) {
                    let frame_start = Instant::now();
                    let elapsed_ns = start_time.elapsed().as_nanos() as u64;
                    if should_publish_now(&last_publish_ns, publish_interval_ns, elapsed_ns) {
                        metrics
                            .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
                        match capture_video_frame_with_context(
                            &mpv,
                            session_id,
                            &render_context,
                            render_size,
                            false,
                        ) {
                            Ok(Some(frame)) => {
                                if !first_frame_logged.swap(true, Ordering::SeqCst) {
                                    info!(
                                        "[ASSET] {}: First libmpv frame captured in {:.3}ms",
                                        source_id,
                                        start_time.elapsed().as_secs_f64() * 1000.0
                                    );
                                }
                                frame_mailbox.publish_frame(source_id.as_ref(), frame);
                                metrics.record_video_backend_metric(
                                    VideoBackendMetricKind::MpvFramePublished,
                                );
                            }
                            Ok(None) => {}
                            Err(e) => {
                                metrics.record_video_backend_metric(
                                    VideoBackendMetricKind::MpvCaptureError,
                                );
                                trace!("[VIDEO] {}: libmpv capture skipped: {}", source_id, e);
                            }
                        }
                    }
                    std::thread::sleep(interval.saturating_sub(frame_start.elapsed()));
                }
            })
            .ok();
    }

    fn spawn_native_render_thread(&mut self) {
        if self.frame_thread.is_some() {
            return;
        }
        let Some(target) = self.native_target.clone() else {
            return;
        };
        let config = MpvNativeRenderThreadConfig {
            mpv: self.mpv.clone(),
            source_id: self.source_id.clone(),
            session_id: self.session_id,
            target,
            stop_requested: self.stop_requested.clone(),
            first_frame_logged: self.first_frame_logged.clone(),
            metrics: self.metrics.clone(),
            player_event_tx: self.player_event_tx.clone(),
            start_time: self.start_time,
            render_interval: self.capture_interval,
        };
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-gl-{}", self.source_id))
            .spawn(move || run_native_render_thread(config))
            .ok();
    }

    fn spawn_composed_render_thread(&mut self) {
        if self.frame_thread.is_some() {
            return;
        }
        let Some(target) = self.composed_target.clone() else {
            return;
        };
        let config = MpvComposedRenderThreadConfig {
            mpv: self.mpv.clone(),
            source_id: self.source_id.clone(),
            session_id: self.session_id,
            target,
            frame_mailbox: self.frame_mailbox.clone(),
            stop_requested: self.stop_requested.clone(),
            first_frame_logged: self.first_frame_logged.clone(),
            metrics: self.metrics.clone(),
            player_event_tx: self.player_event_tx.clone(),
            start_time: self.start_time,
            render_interval: self.capture_interval,
        };
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-gpu-{}", self.source_id))
            .spawn(move || run_composed_render_thread(config))
            .ok();
    }

    fn log_first_frame(&self, phase: &str) {
        if !self.first_frame_logged.swap(true, Ordering::SeqCst) {
            info!(
                "[ASSET] {}: First libmpv frame captured in {:.3}ms ({})",
                self.source_id,
                self.start_time.elapsed().as_secs_f64() * 1000.0,
                phase
            );
        }
    }
}

fn is_ignorable_event_error(error: &libmpv2::Error) -> bool {
    matches!(error, libmpv2::Error::Raw(code) if *code == MPV_NOTHING_TO_PLAY_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_to_play_event_error_is_non_fatal() {
        let error = libmpv2::Error::Raw(MPV_NOTHING_TO_PLAY_ERROR);

        assert!(is_ignorable_event_error(&error));
    }
}
