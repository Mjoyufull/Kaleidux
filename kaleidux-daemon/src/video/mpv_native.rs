use libmpv2::Mpv;
use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};
use wayland_client::backend::ObjectId;

use crate::metrics::PerformanceMetrics;
use crate::observability::video_backend::VideoBackendMetricKind;

use super::{PlayerEvent, PlayerEventKind, VideoBackendKind};

#[path = "mpv_native/egl_render.rs"]
mod egl_render;
use egl_render::NativeGlRenderContext;
#[path = "mpv_native/offscreen_gl.rs"]
mod offscreen_gl;
use offscreen_gl::ComposedGlRenderContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MpvRenderApiRequest {
    ComposedSoftware,
    ComposedGlExperimental,
    NativeGlOverlayExperimental,
    DeprecatedNativeGlAlias,
    Unknown,
}

impl MpvRenderApiRequest {
    pub(crate) fn from_env() -> Self {
        Self::parse(std::env::var("KLD_MPV_RENDER_API").ok().as_deref())
    }

    fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None => Self::ComposedGlExperimental,
            Some("" | "sw" | "software" | "cpu" | "composed") => Self::ComposedSoftware,
            Some("gl-composed" | "opengl-composed" | "gpu-composed") => {
                Self::ComposedGlExperimental
            }
            Some(
                "overlay" | "gl-overlay" | "opengl-overlay" | "native-overlay" | "wayland-overlay",
            ) => Self::NativeGlOverlayExperimental,
            Some("gl" | "opengl" | "native" | "wayland") => Self::DeprecatedNativeGlAlias,
            Some(_) => Self::Unknown,
        }
    }

    pub(crate) fn enables_native_overlay(self) -> bool {
        self == Self::NativeGlOverlayExperimental
    }

    pub(crate) fn enables_composed_gl(self) -> bool {
        self == Self::ComposedGlExperimental
    }
}

#[derive(Clone, Debug)]
pub struct MpvNativeVideoTarget {
    display_ptr: usize,
    surface_id: ObjectId,
    width: u32,
    height: u32,
}

#[derive(Clone)]
pub struct MpvComposedVideoTarget {
    display_ptr: usize,
    wgpu_ctx: Arc<crate::renderer::WgpuContext>,
    width: u32,
    height: u32,
}

impl std::fmt::Debug for MpvComposedVideoTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MpvComposedVideoTarget")
            .field("display_ptr", &self.display_ptr)
            .field("width", &self.width)
            .field("height", &self.height)
            .finish_non_exhaustive()
    }
}

impl MpvComposedVideoTarget {
    pub(crate) fn new(
        display_ptr: *mut c_void,
        wgpu_ctx: Arc<crate::renderer::WgpuContext>,
        width: u32,
        height: u32,
    ) -> Option<Self> {
        if display_ptr.is_null() {
            return None;
        }
        Some(Self {
            display_ptr: display_ptr as usize,
            wgpu_ctx,
            width: width.max(1),
            height: height.max(1),
        })
    }

    pub(super) fn display_ptr(&self) -> *mut c_void {
        self.display_ptr as *mut c_void
    }

    pub(super) fn size(&self) -> (i32, i32) {
        (self.width as i32, self.height as i32)
    }
}

// SAFETY: The target only contains an ObjectId handle plus a Wayland display
// pointer owned by the main Wayland connection. The layer surface itself is kept
// alive by WaylandBackend while player threads use this target.
unsafe impl Send for MpvNativeVideoTarget {}
// SAFETY: All fields are immutable after construction and are used only to
// create EGL objects on the render thread.
unsafe impl Sync for MpvNativeVideoTarget {}

impl MpvNativeVideoTarget {
    pub(crate) fn new(
        display_ptr: *mut c_void,
        surface_id: ObjectId,
        width: u32,
        height: u32,
    ) -> Option<Self> {
        if display_ptr.is_null() {
            return None;
        }
        Some(Self {
            display_ptr: display_ptr as usize,
            surface_id,
            width: width.max(1),
            height: height.max(1),
        })
    }

    pub(super) fn display_ptr(&self) -> *mut c_void {
        self.display_ptr as *mut c_void
    }

    pub(super) fn size(&self) -> (i32, i32) {
        (self.width.max(1) as i32, self.height.max(1) as i32)
    }
}

pub(crate) struct MpvNativeRenderThreadConfig {
    pub(crate) mpv: Arc<Mpv>,
    pub(crate) source_id: Arc<String>,
    pub(crate) session_id: u64,
    pub(crate) target: MpvNativeVideoTarget,
    pub(crate) stop_requested: Arc<AtomicBool>,
    pub(crate) first_frame_logged: Arc<AtomicBool>,
    pub(crate) metrics: Arc<PerformanceMetrics>,
    pub(crate) player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
    pub(crate) start_time: Instant,
    pub(crate) render_interval: Duration,
}

pub(crate) struct MpvComposedRenderThreadConfig {
    pub(crate) mpv: Arc<Mpv>,
    pub(crate) source_id: Arc<String>,
    pub(crate) session_id: u64,
    pub(crate) target: MpvComposedVideoTarget,
    pub(crate) frame_mailbox: super::LatestFrameMailbox,
    pub(crate) stop_requested: Arc<AtomicBool>,
    pub(crate) first_frame_logged: Arc<AtomicBool>,
    pub(crate) metrics: Arc<PerformanceMetrics>,
    pub(crate) player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
    pub(crate) start_time: Instant,
    pub(crate) render_interval: Duration,
}

pub(crate) fn run_composed_render_thread(config: MpvComposedRenderThreadConfig) {
    let mut renderer = match ComposedGlRenderContext::new(&config.mpv, &config.target) {
        Ok(renderer) => renderer,
        Err(error) => {
            report_gl_renderer_failure(&config, &error);
            return;
        }
    };
    info!(
        "[VIDEO] {}: composed libmpv GL render thread started (session={} size={}x{})",
        config.source_id, config.session_id, config.target.width, config.target.height
    );
    while !config.stop_requested.load(Ordering::SeqCst) {
        let frame_start = Instant::now();
        config
            .metrics
            .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
        match renderer.render_frame(config.session_id) {
            Ok(Some(frame)) => {
                if !config.first_frame_logged.swap(true, Ordering::SeqCst) {
                    info!(
                        "[ASSET] {}: First composed libmpv GL frame published in {:.3}ms",
                        config.source_id,
                        config.start_time.elapsed().as_secs_f64() * 1000.0
                    );
                }
                config.frame_mailbox.publish_frame(&config.source_id, frame);
                config
                    .metrics
                    .record_video_backend_metric(VideoBackendMetricKind::MpvFramePublished);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    "[VIDEO] {}: composed libmpv GL render failed: {error}",
                    config.source_id
                );
                config
                    .metrics
                    .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureError);
                break;
            }
        }
        std::thread::sleep(config.render_interval.saturating_sub(frame_start.elapsed()));
    }
    debug!(
        "[VIDEO] {}: composed libmpv GL render thread stopped (session={})",
        config.source_id, config.session_id
    );
}

fn report_gl_renderer_failure(config: &MpvComposedRenderThreadConfig, error: &anyhow::Error) {
    warn!(
        "[VIDEO] {}: composed libmpv GL renderer failed to initialize: {}",
        config.source_id, error
    );
    config
        .metrics
        .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureError);
    let _ = config.player_event_tx.send(PlayerEvent {
        source_id: config.source_id.to_string(),
        session_id: config.session_id,
        backend_kind: VideoBackendKind::MpvExperimental,
        kind: PlayerEventKind::Error,
        reason: format!("composed libmpv GL renderer failed to initialize: {error}"),
    });
}

pub(crate) fn run_native_render_thread(config: MpvNativeRenderThreadConfig) {
    let MpvNativeRenderThreadConfig {
        mpv,
        source_id,
        session_id,
        target,
        stop_requested,
        first_frame_logged,
        metrics,
        player_event_tx,
        start_time,
        render_interval,
    } = config;

    let mut renderer = match NativeGlRenderContext::new(&mpv, &target) {
        Ok(renderer) => renderer,
        Err(error) => {
            warn!(
                "[VIDEO] {}: native libmpv GL renderer failed to initialize: {}",
                source_id, error
            );
            metrics.record_video_backend_metric(VideoBackendMetricKind::MpvCaptureError);
            let _ = player_event_tx.send(PlayerEvent {
                source_id: source_id.to_string(),
                session_id,
                backend_kind: VideoBackendKind::MpvExperimental,
                kind: PlayerEventKind::Error,
                reason: format!("native libmpv GL renderer failed to initialize: {error}"),
            });
            return;
        }
    };

    info!(
        "[VIDEO] {}: native libmpv GL render thread started (session={} size={}x{})",
        source_id, session_id, target.width, target.height
    );

    while !stop_requested.load(Ordering::SeqCst) {
        let frame_start = Instant::now();
        renderer.drain_pending_updates();
        if stop_requested.load(Ordering::SeqCst) {
            break;
        }

        metrics.record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
        match renderer.render(true) {
            Ok(true) => {
                if !first_frame_logged.swap(true, Ordering::SeqCst) {
                    info!(
                        "[ASSET] {}: First native libmpv GL frame presented in {:.3}ms",
                        source_id,
                        start_time.elapsed().as_secs_f64() * 1000.0
                    );
                }
                metrics.record_video_backend_metric(VideoBackendMetricKind::MpvFramePublished);
            }
            Ok(false) => {}
            Err(error) => {
                metrics.record_video_backend_metric(VideoBackendMetricKind::MpvCaptureError);
                trace!(
                    "[VIDEO] {}: native libmpv render skipped: {}",
                    source_id, error
                );
                std::thread::sleep(Duration::from_millis(16));
            }
        }
        std::thread::sleep(render_interval.saturating_sub(frame_start.elapsed()));
    }

    debug!(
        "[VIDEO] {}: native libmpv GL render thread stopped (session={})",
        source_id, session_id
    );
}

#[cfg(test)]
mod tests {
    use super::MpvRenderApiRequest;

    #[test]
    fn native_overlay_requires_explicit_overlay_name() {
        assert_eq!(
            MpvRenderApiRequest::parse(Some("gl-overlay")),
            MpvRenderApiRequest::NativeGlOverlayExperimental
        );
        assert!(MpvRenderApiRequest::parse(Some("overlay")).enables_native_overlay());
    }

    #[test]
    fn default_and_software_names_keep_wgpu_composition() {
        assert_eq!(
            MpvRenderApiRequest::parse(None),
            MpvRenderApiRequest::ComposedGlExperimental
        );
        assert_eq!(
            MpvRenderApiRequest::parse(Some("software")),
            MpvRenderApiRequest::ComposedSoftware
        );
    }

    #[test]
    fn composed_gl_requires_explicit_name_during_bringup() {
        assert_eq!(
            MpvRenderApiRequest::parse(Some("gl-composed")),
            MpvRenderApiRequest::ComposedGlExperimental
        );
    }

    #[test]
    fn old_native_names_do_not_silently_enable_overlay_surfaces() {
        assert_eq!(
            MpvRenderApiRequest::parse(Some("gl")),
            MpvRenderApiRequest::DeprecatedNativeGlAlias
        );
        assert!(!MpvRenderApiRequest::parse(Some("gl")).enables_native_overlay());
    }
}
