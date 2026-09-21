use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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
#[path = "mpv_native/offscreen_gl_support.rs"]
mod offscreen_gl_support;
#[path = "mpv_native/render_wake.rs"]
pub(super) mod render_wake;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MpvRenderApiRequest {
    ComposedSoftware,
    ComposedGl,
    NativeGlOverlayDiagnostic,
    DeprecatedNativeGlAlias,
    Unknown,
}

impl MpvRenderApiRequest {
    pub(crate) fn from_env() -> Self {
        Self::parse(std::env::var("KLD_MPV_RENDER_API").ok().as_deref())
    }

    fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None => Self::ComposedGl,
            Some("" | "sw" | "software" | "cpu" | "composed") => Self::ComposedSoftware,
            Some("gl-composed" | "opengl-composed" | "gpu-composed") => Self::ComposedGl,
            Some(
                "overlay" | "gl-overlay" | "opengl-overlay" | "native-overlay" | "wayland-overlay",
            ) => Self::NativeGlOverlayDiagnostic,
            Some("gl" | "opengl" | "native" | "wayland") => Self::DeprecatedNativeGlAlias,
            Some(_) => Self::Unknown,
        }
    }

    pub(crate) fn enables_native_overlay(self) -> bool {
        self == Self::NativeGlOverlayDiagnostic
    }

    pub(crate) fn enables_composed_gl(self) -> bool {
        self == Self::ComposedGl
    }
}

#[derive(Clone, Debug)]
pub struct MpvNativeVideoTarget {
    display_ptr: usize,
    surface_id: ObjectId,
    _surface: smithay_client_toolkit::shell::wlr_layer::LayerSurface,
    width: u32,
    height: u32,
}

#[derive(Clone)]
pub struct MpvComposedVideoTarget {
    display_ptr: usize,
    display_platform: MpvComposedDisplayPlatform,
    wgpu_ctx: Arc<crate::renderer::WgpuContext>,
    width: u32,
    height: u32,
}

#[derive(Clone, Copy, Debug)]
enum MpvComposedDisplayPlatform {
    Wayland,
    #[cfg(feature = "display-x11")]
    Xcb,
}

impl std::fmt::Debug for MpvComposedVideoTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MpvComposedVideoTarget")
            .field("display_ptr", &self.display_ptr)
            .field("display_platform", &self.display_platform)
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
            display_platform: MpvComposedDisplayPlatform::Wayland,
            wgpu_ctx,
            width: width.max(1),
            height: height.max(1),
        })
    }

    #[cfg(feature = "display-x11")]
    pub(crate) fn new_xcb(
        connection_ptr: *mut c_void,
        wgpu_ctx: Arc<crate::renderer::WgpuContext>,
        width: u32,
        height: u32,
    ) -> Option<Self> {
        if connection_ptr.is_null() {
            return None;
        }
        Some(Self {
            display_ptr: connection_ptr as usize,
            display_platform: MpvComposedDisplayPlatform::Xcb,
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

    pub(super) fn adapter_vendor(&self) -> u32 {
        self.wgpu_ctx.adapter.get_info().vendor
    }

    pub(super) fn egl_platform(&self) -> u32 {
        match self.display_platform {
            MpvComposedDisplayPlatform::Wayland => 0x31D8, // EGL_PLATFORM_WAYLAND_KHR
            #[cfg(feature = "display-x11")]
            MpvComposedDisplayPlatform::Xcb => 0x31DC, // EGL_PLATFORM_XCB_EXT
        }
    }

    pub(super) fn mpv_native_display_param(&self) -> Option<(u32, *mut c_void)> {
        match self.display_platform {
            MpvComposedDisplayPlatform::Wayland => Some((
                sys::mpv_render_param_type_MPV_RENDER_PARAM_WL_DISPLAY,
                self.display_ptr(),
            )),
            // MPV_RENDER_PARAM_X11_DISPLAY requires an Xlib Display*, not an
            // xcb_connection_t*. Hardware probing uses DRM_DISPLAY_V2 when a
            // render node is available; software decoding needs no X display.
            #[cfg(feature = "display-x11")]
            MpvComposedDisplayPlatform::Xcb => None,
        }
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
        surface: smithay_client_toolkit::shell::wlr_layer::LayerSurface,
        width: u32,
        height: u32,
    ) -> Option<Self> {
        if display_ptr.is_null() {
            return None;
        }
        Some(Self {
            display_ptr: display_ptr as usize,
            surface_id: {
                use smithay_client_toolkit::shell::WaylandSurface;
                use wayland_client::Proxy;
                surface.wl_surface().id()
            },
            _surface: surface,
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

/// Handshake that guarantees mpv's render context exists before `loadfile`.
///
/// mpv initializes its `libmpv` VO while loading a file. If no render context
/// is set at that moment it logs `No render context set`, fails VO init, and
/// **permanently deselects the video track for that file** (`Video: no video`)
/// — playback then produces no `MPV_RENDER_UPDATE_FRAME` at all. The render
/// context can only be created on the render thread (its EGL context is made
/// current there), so the player thread has to wait for this signal before
/// loading.
pub(crate) struct RenderReadySignal {
    /// `None` until the render thread reports; then `Ok` or the failure text.
    state: Mutex<Option<Result<(), String>>>,
    condvar: Condvar,
}

impl RenderReadySignal {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(None),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn signal(&self, result: Result<(), String>) {
        if let Ok(mut state) = self.state.lock() {
            if state.is_none() {
                *state = Some(result);
            }
            self.condvar.notify_all();
        }
    }

    /// Blocks until the render thread reports readiness, the render context
    /// fails, or `timeout` elapses.
    pub(crate) fn wait(&self, timeout: Duration) -> anyhow::Result<()> {
        let Ok(state) = self.state.lock() else {
            anyhow::bail!("mpv render-ready signal poisoned");
        };
        let (state, wait_result) = self
            .condvar
            .wait_timeout_while(state, timeout, |state| state.is_none())
            .map_err(|_| anyhow::anyhow!("mpv render-ready signal poisoned"))?;
        if wait_result.timed_out() {
            anyhow::bail!("mpv render context was not ready within {timeout:?}");
        }
        match state.as_ref() {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => anyhow::bail!("mpv render context failed: {error}"),
            None => anyhow::bail!("mpv render-ready signal woke without a result"),
        }
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
    pub(crate) player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
    pub(crate) start_time: Instant,
    pub(crate) render_ready: Arc<RenderReadySignal>,
    pub(crate) stop_wake: Arc<render_wake::RenderWake>,
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
    pub(crate) player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
    pub(crate) start_time: Instant,
    pub(crate) min_render_interval: Option<Duration>,
    pub(crate) render_ready: Arc<RenderReadySignal>,
    pub(crate) stop_wake: Arc<render_wake::RenderWake>,
}

pub(crate) fn run_composed_render_thread(config: MpvComposedRenderThreadConfig) {
    let mut renderer = match ComposedGlRenderContext::new(&config.mpv, &config.target) {
        Ok(renderer) => renderer,
        Err(error) => {
            // Unblock the player thread before reporting: it is waiting on this
            // signal and must fail fast rather than sit out the whole timeout.
            config.render_ready.signal(Err(error.to_string()));
            report_gl_renderer_failure(&config, &error);
            return;
        }
    };
    // The render context now exists, so mpv's VO can initialize. Only after
    // this is it safe for the player thread to issue `loadfile`.
    config.render_ready.signal(Ok(()));
    info!(
        "[VIDEO] {}: composed libmpv GL render thread started (session={} size={}x{})",
        config.source_id, config.session_id, config.target.width, config.target.height
    );
    let mut last_published = None::<Instant>;
    while !config.stop_requested.load(Ordering::SeqCst) {
        if !renderer.wait_for_update_or_stop(&config.stop_wake) {
            break;
        }
        config
            .metrics
            .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
        let publish = config
            .min_render_interval
            .is_none_or(|interval| last_published.is_none_or(|last| last.elapsed() >= interval));
        match renderer.render_frame(config.session_id, publish) {
            Ok(Some(frame)) => {
                last_published = Some(Instant::now());
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
                let _ = config.player_event_tx.blocking_send(PlayerEvent {
                    source_id: config.source_id.to_string(),
                    session_id: config.session_id,
                    backend_kind: VideoBackendKind::Mpv,
                    kind: PlayerEventKind::Error,
                    reason: format!("composed libmpv GL render failed: {error}"),
                });
                break;
            }
        }
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
    if !super::mpv_backend_is_explicitly_forced() {
        warn!("[VIDEO] Switching automatic backend selection to appsink after mpv GL failure");
        super::set_video_backend_request(super::VideoBackendRequest::ForceAppsink);
    }
    let _ = config.player_event_tx.blocking_send(PlayerEvent {
        source_id: config.source_id.to_string(),
        session_id: config.session_id,
        backend_kind: VideoBackendKind::Mpv,
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
        render_ready,
        stop_wake,
    } = config;

    let mut renderer = match NativeGlRenderContext::new(&mpv, &target) {
        Ok(renderer) => renderer,
        Err(error) => {
            // Unblock the waiting player thread before reporting the failure.
            render_ready.signal(Err(error.to_string()));
            warn!(
                "[VIDEO] {}: native libmpv GL renderer failed to initialize: {}",
                source_id, error
            );
            metrics.record_video_backend_metric(VideoBackendMetricKind::MpvCaptureError);
            if !super::mpv_backend_is_explicitly_forced() {
                warn!(
                    "[VIDEO] Switching automatic backend selection to appsink after native mpv GL failure"
                );
                super::set_video_backend_request(super::VideoBackendRequest::ForceAppsink);
            }
            let _ = player_event_tx.blocking_send(PlayerEvent {
                source_id: source_id.to_string(),
                session_id,
                backend_kind: VideoBackendKind::Mpv,
                kind: PlayerEventKind::Error,
                reason: format!("native libmpv GL renderer failed to initialize: {error}"),
            });
            return;
        }
    };
    // mpv's VO can initialize from here on; `loadfile` is now safe to issue.
    render_ready.signal(Ok(()));

    info!(
        "[VIDEO] {}: native libmpv GL render thread started (session={} size={}x{})",
        source_id, session_id, target.width, target.height
    );

    while !stop_requested.load(Ordering::SeqCst) {
        if !renderer.wait_for_update_or_stop(&stop_wake) {
            break;
        }
        if stop_requested.load(Ordering::SeqCst) {
            break;
        }

        metrics.record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
        match renderer.render(false) {
            Ok(true) => {
                if !first_frame_logged.swap(true, Ordering::SeqCst) {
                    info!(
                        "[ASSET] {}: First native libmpv GL frame presented in {:.3}ms",
                        source_id,
                        start_time.elapsed().as_secs_f64() * 1000.0
                    );
                    let _ = player_event_tx.blocking_send(PlayerEvent {
                        source_id: source_id.to_string(),
                        session_id,
                        backend_kind: VideoBackendKind::Mpv,
                        kind: PlayerEventKind::FirstPresent,
                        reason: "native surface presented its first frame".to_string(),
                    });
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
    }

    debug!(
        "[VIDEO] {}: native libmpv GL render thread stopped (session={})",
        source_id, session_id
    );
}

#[cfg(test)]
mod tests {
    use super::{MpvRenderApiRequest, RenderReadySignal};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn native_overlay_requires_explicit_overlay_name() {
        assert_eq!(
            MpvRenderApiRequest::parse(Some("gl-overlay")),
            MpvRenderApiRequest::NativeGlOverlayDiagnostic
        );
        assert!(MpvRenderApiRequest::parse(Some("overlay")).enables_native_overlay());
    }

    #[test]
    fn default_and_software_names_keep_wgpu_composition() {
        assert_eq!(
            MpvRenderApiRequest::parse(None),
            MpvRenderApiRequest::ComposedGl
        );
        assert_eq!(
            MpvRenderApiRequest::parse(Some("software")),
            MpvRenderApiRequest::ComposedSoftware
        );
    }

    #[test]
    fn composed_gl_production_name_is_supported() {
        assert_eq!(
            MpvRenderApiRequest::parse(Some("gl-composed")),
            MpvRenderApiRequest::ComposedGl
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

    // Regression cover for the 2026-07-28 defect: `loadfile` issued before the
    // render context exists makes mpv fail VO init and drop the video track,
    // which looks like a CPU win because nothing decodes.
    #[test]
    fn render_ready_reports_success_to_a_waiter() {
        let signal = Arc::new(RenderReadySignal::new());
        let render_thread = {
            let signal = signal.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                signal.signal(Ok(()));
            })
        };
        assert!(signal.wait(Duration::from_secs(5)).is_ok());
        render_thread.join().unwrap();
    }

    #[test]
    fn render_ready_propagates_render_context_failure() {
        let signal = RenderReadySignal::new();
        signal.signal(Err("EGL context creation failed".to_string()));
        let error = signal.wait(Duration::from_secs(5)).unwrap_err().to_string();
        assert!(error.contains("EGL context creation failed"), "{error}");
    }

    #[test]
    fn render_ready_times_out_instead_of_blocking_forever() {
        let signal = RenderReadySignal::new();
        let error = signal
            .wait(Duration::from_millis(50))
            .unwrap_err()
            .to_string();
        assert!(error.contains("not ready"), "{error}");
    }

    #[test]
    fn render_ready_keeps_the_first_result_and_returns_it_after_the_fact() {
        // Signalling before anyone waits must still be observable, and a later
        // signal must not overwrite the original outcome.
        let signal = RenderReadySignal::new();
        signal.signal(Ok(()));
        signal.signal(Err("late failure".to_string()));
        assert!(signal.wait(Duration::from_millis(50)).is_ok());
    }
}
