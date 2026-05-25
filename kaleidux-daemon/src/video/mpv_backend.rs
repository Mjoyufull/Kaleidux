use gstreamer as gst;
use libmpv2::{Format, Mpv, events};
use libmpv2_sys as sys;
use std::ffi::CString;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{info, trace, warn};

use crate::metrics::PerformanceMetrics;
use crate::observability::video_backend::VideoBackendMetricKind;

use super::{
    LatestFrameMailbox, PlayerEvent, PlayerEventKind, VideoFrame, VideoFrameFormat,
    publish_interval_ns, should_publish_now,
};

const DEFAULT_MPV_CAPTURE_FPS: u32 = 48;
const MAX_MPV_CAPTURE_FPS: u32 = 120;
const MPV_FIRST_FRAME_TIMEOUT: Duration = Duration::from_millis(1500);
const MPV_FIRST_FRAME_POLL: Duration = Duration::from_millis(50);
const SOFTWARE_RENDER_FORMAT: &str = "rgb0";
const RENDER_WITHOUT_TARGET_BLOCK: i32 = 0;

pub struct MpvPlayer {
    mpv: Arc<Mpv>,
    event_client: Option<Mpv>,
    source_id: Arc<String>,
    session_id: u64,
    frame_mailbox: LatestFrameMailbox,
    player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEvent>,
    metrics: Arc<PerformanceMetrics>,
    stop_requested: Arc<AtomicBool>,
    first_frame_logged: Arc<AtomicBool>,
    capture_interval: Duration,
    max_publish_fps: Option<u32>,
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
        start_time: Instant,
    ) -> anyhow::Result<Self> {
        let mpv = Mpv::with_initializer(|init| {
            init.set_option("vo", "null")?;
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
            init.set_option("hwdec", "auto-safe")?;
            Ok(())
        })?;
        mpv.set_property("volume", (volume * 100.0).clamp(0.0, 100.0))?;
        let event_client = mpv.create_client(Some("kaleidux_mpv_events"))?;
        event_client.disable_deprecated_events()?;
        event_client.enable_event(events::mpv_event_id::EndFile)?;
        event_client.enable_event(events::mpv_event_id::FileLoaded)?;
        event_client.enable_event(events::mpv_event_id::PlaybackRestart)?;
        event_client.enable_event(events::mpv_event_id::VideoReconfig)?;
        event_client.enable_event(events::mpv_event_id::Shutdown)?;
        event_client.observe_property("time-pos", Format::Double, 1)?;

        let capture_fps = mpv_capture_fps(max_publish_fps);
        info!(
            "[VIDEO] {}: VideoPlayer created with libmpv experimental backend (session={} capture_fps={} max_publish_fps={:?} uri={})",
            source_id, session_id, capture_fps, max_publish_fps, uri
        );

        let player = Self {
            mpv: Arc::new(mpv),
            event_client: Some(event_client),
            source_id,
            session_id,
            frame_mailbox,
            player_event_tx,
            metrics,
            stop_requested: Arc::new(AtomicBool::new(false)),
            first_frame_logged: Arc::new(AtomicBool::new(false)),
            capture_interval: Duration::from_nanos(1_000_000_000u64 / capture_fps as u64),
            max_publish_fps,
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

        let wait_start = Instant::now();
        let mut render_context = None;
        while wait_start.elapsed() < MPV_FIRST_FRAME_TIMEOUT {
            if should_abort() {
                anyhow::bail!("prebuffer aborted");
            }
            if let Some(frame) =
                capture_video_frame_with_context(&self.mpv, self.session_id, &mut render_context)?
            {
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
        self.spawn_frame_thread();
        self.mpv.set_property("pause", false)?;
        info!(
            "[VIDEO] {}: libmpv experimental backend started (session={})",
            self.source_id, self.session_id
        );
        Ok(())
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
        let Some(event_client) = self.event_client.take() else {
            return;
        };
        let source_id = self.source_id.clone();
        let session_id = self.session_id;
        let player_event_tx = self.player_event_tx.clone();
        let stop_requested = self.stop_requested.clone();
        self.event_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-events-{}", source_id))
            .spawn(move || {
                while !stop_requested.load(Ordering::SeqCst) {
                    let Some(event) = event_client.wait_event(0.25) else {
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
        let source_id = self.source_id.clone();
        let session_id = self.session_id;
        let frame_mailbox = self.frame_mailbox.clone();
        let metrics = self.metrics.clone();
        let stop_requested = self.stop_requested.clone();
        let first_frame_logged = self.first_frame_logged.clone();
        let interval = self.capture_interval;
        let start_time = self.start_time;
        let publish_interval_ns = publish_interval_ns(self.max_publish_fps);
        let last_publish_ns =
            Arc::new(std::sync::atomic::AtomicU64::new(super::NEVER_PUBLISHED_NS));
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-frames-{}", source_id))
            .spawn(move || {
                let mut render_context = None;
                while !stop_requested.load(Ordering::SeqCst) {
                    let frame_start = Instant::now();
                    let elapsed_ns = start_time.elapsed().as_nanos() as u64;
                    if should_publish_now(&last_publish_ns, publish_interval_ns, elapsed_ns) {
                        metrics
                            .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
                        match capture_video_frame_with_context(
                            &mpv,
                            session_id,
                            &mut render_context,
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

fn mpv_capture_fps(max_publish_fps: Option<u32>) -> u32 {
    std::env::var("KLD_MPV_CAPTURE_FPS")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|fps| *fps > 0)
        .or(max_publish_fps)
        .unwrap_or(DEFAULT_MPV_CAPTURE_FPS)
        .clamp(1, MAX_MPV_CAPTURE_FPS)
}

fn capture_video_frame_with_context(
    mpv: &Mpv,
    session_id: u64,
    render_context: &mut Option<SoftwareRenderContext>,
) -> anyhow::Result<Option<VideoFrame>> {
    let Some((width, height)) = video_dimensions(mpv) else {
        return Ok(None);
    };
    if render_context.is_none() {
        *render_context = Some(SoftwareRenderContext::new(mpv)?);
    }
    let render_context = render_context
        .as_ref()
        .expect("mpv software render context was initialized");
    render_video_frame(mpv, session_id, render_context, width, height)
}

fn render_video_frame(
    mpv: &Mpv,
    session_id: u64,
    render_context: &SoftwareRenderContext,
    width: u32,
    height: u32,
) -> anyhow::Result<Option<VideoFrame>> {
    let SoftwareRenderedFrame { data, stride } = render_context.render(width, height)?;
    let buffer = gst::Buffer::from_mut_slice(data);
    Ok(Some(VideoFrame {
        buffer,
        width,
        height,
        stride,
        format: VideoFrameFormat::Rgba,
        session_id,
        pts_ns: current_position_ns(mpv),
        duration_ns: None,
    }))
}

fn video_dimensions(mpv: &Mpv) -> Option<(u32, u32)> {
    let width = mpv
        .get_property::<i64>("dwidth")
        .or_else(|_| mpv.get_property::<i64>("width"))
        .ok()?;
    let height = mpv
        .get_property::<i64>("dheight")
        .or_else(|_| mpv.get_property::<i64>("height"))
        .ok()?;

    let width = u32::try_from(width).ok()?;
    let height = u32::try_from(height).ok()?;
    (width > 0 && height > 0).then_some((width, height))
}

fn current_position_ns(mpv: &Mpv) -> Option<u64> {
    mpv.get_property::<f64>("time-pos")
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|seconds| (seconds * 1_000_000_000.0) as u64)
}

struct SoftwareRenderedFrame {
    stride: u32,
    data: Vec<u8>,
}

struct SoftwareRenderContext {
    context: *mut sys::mpv_render_context,
}

impl SoftwareRenderContext {
    fn new(mpv: &Mpv) -> anyhow::Result<Self> {
        let api_type = CString::new("sw")?;
        let mut params = [
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_API_TYPE,
                data: api_type.as_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];
        let mut context = std::ptr::null_mut();
        // Safety: libmpv only reads the parameter array during this call, and
        // the API string plus terminator remain live until the call returns.
        let result = unsafe {
            sys::mpv_render_context_create(&mut context, mpv.ctx.as_ptr(), params.as_mut_ptr())
        };
        if result < 0 {
            anyhow::bail!("mpv software render context creation failed: {}", result);
        }
        Ok(Self { context })
    }

    fn render(&self, width: u32, height: u32) -> anyhow::Result<SoftwareRenderedFrame> {
        let stride = align_to(width.saturating_mul(4), 64);
        let mut data = vec![0u8; stride as usize * height as usize];
        let size = [width as i32, height as i32];
        let format = CString::new(SOFTWARE_RENDER_FORMAT)?;
        let mut stride_param = stride as usize;
        let mut block_for_target_time = RENDER_WITHOUT_TARGET_BLOCK;
        let mut params = [
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_SIZE,
                data: size.as_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_FORMAT,
                data: format.as_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_STRIDE,
                data: &mut stride_param as *mut _ as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_POINTER,
                data: data.as_mut_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_BLOCK_FOR_TARGET_TIME,
                data: &mut block_for_target_time as *mut _ as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];

        // Safety: `self.context` is owned by this wrapper and all render
        // parameters point at buffers that remain valid for the call duration.
        let _ = unsafe { sys::mpv_render_context_update(self.context) };
        let result = unsafe { sys::mpv_render_context_render(self.context, params.as_mut_ptr()) };
        if result < 0 {
            anyhow::bail!("mpv software render failed: {}", result);
        }
        fill_alpha_for_rgb0(&mut data, width, height, stride);
        Ok(SoftwareRenderedFrame { stride, data })
    }
}

impl Drop for SoftwareRenderContext {
    fn drop(&mut self) {
        // Safety: the context pointer was returned by libmpv, is owned by this
        // wrapper, and is freed exactly once here.
        unsafe {
            sys::mpv_render_context_free(self.context);
        }
    }
}

fn align_to(value: u32, alignment: u32) -> u32 {
    debug_assert!(alignment.is_power_of_two());
    (value + alignment - 1) & !(alignment - 1)
}

fn fill_alpha_for_rgb0(data: &mut [u8], width: u32, height: u32, stride: u32) {
    let row_pixels = width as usize;
    let stride = stride as usize;
    for row in 0..height as usize {
        let row_start = row * stride;
        for pixel in 0..row_pixels {
            let alpha_index = row_start + pixel * 4 + 3;
            if let Some(alpha) = data.get_mut(alpha_index) {
                *alpha = 255;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_to_rounds_to_power_of_two_boundary() {
        assert_eq!(align_to(0, 64), 0);
        assert_eq!(align_to(4, 64), 64);
        assert_eq!(align_to(64, 64), 64);
        assert_eq!(align_to(65, 64), 128);
    }

    #[test]
    fn fill_alpha_for_rgb0_sets_pixels_without_touching_padding() {
        let width = 2;
        let height = 2;
        let stride = 12;
        let mut data = vec![7u8; stride as usize * height as usize];

        fill_alpha_for_rgb0(&mut data, width, height, stride);

        assert_eq!(data[3], 255);
        assert_eq!(data[7], 255);
        assert_eq!(data[15], 255);
        assert_eq!(data[19], 255);
        assert_eq!(data[8], 7);
        assert_eq!(data[11], 7);
        assert_eq!(data[20], 7);
        assert_eq!(data[23], 7);
    }

    #[test]
    fn fill_alpha_for_rgb0_tolerates_short_buffers() {
        let mut data = vec![0u8; 3];

        fill_alpha_for_rgb0(&mut data, 1, 1, 4);

        assert_eq!(data, vec![0, 0, 0]);
    }
}
