use gstreamer as gst;
use libmpv2::{Format, Mpv, events};
use libmpv2_sys as sys;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr;
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
        while wait_start.elapsed() < MPV_FIRST_FRAME_TIMEOUT {
            if should_abort() {
                anyhow::bail!("prebuffer aborted");
            }
            if let Some(frame) = capture_video_frame(&self.mpv, self.session_id)? {
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
                while !stop_requested.load(Ordering::SeqCst) {
                    let frame_start = Instant::now();
                    let elapsed_ns = start_time.elapsed().as_nanos() as u64;
                    if should_publish_now(&last_publish_ns, publish_interval_ns, elapsed_ns) {
                        metrics
                            .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
                        match capture_video_frame(&mpv, session_id) {
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

fn capture_video_frame(mpv: &Mpv, session_id: u64) -> anyhow::Result<Option<VideoFrame>> {
    let Some(raw) = screenshot_raw(mpv)? else {
        return Ok(None);
    };
    let Some(data) = raw.data else {
        return Ok(None);
    };
    if raw.width == 0 || raw.height == 0 || raw.stride == 0 {
        return Ok(None);
    }
    if raw.format != "rgba" {
        anyhow::bail!("unsupported screenshot-raw format '{}'", raw.format);
    }

    let expected_stride = raw.width.saturating_mul(4);
    if raw.stride < expected_stride {
        anyhow::bail!(
            "invalid screenshot-raw stride {} for {}x{} rgba",
            raw.stride,
            raw.width,
            raw.height
        );
    }

    let buffer = gst::Buffer::from_mut_slice(data);
    Ok(Some(VideoFrame {
        buffer,
        width: raw.width,
        height: raw.height,
        stride: raw.stride,
        format: VideoFrameFormat::Rgba,
        session_id,
        pts_ns: current_position_ns(mpv),
        duration_ns: None,
    }))
}

fn current_position_ns(mpv: &Mpv) -> Option<u64> {
    mpv.get_property::<f64>("time-pos")
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|seconds| (seconds * 1_000_000_000.0) as u64)
}

#[derive(Default)]
struct RawScreenshot {
    width: u32,
    height: u32,
    stride: u32,
    format: String,
    data: Option<Vec<u8>>,
}

fn screenshot_raw(mpv: &Mpv) -> anyhow::Result<Option<RawScreenshot>> {
    let args = CommandNode::array(&["screenshot-raw", "video", "rgba"])?;
    let mut result = unsafe { zero_node() };
    let code = unsafe { sys::mpv_command_node(mpv.ctx.as_ptr(), args.as_ptr(), &mut result) };
    if code < 0 {
        return Ok(None);
    }

    let parsed = unsafe { parse_raw_screenshot(&result) };
    unsafe { sys::mpv_free_node_contents(&mut result) };
    parsed.map(Some)
}

unsafe fn parse_raw_screenshot(node: &sys::mpv_node) -> anyhow::Result<RawScreenshot> {
    if node.format != sys::mpv_format_MPV_FORMAT_NODE_MAP {
        anyhow::bail!(
            "screenshot-raw returned unexpected node format {}",
            node.format
        );
    }
    let list = unsafe { node.u.list.as_ref() }
        .ok_or_else(|| anyhow::anyhow!("screenshot-raw returned a null map"))?;
    let values = unsafe { std::slice::from_raw_parts(list.values, list.num as usize) };
    let keys = unsafe { std::slice::from_raw_parts(list.keys, list.num as usize) };

    let mut fields = HashMap::new();
    for (key_ptr, value) in keys.iter().zip(values.iter()) {
        if key_ptr.is_null() {
            continue;
        }
        let key = unsafe { CStr::from_ptr(*key_ptr) }
            .to_string_lossy()
            .into_owned();
        fields.insert(key, value);
    }

    let mut raw = RawScreenshot::default();
    raw.width = node_i64(fields.get("w"))
        .or_else(|| node_i64(fields.get("width")))
        .unwrap_or(0) as u32;
    raw.height = node_i64(fields.get("h"))
        .or_else(|| node_i64(fields.get("height")))
        .unwrap_or(0) as u32;
    raw.stride = node_i64(fields.get("stride")).unwrap_or(0) as u32;
    raw.format = node_string(fields.get("format")).unwrap_or_default();
    raw.data = node_bytes(fields.get("data"));
    Ok(raw)
}

fn node_i64(node: Option<&&sys::mpv_node>) -> Option<i64> {
    let node = *node?;
    if node.format != sys::mpv_format_MPV_FORMAT_INT64 {
        return None;
    }
    Some(unsafe { node.u.int64 })
}

fn node_string(node: Option<&&sys::mpv_node>) -> Option<String> {
    let node = *node?;
    if node.format != sys::mpv_format_MPV_FORMAT_STRING {
        return None;
    }
    let raw = unsafe { node.u.string.as_ref() }?;
    Some(
        unsafe { CStr::from_ptr(raw) }
            .to_string_lossy()
            .into_owned(),
    )
}

fn node_bytes(node: Option<&&sys::mpv_node>) -> Option<Vec<u8>> {
    let node = *node?;
    if node.format != sys::mpv_format_MPV_FORMAT_BYTE_ARRAY {
        return None;
    }
    let bytes = unsafe { node.u.ba.as_ref() }?;
    if bytes.data.is_null() || bytes.size == 0 {
        return None;
    }
    let slice = unsafe { std::slice::from_raw_parts(bytes.data as *const u8, bytes.size) };
    Some(slice.to_vec())
}

struct CommandNode {
    _values: Vec<sys::mpv_node>,
    _strings: Vec<CString>,
    _list: Box<sys::mpv_node_list>,
    root: Box<sys::mpv_node>,
}

impl CommandNode {
    fn array(values: &[&str]) -> anyhow::Result<Self> {
        let strings: Vec<CString> = values
            .iter()
            .map(|value| CString::new(*value))
            .collect::<Result<_, _>>()?;
        let mut nodes = Vec::with_capacity(strings.len());
        for string in &strings {
            nodes.push(sys::mpv_node {
                u: sys::mpv_node__bindgen_ty_1 {
                    string: string.as_ptr() as *mut _,
                },
                format: sys::mpv_format_MPV_FORMAT_STRING,
            });
        }
        let mut list = Box::new(sys::mpv_node_list {
            num: nodes.len() as i32,
            values: nodes.as_mut_ptr(),
            keys: ptr::null_mut(),
        });
        let root = Box::new(sys::mpv_node {
            u: sys::mpv_node__bindgen_ty_1 {
                list: list.as_mut() as *mut _,
            },
            format: sys::mpv_format_MPV_FORMAT_NODE_ARRAY,
        });
        Ok(Self {
            _values: nodes,
            _strings: strings,
            _list: list,
            root,
        })
    }

    fn as_ptr(&self) -> *mut sys::mpv_node {
        self.root.as_ref() as *const _ as *mut _
    }
}

unsafe fn zero_node() -> sys::mpv_node {
    sys::mpv_node {
        u: sys::mpv_node__bindgen_ty_1 { int64: 0 },
        format: sys::mpv_format_MPV_FORMAT_NONE,
    }
}
