//! Shared main loop context and helpers used by both Wayland and X11 backends.
//!
//! This module contains the `MainLoopContext` struct which owns all state shared
//! between backend loops, along with helper methods that deduplicate the
//! channel-drain, scheduling, command-handling, and housekeeping logic.

use crate::cache;
use crate::metrics;
use crate::monitor;
use crate::monitor_manager;
use crate::orchestration;
use crate::queue;
use crate::renderer;
use crate::scripting;
use crate::video;

use kaleidux_common::{Request, Response, Transition};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use zune_core::bytestream::ZCursor;
use zune_core::colorspace::ColorSpace;
use zune_core::options::DecoderOptions;

// Global semaphore to limit concurrent image decode tasks (prevents memory spikes)
// Limit to 2 concurrent decodes since each can be 35-40MB
static IMAGE_DECODE_SEMAPHORE: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(2)));
static IMAGE_PREFETCH_IN_FLIGHT: once_cell::sync::Lazy<Arc<Mutex<HashSet<PathBuf>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(HashSet::new())));

type PendingVideoSessions = Arc<Mutex<HashMap<String, u64>>>;

#[derive(Debug, Clone)]
pub struct LoadedImage {
    pub name: String,
    pub session_id: u64,
    pub data: Option<Vec<u8>>,
    pub width: u32,
    pub height: u32,
    pub profile: Option<ImageLoadProfile>,
    pub _path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_resize_does_not_touch_normal_images() {
        assert_eq!(compute_upload_downscale_dimensions(1280, 720), None);
    }

    #[test]
    fn upload_resize_downscales_only_oversized_sources() {
        assert_eq!(
            compute_upload_downscale_dimensions(MAX_IMAGE_UPLOAD_DIMENSION * 2, 4000),
            Some((MAX_IMAGE_UPLOAD_DIMENSION, 2000))
        );
    }

    #[test]
    fn rgb_prep_expands_to_rgba_without_resize_when_not_needed() {
        let rgb = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 2, 2, 3840, 2160).expect("rgb prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 20, 30, 255, 40, 50, 60, 255, 70, 80, 90, 255, 100, 110, 120, 255,
            ]
        );
    }

    #[test]
    fn rgb_prep_keeps_source_dimensions_even_when_target_is_smaller() {
        let rgb = vec![64; 4 * 1 * 3];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 4, 1, 1, 1).expect("rgb prep should succeed");

        assert_eq!((width, height), (4, 1));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(rgba.len(), 4 * 1 * 4);
    }

    #[test]
    fn luma_prep_expands_to_rgba_without_resize_when_not_needed() {
        let luma = vec![10, 40, 70, 100];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_luma_image(luma, 2, 2, 3840, 2160).expect("luma prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 10, 10, 255, 40, 40, 40, 255, 70, 70, 70, 255, 100, 100, 100, 255,
            ]
        );
    }

    #[test]
    fn lumaa_prep_preserves_alpha() {
        let lumaa = vec![10, 11, 40, 41, 70, 71, 100, 101];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_lumaa_image(lumaa, 2, 2, 3840, 2160).expect("lumaa prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 10, 10, 11, 40, 40, 40, 41, 70, 70, 70, 71, 100, 100, 100, 101,
            ]
        );
    }

    #[test]
    fn prepared_image_cache_roundtrip_preserves_rgba_payload() {
        let unique = format!(
            "kaleidux-cache-test-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        );
        let source_path = std::env::temp_dir().join(unique);
        std::fs::write(&source_path, b"source").expect("temp source should be writable");

        let payload = DecodedImagePayload {
            data: vec![1, 2, 3, 4, 5, 6, 7, 8],
            width: 1,
            height: 2,
            profile: ImageLoadProfile {
                format: "png-fast".to_string(),
                source_width: 100,
                source_height: 200,
                permit_wait: Duration::ZERO,
                decode: Duration::from_millis(1),
                convert: Duration::ZERO,
                resize: Duration::from_millis(2),
                expand: Duration::ZERO,
                resize_filter: Some("bilinear".to_string()),
            },
        };

        store_prepared_image_cache(&source_path, 1920, 1080, &payload);
        let cached = try_load_prepared_image_cache(&source_path, 1920, 1080)
            .expect("prepared cache should load");

        assert_eq!(cached.width, payload.width);
        assert_eq!(cached.height, payload.height);
        assert_eq!(cached.data, payload.data);
        assert_eq!(cached.profile.source_width, payload.profile.source_width);
        assert_eq!(cached.profile.source_height, payload.profile.source_height);
        assert_eq!(cached.profile.format, "prepared-cache");

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1920, 1080) {
            let _ = std::fs::remove_file(cache_path);
        }
        let _ = std::fs::remove_file(source_path);
    }

    #[test]
    fn prepared_image_cache_is_shared_across_output_sizes() {
        let unique = format!(
            "kaleidux-cache-shared-test-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        );
        let source_path = std::env::temp_dir().join(unique);
        std::fs::write(&source_path, b"source").expect("temp source should be writable");

        let payload = DecodedImagePayload {
            data: vec![9, 8, 7, 6],
            width: 1,
            height: 1,
            profile: ImageLoadProfile {
                format: "png-fast".to_string(),
                source_width: 3840,
                source_height: 2160,
                permit_wait: Duration::ZERO,
                decode: Duration::from_millis(1),
                convert: Duration::ZERO,
                resize: Duration::ZERO,
                expand: Duration::ZERO,
                resize_filter: None,
            },
        };

        store_prepared_image_cache(&source_path, 1920, 1080, &payload);
        let cached = try_load_prepared_image_cache(&source_path, 1366, 768)
            .expect("prepared cache should load across output sizes");

        assert_eq!(cached.width, payload.width);
        assert_eq!(cached.height, payload.height);
        assert_eq!(cached.data, payload.data);

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1, 1) {
            let _ = std::fs::remove_file(cache_path);
        }
        let _ = std::fs::remove_file(source_path);
    }

    #[test]
    fn pending_video_session_state_replaces_and_clears() {
        let sessions: PendingVideoSessions = Arc::new(Mutex::new(HashMap::new()));

        set_pending_video_session(&sessions, "DP-2", Some(7));
        assert!(pending_video_session_matches(&sessions, "DP-2", 7));
        assert!(!pending_video_session_matches(&sessions, "DP-2", 8));

        set_pending_video_session(&sessions, "DP-2", Some(9));
        assert!(!pending_video_session_matches(&sessions, "DP-2", 7));
        assert!(pending_video_session_matches(&sessions, "DP-2", 9));

        set_pending_video_session(&sessions, "DP-2", None);
        assert!(!pending_video_session_matches(&sessions, "DP-2", 9));
    }
}

#[derive(Debug, Clone, Default)]
pub struct ImageLoadProfile {
    pub format: String,
    pub source_width: u32,
    pub source_height: u32,
    pub permit_wait: Duration,
    pub decode: Duration,
    pub convert: Duration,
    pub resize: Duration,
    pub expand: Duration,
    pub resize_filter: Option<String>,
}

impl ImageLoadProfile {
    fn cpu_duration(&self) -> Duration {
        self.decode + self.convert + self.resize + self.expand
    }

    fn total_duration(&self) -> Duration {
        self.permit_wait + self.cpu_duration()
    }
}

#[derive(Debug)]
struct DecodedImagePayload {
    data: Vec<u8>,
    width: u32,
    height: u32,
    profile: ImageLoadProfile,
}

pub enum VideoPlayerResult {
    Success(String, u64, video::VideoPlayer, Option<video::VideoFrame>),
    Failure(String, u64),
}

#[derive(Debug, Clone)]
pub struct PendingVideoSwitch {
    pub session_id: u64,
    pub batch_id: Option<u64>,
    pub batch_trigger_time: Option<Instant>,
    pub transition: Transition,
}

#[derive(Debug, Clone)]
pub struct StartupPresentBarrier {
    pub batch_id: u64,
    pub outputs: HashSet<String>,
    pub armed_at: Instant,
    pub deadline: Instant,
}

pub(crate) fn stop_video_player_in_background(name: String, mut player: video::VideoPlayer) {
    let _ = player.request_stop();
    tokio::task::spawn_blocking(move || {
        debug!("[VIDEO] {}: Finalizing player stop on blocking pool", name);
        let _ = player.stop();
    });
}

/// Type aliases to reduce verbosity in signatures
pub type CmdMsg = (Request, tokio::sync::oneshot::Sender<Response>);
pub type FrameMsg = (Arc<String>, video::VideoEvent);

/// Shared state for both Wayland and X11 main loops.
pub struct MainLoopContext {
    pub metrics: Arc<metrics::PerformanceMetrics>,
    pub monitor_manager: monitor_manager::MonitorManager,
    pub renderers: HashMap<String, renderer::Renderer>,
    pub video_players: HashMap<String, video::VideoPlayer>,
    pub pending_video_switches: HashMap<String, PendingVideoSwitch>,
    pub pending_image_video_stops: HashMap<String, video::VideoPlayer>,
    pub pending_video_sessions: PendingVideoSessions,
    pub wgpu_ctx: Option<Arc<renderer::WgpuContext>>,
    pub startup_present_barrier: Option<StartupPresentBarrier>,

    pub cmd_rx: tokio::sync::mpsc::UnboundedReceiver<CmdMsg>,
    pub cmd_tx: tokio::sync::mpsc::UnboundedSender<CmdMsg>,
    pub frame_rx: tokio::sync::mpsc::Receiver<FrameMsg>,
    pub frame_tx: tokio::sync::mpsc::Sender<FrameMsg>,
    pub image_rx: tokio::sync::mpsc::Receiver<LoadedImage>,
    pub image_tx: tokio::sync::mpsc::Sender<LoadedImage>,
    pub player_rx: tokio::sync::mpsc::UnboundedReceiver<VideoPlayerResult>,
    pub player_tx: tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,

    pub dir_watcher: Option<cache::DirectoryWatcher>,
    pub script_manager: scripting::ScriptManager,
    pub shutdown_flag: Arc<AtomicBool>,

    pub next_session_id: u64,
    pub first_frame_recorded: bool,
    pub last_metrics_log: Instant,
    pub last_stats_flush: Instant,
    pub last_pool_cleanup: Instant,
    pub last_script_tick: Instant,
    pub script_tick_interval: u64,
    pub target_frame_time: std::time::Duration,
}

impl MainLoopContext {
    /// Create a new `MainLoopContext` with all shared state initialized.
    /// This is the common pre-loop setup for both Wayland and X11.
    pub async fn new(
        config: orchestration::Config,
        log_level: Option<u8>,
        gstreamer_duration: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let script_path = config.global.script_path.clone();
        let script_tick_interval = config.global.script_tick_interval;
        let metrics = Arc::new(metrics::PerformanceMetrics::new());
        metrics.record_startup_start();
        metrics.record_gstreamer_init(gstreamer_duration);

        // Start resource monitor with metrics
        let sys_monitor = monitor::SystemMonitor::new_with_metrics(Some(metrics.clone()));
        tokio::spawn(async move {
            sys_monitor.run().await;
        });

        let monitor_manager = monitor_manager::MonitorManager::new_with_metrics(
            config.clone(),
            Some(metrics.clone()),
        )?;

        // Initialize directory watcher for cache invalidation
        let cache = monitor_manager.get_cache();
        let dir_watcher = match cache::DirectoryWatcher::new(cache.clone()) {
            Ok(mut watcher) => {
                for output_config in config.outputs.values() {
                    if let Some(path) = &output_config.path {
                        if let Err(e) = watcher.watch(path) {
                            warn!(
                                "[CACHE] Failed to watch directory {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
                Some(watcher)
            }
            Err(e) => {
                warn!("[CACHE] Failed to create directory watcher: {}", e);
                None
            }
        };

        // Log metrics immediately for DEBUG (3) and TRACE (4) levels
        if log_level.map(|l| l >= 3).unwrap_or(false) {
            metrics.log_summary();
        }

        // Create channels
        // Frame channel: increased capacity to 32 to cushion against micro-stutters
        // when multiple video sources are active.
        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<FrameMsg>(32);
        // Image channel: bounded to prevent memory spikes from large images accumulating
        let (image_tx, image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
        let (player_tx, player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<CmdMsg>();

        // IPC Socket Setup
        info!("[STARTUP] Setting up IPC socket");
        let socket_path = dirs::runtime_dir()
            .map(|d| d.join("kaleidux.sock"))
            .unwrap_or_else(|| {
                let uid = std::env::var("USER").unwrap_or_else(|_| "kaleidux".to_string());
                std::path::PathBuf::from(format!("/tmp/kaleidux-{}.sock", uid))
            });

        info!("[STARTUP] IPC socket path: {:?}", socket_path);
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)?;
        info!("[STARTUP] IPC socket bound successfully");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = std::fs::metadata(&socket_path) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o600);
                let _ = std::fs::set_permissions(&socket_path, perms);
            }
        }

        // Spawn IPC Listener
        let cmd_tx_clone = cmd_tx.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let cmd_tx = cmd_tx_clone.clone();
                    tokio::spawn(async move {
                        const MAX_MESSAGE_SIZE: usize = 8192;
                        let mut temp_buf = [0u8; MAX_MESSAGE_SIZE];
                        if let Ok(n) = stream.read(&mut temp_buf).await {
                            if n == 0 || n >= MAX_MESSAGE_SIZE {
                                return;
                            }
                            if let Ok(req_str) = std::str::from_utf8(&temp_buf[..n]) {
                                if let Ok(req) = serde_json::from_str::<Request>(req_str.trim()) {
                                    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                                    if cmd_tx.send((req, resp_tx)).is_ok() {
                                        if let Ok(response) = resp_rx.await {
                                            if let Ok(json) = serde_json::to_string(&response) {
                                                let _ = stream.write_all(json.as_bytes()).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        });

        // Shutdown signal handler
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown_flag.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            warn!("Received shutdown signal, cleaning up...");
            shutdown_clone.store(true, Ordering::SeqCst);
        });

        // Script manager
        info!("[STARTUP] Creating script manager");
        let script_cmd_tx = cmd_tx.clone();
        let mut script_manager = scripting::ScriptManager::new(script_cmd_tx);
        if let Some(path) = &script_path {
            info!("[STARTUP] Loading script from: {:?}", path);
            let _ = script_manager.load(path).await;
        }
        info!("[STARTUP] Script manager initialized");

        let now = Instant::now();

        Ok(MainLoopContext {
            metrics,
            monitor_manager,
            renderers: HashMap::new(),
            video_players: HashMap::new(),
            pending_video_switches: HashMap::new(),
            pending_image_video_stops: HashMap::new(),
            pending_video_sessions: Arc::new(Mutex::new(HashMap::new())),
            wgpu_ctx: None,
            startup_present_barrier: None,
            cmd_rx,
            cmd_tx,
            frame_rx,
            frame_tx,
            image_rx,
            image_tx,
            player_rx,
            player_tx,
            dir_watcher,
            script_manager,
            shutdown_flag,
            next_session_id: 1,
            first_frame_recorded: false,
            last_metrics_log: now,
            last_stats_flush: now,
            last_pool_cleanup: now,
            last_script_tick: now,
            script_tick_interval,
            target_frame_time: std::time::Duration::from_micros(16667), // ~60 FPS
        })
    }

    // ─── Idle wait ──────────────────────────────────────────────────────

    /// Returns true if any renderer is actively transitioning, needs redraw, or playing video.
    pub fn any_active(&self) -> bool {
        self.renderers.values().any(|r| {
            r.transition_active
                || r.needs_redraw
                || r.valid_content_type == queue::ContentType::Video
        })
        // Pre-wake the loop before imminent transitions to avoid cold wake-up latency (S-02)
        || self.monitor_manager.has_imminent_switch(std::time::Duration::from_millis(500))
    }

    /// Idle-wait using `tokio::select!` until any event source fires.
    /// Returns buffered messages from whichever branch triggered.
    pub async fn idle_wait(
        &mut self,
        fd: &AsyncFd<RawFd>,
    ) -> (
        Option<CmdMsg>,
        Option<FrameMsg>,
        Option<LoadedImage>,
        Option<VideoPlayerResult>,
    ) {
        let mut cmd_buf = None;
        let mut frame_buf = None;
        let mut image_buf = None;
        let mut player_buf = None;

        tokio::select! {
            cmd = self.cmd_rx.recv() => { if let Some(c) = cmd { cmd_buf = Some(c); } }
            frame = self.frame_rx.recv() => { if let Some(f) = frame { frame_buf = Some(f); } }
            image = self.image_rx.recv() => { if let Some(i) = image { image_buf = Some(i); } }
            player = self.player_rx.recv() => { if let Some(p) = player { player_buf = Some(p); } }
            result = fd.readable() => {
                if let Ok(mut guard) = result {
                    guard.clear_ready();
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }

        (cmd_buf, frame_buf, image_buf, player_buf)
    }

    // ─── Channel draining ───────────────────────────────────────────────

    /// Process scheduled changes from MonitorManager::tick().
    pub fn process_scheduled(&mut self, loop_start: Instant) {
        let scheduled_changes = self.monitor_manager.tick();
        if !scheduled_changes.is_empty() {
            let batch_id = rand::random::<u64>();
            for (name, (path, content_type)) in scheduled_changes {
                switch_wallpaper_content(
                    &name,
                    &path,
                    content_type,
                    &mut self.next_session_id,
                    &self.frame_tx,
                    &self.monitor_manager,
                    &mut self.renderers,
                    &mut self.video_players,
                    &mut self.pending_video_switches,
                    &mut self.pending_image_video_stops,
                    &self.pending_video_sessions,
                    Some(batch_id),
                    Some(loop_start),
                    &self.image_tx,
                    &self.player_tx,
                    "SCHEDULED",
                );
            }
        }
    }

    /// Process script tick.
    pub fn process_script_tick(&mut self) {
        if self.last_script_tick.elapsed().as_secs() >= self.script_tick_interval {
            self.script_manager.tick();
            self.last_script_tick = Instant::now();
        }
    }

    /// Drain and handle all pending commands.
    pub async fn drain_commands(&mut self, cmd_buf: Option<CmdMsg>, loop_start: Instant) {
        let cmd_iter = std::iter::once(cmd_buf)
            .flatten()
            .chain(std::iter::from_fn(|| self.cmd_rx.try_recv().ok()));
        for (req, resp) in cmd_iter {
            let response = handle_command(
                req,
                &mut self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
                &mut self.pending_video_switches,
                &mut self.pending_image_video_stops,
                &self.pending_video_sessions,
                &self.frame_tx,
                &self.image_tx,
                &self.player_tx,
                &mut self.next_session_id,
                loop_start,
                &self.shutdown_flag,
            )
            .await;
            let _ = resp.send(response);
        }
    }

    /// Drain video frames from channel. Returns latest frame per source, plus stats.
    pub fn drain_frames(
        &mut self,
        frame_buf: Option<FrameMsg>,
    ) -> (HashMap<Arc<String>, video::VideoFrame>, usize, usize) {
        let mut latest_frames: HashMap<Arc<String>, video::VideoFrame> = HashMap::new();
        let mut frames_received = 0;
        let mut frames_discarded = 0;

        if let Some((source_id, event)) = frame_buf {
            frames_received += 1;
            match event {
                video::VideoEvent::Frame(frame) => {
                    if latest_frames.insert(source_id.clone(), frame).is_some() {
                        frames_discarded += 1;
                    }
                }
                video::VideoEvent::Error(msg) => {
                    error!("Video error {}: {}", source_id, msg);
                    self.metrics.record_error("video_decode");
                }
            }
        }
        while let Ok((source_id, event)) = self.frame_rx.try_recv() {
            frames_received += 1;
            match event {
                video::VideoEvent::Frame(frame) => {
                    if latest_frames.insert(source_id.clone(), frame).is_some() {
                        frames_discarded += 1;
                    }
                }
                video::VideoEvent::Error(msg) => {
                    error!("Video error {}: {}", source_id, msg);
                    self.metrics.record_error("video_decode");
                }
            }
        }

        // Track frame channel usage for memory leak detection
        if frames_received > 0 {
            self.metrics.record_frame_channel_size(frames_received);
            if frames_discarded > 0 {
                debug!(
                    "[VIDEO] Discarded {} older frames (keeping latest per source)",
                    frames_discarded
                );
            }
        }

        (latest_frames, frames_received, frames_discarded)
    }

    /// Drain images from channel, upload, and optionally render.
    ///
    /// The `render_fn` closure is called for each image that needs rendering.
    /// It receives (renderer, &name, loop_start) and should perform the
    /// backend-specific render call.
    pub fn drain_images<F>(
        &mut self,
        image_buf: Option<LoadedImage>,
        loop_start: Instant,
        mut render_fn: F,
    ) where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let mut images_received = 0;
        let mut pending_images = Vec::new();
        if let Some(msg) = image_buf {
            pending_images.push(msg);
        }
        while let Ok(msg) = self.image_rx.try_recv() {
            pending_images.push(msg);
        }

        for msg in pending_images {
            images_received += 1;
            let barrier_blocks = self.startup_barrier_blocks_output(&msg.name, loop_start);
            let mut release_pending_video = false;
            let mut transition_completed = false;
            debug!(
                "[IMAGE] Received image for {}: session={}, data={}, size={}x{}",
                msg.name,
                msg.session_id,
                msg.data.is_some(),
                msg.width,
                msg.height
            );
            if let Some(r) = self.renderers.get_mut(&msg.name) {
                if r.valid_content_type != crate::queue::ContentType::Image
                    || r.active_image_session_id != msg.session_id
                {
                    if let Some(profile) = &msg.profile {
                        debug!(
                            "[IMAGE] {}: stale image was prepared via {} in {:.1}ms (wait {:.1}ms, cpu {:.1}ms)",
                            msg.name,
                            profile.format,
                            duration_ms(profile.total_duration()),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.cpu_duration())
                        );
                    }
                    debug!(
                        "[IMAGE] Dropping stale image for {}: session={} active_session={} content_type={:?}",
                        msg.name, msg.session_id, r.active_image_session_id, r.valid_content_type
                    );
                    continue;
                }

                if let Some(data) = msg.data {
                    if let Some(profile) = &msg.profile {
                        self.metrics.record_image_stage_timings(
                            profile.permit_wait,
                            profile.decode,
                            profile.convert,
                            profile.resize,
                            profile.expand,
                        );
                        debug!(
                            "[IMAGE] {}: prepared {} {}x{} -> {}x{} in {:.1}ms (wait {:.1}ms, decode {:.1}ms, convert {:.1}ms, resize {:.1}ms, expand {:.1}ms, filter={})",
                            msg.name,
                            profile.format,
                            profile.source_width,
                            profile.source_height,
                            msg.width,
                            msg.height,
                            duration_ms(profile.total_duration()),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.decode),
                            duration_ms(profile.convert),
                            duration_ms(profile.resize),
                            duration_ms(profile.expand),
                            profile.resize_filter.as_deref().unwrap_or("none")
                        );
                    }
                    debug!(
                        "[IMAGE] Uploading image data for {}: {} bytes",
                        msg.name,
                        data.len()
                    );
                    let upload_start = Instant::now();
                    let _ = r.upload_image_data(data, msg.width, msg.height);
                    self.metrics
                        .record_image_upload_cpu_time(upload_start.elapsed());
                    debug!(
                        "[IMAGE] Upload complete for {}: {:.1}ms",
                        msg.name,
                        duration_ms(upload_start.elapsed())
                    );
                    if barrier_blocks {
                        debug!(
                            "[STARTUP] {}: First image ready, holding present for barrier release",
                            msg.name
                        );
                    } else {
                        debug!("[IMAGE] Rendering after upload for {}", msg.name);
                        render_fn(r, &msg.name, loop_start);
                        if !self.first_frame_recorded {
                            self.metrics.record_first_frame();
                            self.first_frame_recorded = true;
                        }
                    }
                    release_pending_video = true;
                    transition_completed = r.transition_just_completed;
                    if transition_completed {
                        r.transition_just_completed = false;
                    }
                } else {
                    r.abort_transition();
                    release_pending_video = true;
                }
            } else {
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
            }
            if release_pending_video {
                self.release_pending_image_video_stop(&msg.name);
            }
            if transition_completed {
                self.monitor_manager.mark_transition_completed(&msg.name);
            }
        }
        if images_received > 0 {
            self.metrics.record_image_channel_size(images_received);
        }
    }

    fn release_pending_image_video_stop(&mut self, name: &str) {
        if let Some(player) = self.pending_image_video_stops.remove(name) {
            stop_video_player_in_background(name.to_string(), player);
        }
    }

    /// Drain player results from channel, activate deferred video switches, and
    /// render immediately when a preroll frame is available.
    pub fn drain_players<F>(
        &mut self,
        player_buf: Option<VideoPlayerResult>,
        loop_start: Instant,
        mut render_fn: F,
    ) where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let mut pending_players = Vec::new();
        if let Some(res) = player_buf {
            pending_players.push(res);
        }
        while let Ok(res) = self.player_rx.try_recv() {
            pending_players.push(res);
        }

        for res in pending_players {
            match res {
                VideoPlayerResult::Success(name, session_id, mut player, preroll_frame) => {
                    let barrier_blocks = self.startup_barrier_blocks_output(&name, loop_start);
                    let pending = self.pending_video_switches.get(&name).cloned();
                    if let Some(pending) = pending.filter(|p| p.session_id == session_id) {
                        self.pending_video_switches.remove(&name);

                        let mut should_render = false;
                        if let Some(r) = self.renderers.get_mut(&name) {
                            r.active_batch_id = pending.batch_id;
                            r.batch_start_time = pending.batch_trigger_time;
                            r.active_transition = pending.transition;
                            r.set_content_type(crate::queue::ContentType::Video);
                            r.active_image_session_id = 0;
                            r.active_video_session_id = session_id;
                            r.switch_content();

                            if let Some(frame) = preroll_frame.as_ref() {
                                let upload_start = Instant::now();
                                r.upload_frame(frame);
                                self.metrics.record_video_cpu_time(upload_start.elapsed());
                                should_render = true;
                            }
                        } else {
                            stop_video_player_in_background(name, player);
                            continue;
                        }

                        if let Err(e) = player.start() {
                            error!(
                                "[VIDEO] {}: Failed to start deferred video player: {}",
                                name, e
                            );
                            set_pending_video_session(&self.pending_video_sessions, &name, None);
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.abort_transition();
                            }
                            continue;
                        }

                        if let Some(old) = self.video_players.insert(name.clone(), player) {
                            stop_video_player_in_background(name.clone(), old);
                        }

                        if should_render {
                            if barrier_blocks {
                                debug!(
                                    "[STARTUP] {}: First video frame ready, holding present for barrier release",
                                    name
                                );
                            } else if let Some(r) = self.renderers.get_mut(&name) {
                                render_fn(r, &name, loop_start);
                                if !self.first_frame_recorded {
                                    self.metrics.record_first_frame();
                                    self.first_frame_recorded = true;
                                }
                                if r.transition_just_completed {
                                    r.transition_just_completed = false;
                                    self.monitor_manager.mark_transition_completed(&name);
                                }
                            }
                        }
                    } else if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Err(e) = player.start() {
                            error!("[VIDEO] {}: Failed to start video player: {}", name, e);
                            set_pending_video_session(&self.pending_video_sessions, &name, None);
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.abort_transition();
                            }
                            continue;
                        }
                        if let Some(old) = self.video_players.insert(name.clone(), player) {
                            stop_video_player_in_background(name, old);
                        }
                    } else {
                        stop_video_player_in_background(name, player);
                    }
                }
                VideoPlayerResult::Failure(name, session_id) => {
                    if self
                        .pending_video_switches
                        .get(&name)
                        .is_some_and(|p| p.session_id == session_id)
                    {
                        self.pending_video_switches.remove(&name);
                        set_pending_video_session(&self.pending_video_sessions, &name, None);
                    }
                    if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Some(r) = self.renderers.get_mut(&name) {
                            r.abort_transition();
                        }
                    }
                }
            }
        }
    }

    // ─── Housekeeping ───────────────────────────────────────────────────

    /// Record frame time, clean up texture pool, flush stats, process dir watcher,
    /// log metrics summary. Called at the end of each loop iteration.
    pub async fn housekeeping(&mut self, loop_start: Instant, was_idle: bool) {
        // Skip recording frame time for iterations that entered idle_wait (P-26)
        if !was_idle {
            let frame_time = loop_start.elapsed();
            self.metrics.record_frame_time(frame_time);
        }

        // Cleanup texture pool periodically (every 3 seconds)
        if self.last_pool_cleanup.elapsed().as_secs() >= 3 {
            if let Some(ctx) = &self.wgpu_ctx {
                ctx.cleanup_texture_pool(Some(&self.metrics));
            }
            self.last_pool_cleanup = Instant::now();
        }

        // Flush stats every 5 seconds (batched writes)
        if self.last_stats_flush.elapsed().as_secs() >= 5 {
            let _ = self.monitor_manager.flush_all_stats();
            self.last_stats_flush = Instant::now();
        }

        // Process directory watcher events and apply pool updates
        if let Some(ref mut watcher) = self.dir_watcher {
            let pool_events = watcher.process_events().await;
            self.monitor_manager.apply_pool_events(pool_events);
        }

        // Log metrics summary every 10 seconds
        if self.last_metrics_log.elapsed().as_secs() >= 10 {
            if let Some(ctx) = &self.wgpu_ctx {
                let (texture_count, texture_pool_bytes) = ctx.texture_pool_stats();
                let pipeline_count = ctx.transition_pipelines.lock().len()
                    + ctx.blit_pipelines.lock().len()
                    + ctx.mipmap_pipelines.lock().len();
                self.metrics.record_texture_count(texture_count);
                self.metrics.record_pipeline_count(pipeline_count);

                let mut retained = renderer::RetainedTextureFootprint::default();
                let mut per_renderer = Vec::new();
                let to_mb = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
                for (name, r) in &self.renderers {
                    let fp = r.retained_texture_footprint();
                    retained.current_bytes =
                        retained.current_bytes.saturating_add(fp.current_bytes);
                    retained.prev_bytes = retained.prev_bytes.saturating_add(fp.prev_bytes);
                    retained.composition_bytes = retained
                        .composition_bytes
                        .saturating_add(fp.composition_bytes);
                    retained.video_aux_bytes =
                        retained.video_aux_bytes.saturating_add(fp.video_aux_bytes);
                    per_renderer.push(format!(
                        "{}={:.1}MB(c={:.1} p={:.1} comp={:.1} aux={:.1})",
                        name,
                        to_mb(fp.total_bytes()),
                        to_mb(fp.current_bytes),
                        to_mb(fp.prev_bytes),
                        to_mb(fp.composition_bytes),
                        to_mb(fp.video_aux_bytes)
                    ));
                }
                info!(
                    "[MEMORY] Renderer retained textures: total={:.1}MB current={:.1}MB prev={:.1}MB composition={:.1}MB video_aux={:.1}MB pool={:.1}MB | {}",
                    to_mb(retained.total_bytes()),
                    to_mb(retained.current_bytes),
                    to_mb(retained.prev_bytes),
                    to_mb(retained.composition_bytes),
                    to_mb(retained.video_aux_bytes),
                    to_mb(texture_pool_bytes),
                    per_renderer.join(" | ")
                );
            }
            self.metrics.log_summary();
            self.last_metrics_log = Instant::now();
        }
    }

    /// Sleep at vsync rate if actively rendering, then poll device.
    pub async fn timing_and_poll(&self, any_active: bool, loop_start: Instant) {
        let elapsed = loop_start.elapsed();
        if any_active && elapsed < self.target_frame_time {
            tokio::time::sleep(self.target_frame_time - elapsed).await;
        }
        if let Some(ctx) = &self.wgpu_ctx {
            ctx.device.poll(wgpu::Maintain::Poll);
        }
    }

    /// Perform the initial content load.
    /// Calls `monitor_manager.tick()` and dispatches initial wallpaper content.
    pub fn initial_load(&mut self) {
        info!(
            "[STARTUP] Reached Initial Load section, renderers count: {}",
            self.renderers.len()
        );
        info!("[STARTUP] About to call monitor_manager.tick()");
        let initial_changes = self.monitor_manager.tick();
        info!(
            "[STARTUP] Initial changes: {} outputs",
            initial_changes.len()
        );
        for (name, (path, content_type)) in &initial_changes {
            info!(
                "[STARTUP] Change: {} -> {:?} ({:?})",
                name, path, content_type
            );
        }
        if initial_changes.is_empty() {
            warn!("[STARTUP] No initial content changes - wallpapers may not load!");
        }
        let batch_id = rand::random::<u64>();
        let mut startup_outputs = Vec::new();
        for (name, (path, content_type)) in initial_changes {
            if !self.renderers.contains_key(&name) {
                warn!(
                    "[STARTUP] Skipping initial content for {} - renderer does not exist",
                    name
                );
                continue;
            }
            startup_outputs.push(name.clone());
            switch_wallpaper_content(
                &name,
                &path,
                content_type,
                &mut self.next_session_id,
                &self.frame_tx,
                &self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
                &mut self.pending_video_switches,
                &mut self.pending_image_video_stops,
                &self.pending_video_sessions,
                Some(batch_id),
                None,
                &self.image_tx,
                &self.player_tx,
                "STARTUP",
            );
        }
        if startup_outputs.len() > 1 {
            self.arm_startup_present_barrier(batch_id, startup_outputs);
        }
    }

    pub fn arm_startup_present_barrier(&mut self, batch_id: u64, outputs: Vec<String>) {
        let output_set: HashSet<_> = outputs
            .into_iter()
            .filter(|name| self.renderers.contains_key(name))
            .collect();
        if output_set.len() <= 1 {
            return;
        }

        let now = Instant::now();
        self.startup_present_barrier = Some(StartupPresentBarrier {
            batch_id,
            outputs: output_set,
            armed_at: now,
            deadline: now + Duration::from_secs(3),
        });
        info!(
            "[STARTUP] First-present barrier armed for {} outputs (batch {:x})",
            self.startup_present_barrier
                .as_ref()
                .map_or(0, |b| b.outputs.len()),
            batch_id
        );
    }

    pub fn startup_barrier_blocks_output(&self, name: &str, now: Instant) -> bool {
        match &self.startup_present_barrier {
            Some(barrier) if barrier.outputs.contains(name) => !self.startup_barrier_ready(now),
            _ => false,
        }
    }

    pub fn startup_barrier_ready(&self, now: Instant) -> bool {
        let Some(barrier) = &self.startup_present_barrier else {
            return true;
        };

        if now >= barrier.deadline {
            return true;
        }

        barrier.outputs.iter().all(|name| {
            self.renderers
                .get(name)
                .is_some_and(|r| r.has_current_texture())
        })
    }

    pub fn release_startup_present_barrier<F>(&mut self, loop_start: Instant, mut render_fn: F)
    where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let Some(barrier) = self.startup_present_barrier.clone() else {
            return;
        };
        if !self.startup_barrier_ready(loop_start) {
            return;
        }

        let timed_out = loop_start >= barrier.deadline;
        info!(
            "[STARTUP] Releasing first-present barrier for batch {:x} after {:.1}ms{}",
            barrier.batch_id,
            duration_ms(loop_start.saturating_duration_since(barrier.armed_at)),
            if timed_out { " (timeout)" } else { "" }
        );

        for name in &barrier.outputs {
            if let Some(r) = self.renderers.get_mut(name) {
                render_fn(r, name, loop_start);
                if !self.first_frame_recorded {
                    self.metrics.record_first_frame();
                    self.first_frame_recorded = true;
                }
            }
        }

        self.startup_present_barrier = None;
    }

    /// Clean shutdown — stop all video players and save caches.
    pub fn shutdown(&mut self) {
        for (_, mut player) in self.video_players.drain() {
            let _ = player.stop();
        }
        for (_, mut player) in self.pending_image_video_stops.drain() {
            let _ = player.stop();
        }
        self.pending_video_switches.clear();
        if let Ok(mut sessions) = self.pending_video_sessions.lock() {
            sessions.clear();
        }
        // Drop renderer-owned wgpu surfaces while the backend connection still exists.
        self.renderers.clear();
        self.wgpu_ctx = None;
        // Persist WGSL cache to disk on shutdown (P-15 cache layer 1)
        if let Err(e) = crate::shaders::ShaderManager::save_cache() {
            warn!("[SHADER] Failed to save WGSL cache: {}", e);
        }
    }
}

// ─── Standalone helpers ─────────────────────────────────────────────────────

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

pub(crate) fn set_pending_video_session(
    pending_video_sessions: &PendingVideoSessions,
    name: &str,
    session_id: Option<u64>,
) {
    let Ok(mut sessions) = pending_video_sessions.lock() else {
        return;
    };

    match session_id {
        Some(session_id) => {
            sessions.insert(name.to_string(), session_id);
        }
        None => {
            sessions.remove(name);
        }
    }
}

fn pending_video_session_matches(
    pending_video_sessions: &PendingVideoSessions,
    name: &str,
    session_id: u64,
) -> bool {
    pending_video_sessions
        .lock()
        .ok()
        .and_then(|sessions| sessions.get(name).copied())
        == Some(session_id)
}

fn image_format_label(format: Option<image::ImageFormat>, fast_path: bool) -> String {
    let label = match format {
        Some(image::ImageFormat::Avif) => "avif",
        Some(image::ImageFormat::Bmp) => "bmp",
        Some(image::ImageFormat::Gif) => "gif",
        Some(image::ImageFormat::Hdr) => "hdr",
        Some(image::ImageFormat::Ico) => "ico",
        Some(image::ImageFormat::Jpeg) => "jpeg",
        Some(image::ImageFormat::OpenExr) => "openexr",
        Some(image::ImageFormat::Png) => "png",
        Some(image::ImageFormat::Pnm) => "pnm",
        Some(image::ImageFormat::Qoi) => "qoi",
        Some(image::ImageFormat::Tga) => "tga",
        Some(image::ImageFormat::Tiff) => "tiff",
        Some(image::ImageFormat::WebP) => "webp",
        Some(image::ImageFormat::Dds) => "dds",
        Some(image::ImageFormat::Farbfeld) => "farbfeld",
        _ => "unknown",
    };

    if fast_path {
        format!("{}-fast", label)
    } else {
        label.to_string()
    }
}

const PREPARED_IMAGE_CACHE_MAGIC: &[u8; 8] = b"KDXIMG01";

fn prepared_image_cache_dir() -> Option<PathBuf> {
    let dir = dirs::cache_dir()?.join("kaleidux").join("prepared-images");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

fn prepared_image_cache_key(
    path: &Path,
    _target_width: u32,
    _target_height: u32,
) -> Option<String> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let modified = modified.duration_since(UNIX_EPOCH).ok()?;

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.as_os_str().as_encoded_bytes().hash(&mut hasher);
    meta.len().hash(&mut hasher);
    modified.as_secs().hash(&mut hasher);
    modified.subsec_nanos().hash(&mut hasher);
    Some(format!("{:016x}", hasher.finish()))
}

fn prepared_image_cache_path(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<PathBuf> {
    let dir = prepared_image_cache_dir()?;
    let key = prepared_image_cache_key(path, target_width, target_height)?;
    Some(dir.join(format!("{key}.rgba")))
}

fn try_load_prepared_image_cache(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<DecodedImagePayload> {
    let cache_path = prepared_image_cache_path(path, target_width, target_height)?;
    let bytes = std::fs::read(cache_path).ok()?;
    if bytes.len() < PREPARED_IMAGE_CACHE_MAGIC.len() + (4 * 4) {
        return None;
    }
    if &bytes[..PREPARED_IMAGE_CACHE_MAGIC.len()] != PREPARED_IMAGE_CACHE_MAGIC {
        return None;
    }

    let mut cursor = PREPARED_IMAGE_CACHE_MAGIC.len();
    let read_u32 = |buf: &[u8], cursor: &mut usize| -> Option<u32> {
        let end = *cursor + 4;
        let slice = buf.get(*cursor..end)?;
        *cursor = end;
        Some(u32::from_le_bytes(slice.try_into().ok()?))
    };

    let width = read_u32(&bytes, &mut cursor)?;
    let height = read_u32(&bytes, &mut cursor)?;
    let source_width = read_u32(&bytes, &mut cursor)?;
    let source_height = read_u32(&bytes, &mut cursor)?;
    let data = bytes.get(cursor..)?.to_vec();
    if data.len() != (width as usize * height as usize * 4) {
        return None;
    }

    Some(DecodedImagePayload {
        data,
        width,
        height,
        profile: ImageLoadProfile {
            format: "prepared-cache".to_string(),
            source_width,
            source_height,
            permit_wait: Duration::ZERO,
            decode: Duration::ZERO,
            convert: Duration::ZERO,
            resize: Duration::ZERO,
            expand: Duration::ZERO,
            resize_filter: None,
        },
    })
}

fn store_prepared_image_cache(
    path: &Path,
    target_width: u32,
    target_height: u32,
    payload: &DecodedImagePayload,
) {
    let Some(cache_path) = prepared_image_cache_path(path, target_width, target_height) else {
        return;
    };

    let expected_len = payload.width as usize * payload.height as usize * 4;
    if payload.data.len() != expected_len {
        return;
    }

    let mut bytes =
        Vec::with_capacity(PREPARED_IMAGE_CACHE_MAGIC.len() + (4 * 4) + payload.data.len());
    bytes.extend_from_slice(PREPARED_IMAGE_CACHE_MAGIC);
    bytes.extend_from_slice(&payload.width.to_le_bytes());
    bytes.extend_from_slice(&payload.height.to_le_bytes());
    bytes.extend_from_slice(&payload.profile.source_width.to_le_bytes());
    bytes.extend_from_slice(&payload.profile.source_height.to_le_bytes());
    bytes.extend_from_slice(&payload.data);

    let tmp_path = cache_path.with_extension("rgba.tmp");
    if std::fs::write(&tmp_path, &bytes).is_ok() {
        let _ = std::fs::rename(tmp_path, cache_path);
    }
}

const MAX_IMAGE_UPLOAD_DIMENSION: u32 = 8192;

fn compute_upload_downscale_dimensions(
    source_width: u32,
    source_height: u32,
) -> Option<(u32, u32)> {
    if source_width <= MAX_IMAGE_UPLOAD_DIMENSION && source_height <= MAX_IMAGE_UPLOAD_DIMENSION {
        return None;
    }

    let longest_edge = source_width.max(source_height) as f32;
    let scale = MAX_IMAGE_UPLOAD_DIMENSION as f32 / longest_edge;
    let resized_width = ((source_width as f32 * scale).round() as u32).max(1);
    let resized_height = ((source_height as f32 * scale).round() as u32).max(1);
    Some((resized_width, resized_height))
}

fn select_resize_filter(
    source_width: u32,
    source_height: u32,
    resized_width: u32,
    resized_height: u32,
) -> fast_image_resize::FilterType {
    let width_ratio = source_width as f32 / resized_width as f32;
    let height_ratio = source_height as f32 / resized_height as f32;
    if width_ratio >= 2.0 || height_ratio >= 2.0 {
        fast_image_resize::FilterType::Bilinear
    } else {
        fast_image_resize::FilterType::CatmullRom
    }
}

fn resize_filter_label(filter: fast_image_resize::FilterType) -> &'static str {
    match filter {
        fast_image_resize::FilterType::Box => "box",
        fast_image_resize::FilterType::Bilinear => "bilinear",
        fast_image_resize::FilterType::Hamming => "hamming",
        fast_image_resize::FilterType::CatmullRom => "catmull-rom",
        fast_image_resize::FilterType::Mitchell => "mitchell",
        fast_image_resize::FilterType::Gaussian => "gaussian",
        fast_image_resize::FilterType::Lanczos3 => "lanczos3",
        fast_image_resize::FilterType::Custom(_) => "custom",
        _ => "unknown",
    }
}

fn resize_image_buffer(
    source_data: Vec<u8>,
    source_width: u32,
    source_height: u32,
    resized_width: u32,
    resized_height: u32,
    pixel_type: fast_image_resize::PixelType,
    filter: fast_image_resize::FilterType,
) -> anyhow::Result<Vec<u8>> {
    use fast_image_resize as fr;

    let source =
        fr::images::Image::from_vec_u8(source_width, source_height, source_data, pixel_type)
            .map_err(|e| anyhow::anyhow!("invalid source image buffer: {}", e))?;
    let mut resized = fr::images::Image::new(resized_width, resized_height, pixel_type);
    let mut resizer = fr::Resizer::new();
    resizer
        .resize(
            &source,
            &mut resized,
            &fr::ResizeOptions::new().resize_alg(fr::ResizeAlg::Convolution(filter)),
        )
        .map_err(|e| anyhow::anyhow!("image resize failed: {}", e))?;
    Ok(resized.into_vec())
}

fn expand_rgb_to_rgba(rgb: Vec<u8>) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((rgb.len() / 3) * 4);
    for chunk in rgb.chunks_exact(3) {
        rgba.extend_from_slice(&[chunk[0], chunk[1], chunk[2], 255]);
    }
    rgba
}

fn expand_luma_to_rgba(luma: Vec<u8>) -> Vec<u8> {
    let mut rgba = Vec::with_capacity(luma.len() * 4);
    for value in luma {
        rgba.extend_from_slice(&[value, value, value, 255]);
    }
    rgba
}

fn expand_lumaa_to_rgba(lumaa: Vec<u8>) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((lumaa.len() / 2) * 4);
    for chunk in lumaa.chunks_exact(2) {
        rgba.extend_from_slice(&[chunk[0], chunk[0], chunk[0], chunk[1]]);
    }
    rgba
}

fn prepare_rgb_image(
    pixels: Vec<u8>,
    source_width: u32,
    source_height: u32,
    _target_width: u32,
    _target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (rgb_data, width, height) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(source_width, source_height)
    {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x3,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        (resized, resized_width, resized_height)
    } else {
        (pixels, source_width, source_height)
    };

    let expand_start = Instant::now();
    let rgba_data = expand_rgb_to_rgba(rgb_data);
    let expand_duration = expand_start.elapsed();
    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

fn prepare_rgba_image(
    pixels: Vec<u8>,
    source_width: u32,
    source_height: u32,
    _target_width: u32,
    _target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Option<String>)> {
    if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(source_width, source_height)
    {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x4,
            filter,
        )?;
        return Ok((
            resized,
            resized_width,
            resized_height,
            resize_start.elapsed(),
            Some(resize_filter_label(filter).to_string()),
        ));
    }

    Ok((pixels, source_width, source_height, Duration::ZERO, None))
}

fn prepare_luma_image(
    pixels: Vec<u8>,
    source_width: u32,
    source_height: u32,
    _target_width: u32,
    _target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (luma_data, width, height) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(source_width, source_height)
    {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        (resized, resized_width, resized_height)
    } else {
        (pixels, source_width, source_height)
    };

    let expand_start = Instant::now();
    let rgba_data = expand_luma_to_rgba(luma_data);
    let expand_duration = expand_start.elapsed();
    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

fn prepare_lumaa_image(
    pixels: Vec<u8>,
    source_width: u32,
    source_height: u32,
    _target_width: u32,
    _target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (lumaa_data, width, height) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(source_width, source_height)
    {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x2,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        (resized, resized_width, resized_height)
    } else {
        (pixels, source_width, source_height)
    };

    let expand_start = Instant::now();
    let rgba_data = expand_lumaa_to_rgba(lumaa_data);
    let expand_duration = expand_start.elapsed();
    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

fn decode_jpeg_fast(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    let decode_start = Instant::now();
    let encoded = std::fs::read(path)?;
    let options = DecoderOptions::new_fast()
        .set_strict_mode(false)
        .set_max_width(usize::MAX)
        .set_max_height(usize::MAX)
        .jpeg_set_out_colorspace(ColorSpace::RGB);
    let mut decoder =
        zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(encoded.as_slice()), options);
    decoder
        .decode_headers()
        .map_err(|e| anyhow::anyhow!("jpeg header decode failed: {}", e))?;
    let (source_width, source_height) = decoder
        .dimensions()
        .ok_or_else(|| anyhow::anyhow!("jpeg dimensions missing after header decode"))?;
    let source_width =
        u32::try_from(source_width).map_err(|_| anyhow::anyhow!("jpeg width is too large"))?;
    let source_height =
        u32::try_from(source_height).map_err(|_| anyhow::anyhow!("jpeg height is too large"))?;
    let decoded = decoder
        .decode()
        .map_err(|e| anyhow::anyhow!("jpeg decode failed: {}", e))?;
    let decode_duration = decode_start.elapsed();

    let (data, width, height, resize_duration, expand_duration, resize_filter) = prepare_rgb_image(
        decoded,
        source_width,
        source_height,
        target_width,
        target_height,
    )?;

    Ok(DecodedImagePayload {
        data,
        width,
        height,
        profile: ImageLoadProfile {
            format: "jpeg-fast".to_string(),
            source_width,
            source_height,
            permit_wait: Duration::ZERO,
            decode: decode_duration,
            convert: Duration::ZERO,
            resize: resize_duration,
            expand: expand_duration,
            resize_filter,
        },
    })
}

fn decode_png_fast(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    let decode_start = Instant::now();
    let encoded = std::fs::read(path)?;
    let options = DecoderOptions::default()
        .set_strict_mode(false)
        .set_max_width(usize::MAX)
        .set_max_height(usize::MAX)
        .png_set_strip_to_8bit(true);
    let mut decoder =
        zune_png::PngDecoder::new_with_options(ZCursor::new(encoded.as_slice()), options);
    decoder
        .decode_headers()
        .map_err(|e| anyhow::anyhow!("png header decode failed: {}", e))?;
    let (source_width, source_height) = decoder
        .dimensions()
        .ok_or_else(|| anyhow::anyhow!("png dimensions missing after header decode"))?;
    let source_width =
        u32::try_from(source_width).map_err(|_| anyhow::anyhow!("png width is too large"))?;
    let source_height =
        u32::try_from(source_height).map_err(|_| anyhow::anyhow!("png height is too large"))?;
    let colorspace = decoder
        .colorspace()
        .ok_or_else(|| anyhow::anyhow!("png colorspace missing after header decode"))?;
    let decoded = decoder
        .decode_raw()
        .map_err(|e| anyhow::anyhow!("png decode failed: {}", e))?;
    let decode_duration = decode_start.elapsed();

    let (data, width, height, resize_duration, expand_duration, resize_filter) = match colorspace {
        ColorSpace::RGB => prepare_rgb_image(
            decoded,
            source_width,
            source_height,
            target_width,
            target_height,
        )?,
        ColorSpace::RGBA => {
            let (prepared, width, height, resize_duration, resize_filter) = prepare_rgba_image(
                decoded,
                source_width,
                source_height,
                target_width,
                target_height,
            )?;
            (
                prepared,
                width,
                height,
                resize_duration,
                Duration::ZERO,
                resize_filter,
            )
        }
        ColorSpace::Luma => prepare_luma_image(
            decoded,
            source_width,
            source_height,
            target_width,
            target_height,
        )?,
        ColorSpace::LumaA => prepare_lumaa_image(
            decoded,
            source_width,
            source_height,
            target_width,
            target_height,
        )?,
        other => {
            return Err(anyhow::anyhow!(
                "unsupported fast png colorspace {:?} for {}",
                other,
                path.display()
            ));
        }
    };

    Ok(DecodedImagePayload {
        data,
        width,
        height,
        profile: ImageLoadProfile {
            format: "png-fast".to_string(),
            source_width,
            source_height,
            permit_wait: Duration::ZERO,
            decode: decode_duration,
            convert: Duration::ZERO,
            resize: resize_duration,
            expand: expand_duration,
            resize_filter,
        },
    })
}

fn decode_image_generic(
    path: &Path,
    target_width: u32,
    target_height: u32,
    format: Option<image::ImageFormat>,
) -> anyhow::Result<DecodedImagePayload> {
    let decode_start = Instant::now();
    let image = image::open(path)?;
    let decode_duration = decode_start.elapsed();
    let source_width = image.width();
    let source_height = image.height();
    let has_alpha = image.has_alpha();

    let convert_start = Instant::now();
    let (data, width, height, resize_duration, expand_duration, resize_filter) = if has_alpha {
        let rgba = image.into_rgba8().into_raw();
        let (prepared, width, height, resize_duration, resize_filter) = prepare_rgba_image(
            rgba,
            source_width,
            source_height,
            target_width,
            target_height,
        )?;
        (
            prepared,
            width,
            height,
            resize_duration,
            Duration::ZERO,
            resize_filter,
        )
    } else {
        let rgb = image.into_rgb8().into_raw();
        let (prepared, width, height, resize_duration, expand_duration, resize_filter) =
            prepare_rgb_image(
                rgb,
                source_width,
                source_height,
                target_width,
                target_height,
            )?;
        (
            prepared,
            width,
            height,
            resize_duration,
            expand_duration,
            resize_filter,
        )
    };

    let convert_duration = convert_start.elapsed();

    Ok(DecodedImagePayload {
        data,
        width,
        height,
        profile: ImageLoadProfile {
            format: image_format_label(format, false),
            source_width,
            source_height,
            permit_wait: Duration::ZERO,
            decode: decode_duration,
            convert: convert_duration,
            resize: resize_duration,
            expand: expand_duration,
            resize_filter,
        },
    })
}

fn decode_image_for_output(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    if let Some(payload) = try_load_prepared_image_cache(path, target_width, target_height) {
        return Ok(payload);
    }

    let format = image::ImageFormat::from_path(path).ok();
    let payload = match format {
        Some(image::ImageFormat::Jpeg) => match decode_jpeg_fast(path, target_width, target_height)
        {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    "[ASSET] {}: Fast JPEG decode failed, falling back to generic image path: {}",
                    path.display(),
                    e
                );
                decode_image_generic(path, target_width, target_height, format)?
            }
        },
        Some(image::ImageFormat::Png) => match decode_png_fast(path, target_width, target_height) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    "[ASSET] {}: Fast PNG decode failed, falling back to generic image path: {}",
                    path.display(),
                    e
                );
                decode_image_generic(path, target_width, target_height, format)?
            }
        },
        _ => decode_image_generic(path, target_width, target_height, format)?,
    };

    store_prepared_image_cache(path, target_width, target_height, &payload);
    Ok(payload)
}

fn schedule_image_prefetch(name: &str, path: &Path) {
    let prefetch_path = path.to_path_buf();
    let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() else {
        return;
    };
    if !in_flight.insert(prefetch_path.clone()) {
        return;
    }
    drop(in_flight);

    let output_name = name.to_string();
    let semaphore = IMAGE_DECODE_SEMAPHORE.clone();
    tokio::spawn(async move {
        let _permit = match semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                if let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() {
                    in_flight.remove(&prefetch_path);
                }
                return;
            }
        };

        let decode_path = prefetch_path.clone();
        let result =
            tokio::task::spawn_blocking(move || decode_image_for_output(&decode_path, 0, 0)).await;

        match result {
            Ok(Ok(payload)) => debug!(
                "[PREFETCH] {}: Warmed {} as {} {}x{} in {:.1}ms",
                output_name,
                prefetch_path.display(),
                payload.profile.format,
                payload.width,
                payload.height,
                duration_ms(payload.profile.total_duration())
            ),
            Ok(Err(e)) => debug!(
                "[PREFETCH] {}: Failed to warm {}: {}",
                output_name,
                prefetch_path.display(),
                e
            ),
            Err(e) => debug!(
                "[PREFETCH] {}: Prefetch task panicked for {}: {}",
                output_name,
                prefetch_path.display(),
                e
            ),
        }

        if let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() {
            in_flight.remove(&prefetch_path);
        }
    });
}

fn resolve_transition_for_output(
    monitor_manager: &monitor_manager::MonitorManager,
    name: &str,
) -> Transition {
    monitor_manager
        .outputs
        .get(name)
        .map(|orchestrator| {
            if matches!(orchestrator.config.transition, Transition::Random) {
                let picked = crate::shaders::ShaderManager::pick_random_transition();
                debug!(
                    "[TRANSITION] {}: Resolved Random transition to: {}",
                    name,
                    picked.name()
                );
                picked
            } else {
                orchestrator.config.transition.clone()
            }
        })
        .unwrap_or_default()
}

/// Helper function to switch wallpaper content for an output.
#[allow(clippy::too_many_arguments)]
pub fn switch_wallpaper_content(
    name: &str,
    path: &Path,
    content_type: queue::ContentType,
    next_session_id: &mut u64,
    frame_tx: &tokio::sync::mpsc::Sender<FrameMsg>,
    monitor_manager: &monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
    pending_video_switches: &mut HashMap<String, PendingVideoSwitch>,
    pending_image_video_stops: &mut HashMap<String, video::VideoPlayer>,
    pending_video_sessions: &PendingVideoSessions,
    batch_id: Option<u64>,
    batch_trigger_time: Option<std::time::Instant>,
    image_tx: &tokio::sync::mpsc::Sender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    log_prefix: &str,
) {
    info!("{}: {} -> {:?}", log_prefix, name, path.display());
    debug!(
        "[SWITCH] {}: content_type={:?}, renderer exists={}",
        name,
        content_type,
        renderers.contains_key(name)
    );

    let session_id = *next_session_id;
    *next_session_id += 1;

    if let Some(old_pending_stop) = pending_image_video_stops.remove(name) {
        stop_video_player_in_background(name.to_string(), old_pending_stop);
    }
    let mut prior_video_player = video_players.remove(name);

    let mut should_prepare_video = false;
    if let Some(r) = renderers.get_mut(name) {
        let resolved_transition = resolve_transition_for_output(monitor_manager, name);
        r.active_batch_id = batch_id;
        r.batch_start_time = batch_trigger_time;
        r.active_transition = resolved_transition.clone();

        if content_type == queue::ContentType::Image {
            pending_video_switches.remove(name);
            set_pending_video_session(pending_video_sessions, name, None);
            r.set_content_type(content_type);
            r.active_image_session_id = session_id;
            r.active_video_session_id = 0;
            r.switch_content();
            let target_width = r.config.width.clone();
            let target_height = r.config.height.clone();

            let name_clone = name.to_string();
            let path_clone = path.to_path_buf();
            let tx = image_tx.clone();
            let semaphore = IMAGE_DECODE_SEMAPHORE.clone();
            let image_session_id = session_id;
            if let Some(old_video_player) = prior_video_player.take() {
                pending_image_video_stops.insert(name.to_string(), old_video_player);
            }

            debug!(
                "[ASSET] {}: Offloading image decode: {}",
                name,
                path.display()
            );
            tokio::spawn(async move {
                // Acquire permit before decoding to limit concurrent tasks
                let permit_wait_start = Instant::now();
                let _permit = match semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        debug!(
                            "[ASSET] {}: Semaphore closed, skipping image decode",
                            name_clone
                        );
                        return;
                    }
                };
                let permit_wait = permit_wait_start.elapsed();
                if permit_wait > Duration::from_millis(10) {
                    debug!(
                        "[ASSET] {}: Waited {:.1}ms for image decode permit",
                        name_clone,
                        duration_ms(permit_wait)
                    );
                }

                // Decode image in blocking task
                let path_for_decode = path_clone.clone();
                let decode_result = tokio::task::spawn_blocking(move || {
                    decode_image_for_output(&path_for_decode, target_width, target_height)
                })
                .await;

                // Send decoded image (or error) to channel
                match decode_result {
                    Ok(Ok(mut payload)) => {
                        payload.profile.permit_wait = permit_wait;
                        if let Err(e) = tx
                            .send(LoadedImage {
                                name: name_clone.clone(),
                                session_id: image_session_id,
                                data: Some(payload.data),
                                width: payload.width,
                                height: payload.height,
                                profile: Some(payload.profile),
                                _path: path_clone,
                            })
                            .await
                        {
                            debug!(
                                "[ASSET] {}: Failed to send decoded image (channel closed): {}",
                                name_clone, e
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Failed to decode image {}: {}", path_clone.display(), e);
                        let _ = tx
                            .send(LoadedImage {
                                name: name_clone,
                                session_id: image_session_id,
                                data: None,
                                width: 0,
                                height: 0,
                                profile: None,
                                _path: path_clone,
                            })
                            .await;
                    }
                    Err(e) => {
                        error!("Image decode task panicked: {}", e);
                    }
                }
            });
        } else {
            r.active_image_session_id = 0;
            r.active_video_session_id = session_id;
            set_pending_video_session(pending_video_sessions, name, Some(session_id));
            pending_video_switches.insert(
                name.to_string(),
                PendingVideoSwitch {
                    session_id,
                    batch_id,
                    batch_trigger_time,
                    transition: resolved_transition,
                },
            );
            should_prepare_video = true;
        }
    } else {
        set_pending_video_session(pending_video_sessions, name, None);
        pending_video_switches.remove(name);
        if let Some(vp) = prior_video_player.take() {
            stop_video_player_in_background(name.to_string(), vp);
        }
        warn!(
            "[SWITCH] {}: Skipping content switch because renderer no longer exists",
            name
        );
    }

    if content_type == queue::ContentType::Video && should_prepare_video {
        debug!(
            "[TRANSITION] {}: Preparing deferred video player (session_id={})",
            name, session_id
        );
        create_and_start_video_player(
            path,
            name,
            session_id,
            monitor_manager
                .outputs
                .get(name)
                .map(|o| o.config.volume as f64 / 100.0)
                .unwrap_or(1.0),
            frame_tx,
            player_tx,
            pending_video_sessions.clone(),
            prior_video_player,
        );
    }

    if let Some(orchestrator) = monitor_manager.outputs.get(name) {
        if orchestrator.next_content_type == Some(queue::ContentType::Image) {
            if let Some(next_path) = orchestrator.next_path.as_ref() {
                schedule_image_prefetch(name, next_path);
            }
        }
    }
}

fn create_and_start_video_player(
    path: &Path,
    name: &str,
    session_id: u64,
    volume: f64,
    frame_tx: &tokio::sync::mpsc::Sender<FrameMsg>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    pending_video_sessions: PendingVideoSessions,
    old_player: Option<video::VideoPlayer>,
) {
    let path_str = path.to_string_lossy().into_owned();
    let name_arc = Arc::new(name.to_string());
    let name_str = name.to_string();
    let frame_tx_clone = frame_tx.clone();
    let player_tx_clone = player_tx.clone();

    tokio::task::spawn_blocking(move || {
        let name_for_panic = name_str.clone();
        let player_tx_panic = player_tx_clone.clone();
        let session_id_panic = session_id;
        let pending_video_sessions_for_task = pending_video_sessions.clone();

        if let Some(mut old_player) = old_player {
            let stop_start = Instant::now();
            match old_player.stop() {
                Ok(()) => debug!(
                    "[VIDEO] {}: Previous player fully stopped in {:.1}ms before replacement",
                    name_str,
                    duration_ms(stop_start.elapsed())
                ),
                Err(e) => warn!(
                    "[VIDEO] {}: Failed to stop previous player before replacement: {}",
                    name_str, e
                ),
            }
        }

        if !pending_video_session_matches(&pending_video_sessions_for_task, &name_str, session_id) {
            debug!(
                "[VIDEO] {}: Skipping superseded video prepare task for session {} before player creation",
                name_str, session_id
            );
            return;
        }

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || match video::VideoPlayer::new(
                    &path_str,
                    name_arc,
                    session_id,
                    volume,
                    frame_tx_clone,
                ) {
                    Ok(mut vp) => {
                        vp.set_volume(volume);
                        let preroll_frame = match vp.prebuffer() {
                            Ok(frame) => frame,
                            Err(e) => {
                                debug!(
                                    "[VIDEO] {}: Pre-buffering failed (non-fatal): {}",
                                    name_str, e
                                );
                                None
                            }
                        };
                        Ok((vp, preroll_frame))
                    }
                    Err(e) => {
                        error!("[VIDEO] {}: Failed to create video player: {}", name_str, e);
                        Err(e)
                    }
                },
            ));

        match result {
            Ok(Ok((mut vp, preroll_frame))) => {
                if !pending_video_session_matches(&pending_video_sessions, &name_str, session_id) {
                    debug!(
                        "[VIDEO] {}: Discarding superseded prepared player for session {}",
                        name_str, session_id
                    );
                    let _ = vp.stop();
                    return;
                }
                if let Err(e) = player_tx_clone.send(VideoPlayerResult::Success(
                    name_str,
                    session_id,
                    vp,
                    preroll_frame,
                )) {
                    error!("[VIDEO] Failed to send video player back: {}", e);
                }
            }
            Ok(Err(_)) | Err(_) => {
                if result.is_err() {
                    error!("[VIDEO] {}: Video player task panicked!", name_for_panic);
                }
                let _ = player_tx_panic
                    .send(VideoPlayerResult::Failure(name_for_panic, session_id_panic));
            }
        }
    });
}

/// Handle an IPC command request.
#[allow(clippy::too_many_arguments)]
pub async fn handle_command(
    req: Request,
    monitor_manager: &mut monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
    pending_video_switches: &mut HashMap<String, PendingVideoSwitch>,
    pending_image_video_stops: &mut HashMap<String, video::VideoPlayer>,
    pending_video_sessions: &PendingVideoSessions,
    frame_tx: &tokio::sync::mpsc::Sender<FrameMsg>,
    image_tx: &tokio::sync::mpsc::Sender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    next_session_id: &mut u64,
    loop_start: Instant,
    shutdown_flag: &Arc<AtomicBool>,
) -> Response {
    match req {
        Request::QueryOutputs => {
            let outputs = renderers
                .iter()
                .map(|(n, r)| kaleidux_common::OutputInfo {
                    name: n.clone(),
                    width: r.config.width,
                    height: r.config.height,
                    current_wallpaper: monitor_manager
                        .outputs
                        .get(n)
                        .and_then(|o| o.current_path.as_ref().map(|p| p.display().to_string())),
                })
                .collect();
            Response::OutputInfo(outputs)
        }
        Request::Next { output } => {
            let changes = monitor_manager.handle_next(output);
            let batch = rand::random::<u64>();
            for (name, (path, content_type)) in changes {
                switch_wallpaper_content(
                    &name,
                    &path,
                    content_type,
                    next_session_id,
                    frame_tx,
                    monitor_manager,
                    renderers,
                    video_players,
                    pending_video_switches,
                    pending_image_video_stops,
                    pending_video_sessions,
                    Some(batch),
                    Some(loop_start),
                    image_tx,
                    player_tx,
                    "NEXT",
                );
            }
            Response::Ok
        }
        Request::Prev { output } => {
            let changes = monitor_manager.handle_prev(output);
            let batch = rand::random::<u64>();
            for (name, (path, content_type)) in changes {
                switch_wallpaper_content(
                    &name,
                    &path,
                    content_type,
                    next_session_id,
                    frame_tx,
                    monitor_manager,
                    renderers,
                    video_players,
                    pending_video_switches,
                    pending_image_video_stops,
                    pending_video_sessions,
                    Some(batch),
                    Some(loop_start),
                    image_tx,
                    player_tx,
                    "PREV",
                );
            }
            Response::Ok
        }
        Request::Kill => {
            shutdown_flag.store(true, Ordering::SeqCst);
            Response::Ok
        }
        Request::Playlist(cmd) => monitor_manager.handle_playlist_command(cmd),
        Request::Blacklist(cmd) => monitor_manager.handle_blacklist_command(cmd),
        Request::LoveitList => Response::LoveitList(monitor_manager.get_loveitlist()),
        Request::Love { path, multiplier } => monitor_manager
            .love_file(path, multiplier)
            .map(|_| Response::Ok)
            .unwrap_or_else(|e| Response::Error(e.to_string())),
        Request::Unlove { path } => monitor_manager
            .unlove_file(path)
            .map(|_| Response::Ok)
            .unwrap_or_else(|e| Response::Error(e.to_string())),
        Request::History { output } => Response::History(monitor_manager.get_history(output)),
        Request::Reload => {
            info!("Reloading configuration...");
            match orchestration::Config::load().await {
                Ok(new_config) => {
                    monitor_manager.update_config(new_config);
                    for (name, r) in renderers.iter_mut() {
                        if let Some(cfg) = monitor_manager.get_output_config(name) {
                            r.apply_config(cfg);
                        }
                    }
                    info!("Configuration reloaded successfully");
                    Response::Ok
                }
                Err(e) => {
                    error!("Failed to reload config: {}", e);
                    Response::Error(format!("Failed to reload config: {}", e))
                }
            }
        }
        Request::Pause => {
            info!("[CMD] Pausing all video players and wallpaper cycling");
            for (name, player) in video_players.iter() {
                if let Err(e) = player.pause() {
                    error!("[CMD] Failed to pause video for {}: {}", name, e);
                }
            }
            monitor_manager.set_paused(true);
            Response::Ok
        }
        Request::Resume => {
            info!("[CMD] Resuming all video players and wallpaper cycling");
            for (name, player) in video_players.iter() {
                if let Err(e) = player.resume() {
                    error!("[CMD] Failed to resume video for {}: {}", name, e);
                }
            }
            monitor_manager.set_paused(false);
            Response::Ok
        }
        Request::Stop => {
            info!("[CMD] Stopping all video players");
            let names: Vec<String> = video_players.keys().cloned().collect();
            for name in names {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                if let Some(player) = video_players.remove(&name) {
                    stop_video_player_in_background(name, player);
                }
            }
            Response::Ok
        }
        Request::Clear { output } => {
            info!("[CMD] Clearing output: {:?}", output);
            let targets: Vec<String> = match output {
                Some(ref name) => {
                    if renderers.contains_key(name) {
                        vec![name.clone()]
                    } else {
                        return Response::Error(format!("Output not found: {}", name));
                    }
                }
                None => renderers.keys().cloned().collect(),
            };
            for name in targets {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                if let Some(vp) = video_players.remove(&name) {
                    stop_video_player_in_background(name.clone(), vp);
                }
                if let Some(r) = renderers.get_mut(&name) {
                    r.clear();
                }
            }
            Response::Ok
        }
    }
}
