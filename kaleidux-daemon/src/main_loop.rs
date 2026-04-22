//! Shared main loop context and helpers used by both Wayland and X11 backends.
//!
//! This module contains the `MainLoopContext` struct which owns all state shared
//! between backend loops, along with helper methods that deduplicate the
//! channel-drain, scheduling, command-handling, and housekeeping logic.

use crate::background::{self, BackgroundWorkKind};
use crate::cache;
use crate::metrics;
use crate::monitor;
use crate::monitor_manager;
use crate::orchestration;
use crate::queue;
use crate::renderer;
use crate::scripting;
use crate::video;

use gstreamer as gst;
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
use tracing::{debug, error, info, trace, warn};
use zune_core::bytestream::ZCursor;
use zune_core::colorspace::ColorSpace;
use zune_core::options::DecoderOptions;

// Global semaphore to limit concurrent image decode tasks (prevents memory spikes)
// Limit to 2 concurrent decodes since each can be 35-40MB
static IMAGE_DECODE_SEMAPHORE: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(2)));

async fn read_ipc_request_line(
    stream: &mut tokio::net::UnixStream,
    max_message_size: usize,
) -> Option<String> {
    let mut message = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        match stream.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                let chunk = &chunk[..n];
                let bytes_to_take = chunk.iter().position(|&b| b == b'\n').unwrap_or(n);
                if message.len() + bytes_to_take > max_message_size {
                    warn!(
                        "[IPC] Dropping oversized request (>{} bytes) from control socket",
                        max_message_size
                    );
                    return None;
                }
                message.extend_from_slice(&chunk[..bytes_to_take]);
                if bytes_to_take != n {
                    break;
                }
            }
            Err(e) => {
                warn!("[IPC] Failed reading request from control socket: {}", e);
                return None;
            }
        }
    }

    if message.is_empty() {
        return None;
    }

    match String::from_utf8(message) {
        Ok(message) => Some(message),
        Err(e) => {
            warn!("[IPC] Received non-UTF8 request on control socket: {}", e);
            None
        }
    }
}
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
        assert_eq!(
            compute_upload_downscale_dimensions(1280, 720, 1920, 1080),
            None
        );
    }

    #[test]
    fn upload_resize_downscales_only_oversized_sources() {
        assert_eq!(
            compute_upload_downscale_dimensions(
                MAX_IMAGE_UPLOAD_DIMENSION * 2,
                4000,
                MAX_IMAGE_UPLOAD_DIMENSION * 2,
                4000,
            ),
            Some((MAX_IMAGE_UPLOAD_DIMENSION, 2000))
        );
    }

    #[test]
    fn cover_target_downscales_to_minimum_cover_size() {
        assert_eq!(
            compute_upload_downscale_dimensions(6000, 4000, 1920, 1080),
            Some((1920, 1280))
        );
        assert_eq!(
            compute_upload_downscale_dimensions(3000, 4500, 1920, 1080),
            Some((1920, 2880))
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
        let rgb = vec![64; 4 * 3];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 4, 1, 1, 1).expect("rgb prep should succeed");

        assert_eq!((width, height), (4, 1));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(rgba.len(), 4 * 4);
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
    fn prepared_image_cache_is_scoped_to_output_target() {
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
        assert!(try_load_prepared_image_cache(&source_path, 1366, 768).is_none());

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1920, 1080) {
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

    #[test]
    fn video_frames_are_rejected_for_non_video_outputs() {
        assert!(!should_accept_video_frame(
            queue::ContentType::Image,
            42,
            42
        ));
    }

    #[test]
    fn video_frames_are_rejected_for_stale_sessions() {
        assert!(!should_accept_video_frame(
            queue::ContentType::Video,
            42,
            41
        ));
    }

    #[test]
    fn video_frames_are_accepted_for_active_sessions() {
        assert!(should_accept_video_frame(queue::ContentType::Video, 42, 42));
    }

    #[test]
    fn next_idle_wake_prefers_pending_switch_deadline() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);
        let switch_deadline = now + Duration::from_millis(250);

        assert_eq!(
            next_idle_wake_deadline(now, periodic, Some(switch_deadline)),
            switch_deadline
        );
    }

    #[test]
    fn next_idle_wake_falls_back_to_periodic_when_switch_is_later() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);
        let switch_deadline = now + Duration::from_secs(3);

        assert_eq!(
            next_idle_wake_deadline(now, periodic, Some(switch_deadline)),
            now + periodic
        );
    }

    #[test]
    fn next_idle_wake_uses_periodic_when_no_switch_is_pending() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);

        assert_eq!(next_idle_wake_deadline(now, periodic, None), now + periodic);
    }

    #[test]
    fn startup_barrier_releases_after_bounded_skew() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: Some(now),
            release_reason: None,
            outputs: HashMap::from([
                (
                    String::from("DP-2"),
                    StartupOutputState {
                        phase: StartupOutputPhase::Ready,
                        first_ready_at: Some(now),
                        first_present_at: None,
                        retry_count: 0,
                        can_block: true,
                        failed_paths: HashSet::new(),
                    },
                ),
                (String::from("DP-3"), StartupOutputState::pending()),
            ]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + Duration::from_millis(100)),
            None
        );
        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + STARTUP_BARRIER_SKEW_RELEASE),
            Some("bounded_skew")
        );
    }

    #[test]
    fn startup_barrier_releases_failed_outputs_without_waiting() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: HashMap::from([(
                String::from("DP-2"),
                StartupOutputState {
                    phase: StartupOutputPhase::Failed,
                    first_ready_at: None,
                    first_present_at: None,
                    retry_count: STARTUP_RETRY_LIMIT,
                    can_block: false,
                    failed_paths: HashSet::new(),
                },
            )]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now),
            Some("failed_outputs")
        );
    }

    #[test]
    fn startup_barrier_times_out_after_one_second() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: HashMap::from([(String::from("DP-2"), StartupOutputState::pending())]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + STARTUP_BARRIER_TIMEOUT),
            Some("timeout")
        );
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

const STARTUP_BARRIER_SKEW_RELEASE: Duration = Duration::from_millis(150);
const STARTUP_BARRIER_TIMEOUT: Duration = Duration::from_millis(1000);
const STARTUP_RETRY_LIMIT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupOutputPhase {
    Pending,
    Ready,
    Failed,
    Presented,
}

#[derive(Debug, Clone)]
pub struct StartupOutputState {
    pub phase: StartupOutputPhase,
    pub first_ready_at: Option<Instant>,
    pub first_present_at: Option<Instant>,
    pub retry_count: u8,
    pub can_block: bool,
    pub failed_paths: HashSet<PathBuf>,
}

impl StartupOutputState {
    fn pending() -> Self {
        Self {
            phase: StartupOutputPhase::Pending,
            first_ready_at: None,
            first_present_at: None,
            retry_count: 0,
            can_block: true,
            failed_paths: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupPresentBarrier {
    pub batch_id: u64,
    pub armed_at: Instant,
    pub first_ready_at: Option<Instant>,
    pub release_reason: Option<&'static str>,
    pub outputs: HashMap<String, StartupOutputState>,
}

pub(crate) fn stop_video_player_in_background(name: String, mut player: video::VideoPlayer) {
    let _ = player.request_stop();
    if let Some(handle) =
        background::spawn_blocking_tracked(BackgroundWorkKind::PlayerStop, move || {
            debug!("[VIDEO] {}: Finalizing player stop on blocking pool", name);
            let _ = player.stop();
        })
    {
        drop(handle);
    }
}

fn should_accept_video_frame(
    valid_content_type: queue::ContentType,
    active_video_session_id: u64,
    frame_session_id: u64,
) -> bool {
    valid_content_type == queue::ContentType::Video
        && active_video_session_id != 0
        && active_video_session_id == frame_session_id
}

fn next_idle_wake_deadline(
    now: Instant,
    periodic_interval: Duration,
    switch_deadline: Option<Instant>,
) -> Instant {
    let periodic_deadline = now + periodic_interval;
    switch_deadline
        .map(|deadline| deadline.min(periodic_deadline))
        .unwrap_or(periodic_deadline)
}

fn min_optional_deadline(current: Option<Instant>, candidate: Option<Instant>) -> Option<Instant> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn startup_barrier_counts(barrier: &StartupPresentBarrier) -> (usize, usize, usize) {
    let mut pending = 0usize;
    let mut ready = 0usize;
    let mut failed = 0usize;

    for state in barrier.outputs.values() {
        match state.phase {
            StartupOutputPhase::Pending if state.can_block => pending += 1,
            StartupOutputPhase::Ready | StartupOutputPhase::Presented if state.can_block => {
                ready += 1;
            }
            StartupOutputPhase::Failed => failed += 1,
            _ => {}
        }
    }

    (pending, ready, failed)
}

fn startup_barrier_release_candidate(
    barrier: &StartupPresentBarrier,
    now: Instant,
) -> Option<&'static str> {
    if let Some(reason) = barrier.release_reason {
        return Some(reason);
    }

    let (pending, _ready, failed) = startup_barrier_counts(barrier);
    if pending == 0 {
        return Some(if failed > 0 {
            "failed_outputs"
        } else {
            "all_ready"
        });
    }
    if let Some(first_ready_at) = barrier.first_ready_at
        && now >= first_ready_at + STARTUP_BARRIER_SKEW_RELEASE
    {
        return Some("bounded_skew");
    }
    if now >= barrier.armed_at + STARTUP_BARRIER_TIMEOUT {
        return Some("timeout");
    }

    None
}

fn startup_barrier_next_deadline(barrier: &StartupPresentBarrier, now: Instant) -> Option<Instant> {
    if startup_barrier_release_candidate(barrier, now).is_some() {
        return Some(now);
    }

    let mut deadline = Some(barrier.armed_at + STARTUP_BARRIER_TIMEOUT);
    if let Some(first_ready_at) = barrier.first_ready_at {
        deadline = min_optional_deadline(
            deadline,
            Some(first_ready_at + STARTUP_BARRIER_SKEW_RELEASE),
        );
    }
    deadline
}

fn startup_barrier_is_terminal(barrier: &StartupPresentBarrier) -> bool {
    barrier.outputs.values().all(|state| match state.phase {
        StartupOutputPhase::Presented => true,
        StartupOutputPhase::Failed => !state.can_block && state.retry_count >= STARTUP_RETRY_LIMIT,
        StartupOutputPhase::Pending | StartupOutputPhase::Ready => false,
    })
}

/// Type aliases to reduce verbosity in signatures
pub type CmdMsg = (Request, tokio::sync::oneshot::Sender<Response>);
pub type FrameMsg = video::FrameSignal;
pub type PlayerEventMsg = video::PlayerEvent;

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
    pub latest_video_frames: video::LatestFrameMailbox,

    pub cmd_rx: tokio::sync::mpsc::UnboundedReceiver<CmdMsg>,
    pub frame_rx: tokio::sync::mpsc::Receiver<FrameMsg>,
    pub frame_tx: tokio::sync::mpsc::Sender<FrameMsg>,
    pub image_rx: tokio::sync::mpsc::Receiver<LoadedImage>,
    pub image_tx: tokio::sync::mpsc::Sender<LoadedImage>,
    pub player_rx: tokio::sync::mpsc::UnboundedReceiver<VideoPlayerResult>,
    pub player_tx: tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    pub player_event_rx: tokio::sync::mpsc::UnboundedReceiver<PlayerEventMsg>,
    pub player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,

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
        let latest_video_frames = video::LatestFrameMailbox::new();
        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<FrameMsg>(32);
        // Image channel: bounded to prevent memory spikes from large images accumulating
        let (image_tx, image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
        let (player_tx, player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();
        let (player_event_tx, player_event_rx) =
            tokio::sync::mpsc::unbounded_channel::<PlayerEventMsg>();
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
                        let Some(req_str) =
                            read_ipc_request_line(&mut stream, MAX_MESSAGE_SIZE).await
                        else {
                            return;
                        };
                        let Ok(req) = serde_json::from_str::<Request>(req_str.trim()) else {
                            warn!("[IPC] Failed to parse control request JSON");
                            return;
                        };
                        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                        if cmd_tx.send((req, resp_tx)).is_ok()
                            && let Ok(response) = resp_rx.await
                            && let Ok(json) = serde_json::to_string(&response)
                        {
                            let _ = stream.write_all(json.as_bytes()).await;
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
            latest_video_frames,
            cmd_rx,
            frame_rx,
            frame_tx,
            image_rx,
            image_tx,
            player_rx,
            player_tx,
            player_event_rx,
            player_event_tx,
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
    }

    pub fn wayland_hot_loop_active(&self) -> bool {
        self.renderers
            .values()
            .any(renderer::Renderer::needs_wayland_immediate_work)
    }

    pub fn next_common_idle_deadline(&self, now: Instant) -> Option<Instant> {
        let mut deadline = self.monitor_manager.next_switch_deadline();
        let script_deadline =
            Some(self.last_script_tick + Duration::from_secs(self.script_tick_interval));
        deadline = min_optional_deadline(deadline, script_deadline);
        deadline = min_optional_deadline(
            deadline,
            self.startup_present_barrier
                .as_ref()
                .and_then(|barrier| startup_barrier_next_deadline(barrier, now)),
        );
        deadline
    }

    pub fn next_wayland_idle_deadline(&self, now: Instant) -> Option<Instant> {
        let mut deadline = self.next_common_idle_deadline(now);
        for renderer in self.renderers.values() {
            deadline = min_optional_deadline(
                deadline,
                renderer.next_wayland_retry_deadline(Duration::from_millis(500)),
            );
        }
        deadline
    }

    /// Idle-wait using `tokio::select!` until any event source fires.
    /// Returns buffered messages from whichever branch triggered.
    pub async fn idle_wait(
        &mut self,
        fd: &AsyncFd<RawFd>,
        wake_deadline: Option<Instant>,
    ) -> (
        Option<CmdMsg>,
        Option<FrameMsg>,
        Option<LoadedImage>,
        Option<VideoPlayerResult>,
        Option<PlayerEventMsg>,
    ) {
        let mut cmd_buf = None;
        let mut frame_buf = None;
        let mut image_buf = None;
        let mut player_buf = None;
        let mut player_event_buf = None;

        let now = Instant::now();
        if wake_deadline.is_some_and(|deadline| deadline <= now) {
            return (cmd_buf, frame_buf, image_buf, player_buf, player_event_buf);
        }
        let wake_deadline =
            next_idle_wake_deadline(now, std::time::Duration::from_secs(1), wake_deadline);
        let wake_deadline = tokio::time::Instant::from_std(wake_deadline);

        tokio::select! {
            cmd = self.cmd_rx.recv() => { if let Some(c) = cmd { cmd_buf = Some(c); } }
            frame = self.frame_rx.recv() => { if let Some(f) = frame { frame_buf = Some(f); } }
            image = self.image_rx.recv() => { if let Some(i) = image { image_buf = Some(i); } }
            player = self.player_rx.recv() => { if let Some(p) = player { player_buf = Some(p); } }
            player_event = self.player_event_rx.recv() => {
                if let Some(event) = player_event {
                    player_event_buf = Some(event);
                }
            }
            result = fd.readable() => {
                if let Ok(mut guard) = result {
                    guard.clear_ready();
                }
            }
            _ = tokio::time::sleep_until(wake_deadline) => {}
        }

        (cmd_buf, frame_buf, image_buf, player_buf, player_event_buf)
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
                    &self.latest_video_frames,
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
                    &self.player_event_tx,
                    &self.shutdown_flag,
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
                &self.latest_video_frames,
                &self.image_tx,
                &self.player_tx,
                &self.player_event_tx,
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
    ) -> (HashMap<String, video::VideoFrame>, usize, usize) {
        let mut latest_frames: HashMap<String, video::VideoFrame> = HashMap::new();
        let mut frames_received = 0;
        let mut frames_discarded = 0;
        let mut stale_session_discards = 0;
        let superseded_source_discards = self.latest_video_frames.take_overwrite_count() as usize;

        let mut handle_signal = |this: &mut Self, signal: FrameMsg| match signal {
            video::FrameSignal::Ready(source_id) => {
                let Some(frame) = this.latest_video_frames.take_frame(&source_id) else {
                    return;
                };
                frames_received += 1;
                let should_accept = this.renderers.get(source_id.as_str()).is_some_and(|r| {
                    should_accept_video_frame(
                        r.valid_content_type,
                        r.active_video_session_id,
                        frame.session_id,
                    )
                });
                if !should_accept {
                    frames_discarded += 1;
                    stale_session_discards += 1;
                } else {
                    latest_frames.insert(source_id, frame);
                }
            }
        };

        if let Some(signal) = frame_buf {
            handle_signal(self, signal);
        }
        while let Ok(signal) = self.frame_rx.try_recv() {
            handle_signal(self, signal);
        }

        // Track frame channel usage for memory leak detection
        if frames_received > 0 || superseded_source_discards > 0 {
            self.metrics
                .record_frame_channel_size(frames_received + self.latest_video_frames.occupancy());
            if frames_discarded > 0 || superseded_source_discards > 0 {
                trace!(
                    "[VIDEO] Discarded {} frames (stale_session={}, superseded_by_newer_same_source={})",
                    frames_discarded + superseded_source_discards,
                    stale_session_discards,
                    superseded_source_discards
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
            let mut startup_ready = false;
            let mut startup_failure_reason: Option<String> = None;
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
                    debug!(
                        "[IMAGE] Uploading image data for {}: {} bytes",
                        msg.name,
                        data.len()
                    );
                    let upload_start = Instant::now();
                    let _ = r.upload_image_data(data, msg.width, msg.height);
                    let upload_duration = upload_start.elapsed();
                    if let Some(profile) = &msg.profile {
                        self.metrics.record_image_stage_timings(
                            profile.permit_wait,
                            profile.decode,
                            profile.convert,
                            profile.resize,
                            profile.expand,
                            upload_duration,
                        );
                        debug!(
                            "[IMAGE] {}: prepared {} {}x{} -> {}x{} in {:.1}ms (wait {:.1}ms, decode {:.1}ms, convert {:.1}ms, resize {:.1}ms, expand {:.1}ms, upload {:.1}ms, filter={})",
                            msg.name,
                            profile.format,
                            profile.source_width,
                            profile.source_height,
                            msg.width,
                            msg.height,
                            duration_ms(profile.total_duration() + upload_duration),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.decode),
                            duration_ms(profile.convert),
                            duration_ms(profile.resize),
                            duration_ms(profile.expand),
                            duration_ms(upload_duration),
                            profile.resize_filter.as_deref().unwrap_or("none")
                        );
                    }
                    debug!(
                        "[IMAGE] Upload complete for {}: {:.1}ms",
                        msg.name,
                        duration_ms(upload_duration)
                    );
                    startup_ready = true;
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
                        self.mark_output_presented_if_ready(&msg.name);
                    }
                    release_pending_video = true;
                } else {
                    r.abort_transition();
                    startup_failure_reason = Some("image_decode_failed".to_string());
                    release_pending_video = true;
                }
            } else {
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
            }
            if startup_ready {
                self.mark_startup_output_ready(&msg.name, loop_start);
            }
            if let Some(reason) = startup_failure_reason {
                self.handle_startup_content_failure(&msg.name, &reason, loop_start);
            }
            if release_pending_video {
                self.release_pending_image_video_stop(&msg.name);
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

    pub fn mark_output_presented_if_ready(&mut self, name: &str) {
        let should_mark = self.renderers.get_mut(name).is_some_and(|renderer| {
            let ready = renderer.take_display_timer_ready();
            if ready {
                renderer.transition_just_completed = false;
            }
            ready
        });
        if should_mark {
            self.monitor_manager.mark_transition_completed(name);
            self.mark_startup_output_presented(name, Instant::now());
            self.maybe_clear_startup_present_barrier();
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
                        let mut startup_ready = false;
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
                                startup_ready = true;
                                should_render = true;
                            }
                        } else {
                            stop_video_player_in_background(name, player);
                            continue;
                        }
                        if startup_ready {
                            self.mark_startup_output_ready(&name, loop_start);
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
                            self.handle_startup_content_failure(
                                &name,
                                &format!("player_start: {}", e),
                                loop_start,
                            );
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
                            }
                            self.mark_output_presented_if_ready(&name);
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
                            self.handle_startup_content_failure(
                                &name,
                                &format!("player_start: {}", e),
                                loop_start,
                            );
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
                    self.handle_startup_content_failure(&name, "player_prepare_failed", loop_start);
                }
            }
        }
    }

    pub fn drain_player_events(
        &mut self,
        player_event_buf: Option<PlayerEventMsg>,
        loop_start: Instant,
    ) {
        let mut events = Vec::new();
        if let Some(event) = player_event_buf {
            events.push(event);
        }
        while let Ok(event) = self.player_event_rx.try_recv() {
            events.push(event);
        }

        for event in events {
            let is_pending = self
                .pending_video_switches
                .get(&event.source_id)
                .is_some_and(|pending| pending.session_id == event.session_id);
            let is_active = self
                .renderers
                .get(&event.source_id)
                .is_some_and(|renderer| renderer.active_video_session_id == event.session_id);

            if !is_pending && !is_active {
                debug!(
                    "[VIDEO] Ignoring stale player event {} session={} kind={:?} reason={}",
                    event.source_id, event.session_id, event.kind, event.reason
                );
                continue;
            }

            match event.kind {
                video::PlayerEventKind::Eos => {
                    debug!(
                        "[VIDEO] {} session={} reported EOS ({})",
                        event.source_id, event.session_id, event.reason
                    );
                }
                video::PlayerEventKind::Error | video::PlayerEventKind::FatalLifecycle => {
                    error!(
                        "[VIDEO] {} session={} runtime {:?}: {}",
                        event.source_id, event.session_id, event.kind, event.reason
                    );
                    self.metrics.record_error("video_runtime");

                    self.pending_video_switches.remove(&event.source_id);
                    set_pending_video_session(&self.pending_video_sessions, &event.source_id, None);

                    if let Some(player) = self.video_players.remove(&event.source_id) {
                        stop_video_player_in_background(event.source_id.clone(), player);
                    }

                    if let Some(renderer) = self.renderers.get_mut(&event.source_id) {
                        if renderer.active_video_session_id == event.session_id {
                            renderer.abort_transition();
                        }
                    }

                    self.handle_startup_content_failure(
                        &event.source_id,
                        &event.reason,
                        loop_start,
                    );
                }
            }
        }
    }

    fn reset_startup_output_pending(&mut self, name: &str) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };
        if state.can_block && state.phase != StartupOutputPhase::Presented {
            state.phase = StartupOutputPhase::Pending;
        }
    }

    fn handle_startup_content_failure(
        &mut self,
        name: &str,
        reason: &str,
        loop_start: Instant,
    ) -> bool {
        let tracked = self.startup_present_barrier.as_ref().and_then(|barrier| {
            barrier
                .outputs
                .get(name)
                .map(|state| (barrier.batch_id, state.phase))
        });
        let Some((batch_id, phase)) = tracked else {
            return false;
        };

        if phase == StartupOutputPhase::Presented {
            return false;
        }

        let failed_path = self
            .monitor_manager
            .outputs
            .get(name)
            .and_then(|orch| orch.current_path.clone());
        self.mark_startup_output_failed(name, reason, failed_path.as_deref());

        let retry_number = self
            .startup_present_barrier
            .as_mut()
            .and_then(|barrier| barrier.outputs.get_mut(name))
            .and_then(|state| {
                if state.retry_count >= STARTUP_RETRY_LIMIT {
                    None
                } else {
                    state.retry_count += 1;
                    Some(state.retry_count)
                }
            });

        let Some(retry_number) = retry_number else {
            warn!(
                "[STARTUP] {}: retries exhausted after failure ({})",
                name, reason
            );
            if let Some(state) = self
                .startup_present_barrier
                .as_mut()
                .and_then(|barrier| barrier.outputs.get_mut(name))
            {
                state.retry_count = STARTUP_RETRY_LIMIT;
            }
            self.maybe_clear_startup_present_barrier();
            return true;
        };

        let failed_paths = self
            .startup_present_barrier
            .as_ref()
            .and_then(|barrier| barrier.outputs.get(name))
            .map(|state| state.failed_paths.clone())
            .unwrap_or_default();
        let changes = self
            .monitor_manager
            .pick_startup_replacement(name, &failed_paths);

        if changes.is_empty() {
            warn!(
                "[STARTUP] {}: no replacement candidate after failure ({})",
                name, reason
            );
            if let Some(state) = self
                .startup_present_barrier
                .as_mut()
                .and_then(|barrier| barrier.outputs.get_mut(name))
            {
                state.retry_count = STARTUP_RETRY_LIMIT;
            }
            self.maybe_clear_startup_present_barrier();
            return true;
        }

        info!(
            "[STARTUP] {}: retry {}/{} after failure ({})",
            name, retry_number, STARTUP_RETRY_LIMIT, reason
        );

        for changed_name in changes.keys() {
            self.reset_startup_output_pending(changed_name);
        }

        for (changed_name, (path, content_type)) in changes {
            switch_wallpaper_content(
                &changed_name,
                &path,
                content_type,
                &mut self.next_session_id,
                &self.frame_tx,
                &self.latest_video_frames,
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
                &self.player_event_tx,
                &self.shutdown_flag,
                "STARTUP-RETRY",
            );
        }

        true
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

        for renderer in self.renderers.values_mut() {
            renderer.trim_idle_retained_resources();
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
                let active_video_players = self.video_players.len();
                let pending_video_stops = self.pending_image_video_stops.len();
                let pending_video_switches = self.pending_video_switches.len();
                let latest_frame_slots = self.latest_video_frames.occupancy();
                let background_snapshot = background::snapshot();
                let mut appsink_queue_levels = video::AppsinkQueueLevels::default();
                let mut appsink_queue_players = 0usize;
                for player in self.video_players.values() {
                    if let Some(levels) = player.appsink_queue_levels() {
                        appsink_queue_players += 1;
                        appsink_queue_levels.buffers =
                            appsink_queue_levels.buffers.saturating_add(levels.buffers);
                        appsink_queue_levels.bytes =
                            appsink_queue_levels.bytes.saturating_add(levels.bytes);
                        appsink_queue_levels.time_ns =
                            appsink_queue_levels.time_ns.saturating_add(levels.time_ns);
                    }
                }

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
                    "[MEMORY] Renderer retained textures: total={:.1}MB current={:.1}MB prev={:.1}MB composition={:.1}MB video_aux={:.1}MB pool={:.1}MB | video_players={} pending_switches={} pending_stops={} latest_frame_slots={} appsink={}q/{}b/{:.1}ms@{}p | background={} | {}",
                    to_mb(retained.total_bytes()),
                    to_mb(retained.current_bytes),
                    to_mb(retained.prev_bytes),
                    to_mb(retained.composition_bytes),
                    to_mb(retained.video_aux_bytes),
                    to_mb(texture_pool_bytes),
                    active_video_players,
                    pending_video_switches,
                    pending_video_stops,
                    latest_frame_slots,
                    appsink_queue_levels.buffers,
                    appsink_queue_levels.bytes,
                    appsink_queue_levels.time_ns as f64 / 1_000_000.0,
                    appsink_queue_players,
                    background_snapshot.format_compact(),
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
                &self.latest_video_frames,
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
                &self.player_event_tx,
                &self.shutdown_flag,
                "STARTUP",
            );
        }
        if startup_outputs.len() > 1 {
            self.arm_startup_present_barrier(batch_id, startup_outputs);
        }
    }

    pub fn arm_startup_present_barrier(&mut self, batch_id: u64, outputs: Vec<String>) {
        let output_states: HashMap<_, _> = outputs
            .into_iter()
            .filter(|name| {
                self.renderers
                    .get(name)
                    .is_some_and(|renderer| !renderer.has_any_content())
            })
            .map(|name| (name, StartupOutputState::pending()))
            .collect();
        if output_states.len() <= 1 {
            return;
        }

        let now = Instant::now();
        self.startup_present_barrier = Some(StartupPresentBarrier {
            batch_id,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: output_states,
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
        let Some(barrier) = &self.startup_present_barrier else {
            return false;
        };

        let Some(state) = barrier.outputs.get(name) else {
            return false;
        };

        if !state.can_block {
            return false;
        }

        startup_barrier_release_candidate(barrier, now).is_none()
    }

    pub fn release_startup_present_barrier<F>(&mut self, loop_start: Instant, mut render_fn: F)
    where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let Some(reason) = self
            .startup_present_barrier
            .as_ref()
            .and_then(|barrier| startup_barrier_release_candidate(barrier, loop_start))
        else {
            return;
        };

        if let Some(barrier) = self.startup_present_barrier.as_mut() {
            if barrier.release_reason.is_none() {
                let (pending, ready, failed) = startup_barrier_counts(barrier);
                barrier.release_reason = Some(reason);
                info!(
                    "[STARTUP] First-present barrier released for batch {:x} after {:.1}ms reason={} pending={} ready={} failed={}",
                    barrier.batch_id,
                    duration_ms(loop_start.saturating_duration_since(barrier.armed_at)),
                    reason,
                    pending,
                    ready,
                    failed
                );
            }
        }

        let outputs_to_release: Vec<String> = self
            .startup_present_barrier
            .as_ref()
            .map(|barrier| {
                barrier
                    .outputs
                    .iter()
                    .filter_map(|(name, state)| {
                        if state.can_block && state.phase == StartupOutputPhase::Ready {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        for name in outputs_to_release {
            if let Some(r) = self.renderers.get_mut(&name) {
                render_fn(r, &name, loop_start);
                if !self.first_frame_recorded {
                    self.metrics.record_first_frame();
                    self.first_frame_recorded = true;
                }
                self.mark_output_presented_if_ready(&name);
            }
        }

        self.maybe_clear_startup_present_barrier();
    }

    pub(crate) fn mark_startup_output_ready(&mut self, name: &str, now: Instant) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if state.phase == StartupOutputPhase::Presented {
            return;
        }

        if state.first_ready_at.is_none() {
            state.first_ready_at = Some(now);
            info!(
                "[STARTUP] {} first-ready {:.1}ms (batch {:x})",
                name,
                duration_ms(now.saturating_duration_since(barrier.armed_at)),
                barrier.batch_id
            );
        }

        if barrier.first_ready_at.is_none() {
            barrier.first_ready_at = Some(now);
        }

        state.phase = StartupOutputPhase::Ready;
    }

    fn mark_startup_output_failed(&mut self, name: &str, reason: &str, failed_path: Option<&Path>) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if let Some(path) = failed_path {
            state.failed_paths.insert(path.to_path_buf());
        }
        state.phase = StartupOutputPhase::Failed;
        state.can_block = false;

        info!(
            "[STARTUP] {} failed after {:.1}ms reason={} retries={} batch {:x}",
            name,
            duration_ms(Instant::now().saturating_duration_since(barrier.armed_at)),
            reason,
            state.retry_count,
            barrier.batch_id
        );
    }

    fn mark_startup_output_presented(&mut self, name: &str, now: Instant) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if state.first_present_at.is_none() {
            state.first_present_at = Some(now);
            info!(
                "[STARTUP] {} first-present {:.1}ms (batch {:x})",
                name,
                duration_ms(now.saturating_duration_since(barrier.armed_at)),
                barrier.batch_id
            );
        }

        state.phase = StartupOutputPhase::Presented;
        state.can_block = false;
    }

    fn maybe_clear_startup_present_barrier(&mut self) {
        if self
            .startup_present_barrier
            .as_ref()
            .is_some_and(startup_barrier_is_terminal)
        {
            self.startup_present_barrier = None;
        }
    }

    /// Clean shutdown — stop all video players, quiesce background work, and save caches.
    pub async fn shutdown(&mut self) {
        let shutdown_start = Instant::now();
        self.pending_video_switches.clear();
        if let Ok(mut sessions) = self.pending_video_sessions.lock() {
            sessions.clear();
        }

        background::close_global_work();

        let stop_players_start = Instant::now();
        for (_, mut player) in self.video_players.drain() {
            let _ = player.request_stop();
        }
        for (_, mut player) in self.pending_image_video_stops.drain() {
            let _ = player.request_stop();
        }
        let stop_players_duration = stop_players_start.elapsed();

        let background_wait_start = Instant::now();
        let background_quiet = background::wait_for_global_quiet(Duration::from_millis(250)).await;
        let background_wait_duration = background_wait_start.elapsed();

        let bus_shutdown_start = Instant::now();
        crate::video::shutdown_bus_dispatcher(Duration::from_millis(250));
        let bus_shutdown_duration = bus_shutdown_start.elapsed();

        let cache_start = Instant::now();
        // Drop renderer-owned wgpu surfaces while the backend connection still exists.
        self.renderers.clear();
        if let Some(ctx) = &self.wgpu_ctx {
            ctx.persist_pipeline_cache();
        }
        self.wgpu_ctx = None;
        // Persist WGSL cache to disk on shutdown (P-15 cache layer 1)
        if let Err(e) = crate::shaders::ShaderManager::save_cache() {
            warn!("[SHADER] Failed to save WGSL cache: {}", e);
        }

        info!(
            "[SHUTDOWN] stop_players={:.1}ms background_wait={:.1}ms background_quiet={} bus={:.1}ms caches={:.1}ms total={:.1}ms background={}",
            duration_ms(stop_players_duration),
            duration_ms(background_wait_duration),
            background_quiet,
            duration_ms(bus_shutdown_duration),
            duration_ms(cache_start.elapsed()),
            duration_ms(shutdown_start.elapsed()),
            background::snapshot().format_compact()
        );
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

const PREPARED_IMAGE_CACHE_MAGIC: &[u8; 8] = b"KDXIMG02";

fn prepared_image_cache_dir() -> Option<PathBuf> {
    let dir = dirs::cache_dir()?.join("kaleidux").join("prepared-images");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

fn prepared_image_cache_key(path: &Path, target_width: u32, target_height: u32) -> Option<String> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let modified = modified.duration_since(UNIX_EPOCH).ok()?;

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.as_os_str().as_encoded_bytes().hash(&mut hasher);
    meta.len().hash(&mut hasher);
    modified.as_secs().hash(&mut hasher);
    modified.subsec_nanos().hash(&mut hasher);
    target_width.hash(&mut hasher);
    target_height.hash(&mut hasher);
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

fn compute_cover_target_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    renderer::compute_cover_target_dimensions(
        source_width,
        source_height,
        target_width,
        target_height,
    )
}

fn apply_upload_dimension_clamp(source_width: u32, source_height: u32) -> Option<(u32, u32)> {
    if source_width <= MAX_IMAGE_UPLOAD_DIMENSION && source_height <= MAX_IMAGE_UPLOAD_DIMENSION {
        return None;
    }

    let longest_edge = source_width.max(source_height) as f32;
    let scale = MAX_IMAGE_UPLOAD_DIMENSION as f32 / longest_edge;
    let resized_width = ((source_width as f32 * scale).round() as u32).max(1);
    let resized_height = ((source_height as f32 * scale).round() as u32).max(1);
    Some((resized_width, resized_height))
}

fn compute_upload_downscale_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> Option<(u32, u32)> {
    let (cover_width, cover_height) =
        compute_cover_target_dimensions(source_width, source_height, target_width, target_height);
    let (prepared_width, prepared_height) = apply_upload_dimension_clamp(cover_width, cover_height)
        .unwrap_or((cover_width, cover_height));

    if prepared_width == source_width && prepared_height == source_height {
        None
    } else {
        Some((prepared_width, prepared_height))
    }
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
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (rgb_data, width, height) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
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
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Option<String>)> {
    if let Some((resized_width, resized_height)) = compute_upload_downscale_dimensions(
        source_width,
        source_height,
        target_width,
        target_height,
    ) {
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
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (luma_data, width, height) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
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
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (lumaa_data, width, height) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
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

    let (data, width, height, resize_duration, expand_duration, resize_filter, convert_duration) =
        if has_alpha {
            let convert_start = Instant::now();
            let rgba = image.into_rgba8().into_raw();
            let convert_duration = convert_start.elapsed();
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
                convert_duration,
            )
        } else {
            let convert_start = Instant::now();
            let rgb = image.into_rgb8().into_raw();
            let convert_duration = convert_start.elapsed();
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
                convert_duration,
            )
        };

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
    let cache_target = image::image_dimensions(path)
        .ok()
        .map(|(source_width, source_height)| {
            let (cover_width, cover_height) = compute_cover_target_dimensions(
                source_width,
                source_height,
                target_width,
                target_height,
            );
            apply_upload_dimension_clamp(cover_width, cover_height)
                .unwrap_or((cover_width, cover_height))
        });

    if let Some((cache_width, cache_height)) = cache_target {
        if let Some(payload) = try_load_prepared_image_cache(path, cache_width, cache_height) {
            return Ok(payload);
        }
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

    let (cache_width, cache_height) = cache_target.unwrap_or((payload.width, payload.height));
    store_prepared_image_cache(path, cache_width, cache_height, &payload);
    Ok(payload)
}

fn schedule_image_prefetch(name: &str, path: &Path, target_width: u32, target_height: u32) {
    let prefetch_path = path.to_path_buf();
    if !background::is_accepting_new_work() {
        return;
    }
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
        if !background::is_accepting_new_work() {
            if let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() {
                in_flight.remove(&prefetch_path);
            }
            return;
        }

        let _permit = match semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                if let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() {
                    in_flight.remove(&prefetch_path);
                }
                return;
            }
        };

        if !background::is_accepting_new_work() {
            if let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() {
                in_flight.remove(&prefetch_path);
            }
            return;
        }

        let decode_path = prefetch_path.clone();
        let Some(handle) =
            background::spawn_blocking_tracked(BackgroundWorkKind::ImagePrefetch, move || {
                decode_image_for_output(&decode_path, target_width, target_height)
            })
        else {
            if let Ok(mut in_flight) = IMAGE_PREFETCH_IN_FLIGHT.lock() {
                in_flight.remove(&prefetch_path);
            }
            return;
        };
        let result = handle.await;

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
    frame_mailbox: &video::LatestFrameMailbox,
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
    player_event_tx: &tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
    shutdown_flag: &Arc<AtomicBool>,
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

    frame_mailbox.clear_source(name);
    if let Some(old_pending_stop) = pending_image_video_stops.remove(name) {
        stop_video_player_in_background(name.to_string(), old_pending_stop);
    }
    let mut should_prepare_video = false;
    if let Some(r) = renderers.get_mut(name) {
        let resolved_transition = resolve_transition_for_output(monitor_manager, name);
        r.active_batch_id = batch_id;
        r.batch_start_time = batch_trigger_time;
        r.active_transition = resolved_transition.clone();

        if content_type == queue::ContentType::Image {
            let mut prior_video_player = video_players.remove(name);
            pending_video_switches.remove(name);
            set_pending_video_session(pending_video_sessions, name, None);
            r.set_content_type(content_type);
            r.active_image_session_id = session_id;
            r.active_video_session_id = 0;
            r.switch_content();
            let target_width = r.config.width;
            let target_height = r.config.height;

            let name_clone = name.to_string();
            let path_clone = path.to_path_buf();
            let tx = image_tx.clone();
            let semaphore = IMAGE_DECODE_SEMAPHORE.clone();
            let image_session_id = session_id;
            let shutdown_flag = shutdown_flag.clone();
            if let Some(old_video_player) = prior_video_player.take() {
                pending_image_video_stops.insert(name.to_string(), old_video_player);
            }

            debug!(
                "[ASSET] {}: Offloading image decode: {}",
                name,
                path.display()
            );
            tokio::spawn(async move {
                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Skipping image decode because shutdown is in progress",
                        name_clone
                    );
                    return;
                }

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
                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Image decode aborted before blocking task spawn",
                        name_clone
                    );
                    return;
                }
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
                let Some(handle) = background::spawn_blocking_tracked(
                    BackgroundWorkKind::ImageDecode,
                    move || decode_image_for_output(&path_for_decode, target_width, target_height),
                ) else {
                    debug!(
                        "[ASSET] {}: Image decode skipped because shutdown is in progress",
                        name_clone
                    );
                    return;
                };
                let decode_result = handle.await;

                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Discarding decoded image because shutdown is in progress",
                        name_clone
                    );
                    return;
                }

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
        if let Some(vp) = video_players.remove(name) {
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
            frame_mailbox,
            player_tx,
            player_event_tx,
            pending_video_sessions.clone(),
            shutdown_flag.clone(),
        );
    }

    if let Some(orchestrator) = monitor_manager.outputs.get(name) {
        if orchestrator.next_content_type == Some(queue::ContentType::Image) {
            if let Some(next_path) = orchestrator.next_path.as_ref() {
                if let Some(renderer) = renderers.get(name) {
                    schedule_image_prefetch(
                        name,
                        next_path,
                        renderer.config.width,
                        renderer.config.height,
                    );
                }
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
    frame_mailbox: &video::LatestFrameMailbox,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    player_event_tx: &tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
    pending_video_sessions: PendingVideoSessions,
    shutdown_flag: Arc<AtomicBool>,
) {
    let path_str = path.to_string_lossy().into_owned();
    let name_arc = Arc::new(name.to_string());
    let name_str = name.to_string();
    let frame_tx_clone = frame_tx.clone();
    let frame_mailbox_clone = frame_mailbox.clone();
    let player_tx_clone = player_tx.clone();
    let player_event_tx_clone = player_event_tx.clone();
    let Some(handle) = background::spawn_blocking_tracked(
        BackgroundWorkKind::VideoPrepare,
        move || {
            let name_for_panic = name_str.clone();
            let player_tx_panic = player_tx_clone.clone();
            let session_id_panic = session_id;
            let pending_video_sessions_for_task = pending_video_sessions.clone();
            let should_abort = || {
                shutdown_flag.load(Ordering::SeqCst)
                    || !pending_video_session_matches(
                        &pending_video_sessions_for_task,
                        &name_str,
                        session_id,
                    )
            };

            if should_abort() {
                debug!(
                    "[VIDEO] {}: Skipping superseded video prepare task for session {} before player creation",
                    name_str, session_id
                );
                return;
            }

            let prepare_start = Instant::now();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match video::VideoPlayer::new(
                    &path_str,
                    name_arc,
                    session_id,
                    volume,
                    frame_tx_clone,
                    frame_mailbox_clone,
                    player_event_tx_clone,
                ) {
                    Ok(mut vp) => {
                        let create_duration = prepare_start.elapsed();
                        vp.set_volume(volume);
                        if should_abort() {
                            let _ = vp.stop();
                            return Ok(None);
                        }
                        let prebuffer_start = Instant::now();
                        let prebuffer = match vp.prebuffer(should_abort) {
                            Ok(result) => result,
                            Err(e) => {
                                if should_abort() {
                                    debug!(
                                        "[VIDEO] {}: Aborting pre-buffer for superseded/shutdown session {}",
                                        name_str, session_id
                                    );
                                    let _ = vp.stop();
                                    return Ok(None);
                                }
                                debug!(
                                    "[VIDEO] {}: Pre-buffering failed (non-fatal): {}",
                                    name_str, e
                                );
                                video::VideoPrebufferResult {
                                    frame: None,
                                    profile: video::VideoPrebufferProfile {
                                        set_state: Duration::ZERO,
                                        state_wait: Duration::ZERO,
                                        pull_preroll: Duration::ZERO,
                                        set_state_result: "error",
                                        state_wait_settled: false,
                                        current_state: gst::State::Null,
                                        pending_state: gst::State::VoidPending,
                                    },
                                }
                            }
                        };
                        let prebuffer_duration = prebuffer_start.elapsed();
                        debug!(
                            "[VIDEO] {}: Player prepared in {:.1}ms (create {:.1}ms + prebuffer {:.1}ms, set_state {:.1}ms/{} + wait_state {:.1}ms settled={} current={:?} pending={:?} + pull_preroll {:.1}ms, preroll_frame={})",
                            name_str,
                            duration_ms(prepare_start.elapsed()),
                            duration_ms(create_duration),
                            duration_ms(prebuffer_duration),
                            duration_ms(prebuffer.profile.set_state),
                            prebuffer.profile.set_state_result,
                            duration_ms(prebuffer.profile.state_wait),
                            prebuffer.profile.state_wait_settled,
                            prebuffer.profile.current_state,
                            prebuffer.profile.pending_state,
                            duration_ms(prebuffer.profile.pull_preroll),
                            prebuffer.frame.is_some()
                        );
                        if should_abort() {
                            let _ = vp.stop();
                            Ok(None)
                        } else {
                            Ok(Some((vp, prebuffer.frame)))
                        }
                    }
                    Err(e) => {
                        error!("[VIDEO] {}: Failed to create video player: {}", name_str, e);
                        Err(e)
                    }
                }
            }));

            match result {
                Ok(Ok(Some((mut vp, preroll_frame)))) => {
                    if shutdown_flag.load(Ordering::SeqCst)
                        || !pending_video_session_matches(
                            &pending_video_sessions,
                            &name_str,
                            session_id,
                        )
                    {
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
                Ok(Ok(None)) => {}
                Ok(Err(_)) | Err(_) => {
                    if shutdown_flag.load(Ordering::SeqCst) {
                        return;
                    }
                    if result.is_err() {
                        error!("[VIDEO] {}: Video player task panicked!", name_for_panic);
                    }
                    let _ = player_tx_panic
                        .send(VideoPlayerResult::Failure(name_for_panic, session_id_panic));
                }
            }
        },
    ) else {
        debug!(
            "[VIDEO] {}: Skipping video prepare task because shutdown is in progress",
            name
        );
        return;
    };
    drop(handle);
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
    frame_mailbox: &video::LatestFrameMailbox,
    image_tx: &tokio::sync::mpsc::Sender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    player_event_tx: &tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
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
                    frame_mailbox,
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
                    player_event_tx,
                    shutdown_flag,
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
                    frame_mailbox,
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
                    player_event_tx,
                    shutdown_flag,
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
                frame_mailbox.clear_source(&name);
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
                frame_mailbox.clear_source(&name);
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
