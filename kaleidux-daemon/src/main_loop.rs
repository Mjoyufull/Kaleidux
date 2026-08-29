//! Shared main loop context and helpers used by both Wayland and X11 backends.
//!
//! This module contains the `MainLoopContext` struct which owns all state shared
//! between backend loops, along with helper methods that deduplicate the
//! channel-drain, scheduling, command-handling, and housekeeping logic.

use crate::background;
use crate::cache;
use crate::content::sessions::{PendingVideoSessions, PendingVideoSwitch, VideoPlayerResult};
use crate::image::runtime_cache::SLOW_IMAGE_PREPARE_MS;
use crate::image::types::ImageLoadProfile;
use crate::metrics;
use crate::monitor;
use crate::monitor_manager;
use crate::orchestration;
use crate::queue;
use crate::renderer;
use crate::runtime::ipc::read_ipc_request_line;
use crate::runtime::startup_barrier::StartupPresentBarrier;
use crate::runtime::timing::duration_ms;
use crate::scripting;
use crate::video;

use kaleidux_common::{Request, Response};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tracing::{info, warn};

#[derive(Debug)]
pub struct LoadedImage {
    pub name: String,
    pub session_id: u64,
    pub data: Option<Arc<[u8]>>,
    pub width: u32,
    pub height: u32,
    pub profile: Option<ImageLoadProfile>,
    pub _path: PathBuf,
    pub(crate) _byte_permit: Option<crate::image::channel_budget::ImageChannelPermit>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingContentSwitch {
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    pub(crate) content_type: queue::ContentType,
    pub(crate) shared_image_target: Option<(u32, u32)>,
    pub(crate) target_width: u32,
    pub(crate) target_height: u32,
    pub(crate) target_area: u64,
}

/// Type aliases to reduce verbosity in signatures
pub type CmdMsg = (Request, tokio::sync::oneshot::Sender<Response>);
pub type PlayerEventMsg = video::PlayerEvent;
pub type CommandSender = tokio::sync::mpsc::Sender<CmdMsg>;
pub type PlayerReadySender = tokio::sync::mpsc::Sender<VideoPlayerResult>;
pub type PlayerEventSender = tokio::sync::mpsc::Sender<PlayerEventMsg>;

const COMMAND_CHANNEL_CAPACITY: usize = 64;
const PLAYER_READY_CHANNEL_CAPACITY: usize = 32;
const PLAYER_EVENT_CHANNEL_CAPACITY: usize = 128;
const IPC_CONCURRENCY_LIMIT: usize = 32;
const IPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const IPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct CommandContext<'a> {
    pub(crate) monitor_manager: &'a mut monitor_manager::MonitorManager,
    pub(crate) renderers: &'a mut HashMap<String, renderer::Renderer>,
    pub(crate) video_players: &'a mut HashMap<String, video::VideoPlayer>,
    pub(crate) pending_video_switches: &'a mut HashMap<String, PendingVideoSwitch>,
    pub(crate) pending_image_video_stops: &'a mut HashMap<String, video::VideoPlayer>,
    pub(crate) pending_video_sessions: &'a PendingVideoSessions,
    pub(crate) metrics: &'a Arc<metrics::PerformanceMetrics>,
    pub(crate) frame_mailbox: &'a video::LatestFrameMailbox,
    pub(crate) image_tx: &'a tokio::sync::mpsc::Sender<LoadedImage>,
    pub(crate) player_tx: &'a PlayerReadySender,
    pub(crate) player_event_tx: &'a PlayerEventSender,
    pub(crate) next_session_id: &'a mut u64,
    pub(crate) loop_start: Instant,
    pub(crate) shutdown_flag: &'a Arc<AtomicBool>,
    pub(crate) display_power_suspended: bool,
    pub(crate) mpv_native_targets: Option<&'a HashMap<String, video::MpvNativeVideoTarget>>,
    pub(crate) mpv_composed_targets: Option<&'a HashMap<String, video::MpvComposedVideoTarget>>,
}

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
    pub mpv_native_targets: HashMap<String, video::MpvNativeVideoTarget>,
    pub mpv_composed_targets: HashMap<String, video::MpvComposedVideoTarget>,
    pub startup_present_barrier: Option<StartupPresentBarrier>,
    pub latest_video_frames: video::LatestFrameMailbox,

    pub cmd_rx: tokio::sync::mpsc::Receiver<CmdMsg>,
    pub image_rx: tokio::sync::mpsc::Receiver<LoadedImage>,
    pub image_tx: tokio::sync::mpsc::Sender<LoadedImage>,
    pub player_rx: tokio::sync::mpsc::Receiver<VideoPlayerResult>,
    pub player_tx: PlayerReadySender,
    pub player_event_rx: tokio::sync::mpsc::Receiver<PlayerEventMsg>,
    pub player_event_tx: PlayerEventSender,

    pub dir_watcher: Option<cache::DirectoryWatcher>,
    pub script_manager: scripting::ScriptManager,
    pub shutdown_flag: Arc<AtomicBool>,
    pub(crate) display_power_suspended: bool,

    pub next_session_id: u64,
    pub first_frame_recorded: bool,
    pub last_metrics_log: Instant,
    pub last_stats_flush: Instant,
    pub last_pool_cleanup: Instant,
    pub last_device_poll: Instant,
    pub(crate) last_loop_rate_counts: (u64, u64),
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
        let metrics = Arc::new(metrics::PerformanceMetrics::new_with_diagnostics(
            tracing::enabled!(tracing::Level::INFO),
        ));
        metrics.record_startup_start();
        metrics.record_gstreamer_init(gstreamer_duration);

        // Detailed process/GPU sampling is diagnostic work. Do not retain the
        // sysinfo/NVML task or wake every ten seconds when INFO is disabled.
        if tracing::enabled!(tracing::Level::INFO) {
            let sys_monitor = monitor::SystemMonitor::new_with_metrics(Some(metrics.clone()));
            tokio::spawn(async move {
                sys_monitor.run().await;
            });
        }

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
        let latest_video_frames = video::LatestFrameMailbox::new();
        // Image channel: bounded to prevent memory spikes from large images accumulating
        let (image_tx, image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
        let (player_tx, player_rx) =
            tokio::sync::mpsc::channel::<VideoPlayerResult>(PLAYER_READY_CHANNEL_CAPACITY);
        let (player_event_tx, player_event_rx) =
            tokio::sync::mpsc::channel::<PlayerEventMsg>(PLAYER_EVENT_CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<CmdMsg>(COMMAND_CHANNEL_CAPACITY);

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
        let ipc_permits = Arc::new(tokio::sync::Semaphore::new(IPC_CONCURRENCY_LIMIT));
        tokio::spawn(async move {
            loop {
                let permit = match ipc_permits.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                match listener.accept().await {
                    Ok((mut stream, _)) => {
                        let cmd_tx = cmd_tx_clone.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            const MAX_MESSAGE_SIZE: usize = 8192;
                            let Ok(Some(req_str)) = tokio::time::timeout(
                                IPC_REQUEST_TIMEOUT,
                                read_ipc_request_line(&mut stream, MAX_MESSAGE_SIZE),
                            )
                            .await
                            else {
                                warn!("[IPC] Request timed out or was invalid");
                                return;
                            };
                            let Ok(req) = serde_json::from_str::<Request>(req_str.trim()) else {
                                warn!("[IPC] Failed to parse control request JSON");
                                return;
                            };
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            let response = match cmd_tx.try_send((req, resp_tx)) {
                                Ok(()) => tokio::time::timeout(IPC_RESPONSE_TIMEOUT, resp_rx)
                                    .await
                                    .ok()
                                    .and_then(Result::ok),
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    Some(Response::Error(
                                        "control queue busy; retry shortly".to_string(),
                                    ))
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    Some(Response::Error("daemon is shutting down".to_string()))
                                }
                            };
                            if let Some(response) = response
                                && let Ok(json) = serde_json::to_string(&response)
                            {
                                let _ = stream.write_all(json.as_bytes()).await;
                            }
                        });
                    }
                    Err(error) => {
                        drop(permit);
                        warn!("[IPC] Failed to accept control connection: {}", error);
                    }
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
            mpv_native_targets: HashMap::new(),
            mpv_composed_targets: HashMap::new(),
            startup_present_barrier: None,
            latest_video_frames,
            cmd_rx,
            image_rx,
            image_tx,
            player_rx,
            player_tx,
            player_event_rx,
            player_event_tx,
            dir_watcher,
            script_manager,
            shutdown_flag,
            display_power_suspended: false,
            next_session_id: 1,
            first_frame_recorded: false,
            last_metrics_log: now,
            last_stats_flush: now,
            last_pool_cleanup: now,
            last_device_poll: now,
            last_loop_rate_counts: (0, 0),
            last_script_tick: now,
            script_tick_interval,
            target_frame_time: std::time::Duration::from_micros(16667), // ~60 FPS
        })
    }

    // ─── Idle wait ──────────────────────────────────────────────────────

    // ─── Channel draining ───────────────────────────────────────────────

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

    // ─── Housekeeping ───────────────────────────────────────────────────

    /// Record frame time, clean up texture pool, flush stats, process dir watcher,
    /// log metrics summary. Called at the end of each loop iteration.
    /// Sleep at vsync rate if actively rendering and periodically service WGPU
    /// resource retirement without returning to an unconditional per-loop poll.
    pub async fn timing_and_poll(&mut self, any_active: bool, loop_start: Instant) {
        if let Some(ctx) = &self.wgpu_ctx {
            let requested = ctx.poll_device_if_requested();
            if requested || self.last_device_poll.elapsed() >= Duration::from_millis(100) {
                if !requested {
                    ctx.device.poll(wgpu::Maintain::Poll);
                }
                self.last_device_poll = Instant::now();
            }
        }
        if !any_active {
            return;
        }
        let elapsed = loop_start.elapsed();
        if elapsed < self.target_frame_time {
            tokio::time::sleep(self.target_frame_time - elapsed).await;
        }
    }

    /// Perform the initial content load.
    /// Calls `monitor_manager.tick()` and dispatches initial wallpaper content.
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
        // Drop every target retaining WGPU/EGL state while the backend connection
        // is still alive. Some EGL destructors may marshal Wayland requests.
        self.mpv_native_targets.clear();
        self.mpv_composed_targets.clear();
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

pub(crate) fn log_slow_image_prepare(
    output_name: &str,
    profile: &ImageLoadProfile,
    upload_duration: Duration,
    metrics: &metrics::PerformanceMetrics,
) {
    let total = profile.total_duration() + upload_duration;
    if duration_ms(total) < SLOW_IMAGE_PREPARE_MS {
        return;
    }

    metrics.record_image_slow_prepare();
    let stages = [
        ("wait", profile.permit_wait),
        ("decode", profile.decode),
        ("convert", profile.convert),
        ("resize", profile.resize),
        ("expand", profile.expand),
        ("upload", upload_duration),
    ];
    let (worst_stage, worst_duration) = stages
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1))
        .unwrap_or(("unknown", Duration::ZERO));
    warn!(
        "[IMAGE-CACHE] slow_prepare output={} total={:.1}ms worst={}:{:.1}ms format={} source={}x{} target_filter={} stages=wait:{:.1},decode:{:.1},convert:{:.1},resize:{:.1},expand:{:.1},upload:{:.1}",
        output_name,
        duration_ms(total),
        worst_stage,
        duration_ms(worst_duration),
        profile.format,
        profile.source_width,
        profile.source_height,
        profile.resize_filter.as_deref().unwrap_or("none"),
        duration_ms(profile.permit_wait),
        duration_ms(profile.decode),
        duration_ms(profile.convert),
        duration_ms(profile.resize),
        duration_ms(profile.expand),
        duration_ms(upload_duration)
    );
}
