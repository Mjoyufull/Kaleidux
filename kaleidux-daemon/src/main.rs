use kaleidux_common::{Request, Response, Transition};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt as subscriber_fmt;
use tracing_subscriber::{EnvFilter, Registry, prelude::*};
use wayland_client::{Connection, globals::registry_queue_init};
use x11rb::connection::Connection as X11Connection;

// Use jemalloc for better memory fragmentation handling in long-running processes
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Global semaphore to limit concurrent image decode tasks (prevents memory spikes)
// Limit to 2 concurrent decodes since each can be 35-40MB
static IMAGE_DECODE_SEMAPHORE: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(2)));

mod cache;
mod cuda_interop;
mod metrics;
mod monitor;
mod monitor_manager;
mod orchestration;
mod queue;
mod renderer;
mod scripting;
mod shaders;
mod video;
mod wayland;
mod x11;

use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone)]
struct LoadedImage {
    name: String,
    data: Option<Vec<u8>>,
    width: u32,
    height: u32,
    _path: PathBuf,
}

enum VideoPlayerResult {
    Success(String, u64, video::VideoPlayer),
    Failure(String, u64),
}

use chrono::Local;

struct CustomTimer;

impl tracing_subscriber::fmt::time::FormatTime for CustomTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Local::now();
        write!(w, "{}", now.format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

use clap::Parser;

/// Helper function to switch wallpaper content for an output.
#[allow(clippy::too_many_arguments)]
fn switch_wallpaper_content(
    name: &str,
    path: &Path,
    content_type: crate::queue::ContentType,
    next_session_id: &mut u64,
    frame_tx: &tokio::sync::mpsc::Sender<(Arc<String>, video::VideoEvent)>,
    monitor_manager: &monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
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

    let was_playing_video = video_players.contains_key(name);
    if was_playing_video {
        if let Some(mut vp) = video_players.remove(name) {
            debug!(
                "[TRANSITION] {}: Offloading video player stop to background",
                name
            );
            tokio::spawn(async move {
                let _ = vp.stop();
            });
        }
    }

    // CRITICAL: Ensure renderer exists before switching content
    // This prevents race conditions where content is switched before renderer is ready
    if let Some(r) = renderers.get_mut(name) {
        r.active_batch_id = batch_id;
        r.batch_start_time = batch_trigger_time;
        r.set_content_type(content_type);

        // Resolve Random transition if configured for this output
        if let Some(orchestrator) = monitor_manager.outputs.get(name) {
            if matches!(orchestrator.config.transition, Transition::Random) {
                let picked = Transition::pick_random();
                debug!(
                    "[TRANSITION] {}: Resolved Random transition to: {}",
                    name,
                    picked.name()
                );
                r.active_transition = picked;
            }
        }

        r.switch_content();

        if content_type == crate::queue::ContentType::Image {
            let name_clone = name.to_string();
            let path_clone = path.to_path_buf();
            let tx = image_tx.clone();
            let semaphore = IMAGE_DECODE_SEMAPHORE.clone();

            debug!(
                "[ASSET] {}: Offloading image decode: {}",
                name,
                path.display()
            );
            tokio::spawn(async move {
                // Acquire permit before decoding to limit concurrent tasks
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

                // Decode image in blocking task
                let decode_result =
                    tokio::task::spawn_blocking(move || match image::open(&path_clone) {
                        Ok(img) => {
                            let rgba = img.to_rgba8();
                            let (width, height) = rgba.dimensions();
                            let image_data = rgba.into_raw();
                            Ok((name_clone.clone(), image_data, width, height, path_clone))
                        }
                        Err(e) => {
                            error!("Failed to decode image {}: {}", path_clone.display(), e);
                            Err((name_clone, path_clone))
                        }
                    })
                    .await;

                // Send decoded image (or error) to channel
                match decode_result {
                    Ok(Ok((name, image_data, width, height, path))) => {
                        // Use send().await for bounded channel - may wait briefly if channel is full
                        if let Err(e) = tx
                            .send(LoadedImage {
                                name: name.clone(),
                                data: Some(image_data),
                                width,
                                height,
                                _path: path,
                            })
                            .await
                        {
                            debug!(
                                "[ASSET] {}: Failed to send decoded image (channel closed): {}",
                                name, e
                            );
                        }
                    }
                    Ok(Err((name, path))) => {
                        // Send error case - may wait briefly if channel is full
                        let _ = tx
                            .send(LoadedImage {
                                name,
                                data: None,
                                width: 0,
                                height: 0,
                                _path: path,
                            })
                            .await;
                    }
                    Err(e) => {
                        error!("Image decode task panicked: {}", e);
                    }
                }
                // _permit is dropped here, releasing the semaphore
            });
        }
    }

    if content_type == crate::queue::ContentType::Video {
        let session_id = *next_session_id;
        *next_session_id += 1;
        debug!(
            "[TRANSITION] {}: Starting new video player (session_id={})",
            name, session_id
        );
        create_and_start_video_player(
            path,
            name,
            session_id,
            frame_tx,
            monitor_manager,
            renderers,
            player_tx,
        );
    }
}

fn create_and_start_video_player(
    path: &Path,
    name: &str,
    session_id: u64,
    frame_tx: &tokio::sync::mpsc::Sender<(Arc<String>, video::VideoEvent)>,
    monitor_manager: &monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
) {
    if let Some(r) = renderers.get_mut(name) {
        r.active_video_session_id = session_id;
    }

    let path_str = path.to_string_lossy().into_owned();
    let name_arc = Arc::new(name.to_string());
    let name_str = name.to_string();
    let frame_tx_clone = frame_tx.clone();
    let player_tx_clone = player_tx.clone();

    let vol = monitor_manager
        .outputs
        .get(name)
        .map(|o| o.config.volume as f64 / 100.0)
        .unwrap_or(1.0);

    tokio::task::spawn_blocking(move || {
        let name_for_panic = name_str.clone();
        let player_tx_panic = player_tx_clone.clone();
        let session_id_panic = session_id;

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || match video::VideoPlayer::new(&path_str, name_arc, session_id, frame_tx_clone) {
                    Ok(mut vp) => {
                        vp.set_volume(vol);
                        if let Err(e) = vp.prebuffer() {
                            debug!(
                                "[VIDEO] {}: Pre-buffering failed (non-fatal): {}",
                                name_str, e
                            );
                        }
                        if let Err(e) = vp.start() {
                            error!("[VIDEO] {}: Failed to start video player: {}", name_str, e);
                            Err(e.into())
                        } else {
                            Ok(vp)
                        }
                    }
                    Err(e) => {
                        error!("[VIDEO] {}: Failed to create video player: {}", name_str, e);
                        Err(e)
                    }
                },
            ));

        match result {
            Ok(Ok(vp)) => {
                if let Err(e) =
                    player_tx_clone.send(VideoPlayerResult::Success(name_str, session_id, vp))
                {
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

#[derive(Parser, Debug)]
#[command(author, about, long_about = None)]
struct Args {
    #[arg(long)]
    demo: bool,
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=4))]
    log: Option<u8>,

    /// Force video decode mode: "cuda", "dmabuf", "nv12", or "rgba"
    #[arg(long, value_name = "MODE")]
    video_mode: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 1. Initialize Logging
    let log_level = args.log;
    let _guards = {
        let filter = match log_level {
            Some(1) => LevelFilter::WARN,
            Some(2) => LevelFilter::INFO,
            Some(3) => LevelFilter::DEBUG,
            Some(4) => LevelFilter::TRACE,
            None => LevelFilter::OFF,
            _ => LevelFilter::INFO,
        };

        let env_filter = EnvFilter::builder()
            .with_default_directive(filter.into())
            .from_env_lossy()
            .add_directive("wgpu_core=warn".parse().unwrap())
            .add_directive("wgpu_hal=warn".parse().unwrap())
            .add_directive("naga=warn".parse().unwrap())
            .add_directive("calloop=warn".parse().unwrap())
            .add_directive("smithay_client_toolkit=warn".parse().unwrap());

        if let Some(level) = log_level {
            let config_dir = dirs::config_dir()
                .ok_or_else(|| anyhow::anyhow!("Could not find config directory"))?
                .join("kaleidux")
                .join("logs");
            std::fs::create_dir_all(&config_dir)?;

            let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S");
            let log_path = config_dir.join(format!("kaleidux-daemon-{}.log", timestamp));
            let file = std::fs::File::create(&log_path)?;
            println!("Logging to file: {}", log_path.display());
            let (non_blocking_file, file_guard) = tracing_appender::non_blocking(file);
            let (non_blocking_stdout, stdout_guard) =
                tracing_appender::non_blocking(std::io::stdout());

            let file_layer = subscriber_fmt::layer()
                .with_writer(non_blocking_file)
                .with_ansi(false)
                .with_timer(CustomTimer);

            let stdout_layer = subscriber_fmt::layer()
                .with_writer(non_blocking_stdout)
                .with_timer(CustomTimer);

            Registry::default()
                .with(env_filter)
                .with(file_layer)
                .with(stdout_layer)
                .init();

            info!(
                "Kaleidux Daemon starting... (Level {}, File: {})",
                level,
                config_dir.display()
            );
            (Some(file_guard), Some(stdout_guard))
        } else {
            // Default: No logging initialized to improve performance
            (None, None)
        }
    };

    // 1b. Set video decode mode from CLI flag
    if let Some(ref mode_str) = args.video_mode {
        let mode = match mode_str.to_lowercase().as_str() {
            "cuda" | "nvdec" | "nvidia" => crate::video::VideoMode::ForceCuda,
            "dmabuf" | "dma-buf" | "zero-copy" => crate::video::VideoMode::ForceDmaBuf,
            "nv12" => crate::video::VideoMode::ForceNv12,
            "rgba" => crate::video::VideoMode::ForceRgba,
            "auto" => crate::video::VideoMode::Auto,
            other => {
                let msg = format!(
                    "ERROR: Unknown --video-mode '{}', valid: auto, cuda, dmabuf, nv12, rgba",
                    other
                );
                eprintln!("{}", msg);
                error!("{}", msg);
                std::process::exit(1);
            }
        };
        crate::video::set_video_mode(mode);
    }

    // 2. Load Configuration
    let mut config = match orchestration::Config::load().await {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("Failed to load configuration: {}. Using defaults.", e);
            orchestration::Config::default()
        }
    };

    if args.demo {
        info!("Demo mode enabled! Overriding configuration to use current directory...");
        let current_dir = std::env::current_dir()?;
        config.any.path = Some(current_dir);
        config.any.duration = Some(std::time::Duration::from_secs(10));
        config.global.video_ratio = Some(100); // 100% video for the demo file
        config.any.transition_time = Some(1500); // Nice 1.5s transitions
        config.any.transition = Some(Transition::Random); // Cycle through transitions
    }

    // 3. Initialize GStreamer
    let gstreamer_start = Instant::now();
    gstreamer::init()?;
    crate::video::configure_hw_decoders();
    let gstreamer_duration = gstreamer_start.elapsed();
    info!("GStreamer initialized.");

    // 4. Resource Monitor will be started in backend loops with metrics

    // Detect Backend
    let use_x11 = std::env::var("WAYLAND_DISPLAY").is_err() && std::env::var("DISPLAY").is_ok();

    if use_x11 {
        info!("Starting X11 Backend...");
        run_x11_loop(config, log_level, gstreamer_duration).await
    } else {
        info!("Starting Wayland Backend...");
        run_wayland_loop(config, log_level, gstreamer_duration).await
    }
}

async fn run_wayland_loop(
    config: orchestration::Config,
    log_level: Option<u8>,
    gstreamer_duration: std::time::Duration,
) -> anyhow::Result<()> {
    let script_path = config.global.script_path.clone();
    let script_tick_interval = config.global.script_tick_interval;
    let metrics = Arc::new(metrics::PerformanceMetrics::new());
    metrics.record_startup_start();
    metrics.record_gstreamer_init(gstreamer_duration);

    // Start resource monitor with metrics
    let monitor = monitor::SystemMonitor::new_with_metrics(Some(metrics.clone()));
    tokio::spawn(async move {
        monitor.run().await;
    });

    let mut monitor_manager =
        monitor_manager::MonitorManager::new_with_metrics(config.clone(), Some(metrics.clone()))?;
    let mut last_metrics_log = Instant::now();

    // Initialize directory watcher for cache invalidation
    let cache = monitor_manager.get_cache();
    // Directory watcher for cache invalidation (used in main loop)
    let mut dir_watcher = match cache::DirectoryWatcher::new(cache.clone()) {
        Ok(mut watcher) => {
            // Watch all content directories from config
            for output_config in config.outputs.values() {
                if let Some(path) = &output_config.path {
                    if let Err(e) = watcher.watch(path) {
                        tracing::warn!(
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
            tracing::warn!("[CACHE] Failed to create directory watcher: {}", e);
            None
        }
    };

    // Log metrics immediately for DEBUG (3) and TRACE (4) levels
    if log_level.map(|l| l >= 3).unwrap_or(false) {
        metrics.log_summary();
    }

    // Initialize Wayland
    let conn = Connection::connect_to_env()?;
    let (globals, mut event_queue) = registry_queue_init(&conn)?;
    let qh = event_queue.handle();
    let mut backend = wayland::WaylandBackend::new(&globals, &qh)?;

    event_queue.roundtrip(&mut backend)?;

    let mut wgpu_ctx: Option<Arc<renderer::WgpuContext>> = None;
    let mut initial_surface: Option<wgpu::Surface<'static>> = None;

    // Frame channel: increased capacity to 32 to cushion against micro-stutters
    // when multiple video sources are active. Memory cap is still reasonable (~1GB slack).
    let (frame_tx, mut frame_rx) =
        tokio::sync::mpsc::channel::<(Arc<String>, video::VideoEvent)>(32);
    let mut renderers = HashMap::new();
    let outputs: Vec<_> = backend.output_state.outputs().collect();

    let display_ptr = {
        let backend_ref = conn.backend();
        backend_ref.display_ptr() as *mut std::ffi::c_void
    };

    // Phase 1: Collect all output info first (fast, no IO)
    let mut output_infos: Vec<(
        String,
        String,
        wayland_client::protocol::wl_output::WlOutput,
    )> = Vec::new();
    for output in outputs {
        let info = match backend.output_state.info(&output) {
            Some(i) => i,
            None => continue,
        };
        let name = info.name.as_deref().unwrap_or("unknown").to_string();
        let description = info.description.as_deref().unwrap_or("unknown").to_string();
        info!("Found output: {} ({})", name, description);
        output_infos.push((name, description, output));
    }

    // Phase 2: Initialize all outputs sequentially but with shared file discovery cache.
    // The monitor_manager's add_output will reuse cached file lists for duplicate paths.
    for (name, description, _) in &output_infos {
        monitor_manager.add_output(name, description).await;
    }

    // Phase 3: Create Wayland surfaces (fast, no IO)
    let mut surface_infos = Vec::new();
    for (name, _description, output) in &output_infos {
        let output_config = match monitor_manager.get_output_config(name) {
            Some(cfg) => cfg,
            None => continue,
        };

        let layer_surface = backend.create_wallpaper_surface(
            output,
            &qh,
            name.clone(),
            output_config.layer.clone().into(),
        )?;

        let raw_handle_surface = wayland::RawHandleSurface {
            layer_surface,
            display_ptr,
        };
        let surface_arc = Arc::new(raw_handle_surface);
        surface_infos.push((name.clone(), surface_arc));
    }

    if let Some((_, first_surface_arc)) = surface_infos.first() {
        info!("Initializing WGPU context with first surface as compatible...");
        let wgpu_start = Instant::now();
        let (ctx, surface) = renderer::WgpuContext::with_surface(first_surface_arc.clone()).await?;
        let wgpu_duration = wgpu_start.elapsed();
        metrics.record_wgpu_init(wgpu_duration);
        let adapter_name = ctx.adapter.get_info().name.clone();
        wgpu_ctx = Some(ctx);
        initial_surface = Some(surface);
        info!("WGPU initialized on GPU: {:?}", adapter_name);
    }

    match wgpu_ctx.clone() {
        Some(ctx) => {
            let first_name = surface_infos.first().map(|(n, _)| n.clone());

            for (name, surface_arc) in surface_infos {
                let ctx_clone = ctx.clone();
                let is_first = Some(&name) == first_name.as_ref();
                let init_surf = if is_first {
                    initial_surface.take()
                } else {
                    None
                };

                let metrics_clone = metrics.clone();

                info!("[STARTUP] Initializing renderer for {}", name);

                // Offload WGPU surface creation to blocking thread to avoid checking generic runtime
                let name_for_bg = name.clone();
                let spawn_handler = tokio::task::spawn_blocking(move || {
                    renderer::Renderer::new(
                        name_for_bg,
                        ctx_clone,
                        surface_arc,
                        init_surf,
                        Some(metrics_clone),
                    )
                });

                // Set a timeout strictly for the initialization
                match tokio::time::timeout(std::time::Duration::from_secs(5), spawn_handler).await {
                    Ok(join_res) => match join_res {
                        Ok(render_res) => match render_res {
                            Ok(mut r) => {
                                if let Some(output_config) =
                                    monitor_manager.get_output_config(&name)
                                {
                                    r.apply_config(output_config);
                                }
                                renderers.insert(name.clone(), r);
                                info!("[STARTUP] Renderer initialized successfully for {}", name);
                            }
                            Err(e) => {
                                error!("Failed to create renderer for output {}: {}", name, e);
                                metrics.record_error("renderer_creation");
                            }
                        },
                        Err(e) => {
                            error!("Thread join error for output {}: {}", name, e);
                            metrics.record_error("renderer_thread_error");
                        }
                    },
                    Err(_) => {
                        // Timeout occurred
                        error!(
                            "TIMEOUT: Renderer initialization for {} took longer than 5s. Skipping.",
                            name
                        );
                        metrics.record_error("renderer_creation_timeout");
                    }
                }
                // Poll device to process submission/initialization commands
                ctx.device.poll(wgpu::Maintain::Poll);
            }
            // All renderers created - full initialization complete
            metrics.record_full_init();
            if log_level.map(|l| l >= 3).unwrap_or(false) {
                metrics.log_startup_summary();
            }
            info!(
                "[STARTUP] All renderers created, count: {}",
                renderers.len()
            );
        }
        _ => {
            warn!("[STARTUP] No WGPU context available, cannot create renderers!");
        }
    }

    info!("[STARTUP] Creating video players HashMap");
    let mut video_players: HashMap<String, video::VideoPlayer> = HashMap::new();

    let (cmd_tx, mut cmd_rx) =
        tokio::sync::mpsc::unbounded_channel::<(Request, tokio::sync::oneshot::Sender<Response>)>();
    // Image channel: bounded to prevent memory spikes from large images accumulating
    // Increased to 16 to prevent backpressure during batch transitions
    let (image_tx, mut image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
    let (player_tx, mut player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();
    let script_cmd_tx = cmd_tx.clone();

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

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown_flag.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        warn!("Received shutdown signal, cleaning up...");
        shutdown_clone.store(true, Ordering::SeqCst);
    });

    info!("[STARTUP] Creating script manager");
    let mut script_manager = scripting::ScriptManager::new(script_cmd_tx);
    if let Some(path) = &script_path {
        info!("[STARTUP] Loading script from: {:?}", path);
        let _ = script_manager.load(path).await;
    }
    let mut last_script_tick = Instant::now();
    info!("[STARTUP] Script manager initialized");

    let target_frame_time = std::time::Duration::from_micros(16667); // ~60 FPS
    let mut connection_error_count = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 3;
    let mut connection_dead = false;
    let mut last_error_time = Instant::now();
    let mut last_pool_cleanup = Instant::now();
    let mut last_stats_flush = Instant::now();
    let mut first_frame_recorded = false;

    // Initial Load
    info!(
        "[STARTUP] Reached Initial Load section, renderers count: {}",
        renderers.len()
    );

    // CRITICAL: Wait for all renderers to be configured before loading initial content
    // This prevents race conditions where content is sent before renderers are ready
    // Renderers become configured when they receive their first resize event from Wayland
    info!("[STARTUP] Waiting for all renderers to be configured...");
    let total_renderers = renderers.len();
    let wait_start = Instant::now();
    let mut configured_count = 0;
    const MAX_WAIT_TIME: std::time::Duration = std::time::Duration::from_secs(5);

    // Poll Wayland events until all renderers are configured or timeout
    while configured_count < total_renderers && wait_start.elapsed() < MAX_WAIT_TIME {
        // Process Wayland events to allow renderers to receive configure events
        match conn.prepare_read() {
            Some(guard) => {
                use std::os::unix::io::{AsFd, AsRawFd};
                let fd = conn.as_fd().as_raw_fd();
                let mut poll_fd = libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let timeout_ms = 10; // Shorter timeout for more responsive checking
                let ret = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
                if ret > 0 && (poll_fd.revents & libc::POLLIN) != 0 {
                    if let Err(e) = guard.read() {
                        error!("Failed to read Wayland events: {}", e);
                    }
                    if let Err(e) = event_queue.dispatch_pending(&mut backend) {
                        error!("Failed to dispatch Wayland events: {}", e);
                    }
                    // CRITICAL: Flush after dispatch to ensure compositor processes events
                    let _ = conn.flush();
                }
            }
            _ => {
                if let Err(e) = event_queue.dispatch_pending(&mut backend) {
                    error!("Failed to dispatch Wayland events: {}", e);
                }
                // CRITICAL: Flush even if no guard
                let _ = conn.flush();
            }
        }

        // CRITICAL FIX: Process pending_resizes to actually configure renderers
        // This is what was missing - configure events arrive but were never processed
        // The configure handler adds to pending_resizes, but we need to process them
        // to call resize_checked() which sets configured = true
        let resizes: Vec<_> = backend.pending_resizes.drain(..).collect();
        for (name, w, h, _) in resizes {
            if let Some(r) = renderers.get_mut(&name) {
                let width = if w == 0 { r.config.width } else { w };
                let height = if h == 0 { r.config.height } else { h };
                let _ = r.resize_checked(width, height);
                // resize_checked sets configured = true (renderer.rs:788)
            }
        }

        // Check how many renderers are configured
        configured_count = renderers.values().filter(|r| r.configured).count();
        if configured_count < total_renderers {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await; // Shorter sleep
        }
    }

    if configured_count < total_renderers {
        warn!(
            "[STARTUP] Only {}/{} renderers configured after {}ms timeout. Some wallpapers may not load initially.",
            configured_count,
            total_renderers,
            wait_start.elapsed().as_millis()
        );
        // Log which renderers are not configured
        for (name, r) in renderers.iter() {
            if !r.configured {
                warn!("[STARTUP] Renderer {} is not configured", name);
            }
        }
    } else {
        info!(
            "[STARTUP] All {} renderers configured in {:.2}ms",
            configured_count,
            wait_start.elapsed().as_secs_f64() * 1000.0
        );
    }

    // After timeout or all configured, force initial renders for any unconfigured renderers
    // This ensures surfaces are committed and can receive frame callbacks even if configure
    // events arrived late or were missed during the wait loop
    for (name, r) in renderers.iter_mut() {
        if !r.configured && r.config.width > 0 && r.config.height > 0 {
            // Use configured size if available
            if let Some(layer_surface) = backend.surfaces.get(name) {
                // Try to configure with existing size
                let _ = r.resize_checked(r.config.width, r.config.height);
                if r.configured {
                    // Force an initial render to commit the surface
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                        },
                        Instant::now(),
                    );
                    r.request_frame_callback(layer_surface, &qh);
                }
            }
        }
    }

    info!("[STARTUP] About to call monitor_manager.tick()");
    let initial_changes = monitor_manager.tick();
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
    let mut next_session_id = 1u64;
    let batch_id = rand::random::<u64>();
    for (name, (path, content_type)) in initial_changes {
        // Only check if renderer exists - don't skip if not configured
        // The main loop will handle configure events and trigger renders
        // This allows wallpapers to load even if configure events arrive late
        if !renderers.contains_key(&name) {
            warn!(
                "[STARTUP] Skipping initial content for {} - renderer does not exist",
                name
            );
            continue;
        }
        // Proceed even if not configured - main loop will handle it

        switch_wallpaper_content(
            &name,
            &path,
            content_type,
            &mut next_session_id,
            &frame_tx,
            &monitor_manager,
            &mut renderers,
            &mut video_players,
            Some(batch_id),
            None,
            &image_tx,
            &player_tx,
            "STARTUP",
        );
    }

    // Main Loop (Wayland)
    loop {
        let loop_start = Instant::now();
        if shutdown_flag.load(Ordering::SeqCst) {
            for player in video_players.values_mut() {
                let _ = player.stop();
            }
            break;
        }

        if connection_dead {
            if last_error_time.elapsed().as_secs() > 5 {
                connection_dead = false;
                connection_error_count = 0;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        }

        // Wayland Event Polling
        if let Some(guard) = conn.prepare_read() {
            use std::os::unix::io::{AsFd, AsRawFd};
            let fd = conn.as_fd().as_raw_fd();
            let mut poll_fd = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let ret = unsafe { libc::poll(&mut poll_fd, 1, 5) };
            if ret > 0 && (poll_fd.revents & libc::POLLIN != 0) {
                let _ = guard.read();
            }
        }

        if let Err(e) = event_queue.dispatch_pending(&mut backend) {
            let error_str = e.to_string();
            if error_str.contains("Broken pipe") {
                connection_error_count += 1;
                last_error_time = Instant::now();
                if connection_error_count >= MAX_CONSECUTIVE_ERRORS {
                    connection_dead = true;
                }
            }
        }

        if !connection_dead {
            let needs_flush = renderers
                .values()
                .any(|r| r.needs_redraw || r.transition_active);
            if needs_flush {
                let _ = conn.flush();
            }
        }

        // Logic (Common)
        {
            // Remove orphaned renderers
            let active_output_names: std::collections::HashSet<String> = backend
                .output_state
                .outputs()
                .filter_map(|o| backend.output_state.info(&o).and_then(|i| i.name.clone()))
                .collect();
            renderers.retain(|name, _| {
                if !active_output_names.contains(name) {
                    if let Some(mut vp) = video_players.remove(name) {
                        tokio::spawn(async move {
                            let _ = vp.stop();
                        });
                    }
                    false
                } else {
                    true
                }
            });

            // Handle Resizes
            let resizes: Vec<_> = backend.pending_resizes.drain(..).collect();
            for (name, w, h, _) in resizes {
                if let Some(r) = renderers.get_mut(&name) {
                    let width = if w == 0 { r.config.width } else { w };
                    let height = if h == 0 { r.config.height } else { h };
                    let _ = r.resize_checked(width, height);
                    if r.configured {
                        if let Some(layer_surface) = backend.surfaces.get(&name) {
                            // Force an initial render to commit the surface after resize
                            // This ensures the compositor knows the surface is ready and will send frame callbacks
                            let _ = r.render(
                                renderer::BackendContext::Wayland {
                                    surface: layer_surface,
                                    qh: &qh,
                                },
                                loop_start,
                            );
                            r.request_frame_callback(layer_surface, &qh);
                        }
                    }
                }
            }
        }

        // Automated Changes
        let scheduled_changes = monitor_manager.tick();
        if !scheduled_changes.is_empty() {
            let batch_id = rand::random::<u64>();
            for (name, (path, content_type)) in scheduled_changes {
                switch_wallpaper_content(
                    &name,
                    &path,
                    content_type,
                    &mut next_session_id,
                    &frame_tx,
                    &monitor_manager,
                    &mut renderers,
                    &mut video_players,
                    Some(batch_id),
                    Some(loop_start),
                    &image_tx,
                    &player_tx,
                    "SCHEDULED",
                );
            }
        }

        // Scripting
        if last_script_tick.elapsed().as_secs() >= script_tick_interval {
            script_manager.tick();
            last_script_tick = Instant::now();
        }

        // Handle Commands
        while let Ok((req, resp)) = cmd_rx.try_recv() {
            let response = handle_command(
                req,
                &mut monitor_manager,
                &mut renderers,
                &mut video_players,
                &frame_tx,
                &image_tx,
                &player_tx,
                &mut next_session_id,
                loop_start,
                &shutdown_flag,
            )
            .await;
            let _ = resp.send(response);
        }

        // Handle Frames
        // CRITICAL: Process ALL frames in channel, not just latest per source
        // This prevents frame accumulation and memory leaks
        // Keep only the latest frame per source for upload (discard older ones)
        let mut latest_frames: HashMap<Arc<String>, video::VideoFrame> = HashMap::new();
        let mut frames_received = 0;
        let mut frames_discarded = 0;
        while let Ok((source_id, event)) = frame_rx.try_recv() {
            frames_received += 1;
            match event {
                video::VideoEvent::Frame(frame) => {
                    // If we already have a frame for this source, drop the old one
                    // This ensures we only keep the latest frame per source
                    if latest_frames.insert(source_id.clone(), frame).is_some() {
                        frames_discarded += 1;
                    }
                }
                video::VideoEvent::Error(msg) => {
                    error!("Video error {}: {}", source_id, msg);
                    metrics.record_error("video_decode");
                }
            }
        }
        // Track frame channel usage for memory leak detection
        if frames_received > 0 {
            metrics.record_frame_channel_size(frames_received);
            if frames_discarded > 0 {
                debug!(
                    "[VIDEO] Discarded {} older frames (keeping latest per source)",
                    frames_discarded
                );
            }
        }
        // Process all frames (one per source, the latest)
        for (source_id, frame) in latest_frames {
            if let Some(r) = renderers.get_mut(source_id.as_str()) {
                // Video: upload unless frame callbacks are stuck (prevents memory leak from
                // WGPU staging buffers accumulating when compositor isn't consuming frames).
                // Images: only upload when we'll present (callback not pending) or first frame.
                let should_upload = if r.valid_content_type == crate::queue::ContentType::Video {
                    // Always upload first frame (needed to start transition/display).
                    // After that, throttle if callbacks are stuck >1s to prevent memory balloon.
                    !r.has_current_texture() || !r.frame_callback_pending_too_long(1000)
                } else {
                    !r.frame_callback_pending || !r.has_current_texture()
                };

                if should_upload {
                    let _video_start = std::time::Instant::now();
                    r.upload_frame(&frame);
                    let video_duration = _video_start.elapsed();
                    metrics.record_video_cpu_time(video_duration);
                    drop(frame);
                } else {
                    // Frame throttled - drop immediately to release gst::Buffer
                    // This prevents buffer accumulation when renderer is busy or stuck
                    drop(frame);
                }

                if r.valid_content_type == crate::queue::ContentType::Video {
                    if let Some(layer_surface) = backend.surfaces.get(source_id.as_str()) {
                        // Deadlock fix: if this is the first frame of a transition (progress == 0),
                        // we MUST render and commit it to trigger the Wayland frame callback loop,
                        // even if a callback is technically "pending" from the switch event.
                        if !r.frame_callback_pending || r.transition_progress == 0.0 {
                            let _ = r.render(
                                renderer::BackendContext::Wayland {
                                    surface: layer_surface,
                                    qh: &qh,
                                },
                                loop_start,
                            );
                            if !first_frame_recorded {
                                metrics.record_first_frame();
                                first_frame_recorded = true;
                            }
                            r.request_frame_callback(layer_surface, &qh);
                        }
                    }
                }
            } else {
                // Renderer doesn't exist - drop frame immediately
                drop(frame);
            }
        }
        // device.poll deferred to end-of-loop to avoid redundant driver calls (P-14)

        // Handle Images
        let mut images_received = 0;
        while let Ok(msg) = image_rx.try_recv() {
            images_received += 1;
            debug!(
                "[IMAGE] Received image for {}: data={}, size={}x{}",
                msg.name,
                msg.data.is_some(),
                msg.width,
                msg.height
            );
            if let Some(r) = renderers.get_mut(&msg.name) {
                if let Some(data) = msg.data {
                    debug!(
                        "[IMAGE] Uploading image data for {}: {} bytes",
                        msg.name,
                        data.len()
                    );
                    let _ = r.upload_image_data(data, msg.width, msg.height);
                    debug!("[IMAGE] Rendering after upload for {}", msg.name);
                    if r.configured {
                        if let Some(layer_surface) = backend.surfaces.get(&msg.name) {
                            let _ = r.render(
                                renderer::BackendContext::Wayland {
                                    surface: layer_surface,
                                    qh: &qh,
                                },
                                loop_start,
                            );
                            if !first_frame_recorded {
                                metrics.record_first_frame();
                                first_frame_recorded = true;
                            }
                        }
                    }
                } else {
                    r.abort_transition();
                }
            } else {
                // CRITICAL: Renderer doesn't exist - drop image data immediately to prevent memory leak
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
                // msg.data is dropped here, freeing the Vec<u8>
            }
        }
        // Track image channel usage for memory leak detection
        if images_received > 0 {
            metrics.record_image_channel_size(images_received);
        }

        // Async Video Players
        while let Ok(res) = player_rx.try_recv() {
            match res {
                VideoPlayerResult::Success(name, session_id, mut player) => {
                    if renderers.get(&name).map(|r| r.active_video_session_id) == Some(session_id) {
                        if let Some(mut old) = video_players.insert(name, player) {
                            tokio::spawn(async move {
                                let _ = old.stop();
                            });
                        }
                    } else {
                        // Stale player - stop in background to avoid blocking main loop
                        tokio::spawn(async move {
                            let _ = player.stop();
                        });
                    }
                }
                VideoPlayerResult::Failure(name, session_id) => {
                    if renderers.get(&name).map(|r| r.active_video_session_id) == Some(session_id) {
                        if let Some(r) = renderers.get_mut(&name) {
                            r.abort_transition();
                        }
                    }
                }
            }
        }

        // Rendering
        let frame_ready_names: Vec<String> = backend.frame_callback_ready.drain().collect();
        for name in frame_ready_names {
            if let Some(r) = renderers.get_mut(&name) {
                r.frame_callback_pending = false;
                r.last_frame_request = None;
                if let Some(layer_surface) = backend.surfaces.get(&name) {
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                        },
                        loop_start,
                    );
                    if !first_frame_recorded {
                        metrics.record_first_frame();
                        first_frame_recorded = true;
                    }
                }
            }
        }

        // Request missing frames and check for transition completion
        for (name, r) in renderers.iter_mut() {
            // Only request frame callbacks when we have content to render.
            // Without a texture (current or prev), the renderer can't commit a frame,
            // so the compositor will never send a callback -> infinite stuck loop.
            let should_request = r.has_any_content() && (r.needs_redraw || r.transition_active);
            if should_request {
                if let Some(layer_surface) = backend.surfaces.get(name) {
                    r.request_frame_callback(layer_surface, &qh);
                }
            }
            // Check if transition just completed (for cases where render wasn't called this loop)
            if r.transition_just_completed {
                r.transition_just_completed = false; // Clear flag
                monitor_manager.mark_transition_completed(name);
            }
        }

        // Record frame time
        let frame_time = loop_start.elapsed();
        metrics.record_frame_time(frame_time);

        // Cleanup texture pool periodically (every 3 seconds for more aggressive cleanup)
        if last_pool_cleanup.elapsed().as_secs() >= 3 {
            if let Some(ctx) = &wgpu_ctx {
                ctx.cleanup_texture_pool(Some(&metrics));
                // Removed blocking poll(Wait) to prevent UI freezes/deadlocks
            }
            last_pool_cleanup = Instant::now();
        }

        // Flush stats every 5 seconds (batched writes)
        if last_stats_flush.elapsed().as_secs() >= 5 {
            let _ = monitor_manager.flush_all_stats();
            last_stats_flush = Instant::now();
        }

        // Process directory watcher events and apply pool updates
        if let Some(ref mut watcher) = dir_watcher {
            let pool_events = watcher.process_events().await;
            monitor_manager.apply_pool_events(pool_events);
        }

        // Log metrics summary every 30 seconds (or 10 seconds for testing)
        if last_metrics_log.elapsed().as_secs() >= 10 {
            // Record resource counts for leak detection
            if let Some(ctx) = &wgpu_ctx {
                let texture_count = ctx.texture_pool.lock().values().map(|v| v.len()).sum();
                let pipeline_count = ctx.transition_pipelines.lock().len()
                    + ctx.blit_pipelines.lock().len()
                    + ctx.mipmap_pipelines.lock().len();
                metrics.record_texture_count(texture_count);
                metrics.record_pipeline_count(pipeline_count);
            }
            metrics.log_summary();
            last_metrics_log = Instant::now();
        }

        // Timing
        let elapsed = loop_start.elapsed();
        if elapsed < target_frame_time {
            tokio::time::sleep(target_frame_time - elapsed).await;
        }
        if let Some(ctx) = &wgpu_ctx {
            ctx.device.poll(wgpu::Maintain::Poll);
        }
    }

    Ok(())
}

async fn run_x11_loop(
    config: orchestration::Config,
    log_level: Option<u8>,
    gstreamer_duration: std::time::Duration,
) -> anyhow::Result<()> {
    // Similar to run_wayland_loop but with X11 backend
    let script_path = config.global.script_path.clone();
    let script_tick_interval = config.global.script_tick_interval;
    let metrics = Arc::new(metrics::PerformanceMetrics::new());
    metrics.record_startup_start();
    metrics.record_gstreamer_init(gstreamer_duration);

    // Start resource monitor with metrics
    let monitor = monitor::SystemMonitor::new_with_metrics(Some(metrics.clone()));
    tokio::spawn(async move {
        monitor.run().await;
    });

    let mut monitor_manager =
        monitor_manager::MonitorManager::new_with_metrics(config.clone(), Some(metrics.clone()))?;
    let mut last_metrics_log = Instant::now();
    let mut first_frame_recorded_x11 = false;
    let mut last_stats_flush_x11 = Instant::now();

    // Initialize directory watcher for cache invalidation
    let cache = monitor_manager.get_cache();
    let mut dir_watcher = match cache::DirectoryWatcher::new(cache.clone()) {
        Ok(mut watcher) => {
            // Watch all content directories from config
            for output_config in config.outputs.values() {
                if let Some(path) = &output_config.path {
                    if let Err(e) = watcher.watch(path) {
                        tracing::warn!(
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
            tracing::warn!("[CACHE] Failed to create directory watcher: {}", e);
            None
        }
    };

    // Log metrics immediately for DEBUG (3) and TRACE (4) levels
    if log_level.map(|l| l >= 3).unwrap_or(false) {
        metrics.log_summary();
    }

    let mut backend = x11::X11Backend::new()?;
    // Query RandR for monitors
    let monitors = backend.get_monitors()?;
    let mut wgpu_ctx: Option<Arc<renderer::WgpuContext>> = None;
    let mut renderers: HashMap<String, renderer::Renderer> = HashMap::new();
    let mut window_to_renderer = HashMap::new();
    let mut initial_surface: Option<wgpu::Surface<'static>> = None;

    let mut surface_infos = Vec::new();
    for (name, x, y, width, height) in monitors {
        monitor_manager.add_output(&name, "X11 Display").await;
        let win = backend.create_wallpaper_window(&name, x, y, width, height)?;
        window_to_renderer.insert(win, name.clone());

        let raw_handle = x11::RawX11Surface {
            window_id: win,
            connection: backend.conn.clone(),
            screen: backend.screen_num as i32,
        };
        let surface_arc = Arc::new(raw_handle);
        surface_infos.push((name, surface_arc, width, height));
    }

    if let Some((_, surface_arc, _, _)) = surface_infos.first() {
        info!("Initializing WGPU context with first surface as compatible...");
        let wgpu_start = Instant::now();
        let (ctx, surface) = renderer::WgpuContext::with_surface(surface_arc.clone()).await?;
        let wgpu_duration = wgpu_start.elapsed();
        metrics.record_wgpu_init(wgpu_duration);
        wgpu_ctx = Some(ctx);
        initial_surface = Some(surface);
    }

    if let Some(ctx) = wgpu_ctx.clone() {
        let first_name = surface_infos.first().map(|(n, _, _, _)| n.clone());

        for (name, surface_arc, width, height) in surface_infos {
            let ctx_clone = ctx.clone();
            let is_first = Some(&name) == first_name.as_ref();
            let init_surf = if is_first {
                initial_surface.take()
            } else {
                match ctx_clone.instance.create_surface(surface_arc.clone()) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        error!("Failed to create surface for {}: {}", name, e);
                        None
                    }
                }
            };

            let metrics_clone = metrics.clone();

            info!("[STARTUP-X11] Initializing renderer for {}", name);
            let name_for_bg = name.clone();
            let spawn_handler = tokio::task::spawn_blocking(move || {
                renderer::Renderer::new(
                    name_for_bg,
                    ctx_clone,
                    surface_arc,
                    init_surf,
                    Some(metrics_clone),
                )
            });

            match tokio::time::timeout(std::time::Duration::from_secs(5), spawn_handler).await {
                Ok(join_res) => match join_res {
                    Ok(render_res) => match render_res {
                        Ok(mut r) => {
                            let _ = r.resize_checked(width as u32, height as u32);
                            if let Some(cfg) = monitor_manager.get_output_config(&name) {
                                r.apply_config(cfg);
                            }
                            renderers.insert(name, r);
                        }
                        Err(e) => error!("Failed to create renderer for {}: {}", name, e),
                    },
                    Err(e) => error!("Thread join error for output {}: {}", name, e),
                },
                Err(_) => error!(
                    "TIMEOUT: Renderer initialization for {} took longer than 5s. Skipping.",
                    name
                ),
            }
        }

        // All renderers created - full initialization complete
        metrics.record_full_init();
        if log_level.map(|l| l >= 3).unwrap_or(false) {
            metrics.log_startup_summary();
        }
    }

    let mut video_players: HashMap<String, video::VideoPlayer> = HashMap::new();
    // Frame channel: increased capacity to 32 to cushion against micro-stutters
    // when multiple video sources are active. Memory cap is still reasonable (~1GB slack).
    let (frame_tx, mut frame_rx) =
        tokio::sync::mpsc::channel::<(Arc<String>, video::VideoEvent)>(32);
    let (cmd_tx, mut cmd_rx) =
        tokio::sync::mpsc::unbounded_channel::<(Request, tokio::sync::oneshot::Sender<Response>)>();
    // Image channel: bounded to prevent memory spikes from large images accumulating
    // Increased to 16 to prevent backpressure during batch transitions
    let (image_tx, mut image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
    let (player_tx, mut player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();

    // IPC Listener (duplicated setup for now to avoid complexity extracting)
    let socket_path = dirs::runtime_dir()
        .map(|d| d.join("kaleidux.sock"))
        .unwrap_or_else(|| {
            let uid = std::env::var("USER").unwrap_or_else(|_| "kaleidux".to_string());
            std::path::PathBuf::from(format!("/tmp/kaleidux-{}.sock", uid))
        });
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    let cmd_tx_clone = cmd_tx.clone();
    tokio::spawn(async move {
        loop {
            // Simplified IPC loop
            if let Ok((mut stream, _)) = listener.accept().await {
                let cmd_tx = cmd_tx_clone.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    if let Ok(n) = stream.read(&mut buf).await {
                        if let Ok(req) = serde_json::from_slice::<Request>(&buf[..n]) {
                            let (tx, rx) = tokio::sync::oneshot::channel();
                            let _ = cmd_tx.send((req, tx));
                            if let Ok(resp) = rx.await {
                                let _ = stream.write_all(&serde_json::to_vec(&resp).unwrap()).await;
                            }
                        }
                    }
                });
            }
        }
    });

    let mut next_session_id = 1u64;
    // Initial Load
    // X11: Renderers are configured immediately (no Wayland configure events needed)
    // But we still verify they exist before loading content
    info!(
        "[STARTUP] Reached Initial Load section, renderers count: {}",
        renderers.len()
    );
    info!("[STARTUP] About to call monitor_manager.tick()");
    let initial_changes = monitor_manager.tick();
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
    for (name, (path, content_type)) in initial_changes {
        // CRITICAL: Only switch content if renderer exists
        // X11 renderers are configured immediately, so we just check existence
        if !renderers.contains_key(&name) {
            warn!(
                "[STARTUP] Skipping initial content for {} - renderer does not exist",
                name
            );
            continue;
        }

        switch_wallpaper_content(
            &name,
            &path,
            content_type,
            &mut next_session_id,
            &frame_tx,
            &monitor_manager,
            &mut renderers,
            &mut video_players,
            Some(batch_id),
            None,
            &image_tx,
            &player_tx,
            "STARTUP",
        );
    }

    let mut script_manager = scripting::ScriptManager::new(cmd_tx.clone());
    if let Some(path) = &script_path {
        drop(script_manager.load(path));
    }
    let mut last_script_tick = Instant::now();
    let target_frame_time = std::time::Duration::from_micros(16667);
    let mut last_pool_cleanup_x11 = Instant::now();

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown_flag.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        warn!("Received shutdown signal, cleaning up...");
        shutdown_clone.store(true, Ordering::SeqCst);
    });

    // X11 Loop
    loop {
        let loop_start = Instant::now();
        if shutdown_flag.load(Ordering::SeqCst) {
            break;
        }

        // Poll X11 events (non-blocking)
        while let Ok(maybe_event) = backend.conn.poll_for_event() {
            if let Some(event) = maybe_event {
                use x11rb::protocol::Event;
                match event {
                    Event::ConfigureNotify(ev) => {
                        // Handle window resize for specific window
                        if let Some(name) = window_to_renderer.get(&ev.window) {
                            if let Some(r) = renderers.get_mut(name) {
                                let _ = r.resize_checked(ev.width as u32, ev.height as u32);
                            }
                        }
                    }
                    Event::Expose(ev) => {
                        if let Some(name) = window_to_renderer.get(&ev.window) {
                            if let Some(r) = renderers.get_mut(name) {
                                r.needs_redraw = true;
                            }
                        }
                    }
                    Event::RandrNotify(_) | Event::RandrScreenChangeNotify(_) => {
                        debug!("[X11] RandR event received, marking monitors dirty");
                        backend.monitors_dirty.store(true, Ordering::SeqCst);
                    }
                    _ => {}
                }
            } else {
                break;
            }
        }

        // Logic
        if last_script_tick.elapsed().as_secs() >= script_tick_interval {
            script_manager.tick();
            last_script_tick = Instant::now();
        }

        // Automated Changes
        let scheduled_changes = monitor_manager.tick();
        if !scheduled_changes.is_empty() {
            let batch_id = rand::random::<u64>();
            for (name, (path, content_type)) in scheduled_changes {
                switch_wallpaper_content(
                    &name,
                    &path,
                    content_type,
                    &mut next_session_id,
                    &frame_tx,
                    &monitor_manager,
                    &mut renderers,
                    &mut video_players,
                    Some(batch_id),
                    Some(loop_start),
                    &image_tx,
                    &player_tx,
                    "SCHEDULED",
                );
            }
        }

        // Commands
        while let Ok((req, resp)) = cmd_rx.try_recv() {
            let response = handle_command(
                req,
                &mut monitor_manager,
                &mut renderers,
                &mut video_players,
                &frame_tx,
                &image_tx,
                &player_tx,
                &mut next_session_id,
                loop_start,
                &shutdown_flag,
            )
            .await;
            let _ = resp.send(response);
        }

        // Frames / Images / Video Players
        // CRITICAL: Process ALL frames in channel, not just latest per source
        let (latest_frames, frames_received, frames_discarded_x11) = {
            let mut frames = HashMap::new();
            let mut count = 0;
            let mut discarded = 0;
            while let Ok((src, evt)) = frame_rx.try_recv() {
                count += 1;
                if let video::VideoEvent::Frame(f) = evt {
                    // If we already have a frame for this source, drop the old one
                    if frames.insert(src.clone(), f).is_some() {
                        discarded += 1;
                    }
                }
            }
            (frames, count, discarded)
        };
        // Track frame channel usage for memory leak detection
        if frames_received > 0 {
            metrics.record_frame_channel_size(frames_received);
            if frames_discarded_x11 > 0 {
                debug!(
                    "[VIDEO] Discarded {} older frames (keeping latest per source)",
                    frames_discarded_x11
                );
            }
        }
        for (src, frame) in latest_frames {
            if let Some(r) = renderers.get_mut(src.as_str()) {
                // THROTTLING FIX (Updated):
                // For video: Always upload frames - X11 doesn't use frame callbacks, so no throttling needed.
                // For images: Use strict throttling - only upload when callback not pending or first frame.
                // Note: X11 doesn't use frame callbacks, but keeping logic consistent with Wayland path.
                let should_upload = if r.valid_content_type == crate::queue::ContentType::Video {
                    // Video: always upload (X11 has no callback mechanism)
                    true
                } else {
                    // Images: strict throttling (original logic)
                    !r.frame_callback_pending || !r.has_current_texture()
                };

                if should_upload {
                    let _video_start = std::time::Instant::now();
                    r.upload_frame(&frame);
                    // Record video CPU time (frame processing)
                    let video_duration = _video_start.elapsed();
                    metrics.record_video_cpu_time(video_duration);
                    // CRITICAL: Explicitly drop frame after processing to release gst::Buffer
                    drop(frame);
                } else {
                    // Frame throttled - drop immediately to release gst::Buffer
                    // This prevents buffer accumulation when renderer is busy
                    drop(frame);
                }

                // X11: Render immediately if video
                let _ = r.render(renderer::BackendContext::X11, loop_start);
                if !first_frame_recorded_x11 {
                    metrics.record_first_frame();
                    first_frame_recorded_x11 = true;
                }
                // Check if transition just completed and mark it
                if r.transition_just_completed {
                    r.transition_just_completed = false; // Clear flag
                    monitor_manager.mark_transition_completed(src.as_str());
                }
            } else {
                // Renderer doesn't exist - drop frame immediately
                drop(frame);
            }
        }
        let mut images_received_x11 = 0;
        while let Ok(msg) = image_rx.try_recv() {
            images_received_x11 += 1;
            if let Some(r) = renderers.get_mut(&msg.name) {
                if let Some(data) = msg.data {
                    let _ = r.upload_image_data(data, msg.width, msg.height);
                    let _ = r.render(renderer::BackendContext::X11, loop_start);
                    // Check if transition just completed and mark it
                    if r.transition_just_completed {
                        r.transition_just_completed = false; // Clear flag
                        monitor_manager.mark_transition_completed(&msg.name);
                    }
                } else {
                    r.abort_transition();
                }
            } else {
                // CRITICAL: Renderer doesn't exist - drop image data immediately to prevent memory leak
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
                // msg.data is dropped here, freeing the Vec<u8>
            }
        }
        // Track image channel usage for memory leak detection
        if images_received_x11 > 0 {
            metrics.record_image_channel_size(images_received_x11);
        }
        while let Ok(msg) = player_rx.try_recv() {
            match msg {
                VideoPlayerResult::Success(name, session_id, mut p) => {
                    if renderers.get(&name).map(|r| r.active_video_session_id) == Some(session_id) {
                        if let Some(mut existing) = video_players.insert(name, p) {
                            tokio::spawn(async move {
                                let _ = existing.stop();
                            });
                        }
                    } else {
                        // Stale - stop in background
                        tokio::spawn(async move {
                            let _ = p.stop();
                        });
                    }
                }
                VideoPlayerResult::Failure(name, session_id) => {
                    if renderers.get(&name).map(|r| r.active_video_session_id) == Some(session_id) {
                        if let Some(r) = renderers.get_mut(&name) {
                            r.abort_transition();
                        }
                    }
                }
            }
        }

        // Render Loop for Transitions / Redraws
        for (name, r) in renderers.iter_mut() {
            if r.needs_redraw
                || r.transition_active
                || r.valid_content_type == crate::queue::ContentType::Video
            {
                let _ = r.render(renderer::BackendContext::X11, loop_start);
                if !first_frame_recorded_x11 {
                    metrics.record_first_frame();
                    first_frame_recorded_x11 = true;
                }
                // Check if transition just completed and mark it
                if r.transition_just_completed {
                    r.transition_just_completed = false; // Clear flag
                    monitor_manager.mark_transition_completed(name);
                }
            }
        }

        // Ensure X11 commands are sent - only if we actually rendered something
        let needs_flush = renderers
            .values()
            .any(|r| r.needs_redraw || r.transition_active);
        if needs_flush {
            let _ = backend.conn.flush();
        }

        // Record frame time
        let frame_time = loop_start.elapsed();
        metrics.record_frame_time(frame_time);

        // Cleanup texture pool periodically (every 3 seconds for more aggressive cleanup)
        if last_pool_cleanup_x11.elapsed().as_secs() >= 3 {
            if let Some(ctx) = &wgpu_ctx {
                ctx.cleanup_texture_pool(Some(&metrics));
            }
            last_pool_cleanup_x11 = Instant::now();
        }

        // Process directory watcher events and apply pool updates
        if let Some(ref mut watcher) = dir_watcher {
            let pool_events = watcher.process_events().await;
            monitor_manager.apply_pool_events(pool_events);
        }

        // Flush stats every 5 seconds (batched writes)
        if last_stats_flush_x11.elapsed().as_secs() >= 5 {
            let _ = monitor_manager.flush_all_stats();
            last_stats_flush_x11 = Instant::now();
        }

        // Log metrics summary every 10 seconds
        if last_metrics_log.elapsed().as_secs() >= 10 {
            // Record resource counts for leak detection
            if let Some(ctx) = &wgpu_ctx {
                let texture_count = ctx.texture_pool.lock().values().map(|v| v.len()).sum();
                let pipeline_count = ctx.transition_pipelines.lock().len()
                    + ctx.blit_pipelines.lock().len()
                    + ctx.mipmap_pipelines.lock().len();
                metrics.record_texture_count(texture_count);
                metrics.record_pipeline_count(pipeline_count);
            }
            metrics.log_summary();
            last_metrics_log = Instant::now();
        }

        let elapsed = loop_start.elapsed();
        if elapsed < target_frame_time {
            tokio::time::sleep(target_frame_time - elapsed).await;
        }
        if let Some(ctx) = &wgpu_ctx {
            ctx.device.poll(wgpu::Maintain::Poll);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    req: Request,
    monitor_manager: &mut monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
    frame_tx: &tokio::sync::mpsc::Sender<(Arc<String>, video::VideoEvent)>,
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
                    // Refresh renderers with new config
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
                if let Some(mut player) = video_players.remove(&name) {
                    tokio::spawn(async move {
                        let _ = player.stop();
                    });
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
                if let Some(mut vp) = video_players.remove(&name) {
                    tokio::spawn(async move {
                        let _ = vp.stop();
                    });
                }
                if let Some(r) = renderers.get_mut(&name) {
                    r.clear();
                }
            }
            Response::Ok
        }
    }
}
