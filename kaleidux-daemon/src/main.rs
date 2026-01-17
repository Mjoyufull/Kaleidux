use tracing_subscriber::{prelude::*, EnvFilter, Registry};
use tracing_subscriber::fmt as subscriber_fmt;
use tracing_subscriber::filter::LevelFilter;
use tracing::{info, warn, debug, error};
use wayland_client::{globals::registry_queue_init, Connection};
use x11rb::connection::Connection as X11Connection;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kaleidux_common::{Request, Response};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod video;
mod renderer;
mod wayland;
mod x11;
mod orchestration;
mod queue;
mod monitor_manager;
mod shaders;
mod scripting;
mod monitor;
mod cache;
mod metrics;

use std::time::Instant;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct LoadedImage {
    name: String,
    data: Option<Vec<u8>>,
    width: u32,
    height: u32,
    path: PathBuf,
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
    image_tx: &tokio::sync::mpsc::UnboundedSender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    log_prefix: &str,
) {
    info!("{}: {} -> {:?}", log_prefix, name, path.display());

    let was_playing_video = video_players.contains_key(name);
    if was_playing_video {
        if let Some(mut vp) = video_players.remove(name) {
            debug!("[TRANSITION] {}: Offloading video player stop to background", name);
            tokio::spawn(async move {
                let _ = vp.stop();
            });
        }
    }
    
    if let Some(r) = renderers.get_mut(name) {
        r.active_batch_id = batch_id;
        r.batch_start_time = batch_trigger_time; 
        r.set_content_type(content_type);
        r.switch_content();

        if content_type == crate::queue::ContentType::Image {
            let name_clone = name.to_string();
            let path_clone = path.to_path_buf();
            let tx = image_tx.clone();
            
            debug!("[ASSET] {}: Offloading image decode: {}", name, path.display());
            tokio::task::spawn_blocking(move || {
                match image::open(&path_clone) {
                    Ok(img) => {
                        let rgba = img.to_rgba8();
                        let (width, height) = rgba.dimensions();
                        let _ = tx.send(LoadedImage {
                            name: name_clone,
                            data: Some(rgba.into_raw()),
                            width,
                            height,
                            path: path_clone,
                        });
                    }
                    Err(e) => {
                        error!("Failed to decode image {}: {}", path_clone.display(), e);
                        let _ = tx.send(LoadedImage {
                            name: name_clone,
                            data: None,
                            width: 0,
                            height: 0,
                            path: path_clone,
                        });
                    }
                }
            });
        }
    }
    
    if content_type == crate::queue::ContentType::Video {
        let session_id = *next_session_id;
        *next_session_id += 1;
        debug!("[TRANSITION] {}: Starting new video player (session_id={})", name, session_id);
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
    
    let vol = monitor_manager.outputs.get(name)
        .map(|o| o.config.volume as f64 / 100.0)
        .unwrap_or(1.0);

    tokio::task::spawn_blocking(move || {
        match video::VideoPlayer::new(
            &path_str,
            name_arc,
            session_id,
            frame_tx_clone,
        ) {
            Ok(mut vp) => {
                vp.set_volume(vol);
                if vp.start().is_ok() {
                    if let Err(e) = player_tx_clone.send(VideoPlayerResult::Success(name_str, session_id, vp)) {
                         error!("Failed to send video player back: {}", e);
                    }
                } else {
                     error!("Failed to start video player");
                     let _ = player_tx_clone.send(VideoPlayerResult::Failure(name_str, session_id));
                }
            }
            Err(e) => {
                error!("Failed to create video player: {}", e);
                let _ = player_tx_clone.send(VideoPlayerResult::Failure(name_str, session_id));
            }
        }
    });
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Show version information
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version: Option<bool>,

    #[arg(long)]
    demo: bool,
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=4))]
    log: Option<u8>,
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
            None | _ => LevelFilter::INFO, 
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
                .join("kaleidux").join("logs");
            std::fs::create_dir_all(&config_dir)?;
            
            let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S");
            let log_path = config_dir.join(format!("kaleidux-daemon-{}.log", timestamp));
            let file = std::fs::File::create(&log_path)?;
            println!("Logging to file: {}", log_path.display());
            let (non_blocking_file, file_guard) = tracing_appender::non_blocking(file);
            let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());

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
            
            info!("Kaleidux Daemon starting... (Level {}, File: {})", level, config_dir.display());
            (Some(file_guard), Some(stdout_guard))
        } else {
            let stdout_layer = subscriber_fmt::layer()
                .with_writer(std::io::stdout)
                .with_timer(CustomTimer);

            Registry::default()
                .with(env_filter)
                .with(stdout_layer)
                .init();
            info!("Kaleidux Daemon starting...");
            (None, None)
        }
    };

    // 2. Load Configuration
    let config = match orchestration::Config::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("Failed to load configuration: {}. Using defaults.", e);
            orchestration::Config::default()
        }
    };

    // 3. Initialize GStreamer
    gstreamer::init()?;
    info!("GStreamer initialized.");

    // 4. Start Resource Monitor
    let monitor = monitor::SystemMonitor::new();
    tokio::spawn(async move {
        monitor.run().await;
    });
    
    // Detect Backend
    let use_x11 = std::env::var("WAYLAND_DISPLAY").is_err() && std::env::var("DISPLAY").is_ok();
    
    if use_x11 {
        info!("Starting X11 Backend...");
        run_x11_loop(config, log_level).await
    } else {
        info!("Starting Wayland Backend...");
        run_wayland_loop(config, log_level).await
    }
}

async fn run_wayland_loop(config: orchestration::Config, log_level: Option<u8>) -> anyhow::Result<()> {
    let script_path = config.global.script_path.clone();
    let script_tick_interval = config.global.script_tick_interval;
    let metrics = Arc::new(metrics::PerformanceMetrics::new());
    let mut monitor_manager = monitor_manager::MonitorManager::new_with_metrics(config, Some(metrics.clone()))?;
    let mut last_metrics_log = Instant::now();
    
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

    // Frame channel buffer: 60 frames = ~1 second at 60fps
    // This prevents frame drops when renderer is temporarily slow
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<(Arc<String>, video::VideoEvent)>(60);
    let mut renderers = HashMap::new();
    let outputs: Vec<_> = backend.output_state.outputs().collect();
    
    let display_ptr = {
        let backend_ref = conn.backend();
        backend_ref.display_ptr() as *mut std::ffi::c_void
    };
    
    let mut surface_infos = Vec::new();
    for output in outputs {
        let info = match backend.output_state.info(&output) {
            Some(i) => i,
            None => continue,
        };
        let name = info.name.as_deref().unwrap_or("unknown").to_string();
        let description = info.description.as_deref().unwrap_or("unknown").to_string();
        
        info!("Creating surface for output: {} ({})", name, description);
        monitor_manager.add_output(&name, &description);
        let output_config = match monitor_manager.get_output_config(&name) {
            Some(cfg) => cfg,
            None => continue,
        };

        let layer_surface = backend.create_wallpaper_surface(&output, &qh, name.clone(), output_config.layer.clone().into())?;
        
        let raw_handle_surface = wayland::RawHandleSurface {
            layer_surface,
            display_ptr,
        };
        let surface_arc = Arc::new(raw_handle_surface);
        surface_infos.push((name, surface_arc));
    }

    if let Some((_, first_surface_arc)) = surface_infos.first() {
        info!("Initializing WGPU context with first surface as compatible...");
        let (ctx, surface) = renderer::WgpuContext::with_surface(first_surface_arc.clone()).await?;
        let adapter_name = ctx.adapter.get_info().name.clone();
        wgpu_ctx = Some(ctx);
        initial_surface = Some(surface);
        info!("WGPU initialized on GPU: {:?}", adapter_name);
    }

    if let Some(ctx) = wgpu_ctx.clone() {
        let mut renderer_futures = Vec::new();
        let first_name = surface_infos.first().map(|(n, _)| n.clone());
        
        for (name, surface_arc) in surface_infos {
            let ctx_clone = ctx.clone();
            let is_first = Some(&name) == first_name.as_ref();
            let init_surf = if is_first { initial_surface.take() } else { None };
            
            let metrics_clone = metrics.clone();
            renderer_futures.push(async move {
                (name.clone(), renderer::Renderer::new(name, ctx_clone, surface_arc, init_surf, Some(metrics_clone)).await)
            });
        }
        
        let results = futures::future::join_all(renderer_futures).await;
        for (name, res) in results {
            match res {
                Ok(mut r) => {
                    if let Some(output_config) = monitor_manager.get_output_config(&name) {
                        r.apply_config(output_config);
                    }
                    renderers.insert(name, r);
                }
                Err(e) => error!("Failed to create renderer for output {}: {}", name, e),
            }
        }
    }
    
    let mut video_players: HashMap<String, video::VideoPlayer> = HashMap::new();

    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<(Request, tokio::sync::oneshot::Sender<Response>)>();
    let (image_tx, mut image_rx) = tokio::sync::mpsc::unbounded_channel::<LoadedImage>();
    let (player_tx, mut player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();
    let script_cmd_tx = cmd_tx.clone();

    // IPC Socket Setup
    let socket_path = dirs::runtime_dir()
        .map(|d| d.join("kaleidux.sock"))
        .unwrap_or_else(|| {
            let uid = std::env::var("USER").unwrap_or_else(|_| "kaleidux".to_string());
            std::path::PathBuf::from(format!("/tmp/kaleidux-{}.sock", uid))
        });
    
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    
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
                        if n == 0 || n >= MAX_MESSAGE_SIZE { return; }
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

    let mut script_manager = scripting::ScriptManager::new(script_cmd_tx);
    if let Some(path) = &script_path {
        let _ = script_manager.load(path);
    }
    let mut last_script_tick = Instant::now();

    let target_frame_time = std::time::Duration::from_micros(16667); // ~60 FPS
    let mut connection_error_count = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 3;
    let mut connection_dead = false;
    let mut last_error_time = Instant::now();
    let mut last_pool_cleanup = Instant::now();
    
    // Initial Load
    let initial_changes = monitor_manager.tick();
    let mut next_session_id = 1u64;
    let batch_id = rand::random::<u64>();
    for (name, (path, content_type)) in initial_changes {
         switch_wallpaper_content(
            &name, &path, content_type, &mut next_session_id, &frame_tx,
            &monitor_manager, &mut renderers, &mut video_players,
            Some(batch_id), None, &image_tx, &player_tx, "STARTUP"
         );
    }
    
    // Main Loop (Wayland)
    loop {
        let loop_start = Instant::now();
        if shutdown_flag.load(Ordering::SeqCst) {
            for (_, player) in &mut video_players { let _ = player.stop(); }
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
             use std::os::unix::io::{AsRawFd, AsFd};
             let fd = conn.as_fd().as_raw_fd();
             let mut poll_fd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
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
            let needs_flush = renderers.values().any(|r| r.needs_redraw || r.transition_active);
            if needs_flush {
                 let _ = conn.flush();
            }
        }

        // Logic (Common)
        {
            // Remove orphaned renderers
            let active_output_names: std::collections::HashSet<String> = backend.output_state.outputs().filter_map(|o| {
                backend.output_state.info(&o).and_then(|i| i.name.clone())
            }).collect();
            renderers.retain(|name, _| {
                if !active_output_names.contains(name) {
                    if let Some(mut vp) = video_players.remove(name) {
                        tokio::spawn(async move { let _ = vp.stop(); });
                    }
                    false
                } else { true }
            });
            
            // Handle Resizes
            let resizes: Vec<_> = backend.pending_resizes.drain(..).collect();
            for (name, w, h, _) in resizes {
                if let Some(r) = renderers.get_mut(&name) {
                    let width = if w == 0 { r.config.width } else { w };
                    let height = if h == 0 { r.config.height } else { h };
                    let _ = r.resize_checked(width, height);
                    if r.configured {
                        if let Some((_, layer_surface)) = backend.surfaces.iter().find(|(n, _)| n == &name) {
                             // Force an initial render to commit the surface after resize
                             // This ensures the compositor knows the surface is ready and will send frame callbacks
                             let _ = r.render(renderer::BackendContext::Wayland { surface: layer_surface, qh: &qh }, loop_start);
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
                    &name, &path, content_type, &mut next_session_id, &frame_tx,
                    &monitor_manager, &mut renderers, &mut video_players,
                    Some(batch_id), Some(loop_start), &image_tx, &player_tx, "SCHEDULED"
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
             let response = handle_command(req, &mut monitor_manager, &mut renderers, &mut video_players, &frame_tx, &image_tx, &player_tx, &mut next_session_id, loop_start, &shutdown_flag);
             let _ = resp.send(response);
        }
        
        // Handle Frames
        let mut latest_frames: HashMap<Arc<String>, video::VideoFrame> = HashMap::new();
        while let Ok((source_id, event)) = frame_rx.try_recv() {
            match event {
                video::VideoEvent::Frame(frame) => { latest_frames.insert(source_id, frame); }
                video::VideoEvent::Error(msg) => { error!("Video error {}: {}", source_id, msg); }
            }
        }
        for (source_id, frame) in latest_frames {
            if let Some(r) = renderers.get_mut(source_id.as_str()) {
                r.upload_frame(&frame);
                if r.valid_content_type == crate::queue::ContentType::Video {
                    if let Some((_, layer_surface)) = backend.surfaces.iter().find(|(n, _)| n == source_id.as_str()) {
                        // Deadlock fix: if this is the first frame of a transition (progress == 0),
                        // we MUST render and commit it to trigger the Wayland frame callback loop,
                        // even if a callback is technically "pending" from the switch event.
                        if !r.frame_callback_pending || r.transition_progress == 0.0 {
                            let _ = r.render(renderer::BackendContext::Wayland{surface: layer_surface, qh: &qh}, loop_start);
                            r.request_frame_callback(layer_surface, &qh);
                        }
                    }
                }
            }
        }
        
        // Handle Images
        while let Ok(msg) = image_rx.try_recv() {
             if let Some(r) = renderers.get_mut(&msg.name) {
                 if let Some(data) = msg.data {
                     let _ = r.upload_image_data(data, msg.width, msg.height);
                     if r.configured {
                          if let Some((_, layer_surface)) = backend.surfaces.iter().find(|(n, _)| n == &msg.name) {
                              let _ = r.render(renderer::BackendContext::Wayland{surface: layer_surface, qh: &qh}, loop_start);
                          }
                     }
                 } else {
                     r.abort_transition();
                 }
             }
        }
        
        // Async Video Players
        while let Ok(res) = player_rx.try_recv() {
            match res {
                VideoPlayerResult::Success(name, session_id, mut player) => {
                    if renderers.get(&name).map(|r| r.active_video_session_id) == Some(session_id) {
                        video_players.insert(name, player);
                    } else {
                        let _ = player.stop(); // Stale
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
                if let Some((_, layer_surface)) = backend.surfaces.iter().find(|(n, _)| n == &name) {
                     let _ = r.render(renderer::BackendContext::Wayland { surface: layer_surface, qh: &qh }, loop_start);
                }
            }
        }
        
        // Request missing frames and check for transition completion
        for (name, r) in renderers.iter_mut() {
            if (r.needs_redraw || r.transition_active || r.valid_content_type == crate::queue::ContentType::Video) && !r.frame_callback_pending {
                 if let Some((_, layer_surface)) = backend.surfaces.iter().find(|(n, _)| n == name) {
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
        
        // Cleanup texture pool periodically (every 5 seconds)
        if last_pool_cleanup.elapsed().as_secs() >= 5 {
            if let Some(ctx) = &wgpu_ctx {
                ctx.cleanup_texture_pool();
            }
            last_pool_cleanup = Instant::now();
        }
        
        // Log metrics summary every 30 seconds (or 10 seconds for testing)
        if last_metrics_log.elapsed().as_secs() >= 10 {
            metrics.log_summary();
            last_metrics_log = Instant::now();
        }
        
        // Timing
        let elapsed = loop_start.elapsed();
        if elapsed < target_frame_time {
            tokio::time::sleep(target_frame_time - elapsed).await;
        }
        if let Some(ctx) = &wgpu_ctx { ctx.device.poll(wgpu::Maintain::Poll); }
    }
    
    Ok(())
}

async fn run_x11_loop(config: orchestration::Config, log_level: Option<u8>) -> anyhow::Result<()> {
    // Similar to run_wayland_loop but with X11 backend
    let script_path = config.global.script_path.clone();
    let script_tick_interval = config.global.script_tick_interval;
    let metrics = Arc::new(metrics::PerformanceMetrics::new());
    let mut monitor_manager = monitor_manager::MonitorManager::new_with_metrics(config, Some(metrics.clone()))?;
    let mut last_metrics_log = Instant::now();
    
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
        monitor_manager.add_output(&name, "X11 Display"); 
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
        let (ctx, surface) = renderer::WgpuContext::with_surface(surface_arc.clone()).await?;
        wgpu_ctx = Some(ctx);
        initial_surface = Some(surface);
    }

    if let Some(ctx) = wgpu_ctx.clone() {
        let mut renderer_futures = Vec::new();
        let first_name = surface_infos.first().map(|(n, _, _, _)| n.clone());
        
        for (name, surface_arc, width, height) in surface_infos {
            let ctx_clone = ctx.clone();
            let is_first = Some(&name) == first_name.as_ref();
            let init_surf = if is_first { initial_surface.take() } else {
                match ctx_clone.instance.create_surface(surface_arc.clone()) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        error!("Failed to create surface for {}: {}", name, e);
                        None
                    }
                }
            };
            
            let metrics_clone = metrics.clone();
            renderer_futures.push(async move {
                (name.clone(), renderer::Renderer::new(name, ctx_clone, surface_arc, init_surf, Some(metrics_clone)).await, width, height)
            });
        }
        
        let results = futures::future::join_all(renderer_futures).await;
        for (name, res, width, height) in results {
            match res {
                Ok(mut r) => {
                     let _ = r.resize_checked(width as u32, height as u32);
                     if let Some(cfg) = monitor_manager.get_output_config(&name) {
                         r.apply_config(cfg);
                     }
                     renderers.insert(name, r);
                }
                Err(e) => error!("Failed to create renderer for {}: {}", name, e),
            }
        }
    }

    let mut video_players: HashMap<String, video::VideoPlayer> = HashMap::new();
    // Frame channel buffer: 60 frames = ~1 second at 60fps
    // This prevents frame drops when renderer is temporarily slow
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<(Arc<String>, video::VideoEvent)>(60);
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<(Request, tokio::sync::oneshot::Sender<Response>)>();
    let (image_tx, mut image_rx) = tokio::sync::mpsc::unbounded_channel::<LoadedImage>();
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
        loop { // Simplified IPC loop
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
    let initial_changes = monitor_manager.tick();
    let batch_id = rand::random::<u64>();
    for (name, (path, content_type)) in initial_changes {
         switch_wallpaper_content(
            &name, &path, content_type, &mut next_session_id, &frame_tx,
            &monitor_manager, &mut renderers, &mut video_players,
            Some(batch_id), None, &image_tx, &player_tx, "STARTUP"
         );
    }
    
    let mut script_manager = scripting::ScriptManager::new(cmd_tx.clone());
    if let Some(path) = &script_path { let _ = script_manager.load(path); }
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
        if shutdown_flag.load(Ordering::SeqCst) { break; }

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
                    &name, &path, content_type, &mut next_session_id, &frame_tx,
                    &monitor_manager, &mut renderers, &mut video_players,
                    Some(batch_id), Some(loop_start), &image_tx, &player_tx, "SCHEDULED"
                 );
            }
        }
        
        // Commands
        while let Ok((req, resp)) = cmd_rx.try_recv() {
             let response = handle_command(req, &mut monitor_manager, &mut renderers, &mut video_players, &frame_tx, &image_tx, &player_tx, &mut next_session_id, loop_start, &shutdown_flag);
             let _ = resp.send(response);
        }
        
        // Frames / Images / Video Players
        let mut latest_frames = HashMap::new();
        while let Ok((src, evt)) = frame_rx.try_recv() {
            if let video::VideoEvent::Frame(f) = evt { latest_frames.insert(src, f); }
        }
        for (src, frame) in latest_frames {
            if let Some(r) = renderers.get_mut(src.as_str()) {
                r.upload_frame(&frame);
                // X11: Render immediately if video
                let _ = r.render(renderer::BackendContext::X11, loop_start);
                // Check if transition just completed and mark it
                if r.transition_just_completed {
                    r.transition_just_completed = false; // Clear flag
                    monitor_manager.mark_transition_completed(src.as_str());
                }
            }
        }
        while let Ok(msg) = image_rx.try_recv() {
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
            }
        }
        while let Ok(msg) = player_rx.try_recv() {
             match msg {
                 VideoPlayerResult::Success(name, session_id, mut p) => {
                     if renderers.get(&name).map(|r| r.active_video_session_id) == Some(session_id) {
                         if let Some(existing) = video_players.insert(name, p) {
                             let mut old = existing;
                             let _ = old.stop();
                         }
                     } else {
                         let _ = p.stop(); // Stale
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
            if r.needs_redraw || r.transition_active || r.valid_content_type == crate::queue::ContentType::Video {
                let _ = r.render(renderer::BackendContext::X11, loop_start);
                // Check if transition just completed and mark it
                if r.transition_just_completed {
                    r.transition_just_completed = false; // Clear flag
                    monitor_manager.mark_transition_completed(name);
                }
            }
        }
        
        // Ensure X11 commands are sent - only if we actually rendered something
        let needs_flush = renderers.values().any(|r| r.needs_redraw || r.transition_active);
        if needs_flush {
            let _ = backend.conn.flush();
        }

        // Record frame time
        let frame_time = loop_start.elapsed();
        metrics.record_frame_time(frame_time);
        
        // Cleanup texture pool periodically (every 5 seconds)
        if last_pool_cleanup_x11.elapsed().as_secs() >= 5 {
            if let Some(ctx) = &wgpu_ctx {
                ctx.cleanup_texture_pool();
            }
            last_pool_cleanup_x11 = Instant::now();
        }
        
        // Log metrics summary every 10 seconds
        if last_metrics_log.elapsed().as_secs() >= 10 {
            metrics.log_summary();
            last_metrics_log = Instant::now();
        }

        let elapsed = loop_start.elapsed();
        if elapsed < target_frame_time {
            tokio::time::sleep(target_frame_time - elapsed).await;
        }
        if let Some(ctx) = &wgpu_ctx { ctx.device.poll(wgpu::Maintain::Poll); }
    }
    
    Ok(())
}

fn handle_command(
    req: Request,
    monitor_manager: &mut monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
    frame_tx: &tokio::sync::mpsc::Sender<(Arc<String>, video::VideoEvent)>,
    image_tx: &tokio::sync::mpsc::UnboundedSender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    next_session_id: &mut u64,
    loop_start: Instant,
    shutdown_flag: &Arc<AtomicBool>,
) -> Response {
    match req {
        Request::QueryOutputs => {
             let outputs = renderers.iter().map(|(n, r)| kaleidux_common::OutputInfo {
                 name: n.clone(),
                 width: r.config.width,
                 height: r.config.height,
                 current_wallpaper: monitor_manager.outputs.get(n).and_then(|o| o.current_path.as_ref().map(|p| p.display().to_string())),
             }).collect();
             Response::OutputInfo(outputs)
        }
        Request::Next { output } => {
            let changes = monitor_manager.handle_next(output);
            let batch = rand::random::<u64>();
            for (name, (path, content_type)) in changes {
                switch_wallpaper_content(&name, &path, content_type, next_session_id, frame_tx, monitor_manager, renderers, video_players, Some(batch), Some(loop_start), image_tx, player_tx, "NEXT");
            }
            Response::Ok
        }
        Request::Prev { output } => {
            let changes = monitor_manager.handle_prev(output);
            let batch = rand::random::<u64>();
            for (name, (path, content_type)) in changes {
                switch_wallpaper_content(&name, &path, content_type, next_session_id, frame_tx, monitor_manager, renderers, video_players, Some(batch), Some(loop_start), image_tx, player_tx, "PREV");
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
        Request::Love { path, multiplier } => {
             monitor_manager.love_file(path, multiplier).map(|_| Response::Ok).unwrap_or_else(|e| Response::Error(e.to_string()))
        }
        Request::Unlove { path } => {
             monitor_manager.unlove_file(path).map(|_| Response::Ok).unwrap_or_else(|e| Response::Error(e.to_string()))
        }
        Request::History { output } => Response::History(monitor_manager.get_history(output)),
        _ => Response::Ok
    }
}
