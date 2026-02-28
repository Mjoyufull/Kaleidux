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
use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

// Global semaphore to limit concurrent image decode tasks (prevents memory spikes)
// Limit to 2 concurrent decodes since each can be 35-40MB
static IMAGE_DECODE_SEMAPHORE: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(2)));

#[derive(Debug, Clone)]
pub struct LoadedImage {
    pub name: String,
    pub data: Option<Vec<u8>>,
    pub width: u32,
    pub height: u32,
    pub _path: PathBuf,
}

pub enum VideoPlayerResult {
    Success(String, u64, video::VideoPlayer),
    Failure(String, u64),
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
    pub wgpu_ctx: Option<Arc<renderer::WgpuContext>>,

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

        let monitor_manager =
            monitor_manager::MonitorManager::new_with_metrics(config.clone(), Some(metrics.clone()))?;

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
        let (frame_tx, frame_rx) =
            tokio::sync::mpsc::channel::<FrameMsg>(32);
        // Image channel: bounded to prevent memory spikes from large images accumulating
        let (image_tx, image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
        let (player_tx, player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();
        let (cmd_tx, cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<CmdMsg>();

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
            wgpu_ctx: None,
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
    pub async fn drain_commands(
        &mut self,
        cmd_buf: Option<CmdMsg>,
        loop_start: Instant,
    ) {
        let cmd_iter = std::iter::once(cmd_buf)
            .flatten()
            .chain(std::iter::from_fn(|| self.cmd_rx.try_recv().ok()));
        for (req, resp) in cmd_iter {
            let response = handle_command(
                req,
                &mut self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
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
        let image_iter = std::iter::once(image_buf)
            .flatten()
            .chain(std::iter::from_fn(|| self.image_rx.try_recv().ok()));
        for msg in image_iter {
            images_received += 1;
            debug!(
                "[IMAGE] Received image for {}: data={}, size={}x{}",
                msg.name,
                msg.data.is_some(),
                msg.width,
                msg.height
            );
            if let Some(r) = self.renderers.get_mut(&msg.name) {
                if let Some(data) = msg.data {
                    debug!(
                        "[IMAGE] Uploading image data for {}: {} bytes",
                        msg.name,
                        data.len()
                    );
                    let _ = r.upload_image_data(data, msg.width, msg.height);
                    debug!("[IMAGE] Rendering after upload for {}", msg.name);
                    render_fn(r, &msg.name, loop_start);
                    if !self.first_frame_recorded {
                        self.metrics.record_first_frame();
                        self.first_frame_recorded = true;
                    }
                    // Check if transition just completed and mark it
                    if r.transition_just_completed {
                        r.transition_just_completed = false;
                        self.monitor_manager.mark_transition_completed(&msg.name);
                    }
                } else {
                    r.abort_transition();
                }
            } else {
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
            }
        }
        if images_received > 0 {
            self.metrics.record_image_channel_size(images_received);
        }
    }

    /// Drain player results from channel and insert/stop as needed.
    pub fn drain_players(&mut self, player_buf: Option<VideoPlayerResult>) {
        let player_iter = std::iter::once(player_buf)
            .flatten()
            .chain(std::iter::from_fn(|| self.player_rx.try_recv().ok()));
        for res in player_iter {
            match res {
                VideoPlayerResult::Success(name, session_id, mut player) => {
                    if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Some(mut old) = self.video_players.insert(name, player) {
                            tokio::spawn(async move {
                                let _ = old.stop();
                            });
                        }
                    } else {
                        // Stale player - stop in background
                        tokio::spawn(async move {
                            let _ = player.stop();
                        });
                    }
                }
                VideoPlayerResult::Failure(name, session_id) => {
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
    pub async fn housekeeping(&mut self, loop_start: Instant) {
        // Record frame time
        let frame_time = loop_start.elapsed();
        self.metrics.record_frame_time(frame_time);

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
                let texture_count = ctx.texture_pool.lock().values().map(|v| v.len()).sum();
                let pipeline_count = ctx.transition_pipelines.lock().len()
                    + ctx.blit_pipelines.lock().len()
                    + ctx.mipmap_pipelines.lock().len();
                self.metrics.record_texture_count(texture_count);
                self.metrics.record_pipeline_count(pipeline_count);
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
        for (name, (path, content_type)) in initial_changes {
            if !self.renderers.contains_key(&name) {
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
                &mut self.next_session_id,
                &self.frame_tx,
                &self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
                Some(batch_id),
                None,
                &self.image_tx,
                &self.player_tx,
                "STARTUP",
            );
        }
    }

    /// Clean shutdown — stop all video players and save caches.
    pub fn shutdown(&mut self) {
        for player in self.video_players.values_mut() {
            let _ = player.stop();
        }
        // Persist WGSL cache to disk on shutdown (P-15 cache layer 1)
        if let Err(e) = crate::shaders::ShaderManager::save_cache() {
            warn!("[SHADER] Failed to save WGSL cache: {}", e);
        }
    }
}

// ─── Standalone helpers ─────────────────────────────────────────────────────

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

        if content_type == queue::ContentType::Image {
            let target_width = r.config.width.clone();
            let target_height = r.config.height.clone();

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
                            let (orig_w, orig_h) = rgba.dimensions();

                            let (image_data, width, height) = if orig_w > target_width
                                || orig_h > target_height
                            {
                                let scale = (target_width as f32 / orig_w as f32)
                                    .max(target_height as f32 / orig_h as f32);
                                let new_w = ((orig_w as f32 * scale).round() as u32).max(1);
                                let new_h = ((orig_h as f32 * scale).round() as u32).max(1);

                                use fast_image_resize as fr;
                                let src = fr::images::Image::from_vec_u8(
                                    orig_w,
                                    orig_h,
                                    rgba.into_raw(),
                                    fr::PixelType::U8x4,
                                )
                                .unwrap();
                                let mut dst =
                                    fr::images::Image::new(new_w, new_h, fr::PixelType::U8x4);
                                let mut resizer = fr::Resizer::new();
                                resizer
                                    .resize(
                                        &src,
                                        &mut dst,
                                        &fr::ResizeOptions::new().resize_alg(
                                            fr::ResizeAlg::Convolution(fr::FilterType::Lanczos3),
                                        ),
                                    )
                                    .unwrap();
                                (dst.into_vec(), new_w, new_h)
                            } else {
                                (rgba.into_raw(), orig_w, orig_h)
                            };

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
            });
        }
    }

    if content_type == queue::ContentType::Video {
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
    frame_tx: &tokio::sync::mpsc::Sender<FrameMsg>,
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

/// Handle an IPC command request.
#[allow(clippy::too_many_arguments)]
pub async fn handle_command(
    req: Request,
    monitor_manager: &mut monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
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
