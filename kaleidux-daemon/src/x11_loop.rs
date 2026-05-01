//! X11-specific main loop.
//!
//! Contains X11 backend init, RandR event polling, and immediate rendering.
//! All shared logic lives in `main_loop::MainLoopContext`.

use crate::background::{self, BackgroundWorkKind};
use crate::main_loop::MainLoopContext;
use crate::orchestration;
use crate::renderer;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, error, info};
use x11rb::connection::Connection as X11Connection;

pub async fn run(
    config: orchestration::Config,
    log_level: Option<u8>,
    gstreamer_duration: std::time::Duration,
) -> anyhow::Result<()> {
    let mut ctx = MainLoopContext::new(config.clone(), log_level, gstreamer_duration).await?;

    // ─── X11 backend init ───────────────────────────────────────────────

    let mut backend = crate::x11::X11Backend::new()?;
    let monitors = backend.get_monitors()?;
    let mut window_to_renderer: HashMap<u32, String> = HashMap::new();
    let mut initial_surface: Option<wgpu::Surface<'static>> = None;

    let mut surface_infos = Vec::new();
    for (name, x, y, width, height) in monitors {
        ctx.monitor_manager.add_output(&name, "X11 Display").await;
        let win = backend.create_wallpaper_window(&name, x, y, width, height)?;
        window_to_renderer.insert(win, name.clone());

        let raw_handle = crate::x11::RawX11Surface {
            window_id: win,
            connection: backend.conn.clone(),
            screen: backend.screen_num as i32,
        };
        let surface_arc = Arc::new(raw_handle);
        surface_infos.push((name, surface_arc, width, height));
    }

    // Initialize WGPU + renderers
    if let Some((_, surface_arc, _, _)) = surface_infos.first() {
        info!("Initializing WGPU context with first surface as compatible...");
        let wgpu_start = Instant::now();
        let (wgpu_ctx, surface) = renderer::WgpuContext::with_surface(surface_arc.clone()).await?;
        let wgpu_duration = wgpu_start.elapsed();
        ctx.metrics.record_wgpu_init(wgpu_duration);
        ctx.wgpu_ctx = Some(wgpu_ctx);
        initial_surface = Some(surface);
    }

    if let Some(wgpu_ctx) = ctx.wgpu_ctx.clone() {
        let first_name = surface_infos.first().map(|(n, _, _, _)| n.clone());

        for (name, surface_arc, width, height) in surface_infos {
            let ctx_clone = wgpu_ctx.clone();
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

            let metrics_clone = ctx.metrics.clone();

            info!("[STARTUP-X11] Initializing renderer for {}", name);
            let name_for_bg = name.clone();
            let Some(spawn_handler) =
                background::spawn_blocking_tracked(BackgroundWorkKind::RendererInit, move || {
                    renderer::Renderer::new(
                        name_for_bg,
                        ctx_clone,
                        surface_arc,
                        init_surf,
                        Some(metrics_clone),
                    )
                })
            else {
                error!(
                    "[STARTUP-X11] Renderer initialization skipped for {}: shutdown in progress",
                    name
                );
                continue;
            };

            match tokio::time::timeout(std::time::Duration::from_secs(5), spawn_handler).await {
                Ok(join_res) => match join_res {
                    Ok(render_res) => match render_res {
                        Ok(mut r) => {
                            let _ = r.resize_checked(width as u32, height as u32);
                            if let Some(cfg) = ctx.monitor_manager.get_output_config(&name) {
                                r.apply_config(cfg);
                            }
                            ctx.renderers.insert(name, r);
                        }
                        Err(e) => {
                            error!("Failed to create renderer for {}: {}", name, e);
                            ctx.metrics.record_error("renderer_creation");
                        }
                    },
                    Err(e) => {
                        error!("Thread join error for output {}: {}", name, e);
                        ctx.metrics.record_error("renderer_thread_error");
                    }
                },
                Err(_) => {
                    error!(
                        "TIMEOUT: Renderer initialization for {} took longer than 5s. Skipping.",
                        name
                    );
                    ctx.metrics.record_error("renderer_creation_timeout");
                }
            }
        }

        let should_warmup_cuda = std::fs::metadata("/proc/driver/nvidia/gpus").is_ok()
            && ctx
                .monitor_manager
                .outputs
                .values()
                .any(|orch| orch.config.video_ratio > 0);
        if should_warmup_cuda {
            let warmup_ctx = wgpu_ctx.clone();
            if let Some(handle) =
                background::spawn_blocking_tracked(BackgroundWorkKind::CudaWarmup, move || {
                    warmup_ctx.warmup_cuda_interop()
                })
            {
                drop(handle);
            }
        }

        ctx.metrics.record_full_init();
        if log_level.map(|l| l >= 3).unwrap_or(false) {
            ctx.metrics.log_startup_summary();
        }
    }

    // ─── Initial load ───────────────────────────────────────────────────

    ctx.initial_load();

    // ─── Main Loop ──────────────────────────────────────────────────────

    let x11_fd = {
        use std::os::unix::io::AsRawFd;
        tokio::io::unix::AsyncFd::new(backend.conn.as_raw_fd())?
    };

    loop {
        let loop_start = Instant::now();
        if ctx.shutdown_flag.load(Ordering::SeqCst) {
            ctx.shutdown().await;
            break;
        }

        let any_active = ctx.any_active();

        // Idle — block until any event source is ready
        let (
            mut cmd_buf,
            mut frame_ready,
            _x11_fd_ready,
            mut image_buf,
            mut player_buf,
            mut player_event_buf,
        ) = (None, false, false, None, None, None);
        if !any_active {
            let idle_deadline = ctx.next_common_idle_deadline(loop_start);
            let result = ctx.idle_wait(&x11_fd, idle_deadline).await;
            cmd_buf = result.0;
            frame_ready = result.1;
            image_buf = result.3;
            player_buf = result.4;
            player_event_buf = result.5;
        }

        // ─── X11 event polling ──────────────────────────────────────────

        loop {
            match backend.conn.poll_for_event() {
                Ok(Some(event)) => {
                    use x11rb::protocol::Event;
                    match event {
                        Event::ConfigureNotify(ev) => {
                            if let Some(name) = window_to_renderer.get(&ev.window) {
                                if let Some(r) = ctx.renderers.get_mut(name) {
                                    let _ = r.resize_checked(ev.width as u32, ev.height as u32);
                                }
                            }
                        }
                        Event::Expose(ev) => {
                            if let Some(name) = window_to_renderer.get(&ev.window) {
                                if let Some(r) = ctx.renderers.get_mut(name) {
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
                }
                Ok(None) => break,
                Err(err) => {
                    error!("[X11] Connection error while polling events: {}", err);
                    ctx.shutdown().await;
                    return Err(err.into());
                }
            }
        }

        // ─── Shared logic ───────────────────────────────────────────────

        ctx.process_script_tick();
        ctx.process_scheduled(loop_start);
        ctx.drain_commands(cmd_buf, loop_start).await;
        ctx.drain_player_events(player_event_buf, loop_start);

        // ─── Frame handling (X11: immediate render) ─────────────────────

        let (latest_frames, _frames_received, _frames_discarded) =
            ctx.drain_frames(any_active || frame_ready, false);
        for (src, frame) in latest_frames {
            let barrier_blocks = ctx.startup_barrier_blocks_output(src.as_str(), loop_start);
            let mut mark_presented = false;
            let mut mark_ready = false;
            if let Some(r) = ctx.renderers.get_mut(src.as_str()) {
                let should_upload = if r.valid_content_type == crate::queue::ContentType::Video {
                    // Video: always upload (X11 has no callback mechanism)
                    true
                } else {
                    !r.frame_callback_pending || !r.has_current_texture()
                };

                if should_upload {
                    let video_start = std::time::Instant::now();
                    r.upload_frame(&frame);
                    let video_duration = video_start.elapsed();
                    ctx.metrics.record_video_cpu_time(video_duration);
                    ctx.metrics.record_video_frame_uploaded();
                    mark_ready = true;
                    drop(frame);
                } else {
                    drop(frame);
                }

                // X11: render non-video here; video is handled in the shared render loop below.
                if !barrier_blocks && r.valid_content_type != crate::queue::ContentType::Video {
                    let _ = r.render(renderer::BackendContext::X11, loop_start);
                    if !ctx.first_frame_recorded {
                        ctx.metrics.record_first_frame();
                        ctx.first_frame_recorded = true;
                    }
                    mark_presented = true;
                }
            } else {
                drop(frame);
            }
            if mark_ready {
                ctx.mark_startup_output_ready(src.as_str(), loop_start);
            }
            if mark_presented {
                ctx.mark_output_presented_if_ready(src.as_str());
            }
        }

        // ─── Image handling (X11: immediate render) ─────────────────────

        ctx.drain_images(image_buf, loop_start, |r, _name, ls| {
            let _ = r.render(renderer::BackendContext::X11, ls);
        });

        // ─── Player results ─────────────────────────────────────────────

        ctx.drain_players(player_buf, loop_start, |r, _name, ls| {
            let _ = r.render(renderer::BackendContext::X11, ls);
        });

        ctx.release_startup_present_barrier(loop_start, |r, _name, ls| {
            let _ = r.render(renderer::BackendContext::X11, ls);
        });

        // ─── X11 render loop ────────────────────────────────────────────

        let blocked_outputs: HashSet<String> = ctx
            .startup_present_barrier
            .as_ref()
            .map(|barrier| {
                barrier
                    .outputs
                    .iter()
                    .filter_map(|(name, state)| {
                        if state.can_block && barrier.release_reason.is_none() {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut presented_outputs = Vec::new();
        for (name, r) in ctx.renderers.iter_mut() {
            let barrier_blocks = blocked_outputs.contains(name);
            if (r.needs_redraw || r.transition_active) && !barrier_blocks {
                let _ = r.render(renderer::BackendContext::X11, loop_start);
                if !ctx.first_frame_recorded {
                    ctx.metrics.record_first_frame();
                    ctx.first_frame_recorded = true;
                }
                presented_outputs.push(name.clone());
            }
        }
        let rendered_any = !presented_outputs.is_empty();
        for name in presented_outputs {
            ctx.mark_output_presented_if_ready(&name);
        }

        // Flush X11 commands only if something was rendered
        if rendered_any {
            let _ = backend.conn.flush();
        }

        // ─── Housekeeping ───────────────────────────────────────────────

        ctx.housekeeping(loop_start, !any_active).await;
        ctx.timing_and_poll(any_active, loop_start).await;
    }

    Ok(())
}
