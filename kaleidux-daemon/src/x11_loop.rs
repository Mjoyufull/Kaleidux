//! X11-specific main loop.
//!
//! Contains X11 backend init, RandR event polling, and immediate rendering.
//! All shared logic lives in `main_loop::MainLoopContext`.

use crate::background::{self, BackgroundWorkKind};
use crate::content::sessions::{set_pending_video_session, stop_video_player_in_background};
use crate::main_loop::MainLoopContext;
use crate::orchestration;
use crate::renderer;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};
use x11rb::connection::Connection as X11Connection;

struct PendingX11RendererAdd {
    handle: tokio::task::JoinHandle<anyhow::Result<X11RendererInitResult>>,
    geometry: (i16, i16, u16, u16),
    started_at: Instant,
    retire_requested: bool,
    timeout_reported: bool,
}

struct X11RendererInitResult {
    renderer: renderer::Renderer,
    wgpu_ctx: Arc<renderer::WgpuContext>,
}

const HOTPLUG_RENDERER_TIMEOUT: Duration = Duration::from_secs(5);
const HOTPLUG_RENDERER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const RETIRING_RENDERER_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run(
    config: orchestration::Config,
    log_level: Option<u8>,
    gstreamer_duration: std::time::Duration,
) -> anyhow::Result<()> {
    let mut ctx = MainLoopContext::new(config.clone(), log_level, gstreamer_duration).await?;

    // ─── X11 backend init ───────────────────────────────────────────────

    let mut backend = crate::x11::X11Backend::new()?;
    let monitors = backend.get_monitors()?;
    let mut monitor_geometries: HashMap<String, (i16, i16, u16, u16)> = monitors
        .iter()
        .map(|(name, x, y, width, height)| (name.clone(), (*x, *y, *width, *height)))
        .collect();
    let mut window_to_renderer: HashMap<u32, String> = HashMap::new();
    let mut initial_surface: Option<wgpu::Surface<'static>> = None;
    let mut present_telemetry = crate::x11_present::X11PresentTelemetry::default();
    let mut randr_reconcile_deadline: Option<Instant> = None;
    let mut pending_renderer_adds: HashMap<String, PendingX11RendererAdd> = HashMap::new();
    let mut renderer_retries = crate::renderer_retry::RendererRetryBackoff::default();
    let mut next_present_summary = Instant::now() + Duration::from_secs(10);
    let mut power_monitor =
        crate::hyprland_power::HyprlandPowerMonitor::from_environment(Instant::now());

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

        for (name, (_, _, width, height)) in &monitor_geometries {
            refresh_x11_mpv_composed_target(
                &backend,
                &mut ctx,
                &wgpu_ctx,
                name,
                u32::from(*width),
                u32::from(*height),
            );
        }

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
            let Some(spawn_handler) = background::spawn_blocking_tracked_wait(
                BackgroundWorkKind::RendererInit,
                move || {
                    renderer::Renderer::new(
                        name_for_bg,
                        ctx_clone,
                        surface_arc,
                        init_surf,
                        Some(metrics_clone),
                    )
                },
            )
            .await
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
        if renderer_retries
            .next_deadline()
            .is_some_and(|deadline| loop_start >= deadline)
        {
            backend.monitors_dirty.store(true, Ordering::Release);
        }
        if ctx.shutdown_flag.load(Ordering::SeqCst) {
            ctx.shutdown().await;
            break;
        }

        let power_result_ready = power_monitor.needs_output_snapshot();
        let output_names: Vec<String> = if power_result_ready {
            ctx.renderers.keys().cloned().collect()
        } else {
            Vec::new()
        };
        if power_result_ready
            && let Some(update) = power_monitor.retain_outputs(&output_names, loop_start)
        {
            crate::hyprland_power::apply_update(&mut ctx, update);
        }
        let has_active_video = ctx
            .renderers
            .values()
            .any(|renderer| renderer.valid_content_type == crate::queue::ContentType::Video)
            || !ctx.video_players.is_empty();
        if let Some(update) = power_monitor
            .poll_if_due(&output_names, has_active_video, loop_start)
            .await
        {
            crate::hyprland_power::apply_update(&mut ctx, update);
        }
        let any_active = ctx.renderers.iter().any(|(name, renderer)| {
            power_monitor.is_powered(name) && (renderer.transition_active || renderer.needs_redraw)
        });

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
            let idle_deadline = crate::runtime::timing::min_deadline_with_reason(
                ctx.next_common_idle_deadline(loop_start),
                randr_reconcile_deadline.map(|deadline| {
                    (
                        deadline,
                        crate::observability::wake::DeadlineReason::X11Randr,
                    )
                }),
            );
            let idle_deadline = crate::runtime::timing::min_deadline_with_reason(
                idle_deadline,
                power_monitor.next_deadline(!ctx.renderers.is_empty()),
            );
            let idle_deadline = crate::runtime::timing::min_deadline_with_reason(
                idle_deadline,
                backend.monitors_dirty.load(Ordering::Acquire).then_some((
                    randr_reconcile_deadline.unwrap_or(loop_start),
                    crate::observability::wake::DeadlineReason::X11Randr,
                )),
            );
            let idle_deadline = crate::runtime::timing::min_deadline_with_reason(
                idle_deadline,
                renderer_retries.next_deadline().map(|deadline| {
                    (
                        deadline,
                        crate::observability::wake::DeadlineReason::X11Randr,
                    )
                }),
            );
            let pending_renderer_poll = if pending_renderer_adds
                .values()
                .any(|add| !add.timeout_reported && !add.retire_requested)
            {
                HOTPLUG_RENDERER_POLL_INTERVAL
            } else {
                RETIRING_RENDERER_POLL_INTERVAL
            };
            let idle_deadline = crate::runtime::timing::min_deadline_with_reason(
                idle_deadline,
                (!pending_renderer_adds.is_empty()).then_some((
                    loop_start + pending_renderer_poll,
                    crate::observability::wake::DeadlineReason::X11Randr,
                )),
            );
            let result = ctx.idle_wait(&x11_fd, idle_deadline).await;
            cmd_buf = result.cmd;
            frame_ready = result.frame_ready;
            image_buf = result.image;
            player_buf = result.player;
            player_event_buf = result.player_event;
        }

        // ─── X11 event polling ──────────────────────────────────────────

        let mut x11_event_batch = 0_u64;
        loop {
            match backend.conn.poll_for_event() {
                Ok(Some(event)) => {
                    x11_event_batch = x11_event_batch.saturating_add(1);
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
                            debug!(
                                "[X11] RandR event received, scheduling debounced reconciliation"
                            );
                            backend.monitors_dirty.store(true, Ordering::SeqCst);
                            randr_reconcile_deadline =
                                Some(Instant::now() + Duration::from_millis(100));
                            present_telemetry.randr_events =
                                present_telemetry.randr_events.saturating_add(1);
                        }
                        Event::PresentCompleteNotify(event) => {
                            present_telemetry.record_complete(&event);
                        }
                        Event::PresentIdleNotify(event) => {
                            present_telemetry.record_idle(&event);
                        }
                        Event::PresentConfigureNotify(event) => {
                            debug!(
                                "[X11-PRESENT] configure window={} {}x{}@{},{} pixmap={}x{}",
                                event.window,
                                event.width,
                                event.height,
                                event.x,
                                event.y,
                                event.pixmap_width,
                                event.pixmap_height,
                            );
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
        present_telemetry.record_event_batch(x11_event_batch);

        if backend.monitors_dirty.load(Ordering::Acquire)
            && randr_reconcile_deadline.is_none_or(|deadline| Instant::now() >= deadline)
        {
            if let Err(error) = reconcile_monitors(
                &mut backend,
                &mut ctx,
                &mut window_to_renderer,
                &mut monitor_geometries,
                &mut pending_renderer_adds,
                &mut renderer_retries,
            )
            .await
            {
                error!(
                    "[X11] RandR reconciliation failed; retaining the prior topology: {error:#}"
                );
                ctx.metrics.record_error("x11_randr_reconcile");
                backend.monitors_dirty.store(true, Ordering::Release);
                randr_reconcile_deadline = Some(loop_start + Duration::from_millis(100));
            } else {
                randr_reconcile_deadline = None;
            }
            present_telemetry.randr_reconciles =
                present_telemetry.randr_reconciles.saturating_add(1);
        }
        if let Err(error) = drain_pending_x11_renderers(
            &mut backend,
            &mut ctx,
            &mut window_to_renderer,
            &mut monitor_geometries,
            &mut pending_renderer_adds,
            &mut renderer_retries,
            loop_start,
        )
        .await
        {
            error!(
                "[X11] Completing a hotplug renderer failed; a later RandR pass will retry: {error:#}"
            );
            ctx.metrics.record_error("x11_hotplug_renderer_complete");
            backend.monitors_dirty.store(true, Ordering::Release);
            randr_reconcile_deadline = Some(loop_start + Duration::from_millis(100));
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
            if !power_monitor.is_powered(src.as_ref()) {
                drop(frame);
                continue;
            }
            let barrier_blocks = ctx.startup_barrier_blocks_output(src.as_ref(), loop_start);
            let mut mark_presented = false;
            let mut mark_ready = false;
            if let Some(r) = ctx.renderers.get_mut(src.as_ref()) {
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
                    if r.render(renderer::BackendContext::X11, loop_start).is_ok() {
                        present_telemetry.record_wsi_submit();
                    }
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
                ctx.mark_startup_output_ready(src.as_ref(), loop_start);
            }
            if mark_presented {
                ctx.mark_output_presented_if_ready(src.as_ref());
            }
        }

        // ─── Image handling (X11: immediate render) ─────────────────────

        ctx.drain_images(image_buf, loop_start, |r, _name, ls| {
            if !power_monitor.is_powered(&r.name) {
                return;
            }
            if r.render(renderer::BackendContext::X11, ls).is_ok() {
                present_telemetry.record_wsi_submit();
            }
        });

        // ─── Player results ─────────────────────────────────────────────

        ctx.drain_players(player_buf, loop_start, |r, _name, ls| {
            if !power_monitor.is_powered(&r.name) {
                return;
            }
            if r.render(renderer::BackendContext::X11, ls).is_ok() {
                present_telemetry.record_wsi_submit();
            }
        });
        if power_monitor.is_suspended() {
            for player in ctx.video_players.values() {
                let _ = player.pause();
            }
        }

        ctx.release_startup_present_barrier(loop_start, |r, name, ls| {
            if !power_monitor.is_powered(name) {
                return;
            }
            if r.render(renderer::BackendContext::X11, ls).is_ok() {
                present_telemetry.record_wsi_submit();
            }
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
            if !power_monitor.is_powered(name) {
                continue;
            }
            let barrier_blocks = blocked_outputs.contains(name);
            if (r.needs_redraw || r.transition_active) && !barrier_blocks {
                if r.render(renderer::BackendContext::X11, loop_start).is_ok() {
                    present_telemetry.record_wsi_submit();
                }
                if !ctx.first_frame_recorded {
                    ctx.metrics.record_first_frame();
                    ctx.first_frame_recorded = true;
                }
                presented_outputs.push(name.clone());
            }
        }
        let rendered_any = !presented_outputs.is_empty();
        for name in presented_outputs {
            if let Some(player) = ctx.video_players.get(&name) {
                player.request_video_frame();
            }
            ctx.mark_output_presented_if_ready(&name);
        }

        // Flush X11 commands only if something was rendered
        if rendered_any {
            let _ = backend.conn.flush();
        }

        // ─── Housekeeping ───────────────────────────────────────────────

        ctx.housekeeping(loop_start, !any_active).await;
        if loop_start >= next_present_summary {
            info!(
                "[X11-PRESENT] {}",
                present_telemetry.format_summary(backend.capabilities.server_kind())
            );
            next_present_summary = loop_start + Duration::from_secs(10);
        }
        ctx.timing_and_poll(any_active, loop_start).await;
    }

    Ok(())
}

async fn reconcile_monitors(
    backend: &mut crate::x11::X11Backend,
    ctx: &mut MainLoopContext,
    window_to_renderer: &mut HashMap<u32, String>,
    geometries: &mut HashMap<String, (i16, i16, u16, u16)>,
    pending: &mut HashMap<String, PendingX11RendererAdd>,
    retry: &mut crate::renderer_retry::RendererRetryBackoff,
) -> anyhow::Result<()> {
    let current = backend.get_connected_monitors()?;
    let current_names: HashSet<_> = current.iter().map(|(name, ..)| name.clone()).collect();
    retry.retain_outputs(&current_names);
    let mut removed: Vec<_> = geometries
        .keys()
        .chain(pending.keys())
        .chain(backend.windows.keys())
        .filter(|name| !current_names.contains(*name))
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    removed.sort();

    for name in removed {
        info!("[X11] Removing disconnected output {name}");
        let renderer_still_initializing = if let Some(add) = pending.get_mut(&name) {
            // Tokio cannot cancel an already-running spawn_blocking closure.
            // Defer XDestroyWindow until the WSI call returns.
            add.retire_requested = true;
            true
        } else {
            false
        };
        ctx.renderers.remove(&name);
        ctx.latest_video_frames.clear_source(&name);
        if let Some(player) = ctx.video_players.remove(&name) {
            stop_video_player_in_background(name.clone(), player);
        }
        if let Some(player) = ctx.pending_image_video_stops.remove(&name) {
            stop_video_player_in_background(name.clone(), player);
        }
        ctx.pending_video_switches.remove(&name);
        ctx.mpv_composed_targets.remove(&name);
        set_pending_video_session(&ctx.pending_video_sessions, &name, None);
        if let Some(barrier) = &mut ctx.startup_present_barrier {
            barrier.outputs.remove(&name);
        }
        ctx.monitor_manager.remove_output(&name);
        if !renderer_still_initializing
            && let Some(window) = backend.destroy_wallpaper_window(&name)?
        {
            window_to_renderer.remove(&window);
        }
        geometries.remove(&name);
    }

    // A transient X error during rollback can leave a window without either
    // a renderer or an in-flight init. Destroy that orphan before attempting
    // a fresh add so retries cannot replace/leak the old XID.
    let orphaned_windows = current_names
        .iter()
        .filter(|name| {
            backend.windows.contains_key(*name)
                && !geometries.contains_key(*name)
                && !pending.contains_key(*name)
        })
        .cloned()
        .collect::<Vec<_>>();
    for name in orphaned_windows {
        rollback_added_output(backend, ctx, window_to_renderer, &name)?;
    }

    for (name, x, y, width, height) in current {
        let geometry = (x, y, width, height);
        if let Some(previous) = geometries.get_mut(&name) {
            if *previous != geometry {
                if let Some(window) = backend.windows.get(&name).copied() {
                    backend.configure_wallpaper_window(window, x, y, width, height)?;
                }
                if let Some(renderer) = ctx.renderers.get_mut(&name) {
                    let _ = renderer.resize_checked(u32::from(width), u32::from(height));
                }
                if let Some(wgpu_ctx) = ctx.wgpu_ctx.clone() {
                    refresh_x11_mpv_composed_target(
                        backend,
                        ctx,
                        &wgpu_ctx,
                        &name,
                        u32::from(width),
                        u32::from(height),
                    );
                }
                *previous = geometry;
                info!("[X11] Reconfigured output {name} to {width}x{height}@{x},{y}");
            }
            continue;
        }
        if let Some(add) = pending.get_mut(&name) {
            if add.geometry != geometry {
                if let Some(window) = backend.windows.get(&name).copied() {
                    backend.configure_wallpaper_window(window, x, y, width, height)?;
                }
                add.geometry = geometry;
            }
            continue;
        }
        if ctx.wgpu_ctx.is_none() && !pending.is_empty() {
            // Serialize establishment of the one shared WGPU device. The
            // completion path marks RandR dirty so remaining outputs are
            // queued immediately afterward.
            continue;
        }
        if !retry.can_start(&name, Instant::now()) {
            continue;
        }

        info!("[X11] Adding connected output {name} at {width}x{height}@{x},{y}");
        ctx.monitor_manager.add_output(&name, "X11 Display").await;
        let window = backend.create_wallpaper_window(&name, x, y, width, height)?;
        window_to_renderer.insert(window, name.clone());
        let raw = Arc::new(crate::x11::RawX11Surface {
            window_id: window,
            connection: backend.conn.clone(),
            screen: backend.screen_num as i32,
        });
        let renderer_name = name.clone();
        let metrics = ctx.metrics.clone();
        let existing_wgpu_ctx = ctx.wgpu_ctx.clone();
        let handle = tokio::spawn(async move {
            let (renderer_wgpu_ctx, surface) = if let Some(wgpu_ctx) = existing_wgpu_ctx {
                let surface = wgpu_ctx
                    .instance
                    .create_surface(raw.clone())
                    .map_err(|error| {
                        anyhow::anyhow!("creating X11 WGPU surface failed: {error}")
                    })?;
                (wgpu_ctx, surface)
            } else {
                renderer::WgpuContext::with_surface(raw.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("initializing X11 WGPU failed: {error}"))?
            };
            let completed_wgpu_ctx = renderer_wgpu_ctx.clone();
            let Some(handle) = background::spawn_blocking_tracked_wait(
                BackgroundWorkKind::RendererInit,
                move || {
                    renderer::Renderer::new(
                        renderer_name,
                        renderer_wgpu_ctx,
                        raw,
                        Some(surface),
                        Some(metrics),
                    )
                },
            )
            .await
            else {
                anyhow::bail!("shutdown began while waiting for renderer capacity");
            };
            let renderer = handle
                .await
                .map_err(|error| anyhow::anyhow!("renderer task failed: {error}"))??;
            Ok(X11RendererInitResult {
                renderer,
                wgpu_ctx: completed_wgpu_ctx,
            })
        });
        pending.insert(
            name.clone(),
            PendingX11RendererAdd {
                handle,
                geometry,
                started_at: Instant::now(),
                retire_requested: false,
                timeout_reported: false,
            },
        );
        retry.mark_started(&name);
        info!("[X11] Hotplug renderer initialization queued for {name}");
    }
    backend.conn.flush()?;
    Ok(())
}

async fn drain_pending_x11_renderers(
    backend: &mut crate::x11::X11Backend,
    ctx: &mut MainLoopContext,
    window_to_renderer: &mut HashMap<u32, String>,
    geometries: &mut HashMap<String, (i16, i16, u16, u16)>,
    pending: &mut HashMap<String, PendingX11RendererAdd>,
    retry: &mut crate::renderer_retry::RendererRetryBackoff,
    now: Instant,
) -> anyhow::Result<()> {
    let ready = pending
        .iter()
        .filter_map(|(name, add)| add.handle.is_finished().then_some(name.clone()))
        .collect::<Vec<_>>();
    for (name, add) in pending.iter_mut() {
        if !add.timeout_reported
            && now.saturating_duration_since(add.started_at) >= HOTPLUG_RENDERER_TIMEOUT
        {
            add.timeout_reported = true;
            error!(
                "[X11] Hotplug renderer creation for {name} exceeded {:.0}s; initialization remains isolated until the non-cancellable driver call returns",
                HOTPLUG_RENDERER_TIMEOUT.as_secs_f64()
            );
            ctx.metrics.record_error("x11_hotplug_renderer_slow");
        }
    }
    let mut initialized_any = false;
    for name in ready {
        let Some(add) = pending.remove(&name) else {
            continue;
        };
        let retire_requested = add.retire_requested;
        let result = add.handle.await;
        if retire_requested {
            drop(result);
            rollback_added_output(backend, ctx, window_to_renderer, &name)?;
            backend.monitors_dirty.store(true, Ordering::Release);
            continue;
        }
        let initialized = match result {
            Ok(Ok(initialized)) => initialized,
            Ok(Err(error)) => {
                error!("[X11] Hotplug renderer creation for {name} failed: {error:#}");
                rollback_added_output(backend, ctx, window_to_renderer, &name)?;
                let delay = retry.record_failure(&name, now);
                info!("[X11] Retrying {name} in {:.0}s", delay.as_secs_f64());
                if ctx.wgpu_ctx.is_none() {
                    backend.monitors_dirty.store(true, Ordering::Release);
                }
                continue;
            }
            Err(error) => {
                error!("[X11] Hotplug renderer task for {name} failed: {error}");
                rollback_added_output(backend, ctx, window_to_renderer, &name)?;
                let delay = retry.record_failure(&name, now);
                info!("[X11] Retrying {name} in {:.0}s", delay.as_secs_f64());
                if ctx.wgpu_ctx.is_none() {
                    backend.monitors_dirty.store(true, Ordering::Release);
                }
                continue;
            }
        };
        let X11RendererInitResult {
            mut renderer,
            wgpu_ctx,
        } = initialized;
        retry.record_success(&name);
        if ctx.wgpu_ctx.is_none() {
            ctx.wgpu_ctx = Some(wgpu_ctx.clone());
            backend.monitors_dirty.store(true, Ordering::Release);
        }
        let (_, _, width, height) = add.geometry;
        let _ = renderer.resize_checked(u32::from(width), u32::from(height));
        if let Some(config) = ctx.monitor_manager.get_output_config(&name) {
            renderer.apply_config(config);
        }
        ctx.renderers.insert(name.clone(), renderer);
        refresh_x11_mpv_composed_target(
            backend,
            ctx,
            &wgpu_ctx,
            &name,
            u32::from(width),
            u32::from(height),
        );
        geometries.insert(name.clone(), add.geometry);
        info!("[X11] Hotplug renderer initialized successfully for {name}");
        initialized_any = true;
    }
    if initialized_any {
        let changes = ctx.monitor_manager.tick();
        ctx.load_content_changes(changes, "X11-HOTPLUG", false);
    }
    Ok(())
}

fn rollback_added_output(
    backend: &mut crate::x11::X11Backend,
    ctx: &mut MainLoopContext,
    window_to_renderer: &mut HashMap<u32, String>,
    name: &str,
) -> anyhow::Result<()> {
    ctx.renderers.remove(name);
    ctx.mpv_composed_targets.remove(name);
    ctx.monitor_manager.remove_output(name);
    if let Some(window) = backend.destroy_wallpaper_window(name)? {
        window_to_renderer.remove(&window);
    }
    Ok(())
}

fn refresh_x11_mpv_composed_target(
    backend: &crate::x11::X11Backend,
    ctx: &mut MainLoopContext,
    wgpu_ctx: &Arc<renderer::WgpuContext>,
    name: &str,
    width: u32,
    height: u32,
) {
    let should_create = matches!(
        crate::video::resolve_video_backend_request(crate::video::VideoBackendRequest::Auto),
        crate::video::VideoBackendRequest::ForceMpv
    ) && crate::video::MpvRenderApiRequest::from_env().enables_composed_gl();
    if !should_create {
        ctx.mpv_composed_targets.remove(name);
        return;
    }

    let connection_ptr = backend.conn.get_raw_xcb_connection();
    if let Some(target) = crate::video::MpvComposedVideoTarget::new_xcb(
        connection_ptr,
        wgpu_ctx.clone(),
        width,
        height,
    ) {
        ctx.mpv_composed_targets.insert(name.to_string(), target);
        info!("[VIDEO] {name}: prepared composed libmpv XCB GL/WGPU target {width}x{height}");
    } else {
        error!("[VIDEO] {name}: XCB connection is unavailable for composed libmpv GL/WGPU");
    }
}
