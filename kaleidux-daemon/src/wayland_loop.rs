//! Wayland-specific main loop.
//!
//! Contains surface creation, Wayland event polling, frame callback rendering,
//! and connection error recovery. All shared logic lives in `main_loop::MainLoopContext`.

use crate::background::{self, BackgroundWorkKind};
use crate::main_loop::{
    MainLoopContext, should_accept_video_frame, stop_video_player_in_background,
};
use crate::orchestration;
use crate::renderer;

use smithay_client_toolkit::shell::WaylandSurface;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{error, info, warn};
use wayland_client::{Connection, Proxy, globals::registry_queue_init};

fn ensure_native_video_target(
    ctx: &mut MainLoopContext,
    backend: &crate::wayland::WaylandBackend,
    display_ptr: *mut std::ffi::c_void,
    name: &str,
) -> Option<crate::video::NativeWaylandVideoTarget> {
    // waylandsink embeds its own subsurfaces under the supplied parent surface.
    // Reuse the wallpaper layer surface instead of creating a second layer-shell surface.
    let surface = backend.surfaces.get(name)?;

    let mut target = crate::video::NativeWaylandVideoTarget::new(
        name.to_string(),
        display_ptr,
        surface.wl_surface().id().as_ptr() as *mut std::ffi::c_void,
    );
    if let Some(renderer) = ctx.renderers.get(name) {
        target = target.with_size(renderer.config.width, renderer.config.height);
    }
    ctx.native_video_targets
        .insert(name.to_string(), target.clone());
    Some(target)
}

fn destroy_native_video_target(ctx: &mut MainLoopContext, name: &str) {
    ctx.native_video_targets.remove(name);
}

pub async fn run(
    config: orchestration::Config,
    log_level: Option<u8>,
    gstreamer_duration: std::time::Duration,
) -> anyhow::Result<()> {
    let mut ctx = MainLoopContext::new(config.clone(), log_level, gstreamer_duration).await?;

    // ─── Wayland backend init ───────────────────────────────────────────

    let conn = Connection::connect_to_env()?;
    let (globals, mut event_queue) = registry_queue_init(&conn)?;
    let qh = event_queue.handle();
    let mut backend = crate::wayland::WaylandBackend::new(&globals, &qh)?;

    event_queue.roundtrip(&mut backend)?;

    let mut initial_surface: Option<wgpu::Surface<'static>> = None;
    let display_ptr = {
        let backend_ref = conn.backend();
        backend_ref.display_ptr() as *mut std::ffi::c_void
    };

    // Phase 1: Collect all output info (fast, no IO)
    let mut output_infos: Vec<(
        String,
        String,
        wayland_client::protocol::wl_output::WlOutput,
    )> = Vec::new();
    let outputs: Vec<_> = backend.output_state.outputs().collect();
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

    // Phase 2: Initialize all outputs (monitor_manager reuses cached file lists)
    for (name, description, _) in &output_infos {
        ctx.monitor_manager.add_output(name, description).await;
    }

    // Phase 3: Create Wayland surfaces (fast, no IO)
    let mut surface_infos = Vec::new();
    for (name, _description, output) in &output_infos {
        let output_config = match ctx.monitor_manager.get_output_config(name) {
            Some(cfg) => cfg,
            None => continue,
        };

        let layer_surface = backend.create_wallpaper_surface(
            output,
            &qh,
            name.clone(),
            output_config.layer.clone().into(),
        )?;

        let raw_handle_surface = crate::wayland::RawHandleSurface {
            layer_surface,
            display_ptr,
        };
        let surface_arc = Arc::new(raw_handle_surface);
        surface_infos.push((name.clone(), surface_arc));
    }

    // Phase 4: Initialize WGPU + renderers
    if let Some((_, first_surface_arc)) = surface_infos.first() {
        info!("Initializing WGPU context with first surface as compatible...");
        let wgpu_start = Instant::now();
        let mut last_error = None;
        let mut init_result = None;
        for attempt in 1..=3 {
            match renderer::WgpuContext::with_surface(first_surface_arc.clone()).await {
                Ok(result) => {
                    init_result = Some(result);
                    break;
                }
                Err(e) => {
                    let error_text = e.to_string();
                    warn!(
                        "[STARTUP] WGPU initialization attempt {attempt}/3 failed: {}",
                        error_text
                    );
                    last_error = Some(e);
                    if !error_text.to_ascii_lowercase().contains("device is lost") || attempt == 3 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        let (wgpu_ctx, surface) = match init_result {
            Some(result) => result,
            None => return Err(last_error.expect("wgpu init retry loop must capture an error")),
        };
        let wgpu_duration = wgpu_start.elapsed();
        ctx.metrics.record_wgpu_init(wgpu_duration);
        let adapter_name = wgpu_ctx.adapter.get_info().name.clone();
        ctx.wgpu_ctx = Some(wgpu_ctx);
        initial_surface = Some(surface);
        info!("WGPU initialized on GPU: {:?}", adapter_name);
    }

    match ctx.wgpu_ctx.clone() {
        Some(wgpu_ctx) => {
            let first_name = surface_infos.first().map(|(n, _)| n.clone());

            for (name, surface_arc) in surface_infos {
                let ctx_clone = wgpu_ctx.clone();
                let is_first = Some(&name) == first_name.as_ref();
                let init_surf = if is_first {
                    initial_surface.take()
                } else {
                    None
                };

                let metrics_clone = ctx.metrics.clone();

                info!("[STARTUP] Initializing renderer for {}", name);

                let name_for_bg = name.clone();
                let Some(spawn_handler) = background::spawn_blocking_tracked(
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
                ) else {
                    error!(
                        "[STARTUP] Renderer initialization skipped for {}: shutdown in progress",
                        name
                    );
                    continue;
                };

                match tokio::time::timeout(std::time::Duration::from_secs(5), spawn_handler).await {
                    Ok(join_res) => match join_res {
                        Ok(render_res) => match render_res {
                            Ok(mut r) => {
                                if let Some(output_config) =
                                    ctx.monitor_manager.get_output_config(&name)
                                {
                                    r.apply_config(output_config);
                                }
                                ctx.renderers.insert(name.clone(), r);
                                info!("[STARTUP] Renderer initialized successfully for {}", name);
                            }
                            Err(e) => {
                                error!("Failed to create renderer for output {}: {}", name, e);
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
                wgpu_ctx.device.poll(wgpu::Maintain::Poll);
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
            info!(
                "[STARTUP] All renderers created, count: {}",
                ctx.renderers.len()
            );
        }
        _ => {
            warn!("[STARTUP] No WGPU context available, cannot create renderers!");
        }
    }

    // ─── Wait for Wayland configure events ──────────────────────────────

    info!("[STARTUP] Waiting for all renderers to be configured...");
    let total_renderers = ctx.renderers.len();
    let wait_start = Instant::now();
    let mut configured_count = 0;
    const MAX_WAIT_TIME: std::time::Duration = std::time::Duration::from_secs(5);

    while configured_count < total_renderers && wait_start.elapsed() < MAX_WAIT_TIME {
        match conn.prepare_read() {
            Some(guard) => {
                use std::os::unix::io::{AsFd, AsRawFd};
                let fd = conn.as_fd().as_raw_fd();
                let mut poll_fd = libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let timeout_ms = 10;
                let ret = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
                if ret > 0 && (poll_fd.revents & libc::POLLIN) != 0 {
                    if let Err(e) = guard.read() {
                        error!("Failed to read Wayland events: {}", e);
                    }
                    if let Err(e) = event_queue.dispatch_pending(&mut backend) {
                        error!("Failed to dispatch Wayland events: {}", e);
                    }
                    let _ = conn.flush();
                }
            }
            _ => {
                if let Err(e) = event_queue.dispatch_pending(&mut backend) {
                    error!("Failed to dispatch Wayland events: {}", e);
                }
                let _ = conn.flush();
            }
        }

        // Process pending_resizes to configure renderers
        let mut latest_resizes: HashMap<String, (u32, u32)> = HashMap::new();
        for (name, w, h, _) in backend.pending_resizes.drain(..) {
            latest_resizes.insert(name, (w, h));
        }
        for (name, (w, h)) in latest_resizes {
            if let Some(r) = ctx.renderers.get_mut(&name) {
                let width = if w == 0 { r.config.width } else { w };
                let height = if h == 0 { r.config.height } else { h };
                let _ = r.resize_checked(width, height);
                if let Some(target) = ctx.native_video_targets.get_mut(&name) {
                    target.width = width.max(1) as i32;
                    target.height = height.max(1) as i32;
                }
                if let Some(player) = ctx.video_players.get(&name) {
                    let _ = player.update_direct_surface_size(width, height);
                }
            }
        }

        configured_count = ctx.renderers.values().filter(|r| r.configured).count();
        if configured_count < total_renderers {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    if configured_count < total_renderers {
        warn!(
            "[STARTUP] Only {}/{} renderers configured after {}ms timeout. Some wallpapers may not load initially.",
            configured_count,
            total_renderers,
            wait_start.elapsed().as_millis()
        );
        for (name, r) in ctx.renderers.iter() {
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

    // Force initial renders for unconfigured renderers
    let mut initial_callback_flush_needed = false;
    for (name, r) in ctx.renderers.iter_mut() {
        if !r.configured && r.config.width > 0 && r.config.height > 0 {
            if let Some(layer_surface) = backend.surfaces.get(name) {
                let _ = r.resize_checked(r.config.width, r.config.height);
                if r.configured {
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                        },
                        Instant::now(),
                    );
                    initial_callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
                }
            }
        }
    }
    if initial_callback_flush_needed {
        let _ = conn.flush();
    }

    // ─── Initial load ───────────────────────────────────────────────────

    ctx.initial_load();

    // ─── Wayland connection state ───────────────────────────────────────

    let mut connection_error_count = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 3;
    let mut connection_dead = false;
    let mut last_error_time = Instant::now();

    // ─── Main Loop ──────────────────────────────────────────────────────

    let wayland_fd = {
        use std::os::unix::io::{AsFd, AsRawFd};
        tokio::io::unix::AsyncFd::new(conn.as_fd().as_raw_fd())
            .expect("Failed to create AsyncFd for Wayland connection")
    };

    loop {
        let loop_start = Instant::now();
        if ctx.shutdown_flag.load(Ordering::SeqCst) {
            ctx.shutdown().await;
            break;
        }
        let mut callback_flush_needed = false;

        if connection_dead {
            if last_error_time.elapsed().as_secs() > 5 {
                connection_dead = false;
                connection_error_count = 0;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        }

        let hot_loop_active = ctx.wayland_hot_loop_active();
        if hot_loop_active {
            ctx.metrics.record_wayland_hot_loop();
        } else {
            ctx.metrics.record_wayland_idle_loop();
        }

        // Idle — block until any event source is ready
        let (
            mut cmd_buf,
            mut frame_ready,
            mut wayland_fd_ready,
            mut image_buf,
            mut player_buf,
            mut player_event_buf,
        ) = (None, false, false, None, None, None);
        let mut entered_idle_wait = false;
        if !hot_loop_active && !connection_dead {
            let idle_deadline = ctx.next_wayland_idle_deadline(loop_start);
            let result = ctx.idle_wait(&wayland_fd, idle_deadline).await;
            cmd_buf = result.0;
            frame_ready = result.1;
            wayland_fd_ready = result.2;
            image_buf = result.3;
            player_buf = result.4;
            player_event_buf = result.5;
            entered_idle_wait = true;
        }

        // ─── Wayland event polling ──────────────────────────────────────

        if hot_loop_active || wayland_fd_ready {
            if let Some(guard) = conn.prepare_read() {
                use std::os::unix::io::{AsFd, AsRawFd};
                let fd = conn.as_fd().as_raw_fd();
                let mut poll_fd = libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let ret = unsafe { libc::poll(&mut poll_fd, 1, 0) };
                if ret > 0 && (poll_fd.revents & libc::POLLIN != 0) {
                    let _ = guard.read();
                }
            }

            match event_queue.dispatch_pending(&mut backend) {
                Ok(_) => {
                    connection_error_count = 0;
                }
                Err(e) => {
                    let error_str = e.to_string();
                    error!("Failed to dispatch Wayland events in main loop: {}", e);
                    connection_error_count += 1;
                    last_error_time = Instant::now();
                    if connection_error_count >= MAX_CONSECUTIVE_ERRORS {
                        connection_dead = true;
                    }
                    if !error_str.contains("Broken pipe") {
                        tracing::debug!(
                            "[WAYLAND] Non-broken-pipe dispatch error (count={})",
                            connection_error_count
                        );
                    }
                }
            }

            if !connection_dead {
                let needs_flush = ctx
                    .renderers
                    .values()
                    .any(|r| r.needs_redraw || r.transition_active);
                if needs_flush {
                    let _ = conn.flush();
                }
            }
        }

        // ─── Wayland-specific: orphan cleanup + resize ──────────────────

        {
            let active_output_names: std::collections::HashSet<String> = backend
                .output_state
                .outputs()
                .filter_map(|o| backend.output_state.info(&o).and_then(|i| i.name.clone()))
                .collect();
            let removed_outputs: Vec<String> = ctx
                .renderers
                .keys()
                .filter(|name| !active_output_names.contains(*name))
                .cloned()
                .collect();
            for name in &removed_outputs {
                destroy_native_video_target(&mut ctx, name);
                if let Some(vp) = ctx.video_players.remove(name) {
                    stop_video_player_in_background(name.clone(), vp);
                }
                ctx.pending_video_switches.remove(name);
                crate::main_loop::set_pending_video_session(
                    &ctx.pending_video_sessions,
                    name,
                    None,
                );
            }
            ctx.renderers
                .retain(|name, _| active_output_names.contains(name));

            let mut latest_resizes: HashMap<String, (u32, u32)> = HashMap::new();
            for (name, w, h, _) in backend.pending_resizes.drain(..) {
                latest_resizes.insert(name, (w, h));
            }
            for (name, (w, h)) in latest_resizes {
                if let Some(r) = ctx.renderers.get_mut(&name) {
                    let width = if w == 0 { r.config.width } else { w };
                    let height = if h == 0 { r.config.height } else { h };
                    let _ = r.resize_checked(width, height);
                    if let Some(target) = ctx.native_video_targets.get_mut(&name) {
                        target.width = width.max(1) as i32;
                        target.height = height.max(1) as i32;
                    }
                    if let Some(player) = ctx.video_players.get(&name) {
                        let _ = player.update_direct_surface_size(width, height);
                    }
                    if r.configured && !r.is_direct_video_presentation_active() {
                        if let Some(layer_surface) = backend.surfaces.get(&name) {
                            let _ = r.render(
                                renderer::BackendContext::Wayland {
                                    surface: layer_surface,
                                    qh: &qh,
                                },
                                loop_start,
                            );
                            callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
                        }
                    }
                }
            }
        }

        // ─── Shared logic ───────────────────────────────────────────────

        ctx.process_scheduled(loop_start);
        ctx.process_script_tick();
        ctx.drain_commands(cmd_buf, loop_start).await;
        ctx.drain_player_events(player_event_buf, loop_start);

        // ─── Frame handling (Wayland-specific upload + render) ───────────

        let (latest_frames, _frames_received, _frames_discarded) =
            ctx.drain_frames(hot_loop_active || frame_ready, true);
        for (source_id, frame) in latest_frames {
            let barrier_blocks = ctx.startup_barrier_blocks_output(source_id.as_str(), loop_start);
            let mut mark_presented = false;
            let mut mark_ready = false;
            let mut schedule_direct_handoff = false;
            if let Some(r) = ctx.renderers.get_mut(source_id.as_str()) {
                let had_current_texture = r.has_current_texture();
                let should_upload = if r.valid_content_type == crate::queue::ContentType::Video {
                    !had_current_texture
                        || !r.frame_callback_pending
                        || r.frame_callback_pending_too_long(1000)
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

                if r.valid_content_type == crate::queue::ContentType::Video {
                    if let Some(layer_surface) = backend.surfaces.get(source_id.as_str()) {
                        if !barrier_blocks {
                            if mark_ready && !had_current_texture {
                                let _ = r.render(
                                    renderer::BackendContext::Wayland {
                                        surface: layer_surface,
                                        qh: &qh,
                                    },
                                    loop_start,
                                );
                                if !ctx.first_frame_recorded {
                                    ctx.metrics.record_first_frame();
                                    ctx.first_frame_recorded = true;
                                }
                                mark_presented = true;
                                schedule_direct_handoff = r.transition_just_completed();
                                callback_flush_needed |=
                                    r.request_frame_callback(layer_surface, &qh);
                            } else if r.frame_callback_pending_too_long(1000) {
                                let _ = r.render(
                                    renderer::BackendContext::Wayland {
                                        surface: layer_surface,
                                        qh: &qh,
                                    },
                                    loop_start,
                                );
                                if !ctx.first_frame_recorded {
                                    ctx.metrics.record_first_frame();
                                    ctx.first_frame_recorded = true;
                                }
                                mark_presented = true;
                                callback_flush_needed |=
                                    r.request_frame_callback(layer_surface, &qh);
                            } else if !r.frame_callback_pending {
                                callback_flush_needed |=
                                    r.request_frame_callback(layer_surface, &qh);
                            }
                        }
                    }
                }
            } else {
                drop(frame);
            }
            if mark_ready {
                ctx.mark_startup_output_ready(source_id.as_str(), loop_start);
            }
            if mark_presented {
                ctx.mark_output_presented_if_ready(source_id.as_str());
            }
            if schedule_direct_handoff {
                ctx.maybe_schedule_direct_wayland_handoff(source_id.as_str());
            }
        }

        // ─── Image handling (Wayland render via closure) ─────────────────

        ctx.drain_images(image_buf, loop_start, |r, name, ls| {
            if r.configured {
                if let Some(layer_surface) = backend.surfaces.get(name) {
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                        },
                        ls,
                    );
                }
            }
        });

        // ─── Player results ─────────────────────────────────────────────

        ctx.drain_players(player_buf, loop_start, |r, name, ls| {
            if r.configured {
                if let Some(layer_surface) = backend.surfaces.get(name) {
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                        },
                        ls,
                    );
                }
            }
        });

        if crate::video::native_wayland_backend_enabled() {
            let handoff_candidates: Vec<String> = ctx
                .renderers
                .iter()
                .filter_map(|(name, renderer)| {
                    if !renderer.ready_for_direct_video_handoff()
                        || renderer.is_direct_video_presentation_active()
                        || ctx.startup_barrier_blocks_output(name, loop_start)
                    {
                        return None;
                    }

                    let player = ctx.video_players.get(name)?;
                    if !player.is_appsink_backend()
                        || player.session_id() == 0
                        || player.session_id() != renderer.active_video_session_id
                    {
                        return None;
                    }

                    Some(name.clone())
                })
                .collect();
            for name in handoff_candidates {
                let _ = ensure_native_video_target(&mut ctx, &backend, display_ptr, &name);
                ctx.maybe_schedule_direct_wayland_handoff(&name);
            }
        }

        let kept_native_targets: std::collections::HashSet<String> = ctx
            .renderers
            .iter()
            .filter_map(|(name, renderer)| {
                renderer
                    .is_direct_video_presentation_active()
                    .then_some(name.clone())
            })
            .chain(ctx.pending_direct_handoffs.keys().cloned())
            .collect();
        let stale_native_targets: Vec<String> = ctx
            .native_video_targets
            .keys()
            .filter(|name| !kept_native_targets.contains(*name))
            .cloned()
            .collect();
        for name in stale_native_targets {
            destroy_native_video_target(&mut ctx, &name);
        }

        ctx.release_startup_present_barrier(loop_start, |r, name, ls| {
            if r.configured {
                if let Some(layer_surface) = backend.surfaces.get(name) {
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                        },
                        ls,
                    );
                }
            }
        });

        let direct_heartbeat_blocked_outputs: HashSet<String> = ctx
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
        for (name, r) in ctx.renderers.iter_mut() {
            if !r.is_direct_video_presentation_active()
                || direct_heartbeat_blocked_outputs.contains(name)
            {
                continue;
            }

            if r.direct_video_heartbeat_due(renderer::DIRECT_VIDEO_PARENT_HEARTBEAT_INTERVAL) {
                if let Err(e) = r.present_direct_video_heartbeat() {
                    warn!(
                        "[VIDEO] {}: direct video parent heartbeat present failed: {}",
                        name, e
                    );
                }
            }
        }

        // ─── Wayland frame callback rendering ───────────────────────────

        let frame_ready_names: Vec<String> = backend.frame_callback_ready.drain().collect();
        for name in frame_ready_names {
            ctx.metrics.record_wayland_callback_wake();
            let barrier_blocks = ctx.startup_barrier_blocks_output(&name, loop_start);
            let mut mark_presented = false;
            let deferred_frame = ctx.latest_video_frames.take_frame(&name);
            if let Some(r) = ctx.renderers.get_mut(&name) {
                if r.is_direct_video_presentation_active() {
                    r.frame_callback_pending = false;
                    r.last_frame_request = None;
                    continue;
                }
                if let Some(frame) = deferred_frame
                    && should_accept_video_frame(
                        r.valid_content_type,
                        r.active_video_session_id,
                        frame.session_id,
                    )
                {
                    let upload_start = std::time::Instant::now();
                    r.upload_frame(&frame);
                    ctx.metrics.record_video_cpu_time(upload_start.elapsed());
                    ctx.metrics.record_video_frame_uploaded();
                }
                if let Some(wait_duration) = r.frame_callback_pending_duration() {
                    if wait_duration > std::time::Duration::from_millis(250) {
                        tracing::warn!(
                            "[FRAME] {}: Wayland frame callback stalled for {:.1}ms",
                            name,
                            wait_duration.as_secs_f64() * 1000.0
                        );
                    } else if wait_duration > std::time::Duration::from_millis(16) {
                        tracing::trace!(
                            "[FRAME] {}: Wayland frame callback waited {:.1}ms",
                            name,
                            wait_duration.as_secs_f64() * 1000.0
                        );
                    }
                }
                r.frame_callback_pending = false;
                r.last_frame_request = None;
                if !barrier_blocks {
                    if let Some(layer_surface) = backend.surfaces.get(&name) {
                        let _ = r.render(
                            renderer::BackendContext::Wayland {
                                surface: layer_surface,
                                qh: &qh,
                            },
                            loop_start,
                        );
                        if !ctx.first_frame_recorded {
                            ctx.metrics.record_first_frame();
                            ctx.first_frame_recorded = true;
                        }
                        mark_presented = true;
                    }
                }
            }
            if mark_presented {
                ctx.mark_output_presented_if_ready(&name);
            }
        }

        // Request missing frames
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
        for (name, r) in ctx.renderers.iter_mut() {
            let barrier_blocks = blocked_outputs.contains(name);
            let should_request = if r.is_direct_video_presentation_active() {
                false
            } else {
                r.has_any_content() && (r.needs_redraw || r.transition_active)
            } && !barrier_blocks;
            if should_request {
                if let Some(layer_surface) = backend.surfaces.get(name) {
                    callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
                }
            }
        }
        if callback_flush_needed && !connection_dead {
            let _ = conn.flush();
        }

        // ─── Housekeeping ───────────────────────────────────────────────

        ctx.housekeeping(loop_start, entered_idle_wait).await;
        ctx.timing_and_poll(hot_loop_active, loop_start).await;
    }

    Ok(())
}
