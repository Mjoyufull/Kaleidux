//! Wayland-specific main loop.
//!
//! Contains surface creation, Wayland event polling, frame callback rendering,
//! and connection error recovery. All shared logic lives in `main_loop::MainLoopContext`.

use crate::main_loop::MainLoopContext;
use crate::observability::wake::{DeadlineReason, WakeReason};
use crate::orchestration;
use crate::renderer;
use crate::wayland::{frame_callbacks, startup};

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use wayland_client::{Connection, globals::registry_queue_init};

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
    let mut pending_renderer_adds = HashMap::new();
    let mut renderer_retries = crate::renderer_retry::RendererRetryBackoff::default();

    event_queue.roundtrip(&mut backend)?;

    startup::initialize_outputs_and_renderers(
        &mut ctx,
        &conn,
        &mut backend,
        &qh,
        &mut event_queue,
        log_level,
    )
    .await?;

    ctx.initial_load();

    // ─── Main Loop ──────────────────────────────────────────────────────

    let wayland_fd = {
        use std::os::unix::io::{AsFd, AsRawFd};
        tokio::io::unix::AsyncFd::new(conn.as_fd().as_raw_fd())
            .expect("Failed to create AsyncFd for Wayland connection")
    };
    let mut last_presentation_log = Instant::now();
    let mut power_monitor =
        crate::hyprland_power::HyprlandPowerMonitor::from_environment(Instant::now());
    let mut deferred_power_resizes: HashMap<String, (u32, u32)> = HashMap::new();

    loop {
        let loop_start = Instant::now();
        if renderer_retries
            .next_deadline()
            .is_some_and(|deadline| loop_start >= deadline)
        {
            backend.outputs_changed = true;
        }
        if ctx.shutdown_flag.load(Ordering::SeqCst) {
            ctx.shutdown().await;
            break;
        }
        let mut callback_flush_needed = false;

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

        let renderer_activity =
            ctx.renderer_activity_snapshot_for(|name| power_monitor.is_powered(name));
        let hot_loop_active = renderer_activity.wayland_hot;
        if crate::observability::trace_all::trace_all_enabled() {
            tracing::trace!(
                "[TRACE5][LOOP] hot={} any_active={} renderers={} players={} pending_frames={}",
                hot_loop_active,
                renderer_activity.any_active,
                ctx.renderers.len(),
                ctx.video_players.len(),
                ctx.latest_video_frames.occupancy()
            );
        }
        if hot_loop_active {
            ctx.metrics.record_wayland_hot_loop();
            ctx.metrics.record_wake_reason(WakeReason::Immediate);
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
        if !hot_loop_active {
            let mut idle_deadline =
                ctx.next_wayland_idle_deadline_from_snapshot(loop_start, renderer_activity);
            if let Some(hotplug_deadline) = backend.hotplug_wsi_drain_deadline
                && idle_deadline.is_none_or(|(deadline, _)| hotplug_deadline < deadline)
            {
                idle_deadline = Some((hotplug_deadline, DeadlineReason::WaylandRetry));
            }
            if let Some(renderer_deadline) = crate::wayland::hotplug::pending_renderer_deadline(
                &pending_renderer_adds,
                loop_start,
            ) && idle_deadline.is_none_or(|(deadline, _)| renderer_deadline < deadline)
            {
                idle_deadline = Some((renderer_deadline, DeadlineReason::WaylandRetry));
            }
            if let Some(retry_deadline) = renderer_retries.next_deadline()
                && idle_deadline.is_none_or(|(deadline, _)| retry_deadline < deadline)
            {
                idle_deadline = Some((retry_deadline, DeadlineReason::WaylandRetry));
            }
            if backend.outputs_changed {
                // Renderer completion can re-arm topology reconciliation after
                // the Wayland fd was already drained. Do not defer the retry
                // to the generic housekeeping deadline.
                idle_deadline = Some((loop_start, DeadlineReason::WaylandRetry));
            }
            idle_deadline = crate::runtime::timing::min_deadline_with_reason(
                idle_deadline,
                power_monitor.next_deadline(!ctx.renderers.is_empty()),
            );
            let result = ctx.idle_wait(&wayland_fd, idle_deadline).await;
            cmd_buf = result.cmd;
            frame_ready = result.frame_ready;
            wayland_fd_ready = result.fd_ready;
            image_buf = result.image;
            player_buf = result.player;
            player_event_buf = result.player_event;
            entered_idle_wait = true;
        }

        // Video-frame wakes can race with Wayland frame callback readiness. Poll the
        // Wayland fd opportunistically for composed rendering, but direct native
        // surfaces do not use renderer frame callbacks. Their buffer-release event
        // is dispatched by its own fd wake, so polling here would duplicate the
        // Wayland read/dispatch work once per decoded frame.
        let direct_native_video_wake = frame_ready && {
            let mut active_video = ctx.renderers.iter().filter(|(_, renderer)| {
                renderer.valid_content_type == crate::queue::ContentType::Video
            });
            active_video.clone().next().is_some()
                && active_video.all(|(name, renderer)| {
                    power_monitor.is_powered(name)
                        && renderer.can_present_native_wayland_surface()
                        && backend.native_dmabuf_surface_active(name)
                })
        };
        if hot_loop_active || wayland_fd_ready || (frame_ready && !direct_native_video_wake) {
            // Follow wayland-client's external-event-loop contract exactly:
            // drain events already queued in userspace before prepare_read().
            // Otherwise prepare_read() can repeatedly return None while the
            // socket remains epoll-readable, creating a high-CPU readiness
            // loop and delaying async linux-dmabuf created/failed events.
            match event_queue.dispatch_pending(&mut backend) {
                Ok(_) => {}
                Err(error) => {
                    ctx.shutdown().await;
                    drop(ctx);
                    return Err(anyhow::anyhow!(
                        "fatal Wayland dispatch error (connection cannot be reused): {error}"
                    ));
                }
            }
            if let Some(guard) = conn.prepare_read() {
                use std::os::unix::io::{AsFd, AsRawFd};
                let fd = conn.as_fd().as_raw_fd();
                let mut poll_fd = libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: `poll_fd` points to one initialized `libc::pollfd`, the count is 1,
                // and the file descriptor is borrowed from a live Wayland connection.
                let ret = unsafe { libc::poll(&mut poll_fd, 1, 0) };
                if ret > 0 && (poll_fd.revents & libc::POLLIN != 0) {
                    if let Err(error) = guard.read() {
                        ctx.shutdown().await;
                        drop(ctx);
                        return Err(anyhow::anyhow!(
                            "fatal Wayland socket read error (connection cannot be reused): {error}"
                        ));
                    }
                }
            }

            match event_queue.dispatch_pending(&mut backend) {
                Ok(_) => {}
                Err(e) => {
                    ctx.shutdown().await;
                    drop(ctx);
                    return Err(anyhow::anyhow!(
                        "fatal Wayland dispatch error (connection cannot be reused): {e}"
                    ));
                }
            }
        }

        // ─── Wayland-specific: orphan cleanup + resize ──────────────────

        if backend.outputs_changed {
            backend.outputs_changed = false;
            callback_flush_needed |= crate::wayland::hotplug::reconcile_outputs(
                &mut ctx,
                &conn,
                &mut backend,
                &mut pending_renderer_adds,
                &mut renderer_retries,
                &qh,
                loop_start,
            )
            .await;
        }
        crate::wayland::hotplug::drain_pending_renderer_adds(
            &mut ctx,
            &mut backend,
            &mut pending_renderer_adds,
            &mut renderer_retries,
            loop_start,
        )
        .await;
        // A failed first-device attempt may have serialized other connected
        // outputs. Give those outputs one immediate reconciliation pass; the
        // failed output itself remains behind its per-output backoff.
        if backend.outputs_changed {
            backend.outputs_changed = false;
            callback_flush_needed |= crate::wayland::hotplug::reconcile_outputs(
                &mut ctx,
                &conn,
                &mut backend,
                &mut pending_renderer_adds,
                &mut renderer_retries,
                &qh,
                loop_start,
            )
            .await;
        }
        callback_flush_needed |= crate::wayland::hotplug::drain_delayed_wsi_feedback(
            &mut ctx,
            &mut backend,
            &qh,
            loop_start,
        );

        {
            // Keep the newest configure while an output is powered off. A
            // configure is one-shot protocol state; dropping it here would
            // leave the renderer at the pre-DPMS mode/scale indefinitely.
            let mut latest_resizes = std::mem::take(&mut deferred_power_resizes);
            for (name, w, h, _) in backend.pending_resizes.drain(..) {
                latest_resizes.insert(name, (w, h));
            }
            for (name, (w, h)) in latest_resizes {
                if !power_monitor.is_powered(&name) {
                    deferred_power_resizes.insert(name, (w, h));
                    continue;
                }
                if let Some(r) = ctx.renderers.get_mut(&name) {
                    let width = if w == 0 { r.config.width } else { w };
                    let height = if h == 0 { r.config.height } else { h };
                    let _ = r.resize_checked(width, height);
                    if r.configured {
                        if let Some(layer_surface) = backend.surfaces.get(&name) {
                            let _ = r.render(
                                renderer::BackendContext::Wayland {
                                    surface: layer_surface,
                                    qh: &qh,
                                    presentation: backend.presentation_proxy(),
                                },
                                loop_start,
                            );
                            callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
                        }
                    }
                } else {
                    // Hotplug renderer creation is asynchronous. Preserve the
                    // compositor's fractionally-scaled buffer extent until the
                    // renderer is ready instead of falling back to logical
                    // output dimensions.
                    crate::wayland::hotplug::record_pending_resize(
                        &mut pending_renderer_adds,
                        &name,
                        (w, h),
                    );
                }
            }
        }

        ctx.process_scheduled(loop_start);
        ctx.process_script_tick();
        if hot_loop_active || cmd_buf.is_some() {
            ctx.drain_commands(cmd_buf, loop_start).await;
        }
        if hot_loop_active || player_event_buf.is_some() {
            ctx.drain_player_events(player_event_buf, loop_start);
        }

        {
            for (name, renderer) in &ctx.renderers {
                if !renderer.can_present_native_wayland_surface() {
                    callback_flush_needed |= backend.hide_native_dmabuf(name);
                }
            }
        }

        let (latest_frames, _frames_received, _frames_discarded) =
            ctx.drain_frames(hot_loop_active || frame_ready, !direct_native_video_wake);
        for (source_id, frame) in latest_frames {
            if !power_monitor.is_powered(source_id.as_ref()) {
                drop(frame);
                continue;
            }
            backend.set_surface_content_type(source_id.as_ref(), crate::queue::ContentType::Video);
            let mut frame = Some(frame);
            let barrier_blocks = ctx.startup_barrier_blocks_output(source_id.as_ref(), loop_start);
            let mut mark_presented = false;
            let mut mark_ready = false;
            if let Some(r) = ctx.renderers.get_mut(source_id.as_ref()) {
                if crate::observability::trace_all::trace_all_enabled() {
                    let traced_frame = frame.as_ref().expect("frame is live");
                    tracing::trace!(
                        "[TRACE5][VIDEO-DRAIN-FRAME] output={} session={} frame_hash={:016x} size={}x{} pts_ns={:?} duration_ns={:?} pending_age_ms={:?} callback_pending={} transition={}",
                        source_id,
                        traced_frame.session_id,
                        traced_frame.trace_fingerprint(),
                        traced_frame.width,
                        traced_frame.height,
                        traced_frame.pts_ns,
                        traced_frame.duration_ns,
                        ctx.latest_video_frames
                            .pending_frame_age(source_id.as_ref())
                            .map(|age| age.as_secs_f64() * 1000.0),
                        r.frame_callback_pending,
                        r.transition_active
                    );
                }
                let had_current_texture = r.has_current_texture();
                let native_output_size = backend
                    .logical_surface_size(source_id.as_ref(), (r.config.width, r.config.height));
                let native_surface_presented =
                    crate::wayland::native_dmabuf_surface::present_native_frame(
                        &mut backend,
                        r,
                        &qh,
                        &conn,
                        source_id.as_ref(),
                        frame.as_ref().expect("frame is live"),
                        native_output_size,
                    );
                if native_surface_presented {
                    frame.take();
                    mark_ready = true;
                    mark_presented = true;
                    callback_flush_needed = true;
                    if let Some(player) = ctx.video_players.get(source_id.as_ref()) {
                        player.request_video_frame();
                    }
                }
                let should_upload = if r.valid_content_type == crate::queue::ContentType::Video {
                    !r.steady_video_uses_frame_callbacks()
                        || !had_current_texture
                        || !r.frame_callback_pending
                        || r.frame_callback_pending_too_long(1000)
                } else {
                    !r.frame_callback_pending || !r.has_current_texture()
                };

                if !native_surface_presented && should_upload {
                    callback_flush_needed |= backend.hide_native_dmabuf(source_id.as_ref());
                    let video_start = std::time::Instant::now();
                    r.upload_frame(frame.as_ref().expect("frame is live"));
                    let video_duration = video_start.elapsed();
                    ctx.metrics.record_video_cpu_time(video_duration);
                    ctx.metrics.record_video_frame_uploaded();
                    mark_ready = true;
                    frame.take();
                } else if !native_surface_presented {
                    frame.take();
                }

                if !native_surface_presented
                    && r.valid_content_type == crate::queue::ContentType::Video
                    && let Some(layer_surface) = backend.surfaces.get(source_id.as_ref())
                    && !barrier_blocks
                {
                    let immediate_video = !r.steady_video_uses_frame_callbacks()
                        || crate::wayland::video_immediate_present_enabled();
                    let should_render_now = (mark_ready
                        && (immediate_video || !had_current_texture))
                        || r.frame_callback_pending_too_long(1000);
                    if crate::observability::trace_all::trace_all_enabled() {
                        tracing::trace!(
                            "[TRACE5][VIDEO-PRESENT-DECISION] output={} mark_ready={} immediate={} had_current={} callback_pending={} should_render_now={} callback_too_long={} transition_just_completed={}",
                            source_id,
                            mark_ready,
                            immediate_video,
                            had_current_texture,
                            r.frame_callback_pending,
                            should_render_now,
                            r.frame_callback_pending_too_long(1000),
                            r.transition_just_completed()
                        );
                    }
                    if should_render_now {
                        let _ = r.render(
                            renderer::BackendContext::Wayland {
                                surface: layer_surface,
                                qh: &qh,
                                presentation: backend.presentation_proxy(),
                            },
                            loop_start,
                        );
                        if !ctx.first_frame_recorded {
                            ctx.metrics.record_first_frame();
                            ctx.first_frame_recorded = true;
                        }
                        mark_presented = true;
                        let _ = r.transition_just_completed();
                        if !immediate_video {
                            callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
                        } else if let Some(player) = ctx.video_players.get(source_id.as_ref()) {
                            // The native FFmpeg worker decodes one useful frame per
                            // demand. Callback-paced playback receives that demand
                            // from the callback handler; source-driven composition
                            // must continue the chain after each completed present.
                            player.request_video_frame();
                        }
                    } else if !r.frame_callback_pending {
                        callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
                    }
                }
            } else {
                frame.take();
            }
            if mark_ready {
                ctx.mark_startup_output_ready(source_id.as_ref(), loop_start);
            }
            if mark_presented {
                ctx.mark_output_presented_if_ready(source_id.as_ref());
            }
        }

        if hot_loop_active || image_buf.is_some() {
            ctx.drain_images(image_buf, loop_start, |r, name, ls| {
                if !power_monitor.is_powered(name) {
                    return;
                }
                backend.set_surface_content_type(name, crate::queue::ContentType::Image);
                if r.configured {
                    if let Some(layer_surface) = backend.surfaces.get(name) {
                        let _ = r.render(
                            renderer::BackendContext::Wayland {
                                surface: layer_surface,
                                qh: &qh,
                                presentation: backend.presentation_proxy(),
                            },
                            ls,
                        );
                    }
                }
            });
        }

        if hot_loop_active || player_buf.is_some() {
            ctx.drain_players(player_buf, loop_start, |r, name, ls| {
                if !power_monitor.is_powered(name) {
                    return;
                }
                backend.set_surface_content_type(name, r.valid_content_type);
                if r.configured {
                    if let Some(layer_surface) = backend.surfaces.get(name) {
                        let _ = r.render(
                            renderer::BackendContext::Wayland {
                                surface: layer_surface,
                                qh: &qh,
                                presentation: backend.presentation_proxy(),
                            },
                            ls,
                        );
                    }
                }
            });
            if power_monitor.is_suspended() {
                for player in ctx.video_players.values() {
                    let _ = player.pause();
                }
            }
        }

        ctx.release_startup_present_barrier(loop_start, |r, name, ls| {
            if !power_monitor.is_powered(name) {
                return;
            }
            if r.configured {
                if let Some(layer_surface) = backend.surfaces.get(name) {
                    let _ = r.render(
                        renderer::BackendContext::Wayland {
                            surface: layer_surface,
                            qh: &qh,
                            presentation: backend.presentation_proxy(),
                        },
                        ls,
                    );
                }
            }
        });

        let direct_native_steady = {
            let mut active_video = ctx.renderers.iter().filter(|(_, renderer)| {
                renderer.valid_content_type == crate::queue::ContentType::Video
            });
            active_video.clone().next().is_some()
                && active_video.all(|(name, renderer)| {
                    power_monitor.is_powered(name)
                        && renderer.can_present_native_wayland_surface()
                        && backend.native_dmabuf_surface_active(name)
                        && !renderer.transition_active
                        && !renderer.needs_redraw
                })
        };
        if !direct_native_steady || !backend.frame_callback_ready.is_empty() {
            frame_callbacks::process_frame_callbacks(
                &mut ctx,
                &mut backend,
                &qh,
                &conn,
                &power_monitor,
                &mut callback_flush_needed,
                loop_start,
            );
        }
        if callback_flush_needed {
            let _ = conn.flush();
        }

        ctx.housekeeping(loop_start, entered_idle_wait).await;
        if last_presentation_log.elapsed() >= std::time::Duration::from_secs(10) {
            last_presentation_log = Instant::now();
            backend.reap_native_release_fences();
            let native = backend.native_dmabuf_resource_counts();
            tracing::info!(
                "[WAYLAND-RESOURCES] native_surfaces={} native_buffers={} native_busy={} native_owners={} native_pending={} native_retired={} native_explicit_releases={} native_feedbacks={}",
                native.surfaces,
                native.buffers,
                native.busy,
                native.owners,
                native.pending,
                native.retired,
                native.explicit_releases,
                native.feedbacks,
            );
            if backend.presentation_proxy().is_some() {
                tracing::info!(
                    "[WAYLAND-PRESENT] {}",
                    backend.presentation_telemetry.summary()
                );
            }
        }
        ctx.timing_and_poll(renderer_activity.any_active, loop_start)
            .await;
    }

    // `ctx` owns WGPU surfaces and its backend instances. Destroy them while
    // the Wayland connection is still alive: Mesa's EGL teardown may marshal
    // Wayland requests, and Rust's default reverse-local drop order would
    // otherwise destroy `conn` before `ctx`.
    drop(ctx);

    Ok(())
}
