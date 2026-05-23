//! Wayland-specific main loop.
//!
//! Contains surface creation, Wayland event polling, frame callback rendering,
//! and connection error recovery. All shared logic lives in `main_loop::MainLoopContext`.

use crate::content::sessions::stop_video_player_in_background;
use crate::main_loop::MainLoopContext;
use crate::observability::wake::WakeReason;
use crate::orchestration;
use crate::renderer;
use crate::wayland::{frame_callbacks, startup};

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::error;
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

        let renderer_activity = ctx.renderer_activity_snapshot();
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
        if !hot_loop_active && !connection_dead {
            let idle_deadline =
                ctx.next_wayland_idle_deadline_from_snapshot(loop_start, renderer_activity);
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
        // Wayland fd opportunistically on video wakes so ready callbacks do not wait
        // behind the next independent fd wake, which can lower effective video FPS.
        if hot_loop_active || wayland_fd_ready || frame_ready {
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
                if let Some(vp) = ctx.video_players.remove(name) {
                    stop_video_player_in_background(name.clone(), vp);
                }
                ctx.pending_video_switches.remove(name);
                crate::content::sessions::set_pending_video_session(
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
                    if r.configured {
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

        ctx.process_scheduled(loop_start);
        ctx.process_script_tick();
        ctx.drain_commands(cmd_buf, loop_start).await;
        ctx.drain_player_events(player_event_buf, loop_start);

        let (latest_frames, _frames_received, _frames_discarded) =
            ctx.drain_frames(hot_loop_active || frame_ready, true);
        for (source_id, frame) in latest_frames {
            let barrier_blocks = ctx.startup_barrier_blocks_output(source_id.as_str(), loop_start);
            let mut mark_presented = false;
            let mut mark_ready = false;
            if let Some(r) = ctx.renderers.get_mut(source_id.as_str()) {
                if crate::observability::trace_all::trace_all_enabled() {
                    tracing::trace!(
                        "[TRACE5][VIDEO-DRAIN-FRAME] output={} session={} frame_hash={:016x} size={}x{} pts_ns={:?} duration_ns={:?} pending_age_ms={:?} callback_pending={} transition={}",
                        source_id,
                        frame.session_id,
                        frame.trace_fingerprint(),
                        frame.width,
                        frame.height,
                        frame.pts_ns,
                        frame.duration_ns,
                        ctx.latest_video_frames
                            .pending_frame_age(source_id.as_str())
                            .map(|age| age.as_secs_f64() * 1000.0),
                        r.frame_callback_pending,
                        r.transition_active
                    );
                }
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

                if r.valid_content_type == crate::queue::ContentType::Video
                    && let Some(layer_surface) = backend.surfaces.get(source_id.as_str())
                    && !barrier_blocks
                {
                    let immediate_video = crate::wayland::video_immediate_present_enabled();
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
                        }
                    } else if !r.frame_callback_pending {
                        callback_flush_needed |= r.request_frame_callback(layer_surface, &qh);
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
        }

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

        frame_callbacks::process_frame_callbacks(
            &mut ctx,
            &mut backend,
            &qh,
            &conn,
            &mut callback_flush_needed,
            connection_dead,
            loop_start,
        );

        ctx.housekeeping(loop_start, entered_idle_wait).await;
        ctx.timing_and_poll(renderer_activity.any_active, loop_start)
            .await;
    }

    Ok(())
}
