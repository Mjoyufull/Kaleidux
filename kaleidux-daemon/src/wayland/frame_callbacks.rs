use crate::content::sessions::should_accept_video_frame;
use crate::main_loop::MainLoopContext;
use crate::renderer;
use std::collections::HashSet;
use std::time::Instant;
use wayland_client::{Connection, QueueHandle};

const DEFAULT_STEADY_VIDEO_CALLBACK_UPLOAD_MS: u64 = 75;

static STEADY_VIDEO_CALLBACK_UPLOAD_INTERVAL: once_cell::sync::Lazy<std::time::Duration> =
    once_cell::sync::Lazy::new(|| {
        parse_steady_video_callback_upload_interval(
            std::env::var("KLD_VIDEO_CALLBACK_UPLOAD_INTERVAL_MS").ok(),
        )
    });

fn parse_steady_video_callback_upload_interval(value: Option<String>) -> std::time::Duration {
    let ms = value
        .as_deref()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(DEFAULT_STEADY_VIDEO_CALLBACK_UPLOAD_MS as i64)
        .max(0) as u64;
    std::time::Duration::from_millis(ms)
}

fn steady_video_callback_upload_interval() -> std::time::Duration {
    *STEADY_VIDEO_CALLBACK_UPLOAD_INTERVAL
}

pub(crate) fn process_frame_callbacks(
    ctx: &mut MainLoopContext,
    backend: &mut crate::wayland::WaylandBackend,
    qh: &QueueHandle<crate::wayland::WaylandBackend>,
    conn: &Connection,
    callback_flush_needed: &mut bool,
    connection_dead: bool,
    loop_start: Instant,
) {
    // ─── Wayland frame callback rendering ───────────────────────────

    let frame_ready_names: Vec<String> = backend.frame_callback_ready.drain().collect();
    for name in frame_ready_names {
        ctx.metrics.record_wayland_callback_wake();
        let barrier_blocks = ctx.startup_barrier_blocks_output(&name, loop_start);
        let mut mark_presented = false;
        if let Some(r) = ctx.renderers.get_mut(&name) {
            let should_upload_pending_video = r.should_upload_video_frame_on_callback()
                || ctx
                    .latest_video_frames
                    .pending_frame_age(&name)
                    .is_some_and(|age| age >= steady_video_callback_upload_interval());
            if should_upload_pending_video
                && let Some(frame) = ctx.latest_video_frames.take_frame(&name)
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
                } else if wait_duration > std::time::Duration::from_millis(16)
                    && crate::wayland::trace_frame_events_enabled()
                {
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
                            qh,
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
        let should_request =
            r.has_any_content() && (r.needs_redraw || r.transition_active) && !barrier_blocks;
        if should_request {
            if let Some(layer_surface) = backend.surfaces.get(name) {
                *callback_flush_needed |= r.request_frame_callback(layer_surface, qh);
            }
        }
    }
    if *callback_flush_needed && !connection_dead {
        let _ = conn.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::parse_steady_video_callback_upload_interval;

    #[test]
    fn steady_video_callback_upload_interval_defaults_to_half_rate() {
        assert_eq!(
            parse_steady_video_callback_upload_interval(None),
            std::time::Duration::from_millis(75)
        );
    }

    #[test]
    fn steady_video_callback_upload_interval_honors_zero_override() {
        assert_eq!(
            parse_steady_video_callback_upload_interval(Some("0".to_string())),
            std::time::Duration::ZERO
        );
    }
}
