use super::{RawHandleSurface, WaylandBackend, startup};
use crate::background::{self, BackgroundWorkKind};
use crate::content::sessions::{set_pending_video_session, stop_video_player_in_background};
use crate::main_loop::MainLoopContext;
use crate::renderer;
use crate::renderer_retry::RendererRetryBackoff;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info};
use wayland_client::protocol::wl_output;
use wayland_client::{Connection, QueueHandle};

struct LiveOutput {
    description: String,
    proxy: wl_output::WlOutput,
    size: (u32, u32),
}

pub(crate) struct PendingRendererAdd {
    handle: tokio::task::JoinHandle<anyhow::Result<RendererInitResult>>,
    display_ptr: usize,
    size: (u32, u32),
    started_at: Instant,
    retire_requested: bool,
    timeout_reported: bool,
}

struct RendererInitResult {
    renderer: renderer::Renderer,
    wgpu_ctx: Arc<renderer::WgpuContext>,
}

const RENDERER_INIT_TIMEOUT: Duration = Duration::from_secs(5);
const RENDERER_INIT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const RETIRING_RENDERER_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) fn pending_renderer_deadline(
    pending: &HashMap<String, PendingRendererAdd>,
    now: Instant,
) -> Option<Instant> {
    if pending.is_empty() {
        return None;
    }
    let interval = if pending
        .values()
        .any(|add| !add.timeout_reported && !add.retire_requested)
    {
        RENDERER_INIT_POLL_INTERVAL
    } else {
        RETIRING_RENDERER_POLL_INTERVAL
    };
    Some(now + interval)
}

pub(crate) fn record_pending_resize(
    pending: &mut HashMap<String, PendingRendererAdd>,
    name: &str,
    size: (u32, u32),
) -> bool {
    let Some(add) = pending.get_mut(name) else {
        return false;
    };
    add.size = (size.0.max(1), size.1.max(1));
    true
}

fn output_size(info: &smithay_client_toolkit::output::OutputInfo) -> (u32, u32) {
    info.logical_size
        .or_else(|| {
            info.modes
                .iter()
                .find(|mode| mode.current)
                .map(|mode| mode.dimensions)
        })
        .map(|(width, height)| (width.max(1) as u32, height.max(1) as u32))
        .unwrap_or((1920, 1080))
}

fn live_outputs(backend: &WaylandBackend) -> HashMap<String, LiveOutput> {
    backend
        .output_state
        .outputs()
        .filter_map(|proxy| {
            let info = backend.output_state.info(&proxy)?;
            let name = info.name.clone()?;
            let size = output_size(&info);
            Some((
                name,
                LiveOutput {
                    description: info.description.unwrap_or_else(|| "unknown".to_string()),
                    size,
                    proxy,
                },
            ))
        })
        .collect()
}

fn output_delta(
    renderer_names: impl IntoIterator<Item = String>,
    live_names: impl IntoIterator<Item = String>,
) -> (Vec<String>, Vec<String>) {
    let renderers: HashSet<_> = renderer_names.into_iter().collect();
    let live: HashSet<_> = live_names.into_iter().collect();
    let mut removed = renderers.difference(&live).cloned().collect::<Vec<_>>();
    let mut added = live.difference(&renderers).cloned().collect::<Vec<_>>();
    removed.sort();
    added.sort();
    (removed, added)
}

fn retire_output_resources(backend: &mut WaylandBackend, name: &str) {
    backend.retire_native_dmabuf_output(name);
    backend.retire_surface_protocols(name);
    backend.surfaces.remove(name);
    backend.mpv_video_surfaces.remove(name);
    backend.frame_callback_ready.remove(name);
    backend
        .pending_resizes
        .retain(|(output, ..)| output != name);
}

fn retire_output(
    ctx: &mut MainLoopContext,
    backend: &mut WaylandBackend,
    pending: &mut HashMap<String, PendingRendererAdd>,
    name: &str,
) {
    info!("[WAYLAND-HOTPLUG] Removing disconnected output {name}");
    crate::image::runtime_cache::remove_image_prefetch_generation(name);
    ctx.latest_video_frames.clear_source(name);
    ctx.monitor_manager.remove_output(name);
    ctx.mpv_native_targets.remove(name);
    ctx.mpv_composed_targets.remove(name);
    if let Some(player) = ctx.pending_image_video_stops.remove(name) {
        stop_video_player_in_background(name.to_string(), player);
    }
    if let Some(barrier) = ctx.startup_present_barrier.as_mut() {
        barrier.outputs.remove(name);
    }
    if let Some(player) = ctx.video_players.remove(name) {
        stop_video_player_in_background(name.to_string(), player);
    }
    ctx.pending_video_switches.remove(name);
    set_pending_video_session(&ctx.pending_video_sessions, name, None);
    ctx.renderers.remove(name);
    if let Some(add) = pending.get_mut(name) {
        // `spawn_blocking` work cannot be cancelled once it has started. Keep
        // the protocol surface alive until Renderer::new returns, then destroy
        // it from `drain_pending_renderer_adds`. Aborting the async wrapper
        // here would detach WSI work that still uses this surface.
        add.retire_requested = true;
    } else {
        retire_output_resources(backend, name);
    }
}

fn rollback_added_output(
    ctx: &mut MainLoopContext,
    backend: &mut WaylandBackend,
    pending: &mut HashMap<String, PendingRendererAdd>,
    name: &str,
) {
    retire_output(ctx, backend, pending, name);
}

async fn add_output(
    ctx: &mut MainLoopContext,
    conn: &Connection,
    backend: &mut WaylandBackend,
    pending: &mut HashMap<String, PendingRendererAdd>,
    qh: &QueueHandle<WaylandBackend>,
    name: &str,
    output: LiveOutput,
) -> bool {
    info!(
        "[WAYLAND-HOTPLUG] Adding connected output {name} at {}x{}",
        output.size.0, output.size.1
    );
    ctx.monitor_manager
        .add_output(name, &output.description)
        .await;
    let Some(output_config) = ctx.monitor_manager.get_output_config(name).cloned() else {
        rollback_added_output(ctx, backend, pending, name);
        return false;
    };

    let layer_surface = match backend.create_wallpaper_surface(
        &output.proxy,
        qh,
        name.to_string(),
        output_config.layer.clone().into(),
    ) {
        Ok(surface) => surface,
        Err(error) => {
            error!("[WAYLAND-HOTPLUG] Creating layer surface for {name} failed: {error}");
            rollback_added_output(ctx, backend, pending, name);
            return false;
        }
    };

    let display_ptr = conn.backend().display_ptr() as *mut std::ffi::c_void;
    if startup::should_create_mpv_native_surfaces() {
        match backend.create_mpv_video_surface(
            &output.proxy,
            qh,
            name.to_string(),
            output_config.layer.clone().into(),
        ) {
            Ok(surface) => {
                if let Some(target) = crate::video::MpvNativeVideoTarget::new(
                    display_ptr,
                    surface.clone(),
                    output.size.0,
                    output.size.1,
                ) {
                    ctx.mpv_native_targets.insert(name.to_string(), target);
                }
            }
            Err(error) => {
                error!("[WAYLAND-HOTPLUG] Creating libmpv surface for {name} failed: {error}");
                rollback_added_output(ctx, backend, pending, name);
                return false;
            }
        }
    }

    let raw = Arc::new(RawHandleSurface {
        layer_surface,
        display_ptr,
    });
    let renderer_name = name.to_string();
    let existing_wgpu_ctx = ctx.wgpu_ctx.clone();
    let metrics = ctx.metrics.clone();
    let handle = tokio::spawn(async move {
        let (renderer_ctx, initial_surface) = if let Some(wgpu_ctx) = existing_wgpu_ctx {
            let surface = wgpu_ctx
                .instance
                .create_surface(raw.clone())
                .map_err(|error| anyhow::anyhow!("creating WGPU surface failed: {error}"))?;
            (wgpu_ctx, surface)
        } else {
            renderer::WgpuContext::with_surface(raw.clone())
                .await
                .map_err(|error| anyhow::anyhow!("initializing WGPU failed: {error}"))?
        };
        let completed_wgpu_ctx = renderer_ctx.clone();
        let Some(handle) =
            background::spawn_blocking_tracked_wait(BackgroundWorkKind::RendererInit, move || {
                renderer::Renderer::new(
                    renderer_name,
                    renderer_ctx,
                    raw,
                    Some(initial_surface),
                    Some(metrics),
                )
            })
            .await
        else {
            anyhow::bail!("shutdown began while waiting for renderer capacity");
        };
        let renderer = handle
            .await
            .map_err(|error| anyhow::anyhow!("renderer task failed: {error}"))??;
        Ok(RendererInitResult {
            renderer,
            wgpu_ctx: completed_wgpu_ctx,
        })
    });

    pending.insert(
        name.to_string(),
        PendingRendererAdd {
            handle,
            display_ptr: display_ptr as usize,
            size: output.size,
            started_at: Instant::now(),
            retire_requested: false,
            timeout_reported: false,
        },
    );
    info!("[WAYLAND-HOTPLUG] Renderer initialization queued for {name}");
    true
}

pub(crate) async fn drain_pending_renderer_adds(
    ctx: &mut MainLoopContext,
    backend: &mut WaylandBackend,
    pending: &mut HashMap<String, PendingRendererAdd>,
    retry: &mut RendererRetryBackoff,
    now: Instant,
) -> bool {
    let ready = pending
        .iter()
        .filter_map(|(name, add)| add.handle.is_finished().then_some(name.clone()))
        .collect::<Vec<_>>();
    for (name, add) in pending.iter_mut() {
        if !add.timeout_reported
            && now.saturating_duration_since(add.started_at) >= RENDERER_INIT_TIMEOUT
        {
            add.timeout_reported = true;
            error!(
                "[WAYLAND-HOTPLUG] Renderer creation for {name} exceeded {:.0}s; initialization remains isolated and will be adopted or retired when the non-cancellable driver call returns",
                RENDERER_INIT_TIMEOUT.as_secs_f64()
            );
            ctx.metrics.record_error("wayland_hotplug_renderer_slow");
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
            // Drop any completed renderer before destroying the surface it
            // owns. A failed/panicked task is equally safe to retire here.
            drop(result);
            retire_output_resources(backend, &name);
            backend.outputs_changed = true;
            continue;
        }
        let initialized = match result {
            Ok(Ok(initialized)) => initialized,
            Ok(Err(error)) => {
                error!("[WAYLAND-HOTPLUG] Renderer creation for {name} failed: {error:#}");
                rollback_added_output(ctx, backend, pending, &name);
                let delay = retry.record_failure(&name, now);
                info!(
                    "[WAYLAND-HOTPLUG] Retrying {name} in {:.0}s",
                    delay.as_secs_f64()
                );
                // Other outputs may have been skipped while this task held
                // the first-device serialization slot. Reconcile them even
                // when establishing the shared device failed.
                backend.outputs_changed |= ctx.wgpu_ctx.is_none();
                continue;
            }
            Err(error) => {
                error!("[WAYLAND-HOTPLUG] Renderer task for {name} failed: {error}");
                rollback_added_output(ctx, backend, pending, &name);
                let delay = retry.record_failure(&name, now);
                info!(
                    "[WAYLAND-HOTPLUG] Retrying {name} in {:.0}s",
                    delay.as_secs_f64()
                );
                backend.outputs_changed |= ctx.wgpu_ctx.is_none();
                continue;
            }
        };
        let RendererInitResult {
            mut renderer,
            wgpu_ctx,
        } = initialized;
        retry.record_success(&name);
        if ctx.wgpu_ctx.is_none() {
            ctx.wgpu_ctx = Some(wgpu_ctx.clone());
            backend.outputs_changed = true;
        }
        if let Some(config) = ctx.monitor_manager.get_output_config(&name) {
            renderer.apply_config(config);
        }
        let _ = renderer.resize_checked(add.size.0, add.size.1);
        ctx.renderers.insert(name.clone(), renderer);
        if startup::should_create_mpv_composed_targets()
            && let Some(target) = crate::video::MpvComposedVideoTarget::new(
                add.display_ptr as *mut std::ffi::c_void,
                wgpu_ctx,
                add.size.0,
                add.size.1,
            )
        {
            ctx.mpv_composed_targets.insert(name.clone(), target);
        }
        info!("[WAYLAND-HOTPLUG] Renderer initialized successfully for {name}");
        initialized_any = true;
    }
    if initialized_any {
        let changes = ctx.monitor_manager.tick();
        ctx.load_content_changes(changes, "WAYLAND-HOTPLUG", false);
    }
    initialized_any
}

/// Reconcile named compositor outputs with daemon-owned surfaces and renderers.
///
/// A refresh present on surviving outputs is intentional. Mesa's Vulkan WSI
/// owns a private Wayland event queue for per-surface DMA-BUF feedback. Static
/// wallpapers otherwise never enter WSI again, so format-table FDs delivered
/// during output topology changes can remain queued indefinitely.
pub(crate) async fn reconcile_outputs(
    ctx: &mut MainLoopContext,
    conn: &Connection,
    backend: &mut WaylandBackend,
    pending: &mut HashMap<String, PendingRendererAdd>,
    retry: &mut RendererRetryBackoff,
    qh: &QueueHandle<WaylandBackend>,
    now: Instant,
) -> bool {
    let mut live = live_outputs(backend);
    let live_names = live.keys().cloned().collect::<HashSet<_>>();
    retry.retain_outputs(&live_names);
    let mut topology_touched = false;
    for name in std::mem::take(&mut backend.closed_outputs) {
        if ctx.renderers.contains_key(&name) || pending.contains_key(&name) {
            info!("[WAYLAND-HOTPLUG] Rebuilding compositor-closed surface for {name}");
            retire_output(ctx, backend, pending, &name);
            topology_touched = true;
        }
    }
    let owned_names = ctx
        .renderers
        .keys()
        .chain(pending.keys())
        .cloned()
        .collect::<Vec<_>>();
    let (removed, added) = output_delta(owned_names, live.keys().cloned());

    for name in removed {
        retire_output(ctx, backend, pending, &name);
        topology_touched = true;
    }

    for name in added {
        if ctx.wgpu_ctx.is_none() && !pending.is_empty() {
            // Only one output may establish the shared device. Completion
            // immediately re-arms reconciliation for the skipped outputs.
            continue;
        }
        if !retry.can_start(&name, now) {
            continue;
        }
        let Some(output) = live.remove(&name) else {
            continue;
        };
        if add_output(ctx, conn, backend, pending, qh, &name, output).await {
            retry.mark_started(&name);
        } else {
            let delay = retry.record_failure(&name, now);
            info!(
                "[WAYLAND-HOTPLUG] Retrying {name} in {:.0}s",
                delay.as_secs_f64()
            );
        }
        topology_touched = true;
    }

    let flush_needed = topology_touched && refresh_surviving_surfaces(ctx, backend, qh, now);
    if topology_touched {
        backend.hotplug_wsi_drain_deadline = Some(now + Duration::from_millis(100));
    }

    flush_needed
}

fn refresh_surviving_surfaces(
    ctx: &mut MainLoopContext,
    backend: &WaylandBackend,
    qh: &QueueHandle<WaylandBackend>,
    now: Instant,
) -> bool {
    let mut flush_needed = false;
    for (name, renderer) in &mut ctx.renderers {
        if !renderer.configured || !renderer.has_any_content() {
            continue;
        }
        let Some(surface) = backend.surfaces.get(name) else {
            continue;
        };
        renderer.needs_redraw = true;
        if let Err(error) = renderer.render(
            renderer::BackendContext::Wayland {
                surface,
                qh,
                presentation: backend.presentation_proxy(),
            },
            now,
        ) {
            error!("[WAYLAND-HOTPLUG] Refresh present for {name} failed: {error}");
            ctx.metrics.record_error("wayland_hotplug_refresh");
        }
        flush_needed = true;
    }
    flush_needed
}

pub(crate) fn drain_delayed_wsi_feedback(
    ctx: &mut MainLoopContext,
    backend: &mut WaylandBackend,
    qh: &QueueHandle<WaylandBackend>,
    now: Instant,
) -> bool {
    let Some(deadline) = backend.hotplug_wsi_drain_deadline else {
        return false;
    };
    if now < deadline {
        return false;
    }
    backend.hotplug_wsi_drain_deadline = None;
    refresh_surviving_surfaces(ctx, backend, qh, now)
}

#[cfg(test)]
mod tests {
    use super::output_delta;

    #[test]
    fn output_delta_is_stable_and_ignores_unnamed_outputs() {
        let (removed, added) = output_delta(
            ["eDP-1".to_string(), "DP-1".to_string()],
            ["eDP-1".to_string(), "HDMI-A-1".to_string()],
        );
        assert_eq!(removed, ["DP-1"]);
        assert_eq!(added, ["HDMI-A-1"]);
    }

    #[test]
    fn output_delta_does_not_remove_survivors_during_unnamed_probe() {
        let (removed, added) = output_delta(["eDP-1".to_string()], ["eDP-1".to_string()]);
        assert!(removed.is_empty());
        assert!(added.is_empty());
    }
}
