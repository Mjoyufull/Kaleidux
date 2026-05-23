use crate::background::{self, BackgroundWorkKind};
use crate::main_loop::MainLoopContext;
use crate::renderer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use wayland_client::{Connection, QueueHandle};

pub(crate) async fn initialize_outputs_and_renderers(
    ctx: &mut MainLoopContext,
    conn: &Connection,
    backend: &mut crate::wayland::WaylandBackend,
    qh: &QueueHandle<crate::wayland::WaylandBackend>,
    event_queue: &mut wayland_client::EventQueue<crate::wayland::WaylandBackend>,
    log_level: Option<u8>,
) -> anyhow::Result<()> {
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
            qh,
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
                // SAFETY: `poll_fd` points to one initialized `libc::pollfd`, the count is 1,
                // and the file descriptor is borrowed from a live Wayland connection.
                let ret = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
                if ret > 0 && (poll_fd.revents & libc::POLLIN) != 0 {
                    if let Err(e) = guard.read() {
                        error!("Failed to read Wayland events: {}", e);
                    }
                    if let Err(e) = event_queue.dispatch_pending(backend) {
                        error!("Failed to dispatch Wayland events: {}", e);
                    }
                    let _ = conn.flush();
                }
            }
            _ => {
                if let Err(e) = event_queue.dispatch_pending(backend) {
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
                            qh,
                        },
                        Instant::now(),
                    );
                    initial_callback_flush_needed |= r.request_frame_callback(layer_surface, qh);
                }
            }
        }
    }
    if initial_callback_flush_needed {
        let _ = conn.flush();
    }

    Ok(())
}
