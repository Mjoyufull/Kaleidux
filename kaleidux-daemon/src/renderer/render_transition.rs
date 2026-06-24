use std::sync::OnceLock;
use tracing::{debug, error, info, trace, warn};

use super::{TransitionStats, TransitionUniforms};

fn trace_transition_events_enabled() -> bool {
    if crate::observability::trace_all::trace_all_enabled() {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_TRACE_TRANSITION_EVENTS")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

impl super::Renderer {
    pub(super) fn render_transition_layer(
        &mut self,
        encoder: &mut wgpu::CommandEncoder,
        frame_time: std::time::Instant,
    ) -> bool {
        // Update transition progress BEFORE checking if we should render transition
        // This ensures progress is accurate for the current frame
        // For image transitions: always advance progress once started (textures are always available)
        // For video transitions: only freeze if we're waiting for the first video frame
        if self.transition_active {
            // Advance transition normally using frame_time for synchronization
            if let Some(start) = self.transition_start_time {
                let elapsed = frame_time.saturating_duration_since(start).as_secs_f32();
                let new_progress = (elapsed / self.transition_duration).min(1.0);
                if new_progress != self.transition_progress && trace_transition_events_enabled() {
                    trace!(
                        "[TRANSITION] {}: Progress updated {:.3} -> {:.3} (elapsed={:.3}s, duration={:.3}s)",
                        self.name,
                        self.transition_progress,
                        new_progress,
                        elapsed,
                        self.transition_duration
                    );
                }
                self.transition_progress = new_progress;

                // Update stats
                if let Some(stats) = &mut self.transition_stats {
                    stats.frame_count += 1;
                }

                // Check if transition completed
                if self.transition_progress >= 1.0 {
                    // Only set flag if transition was just completed (not already completed)
                    if self.transition_active {
                        self.transition_active = false;
                        self.transition_just_completed = true;
                        self.arm_display_timer_on_present();

                        // Log Audit Report and record metrics
                        if let Some(stats) = self.transition_stats.take() {
                            let duration = stats.start_time.elapsed();
                            let duration_secs = duration.as_secs_f64();

                            // Record transition duration in metrics
                            if let Some(m) = &self.metrics {
                                m.record_transition(duration);
                            }
                            let fps = if duration_secs > 0.001 {
                                stats.frame_count as f64 / duration_secs
                            } else {
                                0.0
                            };
                            let drift = duration.as_secs_f32() - stats.target_duration;
                            let batch_info = stats
                                .batch_id
                                .map(|b| format!(" (Batch: {:x})", b))
                                .unwrap_or_default();

                            info!(
                                "[AUDIT] Transition Completed {}{}:\n  - Duration: {:.3}s (Target: {:.3}s)\n  - Frames: {} (Avg {:.1} FPS)\n  - Drift: {:.3}s",
                                self.name,
                                batch_info,
                                duration.as_secs_f32(),
                                stats.target_duration,
                                stats.frame_count,
                                fps,
                                drift
                            );
                        } else {
                            info!(
                                "[TRANSITION] {}: Transition completed (progress={:.3}) - No stats available",
                                self.name, self.transition_progress
                            );
                        }
                    }
                }
            } else {
                // Start timing from the first render of ready content. This prevents decode or
                // queue delay from consuming the transition before it is ever presented.
                let start = frame_time;
                self.transition_start_time = Some(start);

                self.transition_progress = 0.0;

                // Initialize stats
                self.transition_stats = Some(TransitionStats {
                    start_time: start,
                    frame_count: 0,
                    target_duration: self.transition_duration,
                    batch_id: self.active_batch_id,
                });

                info!(
                    "[TRANSITION] {}: Starting transition (duration={:.3}s, initial_progress={:.3})",
                    self.name, self.transition_duration, self.transition_progress
                );
            }
        }

        // Render transition if we have all required textures and transition is active
        // Ensure component texture exists if transition is active
        // This must be done BEFORE checking should_render_transition to ensure we don't skip
        // the transition just because the texture was lazily dropped or missing.
        if self.transition_active {
            if let Err(e) = self.ensure_composition_texture() {
                error!(
                    "[TRANSITION] {}: Failed to ensure composition texture: {}",
                    self.name, e
                );
            }
        }

        // Render transition if we have all required textures and transition is active
        let should_render_transition = self.transition_active
            && (self.prev_texture.is_some() || self.prev_external_view_available())
            && (self.current_texture.is_some() || self.current_external_view_available())
            && self.composition_texture.is_some()
            && self.composition_texture_view.is_some();

        // CRITICAL: Removed TRACE logs from hot path for performance
        // These were causing 5-10% CPU overhead when called every frame

        if should_render_transition {
            // 1. Get/Create pipeline (this will cache it if needed)
            let pipeline = match self.get_transition_pipeline(&self.active_transition) {
                Some(p) => p,
                None => {
                    warn!(
                        "[TRANSITION] {}: Failed to get/create transition pipeline for {}",
                        self.name,
                        self.active_transition.name()
                    );
                    return false;
                }
            };

            // 2. Now we can do immutable borrows
            let raw_params = self.active_transition.to_params();
            let uniforms = TransitionUniforms {
                progress: self.transition_progress,
                screen_aspect: self.config.width as f32 / self.config.height as f32,
                prev_aspect: self.prev_aspect,
                next_aspect: self.current_aspect,
                params: bytemuck::cast(raw_params),
            };
            self.ctx
                .queue
                .write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));

            // Recreate bind group only if invalidated (set to None when textures change)
            self.update_transition_bind_group();

            if let Some(bind_group) = &self.transition_bind_group {
                let composition_view = match self.composition_texture_view.as_ref() {
                    Some(v) => v,
                    None => {
                        error!("Composition texture view missing during transition render");
                        return false;
                    }
                };
                let mut render_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                    label: Some("Transition Render Pass"),
                    color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                        view: composition_view,
                        resolve_target: None,
                        ops: wgpu::Operations {
                            load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                            store: wgpu::StoreOp::Store,
                        },
                    })],
                    depth_stencil_attachment: None,
                    timestamp_writes: None,
                    occlusion_query_set: None,
                });
                render_pass.set_pipeline(&pipeline);
                render_pass.set_bind_group(0, bind_group, &[]);
                render_pass.draw(0..3, 0..1);

                // Mark that transition was successfully rendered this frame
                self.transition_rendered_this_frame = true;

                // debug!("[TRANSITION] {}: Rendered transition frame ...", self.name);
            } else {
                // Transition should render but bind group is missing
                warn!(
                    "[TRANSITION] {}: Transition render FAILED - bind_group missing, transition={}",
                    self.name,
                    self.active_transition.name()
                );
                // Don't set transition_rendered_this_frame - transition didn't actually render
            }

            // CLEANUP: Return prev_texture to pool when transition is TRULY finished
            if self.transition_progress >= 1.0
                && (self.current_texture.is_some() || self.current_external_view_available())
                && (self.prev_texture.is_some() || self.prev_external_view_available())
            {
                debug!(
                    "[TRANSITION] {}: Transition completed, returning prev_texture to pool",
                    self.name
                );
                // Return prev_texture to pool for reuse (instead of just dropping)
                if let Some(prev_tex) = self.prev_texture.take() {
                    if let Some((w, h)) = self.prev_texture_size.take() {
                        self.ctx.return_texture_to_pool(prev_tex, w, h);
                    }
                    // If size unknown, texture is still dropped here (freed by WGPU)
                }
                #[cfg(feature = "mpv-backend")]
                {
                    self.prev_external_view = None;
                    let frame = self.prev_external_frame.take();
                    self.drop_external_frame(frame);
                }
                self.prev_texture_view = None;
                self.transition_bind_group = None;
                self.blit_bind_group = None;
                self.transition_start_time = None;
                self.transition_active = false;
                self.release_composition_texture("transition completed");
            }
        }

        true
    }
}
