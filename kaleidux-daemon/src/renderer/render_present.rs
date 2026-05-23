use crate::observability::present::RendererPresentKind;
use tracing::{debug, error, warn};

use super::{BackendContext, TransitionUniforms, render_blit::BlitSource};

impl super::Renderer {
    pub fn render(
        &mut self,
        context: BackendContext,
        _frame_time: std::time::Instant,
    ) -> anyhow::Result<()> {
        let render_start = std::time::Instant::now();
        let frame_time = render_start;

        if !self.transition_active && !self.content_swap_pending && self.prev_texture.is_some() {
            self.release_prev_texture("render idle cleanup");
        }
        if !self.transition_active
            && !self.content_swap_pending
            && self.composition_texture.is_some()
        {
            self.release_composition_texture("render idle cleanup");
        }

        // CRITICAL: Reset per-frame state at the start of each render cycle
        // This flag tracks whether a transition was rendered in THIS frame
        self.transition_rendered_this_frame = false;

        // CRITICAL: Always render if transition is in progress, even if needs_redraw is false
        // This ensures transitions continue smoothly
        if !self.configured {
            // Try to configure one last time if we have dimensions
            if self.config.width > 0 && self.config.height > 0 {
                let _ = self.resize_checked(self.config.width, self.config.height);
            }
            if !self.configured {
                return Ok(()); // Skip render until configured
            }
        }

        // Always render if transition is active, regardless of needs_redraw
        if !self.transition_active && !self.needs_redraw {
            return Ok(()); // Skip render if no transition and no redraw needed
        }

        // If we are here, we are going to render.
        // CRITICAL: Always keep needs_redraw=true during transitions to ensure continuous rendering
        // This ensures transitions complete smoothly without getting stuck
        // Note: Don't reset needs_redraw here - do it AFTER we've actually rendered and presented

        let surface_acquire_start = std::time::Instant::now();
        let output = match self.surface.get_current_texture() {
            Ok(t) => t,
            Err(wgpu::SurfaceError::Lost) => {
                warn!(
                    "Surface Lost for {}. Marking not-configured to trigger re-creation.",
                    self.name
                );
                self.configured = false;
                self.needs_redraw = true; // Retry ASAP
                self.frame_callback_pending = false; // Callback won't fire for lost surface
                return Ok(());
            }
            Err(wgpu::SurfaceError::Outdated) => {
                warn!("Surface Outdated for {}. Reconfiguring.", self.name);
                self.configured = false;
                self.needs_redraw = true; // Retry ASAP
                self.frame_callback_pending = false; // Callback won't fire for outdated surface
                return Ok(());
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("timeout") {
                    debug!(
                        "Surface acquisition timeout for {}, skipping frame.",
                        self.name
                    );
                    self.needs_redraw = true; // Try again next loop
                    return Ok(());
                }
                error!(
                    "Failed to get current surface texture for {}: {}",
                    self.name, err_str
                );
                return Ok(());
            }
        };
        let surface_acquire_duration = surface_acquire_start.elapsed();
        if surface_acquire_duration > std::time::Duration::from_millis(8) {
            debug!(
                "[FRAME] {}: Surface acquisition took {:.1}ms",
                self.name,
                surface_acquire_duration.as_secs_f64() * 1000.0
            );
        }
        let view = output
            .texture
            .create_view(&wgpu::TextureViewDescriptor::default());

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Main Render Encoder"),
            });

        if !self.render_transition_layer(&mut encoder, frame_time) {
            return Ok(());
        }

        // Removed the "else if self.transition_active { ... }" block because we moved ensure_composition_texture
        // to the top and we log missing resources above.

        let height = self.config.height as f32;
        if !self.transition_active {
            let uniforms = TransitionUniforms {
                progress: 1.0,
                screen_aspect: self.config.width as f32 / height,
                prev_aspect: 1.0,
                next_aspect: self.current_aspect,
                params: [[0.0; 4]; 7],
            };
            self.ctx
                .queue
                .write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));
        }

        let blit_source = match self.select_blit_source() {
            Some(source) => source,
            None => {
                self.present_black_frame(context, encoder, output, &view, render_start);
                return Ok(());
            }
        };

        let is_comp = blit_source == BlitSource::Composition;
        if !self.ensure_blit_bind_group(blit_source) {
            return Ok(());
        }

        // Get format-specific blit pipeline from shared context
        let blit_pipeline = self.ctx.get_blit_pipeline(self.config.format);

        {
            let mut render_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("Blit Render Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &view,
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

            render_pass.set_pipeline(&blit_pipeline);
            if let Some(bg) = &self.blit_bind_group {
                render_pass.set_bind_group(0, bg, &[]);
                render_pass.draw(0..3, 0..1);
            } else {
                error!(
                    "[RENDER] {}: blit_bind_group is None, cannot render!",
                    self.name
                );
                return Ok(()); // Can't render without bind group
            }
        } // render_pass dropped here

        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        output.present();
        if self.display_timer_pending {
            self.display_timer_pending = false;
            self.display_timer_ready = true;
        }

        // Note: frame_callback_pending is reset by the main loop when callback is received
        // Don't reset it here to avoid race conditions

        if !self.transition_active {
            self.transition_start_time = None;
        }

        self.last_present_time = std::time::Instant::now();
        if is_comp || self.transition_active {
            if let Some(m) = &self.metrics {
                m.record_renderer_present(RendererPresentKind::Transition);
            }
        } else if self.valid_content_type == crate::queue::ContentType::Video {
            self.presented_video_session_id = self.active_video_session_id;
            if let Some(m) = &self.metrics {
                m.record_video_frame_presented();
                m.record_video_frame_present_source();
            }
        } else if self.valid_content_type == crate::queue::ContentType::Image {
            if let Some(m) = &self.metrics {
                m.record_static_image_present();
            }
        }

        // CRITICAL: Reset needs_redraw AFTER we've actually rendered and presented
        // This ensures we render at least once for static images
        // We reset to false even for video; video.rs will set it to true
        // when a new frame is uploaded, preventing redundant renders between frames.
        self.needs_redraw = false;

        // Record renderer CPU time
        if let Some(m) = &self.metrics {
            let render_duration = render_start.elapsed();
            m.record_renderer_cpu_time(render_duration);
        }

        Ok(())
    }
}
