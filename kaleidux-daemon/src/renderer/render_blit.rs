use crate::observability::present::RendererPresentKind;
use tracing::{debug, error, warn};

use super::BackendContext;

#[derive(Copy, Clone, PartialEq, Debug)]
pub(super) enum BlitSource {
    Current,
    Prev,
    Composition,
}

impl super::Renderer {
    pub(super) fn select_blit_source(&self) -> Option<BlitSource> {
        if self.current_texture.is_some() || self.current_external_view_available() {
            if !self.transition_active
                || (self.prev_texture.is_none() && !self.prev_external_view_available())
            {
                Some(BlitSource::Current)
            } else if self.transition_active
                && (self.prev_texture.is_some() || self.prev_external_view_available())
                && (self.current_texture.is_some() || self.current_external_view_available())
                && self.composition_texture.is_some()
                && self.composition_texture_view.is_some()
            {
                if !self.transition_rendered_this_frame {
                    debug!(
                        "[RENDER] {}: Using composition (rendered_this_frame={})",
                        self.name, self.transition_rendered_this_frame
                    );
                }
                Some(BlitSource::Composition)
            } else if self.transition_active
                && (self.prev_texture.is_some() || self.prev_external_view_available())
            {
                warn!(
                    "[RENDER] {}: Transition FALLBACK to PREV - missing resources (comp_view={})",
                    self.name,
                    self.composition_texture_view.is_some()
                );
                Some(BlitSource::Prev)
            } else {
                Some(BlitSource::Current)
            }
        } else if self.prev_texture.is_some() || self.prev_external_view_available() {
            debug!(
                "[RENDER] {}: No current_texture, falling back to prev_texture (transition_active={})",
                self.name, self.transition_active
            );
            Some(BlitSource::Prev)
        } else {
            None
        }
    }

    pub(super) fn present_black_frame(
        &mut self,
        context: BackendContext,
        encoder: wgpu::CommandEncoder,
        output: wgpu::SurfaceTexture,
        view: &wgpu::TextureView,
        render_start: std::time::Instant,
    ) {
        debug!(
            "[RENDER] {}: No blit source available (current={}, prev={})",
            self.name,
            self.current_texture.is_some(),
            self.prev_texture.is_some()
        );

        if !matches!(context, BackendContext::X11) {
            return;
        }

        let mut encoder = encoder;
        {
            let _clear_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("Black Frame Render Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view,
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
        }

        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        output.present();
        self.last_present_time = std::time::Instant::now();
        self.needs_redraw = false;

        if let Some(metrics) = &self.metrics {
            metrics.record_renderer_present(RendererPresentKind::Black);
            metrics.record_renderer_cpu_time(render_start.elapsed());
        }
    }

    pub(super) fn ensure_blit_bind_group(&mut self, blit_source: BlitSource) -> bool {
        let is_comp = blit_source == BlitSource::Composition;
        let is_prev = blit_source == BlitSource::Prev;
        let needs_recreate = self.blit_bind_group.is_none()
            || self.blit_source_is_composition != is_comp
            || self.blit_source_is_prev != is_prev;

        if !needs_recreate {
            return true;
        }

        let texture_view = match blit_source {
            BlitSource::Current => self.current_blit_view(),
            BlitSource::Prev => self.prev_blit_view(),
            BlitSource::Composition => self.composition_texture_view.as_ref(),
        };

        match texture_view {
            Some(view) => {
                let bind_group = self.build_blit_bind_group(view, "Blit Bind Group");
                self.store_blit_bind_group(bind_group, is_comp, is_prev);
                true
            }
            None => self.create_fallback_blit_bind_group(blit_source),
        }
    }

    fn current_blit_view(&self) -> Option<&wgpu::TextureView> {
        self.current_texture_view
            .as_ref()
            .or_else(|| self.current_external_blit_view())
    }

    #[cfg(feature = "mpv-backend")]
    fn current_external_blit_view(&self) -> Option<&wgpu::TextureView> {
        self.current_external_view.as_ref()
    }

    #[cfg(not(feature = "mpv-backend"))]
    fn current_external_blit_view(&self) -> Option<&wgpu::TextureView> {
        None
    }

    fn prev_blit_view(&self) -> Option<&wgpu::TextureView> {
        self.prev_texture_view
            .as_ref()
            .or_else(|| self.prev_external_blit_view())
    }

    #[cfg(feature = "mpv-backend")]
    fn prev_external_blit_view(&self) -> Option<&wgpu::TextureView> {
        self.prev_external_view.as_ref()
    }

    #[cfg(not(feature = "mpv-backend"))]
    fn prev_external_blit_view(&self) -> Option<&wgpu::TextureView> {
        None
    }

    fn create_fallback_blit_bind_group(&mut self, blit_source: BlitSource) -> bool {
        let fallback_view = match blit_source {
            BlitSource::Composition => {
                warn!(
                    "[RENDER] {}: Composition texture view missing, falling back to prev",
                    self.name
                );
                self.prev_texture_view.as_ref().or_else(|| {
                    warn!(
                        "[RENDER] {}: Prev texture view also missing, falling back to current",
                        self.name
                    );
                    self.current_blit_view()
                })
            }
            BlitSource::Prev => {
                warn!(
                    "[RENDER] {}: Prev texture view missing, falling back to current",
                    self.name
                );
                self.current_blit_view()
            }
            BlitSource::Current => {
                error!(
                    "[RENDER] {}: Current texture view missing, cannot render",
                    self.name
                );
                None
            }
        };

        let Some(view) = fallback_view else {
            error!(
                "Texture view missing for blit source {:?} and all fallbacks failed (current={}, prev={}, composition={})",
                blit_source,
                self.current_texture_view.is_some(),
                self.prev_texture_view.is_some(),
                self.composition_texture_view.is_some()
            );
            return false;
        };

        let is_prev = matches!(blit_source, BlitSource::Prev | BlitSource::Composition);
        let bind_group = self.build_blit_bind_group(view, "Blit Bind Group (Fallback)");
        self.store_blit_bind_group(bind_group, false, is_prev);
        true
    }

    pub(super) fn build_blit_bind_group(
        &self,
        texture_view: &wgpu::TextureView,
        label: &'static str,
    ) -> wgpu::BindGroup {
        self.ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some(label),
                layout: &self.ctx.blit_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: self.uniform_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(texture_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            })
    }

    fn store_blit_bind_group(
        &mut self,
        bind_group: wgpu::BindGroup,
        is_composition: bool,
        is_prev: bool,
    ) {
        self.blit_bind_group = Some(bind_group);
        self.blit_source_is_composition = is_composition;
        self.blit_source_is_prev = is_prev;
    }
}
