use crate::shaders::Transition;
use tracing::{debug, info};

pub(crate) fn random_transition_prewarm_set() -> Vec<Transition> {
    vec![
        Transition::Fade,
        Transition::CrossZoom { strength: 0.3 },
        Transition::Radial { smoothness: 0.5 },
        Transition::Circle,
        Transition::Directional {
            direction: [1.0, 0.0],
        },
        Transition::SimpleZoom {
            zoom_quickness: 0.5,
        },
        Transition::Ripple {
            amplitude: 0.1,
            speed: 1.0,
        },
        Transition::Swirl,
        Transition::Pixelize {
            squares_min: [10, 10],
            steps: 10,
        },
        Transition::Mosaic { endx: 20, endy: 20 },
        Transition::Burn,
        Transition::CrossWarp,
        Transition::Dreamy,
        Transition::Morph { strength: 0.1 },
        Transition::Wind { size: 0.2 },
        Transition::Dissolve {
            line_width: 0.1,
            spread_clr: [0.0, 0.0, 0.0],
            hot_clr: [0.9, 0.6, 0.1],
            pow: 5.0,
            intensity: 1.0,
        },
        Transition::FadeColor {
            color: [0.0, 0.0, 0.0],
            color_phase: 0.4,
        },
        Transition::Overexposure,
        Transition::FilmBurn { seed: 2.31 },
        Transition::Pinwheel { speed: 2.0 },
        Transition::Heart,
    ]
}

pub(crate) fn transition_prewarm_candidates(transition: &Transition) -> Vec<Transition> {
    if matches!(transition, Transition::Random) {
        random_transition_prewarm_set()
    } else if matches!(transition, Transition::Fade) {
        vec![Transition::Fade]
    } else {
        vec![Transition::Fade, transition.clone()]
    }
}

impl super::Renderer {
    pub fn switch_content(&mut self) {
        let had_current = self.current_texture.is_some();
        let flattened_active_transition = self.freeze_active_transition_to_current();

        // If a previous transition left a prev texture behind, release it before preparing
        // the next swap. The current texture stays visible until replacement upload is ready.
        if !flattened_active_transition {
            if let Some(old_prev) = self.prev_texture.take() {
                if let Some((w, h)) = self.prev_texture_size.take() {
                    debug!(
                        "[TRANSITION] {}: Releasing carry-over prev_texture before new switch",
                        self.name
                    );
                    self.ctx.return_texture_to_pool(old_prev, w, h);
                }
                drop(self.prev_texture_view.take());
            }
        }

        self.content_swap_pending = true;
        self.transition_progress = 1.0;
        self.transition_start_time = None; // Will be set when content is uploaded
        self.transition_active = false; // Will be set to true when replacement content is ready
        self.transition_just_completed = false; // Reset completion flag
        self.presented_video_session_id = 0;
        self.display_timer_pending = false;
        self.display_timer_ready = false;
        self.transition_bind_group = None; // Invalidate
        self.blit_bind_group = None; // Invalidate
        self.batch_start_time = None; // Reset
        self.video_first_frame_time = if self.valid_content_type == crate::queue::ContentType::Video
        {
            Some(std::time::Instant::now())
        } else {
            None
        };
        self.needs_redraw = true;

        debug!(
            "[TRANSITION] {}: switch_content() - had_current={}, prev_texture={}, pending_swap={}, flattened_active_transition={}",
            self.name,
            had_current,
            self.prev_texture.is_some(),
            self.content_swap_pending,
            flattened_active_transition
        );
    }

    pub fn abort_transition(&mut self) {
        if self.transition_active || self.content_swap_pending || self.current_texture.is_none() {
            if self.transition_active {
                info!(
                    "[TRANSITION] {}: Aborting transition due to load failure",
                    self.name
                );
            }
            self.content_swap_pending = false;
            self.transition_active = false;
            self.transition_just_completed = false; // Reset flag
            self.display_timer_pending = false;
            self.display_timer_ready = false;
            self.transition_progress = 1.0;
            self.transition_start_time = None;
            self.needs_redraw = true;
        }
    }

    /// Clears the renderer to black (removes current and previous textures)
    ///
    /// This explicitly drops all texture resources and forces WGPU to reclaim
    /// GPU memory immediately. Useful for cleanup and preventing memory leaks.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        // Explicitly drop textures to free GPU memory
        self.current_texture = None;
        self.current_texture_view = None;
        self.prev_texture = None;
        self.prev_texture_view = None;
        self.current_texture_size = None;
        self.prev_texture_size = None;
        self.last_video_source_size = None;
        self.last_video_presentation_size = None;
        self.transition_progress = 1.0;
        self.transition_active = false;
        self.transition_just_completed = false; // Reset flag
        self.content_swap_pending = false;
        self.display_timer_pending = false;
        self.display_timer_ready = false;
        self.transition_bind_group = None; // Invalidate
        self.blit_bind_group = None; // Invalidate
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;
        self.needs_redraw = true;
        self.release_composition_texture("clear");
        self.release_video_backend_resources("clear");
        // Reclaim memory immediately - this ensures GPU resources are freed
        // rather than waiting for WGPU's automatic cleanup
        self.active_video_session_id = 0; // Invalidate current video session
        self.configured = false; // Force re-config next time
        self.ctx.device.poll(wgpu::Maintain::Poll);
    }

    #[allow(dead_code)]
    pub fn recreate_surface(&mut self, surface: wgpu::Surface<'static>) {
        self.surface = surface;
        self.configured = false;
        self.needs_redraw = true;
    }

    pub(super) fn begin_content_swap(&mut self) {
        if !self.content_swap_pending {
            return;
        }

        if let Some(curr) = self.current_texture.take() {
            self.prev_texture_view = self.current_texture_view.take();
            self.prev_texture = Some(curr);
            self.prev_aspect = self.current_aspect;
            self.prev_texture_size = self.current_texture_size.take();
        } else {
            self.current_texture_view = None;
            self.current_texture_size = None;
        }

        self.content_swap_pending = false;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;
    }

    fn freeze_active_transition_to_current(&mut self) -> bool {
        if !self.transition_active {
            return false;
        }
        let Some(composition_texture) = self.composition_texture.take() else {
            return false;
        };
        let Some(composition_view) = self.composition_texture_view.take() else {
            self.composition_texture = Some(composition_texture);
            return false;
        };

        debug!(
            "[TRANSITION] {}: Flattening active transition into a stable snapshot before new switch",
            self.name
        );

        if let Some(curr) = self.current_texture.take() {
            if let Some((w, h)) = self.current_texture_size.take() {
                self.ctx.return_texture_to_pool(curr, w, h);
            }
        }

        self.release_prev_texture("flatten active transition");
        self.current_texture = Some(composition_texture);
        self.current_texture_view = Some(composition_view);
        self.current_texture_size = Some((self.config.width.max(1), self.config.height.max(1)));
        self.current_aspect = self.config.width.max(1) as f32 / self.config.height.max(1) as f32;
        self.transition_active = false;
        self.transition_just_completed = false;
        self.transition_progress = 1.0;
        self.transition_start_time = None;
        self.transition_stats = None;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;
        true
    }

    pub(super) fn update_transition_bind_group(&mut self) {
        // Skip recreation if bind group already exists — it's invalidated to None
        // whenever textures change (upload_image_data, upload_frame, switch_content)
        if self.transition_bind_group.is_some() {
            return;
        }

        // Only recreate if both texture views are present
        let prev_view = match self.prev_texture_view.as_ref() {
            Some(v) => v,
            None => {
                return;
            }
        };
        let current_view = match self.current_texture_view.as_ref() {
            Some(v) => v,
            None => {
                return;
            }
        };

        self.transition_bind_group = Some(self.ctx.device.create_bind_group(
            &wgpu::BindGroupDescriptor {
                label: Some("Transition Bind Group (Cached)"),
                layout: &self.ctx.transition_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: self.uniform_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(prev_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::TextureView(current_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            },
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_prewarm_candidates_include_fade_fallback() {
        let transitions = transition_prewarm_candidates(&Transition::Circle);
        assert!(transitions.iter().any(|t| matches!(t, Transition::Fade)));
        assert!(transitions.iter().any(|t| matches!(t, Transition::Circle)));
    }

    #[test]
    fn random_transition_prewarm_contains_multiple_variants() {
        let transitions = transition_prewarm_candidates(&Transition::Random);
        assert!(transitions.len() > 5);
        assert!(transitions.iter().any(|t| matches!(t, Transition::Fade)));
    }
}
