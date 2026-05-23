use super::RetainedTextureFootprint;
use super::texture::texture_byte_size;
use super::video_layout::yuv420_aux_byte_size;
use tracing::{debug, warn};

impl super::Renderer {
    pub(super) fn release_cuda_cache(&mut self) {
        self.cuda_nv12_bind_group = None;
        if let Some(cuda_cache) = self.cuda_textures.take() {
            if let Some(interop) = self.ctx.cuda_interop.lock().as_ref() {
                interop.free_exportable(cuda_cache.y_cuda_alloc);
                interop.free_exportable(cuda_cache.uv_cuda_alloc);
            } else {
                warn!(
                    "[VIDEO] {}: Dropping CUDA texture cache without CUDA interop; exportable allocations will leak until process exit",
                    self.name
                );
            }
        }
    }

    pub(super) fn arm_display_timer_on_present(&mut self) {
        self.display_timer_pending = true;
        self.display_timer_ready = false;
    }

    pub fn take_display_timer_ready(&mut self) -> bool {
        std::mem::take(&mut self.display_timer_ready)
    }

    pub(super) fn release_nv12_staging(&mut self, reason: &str) {
        if self.nv12_staging_size.is_some() {
            debug!(
                "[VIDEO] {}: Releasing NV12 staging textures ({})",
                self.name, reason
            );
        }
        self.nv12_y_texture = None;
        self.nv12_uv_texture = None;
        self.nv12_y_view = None;
        self.nv12_uv_view = None;
        self.nv12_staging_size = None;
    }

    pub(super) fn release_i420_staging(&mut self, reason: &str) {
        if self.i420_staging_size.is_some() {
            debug!(
                "[VIDEO] {}: Releasing I420 staging textures ({})",
                self.name, reason
            );
        }
        self.i420_y_texture = None;
        self.i420_u_texture = None;
        self.i420_v_texture = None;
        self.i420_y_view = None;
        self.i420_u_view = None;
        self.i420_v_view = None;
        self.i420_staging_size = None;
    }

    pub(super) fn release_video_backend_resources(&mut self, reason: &str) {
        self.release_nv12_staging(reason);
        self.release_i420_staging(reason);
        self.release_cuda_cache();
    }

    pub(super) fn release_prev_texture(&mut self, reason: &str) {
        if let Some(prev_tex) = self.prev_texture.take() {
            debug!(
                "[TRANSITION] {}: Releasing stale prev_texture ({})",
                self.name, reason
            );
            if let Some((w, h)) = self.prev_texture_size.take() {
                self.ctx.return_texture_to_pool(prev_tex, w, h);
            }
        }
        self.prev_texture_view = None;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
    }

    pub(super) fn release_composition_texture(&mut self, reason: &str) {
        if self.composition_texture.is_some() || self.composition_texture_view.is_some() {
            debug!(
                "[TRANSITION] {}: Releasing composition texture ({})",
                self.name, reason
            );
        }
        self.composition_texture = None;
        self.composition_texture_view = None;
        self.transition_bind_group = None;
        if self.blit_source_is_composition {
            self.blit_bind_group = None;
            self.blit_source_is_composition = false;
        }
    }

    pub fn retained_texture_footprint(&self) -> RetainedTextureFootprint {
        let current_bytes = match (self.current_texture.as_ref(), self.current_texture_size) {
            (Some(texture), Some((w, h))) => texture_byte_size(w, h, texture.mip_level_count()),
            _ => 0,
        };
        let prev_bytes = match (self.prev_texture.as_ref(), self.prev_texture_size) {
            (Some(texture), Some((w, h))) => texture_byte_size(w, h, texture.mip_level_count()),
            _ => 0,
        };
        let composition_bytes = if self.composition_texture.is_some() {
            texture_byte_size(self.config.width.max(1), self.config.height.max(1), 1)
        } else {
            0
        };

        let mut video_aux_bytes = 0u64;
        if let Some((w, h)) = self.nv12_staging_size {
            video_aux_bytes = video_aux_bytes.saturating_add(yuv420_aux_byte_size(w, h));
        }
        if let Some((w, h)) = self.i420_staging_size {
            video_aux_bytes = video_aux_bytes.saturating_add(yuv420_aux_byte_size(w, h));
        }
        if let Some(cache) = &self.cuda_textures {
            video_aux_bytes =
                video_aux_bytes.saturating_add(yuv420_aux_byte_size(cache.width, cache.height));
        }

        RetainedTextureFootprint {
            current_bytes,
            prev_bytes,
            composition_bytes,
            video_aux_bytes,
        }
    }

    /// Check if current_texture exists.
    pub fn has_current_texture(&self) -> bool {
        self.current_texture.is_some()
    }

    /// Check if any renderable content exists (current or previous texture)
    pub fn has_any_content(&self) -> bool {
        self.current_texture.is_some() || self.prev_texture.is_some()
    }

    pub fn should_hold_video_frame_for_callback(&self) -> bool {
        self.valid_content_type == crate::queue::ContentType::Video
            && self.current_texture.is_some()
            && !self.content_swap_pending
            && !self.transition_active
            && self.frame_callback_pending
            && !self.frame_callback_pending_too_long(1000)
    }

    pub fn should_upload_video_frame_on_callback(&self) -> bool {
        self.valid_content_type == crate::queue::ContentType::Video
            && (!self.has_current_texture() || self.content_swap_pending || self.transition_active)
    }

    pub fn needs_wayland_immediate_work(&self) -> bool {
        let wants_to_draw = self.transition_active || self.needs_redraw;

        wants_to_draw && (!self.frame_callback_pending || self.frame_callback_pending_too_long(500))
    }

    pub fn next_wayland_retry_deadline(
        &self,
        retry_after: std::time::Duration,
    ) -> Option<std::time::Instant> {
        if self.needs_redraw && self.frame_callback_pending {
            self.last_frame_request.map(|t| t + retry_after)
        } else {
            None
        }
    }

    pub fn trim_idle_retained_resources(&mut self) {
        if !self.transition_active && !self.content_swap_pending {
            if self.prev_texture.is_some() {
                self.release_prev_texture("idle trim");
            }
            if self.composition_texture.is_some() {
                self.release_composition_texture("idle trim");
            }
        }

        if !self.has_any_content() || self.valid_content_type != crate::queue::ContentType::Video {
            self.release_video_backend_resources("idle trim");
        }
    }

    /// Get the duration that the frame callback has been pending, if any
    /// Returns None if no callback is pending or if timing info is unavailable
    pub fn frame_callback_pending_duration(&self) -> Option<std::time::Duration> {
        if self.frame_callback_pending {
            self.last_frame_request.map(|t| t.elapsed())
        } else {
            None
        }
    }

    /// Check if frame callback has been pending for too long (indicating we're stuck)
    /// This is used to prevent memory leaks by throttling frame uploads when stuck
    pub fn frame_callback_pending_too_long(&self, threshold_ms: u64) -> bool {
        self.frame_callback_pending_duration()
            .is_some_and(|d| d.as_millis() > threshold_ms as u128)
    }
}

impl Drop for super::Renderer {
    fn drop(&mut self) {
        // Abort background shader precompilation task to prevent resource leaks
        if let Some(handle) = self.shader_precompile_handle.take() {
            handle.abort();
        }

        // Clean up CUDA exportable allocations (Audit Point 1 in renderer.rs)
        if let Some(cuda_cache) = self.cuda_textures.take() {
            if let Some(interop) = self.ctx.cuda_interop.lock().as_ref() {
                interop.free_exportable(cuda_cache.y_cuda_alloc);
                interop.free_exportable(cuda_cache.uv_cuda_alloc);
            } else {
                warn!(
                    "[VIDEO] {}: Renderer dropped without CUDA interop; exportable allocations will leak until process exit",
                    self.name
                );
            }
        }
        self.nv12_y_texture = None;
        self.nv12_uv_texture = None;
        self.nv12_y_view = None;
        self.nv12_uv_view = None;
        self.nv12_staging_size = None;
        self.i420_y_texture = None;
        self.i420_u_texture = None;
        self.i420_v_texture = None;
        self.i420_y_view = None;
        self.i420_u_view = None;
        self.i420_v_view = None;
        self.i420_staging_size = None;
    }
}
