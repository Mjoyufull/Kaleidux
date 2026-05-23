use crate::observability::present::FrameCallbackKind;
use smithay_client_toolkit::shell::{WaylandSurface, wlr_layer::LayerSurface};
use std::sync::OnceLock;
use tracing::warn;
use wayland_client::{Proxy, QueueHandle};

fn trace_frame_events_enabled() -> bool {
    if crate::observability::trace_all::trace_all_enabled() {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_TRACE_FRAME_EVENTS")
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

fn video_callback_full_damage_enabled() -> bool {
    std::env::var("KLD_VIDEO_FRAME_CALLBACK_DAMAGE")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "full" | "surface" | "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

impl super::Renderer {
    /// Request a frame callback from Wayland compositor
    /// This should be called when we need to render, and we'll wait for the callback
    pub fn request_frame_callback(
        &mut self,
        layer_surface: &LayerSurface,
        qh: &QueueHandle<crate::wayland::WaylandBackend>,
    ) -> bool {
        if self.frame_callback_pending {
            // Check failsafe: if pending for > 500ms, assume lost and allow re-request
            if let Some(r) = self.last_frame_request {
                if r.elapsed().as_millis() > 500 {
                    warn!(
                        "[FRAME] {}: Frame callback stuck for 500ms, re-requesting!",
                        self.name
                    );
                    self.frame_callback_pending = false; // Reset to allow re-request
                } else {
                    return false; // Truly pending
                }
            }
        }

        let wl_surface = layer_surface.wl_surface();
        wl_surface.frame(qh, wl_surface.clone());
        let full_surface_damage = self.needs_full_frame_callback_damage();
        if full_surface_damage {
            wl_surface.damage_buffer(
                0,
                0,
                self.config.width.max(1) as i32,
                self.config.height.max(1) as i32,
            );
        } else {
            wl_surface.damage_buffer(0, 0, 1, 1);
        }

        // Commit after requesting the callback. Static clean frames and steady video callbacks
        // use minimal damage; content swaps, transitions, and first presents use full damage.
        // KLD_VIDEO_FRAME_CALLBACK_DAMAGE=full restores historical full-surface video
        // callback damage for compositor debugging.
        wl_surface.commit();

        self.frame_callback_pending = true;
        self.last_frame_request = Some(std::time::Instant::now());
        if let Some(metrics) = &self.metrics {
            metrics.record_frame_callback_request(self.frame_callback_kind());
            metrics.record_frame_callback_damage(full_surface_damage);
        }
        if trace_frame_events_enabled() {
            tracing::trace!(
                "[FRAME] {}: Requested frame callback (configured={}, needs_redraw={}, transition_progress={:.3})",
                self.name,
                self.configured,
                self.needs_redraw,
                self.transition_progress
            );
        }
        if crate::observability::trace_all::trace_all_enabled() {
            tracing::trace!(
                "[TRACE5][FRAME-CALLBACK-REQUEST] output={} surface=#{} full_damage={} configured={} content={:?} callback_kind={:?} transition={} progress={:.3} current_texture={} prev_texture={}",
                self.name,
                wl_surface.id().protocol_id(),
                full_surface_damage,
                self.configured,
                self.valid_content_type,
                self.frame_callback_kind(),
                self.transition_active,
                self.transition_progress,
                self.current_texture.is_some(),
                self.prev_texture.is_some()
            );
        }
        true
    }

    fn needs_full_frame_callback_damage(&self) -> bool {
        self.transition_active
            || self.content_swap_pending
            || (self.valid_content_type == crate::queue::ContentType::Video
                && video_callback_full_damage_enabled())
            || self.current_texture.is_none()
            || self.prev_texture.is_some()
    }

    fn frame_callback_kind(&self) -> FrameCallbackKind {
        if self.transition_active {
            return FrameCallbackKind::Transition;
        }

        match self.valid_content_type {
            crate::queue::ContentType::Image => FrameCallbackKind::StaticImage,
            crate::queue::ContentType::Video => FrameCallbackKind::AppsinkVideo,
        }
    }
}
