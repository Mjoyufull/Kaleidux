//! Reuses the preceding displayed frame at a video loop seam.

use crate::{shaders::Transition, video::VideoFrame};

impl super::Renderer {
    pub(crate) fn frame_crosses_video_loop(&self, frame: &VideoFrame) -> bool {
        self.smooth_loop.is_some()
            && frame.session_id == self.active_video_session_id
            && self
                .last_loop_pts
                .zip(frame.pts_ns)
                .is_some_and(|(previous, next)| {
                    // Ignore duplicate frames and small clock regressions. Decoder
                    // timestamps refer to media time, not time since publication.
                    let tolerance = frame.duration_ns.unwrap_or(0).max(100_000_000);
                    previous.saturating_sub(next) >= tolerance
                })
    }

    pub(crate) fn observe_video_loop(&mut self, frame: &VideoFrame) {
        if self.smooth_loop.is_none() || frame.session_id != self.active_video_session_id {
            return;
        }
        let seam = self.frame_crosses_video_loop(frame);
        if let Some(next) = frame.pts_ns {
            self.last_loop_pts = Some(if seam {
                next
            } else {
                self.last_loop_pts.unwrap_or(next).max(next)
            });
        }
        if !seam || self.content_swap_pending || self.transition_active {
            return;
        }
        self.snapshot_yuv_for_transition();
        if !self.has_current_texture() {
            return;
        }
        let setting = self.smooth_loop.as_ref().expect("loop setting active");
        let transition = match setting
            .transition
            .as_ref()
            .unwrap_or(&self.active_transition)
        {
            Transition::Random => crate::shaders::ShaderManager::pick_random_transition(),
            transition => transition.clone(),
        };
        let duration = setting.duration_ms as f32 / 1000.0;
        if let Some(map) = setting.prepared_map.clone() {
            self.set_reveal_map(&map);
        }
        self.loop_playback = Some((transition, duration));
        self.content_swap_pending = true;
        self.begin_content_swap();
        self.transition_active = true;
        self.transition_has_rendered = false;
        self.transition_start_time = None;
        self.transition_progress = 0.0;
        self.prewarm_transition_resources();
        tracing::debug!("Video loop blend started on {}", self.name);
    }
}
