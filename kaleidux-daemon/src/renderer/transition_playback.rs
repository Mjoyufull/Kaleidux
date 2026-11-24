//! Holds an incoming video after its first successful upload and until present.

use super::Renderer;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TransitionPlayback {
    Playing,
    HoldFirstFrame,
    Held,
}

impl Renderer {
    pub(crate) fn request_transition_playback_hold(&mut self) {
        self.transition_playback = TransitionPlayback::HoldFirstFrame;
    }

    pub(crate) fn hold_transition_playback_if_ready(&mut self) -> bool {
        if self.transition_playback == TransitionPlayback::HoldFirstFrame && self.transition_active
        {
            self.transition_playback = TransitionPlayback::Held;
            true
        } else {
            false
        }
    }

    pub(crate) fn pause_incoming_after_upload(
        &mut self,
        player: Option<&crate::video::VideoPlayer>,
    ) {
        if let Some(player) = player
            && self.hold_transition_playback_if_ready()
            && let Err(error) = player.pause()
        {
            self.end_transition_playback_hold();
            tracing::warn!("[VIDEO] Unable to hold incoming playback: {error}");
        }
    }

    pub(crate) fn transition_playback_is_held(&self) -> bool {
        self.transition_playback == TransitionPlayback::Held
    }

    pub(crate) fn end_transition_playback_hold(&mut self) -> bool {
        std::mem::replace(&mut self.transition_playback, TransitionPlayback::Playing)
            == TransitionPlayback::Held
    }
}
