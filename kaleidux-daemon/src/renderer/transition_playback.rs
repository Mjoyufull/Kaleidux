//! Holds an incoming video after its first successful upload and until present.

use super::Renderer;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TransitionPlayback {
    Playing,
    HoldFirstFrame,
    Held(Option<u64>),
}

impl Renderer {
    pub(crate) fn request_transition_playback_hold(&mut self) {
        self.transition_playback = TransitionPlayback::HoldFirstFrame;
    }

    pub(crate) fn hold_transition_playback_if_ready(&mut self, position_ns: Option<u64>) -> bool {
        if self.transition_playback == TransitionPlayback::HoldFirstFrame && self.transition_active
        {
            self.transition_playback = TransitionPlayback::Held(position_ns);
            true
        } else {
            false
        }
    }

    pub(crate) fn pause_incoming_after_upload(
        &mut self,
        player: Option<&crate::video::VideoPlayer>,
        position_ns: Option<u64>,
    ) {
        if let Some(player) = player
            && self.hold_transition_playback_if_ready(position_ns)
            && let Err(error) = player.pause()
        {
            self.end_transition_playback_hold();
            tracing::warn!("[VIDEO] Unable to hold incoming playback: {error}");
        }
    }

    pub(crate) fn transition_playback_is_held(&self) -> bool {
        matches!(self.transition_playback, TransitionPlayback::Held(_))
    }

    pub(crate) fn held_transition_position(&self) -> Option<u64> {
        match self.transition_playback {
            TransitionPlayback::Held(position) => position,
            _ => None,
        }
    }

    pub(crate) fn end_transition_playback_hold(&mut self) -> bool {
        let held = self.transition_playback_is_held();
        self.transition_playback = TransitionPlayback::Playing;
        held
    }
}
