use crate::content::sessions::{
    clear_pending_video_session_if_matches, stop_video_player_in_background,
};
use crate::main_loop::{MainLoopContext, PlayerEventMsg};
use crate::video::PlayerEventKind;
use std::time::Instant;
use tracing::{debug, error};

impl MainLoopContext {
    pub fn drain_player_events(
        &mut self,
        player_event_buf: Option<PlayerEventMsg>,
        loop_start: Instant,
    ) {
        let mut events = Vec::new();
        if let Some(event) = player_event_buf {
            events.push(event);
        }
        while let Ok(event) = self.player_event_rx.try_recv() {
            events.push(event);
        }

        for event in events {
            let is_pending = self
                .pending_video_switches
                .get(&event.source_id)
                .is_some_and(|pending| pending.session_id == event.session_id);
            let is_active = self
                .renderers
                .get(&event.source_id)
                .is_some_and(|renderer| renderer.active_video_session_id == event.session_id);

            if !is_pending && !is_active {
                debug!(
                    "[VIDEO] Ignoring stale player event {} session={} kind={:?} reason={}",
                    event.source_id, event.session_id, event.kind, event.reason
                );
                continue;
            }

            match event.kind {
                PlayerEventKind::Eos => {
                    debug!(
                        "[VIDEO] {} session={} reported EOS ({})",
                        event.source_id, event.session_id, event.reason
                    );
                }
                PlayerEventKind::Error | PlayerEventKind::FatalLifecycle => {
                    error!(
                        "[VIDEO] {} session={} runtime {:?}: {}",
                        event.source_id, event.session_id, event.kind, event.reason
                    );
                    self.metrics.record_error("video_runtime");
                    if is_pending {
                        self.pending_video_switches.remove(&event.source_id);
                        clear_pending_video_session_if_matches(
                            &self.pending_video_sessions,
                            &event.source_id,
                            event.session_id,
                        );
                    }

                    if is_active
                        && self
                            .video_players
                            .get(&event.source_id)
                            .is_some_and(|player| player.session_id() == event.session_id)
                        && let Some(player) = self.video_players.remove(&event.source_id)
                    {
                        stop_video_player_in_background(event.source_id.clone(), player);
                    }

                    if let Some(renderer) = self.renderers.get_mut(&event.source_id) {
                        if renderer.active_video_session_id == event.session_id {
                            renderer.abort_transition();
                        }
                    }

                    if !self.pending_video_switches.contains_key(&event.source_id) {
                        self.handle_content_failure(&event.source_id, &event.reason, loop_start);
                    }
                }
            }
        }
    }
}
