use crate::background::{self, BackgroundWorkKind};
use crate::queue;
use crate::video;
use kaleidux_common::Transition;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tracing::debug;

pub type PendingVideoSessions = Arc<Mutex<HashMap<String, u64>>>;

pub enum VideoPlayerResult {
    Success(
        String,
        u64,
        Box<video::VideoPlayer>,
        Option<video::VideoFrame>,
    ),
    Failure(String, u64),
}

#[derive(Debug, Clone)]
pub struct PendingVideoSwitch {
    pub session_id: u64,
    pub batch_id: Option<u64>,
    pub batch_trigger_time: Option<Instant>,
    pub transition: Transition,
}

pub(crate) fn stop_video_player_in_background(name: String, mut player: video::VideoPlayer) {
    let _ = player.request_stop();
    if let Some(handle) =
        background::spawn_blocking_tracked(BackgroundWorkKind::PlayerStop, move || {
            debug!("[VIDEO] {}: Finalizing player stop on blocking pool", name);
            let _ = player.stop();
        })
    {
        drop(handle);
    }
}

pub(crate) fn should_accept_video_frame(
    valid_content_type: queue::ContentType,
    active_video_session_id: u64,
    frame_session_id: u64,
) -> bool {
    valid_content_type == queue::ContentType::Video
        && active_video_session_id != 0
        && active_video_session_id == frame_session_id
}

pub(crate) fn set_pending_video_session(
    pending_video_sessions: &PendingVideoSessions,
    name: &str,
    session_id: Option<u64>,
) {
    let Ok(mut sessions) = pending_video_sessions.lock() else {
        return;
    };

    match session_id {
        Some(session_id) => {
            sessions.insert(name.to_string(), session_id);
        }
        None => {
            sessions.remove(name);
        }
    }
}

pub(crate) fn pending_video_session_matches(
    pending_video_sessions: &PendingVideoSessions,
    name: &str,
    session_id: u64,
) -> bool {
    pending_video_sessions
        .lock()
        .ok()
        .and_then(|sessions| sessions.get(name).copied())
        == Some(session_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_video_session_state_replaces_and_clears() {
        let sessions: PendingVideoSessions = Arc::new(Mutex::new(HashMap::new()));

        set_pending_video_session(&sessions, "DP-2", Some(7));
        assert!(pending_video_session_matches(&sessions, "DP-2", 7));
        assert!(!pending_video_session_matches(&sessions, "DP-2", 8));

        set_pending_video_session(&sessions, "DP-2", Some(9));
        assert!(!pending_video_session_matches(&sessions, "DP-2", 7));
        assert!(pending_video_session_matches(&sessions, "DP-2", 9));

        set_pending_video_session(&sessions, "DP-2", None);
        assert!(!pending_video_session_matches(&sessions, "DP-2", 9));
    }

    #[test]
    fn video_frames_are_rejected_for_non_video_outputs() {
        assert!(!should_accept_video_frame(
            queue::ContentType::Image,
            42,
            42
        ));
    }

    #[test]
    fn video_frames_are_rejected_for_stale_sessions() {
        assert!(!should_accept_video_frame(
            queue::ContentType::Video,
            42,
            41
        ));
    }

    #[test]
    fn video_frames_are_accepted_for_active_sessions() {
        assert!(should_accept_video_frame(queue::ContentType::Video, 42, 42));
    }
}
