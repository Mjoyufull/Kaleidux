use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MediaPlayerRequest {
    pub player: Option<String>,
    #[serde(default)]
    pub action: MediaPlayerAction,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum MediaPlayerAction {
    #[default]
    Status,
    Play,
    Pause,
    PlayPause,
    Stop,
    Next,
    Previous,
    Seek {
        offset_us: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackState {
    Stopped,
    Paused,
    Playing,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MediaPlayerStatus {
    pub player: String,
    pub state: PlaybackState,
    pub track_id: Option<String>,
    pub title: Option<String>,
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub position_us: Option<i64>,
    pub duration_us: Option<i64>,
    pub artwork_url: Option<String>,
    pub artwork_path: Option<String>,
    pub can_play: bool,
    pub can_pause: bool,
    pub can_next: bool,
    pub can_previous: bool,
    pub can_seek: bool,
}
