use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct KEntry {
    pub path: String,
    pub multiplier: f32,
    pub count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum Request {
    #[serde(rename = "query_outputs")]
    QueryOutputs,
    #[serde(rename = "next")]
    Next { output: Option<String> },
    #[serde(rename = "jump")]
    Jump {
        path: String,
        output: Option<String>,
    },
    #[serde(rename = "set")]
    Set {
        path: String,
        output: Option<String>,
    },
    #[serde(rename = "img")]
    Img {
        path: String,
        output: Option<String>,
    },
    #[serde(rename = "prev")]
    Prev { output: Option<String> },
    #[serde(rename = "love")]
    Love { path: String, multiplier: f32 },
    #[serde(rename = "unlove")]
    Unlove { path: String },
    #[serde(rename = "loveitlist")]
    LoveitList,
    #[serde(rename = "pause")]
    Pause,
    #[serde(rename = "resume")]
    Resume,
    #[serde(rename = "inhibit")]
    Inhibit { reason: String },
    #[serde(rename = "uninhibit")]
    Uninhibit { reason: String },
    #[serde(rename = "inhibitors", alias = "inhibitors_list")]
    Inhibitors,
    #[serde(rename = "stop")]
    Stop,
    #[serde(rename = "reload")]
    Reload,
    #[serde(rename = "clear")]
    Clear { output: Option<String> },
    #[serde(rename = "kill")]
    Kill,
    #[serde(rename = "playlist")]
    Playlist(PlaylistCommand),
    #[serde(rename = "blacklist")]
    Blacklist(BlacklistCommand),
    #[serde(rename = "history")]
    History { output: Option<String> },
    #[serde(rename = "perf_snapshot")]
    PerfSnapshot,
}

pub const MAX_INHIBITOR_REASON_LEN: usize = 64;
pub const MAX_INHIBITORS: usize = 64;

pub fn validate_inhibit_reason(reason: &str) -> Result<(), &'static str> {
    if reason.is_empty() {
        return Err("inhibit reason cannot be empty");
    }
    if reason.trim().is_empty() {
        return Err("inhibit reason cannot be blank");
    }
    if reason.len() > MAX_INHIBITOR_REASON_LEN {
        return Err("inhibit reason exceeds maximum length");
    }
    if reason.chars().any(|c| c.is_control()) {
        return Err("inhibit reason cannot contain control characters");
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", content = "params")]
pub enum PlaylistCommand {
    #[serde(rename = "create")]
    Create { name: String },
    #[serde(rename = "delete")]
    Delete { name: String },
    #[serde(rename = "add")]
    Add { name: String, path: String },
    #[serde(rename = "remove")]
    Remove { name: String, path: String },
    #[serde(rename = "load")]
    Load { name: Option<String> },
    #[serde(rename = "list")]
    List,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", content = "params")]
pub enum BlacklistCommand {
    #[serde(rename = "add")]
    Add { path: String },
    #[serde(rename = "remove")]
    Remove { path: String },
    #[serde(rename = "list")]
    List,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Error(String),
    OutputInfo(Vec<OutputInfo>),
    LoveitList(Vec<KEntry>),
    Playlists(Vec<String>),
    Blacklist(Vec<String>),
    History(Vec<String>),
    PerfSnapshot(String),
    Inhibitors(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OutputInfo {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub current_wallpaper: Option<String>,
}
