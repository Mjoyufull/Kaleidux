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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OutputInfo {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub current_wallpaper: Option<String>,
}
