//! Defines the public wallpaper timeline and one-shot PNG request.

use serde::{Deserialize, Serialize};

pub const DEFAULT_FRAME_EDGE: u32 = 1920;
pub const MAX_FRAME_EDGE: u32 = 3840;
pub const MAX_FRAME_PNG_BYTES: usize = (MAX_FRAME_EDGE as usize).pow(2) * 4 + 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct FrameRequest {
    pub output: Option<String>,
    /// Decode from the selected excerpt start instead of reading the GPU frame.
    #[serde(default)]
    pub first: bool,
    /// Optional local file that has yet to be selected.
    pub source: Option<String>,
    /// Offset from the scene start, or from zero when a source is supplied.
    pub at_ns: Option<u64>,
    pub max_size: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WallpaperInfo {
    pub output: String,
    pub width: u32,
    pub height: u32,
    pub source: Option<String>,
    pub kind: Option<String>,
    pub loading: bool,
    pub paused: bool,
    pub transition_progress: Option<f32>,
    pub slideshow_duration_ns: u64,
    pub media_duration_ns: Option<u64>,
    pub scene_start_ns: Option<u64>,
    pub scene_duration_ns: Option<u64>,
    /// Playback position relative to the selected scene. Loops return to zero.
    pub position_ns: Option<u64>,
}
