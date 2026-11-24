//! Local IPC for ordered wallpaper filters and screen-space morphs.

use serde::{Deserialize, Serialize};

/// Maximum number of active layers per stage and output.
pub const MAX_LAYERS_PER_STAGE: usize = 8;

/// Layers sample or draw after the wallpaper and its transition are composed.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LayerStage {
    Filter,
    Morph,
}

/// Replace one stage on the requested output or all active outputs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LayerSelection {
    pub output: Option<String>,
    pub stage: LayerStage,
    pub layers: Vec<String>,
    /// Zero makes layers passive. Other values request at most this many idle redraws per second.
    #[serde(default)]
    pub fps: u8,
}

/// Active ordered layers on an output.
#[derive(Debug, Deserialize, Serialize)]
pub struct OutputLayers {
    pub output: String,
    pub filters: Vec<String>,
    pub morphs: Vec<String>,
    pub filter_fps: u8,
    pub morph_fps: u8,
}
