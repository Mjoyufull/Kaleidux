//! Local IPC for ordered wallpaper shaders and screen-space morphs.

use serde::{Deserialize, Serialize};

/// Maximum number of active layers per stage and output.
pub const MAX_LAYERS_PER_STAGE: usize = 8;
/// Maximum number of active morphs across all groups on an output.
pub const MAX_MORPHS_PER_OUTPUT: usize = 64;
/// Animation paced by the display without an application frame-rate limit.
pub const LAYER_FPS_UNLIMITED: u16 = u16::MAX;

/// Layers sample or draw after the wallpaper and its transition are composed.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LayerStage {
    #[serde(rename = "shader", alias = "filter")]
    Filter,
    Morph,
    Wallpaper,
}

/// Replace one stage on the requested output or all active outputs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LayerSelection {
    pub output: Option<String>,
    pub stage: LayerStage,
    pub layers: Vec<String>,
    /// Zero makes layers passive. Other values request at most this many idle redraws per second.
    #[serde(default)]
    pub fps: u16,
    /// Replace only this named morph group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<MorphLayout>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MorphLayout {
    /// Zero uses a single row. One uses a single column.
    pub columns: usize,
    pub bounds: [f32; 4],
    pub gap: f32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MorphGroup {
    pub name: String,
    pub layers: Vec<String>,
    pub fps: u16,
}

/// Active ordered layers on an output.
#[derive(Debug, Deserialize, Serialize)]
pub struct OutputLayers {
    pub output: String,
    #[serde(rename = "shaders", alias = "filters")]
    pub filters: Vec<String>,
    pub morphs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub morph_groups: Vec<MorphGroup>,
    #[serde(rename = "shader_fps", alias = "filter_fps")]
    pub filter_fps: u16,
    pub morph_fps: u16,
    pub wallpaper: Vec<String>,
    pub wallpaper_fps: u16,
}

/// Latest values supplied by an input provider. Omitted fields retain their value.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayerInputs {
    pub output: Option<String>,
    /// Normalized x/y from the top left, followed by primary/secondary button state.
    pub pointer: Option<[f32; 4]>,
    /// False when the cursor leaves. Supplying coordinates alone marks it active.
    pub pointer_active: Option<bool>,
    /// Up to 64 normalized spectrum bands, from low to high frequency.
    pub audio: Option<Vec<f32>>,
    pub data: Option<[f32; 4]>,
    /// Additional shader data. Up to 32 values, padded with zeros when provided.
    pub controls: Option<Vec<f32>>,
    /// Year, month, day, and seconds since local midnight.
    pub date: Option<[f32; 4]>,
    /// Restore the native clock when no date override is supplied.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub live_date: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MorphGesture {
    #[default]
    Primary,
    Secondary,
    ScrollUp,
    ScrollDown,
}

impl MorphGesture {
    pub fn index(self) -> usize {
        self as usize
    }
}

/// An activation inside a morph's configured rectangle.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MorphEvent {
    pub output: String,
    pub action: String,
    #[serde(default)]
    pub gesture: MorphGesture,
    /// Normalized coordinates within the selected morph, from its top left.
    pub position: [f32; 2],
}
