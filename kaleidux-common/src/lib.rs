#![allow(
    dead_code,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_assignments,
    unused_attributes
)]
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod protocol;
mod transition_defaults;
mod transition_impl;
mod transition_params;

use transition_defaults::*;

pub use protocol::{BlacklistCommand, KEntry, OutputInfo, PlaylistCommand, Request, Response};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum Transition {
    Angular {
        #[serde(default = "df_0")]
        starting_angle: f32,
    },
    BookFlip,
    Bounce {
        #[serde(default = "df_black_alpha")]
        shadow_colour: [f32; 4],
        #[serde(default = "df_0_075")]
        shadow_height: f32,
        #[serde(default = "df_3")]
        bounces: f32,
    },
    BowTieHorizontal,
    BowTieVertical,
    BowTieWithParameter {
        #[serde(default = "df_0_5")]
        adjust: f32,
        #[serde(default = "df_false")]
        reverse: bool,
    },
    Burn,
    ButterflyWaveScrawler {
        #[serde(default = "df_1")]
        amplitude: f32,
        #[serde(default = "df_30")]
        waves: f32,
        #[serde(default = "df_0_3")]
        color_separation: f32,
    },
    CannabisLeaf,
    Circle,
    CircleCrop {
        #[serde(default = "df_black_rgba")]
        bgcolor: [f32; 4],
    },
    CircleOpen {
        #[serde(default = "df_0_3")]
        smoothness: f32,
        #[serde(default = "df_true")]
        opening: bool,
    },
    ColorPhase,
    CoordFromIn,
    CrazyParametricFun {
        #[serde(default = "df_4")]
        a: f32,
        #[serde(default = "df_1")]
        b: f32,
        #[serde(default = "df_120")]
        amplitude: f32,
        #[serde(default = "df_0_1")]
        smoothness: f32,
    },
    ColourDistance {
        #[serde(default = "df_5")]
        power: f32,
    },
    CrossHatch,
    CrossWarp,
    CrossZoom {
        #[serde(default = "df_0_4")]
        strength: f32,
    },
    Cube {
        #[serde(default = "df_0_7")]
        persp: f32,
        #[serde(default = "df_0_3")]
        unzoom: f32,
        #[serde(default = "df_0_4")]
        reflection: f32,
        #[serde(default = "df_3")]
        floating: f32,
    },
    Directional {
        #[serde(default = "df_y_up")]
        direction: [f32; 2],
    },
    DirectionalEasing {
        #[serde(default = "df_y_up")]
        direction: [f32; 2],
    },
    DirectionalScaled {
        #[serde(default = "df_y_up")]
        direction: [f32; 2],
        #[serde(default = "df_0_7")]
        scale: f32,
    },
    DirectionalWarp {
        #[serde(default = "df_y_up")]
        direction: [f32; 2],
    },
    #[serde(alias = "wipe")]
    DirectionalWipe {
        #[serde(default = "default_wipe_direction")]
        direction: [f32; 2],
        #[serde(default = "default_wipe_smoothness")]
        smoothness: f32,
    },
    Displacement,
    Dissolve {
        #[serde(default = "df_0_1")]
        line_width: f32,
        #[serde(default = "df_red")]
        spread_clr: [f32; 3],
        #[serde(default = "df_yellow")]
        hot_clr: [f32; 3],
        #[serde(default = "df_5")]
        pow: f32,
        #[serde(default = "df_1")]
        intensity: f32,
    },
    Doom {
        #[serde(default = "df_30_i")]
        bars: i32,
        #[serde(default = "df_2")]
        amplitude: f32,
        #[serde(default = "df_0_1")]
        noise: f32,
        #[serde(default = "df_0_5")]
        frequency: f32,
        #[serde(default = "df_0_5")]
        drip_scale: f32,
    },
    Doorway {
        #[serde(default = "df_0_4")]
        reflection: f32,
        #[serde(default = "df_0_4")]
        perspective: f32,
        #[serde(default = "df_3")]
        depth: f32,
    },
    Dreamy,
    DreamyZoom {
        #[serde(default = "df_6")]
        rotation: f32,
        #[serde(default = "df_1_2")]
        scale: f32,
    },
    Edge {
        #[serde(default = "df_tiny")]
        thickness: f32,
        #[serde(default = "df_8")]
        brightness: f32,
    },
    #[default]
    Fade,
    FadeColor {
        #[serde(default = "df_black_rgb")]
        color: [f32; 3],
        #[serde(default = "df_0_4")]
        color_phase: f32,
    },
    FadeGrayscale {
        #[serde(default = "df_0_3")]
        intensity: f32,
    },
    FilmBurn {
        #[serde(default = "df_2_31")]
        seed: f32,
    },
    FlyEye {
        #[serde(default = "df_0_04")]
        size: f32,
        #[serde(default = "df_0_5")]
        zoom: f32,
        #[serde(default = "df_0_3")]
        color_separation: f32,
    },
    GlitchDisplace,
    GlitchMemories,
    GridFlip {
        #[serde(default = "df_4_4")]
        size: [i32; 2],
        #[serde(default = "df_0_1")]
        pause: f32,
        #[serde(default = "df_0_05")]
        divider_width: f32,
        #[serde(default = "df_black_rgba")]
        bgcolor: [f32; 4],
        #[serde(default = "df_0_1")]
        randomness: f32,
    },
    Heart,
    Hexagonalize {
        #[serde(default = "df_50_i")]
        steps: i32,
        #[serde(default = "df_20")]
        horizontal_hexagons: f32,
    },
    HorizontalClose,
    HorizontalOpen,
    InvertedPageCurl,
    Kaleidoscope {
        #[serde(default = "df_0_5")]
        speed: f32,
        #[serde(default = "df_1")]
        angle: f32,
        #[serde(default = "df_0_3")]
        power: f32,
    },
    LeftRight,
    LinearBlur {
        #[serde(default = "df_0_1")]
        intensity: f32,
    },
    Luma,
    LuminanceMelt {
        #[serde(default = "df_true")]
        direction: bool,
        #[serde(rename = "luma_threshold", default = "df_0_05")]
        luma_threshold: f32,
    },
    Morph {
        #[serde(default = "df_0_1")]
        strength: f32,
    },
    Mosaic {
        #[serde(default = "df_2_i")]
        endx: i32,
        #[serde(default = "df_neg_1_i")]
        endy: i32,
    },
    MosaicTransition {
        #[serde(default = "df_10")]
        mosaic_num: f32,
    },
    MultiplyBlend,
    Overexposure,
    Perlin {
        #[serde(default = "df_4")]
        scale: f32,
        #[serde(default = "df_0_01")]
        smoothness: f32,
        #[serde(default = "df_12")]
        seed: f32,
    },
    Pinwheel {
        #[serde(default = "df_2")]
        speed: f32,
    },
    Pixelize {
        #[serde(default = "df_20_20")]
        squares_min: [i32; 2],
        #[serde(default = "df_50_i")]
        steps: i32,
    },
    PolarFunction {
        #[serde(default = "df_5_i")]
        segments: i32,
    },
    PolkaDotsCurtain {
        #[serde(default = "df_20")]
        dots: f32,
        #[serde(default = "df_origin")]
        center: [f32; 2],
    },
    PowerKaleido {
        #[serde(default = "df_2")]
        scale: f32,
        #[serde(default = "df_1_5")]
        radius: f32,
        #[serde(default = "df_0")]
        angle: f32,
    },
    Radial {
        #[serde(default = "default_radial_smoothness")]
        smoothness: f32,
    },
    RandomNoiseX,
    RandomSquares {
        #[serde(default = "df_10_10")]
        size: [i32; 2],
        #[serde(default = "df_0_5")]
        smoothness: f32,
    },
    Rectangle {
        #[serde(default = "df_black_rgba")]
        bgcolor: [f32; 4],
    },
    RectangleCrop {
        #[serde(default = "df_black_rgba")]
        bgcolor: [f32; 4],
    },
    Ripple {
        #[serde(default = "df_100")]
        amplitude: f32,
        #[serde(default = "df_50_f")]
        speed: f32,
    },
    Rolls {
        #[serde(rename = "rolls_type", default = "df_0_i")]
        rolls_type: i32,
        #[serde(rename = "rot_down", default = "df_false")]
        rot_down: bool,
    },
    Rotate,
    RotateScaleFade {
        #[serde(default = "df_center")]
        center: [f32; 2],
        #[serde(default = "df_1")]
        rotations: f32,
        #[serde(default = "df_8_f")]
        scale: f32,
        #[serde(rename = "back_color", default = "df_dark_grey")]
        back_color: [f32; 4],
    },
    RotateScaleVanish {
        #[serde(rename = "fade_in_second", default = "df_true")]
        fade_in_second: bool,
        #[serde(rename = "reverse_effect", default = "df_false")]
        reverse_effect: bool,
        #[serde(rename = "reverse_rotation", default = "df_false")]
        reverse_rotation: bool,
    },
    ScaleIn,
    SimpleZoom {
        #[serde(default = "df_0_8")]
        zoom_quickness: f32,
    },
    SimpleZoomOut {
        #[serde(default = "df_0_8")]
        zoom_quickness: f32,
        #[serde(default = "df_true")]
        fade_edge: bool,
    },
    Slides {
        #[serde(rename = "slides_type", default = "df_0_i")]
        slides_type: i32,
        #[serde(rename = "slides_in", default = "df_false")]
        slides_in: bool,
    },
    SquaresWire {
        #[serde(default = "df_10_10")]
        squares: [i32; 2],
        #[serde(default = "df_x_up")]
        direction: [f32; 2],
        #[serde(default = "df_1_6")]
        smoothness: f32,
    },
    Squeeze {
        #[serde(default = "df_0_1")]
        color_separation: f32,
    },
    StaticFade {
        #[serde(default = "df_200")]
        n_noise_pixels: f32,
        #[serde(default = "df_0_8")]
        static_luminosity: f32,
    },
    StaticWipe {
        #[serde(default = "df_true")]
        up_to_down: bool,
        #[serde(default = "df_0_5")]
        max_static_span: f32,
    },
    StereoViewer {
        #[serde(default = "df_0_8")]
        zoom: f32,
        #[serde(default = "df_0_22")]
        corner_radius: f32,
    },
    Swap {
        #[serde(default = "df_0_4")]
        reflection: f32,
        #[serde(default = "df_0_2")]
        perspective: f32,
        #[serde(default = "df_3")]
        depth: f32,
    },
    Swirl,
    TangentMotionBlur,
    TopBottom,
    TvStatic {
        #[serde(default = "df_0_05")]
        offset: f32,
    },
    UndulatingBurnOut {
        #[serde(default = "df_0_03")]
        smoothness: f32,
        #[serde(default = "df_center")]
        center: [f32; 2],
        #[serde(default = "df_black_rgb")]
        color: [f32; 3],
    },
    VerticalClose,
    VerticalOpen,
    WaterDrop {
        #[serde(default = "df_30")]
        amplitude: f32,
        #[serde(default = "df_30")]
        speed: f32,
    },
    Wind {
        #[serde(default = "df_0_05")]
        size: f32,
    },
    WindowBlinds,
    WindowSlice {
        #[serde(default = "df_10")]
        count: f32,
        #[serde(default = "df_0_5")]
        smoothness: f32,
    },
    WipeDown,
    WipeLeft,
    WipeRight,
    WipeUp,
    XAxisTranslation,
    ZoomInCircles,
    ZoomLeftWipe {
        #[serde(default = "df_0_8")]
        zoom_quickness: f32,
    },
    ZoomRightWipe {
        #[serde(default = "df_0_8")]
        zoom_quickness: f32,
    },
    Random,
    Custom {
        shader: String,
        #[serde(default)]
        params: HashMap<String, f32>,
    },
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
