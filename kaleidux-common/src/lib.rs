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

impl Transition {
    pub fn pick_random() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let variants = [
            "angular",
            "bookflip",
            "bounce",
            "bowtiehorizontal",
            "bowtievertical",
            "bowtiewithparameter",
            "burn",
            "butterflywavescrawler",
            "cannabisleaf",
            "circle",
            "circlecrop",
            "circleopen",
            "colorphase",
            "coord-from-in",
            "crazyparametricfun",
            "colourdistance",
            "crosshatch",
            "crosswarp",
            "crosszoom",
            "cube",
            "directional",
            "directionaleasing",
            "directionalscaled",
            "directionalwarp",
            "directionalwipe",
            "displacement",
            "dissolve",
            "doom",
            "doorway",
            "dreamy",
            "dreamyzoom",
            "edge",
            "fade",
            "fadecolor",
            "fadegrayscale",
            "filmburn",
            "flyeye",
            "glitchdisplace",
            "glitchmemories",
            "gridflip",
            "heart",
            "hexagonalize",
            "horizontalclose",
            "horizontalopen",
            "invertedpagecurl",
            "kaleidoscope",
            "leftright",
            "linearblur",
            "luma",
            "luminancemelt",
            "morph",
            "mosaic",
            "mosaic_transition",
            "multiplyblend",
            "overexposure",
            "perlin",
            "pinwheel",
            "pixelize",
            "polarfunction",
            "polkadotscurtain",
            "powerkaleido",
            "radial",
            "randomnoisex",
            "randomsquares",
            "rectangle",
            "rectanglecrop",
            "ripple",
            "rolls",
            "rotate",
            "rotatescalefade",
            "rotatescalevanish",
            "scale_in",
            "simplezoom",
            "simplezoomout",
            "slides",
            "squareswire",
            "squeeze",
            "staticfade",
            "static_wipe",
            "stereoviewer",
            "swap",
            "swirl",
            "tangentmotionblur",
            "topbottom",
            "tvstatic",
            "undulatingburnout",
            "verticalclose",
            "verticalopen",
            "waterdrop",
            "wind",
            "windowblinds",
            "windowslice",
            "wipedown",
            "wipeleft",
            "wiperight",
            "wipeup",
            "x-axis-translation",
            "zoomincircles",
            "zoomleftwipe",
            "zoomrightwipe",
        ];
        let name = variants[rng.gen_range(0..variants.len())];
        Self::from_name(name)
    }

    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "angular" => Transition::Angular {
                starting_angle: 0.0,
            },
            "bookflip" => Transition::BookFlip,
            "bounce" => Transition::Bounce {
                shadow_colour: [0.0, 0.0, 0.0, 0.6],
                shadow_height: 0.075,
                bounces: 3.0,
            },
            "bowtiehorizontal" => Transition::BowTieHorizontal,
            "bowtievertical" => Transition::BowTieVertical,
            "bowtiewithparameter" => Transition::BowTieWithParameter {
                adjust: 0.5,
                reverse: false,
            },
            "burn" => Transition::Burn,
            "butterflywavescrawler" => Transition::ButterflyWaveScrawler {
                amplitude: 1.0,
                waves: 30.0,
                color_separation: 0.3,
            },
            "circle" => Transition::Circle,
            "circlecrop" => Transition::CircleCrop {
                bgcolor: [0.0, 0.0, 0.0, 1.0],
            },
            "circleopen" => Transition::CircleOpen {
                smoothness: 0.3,
                opening: true,
            },
            "colorphase" => Transition::ColorPhase,
            "coord-from-in" => Transition::CoordFromIn,
            "crazyparametricfun" => Transition::CrazyParametricFun {
                a: 4.0,
                b: 1.0,
                amplitude: 120.0,
                smoothness: 0.1,
            },
            "colourdistance" => Transition::ColourDistance { power: 5.0 },
            "crosshatch" => Transition::CrossHatch,
            "crosswarp" => Transition::CrossWarp,
            "crosszoom" => Transition::CrossZoom { strength: 0.4 },
            "cube" => Transition::Cube {
                persp: 0.7,
                unzoom: 0.3,
                reflection: 0.4,
                floating: 3.0,
            },
            "directional" => Transition::Directional {
                direction: [0.0, 1.0],
            },
            "directionaleasing" => Transition::DirectionalEasing {
                direction: [0.0, 1.0],
            },
            "directionalscaled" => Transition::DirectionalScaled {
                direction: [0.0, 1.0],
                scale: 0.7,
            },
            "directionalwarp" => Transition::DirectionalWarp {
                direction: [1.0, 0.0],
            },
            "directionalwipe" | "wipe" => Transition::DirectionalWipe {
                direction: [1.0, -1.0],
                smoothness: 0.5,
            },
            "displacement" => Transition::Displacement,
            "dissolve" => Transition::Dissolve {
                line_width: 0.1,
                spread_clr: [1.0, 0.0, 0.0],
                hot_clr: [0.9, 0.9, 0.2],
                pow: 5.0,
                intensity: 1.0,
            },
            "doom" => Transition::Doom {
                bars: 30,
                amplitude: 2.0,
                noise: 0.1,
                frequency: 0.5,
                drip_scale: 0.5,
            },
            "doorway" => Transition::Doorway {
                reflection: 0.4,
                perspective: 0.4,
                depth: 3.0,
            },
            "dreamy" => Transition::Dreamy,
            "dreamyzoom" => Transition::DreamyZoom {
                rotation: 6.0,
                scale: 1.2,
            },
            "edge" => Transition::Edge {
                thickness: 0.001,
                brightness: 8.0,
            },
            "fade" => Transition::Fade,
            "fadecolor" => Transition::FadeColor {
                color: [0.0, 0.0, 0.0],
                color_phase: 0.4,
            },
            "fadegrayscale" => Transition::FadeGrayscale { intensity: 0.3 },
            "filmburn" => Transition::FilmBurn { seed: 2.31 },
            "flyeye" => Transition::FlyEye {
                size: 0.04,
                zoom: 0.5,
                color_separation: 0.3,
            },
            "glitchdisplace" => Transition::GlitchDisplace,
            "glitchmemories" => Transition::GlitchMemories,
            "gridflip" => Transition::GridFlip {
                size: [4, 4],
                pause: 0.1,
                divider_width: 0.05,
                bgcolor: [0.0, 0.0, 0.0, 1.0],
                randomness: 0.1,
            },
            "heart" => Transition::Heart,
            "hexagonalize" => Transition::Hexagonalize {
                steps: 50,
                horizontal_hexagons: 20.0,
            },
            "horizontalclose" => Transition::HorizontalClose,
            "horizontalopen" => Transition::HorizontalOpen,
            "invertedpagecurl" => Transition::InvertedPageCurl,
            "kaleidoscope" => Transition::Kaleidoscope {
                speed: 0.5,
                angle: 1.0,
                power: 0.3,
            },
            "leftright" => Transition::LeftRight,
            "linearblur" => Transition::LinearBlur { intensity: 0.1 },
            "luma" => Transition::Luma,
            "luminancemelt" => Transition::LuminanceMelt {
                direction: true,
                luma_threshold: 0.05,
            },
            "morph" => Transition::Morph { strength: 0.1 },
            "mosaic" => Transition::Mosaic { endx: 2, endy: -1 },
            "mosaic_transition" => Transition::MosaicTransition { mosaic_num: 10.0 },
            "multiplyblend" => Transition::MultiplyBlend,
            "overexposure" => Transition::Overexposure,
            "perlin" => Transition::Perlin {
                scale: 4.0,
                smoothness: 0.01,
                seed: 12.0,
            },
            "pinwheel" => Transition::Pinwheel { speed: 2.0 },
            "pixelize" => Transition::Pixelize {
                squares_min: [20, 20],
                steps: 50,
            },
            "polarfunction" => Transition::PolarFunction { segments: 5 },
            "polkadotscurtain" => Transition::PolkaDotsCurtain {
                dots: 20.0,
                center: [0.0, 0.0],
            },
            "powerkaleido" => Transition::PowerKaleido {
                scale: 2.0,
                radius: 1.5,
                angle: 0.0,
            },
            "radial" => Transition::Radial { smoothness: 1.0 },
            "randomnoisex" => Transition::RandomNoiseX,
            "randomsquares" => Transition::RandomSquares {
                size: [10, 10],
                smoothness: 0.5,
            },
            "rectangle" => Transition::Rectangle {
                bgcolor: [0.0, 0.0, 0.0, 1.0],
            },
            "rectanglecrop" => Transition::RectangleCrop {
                bgcolor: [0.0, 0.0, 0.0, 1.0],
            },
            "ripple" => Transition::Ripple {
                amplitude: 100.0,
                speed: 50.0,
            },
            "rolls" => Transition::Rolls {
                rolls_type: 0,
                rot_down: false,
            },
            "rotate" => Transition::Rotate,
            "rotatescalefade" => Transition::RotateScaleFade {
                center: [0.5, 0.5],
                rotations: 1.0,
                scale: 8.0,
                back_color: [0.15, 0.15, 0.15, 1.0],
            },
            "rotatescalevanish" => Transition::RotateScaleVanish {
                fade_in_second: true,
                reverse_effect: false,
                reverse_rotation: false,
            },
            "scale_in" => Transition::ScaleIn,
            "simplezoom" => Transition::SimpleZoom {
                zoom_quickness: 0.8,
            },
            "simplezoomout" => Transition::SimpleZoomOut {
                zoom_quickness: 0.8,
                fade_edge: true,
            },
            "slides" => Transition::Slides {
                slides_type: 0,
                slides_in: false,
            },
            "squareswire" => Transition::SquaresWire {
                squares: [10, 10],
                direction: [1.0, -0.5],
                smoothness: 1.6,
            },
            "squeeze" => Transition::Squeeze {
                color_separation: 0.1,
            },
            "staticfade" => Transition::StaticFade {
                n_noise_pixels: 200.0,
                static_luminosity: 0.8,
            },
            "static_wipe" => Transition::StaticWipe {
                up_to_down: true,
                max_static_span: 0.5,
            },
            "stereoviewer" => Transition::StereoViewer {
                zoom: 0.88,
                corner_radius: 0.22,
            },
            "swap" => Transition::Swap {
                reflection: 0.4,
                perspective: 0.2,
                depth: 3.0,
            },
            "swirl" => Transition::Swirl,
            "tangentmotionblur" => Transition::TangentMotionBlur,
            "topbottom" => Transition::TopBottom,
            "tvstatic" => Transition::TvStatic { offset: 0.05 },
            "undulatingburnout" => Transition::UndulatingBurnOut {
                smoothness: 0.03,
                center: [0.5, 0.5],
                color: [0.0, 0.0, 0.0],
            },
            "verticalclose" => Transition::VerticalClose,
            "verticalopen" => Transition::VerticalOpen,
            "waterdrop" => Transition::WaterDrop {
                amplitude: 30.0,
                speed: 30.0,
            },
            "wind" => Transition::Wind { size: 0.05 },
            "windowblinds" => Transition::WindowBlinds,
            "windowslice" => Transition::WindowSlice {
                count: 10.0,
                smoothness: 0.5,
            },
            "wipedown" => Transition::WipeDown,
            "wipeleft" => Transition::WipeLeft,
            "wiperight" => Transition::WipeRight,
            "wipeup" => Transition::WipeUp,
            "x-axis-translation" => Transition::XAxisTranslation,
            "zoomincircles" => Transition::ZoomInCircles,
            "zoomleftwipe" => Transition::ZoomLeftWipe {
                zoom_quickness: 0.8,
            },
            "zoomrightwipe" => Transition::ZoomRightWipe {
                zoom_quickness: 0.8,
            },
            "random" => Transition::Random,
            _ => Transition::Fade,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Transition::Angular { .. } => "angular".to_string(),
            Transition::BookFlip => "BookFlip".to_string(),
            Transition::Bounce { .. } => "Bounce".to_string(),
            Transition::BowTieHorizontal => "BowTieHorizontal".to_string(),
            Transition::BowTieVertical => "BowTieVertical".to_string(),
            Transition::BowTieWithParameter { .. } => "BowTieWithParameter".to_string(),
            Transition::Burn => "burn".to_string(),
            Transition::ButterflyWaveScrawler { .. } => "ButterflyWaveScrawler".to_string(),
            Transition::CannabisLeaf => "cannabisleaf".to_string(),
            Transition::Circle => "circle".to_string(),
            Transition::CircleCrop { .. } => "CircleCrop".to_string(),
            Transition::CircleOpen { .. } => "circleopen".to_string(),
            Transition::ColorPhase => "colorphase".to_string(),
            Transition::CoordFromIn => "coord-from-in".to_string(),
            Transition::CrazyParametricFun { .. } => "CrazyParametricFun".to_string(),
            Transition::ColourDistance { .. } => "ColourDistance".to_string(),
            Transition::CrossHatch => "crosshatch".to_string(),
            Transition::CrossWarp => "crosswarp".to_string(),
            Transition::CrossZoom { .. } => "CrossZoom".to_string(),
            Transition::Cube { .. } => "cube".to_string(),
            Transition::Directional { .. } => "Directional".to_string(),
            Transition::DirectionalEasing { .. } => "directional-easing".to_string(),
            Transition::DirectionalScaled { .. } => "DirectionalScaled".to_string(),
            Transition::DirectionalWarp { .. } => "directionalwarp".to_string(),
            Transition::DirectionalWipe { .. } => "directionalwipe".to_string(),
            Transition::Displacement => "displacement".to_string(),
            Transition::Dissolve { .. } => "dissolve".to_string(),
            Transition::Doom { .. } => "DoomScreenTransition".to_string(),
            Transition::Doorway { .. } => "doorway".to_string(),
            Transition::Dreamy => "Dreamy".to_string(),
            Transition::DreamyZoom { .. } => "DreamyZoom".to_string(),
            Transition::Edge { .. } => "EdgeTransition".to_string(),
            Transition::Fade => "fade".to_string(),
            Transition::FadeColor { .. } => "fadecolor".to_string(),
            Transition::FadeGrayscale { .. } => "fadegrayscale".to_string(),
            Transition::FilmBurn { .. } => "FilmBurn".to_string(),
            Transition::FlyEye { .. } => "flyeye".to_string(),
            Transition::GlitchDisplace => "GlitchDisplace".to_string(),
            Transition::GlitchMemories => "GlitchMemories".to_string(),
            Transition::GridFlip { .. } => "GridFlip".to_string(),
            Transition::Heart => "heart".to_string(),
            Transition::Hexagonalize { .. } => "hexagonalize".to_string(),
            Transition::HorizontalClose => "HorizontalClose".to_string(),
            Transition::HorizontalOpen => "HorizontalOpen".to_string(),
            Transition::InvertedPageCurl => "InvertedPageCurl".to_string(),
            Transition::Kaleidoscope { .. } => "kaleidoscope".to_string(),
            Transition::LeftRight => "LeftRight".to_string(),
            Transition::LinearBlur { .. } => "LinearBlur".to_string(),
            Transition::Luma => "luma".to_string(),
            Transition::LuminanceMelt { .. } => "luminance_melt".to_string(),
            Transition::Morph { .. } => "morph".to_string(),
            Transition::Mosaic { .. } => "Mosaic".to_string(),
            Transition::MosaicTransition { .. } => "mosaic_transition".to_string(),
            Transition::MultiplyBlend => "multiply_blend".to_string(),
            Transition::Overexposure => "Overexposure".to_string(),
            Transition::Perlin { .. } => "perlin".to_string(),
            Transition::Pinwheel { .. } => "pinwheel".to_string(),
            Transition::Pixelize { .. } => "pixelize".to_string(),
            Transition::PolarFunction { .. } => "polar_function".to_string(),
            Transition::PolkaDotsCurtain { .. } => "PolkaDotsCurtain".to_string(),
            Transition::PowerKaleido { .. } => "powerKaleido".to_string(),
            Transition::Radial { .. } => "Radial".to_string(),
            Transition::RandomNoiseX => "randomNoisex".to_string(),
            Transition::RandomSquares { .. } => "randomsquares".to_string(),
            Transition::Rectangle { .. } => "Rectangle".to_string(),
            Transition::RectangleCrop { .. } => "RectangleCrop".to_string(),
            Transition::Ripple { .. } => "ripple".to_string(),
            Transition::Rolls { .. } => "Rolls".to_string(),
            Transition::Rotate => "rotateTransition".to_string(),
            Transition::RotateScaleFade { .. } => "rotate_scale_fade".to_string(),
            Transition::RotateScaleVanish { .. } => "RotateScaleVanish".to_string(),
            Transition::ScaleIn => "scale-in".to_string(),
            Transition::SimpleZoom { .. } => "SimpleZoom".to_string(),
            Transition::SimpleZoomOut { .. } => "SimpleZoomOut".to_string(),
            Transition::Slides { .. } => "Slides".to_string(),
            Transition::SquaresWire { .. } => "squareswire".to_string(),
            Transition::Squeeze { .. } => "squeeze".to_string(),
            Transition::StaticFade { .. } => "StaticFade".to_string(),
            Transition::StaticWipe { .. } => "static_wipe".to_string(),
            Transition::StereoViewer { .. } => "StereoViewer".to_string(),
            Transition::Swap { .. } => "swap".to_string(),
            Transition::Swirl => "Swirl".to_string(),
            Transition::TangentMotionBlur => "tangentMotionBlur".to_string(),
            Transition::TopBottom => "TopBottom".to_string(),
            Transition::TvStatic { .. } => "TVStatic".to_string(),
            Transition::UndulatingBurnOut { .. } => "undulatingBurnOut".to_string(),
            Transition::VerticalClose => "VerticalClose".to_string(),
            Transition::VerticalOpen => "VerticalOpen".to_string(),
            Transition::WaterDrop { .. } => "WaterDrop".to_string(),
            Transition::Wind { .. } => "wind".to_string(),
            Transition::WindowBlinds => "windowblinds".to_string(),
            Transition::WindowSlice { .. } => "windowslice".to_string(),
            Transition::WipeDown => "wipeDown".to_string(),
            Transition::WipeLeft => "wipeLeft".to_string(),
            Transition::WipeRight => "wipeRight".to_string(),
            Transition::WipeUp => "wipeUp".to_string(),
            Transition::XAxisTranslation => "x_axis_translation".to_string(),
            Transition::ZoomInCircles => "ZoomInCircles".to_string(),
            Transition::ZoomLeftWipe { .. } => "ZoomLeftWipe".to_string(),
            Transition::ZoomRightWipe { .. } => "ZoomRigthWipe".to_string(),
            Transition::Random => "random".to_string(),
            Transition::Custom { shader, .. } => shader.clone(),
        }
    }

    pub fn to_params(&self) -> [f32; 28] {
        let mut p = [0.0; 28];
        match self {
            Transition::BookFlip
            | Transition::Burn
            | Transition::CannabisLeaf
            | Transition::Circle
            | Transition::ColorPhase
            | Transition::CoordFromIn
            | Transition::CrossHatch
            | Transition::CrossWarp
            | Transition::Displacement
            | Transition::Dreamy
            | Transition::Fade
            | Transition::GlitchDisplace
            | Transition::GlitchMemories
            | Transition::Heart
            | Transition::HorizontalClose
            | Transition::HorizontalOpen
            | Transition::InvertedPageCurl
            | Transition::LeftRight
            | Transition::Luma
            | Transition::MultiplyBlend
            | Transition::Overexposure
            | Transition::RandomNoiseX
            | Transition::Rotate
            | Transition::ScaleIn
            | Transition::Swirl
            | Transition::TangentMotionBlur
            | Transition::TopBottom
            | Transition::VerticalClose
            | Transition::VerticalOpen
            | Transition::WipeDown
            | Transition::WipeLeft
            | Transition::WipeRight
            | Transition::WipeUp
            | Transition::WindowBlinds
            | Transition::XAxisTranslation
            | Transition::ZoomInCircles
            | Transition::Random => {}
            Transition::Angular { starting_angle } => {
                p[0] = *starting_angle;
            }
            Transition::BowTieWithParameter { adjust, reverse } => {
                p[0] = *adjust;
                p[1] = if *reverse { 1.0 } else { 0.0 };
            }
            Transition::BowTieHorizontal | Transition::BowTieVertical => {}
            Transition::Bounce {
                shadow_colour,
                shadow_height,
                bounces,
            } => {
                p[0..4].copy_from_slice(shadow_colour);
                p[4] = *shadow_height;
                p[5] = *bounces;
            }
            Transition::ButterflyWaveScrawler {
                amplitude,
                waves,
                color_separation,
            } => {
                p[0] = *amplitude;
                p[1] = *waves;
                p[2] = *color_separation;
            }
            Transition::CircleCrop { bgcolor } => {
                p[0..4].copy_from_slice(bgcolor);
            }
            Transition::CircleOpen {
                smoothness,
                opening,
            } => {
                p[0] = *smoothness;
                p[1] = if *opening { 1.0 } else { 0.0 };
            }
            Transition::CrazyParametricFun {
                a,
                b,
                amplitude,
                smoothness,
            } => {
                p[0] = *a;
                p[1] = *b;
                p[2] = *amplitude;
                p[3] = *smoothness;
            }
            Transition::ColourDistance { power } => {
                p[0] = *power;
            }
            Transition::CrossZoom { strength } => {
                p[0] = *strength;
            }
            Transition::Cube {
                persp,
                unzoom,
                reflection,
                floating,
            } => {
                p[0] = *persp;
                p[1] = *unzoom;
                p[2] = *reflection;
                p[3] = *floating;
            }
            Transition::Directional { direction } => {
                p[0..2].copy_from_slice(direction);
            }
            Transition::DirectionalEasing { direction } => {
                p[0..2].copy_from_slice(direction);
            }
            Transition::DirectionalScaled { direction, scale } => {
                p[0..2].copy_from_slice(direction);
                p[2] = *scale;
            }
            Transition::DirectionalWarp { direction } => {
                p[0..2].copy_from_slice(direction);
            }
            Transition::DirectionalWipe {
                direction,
                smoothness,
            } => {
                p[0..2].copy_from_slice(direction);
                p[2] = *smoothness;
            }
            Transition::Dissolve {
                line_width,
                spread_clr,
                hot_clr,
                pow,
                intensity,
            } => {
                p[0] = *line_width;
                p[1..4].copy_from_slice(spread_clr);
                p[4..7].copy_from_slice(hot_clr);
                p[7] = *pow;
                p[8] = *intensity;
            }
            Transition::Doom {
                bars,
                amplitude,
                noise,
                frequency,
                drip_scale,
            } => {
                p[0] = *bars as f32;
                p[1] = *amplitude;
                p[2] = *noise;
                p[3] = *frequency;
                p[4] = *drip_scale;
            }
            Transition::Doorway {
                reflection,
                perspective,
                depth,
            } => {
                p[0] = *reflection;
                p[1] = *perspective;
                p[2] = *depth;
            }
            Transition::DreamyZoom { rotation, scale } => {
                p[0] = *rotation;
                p[1] = *scale;
            }
            Transition::Edge {
                thickness,
                brightness,
            } => {
                p[0] = *thickness;
                p[1] = *brightness;
            }
            Transition::FadeColor { color, color_phase } => {
                p[0..3].copy_from_slice(color);
                p[3] = *color_phase;
            }
            Transition::FadeGrayscale { intensity } => {
                p[0] = *intensity;
            }
            Transition::FlyEye {
                size,
                zoom,
                color_separation,
            } => {
                p[0] = *size;
                p[1] = *zoom;
                p[2] = *color_separation;
            }
            Transition::GridFlip {
                size,
                pause,
                divider_width,
                bgcolor,
                randomness,
            } => {
                p[0] = size[0] as f32;
                p[1] = size[1] as f32;
                p[2] = *pause;
                p[3] = *divider_width;
                p[4..8].copy_from_slice(bgcolor);
                p[8] = *randomness;
            }
            Transition::Hexagonalize {
                steps,
                horizontal_hexagons,
            } => {
                p[0] = *steps as f32;
                p[1] = *horizontal_hexagons;
            }
            Transition::Kaleidoscope {
                speed,
                angle,
                power,
            } => {
                p[0] = *speed;
                p[1] = *angle;
                p[2] = *power;
            }
            Transition::LinearBlur { intensity } => {
                p[0] = *intensity;
            }
            Transition::LuminanceMelt {
                direction,
                luma_threshold,
            } => {
                p[0] = if *direction { 1.0 } else { 0.0 };
                p[1] = *luma_threshold;
            }
            Transition::Morph { strength } => {
                p[0] = *strength;
            }
            Transition::Mosaic { endx, endy } => {
                p[0] = *endx as f32;
                p[1] = *endy as f32;
            }
            Transition::MosaicTransition { mosaic_num } => {
                p[0] = *mosaic_num;
            }
            Transition::Perlin {
                scale,
                smoothness,
                seed,
            } => {
                p[0] = *scale;
                p[1] = *smoothness;
                p[2] = *seed;
            }
            Transition::Pinwheel { speed } => {
                p[0] = *speed;
            }
            Transition::Pixelize { squares_min, steps } => {
                p[0] = squares_min[0] as f32;
                p[1] = squares_min[1] as f32;
                p[2] = *steps as f32;
            }
            Transition::PolarFunction { segments } => {
                p[0] = *segments as f32;
            }
            Transition::PolkaDotsCurtain { dots, center } => {
                p[0] = *dots;
                p[1..3].copy_from_slice(center);
            }
            Transition::PowerKaleido {
                scale,
                radius,
                angle,
            } => {
                p[0] = *scale;
                p[1] = *radius;
                p[2] = *angle;
            }
            Transition::Radial { smoothness } => {
                p[0] = *smoothness;
            }
            Transition::RandomSquares { size, smoothness } => {
                p[0] = size[0] as f32;
                p[1] = size[1] as f32;
                p[2] = *smoothness;
            }
            Transition::Rectangle { bgcolor } => {
                p[0..4].copy_from_slice(bgcolor);
            }
            Transition::RectangleCrop { bgcolor } => {
                p[0..4].copy_from_slice(bgcolor);
            }
            Transition::Ripple { amplitude, speed } => {
                p[0] = *amplitude;
                p[1] = *speed;
            }
            Transition::Rolls {
                rolls_type,
                rot_down,
            } => {
                p[0] = *rolls_type as f32;
                p[1] = if *rot_down { 1.0 } else { 0.0 };
            }
            Transition::RotateScaleFade {
                center,
                rotations,
                scale,
                back_color,
            } => {
                p[0..2].copy_from_slice(center);
                p[2] = *rotations;
                p[3] = *scale;
                p[4..8].copy_from_slice(back_color);
            }
            Transition::RotateScaleVanish {
                fade_in_second,
                reverse_effect,
                reverse_rotation,
            } => {
                p[0] = if *fade_in_second { 1.0 } else { 0.0 };
                p[1] = if *reverse_effect { 1.0 } else { 0.0 };
                p[2] = if *reverse_rotation { 1.0 } else { 0.0 };
            }
            Transition::SimpleZoom { zoom_quickness } => {
                p[0] = *zoom_quickness;
            }
            Transition::SimpleZoomOut {
                zoom_quickness,
                fade_edge,
            } => {
                p[0] = *zoom_quickness;
                p[1] = if *fade_edge { 1.0 } else { 0.0 };
            }
            Transition::Slides {
                slides_type,
                slides_in,
            } => {
                p[0] = *slides_type as f32;
                p[1] = if *slides_in { 1.0 } else { 0.0 };
            }
            Transition::SquaresWire {
                squares,
                direction,
                smoothness,
            } => {
                p[0] = squares[0] as f32;
                p[1] = squares[1] as f32;
                p[2] = direction[0];
                p[3] = direction[1];
                p[4] = *smoothness;
            }
            Transition::Squeeze { color_separation } => {
                p[0] = *color_separation;
            }
            Transition::StaticFade {
                n_noise_pixels,
                static_luminosity,
            } => {
                p[0] = *n_noise_pixels;
                p[1] = *static_luminosity;
            }
            Transition::StaticWipe {
                up_to_down,
                max_static_span,
            } => {
                p[0] = if *up_to_down { 1.0 } else { 0.0 };
                p[1] = *max_static_span;
            }
            Transition::StereoViewer {
                zoom,
                corner_radius,
            } => {
                p[0] = *zoom;
                p[1] = *corner_radius;
            }
            Transition::Swap {
                reflection,
                perspective,
                depth,
            } => {
                p[0] = *reflection;
                p[1] = *perspective;
                p[2] = *depth;
            }
            Transition::TvStatic { offset } => {
                p[0] = *offset;
            }
            Transition::UndulatingBurnOut {
                smoothness,
                center,
                color,
            } => {
                p[0] = *smoothness;
                p[1..3].copy_from_slice(center);
                p[3..6].copy_from_slice(color);
            }
            Transition::WaterDrop { amplitude, speed } => {
                p[0] = *amplitude;
                p[1] = *speed;
            }
            Transition::Wind { size } => {
                p[0] = *size;
            }
            Transition::Custom { .. } => {
                // Named parameters handled via #define in daemon for Custom
            }
            Transition::WindowSlice { count, smoothness } => {
                p[0] = *count;
                p[1] = *smoothness;
            }
            Transition::ZoomLeftWipe { zoom_quickness }
            | Transition::ZoomRightWipe { zoom_quickness } => {
                p[0] = *zoom_quickness;
            }
            _ => {}
        }
        p
    }
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OutputInfo {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub current_wallpaper: Option<String>,
}

fn default_wipe_direction() -> [f32; 2] {
    [1.0, -1.0]
}

fn default_wipe_smoothness() -> f32 {
    0.5
}

fn default_radial_smoothness() -> f32 {
    1.0
}

fn df_0() -> f32 {
    0.0
}
fn df_0_075() -> f32 {
    0.075
}
fn df_3() -> f32 {
    3.0
}
fn df_1() -> f32 {
    1.0
}
fn df_30() -> f32 {
    30.0
}
fn df_0_3() -> f32 {
    0.3
}
fn df_black_rgba() -> [f32; 4] {
    [0.0, 0.0, 0.0, 1.0]
}
fn df_black_alpha() -> [f32; 4] {
    [0.0, 0.0, 0.0, 0.6]
}
fn df_true() -> bool {
    true
}
fn df_5() -> f32 {
    5.0
}
fn df_0_4() -> f32 {
    0.4
}
fn df_0_7() -> f32 {
    0.7
}
fn df_y_up() -> [f32; 2] {
    [0.0, 1.0]
}
fn df_0_1() -> f32 {
    0.1
}
fn df_red() -> [f32; 3] {
    [1.0, 0.0, 0.0]
}
fn df_yellow() -> [f32; 3] {
    [0.9, 0.9, 0.2]
}
fn df_2() -> f32 {
    2.0
}
fn df_6() -> f32 {
    6.0
}
fn df_1_2() -> f32 {
    1.2
}
fn df_tiny() -> f32 {
    0.001
}
fn df_8() -> f32 {
    8.0
}
fn df_black_rgb() -> [f32; 3] {
    [0.0, 0.0, 0.0]
}
fn df_2_31() -> f32 {
    2.31
}
fn df_0_04() -> f32 {
    0.04
}
fn df_0_5() -> f32 {
    0.5
}
fn df_4_4() -> [i32; 2] {
    [4, 4]
}
fn df_0_05() -> f32 {
    0.05
}
fn df_50_i() -> i32 {
    50
}
fn df_20() -> f32 {
    20.0
}
fn df_0_05_f() -> f32 {
    0.05
}
fn df_2_i() -> i32 {
    2
}
fn df_neg_1_i() -> i32 {
    -1
}
fn df_4() -> f32 {
    4.0
}
fn df_0_01() -> f32 {
    0.01
}
fn df_12() -> f32 {
    12.0
}
fn df_20_20() -> [i32; 2] {
    [20, 20]
}
fn df_5_i() -> i32 {
    5
}
fn df_center() -> [f32; 2] {
    [0.5, 0.5]
}
fn df_origin() -> [f32; 2] {
    [0.0, 0.0]
}
fn df_1_5() -> f32 {
    1.5
}
fn df_10_10() -> [i32; 2] {
    [10, 10]
}
fn df_100() -> f32 {
    100.0
}
fn df_50_f() -> f32 {
    50.0
}
fn df_0_i() -> i32 {
    0
}
fn df_false() -> bool {
    false
}
fn df_8_f() -> f32 {
    8.0
}
fn df_dark_grey() -> [f32; 4] {
    [0.15, 0.15, 0.15, 1.0]
}
fn df_0_8() -> f32 {
    0.8
}
fn df_200() -> f32 {
    200.0
}
fn df_0_2() -> f32 {
    0.2
}
fn df_0_22() -> f32 {
    0.22
}
fn df_30_i() -> i32 {
    30
}
fn df_10() -> f32 {
    10.0
}
fn df_x_up() -> [f32; 2] {
    [1.0, 0.0]
}
fn df_120() -> f32 {
    120.0
}
fn df_1_6() -> f32 {
    1.6
}
fn df_0_03() -> f32 {
    0.03
}
