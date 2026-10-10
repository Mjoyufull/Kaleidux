use kaleidux_common::Transition;

pub(super) fn builtin_shader_mapping(transition: &Transition) -> &'static str {
    match transition {
        Transition::Angular { .. } => "float startingAngle = getFromParams(0);",
        Transition::Bounce { .. } => {
            "vec4 shadow_colour = vec4(getFromParams(0), getFromParams(1), getFromParams(2), getFromParams(3)); float shadow_height = getFromParams(4); float bounces = getFromParams(5);"
        }
        Transition::BowTieWithParameter { .. } => {
            "float adjust = getFromParams(0); bool reverse = getFromParams(1) > 0.5;"
        }
        Transition::Burn => "vec3 color = vec3(0.9, 0.4, 0.2);",
        Transition::ButterflyWaveScrawler { .. } => {
            "float amplitude = getFromParams(0); float waves = getFromParams(1); float colorSeparation = getFromParams(2);"
        }
        Transition::Circle => "vec2 center = vec2(0.5, 0.5); vec3 backColor = vec3(0.1, 0.1, 0.1);",
        Transition::CircleCrop { .. } => {
            "vec4 bgcolor = vec4(getFromParams(0), getFromParams(1), getFromParams(2), getFromParams(3));"
        }
        Transition::CircleOpen { .. } => {
            "float smoothness = getFromParams(0); bool opening = getFromParams(1) > 0.5;"
        }
        Transition::ColorPhase => {
            "vec4 fromStep = vec4(0.0, 0.2, 0.4, 0.0); vec4 toStep = vec4(0.6, 0.8, 1.0, 1.0);"
        }
        Transition::CoordFromIn => "",
        Transition::CrazyParametricFun { .. } => {
            "float a = getFromParams(0); float b = getFromParams(1); float amplitude = getFromParams(2); float smoothness = getFromParams(3);"
        }
        Transition::ColourDistance { .. } => "float power = getFromParams(0);",
        Transition::CrossHatch => {
            "vec2 center = vec2(0.5); float threshold = 3.0; float fadeEdge = 0.1;"
        }
        Transition::CrossZoom { .. } => "float strength = getFromParams(0);",
        Transition::CrossWarp => "",
        Transition::Cube { .. } => "",
        Transition::Directional { .. } => {
            "vec2 direction = vec2(getFromParams(0), getFromParams(1));"
        }
        Transition::DirectionalEasing { .. } => {
            "vec2 direction = vec2(getFromParams(0), getFromParams(1));"
        }
        Transition::DirectionalScaled { .. } => {
            "vec2 direction = vec2(getFromParams(0), getFromParams(1)); float scale = getFromParams(2);"
        }
        Transition::DirectionalWarp { .. } => {
            "vec2 direction = vec2(getFromParams(0), getFromParams(1)); float smoothness = getFromParams(2);"
        }
        Transition::DirectionalWipe { .. } => {
            "vec2 direction = vec2(getFromParams(0), getFromParams(1)); float smoothness = getFromParams(2);"
        }
        Transition::Displacement => "",
        Transition::Dissolve { .. } => {
            "float uLineWidth = getFromParams(0); vec3 uSpreadClr = vec3(getFromParams(1), getFromParams(2), getFromParams(3)); vec3 uHotClr = vec3(getFromParams(4), getFromParams(5), getFromParams(6)); float uPow = getFromParams(7); float uIntensity = getFromParams(8);"
        }
        Transition::Doom { .. } => {
            "int bars = int(getFromParams(0)); float amplitude = getFromParams(1); float noise = getFromParams(2); float frequency = getFromParams(3); float dripScale = getFromParams(4);"
        }
        Transition::Doorway { .. } => {
            "float reflection = getFromParams(0); float perspective = getFromParams(1); float depth = getFromParams(2);"
        }
        Transition::DreamyZoom { .. } => {
            "float rotation = getFromParams(0); float scale = getFromParams(1);"
        }
        Transition::Edge { .. } => {
            "float edge_thickness = getFromParams(0); float edge_brightness = getFromParams(1);"
        }
        Transition::FadeColor { .. } => {
            "vec3 color = vec3(getFromParams(0), getFromParams(1), getFromParams(2)); float colorPhase = getFromParams(3);"
        }
        Transition::FadeGrayscale { .. } => "float intensity = getFromParams(0);",
        Transition::FilmBurn { .. } => "float Seed = getFromParams(0);",
        Transition::FlyEye { .. } => {
            "float size = getFromParams(0); float zoom = getFromParams(1); float colorSeparation = getFromParams(2);"
        }
        Transition::GridFlip { .. } => {
            "ivec2 size = ivec2(int(getFromParams(0)), int(getFromParams(1))); float pause = getFromParams(2); float dividerWidth = getFromParams(3); vec4 bgcolor = vec4(getFromParams(4), getFromParams(5), getFromParams(6), getFromParams(7)); float randomness = getFromParams(8);"
        }
        Transition::Hexagonalize { .. } => {
            "int steps = int(getFromParams(0)); float horizontalHexagons = getFromParams(1);"
        }
        Transition::Kaleidoscope { .. } => {
            "float speed = getFromParams(0); float angle = getFromParams(1); float power = getFromParams(2);"
        }
        Transition::LinearBlur { .. } => "float intensity = getFromParams(0);",
        Transition::LuminanceMelt { .. } => {
            "bool direction = getFromParams(0) > 0.5; float l_threshold = getFromParams(1); bool above = false;"
        }
        Transition::Luma => "",
        Transition::Morph { .. } => "float strength = getFromParams(0);",
        Transition::Mosaic { .. } => {
            "int endx = int(getFromParams(0)); int endy = int(getFromParams(1));"
        }
        Transition::MosaicTransition { .. } => "float mosaicNum = getFromParams(0);",
        Transition::Perlin { .. } => {
            "float scale = getFromParams(0); float smoothness = getFromParams(1); float seed = getFromParams(2);"
        }
        Transition::Pinwheel { .. } => "float speed = getFromParams(0);",
        Transition::Pixelize { .. } => {
            "ivec2 squaresMin = ivec2(int(getFromParams(0)), int(getFromParams(1))); int steps = int(getFromParams(2));"
        }
        Transition::PolarFunction { .. } => "int segments = int(getFromParams(0));",
        Transition::PolkaDotsCurtain { .. } => {
            "float dots = getFromParams(0); vec2 center = vec2(getFromParams(1), getFromParams(2));"
        }
        Transition::PowerKaleido { .. } => {
            "float scale = getFromParams(0); float z = getFromParams(1); float speed = getFromParams(2);"
        }
        Transition::Radial { .. } => "float smoothness = getFromParams(0);",
        Transition::RandomSquares { .. } => {
            "ivec2 size = ivec2(int(getFromParams(0)), int(getFromParams(1))); float smoothness = getFromParams(2);"
        }
        Transition::Rectangle { .. } => {
            "vec4 bgcolor = vec4(getFromParams(0), getFromParams(1), getFromParams(2), getFromParams(3));"
        }
        Transition::RectangleCrop { .. } => {
            "vec4 bgcolor = vec4(getFromParams(0), getFromParams(1), getFromParams(2), getFromParams(3));"
        }
        Transition::Ripple { .. } => {
            "float amplitude = getFromParams(0); float speed = getFromParams(1);"
        }
        Transition::Rolls { .. } => {
            "int type = int(getFromParams(0)); bool RotDown = getFromParams(1) > 0.5;"
        } // Rolls.glsl: `uniform int type; uniform bool RotDown;`
        Transition::Rotate => "",
        Transition::RotateScaleFade { .. } => {
            "vec2 center = vec2(getFromParams(0), getFromParams(1)); float rotations = getFromParams(2); float scale = getFromParams(3); vec4 backColor = vec4(getFromParams(4), getFromParams(5), getFromParams(6), getFromParams(7));"
        } // rotate_scale_fade.glsl: `uniform vec4 backColor;` (Grep showed backColor or back_color? Grep output snippet was truncated/not showed full. `backColor` is common. Let's guess backColor. Correction: grep said `src/shaders/transitions/rotate_scale_fade.glsl:uniform vec4 backColor`)
        Transition::RotateScaleVanish { .. } => {
            "bool FadeInSecond = getFromParams(0) > 0.5; bool ReverseEffect = getFromParams(1) > 0.5; bool ReverseRotation = getFromParams(2) > 0.5;"
        } // PascalCase in shader.
        Transition::ScaleIn => "",
        Transition::SimpleZoom { .. } => "float zoom_quickness = getFromParams(0);",
        Transition::SimpleZoomOut { .. } => {
            "float zoom_quickness = getFromParams(0); bool fade = getFromParams(1) > 0.5;"
        }
        Transition::Slides { .. } => {
            "int type = int(getFromParams(0)); bool In = getFromParams(1) > 0.5;"
        } // Slides.glsl: `uniform int type; uniform bool In;`
        Transition::Squeeze { .. } => "float colorSeparation = getFromParams(0);", // Grep didn't show. Guessing camelCase.
        Transition::StaticFade { .. } => {
            "float n_noise_pixels = getFromParams(0); float static_luminosity = getFromParams(1);"
        }
        Transition::StaticWipe { .. } => {
            "bool u_transitionUpToDown = getFromParams(0) > 0.5; float u_max_static_span = getFromParams(1);"
        }
        Transition::StereoViewer { .. } => {
            "float zoom = getFromParams(0); float corner_radius = getFromParams(1);"
        }
        Transition::Swap { .. } => {
            "float reflection = getFromParams(0); float perspective = getFromParams(1); float depth = getFromParams(2);"
        }
        Transition::TvStatic { .. } => "float offset = getFromParams(0);",
        Transition::UndulatingBurnOut { .. } => {
            "float smoothness = getFromParams(0); vec2 center = vec2(getFromParams(1), getFromParams(2)); vec3 color = vec3(getFromParams(3), getFromParams(4), getFromParams(5));"
        }
        Transition::WaterDrop { .. } => {
            "float amplitude = getFromParams(0); float speed = getFromParams(1);"
        }
        Transition::Wind { .. } => "float size = getFromParams(0);",
        Transition::WindowSlice { .. } => {
            "float count = getFromParams(0); float smoothness = getFromParams(1);"
        }
        Transition::ZoomLeftWipe { .. } | Transition::ZoomRightWipe { .. } => {
            "float zoom_quickness = getFromParams(0);"
        }
        Transition::Overexposure => "float strength = 0.6;",
        Transition::SquaresWire { .. } => {
            "ivec2 squares = ivec2(int(getFromParams(0)), int(getFromParams(1))); vec2 direction = vec2(getFromParams(2), getFromParams(3)); float smoothness = getFromParams(4);"
        }
        _ => "",
    }
}
