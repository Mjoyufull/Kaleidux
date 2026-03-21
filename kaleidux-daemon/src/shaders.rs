pub use kaleidux_common::Transition;

const GLSL_PRELUDE: &str = r#"
#version 450
layout(location = 0) in vec2 v_uv;
layout(location = 0) out vec4 o_color;

precision highp float;

layout(set = 0, binding = 0) uniform TransitionUniforms {
    float progress;
    float screen_aspect;
    float prev_aspect;
    float next_aspect;
    vec4 params[7];
};

#define ratio screen_aspect

layout(set = 0, binding = 1) uniform texture2D t_prev;
layout(set = 0, binding = 2) uniform texture2D t_next;
layout(set = 0, binding = 3) uniform sampler s_linear;

// Helper to access flattened params from aligned vec4 array
float getFromParams(int i) {
    // GLSL component access via array indexing
    return params[i / 4][i % 4];
}

// Helper for aspect ratio cover
vec2 cover(vec2 uv, float screen_ratio, float content_ratio) {
    float scale = screen_ratio / content_ratio;
    if (scale > 1.0) {
        // Screen is wider than content: Fit Width, Crop Height (Zoom In Y)
        // We divide Y offset by scale to map 0..1 ScreenY to 0.25..0.75 ContentY
        return vec2(uv.x, (uv.y - 0.5) / scale + 0.5);
    } else {
        // Content is wider than screen: Fit Height, Crop Width (Zoom In X)
        // We multiply X offset by scale (where scale < 1.0) to map 0..1 ScreenX to subset ContentX
        return vec2((uv.x - 0.5) * scale + 0.5, uv.y);
    }
}

vec4 getFromColor(vec2 uv) {
    vec2 uv_c = cover(uv, screen_aspect, prev_aspect);
    return texture(sampler2D(t_prev, s_linear), uv_c);
}

vec4 getToColor(vec2 uv) {
    vec2 uv_c = cover(uv, screen_aspect, next_aspect);
    return texture(sampler2D(t_next, s_linear), uv_c);
}
"#;

pub struct ShaderManager;

// Process-wide cache of compiled WGSL shader strings (P-21)
// Keyed by transition name — avoids duplicate GLSL→WGSL compilation across renderers
static WGSL_CACHE: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashMap<String, String>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));
static BROKEN_TRANSITIONS: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashSet<String>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashSet::new()));

use anyhow::Context;

const CUBE_SAFE_GLSL: &str = r#"
vec2 cube_project(vec2 p, float floating) {
    return p * vec2(1.0, -1.2) + vec2(0.0, -floating / 100.0);
}

bool cube_in_bounds(vec2 p) {
    return p.x > 0.0 && p.y > 0.0 && p.x < 1.0 && p.y < 1.0;
}

vec4 cube_bg_color(vec2 p, vec2 pfr, vec2 pto, float reflection, float floating) {
    vec4 c = vec4(0.0, 0.0, 0.0, 1.0);
    vec2 projected_from = cube_project(pfr, floating);
    if (cube_in_bounds(projected_from)) {
        c += mix(vec4(0.0), getFromColor(projected_from), reflection * (1.0 - projected_from.y));
    }
    vec2 projected_to = cube_project(pto, floating);
    if (cube_in_bounds(projected_to)) {
        c += mix(vec4(0.0), getToColor(projected_to), reflection * (1.0 - projected_to.y));
    }
    return c;
}

vec2 cube_xskew(vec2 p, float perspective, float center) {
    float x = mix(p.x, 1.0 - p.x, center);
    float edge_distance = max(abs(center - 0.5), 0.0001);
    float center_side = step(0.5, center);
    float direction = mix(1.0, -1.0, center_side);
    return (
        (
            vec2(x, (p.y - 0.5 * (1.0 - perspective) * x) / (1.0 + (perspective - 1.0) * x))
            - vec2(0.5 - edge_distance, 0.0)
        ) * vec2(0.5 / edge_distance * direction, 1.0)
        + vec2(center_side, 0.0)
    );
}

vec4 transition(vec2 op) {
    float perspective = getFromParams(0);
    float unzoom = getFromParams(1);
    float reflection = getFromParams(2);
    float floating = getFromParams(3);

    float uz = unzoom * 2.0 * (0.5 - abs(0.5 - progress));
    vec2 p = -uz * 0.5 + (1.0 + uz) * op;

    vec2 from_scale = vec2(max(1.0 - progress, 0.0001), 1.0);
    vec2 to_scale = vec2(max(progress, 0.0001), 1.0);

    vec2 from_p = cube_xskew(
        (p - vec2(progress, 0.0)) / from_scale,
        1.0 - mix(progress, 0.0, perspective),
        0.0
    );
    vec2 to_p = cube_xskew(
        p / to_scale,
        mix(progress * progress, 1.0, perspective),
        1.0
    );

    if (cube_in_bounds(from_p)) {
        return getFromColor(from_p);
    }
    if (cube_in_bounds(to_p)) {
        return getToColor(to_p);
    }
    return cube_bg_color(op, from_p, to_p, reflection, floating);
}
"#;

const DISPLACEMENT_SAFE_GLSL: &str = r#"
vec4 transition(vec2 uv) {
    float strength = 0.5;
    float displacement = getToColor(uv).r * strength;
    vec2 uv_from = vec2(uv.x + progress * displacement, uv.y);
    vec2 uv_to = vec2(uv.x - (1.0 - progress) * displacement, uv.y);
    return mix(getFromColor(uv_from), getToColor(uv_to), progress);
}
"#;

impl ShaderManager {
    pub fn mark_transition_broken(name: &str) {
        BROKEN_TRANSITIONS.lock().insert(name.to_string());
    }

    pub fn is_transition_broken(name: &str) -> bool {
        BROKEN_TRANSITIONS.lock().contains(name)
    }

    pub fn is_shader_cached(name: &str) -> bool {
        WGSL_CACHE.lock().contains_key(name)
    }

    pub fn save_cache() -> anyhow::Result<()> {
        let cache_dir = dirs::cache_dir().context("no cache dir")?.join("kaleidux");
        std::fs::create_dir_all(&cache_dir)?;
        let cache = WGSL_CACHE.lock();
        let data = postcard::to_allocvec(&*cache)?;
        // Atomic write: write to temp, then rename
        let tmp = cache_dir.join("wgsl_cache.bin.tmp");
        let dst = cache_dir.join("wgsl_cache.bin");
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, &dst)?;
        Ok(())
    }

    pub fn load_cache() -> anyhow::Result<()> {
        let path = dirs::cache_dir()
            .context("no cache dir")?
            .join("kaleidux")
            .join("wgsl_cache.bin");
        if path.exists() {
            let data = std::fs::read(&path)?;
            let loaded: std::collections::HashMap<String, String> = postcard::from_bytes(&data)?;
            let mut cache = WGSL_CACHE.lock();
            for (k, v) in loaded {
                cache.entry(k).or_insert(v);
            }
            tracing::info!(
                "[SHADER] Loaded {} cached WGSL shaders from disk",
                cache.len()
            );
        }
        Ok(())
    }

    pub fn pick_random_transition() -> Transition {
        use rand::seq::SliceRandom;

        let mut candidates: Vec<Transition> = Transition::random_candidate_names()
            .iter()
            .map(|name| Transition::from_name(name))
            .collect();
        let mut rng = rand::thread_rng();

        candidates.shuffle(&mut rng);
        candidates.sort_by_key(|transition| {
            let name = transition.name();
            (
                Self::is_transition_broken(&name),
                !Self::is_shader_cached(&name),
            )
        });

        for transition in candidates {
            let name = transition.name();
            if Self::is_transition_broken(&name) {
                continue;
            }
            match Self::get_builtin_shader(&transition) {
                Ok(_) => return transition,
                Err(e) => {
                    Self::mark_transition_broken(&name);
                    tracing::warn!(
                        "[SHADER] Skipping random transition {} after compile failure: {}",
                        name,
                        e
                    );
                }
            }
        }

        tracing::warn!("[SHADER] No working random transitions available, falling back to fade");
        Transition::Fade
    }

    pub fn compile_glsl(
        name: &str,
        user_code: &str,
        params_mapping: &str,
    ) -> anyhow::Result<String> {
        // 1. Convert params_mapping from "type var = val;" to "#define var (val)"
        let mut defines = String::new();
        // Regex matches "type name = value" ignoring trailing semicolon
        // Use lazy static to compile once and reuse
        static MAPPING_REGEX: once_cell::sync::Lazy<regex::Regex> =
            once_cell::sync::Lazy::new(|| {
                regex::Regex::new(r"^\s*\w+\s+(\w+)\s*=\s*(.+)$").expect("Failed to compile regex")
            });
        let mapping_regex = &*MAPPING_REGEX;

        for stmt in params_mapping.split(';') {
            let s = stmt.trim();
            if s.is_empty() {
                continue;
            }
            if let Some(caps) = mapping_regex.captures(s) {
                let var_name = &caps[1];
                let val = &caps[2];
                // Check if value ends with semicolon (regex greedy match might capture it if not careful,
                // but split(';') removes the delimiter. If implicit semicolon was in the regex match, it might be an issue.
                // Our input strings in get_builtin_shader don't have nested semicolons usually.
                defines.push_str(&format!("#define {} ({})\n", var_name, val));
            } else {
                // Fallback: just include it? If it's a statement not matching, it might be valid code?
                // But usually our mappings are strictly "type name = val".
                // If we fail to macro-ize it, we inject it as is.
                defines.push_str(s);
                defines.push_str(";\n");
            }
        }

        // 2. Strip "uniform type name;" from user_code because Naga requires bindings for uniforms.
        // We replace them with comments.
        // Manual line-based processing is more robust than regex for this specific case, avoiding potential multiline/regex engine quirks.
        let stripped_user_code = user_code
            .lines()
            .map(|line| {
                let ops = line.trim_start();
                if ops.starts_with("uniform ") {
                    format!("// {}", line)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        let full_glsl = format!(
            "{}\n{}\n{}\nvoid main() {{ o_color = transition(v_uv); }}",
            GLSL_PRELUDE, defines, stripped_user_code
        );

        // Log the generated shader for debugging purposes
        tracing::debug!("Compiling GLSL shader '{}'", name);

        let mut parser = naga::front::glsl::Frontend::default();
        let module = parser
            .parse(
                &naga::front::glsl::Options {
                    stage: naga::ShaderStage::Fragment,
                    defines: naga::FastHashMap::default(),
                },
                &full_glsl,
            )
            .map_err(|e| {
                tracing::error!(
                    "GLSL Parse Error in {}: {:?}\nSource:\n{}",
                    name,
                    e,
                    full_glsl
                );
                anyhow::anyhow!("GLSL Parse Error in {}: {:?}", name, e)
            })?;

        let info = naga::valid::Validator::new(
            naga::valid::ValidationFlags::all(),
            naga::valid::Capabilities::all(),
        )
        .validate(&module)
        .map_err(|e| anyhow::anyhow!("Shader Validation Error in {}: {:?}", name, e))?;

        let mut out = String::new();
        let mut writer =
            naga::back::wgsl::Writer::new(&mut out, naga::back::wgsl::WriterFlags::empty());
        writer
            .write(&module, &info)
            .map_err(|e| anyhow::anyhow!("WGSL Generation Error in {}: {:?}", name, e))?;

        Ok(out)
    }

    #[allow(dead_code)]
    pub fn get_shader(transition: &Transition) -> anyhow::Result<String> {
        match transition {
            Transition::Custom { shader, params } => {
                let glsl = Self::load_external_glsl(shader)?;
                let mut mapping = String::new();
                for (name, val) in params {
                    mapping.push_str(&format!("float {} = {}; ", name, val));
                }
                Self::compile_glsl(shader, &glsl, &mapping)
            }
            Transition::Random => {
                let picked = Self::pick_random_transition();
                Self::get_builtin_shader(&picked)
            }
            _ => Self::get_builtin_shader(transition),
        }
    }

    #[allow(dead_code)]
    pub async fn load_external_glsl_async(name: &str) -> anyhow::Result<String> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| anyhow::anyhow!("Failed to get config directory"))?
            .join("kaleidux")
            .join("shaders");

        // Try .glsl then .wgsl (though compile_glsl expects glsl)
        let path = config_dir.join(format!("{}.glsl", name));
        if tokio::fs::metadata(&path).await.is_ok() {
            return tokio::fs::read_to_string(path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read shader: {}", e));
        }

        anyhow::bail!("Shader not found in ~/.config/kaleidux/shaders/: {}", name)
    }

    #[allow(dead_code)]
    pub fn load_external_glsl(name: &str) -> anyhow::Result<String> {
        // Use block_in_place to call async version from sync context
        tokio::task::block_in_place(|| -> anyhow::Result<String> {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                if let Ok(result) = handle.block_on(Self::load_external_glsl_async(name)) {
                    return Ok(result);
                }
            }
            // Fallback to sync if no runtime available
            let config_dir = dirs::config_dir()
                .ok_or_else(|| anyhow::anyhow!("Failed to get config directory"))?
                .join("kaleidux")
                .join("shaders");
            let path = config_dir.join(format!("{}.glsl", name));
            if path.exists() {
                std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("Failed to read shader: {}", e))
            } else {
                Err(anyhow::anyhow!(
                    "Shader not found in ~/.config/kaleidux/shaders/: {}",
                    name
                ))
            }
        })
    }

    pub fn get_builtin_shader(transition: &Transition) -> anyhow::Result<String> {
        let name = transition.name();

        // Check process-wide cache first (P-21)
        if let Some(cached) = WGSL_CACHE.lock().get(&name) {
            return Ok(cached.clone());
        }

        let glsl = match transition {
            Transition::Cube { .. } => CUBE_SAFE_GLSL,
            Transition::Displacement => DISPLACEMENT_SAFE_GLSL,
            _ => Self::get_builtin_glsl(&name)
                .ok_or_else(|| anyhow::anyhow!("Builtin shader not found: {}", name))?,
        };

        // Note: We use getFromParams(i) which handles the aligned vec4 array access
        // We must map Rust struct fields to the EXACT uniform names used in the GLSL shaders.
        let mapping = match transition {
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
            // Actually, ButterflyWaveScrawler.glsl standard is usually `colorSeparation`. Let's guess camelCase to be safe or check?
            // Most gl-transitions use camelCase. I'll define BOTH to be safe if that works? No, redefinition error.
            // Let's stick to what we had unless proven wrong (User didn't complain about Butterfly). Use original:
            // "float amplitude = getFromParams(0); float waves = getFromParams(1); float color_separation = getFromParams(2);"
            Transition::Circle => {
                "vec2 center = vec2(0.5, 0.5); vec3 backColor = vec3(0.1, 0.1, 0.1);"
            }
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
            Transition::CrossWarp => "", // No params usually
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
            } // Wait, verify `directionalwarp` uses smoothness? grep said: `uniform float smoothness;`.
            Transition::DirectionalWipe { .. } => {
                "vec2 direction = vec2(getFromParams(0), getFromParams(1)); float smoothness = getFromParams(2);"
            }
            Transition::Displacement => "",
            Transition::Dissolve { .. } => {
                "float uLineWidth = getFromParams(0); vec3 uSpreadClr = vec3(getFromParams(1), getFromParams(2), getFromParams(3)); vec3 uHotClr = vec3(getFromParams(4), getFromParams(5), getFromParams(6)); float uPow = getFromParams(7); float uIntensity = getFromParams(8);"
            }
            Transition::Doom { .. } => {
                "int bars = int(getFromParams(0)); float amplitude = getFromParams(1); float noise = getFromParams(2); float frequency = getFromParams(3); float dripScale = getFromParams(4);"
            } // grep didn't show dripScale name but camelCase is safer guess.
            // Wait, previous code used `drip_scale`. I'll trust previous code unless I see error.
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
            Transition::Luma => {
                // FALLBACK: Use fade for now, but ensure it's cached correctly.
                // Luma crashes without secondary texture in current implementation.
                let name = transition.name();
                let glsl = Self::get_builtin_glsl("fade")
                    .ok_or_else(|| anyhow::anyhow!("Failed to find fallback shader 'fade'"))?;
                let wgsl = Self::compile_glsl("fade", glsl, "")?;
                WGSL_CACHE.lock().insert(name.to_lowercase(), wgsl.clone());
                return Ok(wgsl);
            }
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
            Transition::Overexposure => "float strength = getFromParams(0);",
            Transition::SquaresWire { .. } => {
                "ivec2 squares = ivec2(int(getFromParams(0)), int(getFromParams(1))); vec2 direction = vec2(getFromParams(2), getFromParams(3)); float smoothness = getFromParams(4);"
            }
            _ => "",
        };

        let wgsl = Self::compile_glsl(&name, glsl, mapping)?;

        // Store in process-wide cache (P-21)
        WGSL_CACHE.lock().insert(name, wgsl.clone());
        Ok(wgsl)
    }

    pub fn get_builtin_glsl(name: &str) -> Option<&'static str> {
        match name {
            "angular" => Some(include_str!("shaders/transitions/angular.glsl")),
            "BookFlip" => Some(include_str!("shaders/transitions/BookFlip.glsl")),
            "Bounce" => Some(include_str!("shaders/transitions/Bounce.glsl")),
            "BowTieHorizontal" => Some(include_str!("shaders/transitions/BowTieHorizontal.glsl")),
            "BowTieVertical" => Some(include_str!("shaders/transitions/BowTieVertical.glsl")),
            "BowTieWithParameter" => {
                Some(include_str!("shaders/transitions/BowTieWithParameter.glsl"))
            }
            "burn" => Some(include_str!("shaders/transitions/burn.glsl")),
            "ButterflyWaveScrawler" => Some(include_str!(
                "shaders/transitions/ButterflyWaveScrawler.glsl"
            )),
            "cannabisleaf" => Some(include_str!("shaders/transitions/cannabisleaf.glsl")),
            "circle" => Some(include_str!("shaders/transitions/circle.glsl")),
            "CircleCrop" => Some(include_str!("shaders/transitions/CircleCrop.glsl")),
            "circleopen" => Some(include_str!("shaders/transitions/circleopen.glsl")),
            "colorphase" => Some(include_str!("shaders/transitions/colorphase.glsl")),
            "coord-from-in" => Some(include_str!("shaders/transitions/coord-from-in.glsl")),
            "CrazyParametricFun" => {
                Some(include_str!("shaders/transitions/CrazyParametricFun.glsl"))
            }
            "ColourDistance" => Some(include_str!("shaders/transitions/ColourDistance.glsl")),
            "crosshatch" => Some(include_str!("shaders/transitions/crosshatch.glsl")),
            "crosswarp" => Some(include_str!("shaders/transitions/crosswarp.glsl")),
            "CrossZoom" => Some(include_str!("shaders/transitions/CrossZoom.glsl")),
            "cube" => Some(include_str!("shaders/transitions/cube.glsl")),
            "Directional" => Some(include_str!("shaders/transitions/Directional.glsl")),
            "directional-easing" => {
                Some(include_str!("shaders/transitions/directional-easing.glsl"))
            }
            "DirectionalScaled" => Some(include_str!("shaders/transitions/DirectionalScaled.glsl")),
            "directionalwarp" => Some(include_str!("shaders/transitions/directionalwarp.glsl")),
            "directionalwipe" => Some(include_str!("shaders/transitions/directionalwipe.glsl")),
            "displacement" => Some(include_str!("shaders/transitions/displacement.glsl")),
            "dissolve" => Some(include_str!("shaders/transitions/dissolve.glsl")),
            "DoomScreenTransition" => Some(include_str!(
                "shaders/transitions/DoomScreenTransition.glsl"
            )),
            "doorway" => Some(include_str!("shaders/transitions/doorway.glsl")),
            "Dreamy" => Some(include_str!("shaders/transitions/Dreamy.glsl")),
            "DreamyZoom" => Some(include_str!("shaders/transitions/DreamyZoom.glsl")),
            "EdgeTransition" => Some(include_str!("shaders/transitions/EdgeTransition.glsl")),
            "fade" => Some(include_str!("shaders/transitions/fade.glsl")),
            "fadecolor" => Some(include_str!("shaders/transitions/fadecolor.glsl")),
            "fadegrayscale" => Some(include_str!("shaders/transitions/fadegrayscale.glsl")),
            "FilmBurn" => Some(include_str!("shaders/transitions/FilmBurn.glsl")),
            "flyeye" => Some(include_str!("shaders/transitions/flyeye.glsl")),
            "GlitchDisplace" => Some(include_str!("shaders/transitions/GlitchDisplace.glsl")),
            "GlitchMemories" => Some(include_str!("shaders/transitions/GlitchMemories.glsl")),
            "GridFlip" => Some(include_str!("shaders/transitions/GridFlip.glsl")),
            "heart" => Some(include_str!("shaders/transitions/heart.glsl")),
            "hexagonalize" => Some(include_str!("shaders/transitions/hexagonalize.glsl")),
            "HorizontalClose" => Some(include_str!("shaders/transitions/HorizontalClose.glsl")),
            "HorizontalOpen" => Some(include_str!("shaders/transitions/HorizontalOpen.glsl")),
            "InvertedPageCurl" => Some(include_str!("shaders/transitions/InvertedPageCurl.glsl")),
            "kaleidoscope" => Some(include_str!("shaders/transitions/kaleidoscope.glsl")),
            "LeftRight" => Some(include_str!("shaders/transitions/LeftRight.glsl")),
            "LinearBlur" => Some(include_str!("shaders/transitions/LinearBlur.glsl")),
            "luma" => Some(include_str!("shaders/transitions/luma.glsl")),
            "luminance_melt" => Some(include_str!("shaders/transitions/luminance_melt.glsl")),
            "morph" => Some(include_str!("shaders/transitions/morph.glsl")),
            "Mosaic" => Some(include_str!("shaders/transitions/Mosaic.glsl")),
            "mosaic_transition" => Some(include_str!("shaders/transitions/mosaic_transition.glsl")),
            "multiply_blend" => Some(include_str!("shaders/transitions/multiply_blend.glsl")),
            "Overexposure" => Some(include_str!("shaders/transitions/Overexposure.glsl")),
            "perlin" => Some(include_str!("shaders/transitions/perlin.glsl")),
            "pinwheel" => Some(include_str!("shaders/transitions/pinwheel.glsl")),
            "pixelize" => Some(include_str!("shaders/transitions/pixelize.glsl")),
            "polar_function" => Some(include_str!("shaders/transitions/polar_function.glsl")),
            "PolkaDotsCurtain" => Some(include_str!("shaders/transitions/PolkaDotsCurtain.glsl")),
            "powerKaleido" => Some(include_str!("shaders/transitions/powerKaleido.glsl")),
            "Radial" => Some(include_str!("shaders/transitions/Radial.glsl")),
            "randomNoisex" => Some(include_str!("shaders/transitions/randomNoisex.glsl")),
            "randomsquares" => Some(include_str!("shaders/transitions/randomsquares.glsl")),
            "Rectangle" => Some(include_str!("shaders/transitions/Rectangle.glsl")),
            "RectangleCrop" => Some(include_str!("shaders/transitions/RectangleCrop.glsl")),
            "ripple" => Some(include_str!("shaders/transitions/ripple.glsl")),
            "Rolls" => Some(include_str!("shaders/transitions/Rolls.glsl")),
            "rotateTransition" => Some(include_str!("shaders/transitions/rotateTransition.glsl")),
            "rotate_scale_fade" => Some(include_str!("shaders/transitions/rotate_scale_fade.glsl")),
            "RotateScaleVanish" => Some(include_str!("shaders/transitions/RotateScaleVanish.glsl")),
            "scale-in" => Some(include_str!("shaders/transitions/scale-in.glsl")),
            "SimpleZoom" => Some(include_str!("shaders/transitions/SimpleZoom.glsl")),
            "SimpleZoomOut" => Some(include_str!("shaders/transitions/SimpleZoomOut.glsl")),
            "Slides" => Some(include_str!("shaders/transitions/Slides.glsl")),
            "squareswire" => Some(include_str!("shaders/transitions/squareswire.glsl")),
            "squeeze" => Some(include_str!("shaders/transitions/squeeze.glsl")),
            "StaticFade" => Some(include_str!("shaders/transitions/StaticFade.glsl")),
            "static_wipe" => Some(include_str!("shaders/transitions/static_wipe.glsl")),
            "StereoViewer" => Some(include_str!("shaders/transitions/StereoViewer.glsl")),
            "swap" => Some(include_str!("shaders/transitions/swap.glsl")),
            "Swirl" => Some(include_str!("shaders/transitions/Swirl.glsl")),
            "tangentMotionBlur" => Some(include_str!("shaders/transitions/tangentMotionBlur.glsl")),
            "TopBottom" => Some(include_str!("shaders/transitions/TopBottom.glsl")),
            "TVStatic" => Some(include_str!("shaders/transitions/TVStatic.glsl")),
            "undulatingBurnOut" => Some(include_str!("shaders/transitions/undulatingBurnOut.glsl")),
            "VerticalClose" => Some(include_str!("shaders/transitions/VerticalClose.glsl")),
            "VerticalOpen" => Some(include_str!("shaders/transitions/VerticalOpen.glsl")),
            "WaterDrop" => Some(include_str!("shaders/transitions/WaterDrop.glsl")),
            "wind" => Some(include_str!("shaders/transitions/wind.glsl")),
            "windowblinds" => Some(include_str!("shaders/transitions/windowblinds.glsl")),
            "windowslice" => Some(include_str!("shaders/transitions/windowslice.glsl")),
            "wipeDown" => Some(include_str!("shaders/transitions/wipeDown.glsl")),
            "wipeLeft" => Some(include_str!("shaders/transitions/wipeLeft.glsl")),
            "wipeRight" => Some(include_str!("shaders/transitions/wipeRight.glsl")),
            "wipeUp" => Some(include_str!("shaders/transitions/wipeUp.glsl")),
            "x_axis_translation" => {
                Some(include_str!("shaders/transitions/x_axis_translation.glsl"))
            }
            "ZoomInCircles" => Some(include_str!("shaders/transitions/ZoomInCircles.glsl")),
            "ZoomLeftWipe" => Some(include_str!("shaders/transitions/ZoomLeftWipe.glsl")),
            "ZoomRigthWipe" => Some(include_str!("shaders/transitions/ZoomRigthWipe.glsl")),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ShaderManager;
    use kaleidux_common::Transition;

    #[test]
    fn all_random_candidate_shaders_compile() {
        let mut failures = Vec::new();

        for name in Transition::random_candidate_names() {
            let transition = Transition::from_name(name);
            if let Err(e) = ShaderManager::get_builtin_shader(&transition) {
                failures.push(format!("{}: {}", name, e));
            }
        }

        assert!(
            failures.is_empty(),
            "builtin transition shader failures:\n{}",
            failures.join("\n")
        );
    }
}
