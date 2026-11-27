//! Adapts desktop and GLSL ES fragments to the layer uniform and texture ABI.

use anyhow::{Context, Result};
use kaleidux_common::LayerStage;

const GLSL_FRAME: &str = r#"
layout(set = 0, binding = 0, std140) uniform KldFrame {
    vec2 size;
    float seconds;
    float progress;
    vec4 params;
    vec4 timing;
    vec4 pointer;
    vec4 data;
    vec4 date;
    vec4 audio[16];
    vec4 controls[8];
    vec4 cursor;
} kld_frame;
layout(location = 0) in vec2 kld_uv;
#define iResolution vec3(kld_frame.size, 1.0)
#define iTime kld_frame.seconds
#define iGlobalTime kld_frame.seconds
#define iFrame int(kld_frame.timing.y)
#define iTimeDelta (kld_frame.timing.x)
#define iFrameRate (kld_frame.timing.x > 0.0 ? 1.0 / kld_frame.timing.x : 0.0)
#define iMouse vec4(vec2(kld_frame.pointer.x, 1.0-kld_frame.pointer.y)*kld_frame.size, kld_frame.pointer.zw)
#define iDate kld_frame.date
#define kldPointer kld_frame.pointer
#define kldCursor kld_frame.cursor
#define kldData kld_frame.data
float kldAudio(int index) { int n = clamp(index, 0, 63); return kld_frame.audio[n / 4][n % 4]; }
float kldControl(int index) { int n = clamp(index, 0, 31); return kld_frame.controls[n / 4][n % 4]; }
#define u_resolution kld_frame.size
#define u_time kld_frame.seconds
#define v_uv kld_uv
"#;

const GLSL_SAMPLE: &str = r#"
layout(set = 0, binding = 1) uniform texture2D kld_input;
layout(set = 0, binding = 2) uniform sampler kld_sampler;
#define iChannel0 sampler2D(kld_input, kld_sampler)
#define iChannelTime float[4](iTime, 0.0, 0.0, 0.0)
#define u_tex iChannel0
"#;

const GLSL_WALLPAPER_SAMPLE: &str = r#"
layout(set = 0, binding = 3) uniform texture2D kld_channel1;
layout(set = 0, binding = 4) uniform texture2D kld_channel2;
layout(set = 0, binding = 5) uniform texture2D kld_channel3;
#define iChannel1 sampler2D(kld_channel1, kld_sampler)
#define iChannel2 sampler2D(kld_channel2, kld_sampler)
#define iChannel3 sampler2D(kld_channel3, kld_sampler)
#define iChannelResolution vec3[4](vec3(textureSize(iChannel0, 0), 1.0), vec3(textureSize(iChannel1, 0), 1.0), vec3(textureSize(iChannel2, 0), 1.0), vec3(textureSize(iChannel3, 0), 1.0))
"#;

const GLSL_EFFECT_SAMPLE: &str = r#"
layout(set = 0, binding = 3) uniform texture2D kld_depth;
#define iChannelResolution vec3[4](iResolution, vec3(0.0), vec3(0.0), vec3(0.0))
vec4 sampleWallpaper(vec2 uv) { return texture(u_tex, uv); }
"#;

const RETRO_FRAGMENT: &str = r#"
#define FRAGMENT 1
#define Texture iChannel0
#define InputSize kld_frame.size
#define TextureSize kld_frame.size
#define OutputSize kld_frame.size
#define FrameCount iFrame
#define FrameDirection 1
#define texture2D texture
#define gl_FragColor kld_retro_colour
layout(location = 0) out vec4 kld_retro_colour;
"#;

static SCREEN_UNIFORMS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"(?m)^[\t ]*uniform\s+(sampler2D\s+tex|float\s+time|vec2\s+full_size)\s*;")
        .expect("fixed screen shader uniform pattern")
});

pub(super) fn uses_screen_texture(source: &str) -> bool {
    SCREEN_UNIFORMS
        .captures_iter(source)
        .any(|value| value[1].starts_with("sampler2D"))
}

pub(super) fn translate(stage: LayerStage, source: &str) -> Result<String> {
    translate_fragment(stage, source, None)
}

pub(super) fn translate_shadertoy(source: &str, buffer: bool) -> Result<String> {
    translate_fragment(LayerStage::Wallpaper, source, Some(buffer))
}

fn translate_fragment(
    stage: LayerStage,
    source: &str,
    shadertoy_buffer: Option<bool>,
) -> Result<String> {
    let mut screen = String::new();
    let screen_shader = shadertoy_buffer.is_none() && uses_screen_texture(source);
    let original = if screen_shader {
        for uniform in SCREEN_UNIFORMS.captures_iter(source) {
            if let Some(name) = uniform[1].split_whitespace().last() {
                let alias = match name {
                    "tex" => "#define tex iChannel0\n",
                    "time" => "#define time kld_frame.seconds\n",
                    "full_size" => "#define full_size kld_frame.size\n",
                    _ => "",
                };
                if !screen.contains(alias) {
                    screen.push_str(alias);
                }
            }
        }
        SCREEN_UNIFORMS.replace_all(source, |matches: &regex::Captures<'_>| {
            matches[0]
                .chars()
                .map(|character| if character == '\n' { '\n' } else { ' ' })
                .collect::<String>()
        })
    } else {
        std::borrow::Cow::Borrowed(source)
    };
    // Preserve source line numbers when dropping declarations supplied by this ABI.
    static DECLARATIONS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(?m)^[\t ]*(?:#version[^\n]*|precision\s+(?:lowp|mediump|highp)\s+\w+\s*;|uniform\s+\w+\s+(?:iResolution|iTime|iGlobalTime|iFrame|iTimeDelta|iFrameRate|iDate|iMouse|iChannel[0-3]|iChannelResolution|iChannelTime|u_resolution|u_time|u_tex)(?:\s*\[\s*4\s*\])?\s*;)",
        )
        .expect("fixed GLSL declarations")
    });
    static OUTPUT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?m)^([\t ]*)out\s+vec4\s+([A-Za-z_]\w*)\s*;")
            .expect("fixed fragment output pattern")
    });
    static COORDINATE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?m)^[\t ]*(?:layout\s*\(\s*location\s*=\s*0\s*\)\s*)?(?:varying|in)\s+vec2\s+([A-Za-z_]\w*)\s*;")
            .expect("fixed fragment coordinate declaration")
    });
    static RETRO_UNIFORMS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?m)^[\t ]*uniform\s+\w+\s+(?:Texture|TextureSize|InputSize|OutputSize|FrameCount|FrameDirection)\s*;")
            .expect("fixed Libretro uniform declaration")
    });
    let body = DECLARATIONS.replace_all(&original, |captures: &regex::Captures<'_>| {
        captures[0]
            .chars()
            .map(|ch| if ch == '\n' { '\n' } else { ' ' })
            .collect::<String>()
    });
    let body = OUTPUT.replace_all(&body, "${1}layout(location = 0) out vec4 ${2};");
    let body = COORDINATE.replace_all(&body, |matches: &regex::Captures<'_>| {
        if &matches[1] == "v_uv" {
            String::new()
        } else {
            format!("#define {} kld_uv", &matches[1])
        }
    });
    let retro = stage == LayerStage::Filter && source.contains("FRAGMENT");
    let (body, retro_prelude) = if retro {
        (
            RETRO_UNIFORMS.replace_all(&body, "").into_owned(),
            RETRO_FRAGMENT,
        )
    } else {
        (body.into_owned(), "")
    };
    let sample = if stage == LayerStage::Filter {
        GLSL_EFFECT_SAMPLE
    } else {
        GLSL_WALLPAPER_SAMPLE
    };
    let sample = if shadertoy_buffer.is_some() {
        let mut sample = sample.to_owned();
        for (channel, binding) in [(1, 6), (2, 7), (3, 8)] {
            sample = sample.replace(
                &format!("kld_channel{channel}, kld_sampler)"),
                &format!("kld_channel{channel}, kld_sampler{channel})"),
            );
            sample = format!(
                "layout(set = 0, binding = {binding}) uniform sampler kld_sampler{channel};\n{sample}"
            );
        }
        sample
    } else {
        sample.to_owned()
    };
    let entry = if shadertoy_buffer.is_some()
        || body.contains("mainImage(")
        || body.contains("mainImage (")
    {
        let coordinates = if shadertoy_buffer == Some(true) {
            "kld_uv"
        } else {
            "vec2(kld_uv.x, 1.0 - kld_uv.y)"
        };
        format!(
            r#"
layout(location = 0) out vec4 kld_colour;
void main() {{
    mainImage(kld_colour, {coordinates} * kld_frame.size);
}}
"#
        )
    } else {
        String::new()
    };
    let full = format!(
        "#version 450\n{GLSL_FRAME}\n{GLSL_SAMPLE}\n{sample}\n{retro_prelude}\n{screen}\n{body}\n{entry}"
    );
    let module = naga::front::glsl::Frontend::default()
        .parse(
            &naga::front::glsl::Options::from(naga::ShaderStage::Fragment),
            &full,
        )
        .map_err(|error| anyhow::anyhow!("{}", error.emit_to_string(&full)))?;
    let valid = naga::valid::Validator::new(
        naga::valid::ValidationFlags::all(),
        naga::valid::Capabilities::empty(),
    )
    .validate(&module)
    .context("GLSL fragment validation failed")?;
    naga::back::wgsl::write_string(&module, &valid, naga::back::wgsl::WriterFlags::empty())
        .context("GLSL fragment translation failed")
}
