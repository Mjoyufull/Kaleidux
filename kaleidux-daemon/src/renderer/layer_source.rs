//! Reads bounded local layer sources and validates their ABI before rendering uses them.

use anyhow::{Context, Result};
use kaleidux_common::{
    LayerSelection, LayerStage, MAX_LAYERS_PER_STAGE, MAX_MORPHS_PER_OUTPUT, MorphGesture,
    MorphLayout,
};
use std::io::Read;
use std::path::Path;

const MAX_SHADER_BYTES: usize = 64 * 1024;
pub(crate) const MAX_LAYER_TEXTURE_BYTES: usize = 96 * 1024 * 1024;
const FRAME: &str = include_str!("../shaders/layers/frame.wgsl");
const SAMPLE: &str = include_str!("../shaders/layers/sample.wgsl");
pub(super) const HEART: &str = include_str!("../shaders/layers/heart.wgsl");

pub(crate) struct PreparedLayers {
    pub(crate) stage: LayerStage,
    pub(crate) fps: u16,
    pub(crate) shaders: Vec<PreparedLayer>,
    pub(crate) source: Option<String>,
    pub(crate) feedback: Option<super::layer_shadertoy::PreparedToy>,
    #[cfg(feature = "retroarch-shaders")]
    pub(crate) retro: Option<super::layer_retro::PreparedPreset>,
}

pub(crate) struct PreparedLayer {
    pub(crate) name: String,
    pub(super) source: String,
    pub(crate) fragment_only: bool,
    pub(crate) assets: [Option<image::RgbaImage>; 4],
    pub(crate) rectangle: Option<[f32; 4]>,
    pub(super) actions: [Option<String>; 4],
    pub(super) track_pointer: bool,
    pub(super) track_audio: bool,
    pub(super) track_cursor: bool,
}

impl PreparedLayer {
    pub(crate) fn texture_bytes(&self) -> usize {
        self.assets
            .iter()
            .flatten()
            .map(|asset| asset.as_raw().len())
            .sum()
    }
}

impl PreparedLayers {
    pub(crate) fn is_empty(&self) -> bool {
        self.shaders.is_empty() && self.feedback.is_none()
    }

    pub(crate) fn stage(&self) -> LayerStage {
        self.stage
    }

    /// Read and validate sources on a bounded worker before sending the IPC command.
    pub(crate) async fn load(
        selection: &LayerSelection,
        detect_screen_shader: bool,
    ) -> Result<Self> {
        anyhow::ensure!(
            selection.stage != LayerStage::Wallpaper || selection.layers.len() <= 1,
            "Choose one shader wallpaper",
        );
        let limit = layer_limit(selection.stage);
        anyhow::ensure!(
            selection.layers.len() <= limit,
            "This stage supports at most {limit} layers"
        );
        anyhow::ensure!(
            selection.stage == LayerStage::Morph
                || (selection.group.is_none() && selection.layout.is_none()),
            "Groups and automatic layouts apply to morphs"
        );
        if let Some(group) = &selection.group {
            anyhow::ensure!(
                short_identifier(group),
                "Morph group must be a short identifier"
            );
        }
        if selection.layers.is_empty() {
            return Ok(Self {
                stage: selection.stage,
                fps: 0,
                shaders: Vec::new(),
                source: None,
                feedback: None,
                #[cfg(feature = "retroarch-shaders")]
                retro: None,
            });
        }
        let selection = selection.clone();
        crate::background::spawn_blocking_tracked_wait(
            crate::background::BackgroundWorkKind::RendererInit,
            move || Self::read_and_validate(selection, detect_screen_shader),
        )
        .await
        .context("Renderer workers are shutting down")?
        .await
        .context("Layer preparation did not complete")?
    }

    fn read_and_validate(
        mut selection: LayerSelection,
        detect_screen_shader: bool,
    ) -> Result<Self> {
        if detect_screen_shader
            && selection.stage == LayerStage::Wallpaper
            && let Some(path) = selection.layers.first().map(Path::new)
            && is_fragment_file(path)
            && super::layer_glsl::uses_screen_texture(&read_shader(path)?)
        {
            selection.stage = LayerStage::Filter;
            selection.fps = 0;
        }
        if selection.stage == LayerStage::Wallpaper
            && let Some(source) = selection.layers.first()
            && super::layer_shadertoy::is_manifest(Path::new(source))
        {
            return Ok(Self {
                stage: selection.stage,
                fps: selection.fps,
                shaders: Vec::new(),
                source: Some(source.clone()),
                feedback: Some(super::layer_shadertoy::load(Path::new(source))?),
                #[cfg(feature = "retroarch-shaders")]
                retro: None,
            });
        }
        if selection.stage == LayerStage::Wallpaper
            && let Some(source) = selection.layers.first()
            && crate::scene::is_scene(Path::new(source))
        {
            return crate::scene::open(Path::new(source));
        }
        if selection.stage == LayerStage::Filter
            && let Some(path) = selection.layers.iter().map(Path::new).find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "slangp")
            })
        {
            anyhow::ensure!(
                selection.layers.len() == 1,
                "Choose a Slang preset on its own"
            );
            #[cfg(feature = "retroarch-shaders")]
            return super::layer_retro::prepare(path, selection.fps);
            #[cfg(not(feature = "retroarch-shaders"))]
            {
                let _ = path;
                anyhow::bail!("This build does not include retroarch-shaders");
            }
        }
        let mut shaders = Vec::new();
        let cells = selection
            .layout
            .as_ref()
            .map(|layout| layout_cells(layout, selection.layers.len()))
            .transpose()?;
        for (index, name) in selection.layers.iter().enumerate() {
            let path = Path::new(name);
            if selection.stage == LayerStage::Filter
                && path.extension().is_some_and(|ext| ext == "glslp")
            {
                for pass in preset_paths(path)? {
                    shaders.push(prepare_layer(selection.stage, &pass.to_string_lossy())?);
                }
            } else {
                shaders.push(
                    prepare_layer_in(
                        selection.stage,
                        name,
                        cells.as_ref().map(|cells| cells[index]),
                    )
                    .with_context(|| format!("Invalid layer {name:?}"))?,
                );
            }
            anyhow::ensure!(
                shaders.len() <= layer_limit(selection.stage),
                "Too many shader passes"
            );
            anyhow::ensure!(
                shaders
                    .iter()
                    .map(PreparedLayer::texture_bytes)
                    .sum::<usize>()
                    <= MAX_LAYER_TEXTURE_BYTES,
                "Shader textures exceed 96 MiB"
            );
        }
        if selection.stage == LayerStage::Filter {
            anyhow::ensure!(
                shaders
                    .iter()
                    .filter(|layer| layer.assets[0].is_some())
                    .count()
                    <= 1,
                "Only one depth map may appear in a shader stack"
            );
        }
        Ok(Self {
            stage: selection.stage,
            fps: selection.fps,
            shaders,
            feedback: None,
            #[cfg(feature = "retroarch-shaders")]
            retro: None,
            source: selection.layers.first().map(|name| {
                name.split_once('|')
                    .map_or(name.as_str(), |(path, _)| path)
                    .to_owned()
            }),
        })
    }
}

pub(crate) fn prepare_layer(stage: LayerStage, spec: &str) -> Result<PreparedLayer> {
    prepare_layer_in(stage, spec, None)
}

fn prepare_layer_in(
    stage: LayerStage,
    spec: &str,
    layout_rectangle: Option<[f32; 4]>,
) -> Result<PreparedLayer> {
    let (name, options) = spec.split_once('|').unwrap_or((spec, ""));
    let shader_path = Path::new(name);
    let fragment_only = !["text=", "image=", "parallax="]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        && is_fragment_file(shader_path);
    let mut assets = [None, None, None, None];
    let mut rectangle = layout_rectangle;
    let mut actions = [None, None, None, None];
    let cursor_effect = stage == LayerStage::Filter
        && (matches!(name, "pan" | "auto-pan" | "parallax") || name.starts_with("parallax="));
    let mut track_pointer = cursor_effect || (stage == LayerStage::Morph && name == "pointer");
    let mut intensity = 0.7f32;
    let mut font = None;
    let mut colour = "#ffffff";
    for option in options.split('|').filter(|option| !option.is_empty()) {
        let (key, value) = option
            .split_once('=')
            .context("Expected layer option key=value")?;
        match key {
            "rect" if stage == LayerStage::Morph => rectangle = Some(parse_rectangle(value)?),
            "pointer" => {
                track_pointer = value.parse().context("Use pointer=true or pointer=false")?
            }
            "intensity" if cursor_effect => {
                intensity = value.parse().context("Use an intensity between 0 and 1")?;
                anyhow::ensure!(
                    (0.0..=1.0).contains(&intensity),
                    "Use an intensity between 0 and 1"
                );
            }
            "action" | "secondary" | "scroll-up" | "scroll-down" if stage == LayerStage::Morph => {
                anyhow::ensure!(
                    short_identifier(value),
                    "Morph action must be a short identifier"
                );
                let gesture = match key {
                    "secondary" => MorphGesture::Secondary,
                    "scroll-up" => MorphGesture::ScrollUp,
                    "scroll-down" => MorphGesture::ScrollDown,
                    _ => MorphGesture::Primary,
                };
                actions[gesture.index()] = Some(value.to_owned());
            }
            "font" if stage == LayerStage::Morph => font = Some(value),
            "colour" | "color" if stage == LayerStage::Morph => colour = value,
            "channel0" | "channel1" | "channel2" | "channel3"
                if fragment_only && stage != LayerStage::Filter =>
            {
                let index = usize::from(key.as_bytes()[7] - b'0');
                let directory = shader_path
                    .parent()
                    .context("GLSL path needs a parent directory")?;
                anyhow::ensure!(assets[index].is_none(), "Duplicate GLSL channel {index}");
                assets[index] = Some(super::layer_bitmap::read(&directory.join(value))?);
                anyhow::ensure!(
                    assets
                        .iter()
                        .flatten()
                        .map(|asset| asset.as_raw().len())
                        .sum::<usize>()
                        <= MAX_LAYER_TEXTURE_BYTES,
                    "Shader textures exceed 96 MiB"
                );
            }
            _ => anyhow::bail!("Unknown layer option {key:?}"),
        }
    }
    let body = if stage != LayerStage::Filter
        && let Some(path) = name.strip_prefix("image=")
    {
        assets[0] = Some(super::layer_bitmap::read(Path::new(path))?);
        include_str!("../shaders/layers/bitmap.wgsl").into()
    } else if stage == LayerStage::Morph
        && let Some(text) = name.strip_prefix("text=")
        && (font.is_some() || !text.is_ascii() || colour != "#ffffff")
    {
        assets[0] = Some(super::layer_bitmap::label(
            text,
            font.unwrap_or("Sans 24"),
            colour,
        )?);
        rectangle.get_or_insert([0.03, 0.03, 0.35, 0.07]);
        include_str!("../shaders/layers/bitmap.wgsl").into()
    } else if stage == LayerStage::Filter
        && let Some(path) = name.strip_prefix("parallax=")
    {
        assets[0] = Some(super::layer_bitmap::depth(Path::new(path))?);
        include_str!("../shaders/layers/parallax.wgsl").into()
    } else {
        read_body(stage, name)?
    };
    let track_audio = uses_audio(&body);
    let track_cursor = uses_cursor(&body);
    let body = if cursor_effect {
        format!(
            "const CURSOR_STRENGTH: f32 = {intensity:.8};\nconst USE_DEPTH: bool = {};\n{body}",
            assets[0].is_some()
        )
    } else {
        body
    };
    let source = if fragment_only {
        super::layer_glsl::translate(stage, &body)?
    } else {
        assemble(stage, &body)
    };
    validate_abi(stage, &source, fragment_only, assets[0].is_some())?;
    anyhow::ensure!(
        actions.iter().all(Option::is_none) || rectangle.is_some(),
        "A morph action needs a rectangle"
    );
    Ok(PreparedLayer {
        name: spec.to_owned(),
        source,
        fragment_only,
        assets,
        rectangle,
        actions,
        track_pointer,
        track_audio,
        track_cursor,
    })
}

pub(crate) fn prepare_bitmap_layer(
    name: String,
    bitmap: image::RgbaImage,
    rectangle: Option<[f32; 4]>,
    body: &str,
) -> Result<PreparedLayer> {
    let source = assemble(LayerStage::Morph, body);
    validate_abi(LayerStage::Morph, &source, false, true)?;
    Ok(PreparedLayer {
        name,
        source,
        fragment_only: false,
        assets: [Some(bitmap), None, None, None],
        rectangle,
        actions: Default::default(),
        track_pointer: false,
        track_audio: false,
        track_cursor: false,
    })
}

pub(super) fn uses_audio(source: &str) -> bool {
    ["audio_band(", "frame.audio", "kldAudio("]
        .iter()
        .any(|symbol| source.contains(symbol))
}

pub(super) fn uses_cursor(source: &str) -> bool {
    source.contains("frame.cursor") || source.contains("kldCursor")
}

fn parse_rectangle(value: &str) -> Result<[f32; 4]> {
    let coordinates: Vec<f32> = value.split(',').map(str::parse).collect::<Result<_, _>>()?;
    let result: [f32; 4] = coordinates
        .try_into()
        .map_err(|_| anyhow::anyhow!("Use rect=x,y,width,height"))?;
    validate_rectangle(result)?;
    Ok(result)
}

fn validate_rectangle(result: [f32; 4]) -> Result<()> {
    let [x, y, w, h] = result;
    anyhow::ensure!(
        result
            .iter()
            .all(|value| value.is_finite() && *value >= 0.0 && *value <= 1.0)
            && w > 0.0
            && h > 0.0
            && x + w <= 1.0
            && y + h <= 1.0,
        "Morph rectangle must fit inside the normalized output"
    );
    Ok(())
}

fn layer_limit(stage: LayerStage) -> usize {
    if stage == LayerStage::Morph {
        MAX_MORPHS_PER_OUTPUT
    } else {
        MAX_LAYERS_PER_STAGE
    }
}

fn short_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn layout_cells(layout: &MorphLayout, count: usize) -> Result<Vec<[f32; 4]>> {
    validate_rectangle(layout.bounds)?;
    anyhow::ensure!(
        (0.0..=1.0).contains(&layout.gap),
        "Morph layout gap must be in 0..=1"
    );
    anyhow::ensure!(
        layout.columns <= MAX_MORPHS_PER_OUTPUT,
        "Too many layout columns"
    );
    let columns = if layout.columns == 0 {
        count
    } else {
        layout.columns.min(count)
    };
    let rows = count.div_ceil(columns);
    let [x, y, width, height] = layout.bounds;
    let cell_width = (width - layout.gap * (columns - 1) as f32) / columns as f32;
    let cell_height = (height - layout.gap * (rows - 1) as f32) / rows as f32;
    anyhow::ensure!(
        cell_width > 0.0 && cell_height > 0.0,
        "Morph layout gap leaves no room for its layers"
    );
    Ok((0..count)
        .map(|index| {
            [
                x + (index % columns) as f32 * (cell_width + layout.gap),
                y + (index / columns) as f32 * (cell_height + layout.gap),
                cell_width,
                cell_height,
            ]
        })
        .collect())
}

pub(super) fn assemble(stage: LayerStage, body: &str) -> String {
    match stage {
        LayerStage::Filter => [FRAME, SAMPLE, body].join("\n"),
        LayerStage::Morph | LayerStage::Wallpaper => [FRAME, body].join("\n"),
    }
}

fn validate_abi(stage: LayerStage, source: &str, fragment_only: bool, bitmap: bool) -> Result<()> {
    let module = naga::front::wgsl::parse_str(source)
        .map_err(|error| anyhow::anyhow!("{}", error.emit_to_string(source)))?;
    anyhow::ensure!(
        module.entry_points.len() == if fragment_only { 1 } else { 2 },
        "Layer defines an extra entry point"
    );
    let expected_bindings = if stage == LayerStage::Filter {
        4
    } else if fragment_only {
        6
    } else if bitmap {
        3
    } else {
        1
    };
    anyhow::ensure!(
        module
            .global_variables
            .iter()
            .filter(|(_, item)| item.binding.is_some())
            .count()
            == expected_bindings,
        "Layer defines an extra resource binding"
    );
    naga::valid::Validator::new(
        naga::valid::ValidationFlags::all(),
        naga::valid::Capabilities::empty(),
    )
    .validate(&module)
    .map_err(|error| anyhow::anyhow!("{}", error.emit_to_string(source)))?;
    Ok(())
}

fn read_body(stage: LayerStage, name: &str) -> Result<String> {
    if stage == LayerStage::Morph
        && let Some(text) = name.strip_prefix("text=")
    {
        return super::layer_text::body(text);
    }
    match (stage, name) {
        (LayerStage::Filter, "crt") => return Ok(include_str!("../shaders/layers/crt.wgsl").into()),
        (LayerStage::Filter, "mono") => {
            return Ok(include_str!("../shaders/layers/mono.wgsl").into());
        }
        (LayerStage::Filter, "parallax") => {
            return Ok(include_str!("../shaders/layers/parallax.wgsl").into());
        }
        (LayerStage::Filter, "pan" | "auto-pan") => {
            return Ok(include_str!("../shaders/layers/pan.wgsl").into());
        }
        (LayerStage::Morph, "spectrum") => {
            return Ok(include_str!("../shaders/layers/spectrum.wgsl").into());
        }
        (LayerStage::Morph, "pulse") => {
            return Ok(include_str!("../shaders/layers/pulse.wgsl").into());
        }
        (LayerStage::Morph, "audio-orbit") => {
            return Ok(include_str!("../shaders/layers/audio_orbit.wgsl").into());
        }
        (LayerStage::Morph, "rain") => {
            return Ok(include_str!("../shaders/layers/rain.wgsl").into());
        }
        (LayerStage::Morph, "snow") => {
            return Ok(include_str!("../shaders/layers/snow.wgsl").into());
        }
        (LayerStage::Morph, "fog") => return Ok(include_str!("../shaders/layers/fog.wgsl").into()),
        (LayerStage::Morph, "lightning") => {
            return Ok(include_str!("../shaders/layers/lightning.wgsl").into());
        }
        (LayerStage::Morph, "heart") => return Ok(HEART.into()),
        (LayerStage::Morph, "pointer") => {
            return Ok("fn layer(uv: vec2<f32>) -> vec4<f32> { return vec4<f32>(0.0); }".into());
        }
        _ => {}
    }
    let path = Path::new(name);
    if path.is_absolute() {
        anyhow::ensure!(
            is_fragment_file(path) || path.extension().is_some_and(|ext| ext == "wgsl"),
            "Expected a .wgsl, .glsl or .frag file"
        );
        return read_shader(path);
    }
    anyhow::ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        "Use a layer name or an absolute .wgsl file path"
    );
    let directory = match stage {
        LayerStage::Filter | LayerStage::Wallpaper => "shaders",
        LayerStage::Morph => "morphs",
    };
    let root = dirs::config_dir()
        .context("Missing configuration directory")?
        .join("kaleidux");
    let file = format!("{name}.wgsl");
    match read_shader(&root.join(directory).join(&file)) {
        Err(error)
            if stage == LayerStage::Filter
                && error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io_error| io_error.kind() == std::io::ErrorKind::NotFound) =>
        {
            read_shader(&root.join("filters").join(file))
        }
        source => source,
    }
}

pub(crate) fn is_shader_file(path: &Path) -> bool {
    is_fragment_file(path)
        || path.extension().is_some_and(|ext| ext == "wgsl")
        || super::layer_shadertoy::is_manifest(path)
}

pub(crate) fn is_fragment_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("glsl" | "frag" | "fs" | "fsh")
    )
}

fn preset_paths(path: &Path) -> Result<Vec<std::path::PathBuf>> {
    let source = read_shader(path)?;
    let root = path
        .parent()
        .context("GLSL preset has no parent directory")?;
    let mut fields = std::collections::HashMap::new();
    for line in source
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        anyhow::ensure!(
            !line.starts_with("#reference"),
            "Inherited GLSL presets need to be flattened"
        );
        if line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .context("Expected GLSL preset key = value")?;
        anyhow::ensure!(
            fields
                .insert(key.trim(), value.trim().trim_matches('"'))
                .is_none(),
            "Duplicate GLSL preset key {key:?}"
        );
    }
    let count: usize = fields
        .remove("shaders")
        .context("GLSL preset needs a shader count")?
        .parse()?;
    anyhow::ensure!(
        (1..=MAX_LAYERS_PER_STAGE).contains(&count),
        "Invalid GLSL preset shader count"
    );
    let mut passes = Vec::with_capacity(count);
    for index in 0..count {
        let entry = fields
            .remove(format!("shader{index}").as_str())
            .context("GLSL preset is missing a pass")?;
        let pass = root.join(entry).canonicalize()?;
        anyhow::ensure!(
            is_fragment_file(&pass),
            "GLSL preset pass must be a fragment shader"
        );
        for (prefix, supported) in [
            ("scale_type", "source"),
            ("scale", "1.0"),
            ("filter_linear", "true"),
            ("wrap_mode", "clamp_to_edge"),
        ] {
            if let Some(value) = fields.remove(format!("{prefix}{index}").as_str()) {
                let supported_scale =
                    prefix == "scale" && value.parse::<f32>().is_ok_and(|scale| scale == 1.0);
                anyhow::ensure!(
                    value == supported || supported_scale,
                    "Unsupported GLSL preset setting {prefix}{index} = {value}"
                );
            }
        }
        passes.push(pass);
    }
    anyhow::ensure!(
        fields.is_empty(),
        "Unsupported GLSL preset settings {:?}",
        fields.keys()
    );
    Ok(passes)
}

fn read_shader(path: &Path) -> Result<String> {
    read_bounded(path, MAX_SHADER_BYTES).context("Read shader source")
}

pub(super) fn read_bounded(path: &Path, max_bytes: usize) -> Result<String> {
    // Non-regular inputs can keep a worker occupied indefinitely.
    // O_NONBLOCK also covers replacement with a FIFO between path lookup and open.
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= max_bytes as u64,
        "Shader source must be a regular file no larger than {max_bytes} bytes"
    );
    let mut source = String::new();
    file.take((max_bytes + 1) as u64)
        .read_to_string(&mut source)?;
    anyhow::ensure!(
        source.len() <= max_bytes,
        "Shader exceeds {max_bytes} bytes"
    );
    Ok(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_layers_obey_the_fragment_abi() {
        for (stage, name) in [
            (LayerStage::Filter, "crt"),
            (LayerStage::Filter, "mono"),
            (LayerStage::Filter, "pan"),
            (LayerStage::Filter, "parallax|intensity=0.8"),
            (LayerStage::Morph, "spectrum"),
            (LayerStage::Morph, "audio-orbit"),
            (LayerStage::Morph, "rain"),
            (LayerStage::Morph, "snow"),
            (LayerStage::Morph, "fog"),
            (LayerStage::Morph, "lightning"),
            (LayerStage::Morph, "heart"),
            (LayerStage::Morph, "pulse"),
            (LayerStage::Morph, "text=Kaleidux"),
        ] {
            prepare_layer(stage, name).unwrap();
        }
    }

    #[test]
    fn shadertoy_fragment_uses_the_layer_layout() {
        let source = super::super::layer_glsl::translate(
            LayerStage::Filter,
            "void mainImage(out vec4 colour, in vec2 xy) { colour = texture(iChannel0, xy / iResolution.xy); }",
        )
        .unwrap();
        validate_abi(LayerStage::Filter, &source, true, false).unwrap();
    }

    #[test]
    fn shader_cannot_add_bindings() {
        let body = "@group(0) @binding(1) var x: sampler;
                    fn layer(uv: vec2<f32>) -> vec4<f32> { return vec4<f32>(uv, 0.0, 1.0); }";
        assert!(
            validate_abi(
                LayerStage::Morph,
                &assemble(LayerStage::Morph, body),
                false,
                false
            )
            .is_err()
        );
    }
}
