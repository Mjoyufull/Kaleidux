//! Reads bounded local layer sources and validates their ABI before rendering uses them.

use anyhow::{Context, Result};
use kaleidux_common::{LayerSelection, LayerStage, MAX_LAYERS_PER_STAGE};
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
    pub(super) action: Option<String>,
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
    pub(crate) async fn load(selection: &LayerSelection) -> Result<Self> {
        anyhow::ensure!(
            selection.stage != LayerStage::Wallpaper || selection.layers.len() <= 1,
            "Choose one shader wallpaper",
        );
        anyhow::ensure!(
            selection.layers.len() <= MAX_LAYERS_PER_STAGE,
            "A stage supports at most {MAX_LAYERS_PER_STAGE} layers"
        );
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
            move || Self::read_and_validate(selection),
        )
        .await
        .context("Renderer workers are shutting down")?
        .await
        .context("Layer preparation did not complete")?
    }

    fn read_and_validate(selection: LayerSelection) -> Result<Self> {
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
        for name in &selection.layers {
            let path = Path::new(name);
            if selection.stage == LayerStage::Filter
                && path.extension().is_some_and(|ext| ext == "glslp")
            {
                for pass in preset_paths(path)? {
                    shaders.push(prepare_layer(selection.stage, &pass.to_string_lossy())?);
                }
            } else {
                shaders.push(
                    prepare_layer(selection.stage, name)
                        .with_context(|| format!("Invalid layer {name:?}"))?,
                );
            }
            anyhow::ensure!(
                shaders.len() <= MAX_LAYERS_PER_STAGE,
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
    let (name, options) = spec.split_once('|').unwrap_or((spec, ""));
    let shader_path = Path::new(name);
    let fragment_only = !["text=", "image=", "parallax="]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        && is_fragment_file(shader_path);
    let mut assets = [None, None, None, None];
    let mut rectangle = None;
    let mut action = None;
    let mut font = None;
    let mut colour = "#ffffff";
    for option in options.split('|').filter(|option| !option.is_empty()) {
        let (key, value) = option
            .split_once('=')
            .context("Expected layer option key=value")?;
        match key {
            "rect" if stage == LayerStage::Morph => rectangle = Some(parse_rectangle(value)?),
            "action" if stage == LayerStage::Morph => {
                anyhow::ensure!(
                    !value.is_empty()
                        && value.len() <= 64
                        && value.bytes().all(|byte| byte.is_ascii_alphanumeric()
                            || matches!(byte, b'-' | b'_' | b'.')),
                    "Morph action must be a short identifier"
                );
                action = Some(value.to_owned());
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
    let source = if fragment_only {
        super::layer_glsl::translate(stage, &body)?
    } else {
        assemble(stage, &body)
    };
    validate_abi(stage, &source, fragment_only, assets[0].is_some())?;
    anyhow::ensure!(
        action.is_none() || rectangle.is_some(),
        "A morph action needs a rectangle"
    );
    Ok(PreparedLayer {
        name: spec.to_owned(),
        source,
        fragment_only,
        assets,
        rectangle,
        action,
    })
}

pub(crate) fn prepare_rgba_layer(
    name: String,
    bitmap: image::RgbaImage,
    rectangle: Option<[f32; 4]>,
) -> Result<PreparedLayer> {
    let source = assemble(
        LayerStage::Morph,
        include_str!("../shaders/layers/bitmap.wgsl"),
    );
    validate_abi(LayerStage::Morph, &source, false, true)?;
    Ok(PreparedLayer {
        name,
        source,
        fragment_only: false,
        assets: [Some(bitmap), None, None, None],
        rectangle,
        action: None,
    })
}

fn parse_rectangle(value: &str) -> Result<[f32; 4]> {
    let coordinates: Vec<f32> = value.split(',').map(str::parse).collect::<Result<_, _>>()?;
    let result: [f32; 4] = coordinates
        .try_into()
        .map_err(|_| anyhow::anyhow!("Use rect=x,y,width,height"))?;
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
    Ok(result)
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
        (LayerStage::Filter, "pan") => return Ok(include_str!("../shaders/layers/pan.wgsl").into()),
        (LayerStage::Morph, "spectrum") => {
            return Ok(include_str!("../shaders/layers/spectrum.wgsl").into());
        }
        (LayerStage::Morph, "heart") => return Ok(HEART.into()),
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
            (LayerStage::Morph, "spectrum"),
            (LayerStage::Morph, "heart"),
            (LayerStage::Morph, "text=Kaleidux"),
        ] {
            let source = assemble(stage, &read_body(stage, name).unwrap());
            validate_abi(stage, &source, false, false).unwrap();
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
