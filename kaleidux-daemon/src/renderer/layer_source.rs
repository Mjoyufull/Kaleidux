//! Reads bounded local layer sources and validates their ABI before rendering uses them.

use anyhow::{Context, Result};
use kaleidux_common::{LayerSelection, LayerStage, MAX_LAYERS_PER_STAGE};
use std::io::Read;
use std::path::Path;

const MAX_SHADER_BYTES: usize = 64 * 1024;
const FRAME: &str = include_str!("../shaders/layers/frame.wgsl");
const SAMPLE: &str = include_str!("../shaders/layers/sample.wgsl");
pub(super) const HEART: &str = include_str!("../shaders/layers/heart.wgsl");

pub(crate) struct PreparedLayers {
    pub(super) stage: LayerStage,
    pub(super) fps: u8,
    pub(super) shaders: Vec<PreparedLayer>,
}

pub(super) struct PreparedLayer {
    pub(super) name: String,
    pub(super) source: String,
}

impl PreparedLayers {
    pub(crate) fn is_empty(&self) -> bool {
        self.shaders.is_empty()
    }

    pub(crate) fn stage(&self) -> LayerStage {
        self.stage
    }

    /// Read and validate sources on a bounded worker before sending the IPC command.
    pub(crate) async fn load(selection: &LayerSelection) -> Result<Self> {
        anyhow::ensure!(selection.fps <= 60, "Layer FPS must be between 0 and 60");
        anyhow::ensure!(
            selection.layers.len() <= MAX_LAYERS_PER_STAGE,
            "A stage supports at most {MAX_LAYERS_PER_STAGE} layers"
        );
        if selection.layers.is_empty() {
            return Ok(Self {
                stage: selection.stage,
                fps: 0,
                shaders: Vec::new(),
            });
        }
        let selection = selection.clone();
        crate::background::spawn_blocking_tracked(
            crate::background::BackgroundWorkKind::RendererInit,
            move || Self::read_and_validate(selection),
        )
        .context("Renderer workers busy; try again shortly")?
        .await
        .context("Layer preparation did not complete")?
    }

    fn read_and_validate(selection: LayerSelection) -> Result<Self> {
        let shaders = selection
            .layers
            .iter()
            .map(|name| {
                let body = read_body(selection.stage, name)
                    .with_context(|| format!("Could not read layer {name:?}"))?;
                let source = assemble(selection.stage, &body);
                validate_abi(selection.stage, &source)
                    .with_context(|| format!("Invalid layer {name:?}"))?;
                Ok(PreparedLayer {
                    name: name.clone(),
                    source,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            stage: selection.stage,
            fps: selection.fps,
            shaders,
        })
    }
}

pub(super) fn assemble(stage: LayerStage, body: &str) -> String {
    match stage {
        LayerStage::Filter => [FRAME, SAMPLE, body].join("\n"),
        LayerStage::Morph => [FRAME, body].join("\n"),
    }
}

fn validate_abi(stage: LayerStage, source: &str) -> Result<()> {
    let module = naga::front::wgsl::parse_str(source)
        .map_err(|error| anyhow::anyhow!("{}", error.emit_to_string(source)))?;
    anyhow::ensure!(
        module.entry_points.len() == 2,
        "Layer defines an extra entry point"
    );
    let expected_bindings = if stage == LayerStage::Filter { 3 } else { 1 };
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
        (LayerStage::Morph, "heart") => return Ok(HEART.into()),
        _ => {}
    }
    let path = Path::new(name);
    if path.is_absolute() {
        anyhow::ensure!(
            path.extension().is_some_and(|ext| ext == "wgsl"),
            "Expected a .wgsl file"
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
        LayerStage::Filter => "filters",
        LayerStage::Morph => "morphs",
    };
    let root = dirs::config_dir().context("Missing configuration directory")?;
    read_shader(
        &root
            .join("kaleidux")
            .join(directory)
            .join(format!("{name}.wgsl")),
    )
}

fn read_shader(path: &Path) -> Result<String> {
    // Non-regular inputs can keep a worker occupied indefinitely.
    // O_NONBLOCK also covers replacement with a FIFO between path lookup and open.
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= MAX_SHADER_BYTES as u64,
        "Shader must be a regular file no larger than 64 KiB"
    );
    let mut source = String::new();
    file.take((MAX_SHADER_BYTES + 1) as u64)
        .read_to_string(&mut source)?;
    anyhow::ensure!(source.len() <= MAX_SHADER_BYTES, "Shader exceeds 64 KiB");
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
            (LayerStage::Morph, "heart"),
            (LayerStage::Morph, "text=Kaleidux"),
        ] {
            let source = assemble(stage, &read_body(stage, name).unwrap());
            validate_abi(stage, &source).unwrap();
        }
    }

    #[test]
    fn shader_cannot_add_bindings() {
        let body = "@group(0) @binding(1) var x: sampler;
                    fn layer(uv: vec2<f32>) -> vec4<f32> { return vec4<f32>(uv, 0.0, 1.0); }";
        assert!(validate_abi(LayerStage::Morph, &assemble(LayerStage::Morph, body)).is_err());
    }
}
