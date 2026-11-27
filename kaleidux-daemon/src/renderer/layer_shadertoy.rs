//! Loads Shadertoy exports with local texture channels and Buffer A–D.

use super::layer_source::{MAX_LAYER_TEXTURE_BYTES, PreparedLayer, read_bounded};
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::path::{Path, PathBuf};

const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_PASS_BYTES: usize = 128 * 1024;
pub(super) const IMAGE: usize = 4;

pub(crate) struct PreparedToy {
    pub(super) passes: Vec<PreparedPass>,
    pub(super) textures: Vec<PreparedTexture>,
}

pub(super) struct PreparedPass {
    pub(super) layer: PreparedLayer,
    pub(super) order: usize,
    pub(super) channels: [Channel; 4],
    pub(super) feedback: bool,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Channel {
    pub(super) source: ChannelSource,
    pub(super) nearest: bool,
    pub(super) repeat: bool,
}

#[derive(Clone, Copy, Default)]
pub(super) enum ChannelSource {
    #[default]
    Empty,
    Texture(usize),
    Buffer(usize),
}

pub(super) struct PreparedTexture {
    pub(super) bitmap: image::RgbaImage,
    pub(super) srgb: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Manifest {
    Wrapped {
        #[serde(rename = "Shader")]
        shader: Export,
    },
    Plain(Export),
}

#[derive(Deserialize)]
struct Export {
    renderpass: Vec<Pass>,
}

#[derive(Deserialize)]
struct Pass {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    name: String,
    code: String,
    #[serde(default)]
    inputs: Vec<Input>,
    #[serde(default)]
    outputs: Vec<Output>,
}

#[derive(Deserialize)]
struct Input {
    channel: usize,
    #[serde(alias = "type")]
    ctype: String,
    id: Option<Id>,
    #[serde(default)]
    src: String,
    filepath: Option<PathBuf>,
    #[serde(default)]
    sampler: Sampler,
}

#[derive(Deserialize)]
struct Output {
    id: Id,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Id {
    Number(u64),
    Name(String),
}

impl Id {
    fn key(&self) -> String {
        match self {
            Self::Number(value) => value.to_string(),
            Self::Name(value) => value.clone(),
        }
    }
}

#[derive(Default, Deserialize)]
struct Sampler {
    filter: Option<String>,
    wrap: Option<String>,
    vflip: Option<Switch>,
    srgb: Option<Switch>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Switch {
    Bool(bool),
    Text(String),
}

impl Switch {
    fn enabled(&self) -> bool {
        match self {
            Self::Bool(value) => *value,
            Self::Text(value) => value == "true",
        }
    }
}

pub(crate) fn is_manifest(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.as_encoded_bytes().ends_with(b".shadertoy.json"))
}

pub(super) fn load(path: &Path) -> Result<PreparedToy> {
    let source = read_bounded(path, MAX_MANIFEST_BYTES).context("Read Shadertoy export")?;
    let root = path.parent().context("Shadertoy export needs a parent")?;
    parse(&source, root)
}

fn parse(source: &str, root: &Path) -> Result<PreparedToy> {
    let export = match serde_json::from_str(source).context("Invalid Shadertoy export")? {
        Manifest::Wrapped { shader } | Manifest::Plain(shader) => shader,
    };
    ensure!(export.renderpass.len() <= 6, "Too many Shadertoy passes");
    let mut common = None;
    let mut passes = [None, None, None, None, None];
    let mut ids = std::collections::HashMap::new();
    for pass in export.renderpass {
        let order = match (pass.kind.as_str(), pass.name.as_str()) {
            ("common", _) => {
                ensure!(common.replace(pass.code).is_none(), "Duplicate Common pass");
                continue;
            }
            ("image", _) => IMAGE,
            ("buffer", "Buffer A") => 0,
            ("buffer", "Buffer B") => 1,
            ("buffer", "Buffer C") => 2,
            ("buffer", "Buffer D") => 3,
            _ => bail!("Unsupported Shadertoy pass {:?}", pass.name),
        };
        if order != IMAGE {
            let [output] = pass.outputs.as_slice() else {
                bail!("Each Shadertoy buffer needs one output ID");
            };
            ensure!(
                ids.insert(output.id.key(), order).is_none(),
                "Duplicate Shadertoy buffer ID"
            );
        }
        ensure!(
            passes[order].replace(pass).is_none(),
            "Duplicate Shadertoy pass"
        );
    }
    ensure!(
        passes[IMAGE].is_some(),
        "Shadertoy export needs an Image pass"
    );
    let mut texture_keys = Vec::new();
    let mut textures = Vec::new();
    let mut texture_bytes = 0;
    let mut feedback = [false; IMAGE];
    let mut prepared = Vec::new();
    for (order, pass) in passes
        .into_iter()
        .enumerate()
        .filter_map(|(order, pass)| pass.map(|pass| (order, pass)))
    {
        let mut channels = std::array::from_fn(|_| Channel::default());
        let mut seen = [false; 4];
        for input in pass.inputs {
            let index = input.channel;
            ensure!(
                index < 4 && !seen[index],
                "Invalid or duplicate Shadertoy channel"
            );
            seen[index] = true;
            let nearest = match input.sampler.filter.as_deref() {
                None | Some("linear") => false,
                Some("nearest") => true,
                Some(other) => bail!("Unsupported Shadertoy sampler filter {other:?}"),
            };
            let repeat = match input.sampler.wrap.as_deref() {
                None | Some("clamp") => false,
                Some("repeat") => true,
                Some(other) => bail!("Unsupported Shadertoy sampler wrap {other:?}"),
            };
            let vflip = input.sampler.vflip.as_ref().is_some_and(Switch::enabled);
            let srgb = input.sampler.srgb.as_ref().is_some_and(Switch::enabled);
            let source = match input.ctype.as_str() {
                "buffer" => {
                    let id = input.id.context("Shadertoy buffer channel needs an ID")?;
                    let buffer = *ids.get(&id.key()).context("Unknown Shadertoy buffer ID")?;
                    if buffer >= order {
                        feedback[buffer] = true;
                    }
                    ChannelSource::Buffer(buffer)
                }
                "texture" => {
                    let path = texture_path(root, &input)?;
                    let key = (path, vflip, srgb);
                    let texture =
                        if let Some(index) = texture_keys.iter().position(|item| *item == key) {
                            index
                        } else {
                            let mut bitmap = super::layer_bitmap::read(&key.0)?;
                            texture_bytes += bitmap.as_raw().len();
                            ensure!(
                                texture_bytes <= MAX_LAYER_TEXTURE_BYTES,
                                "Shadertoy textures exceed 96 MiB"
                            );
                            if vflip {
                                image::imageops::flip_vertical_in_place(&mut bitmap);
                            }
                            let index = textures.len();
                            textures.push(PreparedTexture { bitmap, srgb });
                            texture_keys.push(key);
                            index
                        };
                    ChannelSource::Texture(texture)
                }
                other => bail!("Unsupported Shadertoy channel type {other:?}"),
            };
            channels[index] = Channel {
                source,
                nearest,
                repeat,
            };
        }
        let code = common.as_deref().unwrap_or_default();
        ensure!(
            code.len() + pass.code.len() <= MAX_PASS_BYTES,
            "Shadertoy pass exceeds 128 KiB"
        );
        let source = super::layer_glsl::translate_shadertoy(
            &format!("{code}\n{}", pass.code),
            order != IMAGE,
        )
        .with_context(|| format!("Compile Shadertoy pass {:?}", pass.name))?;
        let track_audio =
            super::layer_source::uses_audio(code) || super::layer_source::uses_audio(&pass.code);
        let track_cursor =
            super::layer_source::uses_cursor(code) || super::layer_source::uses_cursor(&pass.code);
        prepared.push(PreparedPass {
            layer: PreparedLayer {
                name: if order == IMAGE {
                    "Image".into()
                } else {
                    pass.name
                },
                source,
                fragment_only: true,
                assets: [None, None, None, None],
                rectangle: None,
                actions: Default::default(),
                track_pointer: false,
                track_audio,
                track_cursor,
            },
            order,
            channels,
            feedback: false,
        });
    }
    for pass in prepared.iter_mut().filter(|pass| pass.order != IMAGE) {
        pass.feedback = feedback[pass.order];
    }
    Ok(PreparedToy {
        passes: prepared,
        textures,
    })
}

fn texture_path(root: &Path, input: &Input) -> Result<PathBuf> {
    let path = input.filepath.as_deref().unwrap_or_else(|| {
        let source = input.src.as_str();
        Path::new(if source.starts_with("/media/") {
            &source[1..]
        } else {
            source
        })
    });
    ensure!(
        !path.as_os_str().is_empty() && path.is_relative(),
        "Shadertoy textures need local relative paths"
    );
    let root = root.canonicalize()?;
    let path = root
        .join(path)
        .canonicalize()
        .context("Find local Shadertoy texture")?;
    ensure!(
        path.starts_with(root),
        "Shadertoy texture is outside the export directory"
    );
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_buffers_and_marks_previous_frame_reads() {
        let toy = parse(r#"{"Shader":{"renderpass":[
            {"type":"image","code":"void mainImage(out vec4 c, in vec2 p) { c = texture(iChannel2, p/iResolution.xy); }","inputs":[{"channel":2,"ctype":"buffer","id":"b"}]},
            {"type":"buffer","name":"Buffer B","code":"void mainImage(out vec4 c, in vec2 p) { c = texture(iChannel0, p/iResolution.xy); }","outputs":[{"id":"b"}],"inputs":[{"channel":0,"ctype":"buffer","id":"a"}]},
            {"type":"buffer","name":"Buffer A","code":"void mainImage(out vec4 c, in vec2 p) { c = texture(iChannel1, p/iResolution.xy) + decay(); }","outputs":[{"id":"a"}],"inputs":[{"channel":1,"ctype":"buffer","id":"b"}]},
            {"type":"common","code":"vec4 decay() { return vec4(0.1); }"}
        ]}}"#, Path::new(".")).unwrap();
        assert_eq!(
            toy.passes.iter().map(|pass| pass.order).collect::<Vec<_>>(),
            [0, 1, IMAGE]
        );
        assert!(!toy.passes[0].feedback);
        assert!(toy.passes[1].feedback);
    }
}
