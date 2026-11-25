//! Loads local scene descriptions and the supported Wallpaper Engine image layers.

mod package;
mod texture;
mod workshop;

use crate::renderer::layer_source::{PreparedLayer, PreparedLayers, prepare_layer};
use anyhow::{Context, Result};
use kaleidux_common::{LAYER_FPS_UNLIMITED, LayerStage, MAX_LAYERS_PER_STAGE};
use serde::Deserialize;
use serde_json::Value;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const MAX_SCENE_BYTES: u64 = 1024 * 1024;

pub(crate) fn is_scene(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.ends_with(".kldscene.toml") || name.ends_with(".pkg") || name == "scene.json"
        })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scene {
    background: Option<String>,
    fps: Option<u16>,
    #[serde(default)]
    layers: Vec<String>,
}

pub(crate) fn open(path: &Path) -> Result<PreparedLayers> {
    let (shaders, fps) = if path
        .file_name()
        .is_some_and(|name| name.as_encoded_bytes().ends_with(b".kldscene.toml"))
    {
        let input = read_local(path, MAX_SCENE_BYTES)?;
        let scene: Scene = toml::from_str(std::str::from_utf8(&input)?)?;
        let root = path
            .parent()
            .context("Scene needs a containing directory")?;
        let mut layers = Vec::new();
        if let Some(background) = scene.background {
            push_layer(&mut layers, scene_layer(root, &background, true)?)?;
        }
        for layer in scene.layers {
            push_layer(&mut layers, scene_layer(root, &layer, false)?)?;
        }
        let fps = scene.fps.unwrap_or_else(|| {
            if layers
                .iter()
                .any(|layer| layer.fragment_only || layer.name.ends_with(".wgsl"))
            {
                LAYER_FPS_UNLIMITED
            } else {
                0
            }
        });
        (layers, fps)
    } else {
        (workshop::load(path)?, 0)
    };
    anyhow::ensure!(
        !shaders.is_empty() && shaders.len() <= MAX_LAYERS_PER_STAGE,
        "Scene must contain 1–{MAX_LAYERS_PER_STAGE} layers"
    );
    Ok(PreparedLayers {
        stage: LayerStage::Wallpaper,
        fps,
        shaders,
        source: Some(path.to_string_lossy().into_owned()),
        feedback: None,
        #[cfg(feature = "retroarch-shaders")]
        retro: None,
    })
}

fn push_layer(layers: &mut Vec<PreparedLayer>, layer: PreparedLayer) -> Result<()> {
    anyhow::ensure!(
        layers.len() < MAX_LAYERS_PER_STAGE,
        "Scene has too many layers"
    );
    let retained: usize = layers
        .iter()
        .chain(std::iter::once(&layer))
        .map(PreparedLayer::texture_bytes)
        .sum();
    anyhow::ensure!(
        retained <= crate::renderer::layer_source::MAX_LAYER_TEXTURE_BYTES,
        "Scene textures exceed 96 MiB"
    );
    layers.push(layer);
    Ok(())
}

fn scene_layer(root: &Path, spec: &str, background: bool) -> Result<PreparedLayer> {
    let (base, options) = spec.split_once('|').unwrap_or((spec, ""));
    let (prefix, path) = if let Some(path) = base.strip_prefix("image=") {
        ("image=", Some(path))
    } else if crate::renderer::is_shader_file(Path::new(base)) {
        ("", Some(base))
    } else if background {
        ("image=", Some(base))
    } else {
        ("", None)
    };
    let resolved = path.map_or_else(
        || base.to_owned(),
        |path| format!("{prefix}{}", root.join(path).display()),
    );
    let mut layer = prepare_layer(
        LayerStage::Morph,
        &if options.is_empty() {
            resolved
        } else {
            format!("{resolved}|{options}")
        },
    )?;
    if background {
        layer.rectangle = None;
    }
    Ok(layer)
}

fn read_local(path: &Path, max: u64) -> Result<Vec<u8>> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    anyhow::ensure!(
        meta.is_file() && meta.len() <= max,
        "Scene source is not a bounded regular file"
    );
    let mut bytes = Vec::new();
    file.take(max + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= max,
        "Scene source exceeds its size limit"
    );
    Ok(bytes)
}

pub(crate) fn workshop_path(path: &Path) -> Result<PathBuf> {
    let path = if path
        .to_str()
        .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
    {
        let home = dirs::home_dir().context("Could not determine the Steam home directory")?;
        [home.join(".local/share/Steam"), home.join(".steam/steam")]
            .into_iter()
            .map(|root| root.join("steamapps/workshop/content/431960").join(path))
            .find(|candidate| candidate.is_dir())
            .context("Workshop item was not found")?
    } else {
        path.to_owned()
    };
    let project_path = if path.is_dir() {
        path.join("project.json")
    } else {
        path
    };
    if project_path
        .file_name()
        .is_some_and(|name| name == "project.json")
    {
        let project: Value = serde_json::from_slice(&read_local(&project_path, MAX_SCENE_BYTES)?)?;
        let root = project_path
            .parent()
            .context("Workshop project has no directory")?
            .canonicalize()?;
        let file = project["file"]
            .as_str()
            .context("Workshop project has no entry file")?;
        let entry = root.join(file).canonicalize()?;
        anyhow::ensure!(
            entry.starts_with(&root) && entry.is_file(),
            "Workshop entry escapes its project"
        );
        Ok(entry)
    } else {
        Ok(project_path)
    }
}
