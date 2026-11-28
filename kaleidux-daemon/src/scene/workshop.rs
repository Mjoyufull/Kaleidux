//! Loads static image layers from a Wallpaper Engine package or unpacked project.

use super::{MAX_SCENE_BYTES, package::Package, push_layer, read_local, texture};
use crate::renderer::layer_source::{PreparedLayer, prepare_bitmap_layer};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::path::{Path, PathBuf};

const MAX_ASSET_BYTES: usize = 64 * 1024 * 1024;

enum Assets {
    Package(Package),
    Directory(PathBuf),
}

impl Assets {
    fn asset(&self, path: &str) -> Option<PathBuf> {
        let Self::Directory(root) = self else {
            return None;
        };
        let path = root.join(path.replace('\\', "/")).canonicalize().ok()?;
        (path.starts_with(root) && path.is_file()).then_some(path)
    }

    fn has(&self, path: &str) -> bool {
        match self {
            Self::Package(package) => package.has(path),
            Self::Directory(_) => self.asset(path).is_some(),
        }
    }

    fn read(&mut self, path: &str, limit: usize) -> Result<Vec<u8>> {
        match self {
            Self::Package(package) => package.read(path, limit),
            Self::Directory(_) => {
                let path = self
                    .asset(path)
                    .context("Scene asset is outside the project or absent")?;
                read_local(&path, limit as u64)
            }
        }
    }

    fn json(&mut self, path: &str) -> Result<Value> {
        Ok(serde_json::from_slice(
            &self.read(path, MAX_SCENE_BYTES as usize)?,
        )?)
    }
}

pub(super) fn load(path: &Path) -> Result<Vec<PreparedLayer>> {
    let (mut assets, scene) = if path.extension().is_some_and(|ext| ext == "pkg") {
        let mut package = Package::open(path)?;
        let scene = package.json("scene.json")?;
        (Assets::Package(package), scene)
    } else {
        let root = path
            .parent()
            .context("Scene has no project directory")?
            .canonicalize()?;
        let scene = serde_json::from_slice(&read_local(path, MAX_SCENE_BYTES)?)?;
        (Assets::Directory(root), scene)
    };
    let projection = &scene["general"]["orthogonalprojection"];
    let width = dimension(&projection["width"], 1920.0);
    let height = dimension(&projection["height"], 1080.0);
    let objects = scene["objects"]
        .as_array()
        .context("Scene needs an object list")?;
    let mut layers = Vec::new();
    for object in objects {
        let Some(source) = field(&object["image"]).as_str() else {
            continue;
        };
        let alpha = number(&object["alpha"]).unwrap_or(1.0).clamp(0.0, 1.0);
        if field(&object["visible"]).as_bool() == Some(false) || alpha == 0.0 {
            continue;
        }
        let name = resolve_texture(&mut assets, source)?;
        let bytes = assets.read(&name, MAX_ASSET_BYTES)?;
        let image = texture::decode(&bytes)
            .with_context(|| format!("Unsupported scene texture {name:?}"))?;
        if let Some(layer) = sprite(name, image, object, [width, height], alpha)? {
            push_layer(&mut layers, layer)?;
        }
    }
    ensure!(!layers.is_empty(), "Scene has no supported image layers");
    Ok(layers)
}

fn resolve_texture(assets: &mut Assets, source: &str) -> Result<String> {
    let source = if source.to_ascii_lowercase().ends_with(".json") {
        let model = assets.json(source)?;
        let material = if let Some(path) = model["material"].as_str() {
            assets.json(path)?
        } else {
            model
        };
        material["passes"][0]["textures"][0]
            .as_str()
            .context("Scene material needs a base texture")?
            .to_owned()
    } else {
        source.to_owned()
    };
    [
        source.clone(),
        format!("{source}.tex"),
        format!("materials/{source}.tex"),
        format!("materials/{source}"),
    ]
    .into_iter()
    .find(|path| assets.has(path))
    .with_context(|| format!("Scene texture {source:?} was not found"))
}

fn sprite(
    name: String,
    bitmap: image::RgbaImage,
    object: &Value,
    [canvas_w, canvas_h]: [f64; 2],
    alpha: f64,
) -> Result<Option<PreparedLayer>> {
    let [x, y] = vector(&object["origin"]).unwrap_or([canvas_w * 0.5, canvas_h * 0.5]);
    let y = canvas_h - y;
    let [w, h] =
        vector(&object["size"]).unwrap_or([f64::from(bitmap.width()), f64::from(bitmap.height())]);
    let [sx, sy] = vector(&object["scale"]).unwrap_or([1.0, 1.0]);
    let (w, h) = (w * sx, h * sy);
    if w.abs() < 0.0001 || h.abs() < 0.0001 {
        return Ok(None);
    }
    let angle = vector::<3>(&object["angles"]).map_or(0.0, |angles| angles[2]);
    let (sin, cos) = (-angle.to_radians()).sin_cos();
    let ex = (w * cos).abs() * 0.5 + (h * sin).abs() * 0.5;
    let ey = (w * sin).abs() * 0.5 + (h * cos).abs() * 0.5;
    let left = (x - ex).clamp(0.0, canvas_w);
    let right = (x + ex).clamp(0.0, canvas_w);
    let top = (y - ey).clamp(0.0, canvas_h);
    let bottom = (y + ey).clamp(0.0, canvas_h);
    let (clip_w, clip_h) = (right - left, bottom - top);
    if clip_w <= 0.0 || clip_h <= 0.0 {
        return Ok(None);
    }
    let rect = [
        left / canvas_w,
        top / canvas_h,
        clip_w / canvas_w,
        clip_h / canvas_h,
    ]
    .map(|v| v as f32);
    let [r, g, b] = vector(&object["color"])
        .unwrap_or([1.0; 3])
        .map(|channel| channel.clamp(0.0, 1.0));
    let (dx, dy) = (left - x, top - y);
    let body = format!(
        r#"
@group(0) @binding(1) var art: texture_2d<f32>;
@group(0) @binding(2) var art_sampler: sampler;
fn layer(uv: vec2<f32>) -> vec4<f32> {{
    let p = vec2<f32>({dx}, {dy}) + uv * vec2<f32>({clip_w}, {clip_h});
    let c = vec2<f32>({cos}, {sin});
    let q = vec2<f32>(dot(p, c), dot(p, vec2<f32>(-c.y, c.x))) / vec2<f32>({w}, {h}) + 0.5;
    let rgba = textureSample(art, art_sampler, q);
    if any(q < vec2<f32>(0.0)) || any(q > vec2<f32>(1.0)) {{ return vec4<f32>(0.0); }}
    return rgba * vec4<f32>({r}, {g}, {b}, {alpha});
}}
"#
    );
    Ok(Some(prepare_bitmap_layer(name, bitmap, Some(rect), &body)?))
}

fn field(value: &Value) -> &Value {
    value.get("value").unwrap_or(value)
}

fn number(value: &Value) -> Option<f64> {
    let value = field(value);
    let n: f64 = value.as_f64().or_else(|| value.as_str()?.parse().ok())?;
    (n.is_finite() && n.abs() <= 1.0e9).then_some(n)
}

fn dimension(value: &Value, default: f64) -> f64 {
    number(value).filter(|axis| *axis > 0.0).unwrap_or(default)
}

fn vector<const N: usize>(value: &Value) -> Option<[f64; N]> {
    let value = field(value);
    let mut result = [0.0; N];
    if let Some(text) = value.as_str() {
        let mut words = text.split_whitespace();
        for result in &mut result {
            *result = words.next()?.parse().ok()?;
        }
    } else {
        for (index, result) in result.iter_mut().enumerate() {
            *result = number(value.get(index)?)?;
        }
    }
    result
        .iter()
        .all(|n| n.is_finite() && n.abs() <= 1.0e9)
        .then_some(result)
}
