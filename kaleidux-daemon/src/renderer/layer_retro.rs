//! Runs local RetroArch Slang presets through librashader on the renderer's device.

use super::WgpuContext;
use super::layer_pipeline::FILTER_FORMAT;
use super::layer_source::{
    MAX_LAYER_TEXTURE_BYTES, PreparedLayer, PreparedLayers, assemble, read_bounded,
};
use anyhow::{Context, Result, ensure};
use kaleidux_common::LayerStage;
use librashader_common::{Size, Viewport};
use librashader_pack::{LoadableResource, PassResource, ShaderPresetPack, TextureResource};
use librashader_presets::{PassMeta, ShaderFeatures, ShaderPreset};
use librashader_runtime::scaling::ViewportSize;
use librashader_runtime_wgpu::{FilterChainWgpu, WgpuOutputView};
use std::path::Path;
use std::sync::Arc;

const SHADER_BYTES: usize = 512 * 1024;
const PASS_LIMIT: usize = 32;
const GPU_BUDGET: u64 = 512 * 1024 * 1024;
pub(super) const ENCODED_FORMAT: wgpu::TextureFormat = wgpu::TextureFormat::Rgba8Unorm;

pub(crate) struct PreparedPreset {
    pack: ShaderPresetPack,
    history: u64,
}

pub(super) struct Retro {
    chain: FilterChainWgpu,
    passes: Vec<PassBudget>,
    history: u64,
    size: Option<[u32; 2]>,
    retained_bytes: u64,
    bypass: bool,
    frame: usize,
}

struct PassBudget {
    meta: PassMeta,
    bytes_per_texel: u64,
}

pub(super) fn prepare(path: &Path, fps: u16) -> Result<PreparedLayers> {
    read_bounded(path, SHADER_BYTES).context("Read Slang preset")?;
    let preset =
        ShaderPreset::try_parse(path, ShaderFeatures::empty()).context("Read Slang preset")?;
    ensure!(
        (1..=PASS_LIMIT).contains(&preset.passes.len())
            && preset.pass_count as usize == preset.passes.len()
            && preset.textures.len() <= PASS_LIMIT,
        "Slang preset exceeds the pass or texture limit"
    );
    let mut passes = Vec::with_capacity(preset.passes.len());
    let mut history = 0;
    for pass in preset.passes {
        read_bounded(&pass.path, SHADER_BYTES).context("Read Slang shader")?;
        let data = PassMeta::load(&pass.path, preset.features).context("Load Slang shader")?;
        ensure!(
            data.vertex.len() + data.fragment.len() <= 4 * SHADER_BYTES,
            "Expanded Slang shader is too large"
        );
        history = history
            .max(original_history_count(&data.fragment).max(original_history_count(&data.vertex)));
        passes.push(PassResource {
            data,
            meta: pass.meta,
        });
    }
    ensure!(
        history <= PASS_LIMIT as u64,
        "Slang preset requests too many history frames"
    );
    let mut textures = Vec::with_capacity(preset.textures.len());
    let mut texture_bytes = 0;
    for texture in preset.textures {
        let bitmap = super::layer_bitmap::read(&texture.path)?;
        texture_bytes += bitmap.as_raw().len();
        ensure!(
            texture_bytes <= MAX_LAYER_TEXTURE_BYTES,
            "Slang textures exceed 96 MiB"
        );
        textures.push(TextureResource {
            data: bitmap.into(),
            meta: texture.meta,
        });
    }
    let name = path.to_string_lossy().into_owned();
    Ok(PreparedLayers {
        stage: LayerStage::Filter,
        fps,
        source: Some(name.clone()),
        shaders: vec![PreparedLayer {
            name,
            source: assemble(
                LayerStage::Filter,
                "fn layer(uv: vec2<f32>) -> vec4<f32> { return sample(uv); }",
            ),
            fragment_only: false,
            assets: [None, None, None, None],
            rectangle: None,
            actions: Default::default(),
            track_pointer: false,
            track_audio: false,
        }],
        feedback: None,
        retro: Some(PreparedPreset {
            pack: ShaderPresetPack {
                pass_count: preset.pass_count,
                passes,
                textures,
                parameters: preset.parameters,
            },
            history,
        }),
    })
}

impl Retro {
    pub(super) fn compile(ctx: &WgpuContext, preset: &PreparedPreset) -> Result<Self> {
        let passes = preset
            .pack
            .passes
            .iter()
            .map(|pass| {
                let format: Option<wgpu::TextureFormat> = pass
                    .meta
                    .get_format_override()
                    .unwrap_or(pass.data.format)
                    .into();
                let bytes_per_texel =
                    format.map_or(4, |format| format.block_copy_size(None).unwrap_or(16));
                PassBudget {
                    meta: pass.meta.clone(),
                    bytes_per_texel: u64::from(bytes_per_texel),
                }
            })
            .collect::<Vec<_>>();
        target_bytes(
            &passes,
            preset.history,
            [1, 1],
            ctx.device.limits().max_texture_dimension_2d,
        )
        .context("Slang preset exceeds the GPU texture budget")?;
        let mut encoder = ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Slang preset initialization"),
            });
        let mut chain = FilterChainWgpu::load_from_pack_deferred(
            preset.pack.clone(),
            ctx.device.clone(),
            ctx.queue.clone(),
            &mut encoder,
            None,
        )
        .context("Compile Slang preset")?;
        let submitted = ctx.submit([encoder.finish()]);
        ctx.device
            .poll(wgpu::Maintain::WaitForSubmissionIndex(submitted));

        // The final output pipeline is built lazily by the runtime.
        // Validate it before the candidate replaces the current preset.
        let input = Arc::new(target_texture(ctx, [1, 1]));
        let output = target_texture(ctx, [1, 1]).create_view(&Default::default());
        let mut encoder = ctx.device.create_command_encoder(&Default::default());
        ctx.with_raw_queue_lock(|| {
            chain.frame(input, &viewport(&output, [1, 1]), &mut encoder, 0, None)
        })
        .context("Validate Slang preset output")?;
        drop(encoder.finish());
        Ok(Self {
            chain,
            passes,
            history: preset.history,
            size: None,
            retained_bytes: 0,
            bypass: false,
            frame: 0,
        })
    }

    pub(super) fn render(
        &mut self,
        ctx: &WgpuContext,
        encoder: &mut wgpu::CommandEncoder,
        input: &Arc<wgpu::Texture>,
        output: &wgpu::TextureView,
        size: [u32; 2],
    ) -> bool {
        if self.size != Some(size) {
            self.size = Some(size);
            let bytes = target_bytes(
                &self.passes,
                self.history,
                size,
                ctx.device.limits().max_texture_dimension_2d,
            );
            self.bypass = bytes.is_none();
            if let Some(bytes) = bytes {
                self.retained_bytes = bytes - u64::from(size[0]) * u64::from(size[1]) * 8;
            } else {
                tracing::warn!(
                    "Slang preset exceeds the GPU texture budget at {}x{}",
                    size[0],
                    size[1]
                );
            }
        }
        if self.bypass {
            return false;
        }
        let result = ctx.with_raw_queue_lock(|| {
            self.chain.frame(
                input.clone(),
                &viewport(output, size),
                encoder,
                self.frame,
                None,
            )
        });
        if let Err(error) = result {
            self.bypass = true;
            tracing::warn!("Slang preset render failed: {error}");
            return false;
        }
        self.frame = self.frame.wrapping_add(1);
        true
    }

    pub(super) fn retained_texture_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

pub(super) fn target_texture(ctx: &WgpuContext, [width, height]: [u32; 2]) -> wgpu::Texture {
    ctx.device.create_texture(&wgpu::TextureDescriptor {
        label: Some("Slang input or output"),
        size: wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: ENCODED_FORMAT,
        usage: wgpu::TextureUsages::RENDER_ATTACHMENT
            | wgpu::TextureUsages::TEXTURE_BINDING
            | wgpu::TextureUsages::COPY_SRC,
        view_formats: &[FILTER_FORMAT],
    })
}

fn viewport(
    output: &wgpu::TextureView,
    [width, height]: [u32; 2],
) -> Viewport<'_, WgpuOutputView<'_>> {
    let size = Size::new(width, height);
    Viewport {
        x: 0.0,
        y: 0.0,
        mvp: None,
        output: WgpuOutputView::new_from_raw(output, size, ENCODED_FORMAT),
        size,
    }
}

fn target_bytes(
    passes: &[PassBudget],
    history: u64,
    [width, height]: [u32; 2],
    max_axis: u32,
) -> Option<u64> {
    let viewport = Size::new(width, height);
    let mut bytes = u64::from(width) * u64::from(height) * 4 * (history + 2);
    let mut size = viewport;
    for (index, pass) in passes.iter().enumerate() {
        size = size.scale_viewport(pass.meta.scaling.clone(), viewport, viewport);
        if size.width > max_axis || size.height > max_axis {
            return None;
        }
        let target = u64::from(size.width) * u64::from(size.height) * pass.bytes_per_texel;
        bytes += 2 * target;
        if passes
            .get(index + 1)
            .is_some_and(|pass| pass.meta.mipmap_input)
        {
            bytes += target.div_ceil(3) * 2;
        }
    }
    (bytes <= GPU_BUDGET).then_some(bytes)
}

fn original_history_count(shader: &str) -> u64 {
    shader
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter_map(|word| word.strip_prefix("OriginalHistory"))
        .filter_map(|index| index.trim_end_matches("Size").parse::<u64>().ok())
        .max()
        .unwrap_or_default()
}
