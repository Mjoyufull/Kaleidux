//! Compiles validated layer sources and binds per-stage uniforms and intermediate targets.

use super::WgpuContext;
use super::layer_source::{HEART, PreparedLayers, assemble};
use kaleidux_common::LayerStage;
use std::time::{Duration, Instant};

pub(super) const FILTER_FORMAT: wgpu::TextureFormat = wgpu::TextureFormat::Rgba8UnormSrgb;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub(super) struct LayerFrame {
    pub(super) size: [f32; 2],
    pub(super) seconds: f32,
    pub(super) progress: f32,
    pub(super) params: [f32; 4],
}

pub(crate) struct LayerPipelines {
    pub(super) names: Vec<String>,
    pub(super) fps: u8,
    pub(super) start: Instant,
    pub(super) pipelines: Vec<wgpu::RenderPipeline>,
    layout: wgpu::BindGroupLayout,
    uniform: wgpu::Buffer,
    last_frame: Option<LayerFrame>,
    sampler: Option<wgpu::Sampler>,
    pub(super) morph_binding: Option<wgpu::BindGroup>,
}

impl LayerPipelines {
    pub(crate) async fn compile(
        ctx: &WgpuContext,
        format: wgpu::TextureFormat,
        prepared: &PreparedLayers,
    ) -> anyhow::Result<Option<Self>> {
        if prepared.shaders.is_empty() {
            return Ok(None);
        }
        ctx.device.push_error_scope(wgpu::ErrorFilter::Validation);
        let layers = Self::create(ctx, format, prepared);
        if let Some(error) = ctx.device.pop_error_scope().await {
            anyhow::bail!("Layer pipeline rejected by the graphics device: {error}");
        }
        Ok(Some(layers))
    }

    fn create(ctx: &WgpuContext, format: wgpu::TextureFormat, prepared: &PreparedLayers) -> Self {
        let filter = prepared.stage == LayerStage::Filter;
        let (layout, uniform, sampler) = resources(ctx, filter);
        let pipeline_layout = ctx
            .device
            .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                label: Some("Wallpaper layer layout"),
                bind_group_layouts: &[&layout],
                push_constant_ranges: &[],
            });
        let pipelines = prepared
            .shaders
            .iter()
            .enumerate()
            .map(|(i, layer)| {
                let target = if filter && i + 1 < prepared.shaders.len() {
                    FILTER_FORMAT
                } else {
                    format
                };
                pipeline(ctx, &pipeline_layout, target, &layer.source, filter)
            })
            .collect();
        let morph_binding = (!filter).then(|| {
            ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("Morph uniforms"),
                layout: &layout,
                entries: &[uniform_entry(&uniform)],
            })
        });
        Self {
            names: prepared
                .shaders
                .iter()
                .map(|layer| layer.name.clone())
                .collect(),
            fps: prepared.fps,
            start: Instant::now(),
            pipelines,
            layout,
            uniform,
            last_frame: None,
            sampler,
            morph_binding,
        }
    }

    pub(super) fn love(ctx: &WgpuContext, format: wgpu::TextureFormat) -> Self {
        let prepared = PreparedLayers {
            stage: LayerStage::Morph,
            fps: 30,
            shaders: vec![super::layer_source::PreparedLayer {
                name: "heart".into(),
                source: assemble(LayerStage::Morph, HEART),
            }],
        };
        Self::create(ctx, format, &prepared)
    }

    pub(super) fn frame_interval(&self) -> Option<Duration> {
        (self.fps != 0).then(|| Duration::from_secs_f64(1.0 / f64::from(self.fps)))
    }

    pub(super) fn write_frame(&mut self, ctx: &WgpuContext, frame: LayerFrame) {
        if self.last_frame != Some(frame) {
            ctx.write_buffer(&self.uniform, 0, bytemuck::bytes_of(&frame));
            self.last_frame = Some(frame);
        }
    }

    pub(super) fn filter_binding(
        &self,
        ctx: &WgpuContext,
        view: &wgpu::TextureView,
    ) -> wgpu::BindGroup {
        let sampler = self
            .sampler
            .as_ref()
            .expect("filter pipeline has a sampler");
        ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Wallpaper filter input"),
            layout: &self.layout,
            entries: &[
                uniform_entry(&self.uniform),
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: wgpu::BindingResource::TextureView(view),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: wgpu::BindingResource::Sampler(sampler),
                },
            ],
        })
    }
}

fn uniform_entry(uniform: &wgpu::Buffer) -> wgpu::BindGroupEntry<'_> {
    wgpu::BindGroupEntry {
        binding: 0,
        resource: uniform.as_entire_binding(),
    }
}

fn resources(
    ctx: &WgpuContext,
    filter: bool,
) -> (wgpu::BindGroupLayout, wgpu::Buffer, Option<wgpu::Sampler>) {
    let entry = |binding, ty| wgpu::BindGroupLayoutEntry {
        binding,
        ty,
        visibility: wgpu::ShaderStages::FRAGMENT,
        count: None,
    };
    let entries = [
        entry(
            0,
            wgpu::BindingType::Buffer {
                ty: wgpu::BufferBindingType::Uniform,
                has_dynamic_offset: false,
                min_binding_size: wgpu::BufferSize::new(std::mem::size_of::<LayerFrame>() as u64),
            },
        ),
        entry(
            1,
            wgpu::BindingType::Texture {
                sample_type: wgpu::TextureSampleType::Float { filterable: true },
                view_dimension: wgpu::TextureViewDimension::D2,
                multisampled: false,
            },
        ),
        entry(
            2,
            wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
        ),
    ];
    let layout = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Wallpaper layer bindings"),
            entries: if filter { &entries } else { &entries[..1] },
        });
    let uniform = ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("Wallpaper layer frame"),
        size: std::mem::size_of::<LayerFrame>() as u64,
        usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    let sampler = filter.then(|| {
        ctx.device.create_sampler(&wgpu::SamplerDescriptor {
            label: Some("Wallpaper filter sampler"),
            mag_filter: wgpu::FilterMode::Linear,
            min_filter: wgpu::FilterMode::Linear,
            ..Default::default()
        })
    });
    (layout, uniform, sampler)
}

fn pipeline(
    ctx: &WgpuContext,
    layout: &wgpu::PipelineLayout,
    format: wgpu::TextureFormat,
    wgsl: &str,
    filter: bool,
) -> wgpu::RenderPipeline {
    let shader = ctx
        .device
        .create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Wallpaper layer shader"),
            source: wgpu::ShaderSource::Wgsl(wgsl.into()),
        });
    ctx.device
        .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("Wallpaper layer pipeline"),
            layout: Some(layout),
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: Some("kld_vertex"),
                compilation_options: Default::default(),
                buffers: &[],
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: Some("kld_fragment"),
                compilation_options: Default::default(),
                targets: &[Some(wgpu::ColorTargetState {
                    format,
                    blend: if filter {
                        None
                    } else {
                        Some(wgpu::BlendState::ALPHA_BLENDING)
                    },
                    write_mask: wgpu::ColorWrites::ALL,
                })],
            }),
            primitive: Default::default(),
            depth_stencil: None,
            multisample: Default::default(),
            multiview: None,
            cache: None,
        })
}
