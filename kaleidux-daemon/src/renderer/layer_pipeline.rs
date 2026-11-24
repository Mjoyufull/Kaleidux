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
    pub(super) timing: [f32; 4],
    pub(super) signals: super::layers::Signals,
}

pub(crate) struct LayerPipelines {
    pub(crate) names: Vec<String>,
    pub(super) fps: u16,
    pub(super) start: Instant,
    pub(super) pipelines: Vec<wgpu::RenderPipeline>,
    pub(super) wallpaper_intermediate: Vec<wgpu::RenderPipeline>,
    pub(crate) source: Option<String>,
    layout: wgpu::BindGroupLayout,
    uniform: wgpu::Buffer,
    last_frame: Option<LayerFrame>,
    frame_number: u32,
    sampler: Option<wgpu::Sampler>,
    depth: Option<wgpu::TextureView>,
    pub(super) morph_bindings: Vec<wgpu::BindGroup>,
    pub(super) rectangles: Vec<Option<[f32; 4]>>,
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
        let has_bitmap = prepared.shaders.iter().any(|layer| layer.bitmap.is_some());
        let (layout, uniform, sampler) = resources(ctx, filter, has_bitmap);
        let depth = filter.then(|| {
            let source = prepared
                .shaders
                .iter()
                .find_map(|layer| layer.bitmap.as_ref());
            asset_view(ctx, source, wgpu::TextureFormat::Rgba8Unorm)
        });
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
                pipeline(ctx, &pipeline_layout, target, layer, filter)
            })
            .collect();
        let morph_bindings = if filter {
            Vec::new()
        } else {
            prepared
                .shaders
                .iter()
                .map(|layer| {
                    let mut entries = vec![uniform_entry(&uniform)];
                    let view = sampler.as_ref().map(|sampler| {
                        let view = asset_view(ctx, layer.bitmap.as_ref(), FILTER_FORMAT);
                        (view, sampler)
                    });
                    if let Some((view, sampler)) = &view {
                        entries.extend([
                            texture_entry(1, view),
                            wgpu::BindGroupEntry {
                                binding: 2,
                                resource: wgpu::BindingResource::Sampler(sampler),
                            },
                        ]);
                    }
                    ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
                        label: Some("Morph uniforms and texture"),
                        layout: &layout,
                        entries: &entries,
                    })
                })
                .collect()
        };
        let wallpaper_intermediate =
            if prepared.stage == LayerStage::Wallpaper && format != FILTER_FORMAT {
                prepared
                    .shaders
                    .iter()
                    .map(|layer| pipeline(ctx, &pipeline_layout, FILTER_FORMAT, layer, false))
                    .collect()
            } else {
                Vec::new()
            };
        Self {
            names: prepared
                .shaders
                .iter()
                .map(|layer| layer.name.clone())
                .collect(),
            fps: prepared.fps,
            start: Instant::now(),
            pipelines,
            wallpaper_intermediate,
            source: prepared.source.clone(),
            layout,
            uniform,
            last_frame: None,
            frame_number: 0,
            sampler,
            depth,
            morph_bindings,
            rectangles: prepared
                .shaders
                .iter()
                .map(|layer| layer.rectangle)
                .collect(),
        }
    }

    pub(super) fn love(ctx: &WgpuContext, format: wgpu::TextureFormat) -> Self {
        let prepared = PreparedLayers {
            stage: LayerStage::Morph,
            fps: 30,
            source: None,
            shaders: vec![super::layer_source::PreparedLayer {
                name: "heart".into(),
                source: assemble(LayerStage::Morph, HEART),
                fragment_only: false,
                bitmap: None,
                rectangle: None,
            }],
        };
        Self::create(ctx, format, &prepared)
    }

    pub(super) fn frame_interval(&self) -> Option<Duration> {
        match self.fps {
            0 => None,
            kaleidux_common::LAYER_FPS_UNLIMITED => Some(Duration::ZERO),
            fps => Some(Duration::from_secs_f64(1.0 / f64::from(fps))),
        }
    }

    pub(super) fn write_frame(&mut self, ctx: &WgpuContext, mut frame: LayerFrame) {
        if self.fps != 0 {
            let delta = self
                .last_frame
                .map_or(0.0, |last| (frame.seconds - last.seconds).max(0.0));
            frame.timing = [delta, self.frame_number as f32, f32::from(self.fps), 0.0];
            self.frame_number = self.frame_number.wrapping_add(1);
        }
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
                texture_entry(3, self.depth.as_ref().expect("shader depth texture")),
            ],
        })
    }
}

fn texture_entry(binding: u32, view: &wgpu::TextureView) -> wgpu::BindGroupEntry<'_> {
    wgpu::BindGroupEntry {
        binding,
        resource: wgpu::BindingResource::TextureView(view),
    }
}

fn asset_view(
    ctx: &WgpuContext,
    image: Option<&image::RgbaImage>,
    format: wgpu::TextureFormat,
) -> wgpu::TextureView {
    let (width, height) = image.map_or((1, 1), image::RgbaImage::dimensions);
    let size = wgpu::Extent3d {
        width,
        height,
        depth_or_array_layers: 1,
    };
    let texture = ctx.device.create_texture(&wgpu::TextureDescriptor {
        label: Some("Shader layer asset"),
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format,
        usage: wgpu::TextureUsages::COPY_DST | wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[],
    });
    ctx.write_texture(
        texture.as_image_copy(),
        image.map_or(&[0_u8; 4][..], |image| image.as_raw().as_slice()),
        wgpu::ImageDataLayout {
            offset: 0,
            bytes_per_row: Some(width * 4),
            rows_per_image: Some(height),
        },
        size,
    );
    texture.create_view(&wgpu::TextureViewDescriptor::default())
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
    has_bitmap: bool,
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
        entry(
            3,
            wgpu::BindingType::Texture {
                sample_type: wgpu::TextureSampleType::Float { filterable: true },
                view_dimension: wgpu::TextureViewDimension::D2,
                multisampled: false,
            },
        ),
    ];
    let layout = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Wallpaper layer bindings"),
            entries: if filter {
                &entries
            } else if has_bitmap {
                &entries[..3]
            } else {
                &entries[..1]
            },
        });
    let uniform = ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("Wallpaper layer frame"),
        size: std::mem::size_of::<LayerFrame>() as u64,
        usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    let sampler = (filter || has_bitmap).then(|| {
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
    layer: &super::layer_source::PreparedLayer,
    replace: bool,
) -> wgpu::RenderPipeline {
    let shader = ctx
        .device
        .create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Wallpaper layer shader"),
            source: wgpu::ShaderSource::Wgsl(layer.source.as_str().into()),
        });
    let vertex = layer.fragment_only.then(|| {
        ctx.device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("GLSL layer vertex"),
                source: wgpu::ShaderSource::Wgsl(
                    assemble(
                        LayerStage::Morph,
                        "fn layer(uv: vec2<f32>) -> vec4<f32> { return vec4<f32>(uv, 0.0, 0.0); }",
                    )
                    .into(),
                ),
            })
    });
    ctx.device
        .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("Wallpaper layer pipeline"),
            layout: Some(layout),
            vertex: wgpu::VertexState {
                module: vertex.as_ref().unwrap_or(&shader),
                entry_point: Some("kld_vertex"),
                compilation_options: Default::default(),
                buffers: &[],
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: Some(if layer.fragment_only {
                    "main"
                } else {
                    "kld_fragment"
                }),
                compilation_options: Default::default(),
                targets: &[Some(wgpu::ColorTargetState {
                    format,
                    blend: if replace {
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
