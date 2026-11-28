//! Executes Shadertoy passes in order, keeping a second target only for previous-frame reads.

use super::WgpuContext;
use super::layer_pipeline::{
    FILTER_FORMAT, LayerFrame, asset_view, frame_uniform, pipeline, texture_entry, uniform_entry,
};
use super::layer_shadertoy::{Channel, ChannelSource, IMAGE, PreparedToy};
use super::layers::{begin_pass, draw};

const BUFFER_FORMAT: wgpu::TextureFormat = wgpu::TextureFormat::Rgba16Float;
const BUFFER_BUDGET_BYTES: u64 = 192 * 1024 * 1024;
const TEXTURE_BINDINGS: [u32; 4] = [1, 3, 4, 5];
const SAMPLER_BINDINGS: [u32; 4] = [2, 6, 7, 8];

pub(super) struct Feedback {
    layout: wgpu::BindGroupLayout,
    passes: Vec<Pass>,
    image_intermediate: Option<wgpu::RenderPipeline>,
    textures: Vec<wgpu::TextureView>,
    empty: wgpu::TextureView,
    samplers: [wgpu::Sampler; 4],
    targets: Option<Targets>,
}

struct Pass {
    order: usize,
    channels: [Channel; 4],
    feedback: bool,
    pipeline: wgpu::RenderPipeline,
    uniform: wgpu::Buffer,
}

struct Targets {
    output_size: [u32; 2],
    buffer_size: [u32; 2],
    buffers: [Vec<wgpu::TextureView>; IMAGE],
    bindings: Vec<Vec<wgpu::BindGroup>>,
    frame: Option<u32>,
    seconds: f32,
    delta: f32,
    bytes: u64,
}

impl Feedback {
    pub(super) fn new(ctx: &WgpuContext, format: wgpu::TextureFormat, toy: &PreparedToy) -> Self {
        let device = &ctx.device;
        let entry = |binding, ty| wgpu::BindGroupLayoutEntry {
            binding,
            ty,
            visibility: wgpu::ShaderStages::FRAGMENT,
            count: None,
        };
        let mut entries = vec![entry(
            0,
            wgpu::BindingType::Buffer {
                ty: wgpu::BufferBindingType::Uniform,
                has_dynamic_offset: false,
                min_binding_size: wgpu::BufferSize::new(std::mem::size_of::<LayerFrame>() as u64),
            },
        )];
        entries.extend(TEXTURE_BINDINGS.map(|binding| {
            entry(
                binding,
                wgpu::BindingType::Texture {
                    sample_type: wgpu::TextureSampleType::Float { filterable: true },
                    view_dimension: wgpu::TextureViewDimension::D2,
                    multisampled: false,
                },
            )
        }));
        entries.extend(SAMPLER_BINDINGS.map(|binding| {
            entry(
                binding,
                wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
            )
        }));
        let layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Shadertoy channels"),
            entries: &entries,
        });
        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Shadertoy pipeline layout"),
            bind_group_layouts: &[&layout],
            push_constant_ranges: &[],
        });
        let image_intermediate = toy
            .passes
            .last()
            .filter(|_| format != FILTER_FORMAT)
            .map(|image| pipeline(ctx, &pipeline_layout, FILTER_FORMAT, &image.layer, true));
        let passes = toy
            .passes
            .iter()
            .map(|pass| Pass {
                order: pass.order,
                channels: pass.channels,
                feedback: pass.feedback,
                pipeline: pipeline(
                    ctx,
                    &pipeline_layout,
                    if pass.order == IMAGE {
                        format
                    } else {
                        BUFFER_FORMAT
                    },
                    &pass.layer,
                    true,
                ),
                uniform: frame_uniform(ctx),
            })
            .collect();
        let textures = toy
            .textures
            .iter()
            .map(|texture| {
                let format = if texture.srgb {
                    FILTER_FORMAT
                } else {
                    wgpu::TextureFormat::Rgba8Unorm
                };
                asset_view(ctx, Some(&texture.bitmap), format)
            })
            .collect();
        let samplers = std::array::from_fn(|index| {
            let filter = if index & 1 == 0 {
                wgpu::FilterMode::Linear
            } else {
                wgpu::FilterMode::Nearest
            };
            let wrap = if index & 2 == 0 {
                wgpu::AddressMode::ClampToEdge
            } else {
                wgpu::AddressMode::Repeat
            };
            device.create_sampler(&wgpu::SamplerDescriptor {
                label: Some("Shadertoy channel sampler"),
                min_filter: filter,
                mag_filter: filter,
                address_mode_u: wrap,
                address_mode_v: wrap,
                ..Default::default()
            })
        });
        Self {
            layout,
            passes,
            image_intermediate,
            textures,
            empty: asset_view(ctx, None, FILTER_FORMAT),
            samplers,
            targets: None,
        }
    }

    pub(super) fn render(
        &mut self,
        ctx: &WgpuContext,
        encoder: &mut wgpu::CommandEncoder,
        destination: &wgpu::TextureView,
        mut frame: LayerFrame,
        fps: u16,
        intermediate: bool,
    ) {
        let size = frame.size.map(|axis| axis as u32);
        if self
            .targets
            .as_ref()
            .is_none_or(|targets| targets.output_size != size)
        {
            self.targets = Some(self.allocate_targets(ctx, size));
        }
        let targets = self.targets.as_mut().expect("Shadertoy targets allocated");
        let elapsed = frame.seconds - targets.seconds;
        let step = if fps == kaleidux_common::LAYER_FPS_UNLIMITED {
            0.0
        } else {
            (1.0 / f32::from(fps.max(1)) - 0.000_5).max(0.0)
        };
        let advance = targets.frame.is_none() || (fps != 0 && elapsed > 0.0 && elapsed >= step);
        if advance {
            targets.delta = if targets.frame.is_some() {
                elapsed
            } else {
                0.0
            };
            targets.seconds = frame.seconds;
            targets.frame = Some(targets.frame.map_or(0, |frame| frame.wrapping_add(1)));
        }
        let frame_number = targets.frame.unwrap_or_default();
        frame.seconds = targets.seconds;
        frame.timing = [targets.delta, frame_number as f32, 0.0, 0.0];
        let parity = frame_number as usize % targets.bindings.len();
        let bindings = &targets.bindings[parity];
        for (pass, binding) in self.passes.iter().zip(bindings) {
            let (target, pipeline) = if pass.order == IMAGE {
                frame.size = size.map(|axis| axis as f32);
                let alternate = if intermediate {
                    self.image_intermediate.as_ref()
                } else {
                    None
                };
                (destination, alternate.unwrap_or(&pass.pipeline))
            } else if advance {
                frame.size = targets.buffer_size.map(|axis| axis as f32);
                let views = &targets.buffers[pass.order];
                (&views[parity % views.len()], &pass.pipeline)
            } else {
                continue;
            };
            ctx.write_buffer(&pass.uniform, 0, bytemuck::bytes_of(&frame));
            let mut draw_pass = begin_pass(
                encoder,
                target,
                wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT),
            );
            draw(&mut draw_pass, pipeline, binding);
        }
    }

    fn allocate_targets(&self, ctx: &WgpuContext, output_size: [u32; 2]) -> Targets {
        let mut counts = [0; IMAGE];
        for pass in self.passes.iter().filter(|pass| pass.order != IMAGE) {
            counts[pass.order] = 1 + usize::from(pass.feedback);
        }
        let bytes_per_pixel = counts.iter().sum::<usize>() as u64 * 8;
        let bytes =
            |[width, height]: [u32; 2]| u64::from(width) * u64::from(height) * bytes_per_pixel;
        let mut buffer_size = output_size;
        if bytes(buffer_size) > BUFFER_BUDGET_BYTES {
            let scale = (BUFFER_BUDGET_BYTES as f64 / bytes(buffer_size) as f64).sqrt();
            buffer_size = buffer_size.map(|axis| ((f64::from(axis) * scale) as u32).max(1));
        }
        let buffers = counts.map(|count| {
            (0..count)
                .map(|_| {
                    ctx.device
                        .create_texture(&wgpu::TextureDescriptor {
                            label: Some("Shadertoy buffer"),
                            size: wgpu::Extent3d {
                                width: buffer_size[0],
                                height: buffer_size[1],
                                depth_or_array_layers: 1,
                            },
                            dimension: wgpu::TextureDimension::D2,
                            format: BUFFER_FORMAT,
                            usage: wgpu::TextureUsages::TEXTURE_BINDING
                                | wgpu::TextureUsages::RENDER_ATTACHMENT,
                            sample_count: 1,
                            mip_level_count: 1,
                            view_formats: &[],
                        })
                        .create_view(&wgpu::TextureViewDescriptor::default())
                })
                .collect()
        });
        let binding_count = counts.into_iter().max().unwrap_or_default().max(1);
        let bindings = (0..binding_count)
            .map(|parity| {
                self.passes
                    .iter()
                    .map(|pass| self.binding(ctx, pass, &buffers, parity))
                    .collect()
            })
            .collect();
        Targets {
            output_size,
            buffer_size,
            buffers,
            bindings,
            frame: None,
            seconds: 0.0,
            delta: 0.0,
            bytes: bytes(buffer_size),
        }
    }

    fn binding(
        &self,
        ctx: &WgpuContext,
        pass: &Pass,
        buffers: &[Vec<wgpu::TextureView>; IMAGE],
        parity: usize,
    ) -> wgpu::BindGroup {
        let mut entries = vec![uniform_entry(&pass.uniform)];
        for (index, channel) in pass.channels.iter().enumerate() {
            let view = match channel.source {
                ChannelSource::Empty => &self.empty,
                ChannelSource::Texture(index) => &self.textures[index],
                ChannelSource::Buffer(order) => {
                    let views = &buffers[order];
                    let previous = usize::from(order >= pass.order);
                    &views[(parity + previous) % views.len()]
                }
            };
            let sampler = usize::from(channel.nearest) + 2 * usize::from(channel.repeat);
            entries.push(texture_entry(TEXTURE_BINDINGS[index], view));
            entries.push(wgpu::BindGroupEntry {
                binding: SAMPLER_BINDINGS[index],
                resource: wgpu::BindingResource::Sampler(&self.samplers[sampler]),
            });
        }
        ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Shadertoy pass channels"),
            layout: &self.layout,
            entries: &entries,
        })
    }

    pub(super) fn retained_texture_bytes(&self) -> u64 {
        self.targets.as_ref().map_or(0, |targets| targets.bytes)
    }
}
