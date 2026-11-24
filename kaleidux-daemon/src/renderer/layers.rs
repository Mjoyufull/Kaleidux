//! Owns each output's layer stacks, filter targets and finite love animation.

use super::WgpuContext;
use super::layer_pipeline::{FILTER_FORMAT, LayerFrame, LayerPipelines};
use kaleidux_common::{LayerStage, OutputLayers};
use std::time::{Duration, Instant};

const LOVE_LIFETIME: Duration = Duration::from_millis(900);

#[derive(Default)]
pub(super) struct Layers {
    filters: Option<LayerPipelines>,
    morphs: Option<LayerPipelines>,
    filter_targets: Option<FilterTargets>,
    love: Option<LovePulse>,
}

struct LovePulse {
    pipeline: LayerPipelines,
    position: [f32; 2],
}

struct FilterTargets {
    size: [u32; 2],
    targets: Vec<FilterTarget>,
}

struct FilterTarget {
    view: wgpu::TextureView,
    binding: wgpu::BindGroup,
}

impl Layers {
    pub(super) fn requires_composition(&self) -> bool {
        self.filters.is_some() || self.morphs.is_some() || self.love.is_some()
    }

    fn replace(&mut self, stage: LayerStage, next: Option<LayerPipelines>) {
        match stage {
            LayerStage::Filter => {
                self.filters = next;
                self.filter_targets = None;
            }
            LayerStage::Morph => self.morphs = next,
        }
    }

    pub(super) fn base_view(
        &mut self,
        ctx: &WgpuContext,
        width: u32,
        height: u32,
    ) -> Option<&wgpu::TextureView> {
        let filters = self.filters.as_ref()?;
        let size = [width, height];
        let targets = self
            .filter_targets
            .get_or_insert_with(|| FilterTargets::new(ctx, filters, size));
        if targets.size != size {
            *targets = FilterTargets::new(ctx, filters, size);
        }
        Some(&targets.targets[0].view)
    }

    pub(super) fn render(
        &mut self,
        ctx: &WgpuContext,
        encoder: &mut wgpu::CommandEncoder,
        destination: &wgpu::TextureView,
        size: [u32; 2],
        now: Instant,
        progress: f32,
    ) {
        if let (Some(filters), Some(targets)) = (&mut self.filters, &self.filter_targets) {
            let frame = layer_frame(size, filters, now, progress);
            filters.write_frame(ctx, frame);
            for (index, pipeline) in filters.pipelines.iter().enumerate() {
                let input = &targets.targets[index % targets.targets.len()];
                let last = index + 1 == filters.pipelines.len();
                let target = if last {
                    destination
                } else {
                    &targets.targets[(index + 1) % 2].view
                };
                let mut pass = begin_pass(encoder, target, wgpu::LoadOp::Clear(wgpu::Color::BLACK));
                draw(&mut pass, pipeline, &input.binding);
            }
        }

        if self.love.as_ref().is_some_and(|pulse| {
            now.saturating_duration_since(pulse.pipeline.start) >= LOVE_LIFETIME
        }) {
            self.love = None;
        }
        if self.morphs.is_none() && self.love.is_none() {
            return;
        }
        let mut pass = begin_pass(encoder, destination, wgpu::LoadOp::Load);
        if let Some(morphs) = &mut self.morphs {
            morphs.write_frame(ctx, layer_frame(size, morphs, now, progress));
            let binding = morphs
                .morph_binding
                .as_ref()
                .expect("morph has a uniform binding");
            for (name, pipeline) in morphs.names.iter().zip(&morphs.pipelines) {
                if name == "heart" {
                    scissor_heart(&mut pass, size, [0.91, 0.88]);
                } else {
                    pass.set_scissor_rect(0, 0, size[0], size[1]);
                }
                draw(&mut pass, pipeline, binding);
            }
        }
        if let Some(pulse) = &mut self.love {
            let mut frame = layer_frame(size, &pulse.pipeline, now, progress);
            frame.progress = frame.seconds / LOVE_LIFETIME.as_secs_f32();
            frame.params = [pulse.position[0], pulse.position[1], 0.0, 1.0];
            pulse.pipeline.write_frame(ctx, frame);
            let binding = pulse
                .pipeline
                .morph_binding
                .as_ref()
                .expect("heart has a uniform binding");
            scissor_heart(&mut pass, size, pulse.position);
            draw(&mut pass, &pulse.pipeline.pipelines[0], binding);
        }
    }

    fn frame_interval(&self) -> Option<Duration> {
        self.filters
            .iter()
            .chain(&self.morphs)
            .filter_map(LayerPipelines::frame_interval)
            .chain(
                self.love
                    .as_ref()
                    .and_then(|pulse| pulse.pipeline.frame_interval()),
            )
            .min()
    }

    pub(super) fn retained_texture_bytes(&self) -> u64 {
        self.filter_targets.as_ref().map_or(0, |targets| {
            super::texture::texture_byte_size(targets.size[0], targets.size[1], 1)
                .saturating_mul(targets.targets.len() as u64)
        })
    }
}

fn scissor_heart(pass: &mut wgpu::RenderPass<'_>, size: [u32; 2], position: [f32; 2]) {
    let radius = (size[0].min(size[1]) as f32 * 0.1).ceil();
    let center = [position[0] * size[0] as f32, position[1] * size[1] as f32];
    let x = (center[0] - radius).max(0.0) as u32;
    let y = (center[1] - radius).max(0.0) as u32;
    let width = ((center[0] + radius).ceil() as u32).min(size[0]) - x;
    let height = ((center[1] + radius).ceil() as u32).min(size[1]) - y;
    pass.set_scissor_rect(x, y, width, height);
}

fn layer_frame(size: [u32; 2], layers: &LayerPipelines, now: Instant, progress: f32) -> LayerFrame {
    LayerFrame {
        size: [size[0] as f32, size[1] as f32],
        seconds: if layers.fps != 0 {
            now.saturating_duration_since(layers.start).as_secs_f32()
        } else {
            0.0
        },
        progress,
        params: [0.0; 4],
    }
}

impl FilterTargets {
    fn new(ctx: &WgpuContext, filters: &LayerPipelines, size: [u32; 2]) -> Self {
        let targets = (0..filters.pipelines.len().min(2))
            .map(|_| {
                let texture = ctx.device.create_texture(&wgpu::TextureDescriptor {
                    label: Some("Wallpaper filter intermediate"),
                    size: wgpu::Extent3d {
                        width: size[0],
                        height: size[1],
                        depth_or_array_layers: 1,
                    },
                    mip_level_count: 1,
                    sample_count: 1,
                    dimension: wgpu::TextureDimension::D2,
                    format: FILTER_FORMAT,
                    usage: wgpu::TextureUsages::RENDER_ATTACHMENT
                        | wgpu::TextureUsages::TEXTURE_BINDING,
                    view_formats: &[],
                });
                let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
                FilterTarget {
                    binding: filters.filter_binding(ctx, &view),
                    view,
                }
            })
            .collect();
        Self { size, targets }
    }
}

fn begin_pass<'a>(
    encoder: &'a mut wgpu::CommandEncoder,
    target: &'a wgpu::TextureView,
    load: wgpu::LoadOp<wgpu::Color>,
) -> wgpu::RenderPass<'a> {
    encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
        label: Some("Wallpaper layer pass"),
        color_attachments: &[Some(wgpu::RenderPassColorAttachment {
            view: target,
            resolve_target: None,
            ops: wgpu::Operations {
                load,
                store: wgpu::StoreOp::Store,
            },
        })],
        ..Default::default()
    })
}

fn draw(
    pass: &mut wgpu::RenderPass<'_>,
    pipeline: &wgpu::RenderPipeline,
    binding: &wgpu::BindGroup,
) {
    pass.set_pipeline(pipeline);
    pass.set_bind_group(0, binding, &[]);
    pass.draw(0..3, 0..1);
}

impl super::Renderer {
    pub(crate) fn replace_layers(&mut self, stage: LayerStage, next: Option<LayerPipelines>) {
        if next.is_some() {
            self.retain_native_frame_for_layers();
        }
        self.layers.replace(stage, next);
        self.needs_redraw = true;
    }

    pub(crate) fn output_layers(&self) -> OutputLayers {
        let fields = |stack: Option<&LayerPipelines>| {
            stack.map_or_else(
                || (Vec::new(), 0),
                |pipeline| (pipeline.names.clone(), pipeline.fps),
            )
        };
        let (filters, filter_fps) = fields(self.layers.filters.as_ref());
        let (morphs, morph_fps) = fields(self.layers.morphs.as_ref());
        OutputLayers {
            output: self.name.clone(),
            filters,
            morphs,
            filter_fps,
            morph_fps,
        }
    }

    pub(crate) fn show_love(&mut self, position: [f32; 2]) {
        self.retain_native_frame_for_layers();
        let mut pipeline = self.layers.love.take().map_or_else(
            || LayerPipelines::love(&self.ctx, self.config.format),
            |pulse| pulse.pipeline,
        );
        pipeline.start = Instant::now();
        self.layers.love = Some(LovePulse { pipeline, position });
        self.needs_redraw = true;
    }

    fn retain_native_frame_for_layers(&mut self) {
        if self.native_wayland_snapshot_frame.is_some() {
            self.snapshot_yuv_for_transition();
            self.blit_bind_group = None;
        }
    }

    pub(crate) fn layer_animation_deadline(&self) -> Option<Instant> {
        if self.needs_redraw || self.transition_active {
            return None;
        }
        self.layers
            .frame_interval()
            .map(|delta| self.last_present_time + delta)
    }
}
