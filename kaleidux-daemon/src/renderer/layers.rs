//! Owns each output's layer stacks, filter targets and finite love animation.

use super::WgpuContext;
use super::layer_pipeline::{FILTER_FORMAT, LayerFrame, LayerPipelines};
use kaleidux_common::{LayerInputs, LayerStage, OutputLayers};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const LOVE_LIFETIME: Duration = Duration::from_millis(900);

#[derive(Default)]
pub(super) struct Layers {
    wallpaper: Option<LayerPipelines>,
    filters: LayerSlot,
    morphs: LayerSlot,
    rule_path: Option<PathBuf>,
    rule_ticket: u64,
    filter_targets: Option<FilterTargets>,
    love: Option<LovePulse>,
    signals: Signals,
    paused_at: Option<Instant>,
}

#[derive(Default)]
struct LayerSlot {
    manual: Option<LayerPipelines>,
    selected: Option<Option<LayerPipelines>>,
}

impl LayerSlot {
    fn active(&self) -> Option<&LayerPipelines> {
        self.selected.as_ref().unwrap_or(&self.manual).as_ref()
    }

    fn active_mut(&mut self) -> Option<&mut LayerPipelines> {
        self.selected.as_mut().unwrap_or(&mut self.manual).as_mut()
    }

    fn set(&mut self, next: Option<LayerPipelines>, rule: bool) {
        if rule {
            self.selected = Some(next);
        } else {
            self.manual = next;
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub(super) struct Signals {
    pointer: [f32; 4],
    data: [f32; 4],
    date: [f32; 4],
    audio: [[f32; 4]; 16],
}

impl Default for Signals {
    fn default() -> Self {
        Self {
            pointer: [0.5, 0.5, 0.0, 0.0],
            data: [0.0; 4],
            date: [0.0; 4],
            audio: [[0.0; 4]; 16],
        }
    }
}

impl Signals {
    fn apply(mut self, input: &LayerInputs) -> Self {
        if let Some(pointer) = input.pointer {
            self.pointer = pointer;
        }
        if let Some(data) = input.data {
            self.data = data;
        }
        if let Some(date) = input.date {
            self.date = date;
        }
        if let Some(bands) = &input.audio {
            self.audio.fill([0.0; 4]);
            for (index, band) in bands.iter().enumerate() {
                self.audio[index / 4][index % 4] = *band;
            }
        }
        self
    }
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
        self.wallpaper.is_some()
            || self.filters.active().is_some()
            || self.morphs.active().is_some()
            || self.love.is_some()
    }

    fn replace(&mut self, stage: LayerStage, next: Option<LayerPipelines>, rule: bool) {
        match stage {
            LayerStage::Filter => {
                self.filters.set(next, rule);
                self.filter_targets = None;
            }
            LayerStage::Morph => self.morphs.set(next, rule),
            LayerStage::Wallpaper => self.wallpaper = next,
        }
    }

    pub(super) fn base_view(
        &mut self,
        ctx: &WgpuContext,
        width: u32,
        height: u32,
    ) -> Option<&wgpu::TextureView> {
        let filters = self.filters.active()?;
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
        let animation_time = self.paused_at.unwrap_or(now);
        if let Some(wallpaper) = &mut self.wallpaper {
            let target = self
                .filter_targets
                .as_ref()
                .map_or(destination, |targets| &targets.targets[0].view);
            wallpaper.write_frame(
                ctx,
                layer_frame(size, wallpaper, animation_time, progress, self.signals),
            );
            let mut pass = begin_pass(
                encoder,
                target,
                wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT),
            );
            let pipelines =
                if self.filter_targets.is_some() && !wallpaper.wallpaper_intermediate.is_empty() {
                    &wallpaper.wallpaper_intermediate
                } else {
                    &wallpaper.pipelines
                };
            for (index, pipeline) in pipelines.iter().enumerate() {
                pass.set_viewport(0.0, 0.0, size[0] as f32, size[1] as f32, 0.0, 1.0);
                pass.set_scissor_rect(0, 0, size[0], size[1]);
                if let Some(rect) = wallpaper.rectangles[index] {
                    set_rectangle(&mut pass, size, rect);
                }
                draw(&mut pass, pipeline, &wallpaper.morph_bindings[index]);
            }
        }
        if let (Some(filters), Some(targets)) = (self.filters.active_mut(), &self.filter_targets) {
            let frame = layer_frame(size, filters, animation_time, progress, self.signals);
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
        if self.morphs.active().is_none() && self.love.is_none() {
            return;
        }
        let mut pass = begin_pass(encoder, destination, wgpu::LoadOp::Load);
        if let Some(morphs) = self.morphs.active_mut() {
            morphs.write_frame(
                ctx,
                layer_frame(size, morphs, animation_time, progress, self.signals),
            );
            for (index, pipeline) in morphs.pipelines.iter().enumerate() {
                pass.set_viewport(0.0, 0.0, size[0] as f32, size[1] as f32, 0.0, 1.0);
                pass.set_scissor_rect(0, 0, size[0], size[1]);
                if let Some(rect) = morphs.rectangles[index] {
                    set_rectangle(&mut pass, size, rect);
                } else if morphs.names[index] == "heart" {
                    scissor_heart(&mut pass, size, [0.91, 0.88]);
                }
                draw(&mut pass, pipeline, &morphs.morph_bindings[index]);
            }
        }
        if let Some(pulse) = &mut self.love {
            let mut frame = layer_frame(size, &pulse.pipeline, now, progress, self.signals);
            frame.progress = frame.seconds / LOVE_LIFETIME.as_secs_f32();
            frame.params = [pulse.position[0], pulse.position[1], 0.0, 1.0];
            pulse.pipeline.write_frame(ctx, frame);
            let binding = &pulse.pipeline.morph_bindings[0];
            pass.set_viewport(0.0, 0.0, size[0] as f32, size[1] as f32, 0.0, 1.0);
            scissor_heart(&mut pass, size, pulse.position);
            draw(&mut pass, &pulse.pipeline.pipelines[0], binding);
        }
    }

    fn frame_interval(&self) -> Option<Duration> {
        self.wallpaper
            .iter()
            .chain(self.filters.active())
            .chain(self.morphs.active())
            .filter(|_| self.paused_at.is_none())
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

fn set_rectangle(pass: &mut wgpu::RenderPass<'_>, size: [u32; 2], rect: [f32; 4]) {
    let width = size[0] as f32;
    let height = size[1] as f32;
    let x = (rect[0] * width).floor();
    let y = (rect[1] * height).floor();
    let right = ((rect[0] + rect[2]) * width).ceil().min(width);
    let bottom = ((rect[1] + rect[3]) * height).ceil().min(height);
    if right > x && bottom > y {
        pass.set_viewport(x, y, right - x, bottom - y, 0.0, 1.0);
        pass.set_scissor_rect(x as u32, y as u32, (right - x) as u32, (bottom - y) as u32);
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

fn layer_frame(
    size: [u32; 2],
    layers: &LayerPipelines,
    now: Instant,
    progress: f32,
    signals: Signals,
) -> LayerFrame {
    LayerFrame {
        size: [size[0] as f32, size[1] as f32],
        seconds: if layers.fps != 0 {
            now.saturating_duration_since(layers.start).as_secs_f32()
        } else {
            0.0
        },
        progress,
        params: [0.0; 4],
        timing: [0.0; 4],
        signals,
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
    pub(crate) fn replace_layers(
        &mut self,
        stage: LayerStage,
        next: Option<LayerPipelines>,
        rule: bool,
    ) {
        if next.is_some() {
            self.retain_native_frame_for_layers();
        }
        self.layers.replace(stage, next, rule);
        self.needs_redraw = true;
    }

    pub(crate) fn prepare_layer_rule(&mut self, path: Option<&Path>) -> Option<u64> {
        if self.layers.rule_path.as_deref() == path {
            return None;
        }
        self.invalidate_layer_rule();
        self.layers.rule_path = path.map(Path::to_owned);
        Some(self.layers.rule_ticket)
    }

    pub(crate) fn invalidate_layer_rule(&mut self) {
        self.layers.rule_ticket = rand::random();
        self.layers.rule_path = None;
        let had_shaders = self.layers.filters.selected.take().is_some();
        let had_morphs = self.layers.morphs.selected.take().is_some();
        if had_shaders {
            self.layers.filter_targets = None;
        }
        self.needs_redraw |= had_shaders || had_morphs;
    }

    pub(crate) fn layer_rule_ticket(&self) -> u64 {
        self.layers.rule_ticket
    }

    pub(crate) fn layer_rule_matches(&self, ticket: u64, source: Option<&Path>) -> bool {
        self.layers.rule_ticket == ticket && self.layers.rule_path.as_deref() == source
    }

    pub(crate) fn apply_layer_inputs(&mut self, inputs: &LayerInputs) {
        let signals = self.layers.signals.apply(inputs);
        if signals != self.layers.signals {
            self.layers.signals = signals;
            self.needs_redraw |= self.layers.requires_composition();
        }
    }

    pub(crate) fn output_layers(&self) -> OutputLayers {
        let fields = |stack: Option<&LayerPipelines>| {
            stack.map_or_else(
                || (Vec::new(), 0),
                |pipeline| (pipeline.names.clone(), pipeline.fps),
            )
        };
        let (wallpaper, wallpaper_fps) = fields(self.layers.wallpaper.as_ref());
        let (filters, filter_fps) = fields(self.layers.filters.active());
        let (morphs, morph_fps) = fields(self.layers.morphs.active());
        OutputLayers {
            output: self.name.clone(),
            filters,
            morphs,
            filter_fps,
            morph_fps,
            wallpaper,
            wallpaper_fps,
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

    pub(crate) fn shader_wallpaper(&self) -> Option<&str> {
        self.layers.wallpaper.as_ref()?.source.as_deref()
    }

    pub(crate) fn clear_shader_wallpaper(&mut self) {
        self.layers.wallpaper = None;
    }

    pub(super) fn take_shader_wallpaper(&mut self) -> Option<LayerPipelines> {
        self.layers.wallpaper.take()
    }

    pub(super) fn restore_shader_wallpaper(&mut self, wallpaper: Option<LayerPipelines>) {
        self.layers.wallpaper = wallpaper;
    }

    pub(crate) fn update_layer_clock(&mut self, paused: bool, now: Instant) {
        if paused {
            self.layers.paused_at.get_or_insert(now);
        } else if let Some(previous) = self.layers.paused_at.take() {
            let LayerSlot {
                manual: shaders,
                selected: rule_shaders,
            } = &mut self.layers.filters;
            let LayerSlot {
                manual: morphs,
                selected: rule_morphs,
            } = &mut self.layers.morphs;
            for stack in [
                self.layers.wallpaper.as_mut(),
                shaders.as_mut(),
                rule_shaders.as_mut().and_then(Option::as_mut),
                morphs.as_mut(),
                rule_morphs.as_mut().and_then(Option::as_mut),
            ]
            .into_iter()
            .flatten()
            {
                stack.start += now.saturating_duration_since(previous.max(stack.start));
            }
        }
    }

    pub(crate) fn layer_uses_display_clock(&self) -> bool {
        self.layers
            .frame_interval()
            .is_some_and(|interval| interval.is_zero())
    }

    pub(crate) fn layer_animation_deadline(&self) -> Option<Instant> {
        if self.needs_redraw || self.transition_active || self.frame_callback_pending {
            return None;
        }
        self.layers
            .frame_interval()
            .map(|delta| self.last_present_time + delta)
    }
}
