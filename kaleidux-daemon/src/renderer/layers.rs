//! Owns each output's layer stacks, filter targets and finite love animation.

use super::WgpuContext;
use super::layer_pipeline::{FILTER_FORMAT, LayerFrame, LayerPipelines};
use chrono::{Datelike, Local, Timelike};
use kaleidux_common::{
    LayerInputs, LayerStage, MAX_MORPHS_PER_OUTPUT, MorphGesture, MorphGroup, OutputLayers,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

mod cursor;
mod love;
use cursor::CursorMotion;
pub(crate) use love::{LOVE_FRAME_INTERVAL, LoveMark};
use love::{LoveVisual, scissor_heart};

#[derive(Default)]
pub(super) struct Layers {
    wallpaper: Option<LayerPipelines>,
    filters: LayerSlot,
    morphs: LayerSlot,
    morph_groups: BTreeMap<String, LayerPipelines>,
    rule_path: Option<PathBuf>,
    rule_ticket: u64,
    filter_targets: Option<FilterTargets>,
    love: Option<LoveMark>,
    love_pipeline: Option<LayerPipelines>,
    signals: Signals,
    date_override: Option<[f32; 4]>,
    cursor: CursorMotion,
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
    controls: [[f32; 4]; 8],
    cursor: [f32; 4],
}

impl Default for Signals {
    fn default() -> Self {
        Self {
            pointer: [0.5, 0.5, 0.0, 0.0],
            data: [0.0; 4],
            date: [0.0; 4],
            audio: [[0.0; 4]; 16],
            controls: [[0.0; 4]; 8],
            cursor: [0.5, 0.5, 0.0, 0.0],
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
        if let Some(bands) = &input.audio {
            self.audio.fill([0.0; 4]);
            for (index, band) in bands.iter().enumerate() {
                self.audio[index / 4][index % 4] = *band;
            }
        }
        if let Some(values) = &input.controls {
            self.controls.fill([0.0; 4]);
            for (index, value) in values.iter().enumerate() {
                self.controls[index / 4][index % 4] = *value;
            }
        }
        self
    }
}

struct FilterTargets {
    size: [u32; 2],
    targets: Vec<FilterTarget>,
    #[cfg(feature = "retroarch-shaders")]
    retro: Option<RetroTargets>,
}

#[cfg(feature = "retroarch-shaders")]
struct RetroTargets {
    input: std::sync::Arc<wgpu::Texture>,
    output: wgpu::TextureView,
    binding: wgpu::BindGroup,
}

struct FilterTarget {
    view: wgpu::TextureView,
    binding: wgpu::BindGroup,
}

impl Layers {
    fn active_stacks(&self) -> impl Iterator<Item = &LayerPipelines> {
        self.wallpaper
            .iter()
            .chain(self.filters.active())
            .chain(self.morphs.active())
            .chain(self.morph_groups.values())
    }

    fn uses_cursor(&self) -> bool {
        self.active_stacks().any(|stack| stack.uses_cursor)
    }

    fn uses_date(&self) -> bool {
        self.active_stacks().any(|stack| stack.uses_date)
    }

    pub(super) fn requires_composition(&self) -> bool {
        self.wallpaper.is_some()
            || self.filters.active().is_some()
            || self.morphs.active().is_some()
            || !self.morph_groups.is_empty()
            || self.love.as_ref().is_some_and(|love| love.composed)
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
        if self.paused_at.is_none() && self.uses_cursor() {
            self.signals.cursor = self.cursor.update(now);
        }
        let mut signals = self.signals;
        if self.uses_date() {
            signals.date = self.date_override.unwrap_or_else(local_date);
        }
        if let Some(wallpaper) = &mut self.wallpaper {
            let target = self
                .filter_targets
                .as_ref()
                .map_or(destination, |targets| &targets.targets[0].view);
            let frame = layer_frame(size, wallpaper, animation_time, progress, signals);
            let intermediate = self.filter_targets.is_some();
            if let Some(feedback) = &mut wallpaper.feedback {
                feedback.render(ctx, encoder, target, frame, wallpaper.fps, intermediate);
            } else {
                wallpaper.write_frame(ctx, frame);
                draw_wallpaper(encoder, target, size, wallpaper, intermediate);
            }
        }
        if let (Some(filters), Some(targets)) = (self.filters.active_mut(), &self.filter_targets) {
            let frame = layer_frame(size, filters, animation_time, progress, signals);
            filters.write_frame(ctx, frame);
            targets.render(ctx, encoder, destination, filters);
        }

        let love = LoveVisual::resolve(&mut self.love, now);
        if self.morphs.active().is_none() && self.morph_groups.is_empty() && love.is_none() {
            return;
        }
        let mut pass = begin_pass(encoder, destination, wgpu::LoadOp::Load);
        for morphs in self
            .morphs
            .active_mut()
            .into_iter()
            .chain(self.morph_groups.values_mut())
        {
            morphs.write_frame(
                ctx,
                layer_frame(size, morphs, animation_time, progress, signals),
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
        if let (Some(love), Some(pipeline)) = (love, &mut self.love_pipeline) {
            love.render(&mut pass, ctx, pipeline, size, now, self.signals);
        }
    }

    fn frame_interval(&self) -> Option<Duration> {
        self.wallpaper
            .iter()
            .chain(self.filters.active())
            .chain(self.morphs.active())
            .chain(self.morph_groups.values())
            .filter(|_| self.paused_at.is_none())
            .filter_map(LayerPipelines::frame_interval)
            .chain(
                (self.paused_at.is_none() && self.cursor.pending() && self.uses_cursor())
                    .then_some(Duration::ZERO),
            )
            .chain(
                self.love
                    .as_ref()
                    .filter(|mark| mark.composed && mark.animation.is_some())
                    .map(|_| LOVE_FRAME_INTERVAL),
            )
            .min()
    }

    pub(super) fn retained_texture_bytes(&self) -> u64 {
        let filter_bytes = self.filter_targets.as_ref().map_or(0, |targets| {
            #[cfg(feature = "retroarch-shaders")]
            if let (Some(_), Some(retro)) = (
                &targets.retro,
                self.filters
                    .active()
                    .and_then(|filter| filter.retro.as_ref()),
            ) {
                let bytes =
                    super::texture::texture_byte_size(targets.size[0], targets.size[1], 1) * 2;
                return bytes + retro.retained_texture_bytes();
            }
            super::texture::texture_byte_size(targets.size[0], targets.size[1], 1)
                .saturating_mul(targets.targets.len() as u64)
        });
        let feedback_bytes = self
            .wallpaper
            .as_ref()
            .and_then(|wallpaper| wallpaper.feedback.as_ref())
            .map_or(0, super::layer_feedback::Feedback::retained_texture_bytes);
        filter_bytes + feedback_bytes
    }
}

fn draw_wallpaper(
    encoder: &mut wgpu::CommandEncoder,
    target: &wgpu::TextureView,
    size: [u32; 2],
    wallpaper: &LayerPipelines,
    intermediate: bool,
) {
    let mut pass = begin_pass(
        encoder,
        target,
        wgpu::LoadOp::Clear(wgpu::Color::TRANSPARENT),
    );
    let pipelines = if intermediate && !wallpaper.wallpaper_intermediate.is_empty() {
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

fn layer_frame(
    size: [u32; 2],
    layers: &LayerPipelines,
    now: Instant,
    progress: f32,
    mut signals: Signals,
) -> LayerFrame {
    if !layers.uses_date {
        signals.date = [0.0; 4];
    }
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

fn local_date() -> [f32; 4] {
    let now = Local::now();
    [
        now.year() as f32,
        now.month() as f32,
        now.day() as f32,
        now.num_seconds_from_midnight() as f32 + now.nanosecond() as f32 / 1_000_000_000.0,
    ]
}

impl FilterTargets {
    fn new(ctx: &WgpuContext, filters: &LayerPipelines, size: [u32; 2]) -> Self {
        #[cfg(feature = "retroarch-shaders")]
        if filters.retro.is_some() {
            use super::layer_retro::target_texture;
            let input = std::sync::Arc::new(target_texture(ctx, size));
            let output = target_texture(ctx, size);
            let srgb = wgpu::TextureViewDescriptor {
                format: Some(FILTER_FORMAT),
                ..Default::default()
            };
            let view = input.create_view(&srgb);
            return Self {
                size,
                targets: vec![FilterTarget {
                    binding: filters.filter_binding(ctx, &view),
                    view,
                }],
                retro: Some(RetroTargets {
                    input,
                    output: output.create_view(&Default::default()),
                    binding: filters.filter_binding(ctx, &output.create_view(&srgb)),
                }),
            };
        }

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
        Self {
            size,
            targets,
            #[cfg(feature = "retroarch-shaders")]
            retro: None,
        }
    }

    fn render(
        &self,
        _ctx: &WgpuContext,
        encoder: &mut wgpu::CommandEncoder,
        destination: &wgpu::TextureView,
        filters: &mut LayerPipelines,
    ) {
        #[cfg(feature = "retroarch-shaders")]
        if let (Some(targets), Some(retro)) = (&self.retro, &mut filters.retro) {
            let success = retro.render(_ctx, encoder, &targets.input, &targets.output, self.size);
            let binding = if success {
                &targets.binding
            } else {
                &self.targets[0].binding
            };
            let mut pass = begin_pass(
                encoder,
                destination,
                wgpu::LoadOp::Clear(wgpu::Color::BLACK),
            );
            draw(&mut pass, &filters.pipelines[0], binding);
            return;
        }

        for (index, pipeline) in filters.pipelines.iter().enumerate() {
            let input = &self.targets[index % self.targets.len()];
            let target = if index + 1 == filters.pipelines.len() {
                destination
            } else {
                &self.targets[(index + 1) % 2].view
            };
            let mut pass = begin_pass(encoder, target, wgpu::LoadOp::Clear(wgpu::Color::BLACK));
            draw(&mut pass, pipeline, &input.binding);
        }
    }
}

pub(super) fn begin_pass<'a>(
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

pub(super) fn draw(
    pass: &mut wgpu::RenderPass<'_>,
    pipeline: &wgpu::RenderPipeline,
    binding: &wgpu::BindGroup,
) {
    pass.set_pipeline(pipeline);
    pass.set_bind_group(0, binding, &[]);
    pass.draw(0..3, 0..1);
}

impl super::Renderer {
    pub(crate) fn has_composed_layers(&self) -> bool {
        self.layers.requires_composition()
    }

    pub(crate) fn replace_layers(
        &mut self,
        stage: LayerStage,
        next: Option<LayerPipelines>,
        rule: bool,
        group: Option<&str>,
    ) {
        if next.is_some() {
            self.retain_native_frame_for_layers();
        }
        if let Some(group) = group {
            if let Some(stack) = next {
                self.layers.morph_groups.insert(group.to_owned(), stack);
            } else {
                self.layers.morph_groups.remove(group);
            }
        } else {
            self.layers.replace(stage, next, rule);
        }
        self.needs_redraw = true;
    }

    pub(crate) fn check_morph_budget(
        &self,
        next: &LayerPipelines,
        rule: bool,
        group: Option<&str>,
    ) -> anyhow::Result<()> {
        let layers = &self.layers;
        let manual = layers.morphs.manual.as_ref();
        let selected = layers.morphs.selected.as_ref().and_then(Option::as_ref);
        let (manual, selected) = match (group, rule) {
            (None, false) => (Some(next), selected),
            (None, true) => (manual, Some(next)),
            (Some(_), _) => (manual, selected),
        };
        let base_count = [manual, selected]
            .into_iter()
            .flatten()
            .map(|stack| stack.names.len())
            .max()
            .unwrap_or(0);
        let base_bytes: usize = [manual, selected]
            .into_iter()
            .flatten()
            .map(|stack| stack.asset_bytes)
            .sum();
        let (count, bytes) = layers
            .morph_groups
            .iter()
            .filter(|(name, _)| Some(name.as_str()) != group)
            .map(|(_, stack)| stack)
            .chain(group.map(|_| next))
            .fold((base_count, base_bytes), |(count, bytes), stack| {
                (count + stack.names.len(), bytes + stack.asset_bytes)
            });
        anyhow::ensure!(
            count <= MAX_MORPHS_PER_OUTPUT,
            "An output supports at most {MAX_MORPHS_PER_OUTPUT} morphs across its groups"
        );
        anyhow::ensure!(
            bytes <= super::layer_source::MAX_LAYER_TEXTURE_BYTES,
            "Morph textures exceed 96 MiB on this output"
        );
        Ok(())
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
        if inputs.date.is_some() || inputs.live_date {
            let previous = std::mem::replace(&mut self.layers.date_override, inputs.date);
            self.needs_redraw |= previous != inputs.date && self.layers.uses_date();
        }
        if let Some(active) = inputs.pointer_active.or(inputs.pointer.map(|_| true))
            && self.layers.cursor.set(signals.pointer, active)
        {
            self.needs_redraw |= self.layers.uses_cursor() && self.layers.paused_at.is_none();
        }
        if signals != self.layers.signals {
            self.layers.signals = signals;
            self.needs_redraw |= self.layers.requires_composition();
        }
    }

    pub(crate) fn morph_buttons(
        &self,
        gesture: MorphGesture,
    ) -> impl DoubleEndedIterator<Item = ([f32; 4], &str)> + Clone {
        self.layers
            .morphs
            .active()
            .into_iter()
            .chain(self.layers.morph_groups.values())
            .flat_map(move |stack| {
                stack
                    .rectangles
                    .iter()
                    .zip(&stack.actions)
                    .filter_map(move |(rect, actions)| {
                        Some(((*rect)?, actions[gesture.index()].as_deref()?))
                    })
            })
    }

    pub(crate) fn pointer_regions(&self) -> impl Iterator<Item = [f32; 4]> + Clone {
        [
            self.layers.wallpaper.as_ref(),
            self.layers.filters.active(),
            self.layers.morphs.active(),
        ]
        .into_iter()
        .flatten()
        .chain(self.layers.morph_groups.values())
        .flat_map(|stack| stack.pointer_regions.iter().copied())
        .chain(self.composed_heart_bounds())
    }

    pub(crate) fn uses_desktop_audio(&self) -> bool {
        self.layers.active_stacks().any(|stack| stack.uses_audio)
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
            morph_groups: self
                .layers
                .morph_groups
                .iter()
                .map(|(name, stack)| MorphGroup {
                    name: name.clone(),
                    layers: stack.names.clone(),
                    fps: stack.fps,
                })
                .collect(),
            filter_fps,
            morph_fps,
            wallpaper,
            wallpaper_fps,
        }
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

    pub(super) fn layer_clock_at(&self, now: Instant) -> Instant {
        self.layers.paused_at.unwrap_or(now)
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
            .chain(self.layers.morph_groups.values_mut())
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
