//! Owns the favorite indicator and its composed renderer.

use super::{LayerPipelines, Signals, WgpuContext, draw, layer_frame};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(crate) const LOVE_LIFETIME: Duration = Duration::from_millis(900);
pub(crate) const LOVE_FRAME_INTERVAL: Duration = Duration::from_millis(33);

pub(crate) struct LoveMark {
    pub(crate) source: PathBuf,
    pub(crate) loved: bool,
    pub(crate) position: [f32; 2],
    pub(crate) animation: Option<Instant>,
    pub(super) composed: bool,
}

impl LoveMark {
    pub(crate) fn progress(&self, now: Instant) -> f32 {
        self.animation.map_or(1.0, |start| {
            (now.saturating_duration_since(start).as_secs_f32() / LOVE_LIFETIME.as_secs_f32())
                .min(1.0)
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct LoveVisual {
    position: [f32; 2],
    loved: bool,
    progress: f32,
}

impl LoveVisual {
    pub(super) fn resolve(mark: &mut Option<LoveMark>, now: Instant) -> Option<Self> {
        mark.as_mut().filter(|mark| mark.composed).map(|mark| {
            let progress = mark.progress(now);
            if progress == 1.0 {
                mark.animation = None;
            }
            LoveVisual {
                position: mark.position,
                loved: mark.loved,
                progress,
            }
        })
    }

    pub(super) fn render(
        self,
        pass: &mut wgpu::RenderPass<'_>,
        ctx: &WgpuContext,
        pipeline: &mut LayerPipelines,
        size: [u32; 2],
        now: Instant,
        signals: Signals,
    ) {
        let love = self;
        let mut frame = layer_frame(size, pipeline, now, love.progress, signals);
        frame.params = [
            love.position[0],
            love.position[1],
            f32::from(u8::from(love.loved)),
            1.0,
        ];
        pipeline.write_frame(ctx, frame);
        let binding = &pipeline.morph_bindings[0];
        pass.set_viewport(0.0, 0.0, size[0] as f32, size[1] as f32, 0.0, 1.0);
        scissor_heart(pass, size, love.position);
        draw(pass, &pipeline.pipelines[0], binding);
    }
}

pub(super) fn scissor_heart(pass: &mut wgpu::RenderPass<'_>, size: [u32; 2], position: [f32; 2]) {
    let radius = (size[0].min(size[1]) as f32 * 0.1).ceil();
    let center = [position[0] * size[0] as f32, position[1] * size[1] as f32];
    let x = (center[0] - radius).max(0.0) as u32;
    let y = (center[1] - radius).max(0.0) as u32;
    let width = ((center[0] + radius).ceil() as u32).min(size[0]) - x;
    let height = ((center[1] + radius).ceil() as u32).min(size[1]) - y;
    pass.set_scissor_rect(x, y, width, height);
}

impl super::super::Renderer {
    pub(crate) fn love_mark(&self) -> Option<&LoveMark> {
        self.layers.love.as_ref()
    }

    pub(crate) fn composed_heart_bounds(&self) -> Option<[f32; 4]> {
        let mark = self.love_mark().filter(|mark| mark.composed)?;
        let width = self.config.width.max(1) as f32;
        let height = self.config.height.max(1) as f32;
        let radius = width.min(height) * 0.042;
        let [x, y] = mark.position;
        let left = (x - radius / width).max(0.0);
        let top = (y - radius / height).max(0.0);
        let right = (x + radius / width).min(1.0);
        let bottom = (y + radius / height).min(1.0);
        Some([left, top, right - left, bottom - top])
    }

    pub(crate) fn set_love_mark(
        &mut self,
        source: &Path,
        position: [f32; 2],
        loved: bool,
        animation: Option<Instant>,
        composed: bool,
    ) {
        if composed && self.layers.love_pipeline.is_none() {
            self.layers.love_pipeline = Some(LayerPipelines::love(&self.ctx, self.config.format));
        }
        self.layers.love = Some(LoveMark {
            source: source.to_owned(),
            position,
            loved,
            animation,
            composed,
        });
        self.needs_redraw |= composed;
    }

    pub(crate) fn set_love_mark_position(&mut self, position: [f32; 2]) {
        if let Some(mark) = &mut self.layers.love
            && mark.position != position
        {
            mark.position = position;
            self.needs_redraw |= mark.composed;
        }
    }

    pub(crate) fn clear_love_mark(&mut self) {
        if let Some(mark) = self.layers.love.take() {
            self.needs_redraw |= mark.composed;
        }
    }

    pub(crate) fn set_loved(&mut self, source: &Path, loved: bool, now: Instant) {
        if let Some(mark) = &mut self.layers.love
            && mark.source == source
            && mark.loved != loved
        {
            mark.loved = loved;
            mark.animation = loved.then_some(now);
            self.needs_redraw |= mark.composed;
        }
    }
}
