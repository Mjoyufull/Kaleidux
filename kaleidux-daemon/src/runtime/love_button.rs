//! Keeps the heart in sync with the wallpaper actually selected on each output.

use crate::{main_loop::MainLoopContext, monitor_manager::MonitorManager, renderer::Renderer};
use std::{collections::HashMap, path::Path, time::Instant};

impl MainLoopContext {
    pub(crate) fn sync_love_buttons(&mut self, composed: bool, now: Instant) {
        let position = self.monitor_manager.love_heart_position();
        for (name, renderer) in &mut self.renderers {
            let source = self
                .monitor_manager
                .outputs
                .get(name)
                .and_then(|output| output.current_path.as_deref());
            let Some((source, position)) = source.zip(position).filter(|(source, _)| {
                self.preview.is_none()
                    && !renderer.content_is_loading()
                    && !self.pending_video_switches.contains_key(name)
                    && (!crate::renderer::is_shader_file(source) && !crate::scene::is_scene(source)
                        || renderer.shader_wallpaper().map(Path::new) == Some(source))
            }) else {
                renderer.clear_love_mark();
                continue;
            };
            if renderer
                .love_mark()
                .is_some_and(|mark| mark.source == source)
            {
                renderer.set_love_mark_position(position);
            } else {
                let loved = self.monitor_manager.is_loved(source);
                let animation =
                    (loved && self.monitor_manager.animate_loved_wallpapers()).then_some(now);
                renderer.set_love_mark(source, position, loved, animation, composed);
            }
        }
    }
}

pub(crate) fn toggle(ctx: &mut MainLoopContext, output: &str) -> anyhow::Result<()> {
    if ctx.preview.is_some() || ctx.powered_off_outputs.contains(output) {
        return Ok(());
    }
    let Some(mark) = ctx.renderers.get(output).and_then(Renderer::love_mark) else {
        return Ok(());
    };
    let source = mark.source.to_string_lossy().into_owned();
    let multiplier = if mark.loved { 1.0 } else { 2.0 };
    set(
        &mut ctx.monitor_manager,
        &mut ctx.renderers,
        source,
        multiplier,
    )
}

pub(super) fn set(
    manager: &mut MonitorManager,
    renderers: &mut HashMap<String, Renderer>,
    source: String,
    multiplier: f32,
) -> anyhow::Result<()> {
    manager.love_file(source.clone(), multiplier)?;
    let now = Instant::now();
    for renderer in renderers.values_mut() {
        renderer.set_loved(Path::new(&source), multiplier > 1.0, now);
    }
    Ok(())
}
