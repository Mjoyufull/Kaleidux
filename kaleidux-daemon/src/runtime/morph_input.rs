//! Sends platform pointer input to the shared morph and shader state.

use crate::main_loop::MainLoopContext;
use kaleidux_common::{LayerInputs, MorphEvent, MorphGesture};

pub(crate) fn position(ctx: &mut MainLoopContext, output: &str, pointer: [f32; 4], active: bool) {
    if ctx.powered_off_outputs.contains(output) {
        return;
    }
    let inputs = LayerInputs {
        pointer: Some(pointer),
        pointer_active: Some(active),
        ..Default::default()
    };
    if let Some(renderer) = ctx.renderers.get_mut(output) {
        renderer.apply_layer_inputs(&inputs);
    }
    if let Some(player) = ctx.video_players.get(output) {
        player.apply_web_inputs(&inputs);
    }
}

pub(crate) fn activate(
    ctx: &mut MainLoopContext,
    output: String,
    pressed: [f32; 2],
    released: [f32; 2],
    gesture: MorphGesture,
) {
    if ctx.powered_off_outputs.contains(&output) {
        return;
    }
    let Some(renderer) = ctx.renderers.get(&output) else {
        return;
    };
    if gesture == MorphGesture::Primary
        && let Some(bounds) = renderer.composed_heart_bounds()
        && local_position(bounds, pressed).is_some()
        && local_position(bounds, released).is_some()
    {
        if let Err(error) = super::love_button::toggle(ctx, &output) {
            tracing::warn!("Love button failed: {error:#}");
        }
        return;
    }
    if let Some((action, position)) =
        renderer
            .morph_buttons(gesture)
            .rev()
            .find_map(|(bounds, action)| {
                local_position(bounds, pressed)?;
                let position = local_position(bounds, released)?;
                Some((action, position))
            })
    {
        ctx.script_manager.morph_action(action, &output, position);
        if ctx.morph_events.receiver_count() != 0 {
            let _ = ctx.morph_events.send(MorphEvent {
                output,
                action: action.to_owned(),
                gesture,
                position,
            });
        }
    }
}

pub(crate) fn pixel_region([x, y, width, height]: [f32; 4], (w, h): (u32, u32)) -> [i32; 4] {
    let (w, h) = (w as f32, h as f32);
    let left = (x * w).floor();
    let top = (y * h).floor();
    let right = ((x + width) * w).ceil().min(w);
    let bottom = ((y + height) * h).ceil().min(h);
    [
        left as i32,
        top as i32,
        (right - left) as i32,
        (bottom - top) as i32,
    ]
}

fn local_position([x, y, w, h]: [f32; 4], [px, py]: [f32; 2]) -> Option<[f32; 2]> {
    (px >= x && px < x + w && py >= y && py < y + h).then_some([(px - x) / w, (py - y) / h])
}
