//! Applies output configuration and recreates video sessions whose cadence changed.

use crate::content::sessions::stop_video_player_in_background;
use crate::content::switch::{
    ContentSwitchContext, ContentSwitchRequest, switch_wallpaper_content,
};
use crate::main_loop::CommandContext;
use crate::orchestration::Config;
use crate::queue::ContentType;
use std::collections::HashMap;

pub(super) fn apply(config: Config, ctx: CommandContext<'_>) {
    let old_fps: HashMap<_, _> = ctx
        .monitor_manager
        .outputs
        .iter()
        .map(|(name, output)| {
            (
                name.clone(),
                (output.config.video_fps, output.current_path.clone()),
            )
        })
        .collect();
    ctx.monitor_manager.update_config(config);
    let mut restarts = Vec::new();
    for (name, renderer) in ctx.renderers.iter_mut() {
        if let Some(output_config) = ctx.monitor_manager.get_output_config(name) {
            if old_fps
                .get(name)
                .is_some_and(|(old, _)| *old != output_config.video_fps)
                && (renderer.valid_content_type == ContentType::Video
                    || ctx.pending_video_switches.contains_key(name))
                && let Some(path) = old_fps.get(name).and_then(|(_, path)| path.clone())
            {
                restarts.push((name.clone(), path));
            }
            if let Some(player) = ctx.video_players.get_mut(name) {
                player.set_volume(output_config.volume as f64 / 100.0);
            }
            renderer.apply_config(output_config);
        }
    }
    let batch = rand::random::<u64>();
    for (name, path) in restarts {
        // Bypass same-source native reuse, whose decoder key contains old FPS.
        if let Some(player) = ctx.video_players.remove(&name) {
            stop_video_player_in_background(name.clone(), player);
        }
        if let Some(output) = ctx.monitor_manager.outputs.get_mut(&name) {
            output.current_path = Some(path.clone());
        }
        switch_wallpaper_content(
            ContentSwitchRequest {
                name,
                path,
                content_type: ContentType::Video,
                batch_id: Some(batch),
                batch_trigger_time: Some(ctx.loop_start),
                shared_image_target: None,
                log_prefix: "RELOAD",
            },
            ContentSwitchContext {
                metrics: ctx.metrics,
                next_session_id: ctx.next_session_id,
                frame_mailbox: ctx.frame_mailbox,
                monitor_manager: ctx.monitor_manager,
                renderers: ctx.renderers,
                video_players: ctx.video_players,
                pending_video_switches: ctx.pending_video_switches,
                pending_image_video_stops: ctx.pending_image_video_stops,
                pending_video_sessions: ctx.pending_video_sessions,
                image_tx: ctx.image_tx,
                player_tx: ctx.player_tx,
                player_event_tx: ctx.player_event_tx,
                shutdown_flag: ctx.shutdown_flag,
                mpv_native_targets: ctx.mpv_native_targets,
                mpv_composed_targets: ctx.mpv_composed_targets,
            },
        );
    }
}
