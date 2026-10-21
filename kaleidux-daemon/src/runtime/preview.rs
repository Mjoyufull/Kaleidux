//! Owns paused original players and GPU snapshots until preview apply or cancel.

use crate::content::sessions::{set_pending_video_session, stop_video_player_in_background};
use crate::main_loop::CommandContext;
use std::collections::HashMap;

pub(crate) struct PreviewState {
    queues: crate::monitor_manager::preview::QueueSnapshot,
    originals: HashMap<
        String,
        (
            crate::renderer::preview::FrozenWallpaper,
            Option<crate::video::VideoPlayer>,
        ),
    >,
}

impl PreviewState {
    pub(crate) fn retire_output(&mut self, name: &str) {
        self.queues.retire_output(name);
        if let Some((_wallpaper, Some(player))) = self.originals.remove(name) {
            stop_video_player_in_background(name.to_owned(), player);
        }
    }
    pub(crate) fn is_loading(&self, ctx: &CommandContext<'_>) -> bool {
        self.originals.keys().any(|name| {
            ctx.pending_video_switches.contains_key(name)
                || ctx
                    .renderers
                    .get(name)
                    .is_some_and(|renderer| renderer.content_is_loading())
        })
    }
    pub(crate) fn begin(
        queues: crate::monitor_manager::preview::QueueSnapshot,
        names: impl Iterator<Item = String>,
        renderers: &mut HashMap<String, crate::renderer::Renderer>,
        players: &mut HashMap<String, crate::video::VideoPlayer>,
    ) -> Self {
        let mut originals = HashMap::new();
        for name in names {
            if let Some(renderer) = renderers.get_mut(&name) {
                let player = players.remove(&name);
                if let Some(player) = player.as_ref() {
                    let _ = player.pause();
                }
                originals.insert(name, (renderer.freeze_wallpaper(), player));
            }
        }
        Self { queues, originals }
    }

    pub(crate) fn finish(self, apply: bool, ctx: &mut CommandContext<'_>) {
        ctx.monitor_manager.end_preview(self.queues, apply);
        for (name, (wallpaper, player)) in self.originals {
            if apply {
                if let Some(player) = player {
                    stop_video_player_in_background(name, player);
                }
                continue;
            }
            ctx.pending_video_switches.remove(&name);
            set_pending_video_session(ctx.pending_video_sessions, &name, None);
            ctx.frame_mailbox.clear_source(&name);
            crate::image::runtime_prefetch::begin_image_prefetch_generation(&name);
            if let Some(candidate) = ctx.video_players.remove(&name) {
                stop_video_player_in_background(name.clone(), candidate);
            }
            if let Some(candidate) = ctx.pending_image_video_stops.remove(&name) {
                stop_video_player_in_background(name.clone(), candidate);
            }
            let Some(renderer) = ctx.renderers.get_mut(&name) else {
                if let Some(player) = player {
                    stop_video_player_in_background(name, player);
                }
                continue;
            };
            renderer.restore_wallpaper(wallpaper);
            if let Some(player) = player {
                ctx.frame_mailbox.restore_session(
                    &name,
                    player.session_id(),
                    ctx.next_session_id.saturating_sub(1),
                );
                if !ctx.monitor_manager.is_paused()
                    && !ctx.display_power_suspended
                    && !ctx.powered_off_outputs.contains(&name)
                {
                    let _ = player.resume();
                    player.request_video_frame();
                }
                ctx.video_players.insert(name, player);
            }
        }
    }
}
