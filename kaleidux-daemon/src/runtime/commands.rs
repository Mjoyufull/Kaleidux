use crate::content::sessions::{set_pending_video_session, stop_video_player_in_background};
use crate::content::switch::{
    ContentSwitchContext, ContentSwitchRequest, switch_wallpaper_content,
};
use crate::image::runtime_cache::ordered_pending_content_switches;
use crate::main_loop::CommandContext;
use crate::orchestration;
use kaleidux_common::{Request, Response};
use std::sync::atomic::Ordering;
use tracing::{error, info};

/// Handle an IPC command request.
pub(crate) async fn handle_command(req: Request, ctx: CommandContext<'_>) -> Response {
    let CommandContext {
        monitor_manager,
        renderers,
        video_players,
        pending_video_switches,
        pending_image_video_stops,
        pending_video_sessions,
        metrics,
        frame_mailbox,
        image_tx,
        player_tx,
        player_event_tx,
        next_session_id,
        loop_start,
        shutdown_flag,
    } = ctx;
    match req {
        Request::PerfSnapshot => Response::PerfSnapshot(metrics.perf_snapshot()),
        Request::QueryOutputs => {
            let outputs = renderers
                .iter()
                .map(|(n, r)| kaleidux_common::OutputInfo {
                    name: n.clone(),
                    width: r.config.width,
                    height: r.config.height,
                    current_wallpaper: monitor_manager
                        .outputs
                        .get(n)
                        .and_then(|o| o.current_path.as_ref().map(|p| p.display().to_string())),
                })
                .collect();
            Response::OutputInfo(outputs)
        }
        Request::Next { output } => {
            let changes = monitor_manager.handle_next(output);
            let batch = rand::random::<u64>();
            let ordered_changes = ordered_pending_content_switches(renderers, changes);
            for change in ordered_changes {
                switch_wallpaper_content(
                    ContentSwitchRequest {
                        name: change.name,
                        path: change.path,
                        content_type: change.content_type,
                        batch_id: Some(batch),
                        batch_trigger_time: Some(loop_start),
                        shared_image_target: change.shared_image_target,
                        log_prefix: "NEXT",
                    },
                    ContentSwitchContext {
                        metrics,
                        next_session_id,
                        frame_mailbox,
                        monitor_manager,
                        renderers,
                        video_players,
                        pending_video_switches,
                        pending_image_video_stops,
                        pending_video_sessions,
                        image_tx,
                        player_tx,
                        player_event_tx,
                        shutdown_flag,
                    },
                );
            }
            Response::Ok
        }
        Request::Prev { output } => {
            let changes = monitor_manager.handle_prev(output);
            let batch = rand::random::<u64>();
            let ordered_changes = ordered_pending_content_switches(renderers, changes);
            for change in ordered_changes {
                switch_wallpaper_content(
                    ContentSwitchRequest {
                        name: change.name,
                        path: change.path,
                        content_type: change.content_type,
                        batch_id: Some(batch),
                        batch_trigger_time: Some(loop_start),
                        shared_image_target: change.shared_image_target,
                        log_prefix: "PREV",
                    },
                    ContentSwitchContext {
                        metrics,
                        next_session_id,
                        frame_mailbox,
                        monitor_manager,
                        renderers,
                        video_players,
                        pending_video_switches,
                        pending_image_video_stops,
                        pending_video_sessions,
                        image_tx,
                        player_tx,
                        player_event_tx,
                        shutdown_flag,
                    },
                );
            }
            Response::Ok
        }
        Request::Kill => {
            shutdown_flag.store(true, Ordering::SeqCst);
            Response::Ok
        }
        Request::Playlist(cmd) => monitor_manager.handle_playlist_command(cmd),
        Request::Blacklist(cmd) => monitor_manager.handle_blacklist_command(cmd),
        Request::LoveitList => Response::LoveitList(monitor_manager.get_loveitlist()),
        Request::Love { path, multiplier } => monitor_manager
            .love_file(path, multiplier)
            .map(|_| Response::Ok)
            .unwrap_or_else(|e| Response::Error(e.to_string())),
        Request::Unlove { path } => monitor_manager
            .unlove_file(path)
            .map(|_| Response::Ok)
            .unwrap_or_else(|e| Response::Error(e.to_string())),
        Request::History { output } => Response::History(monitor_manager.get_history(output)),
        Request::Reload => {
            info!("Reloading configuration...");
            match orchestration::Config::load().await {
                Ok(new_config) => {
                    monitor_manager.update_config(new_config);
                    for (name, r) in renderers.iter_mut() {
                        if let Some(cfg) = monitor_manager.get_output_config(name) {
                            r.apply_config(cfg);
                        }
                    }
                    info!("Configuration reloaded successfully");
                    Response::Ok
                }
                Err(e) => {
                    error!("Failed to reload config: {}", e);
                    Response::Error(format!("Failed to reload config: {}", e))
                }
            }
        }
        Request::Pause => {
            info!("[CMD] Pausing all video players and wallpaper cycling");
            for (name, player) in video_players.iter() {
                if let Err(e) = player.pause() {
                    error!("[CMD] Failed to pause video for {}: {}", name, e);
                }
            }
            monitor_manager.set_paused(true);
            Response::Ok
        }
        Request::Resume => {
            info!("[CMD] Resuming all video players and wallpaper cycling");
            for (name, player) in video_players.iter() {
                if let Err(e) = player.resume() {
                    error!("[CMD] Failed to resume video for {}: {}", name, e);
                }
            }
            monitor_manager.set_paused(false);
            Response::Ok
        }
        Request::Stop => {
            info!("[CMD] Stopping all video players");
            let names: Vec<String> = video_players.keys().cloned().collect();
            for name in names {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                frame_mailbox.clear_source(&name);
                if let Some(player) = video_players.remove(&name) {
                    stop_video_player_in_background(name, player);
                }
            }
            Response::Ok
        }
        Request::Clear { output } => {
            info!("[CMD] Clearing output: {:?}", output);
            let targets: Vec<String> = match output {
                Some(ref name) => {
                    if renderers.contains_key(name) {
                        vec![name.clone()]
                    } else {
                        return Response::Error(format!("Output not found: {}", name));
                    }
                }
                None => renderers.keys().cloned().collect(),
            };
            for name in targets {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                frame_mailbox.clear_source(&name);
                if let Some(vp) = video_players.remove(&name) {
                    stop_video_player_in_background(name.clone(), vp);
                }
                if let Some(r) = renderers.get_mut(&name) {
                    r.clear();
                }
            }
            Response::Ok
        }
    }
}
