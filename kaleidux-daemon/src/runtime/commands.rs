use crate::content::sessions::{set_pending_video_session, stop_video_player_in_background};
use crate::content::switch::{
    ContentSwitchContext, ContentSwitchRequest, switch_wallpaper_content,
};
use crate::image::runtime_cache::ordered_pending_content_switches;
use crate::main_loop::CommandContext;
use crate::orchestration;
use kaleidux_common::{Request, Response};
use std::collections::{HashMap, HashSet};
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
        display_power_suspended,
        powered_off_outputs,
        mpv_native_targets,
        mpv_composed_targets,
    } = ctx;
    let target = match &req {
        Request::Jump { output, .. }
        | Request::Set { output, .. }
        | Request::Img { output, .. }
        | Request::Next { output }
        | Request::Prev { output }
        | Request::Clear { output }
        | Request::History { output } => Some(output.clone()),
        _ => None,
    };
    let selected_output = if let Some(target) = target {
        match monitor_manager.command_output(
            target,
            renderers
                .iter()
                .map(|(name, renderer)| (name, renderer.config.width, renderer.config.height)),
        ) {
            Ok(output) => output,
            Err(error) => return Response::Error(error.to_string()),
        }
    } else {
        None
    };
    let req = match req {
        Request::Jump { path, .. } => Request::Jump {
            path,
            output: selected_output,
        },
        Request::Set { path, .. } => Request::Set {
            path,
            output: selected_output,
        },
        Request::Img { path, .. } => Request::Img {
            path,
            output: selected_output,
        },
        Request::Next { .. } => Request::Next {
            output: selected_output,
        },
        Request::Prev { .. } => Request::Prev {
            output: selected_output,
        },
        Request::Clear { .. } => Request::Clear {
            output: selected_output,
        },
        Request::History { .. } => Request::History {
            output: selected_output,
        },
        request => request,
    };
    let req = match req {
        Request::Jump { path, output } => {
            if let Err(error) = monitor_manager.prepare_media_selection(&path, &output, Some(true))
            {
                return Response::Error(error.to_string());
            }
            Request::Next { output }
        }
        Request::Set { path, output } => {
            if let Err(error) = monitor_manager.prepare_media_selection(&path, &output, Some(false))
            {
                return Response::Error(error.to_string());
            }
            Request::Next { output }
        }
        Request::Img { path, output } => {
            if let Err(error) = monitor_manager.prepare_media_selection(&path, &output, None) {
                return Response::Error(error.to_string());
            }
            Request::Next { output }
        }
        request => request,
    };
    match req {
        Request::Jump { .. } | Request::Set { .. } | Request::Img { .. } => {
            unreachable!("normalized image selection")
        }
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
                        mpv_native_targets,
                        mpv_composed_targets,
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
                        mpv_native_targets,
                        mpv_composed_targets,
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
                            if let Some(player) = video_players.get_mut(name) {
                                player.set_volume(cfg.volume as f64 / 100.0);
                            }
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
            info!("[CMD] Manual pause requested");
            let transitioned = monitor_manager.set_paused(true);
            if transitioned {
                pause_active_video_players(video_players, pending_image_video_stops);
            }
            Response::Ok
        }
        Request::Resume => {
            info!("[CMD] Manual resume requested");
            let transitioned = monitor_manager.set_paused(false);
            if transitioned {
                resume_active_video_players(
                    video_players,
                    pending_image_video_stops,
                    display_power_suspended,
                    powered_off_outputs,
                );
            } else if monitor_manager.is_paused() {
                info!(
                    "[CMD] Manual pause cleared, but cycling remains inhibited by: {:?}",
                    monitor_manager.inhibitors()
                );
            }
            Response::Ok
        }
        Request::Inhibit { reason } => match monitor_manager.inhibit(reason) {
            Ok(transitioned) => {
                if transitioned {
                    info!("[CMD] Inhibit requested; pausing video players");
                    pause_active_video_players(video_players, pending_image_video_stops);
                }
                Response::Ok
            }
            Err(e) => Response::Error(e.to_string()),
        },
        Request::Uninhibit { reason } => match monitor_manager.uninhibit(&reason) {
            Ok(transitioned) => {
                if transitioned {
                    info!("[CMD] All pause reasons cleared; resuming video players");
                    resume_active_video_players(
                        video_players,
                        pending_image_video_stops,
                        display_power_suspended,
                        powered_off_outputs,
                    );
                } else if monitor_manager.is_paused() {
                    info!(
                        "[CMD] Inhibitor removed, but cycling remains paused (manual_pause={}, inhibitors={:?})",
                        monitor_manager.is_manual_paused(),
                        monitor_manager.inhibitors()
                    );
                }
                Response::Ok
            }
            Err(e) => Response::Error(e.to_string()),
        },
        Request::Inhibitors => Response::Inhibitors(monitor_manager.inhibitors()),
        Request::Stop => {
            info!("[CMD] Stopping all video players");
            let names: HashSet<String> = video_players
                .keys()
                .chain(pending_video_switches.keys())
                .chain(pending_image_video_stops.keys())
                .cloned()
                .collect();
            for name in names {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                frame_mailbox.clear_source(&name);
                if let Some(player) = video_players.remove(&name) {
                    stop_video_player_in_background(name.clone(), player);
                }
                if let Some(player) = pending_image_video_stops.remove(&name) {
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
                if let Some(vp) = pending_image_video_stops.remove(&name) {
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

fn pause_active_video_players(
    video_players: &HashMap<String, crate::video::VideoPlayer>,
    pending_image_video_stops: &HashMap<String, crate::video::VideoPlayer>,
) {
    for (name, player) in video_players.iter().chain(pending_image_video_stops.iter()) {
        if let Err(e) = player.pause() {
            error!("[CMD] Failed to pause video for {}: {}", name, e);
        }
    }
}

fn resume_active_video_players(
    video_players: &HashMap<String, crate::video::VideoPlayer>,
    pending_image_video_stops: &HashMap<String, crate::video::VideoPlayer>,
    display_power_suspended: bool,
    powered_off_outputs: &std::collections::HashSet<String>,
) {
    if display_power_suspended {
        info!("[CMD] Video resume deferred until compositor outputs are powered on");
    } else {
        for (name, player) in video_players.iter().chain(pending_image_video_stops.iter()) {
            if powered_off_outputs.contains(name) {
                continue;
            }
            if let Err(e) = player.resume() {
                error!("[CMD] Failed to resume video for {}: {}", name, e);
            } else {
                // Demand-driven native playback needs one seed credit after pause.
                player.request_video_frame();
            }
        }
    }
}
