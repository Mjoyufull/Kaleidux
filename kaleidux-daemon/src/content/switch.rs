use crate::background::{self, BackgroundWorkKind};
use crate::content::sessions::{
    PendingVideoSessions, PendingVideoSwitch, VideoPlayerResult, set_pending_video_session,
    stop_video_player_in_background,
};
use crate::content::video_start::{
    VideoPlayerStartContext, VideoPlayerStartRequest, create_and_start_video_player,
};
use crate::image::prefetch;
use crate::image::runtime_cache::{
    begin_image_prefetch_generation, request_prepared_image_payload, schedule_image_prefetch_plan,
};
use crate::main_loop::{LoadedImage, PlayerEventMsg};
use crate::metrics;
use crate::monitor_manager;
use crate::orchestration::VideoFpsProfile;
use crate::queue;
use crate::renderer;
use crate::video;
use kaleidux_common::Transition;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::{debug, error, info, warn};

const DEFAULT_LOW_POWER_MAX_PUBLISH_FPS: u32 = 12;
const DEFAULT_MEDIUM_MAX_PUBLISH_FPS: u32 = 24;
const DEFAULT_HIGH_MAX_PUBLISH_FPS: u32 = 48;
const MAX_CONFIGURED_MAX_PUBLISH_FPS: u32 = 120;

static STOP_VIDEO_ON_IMAGE_SWITCH: once_cell::sync::Lazy<bool> = once_cell::sync::Lazy::new(|| {
    parse_stop_video_on_image_switch(std::env::var("KLD_STOP_VIDEO_ON_IMAGE_SWITCH").ok())
});

fn parse_stop_video_on_image_switch(value: Option<String>) -> bool {
    match value.as_deref().map(str::trim).map(str::to_ascii_lowercase) {
        Some(value)
            if matches!(
                value.as_str(),
                "0" | "false" | "no" | "off" | "defer" | "legacy"
            ) =>
        {
            false
        }
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on" | "immediate") => true,
        _ => true,
    }
}

fn stop_video_on_image_switch() -> bool {
    *STOP_VIDEO_ON_IMAGE_SWITCH
}

fn default_max_publish_fps(profile: VideoFpsProfile) -> Option<u32> {
    match profile {
        VideoFpsProfile::Low => Some(DEFAULT_LOW_POWER_MAX_PUBLISH_FPS),
        VideoFpsProfile::Medium => Some(DEFAULT_MEDIUM_MAX_PUBLISH_FPS),
        VideoFpsProfile::High => Some(DEFAULT_HIGH_MAX_PUBLISH_FPS),
        VideoFpsProfile::Unlimited => None,
    }
}

fn configured_max_publish_fps(profile: VideoFpsProfile) -> Option<u32> {
    let Ok(value) = std::env::var("KLD_LOW_POWER_MAX_PUBLISH_FPS") else {
        return default_max_publish_fps(profile);
    };

    let Ok(fps) = value.trim().parse::<u32>() else {
        return default_max_publish_fps(profile);
    };

    if fps == 0 {
        return None;
    }

    Some(fps.min(MAX_CONFIGURED_MAX_PUBLISH_FPS))
}

pub(crate) struct ContentSwitchRequest {
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    pub(crate) content_type: queue::ContentType,
    pub(crate) batch_id: Option<u64>,
    pub(crate) batch_trigger_time: Option<Instant>,
    pub(crate) shared_image_target: Option<(u32, u32)>,
    pub(crate) log_prefix: &'static str,
}

pub(crate) struct ContentSwitchContext<'a> {
    pub(crate) metrics: &'a Arc<metrics::PerformanceMetrics>,
    pub(crate) next_session_id: &'a mut u64,
    pub(crate) frame_mailbox: &'a video::LatestFrameMailbox,
    pub(crate) monitor_manager: &'a monitor_manager::MonitorManager,
    pub(crate) renderers: &'a mut HashMap<String, renderer::Renderer>,
    pub(crate) video_players: &'a mut HashMap<String, video::VideoPlayer>,
    pub(crate) pending_video_switches: &'a mut HashMap<String, PendingVideoSwitch>,
    pub(crate) pending_image_video_stops: &'a mut HashMap<String, video::VideoPlayer>,
    pub(crate) pending_video_sessions: &'a PendingVideoSessions,
    pub(crate) image_tx: &'a tokio::sync::mpsc::Sender<LoadedImage>,
    pub(crate) player_tx: &'a tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    pub(crate) player_event_tx: &'a tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
    pub(crate) shutdown_flag: &'a Arc<AtomicBool>,
}

fn resolve_transition_for_output(
    monitor_manager: &monitor_manager::MonitorManager,
    name: &str,
) -> Transition {
    monitor_manager
        .outputs
        .get(name)
        .map(|orchestrator| {
            if matches!(orchestrator.config.transition, Transition::Random) {
                let picked = crate::shaders::ShaderManager::pick_random_transition();
                debug!(
                    "[TRANSITION] {}: Resolved Random transition to: {}",
                    name,
                    picked.name()
                );
                picked
            } else {
                orchestrator.config.transition.clone()
            }
        })
        .unwrap_or_default()
}

/// Helper function to switch wallpaper content for an output.
pub(crate) fn switch_wallpaper_content(
    request: ContentSwitchRequest,
    ctx: ContentSwitchContext<'_>,
) {
    let ContentSwitchRequest {
        name,
        path,
        content_type,
        batch_id,
        batch_trigger_time,
        shared_image_target,
        log_prefix,
    } = request;
    let ContentSwitchContext {
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
    } = ctx;

    info!("{}: {} -> {:?}", log_prefix, name, path.display());
    debug!(
        "[SWITCH] {}: content_type={:?}, renderer exists={}",
        name,
        content_type,
        renderers.contains_key(&name)
    );

    let session_id = *next_session_id;
    *next_session_id += 1;

    frame_mailbox.clear_source(&name);
    if let Some(old_pending_stop) = pending_image_video_stops.remove(&name) {
        stop_video_player_in_background(name.clone(), old_pending_stop);
    }
    let mut should_prepare_video = false;
    let mut backend_request = video::VideoBackendRequest::Auto;
    let mut broker_start_position_ns = None;
    if let Some(r) = renderers.get_mut(&name) {
        let resolved_transition = resolve_transition_for_output(monitor_manager, &name);
        r.active_batch_id = batch_id;
        r.batch_start_time = batch_trigger_time;
        r.active_transition = resolved_transition.clone();

        if content_type == queue::ContentType::Image {
            let mut prior_video_player = video_players.remove(&name);
            pending_video_switches.remove(&name);
            set_pending_video_session(pending_video_sessions, &name, None);
            r.set_content_type(content_type);
            r.active_image_session_id = session_id;
            r.active_video_session_id = 0;
            r.switch_content();
            let output_width = r.config.width;
            let output_height = r.config.height;
            let (target_width, target_height) = shared_image_target
                .filter(|(width, height)| *width >= output_width && *height >= output_height)
                .unwrap_or((output_width, output_height));

            let name_clone = name.clone();
            let path_clone = path.to_path_buf();
            let tx = image_tx.clone();
            let image_session_id = session_id;
            let shutdown_flag = shutdown_flag.clone();
            let metrics = metrics.clone();
            if let Some(old_video_player) = prior_video_player.take() {
                if stop_video_on_image_switch() {
                    stop_video_player_in_background(name.clone(), old_video_player);
                } else {
                    pending_image_video_stops.insert(name.clone(), old_video_player);
                }
            }

            debug!(
                "[ASSET] {}: Offloading image decode: {} target={}x{} output={}x{} shared_target={}",
                name,
                path.display(),
                target_width,
                target_height,
                output_width,
                output_height,
                shared_image_target.is_some()
            );
            tokio::spawn(async move {
                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Skipping image decode because shutdown is in progress",
                        name_clone
                    );
                    return;
                }

                let request_start = Instant::now();
                let decode_result = request_prepared_image_payload(
                    &path_clone,
                    target_width,
                    target_height,
                    BackgroundWorkKind::ImageDecode,
                    &metrics,
                )
                .await;

                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Discarding decoded image because shutdown is in progress",
                        name_clone
                    );
                    return;
                }

                // Send decoded image (or error) to channel
                match decode_result {
                    Ok(mut payload) => {
                        let observed_total = request_start.elapsed();
                        payload.profile.permit_wait =
                            observed_total.saturating_sub(payload.profile.cpu_duration());
                        if let Err(e) = tx
                            .send(LoadedImage {
                                name: name_clone.clone(),
                                session_id: image_session_id,
                                data: Some(payload.data),
                                width: payload.width,
                                height: payload.height,
                                profile: Some(payload.profile),
                                _path: path_clone,
                            })
                            .await
                        {
                            debug!(
                                "[ASSET] {}: Failed to send decoded image (channel closed): {}",
                                name_clone, e
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to decode image {}: {}", path_clone.display(), e);
                        let _ = tx
                            .send(LoadedImage {
                                name: name_clone,
                                session_id: image_session_id,
                                data: None,
                                width: 0,
                                height: 0,
                                profile: None,
                                _path: path_clone,
                            })
                            .await;
                    }
                }
            });
        } else {
            backend_request = video::VideoBackendRequest::Auto;
            if let Some((peer_name, peer_player)) = video_players
                .iter()
                .find(|(output, player)| {
                    output.as_str() != name.as_str()
                        && monitor_manager
                            .outputs
                            .get(output.as_str())
                            .and_then(|o| o.current_path.as_deref())
                            .is_some_and(|p| p == path.as_path())
                        && player.current_position_ns().is_some()
                })
                .map(|(output, player)| (output.clone(), player))
            {
                broker_start_position_ns = peer_player.current_position_ns();
                metrics.record_shared_broker_hit();
                debug!(
                    "[VIDEO] {}: Shared broker prototype hit via peer {} (start_position_ms={:.1})",
                    name,
                    peer_name,
                    broker_start_position_ns.unwrap_or(0) as f64 / 1_000_000.0
                );
            } else {
                metrics.record_shared_broker_miss();
            }
            set_pending_video_session(pending_video_sessions, &name, Some(session_id));
            pending_video_switches.insert(
                name.clone(),
                PendingVideoSwitch {
                    session_id,
                    batch_id,
                    batch_trigger_time,
                    transition: resolved_transition,
                },
            );
            should_prepare_video = true;
        }
    } else {
        set_pending_video_session(pending_video_sessions, &name, None);
        pending_video_switches.remove(&name);
        if let Some(vp) = video_players.remove(&name) {
            stop_video_player_in_background(name.to_string(), vp);
        }
        warn!(
            "[SWITCH] {}: Skipping content switch because renderer no longer exists",
            name
        );
    }

    if content_type == queue::ContentType::Video && should_prepare_video {
        debug!(
            "[TRANSITION] {}: Preparing deferred video player (session_id={})",
            name, session_id
        );
        let volume = monitor_manager
            .outputs
            .get(&name)
            .map(|o| o.config.volume as f64 / 100.0)
            .unwrap_or_else(|| {
                warn!(
                    "[VIDEO] {}: Missing output config while creating player; defaulting volume to 0",
                    name
                );
                0.0
            });
        let max_publish_fps = monitor_manager
            .outputs
            .get(&name)
            .and_then(|output| configured_max_publish_fps(output.config.video_fps));
        create_and_start_video_player(
            VideoPlayerStartRequest {
                path: path.clone(),
                output_name: name.clone(),
                session_id,
                volume,
                backend_request,
                start_position_ns: broker_start_position_ns,
                max_publish_fps,
            },
            VideoPlayerStartContext {
                frame_mailbox,
                player_tx,
                player_event_tx,
                metrics: metrics.clone(),
                pending_video_sessions: pending_video_sessions.clone(),
                shutdown_flag: shutdown_flag.clone(),
            },
        );
    }

    let prefetch_generation = begin_image_prefetch_generation(&name);
    let prefetch_plan = prefetch::build_plan(monitor_manager, renderers, &name);
    schedule_image_prefetch_plan(&name, prefetch_generation, prefetch_plan, metrics.clone());
}

#[cfg(test)]
mod tests;
