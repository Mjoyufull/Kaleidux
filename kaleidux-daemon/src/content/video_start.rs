use crate::background::{self, BackgroundWorkKind};
use crate::content::sessions::{
    PendingVideoSessions, VideoPlayerResult, pending_video_session_matches,
};
use crate::metrics;
use crate::runtime::timing::duration_ms;
use crate::video;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::{debug, error, warn};

pub(crate) struct VideoPlayerStartRequest {
    pub(crate) path: PathBuf,
    pub(crate) output_name: String,
    pub(crate) session_id: u64,
    pub(crate) volume: f64,
    pub(crate) backend_request: video::VideoBackendRequest,
    pub(crate) decode_group_id: Option<u64>,
    pub(crate) start_position_ns: Option<u64>,
    pub(crate) max_publish_fps: Option<u32>,
    pub(crate) render_size: Option<(u32, u32)>,
    pub(crate) mpv_native_target: Option<video::MpvNativeVideoTarget>,
    pub(crate) mpv_composed_target: Option<video::MpvComposedVideoTarget>,
}

pub(crate) struct VideoPlayerStartContext<'a> {
    pub(crate) frame_mailbox: &'a video::LatestFrameMailbox,
    pub(crate) player_tx: &'a crate::main_loop::PlayerReadySender,
    pub(crate) player_event_tx: &'a crate::main_loop::PlayerEventSender,
    pub(crate) metrics: Arc<metrics::PerformanceMetrics>,
    pub(crate) pending_video_sessions: PendingVideoSessions,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
}

pub(crate) fn create_and_start_video_player(
    request: VideoPlayerStartRequest,
    ctx: VideoPlayerStartContext<'_>,
) {
    let VideoPlayerStartRequest {
        path,
        output_name,
        session_id,
        volume,
        backend_request,
        decode_group_id,
        start_position_ns,
        max_publish_fps,
        render_size,
        mpv_native_target,
        mpv_composed_target,
    } = request;
    let VideoPlayerStartContext {
        frame_mailbox,
        player_tx,
        player_event_tx,
        metrics,
        pending_video_sessions,
        shutdown_flag,
    } = ctx;

    let path_str = path.to_string_lossy().into_owned();
    let name_arc = Arc::new(output_name.clone());
    let name_str = output_name;
    let skipped_name = name_str.clone();
    let frame_mailbox_clone = frame_mailbox.clone();
    let player_tx_clone = player_tx.clone();
    let player_event_tx_clone = player_event_tx.clone();
    tokio::spawn(async move {
        let rt_handle = tokio::runtime::Handle::current();
        let Some(handle) = background::spawn_blocking_tracked_wait(
        BackgroundWorkKind::VideoPrepare,
        move || {
            let name_for_panic = name_str.clone();
            let player_tx_panic = player_tx_clone.clone();
            let session_id_panic = session_id;
            let pending_video_sessions_for_task = pending_video_sessions.clone();
            let should_abort = || {
                shutdown_flag.load(Ordering::SeqCst)
                    || !pending_video_session_matches(
                        &pending_video_sessions_for_task,
                        &name_str,
                        session_id,
                    )
            };

            if should_abort() {
                debug!(
                    "[VIDEO] {}: Skipping superseded video prepare task for session {} before player creation",
                    name_str, session_id
                );
                return;
            }

            let prepare_start = Instant::now();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let candidates = video::candidate_video_backends(backend_request);
                let num_candidates = candidates.len();
                let backend_is_explicitly_forced = video::backend_is_explicitly_forced(backend_request);
                let mut last_error = None;

                for (idx, candidate) in candidates.into_iter().enumerate() {
                    let is_last = idx + 1 == num_candidates;
                    let is_provisional = !is_last && !backend_is_explicitly_forced;

                    let (attempt_event_tx, attempt_event_rx) = if is_provisional {
                        let (tx, rx) = tokio::sync::mpsc::channel(32);
                        (tx, Some(rx))
                    } else {
                        (player_event_tx_clone.clone(), None)
                    };

                    let create_start = Instant::now();
                    let mut vp = match video::VideoPlayer::new(
                        &path_str,
                        name_arc.clone(),
                        session_id,
                        volume,
                        frame_mailbox_clone.clone(),
                        attempt_event_tx,
                        metrics.clone(),
                        candidate,
                        decode_group_id,
                        max_publish_fps,
                        render_size,
                        mpv_native_target.clone(),
                        mpv_composed_target.clone(),
                    ) {
                        Ok(player) => player,
                        Err(e) => {
                            frame_mailbox_clone.clear_session(&name_str, session_id);
                            if should_abort() {
                                return Ok(None);
                            }
                            if is_provisional {
                                warn!(
                                    "[VIDEO] {}: provisional {:?} backend creation failed ({:#}); trying next backend",
                                    name_str, candidate, e
                                );
                                last_error = Some(e);
                                continue;
                            } else {
                                error!("[VIDEO] {}: Failed to create video player: {}", name_str, e);
                                return Err(e);
                            }
                        }
                    };

                    let create_duration = create_start.elapsed();
                    vp.set_volume(volume);
                    if should_abort() {
                        let _ = vp.stop();
                        frame_mailbox_clone.clear_session(&name_str, session_id);
                        return Ok(None);
                    }

                    let prebuffer_start = Instant::now();
                    let mut prebuffer = match vp.prebuffer(should_abort) {
                        Ok(result) => result,
                        Err(e) => {
                            let _ = vp.stop();
                            frame_mailbox_clone.clear_session(&name_str, session_id);
                            if should_abort() {
                                debug!(
                                    "[VIDEO] {}: Aborting pre-buffer for superseded/shutdown session {}",
                                    name_str, session_id
                                );
                                return Ok(None);
                            }
                            if is_provisional {
                                warn!(
                                    "[VIDEO] {}: provisional {:?} backend prebuffer failed ({:#}); trying next backend",
                                    name_str, candidate, e
                                );
                                last_error = Some(e);
                                continue;
                            } else {
                                error!(
                                    "[VIDEO] {}: Pre-buffering failed: {}",
                                    name_str, e
                                );
                                return Err(e);
                            }
                        }
                    };
                    let prebuffer_duration = prebuffer_start.elapsed();

                    if let Some(position_ns) = start_position_ns.filter(|pos| *pos > 0) {
                        vp.set_start_position_ns(position_ns);
                        prebuffer.frame = None;
                    }

                    debug!(
                        "[VIDEO] {}: Player prepared in {:.1}ms (create {:.1}ms + prebuffer {:.1}ms, set_state {:.1}ms/{} + wait_state {:.1}ms settled={} current={:?} pending={:?} + pull_preroll {:.1}ms, preroll_frame={})",
                        name_str,
                        duration_ms(prepare_start.elapsed()),
                        duration_ms(create_duration),
                        duration_ms(prebuffer_duration),
                        duration_ms(prebuffer.profile.set_state),
                        prebuffer.profile.set_state_result,
                        duration_ms(prebuffer.profile.state_wait),
                        prebuffer.profile.state_wait_settled,
                        prebuffer.profile.current_state,
                        prebuffer.profile.pending_state,
                        duration_ms(prebuffer.profile.pull_preroll),
                        prebuffer.frame.is_some()
                    );

                    if should_abort() {
                        let _ = vp.stop();
                        frame_mailbox_clone.clear_session(&name_str, session_id);
                        return Ok(None);
                    }

                    if let Err(error) = vp.start() {
                        let _ = vp.stop();
                        frame_mailbox_clone.clear_session(&name_str, session_id);
                        if should_abort() {
                            return Ok(None);
                        }
                        if is_provisional {
                            warn!("[VIDEO] {}: provisional {:?} start failed ({error:#}); trying next backend", name_str, candidate);
                            last_error = Some(error);
                            continue;
                        }
                        return Err(error);
                    }

                    if let Some(mut rx) = attempt_event_rx {
                        let forward_target = player_event_tx_clone.clone();
                        rt_handle.spawn(async move {
                            while let Some(event) = rx.recv().await {
                                if forward_target.send(event).await.is_err() {
                                    break;
                                }
                            }
                        });
                    }

                    return Ok(Some((vp, prebuffer.frame)));
                }

                Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No video backends available")))
            }));

            match result {
                Ok(Ok(Some((mut vp, preroll_frame)))) => {
                    if shutdown_flag.load(Ordering::SeqCst)
                        || !pending_video_session_matches(
                            &pending_video_sessions,
                            &name_str,
                            session_id,
                        )
                    {
                        debug!(
                            "[VIDEO] {}: Discarding superseded prepared player for session {}",
                            name_str, session_id
                        );
                        let _ = vp.stop();
                        return;
                    }
                    if let Err(e) = player_tx_clone.blocking_send(VideoPlayerResult::Success(
                        Box::new(crate::content::sessions::VideoPlayerSuccess {
                            name: name_str,
                            session_id,
                            player: Box::new(vp),
                            preroll_frame,
                        }),
                    )) {
                        error!("[VIDEO] Failed to send video player back: {}", e);
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(_)) | Err(_) => {
                    if shutdown_flag.load(Ordering::SeqCst) {
                        return;
                    }
                    if let Ok(Err(error)) = &result {
                        error!("[VIDEO] {}: Player preparation/start failed: {error:#}", name_for_panic);
                    }
                    if result.is_err() {
                        error!("[VIDEO] {}: Video player task panicked!", name_for_panic);
                    }
                    let _ = player_tx_panic.blocking_send(VideoPlayerResult::Failure(
                        name_for_panic,
                        session_id_panic,
                    ));
                }
            }
        },
    ).await else {
        debug!(
            "[VIDEO] {}: Skipping video prepare task because shutdown is in progress",
            skipped_name
        );
        return;
    };
        drop(handle);
    });
}
