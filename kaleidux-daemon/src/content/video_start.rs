use crate::background::{self, BackgroundWorkKind};
use crate::content::sessions::{
    PendingVideoSessions, VideoPlayerResult, pending_video_session_matches,
};
use crate::main_loop::PlayerEventMsg;
use crate::metrics;
use crate::runtime::timing::duration_ms;
use crate::video;
use gstreamer as gst;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error};

pub(crate) struct VideoPlayerStartRequest {
    pub(crate) path: PathBuf,
    pub(crate) output_name: String,
    pub(crate) session_id: u64,
    pub(crate) volume: f64,
    pub(crate) backend_request: video::VideoBackendRequest,
    pub(crate) start_position_ns: Option<u64>,
    pub(crate) max_publish_fps: Option<u32>,
    pub(crate) render_size: Option<(u32, u32)>,
    #[cfg(feature = "mpv-backend")]
    pub(crate) mpv_native_target: Option<video::MpvNativeVideoTarget>,
    #[cfg(feature = "mpv-backend")]
    pub(crate) mpv_composed_target: Option<video::MpvComposedVideoTarget>,
}

pub(crate) struct VideoPlayerStartContext<'a> {
    pub(crate) frame_mailbox: &'a video::LatestFrameMailbox,
    pub(crate) player_tx: &'a tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    pub(crate) player_event_tx: &'a tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
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
        start_position_ns,
        max_publish_fps,
        render_size,
        #[cfg(feature = "mpv-backend")]
        mpv_native_target,
        #[cfg(feature = "mpv-backend")]
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
    let Some(handle) = background::spawn_blocking_tracked(
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
                match video::VideoPlayer::new(
                    &path_str,
                    name_arc,
                    session_id,
                    volume,
                    frame_mailbox_clone,
                    player_event_tx_clone,
                    metrics.clone(),
                    backend_request,
                    max_publish_fps,
                    render_size,
                    #[cfg(feature = "mpv-backend")]
                    mpv_native_target,
                    #[cfg(feature = "mpv-backend")]
                    mpv_composed_target,
                ) {
                    Ok(mut vp) => {
                        let create_duration = prepare_start.elapsed();
                        vp.set_volume(volume);
                        if should_abort() {
                            let _ = vp.stop();
                            return Ok(None);
                        }
                        let prebuffer_start = Instant::now();
                        let prebuffer = match vp.prebuffer(should_abort) {
                            Ok(result) => result,
                            Err(e) => {
                                if should_abort() {
                                    debug!(
                                        "[VIDEO] {}: Aborting pre-buffer for superseded/shutdown session {}",
                                        name_str, session_id
                                    );
                                    let _ = vp.stop();
                                    return Ok(None);
                                }
                                debug!(
                                    "[VIDEO] {}: Pre-buffering failed (non-fatal): {}",
                                    name_str, e
                                );
                                video::VideoPrebufferResult {
                                    frame: None,
                                    profile: video::VideoPrebufferProfile {
                                        set_state: Duration::ZERO,
                                        state_wait: Duration::ZERO,
                                        pull_preroll: Duration::ZERO,
                                        set_state_result: "error",
                                        state_wait_settled: false,
                                        current_state: gst::State::Null,
                                        pending_state: gst::State::VoidPending,
                                    },
                                }
                            }
                        };
                        let prebuffer_duration = prebuffer_start.elapsed();
                        if let Some(position_ns) = start_position_ns.filter(|pos| *pos > 0) {
                            vp.set_start_position_ns(position_ns);
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
                            Ok(None)
                        } else {
                            Ok(Some((vp, prebuffer.frame)))
                        }
                    }
                    Err(e) => {
                        error!("[VIDEO] {}: Failed to create video player: {}", name_str, e);
                        Err(e)
                    }
                }
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
                    if let Err(e) = player_tx_clone.send(VideoPlayerResult::Success(
                        name_str,
                        session_id,
                        Box::new(vp),
                        preroll_frame,
                    )) {
                        error!("[VIDEO] Failed to send video player back: {}", e);
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(_)) | Err(_) => {
                    if shutdown_flag.load(Ordering::SeqCst) {
                        return;
                    }
                    if result.is_err() {
                        error!("[VIDEO] {}: Video player task panicked!", name_for_panic);
                    }
                    let _ = player_tx_panic
                        .send(VideoPlayerResult::Failure(name_for_panic, session_id_panic));
                }
            }
        },
    ) else {
        debug!(
            "[VIDEO] {}: Skipping video prepare task because shutdown is in progress",
            skipped_name
        );
        return;
    };
    drop(handle);
}
