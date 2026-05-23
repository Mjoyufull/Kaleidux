use crate::content::sessions::{
    VideoPlayerResult, set_pending_video_session, stop_video_player_in_background,
};
use crate::content::switch::{
    ContentSwitchContext, ContentSwitchRequest, switch_wallpaper_content,
};
use crate::main_loop::MainLoopContext;
use crate::renderer;
use crate::runtime::startup_barrier::{STARTUP_RETRY_LIMIT, StartupOutputPhase};
use std::time::Instant;
use tracing::{debug, error, info, warn};

impl MainLoopContext {
    pub fn drain_players<F>(
        &mut self,
        player_buf: Option<VideoPlayerResult>,
        loop_start: Instant,
        mut render_fn: F,
    ) where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let mut pending_players = Vec::new();
        if let Some(res) = player_buf {
            pending_players.push(res);
        }
        while let Ok(res) = self.player_rx.try_recv() {
            pending_players.push(res);
        }

        for res in pending_players {
            match res {
                VideoPlayerResult::Success(name, session_id, player, preroll_frame) => {
                    let mut player = *player;
                    let barrier_blocks = self.startup_barrier_blocks_output(&name, loop_start);
                    let pending = self.pending_video_switches.get(&name).cloned();
                    if let Some(pending) = pending.filter(|p| p.session_id == session_id) {
                        self.pending_video_switches.remove(&name);

                        let mut should_render = false;
                        let mut startup_ready = false;
                        if let Some(r) = self.renderers.get_mut(&name) {
                            r.active_batch_id = pending.batch_id;
                            r.batch_start_time = pending.batch_trigger_time;
                            r.active_transition = pending.transition;
                            r.set_content_type(crate::queue::ContentType::Video);
                            r.active_image_session_id = 0;
                            r.active_video_session_id = session_id;
                            r.switch_content();

                            if let Some(frame) = preroll_frame.as_ref() {
                                let upload_start = Instant::now();
                                r.upload_frame(frame);
                                self.metrics.record_video_cpu_time(upload_start.elapsed());
                                self.metrics.record_video_frame_uploaded();
                                startup_ready = true;
                                should_render = true;
                            }
                        } else {
                            stop_video_player_in_background(name, player);
                            continue;
                        }
                        if startup_ready {
                            self.mark_startup_output_ready(&name, loop_start);
                        }

                        if let Err(e) = player.start() {
                            error!(
                                "[VIDEO] {}: Failed to start deferred video player: {}",
                                name, e
                            );
                            set_pending_video_session(&self.pending_video_sessions, &name, None);
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.abort_transition();
                            }
                            self.handle_startup_content_failure(
                                &name,
                                &format!("player_start: {}", e),
                                loop_start,
                            );
                            continue;
                        }

                        let old_player = self.video_players.remove(&name);
                        self.video_players.insert(name.clone(), player);
                        if let Some(old) = old_player {
                            stop_video_player_in_background(name.clone(), old);
                        }

                        if should_render {
                            if barrier_blocks {
                                debug!(
                                    "[STARTUP] {}: First video frame ready, holding present for barrier release",
                                    name
                                );
                            } else if let Some(r) = self.renderers.get_mut(&name) {
                                render_fn(r, &name, loop_start);
                                if !self.first_frame_recorded {
                                    self.metrics.record_first_frame();
                                    self.first_frame_recorded = true;
                                }
                            }
                            self.mark_output_presented_if_ready(&name);
                        }
                    } else if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Err(e) = player.start() {
                            error!("[VIDEO] {}: Failed to start video player: {}", name, e);
                            set_pending_video_session(&self.pending_video_sessions, &name, None);
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.abort_transition();
                            }
                            self.handle_startup_content_failure(
                                &name,
                                &format!("player_start: {}", e),
                                loop_start,
                            );
                            continue;
                        }

                        if let Some(r) = self.renderers.get_mut(&name) {
                            if let Some(frame) = preroll_frame.as_ref() {
                                let upload_start = Instant::now();
                                r.upload_frame(frame);
                                self.metrics.record_video_cpu_time(upload_start.elapsed());
                                self.metrics.record_video_frame_uploaded();
                                render_fn(r, &name, loop_start);
                                if !self.first_frame_recorded {
                                    self.metrics.record_first_frame();
                                    self.first_frame_recorded = true;
                                }
                            }
                        }

                        let old_player = self.video_players.remove(&name);
                        self.video_players.insert(name.clone(), player);
                        if let Some(old) = old_player {
                            stop_video_player_in_background(name.clone(), old);
                        }
                        self.mark_output_presented_if_ready(&name);
                    } else {
                        stop_video_player_in_background(name, player);
                    }
                }
                VideoPlayerResult::Failure(name, session_id) => {
                    if self
                        .pending_video_switches
                        .get(&name)
                        .is_some_and(|p| p.session_id == session_id)
                    {
                        self.pending_video_switches.remove(&name);
                        set_pending_video_session(&self.pending_video_sessions, &name, None);
                    }
                    if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Some(r) = self.renderers.get_mut(&name) {
                            r.abort_transition();
                        }
                    }
                    self.handle_startup_content_failure(&name, "player_prepare_failed", loop_start);
                }
            }
        }
    }

    pub(crate) fn reset_startup_output_pending(&mut self, name: &str) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };
        if state.can_block && state.phase != StartupOutputPhase::Presented {
            state.phase = StartupOutputPhase::Pending;
        }
    }

    pub(crate) fn handle_startup_content_failure(
        &mut self,
        name: &str,
        reason: &str,
        loop_start: Instant,
    ) -> bool {
        let tracked = self.startup_present_barrier.as_ref().and_then(|barrier| {
            barrier
                .outputs
                .get(name)
                .map(|state| (barrier.batch_id, state.phase))
        });
        let Some((batch_id, phase)) = tracked else {
            return false;
        };

        if phase == StartupOutputPhase::Presented {
            return false;
        }

        let failed_path = self
            .monitor_manager
            .outputs
            .get(name)
            .and_then(|orch| orch.current_path.clone());
        self.mark_startup_output_failed(name, reason, failed_path.as_deref());

        let retry_number = self
            .startup_present_barrier
            .as_mut()
            .and_then(|barrier| barrier.outputs.get_mut(name))
            .and_then(|state| {
                if state.retry_count >= STARTUP_RETRY_LIMIT {
                    None
                } else {
                    state.retry_count += 1;
                    Some(state.retry_count)
                }
            });

        let Some(retry_number) = retry_number else {
            warn!(
                "[STARTUP] {}: retries exhausted after failure ({})",
                name, reason
            );
            if let Some(state) = self
                .startup_present_barrier
                .as_mut()
                .and_then(|barrier| barrier.outputs.get_mut(name))
            {
                state.retry_count = STARTUP_RETRY_LIMIT;
            }
            self.maybe_clear_startup_present_barrier();
            return true;
        };

        let failed_paths = self
            .startup_present_barrier
            .as_ref()
            .and_then(|barrier| barrier.outputs.get(name))
            .map(|state| state.failed_paths.clone())
            .unwrap_or_default();
        let changes = self
            .monitor_manager
            .pick_startup_replacement(name, &failed_paths);

        if changes.is_empty() {
            warn!(
                "[STARTUP] {}: no replacement candidate after failure ({})",
                name, reason
            );
            if let Some(state) = self
                .startup_present_barrier
                .as_mut()
                .and_then(|barrier| barrier.outputs.get_mut(name))
            {
                state.retry_count = STARTUP_RETRY_LIMIT;
            }
            self.maybe_clear_startup_present_barrier();
            return true;
        }

        info!(
            "[STARTUP] {}: retry {}/{} after failure ({})",
            name, retry_number, STARTUP_RETRY_LIMIT, reason
        );

        for changed_name in changes.keys() {
            self.reset_startup_output_pending(changed_name);
        }

        for (changed_name, (path, content_type)) in changes {
            switch_wallpaper_content(
                ContentSwitchRequest {
                    name: changed_name,
                    path,
                    content_type,
                    batch_id: Some(batch_id),
                    batch_trigger_time: Some(loop_start),
                    shared_image_target: None,
                    log_prefix: "STARTUP-RETRY",
                },
                ContentSwitchContext {
                    metrics: &self.metrics,
                    next_session_id: &mut self.next_session_id,
                    frame_mailbox: &self.latest_video_frames,
                    monitor_manager: &self.monitor_manager,
                    renderers: &mut self.renderers,
                    video_players: &mut self.video_players,
                    pending_video_switches: &mut self.pending_video_switches,
                    pending_image_video_stops: &mut self.pending_image_video_stops,
                    pending_video_sessions: &self.pending_video_sessions,
                    image_tx: &self.image_tx,
                    player_tx: &self.player_tx,
                    player_event_tx: &self.player_event_tx,
                    shutdown_flag: &self.shutdown_flag,
                },
            );
        }

        true
    }
}
