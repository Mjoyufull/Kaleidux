use crate::content::sessions::{should_accept_video_frame, stop_video_player_in_background};
use crate::content::switch::{
    ContentSwitchContext, ContentSwitchRequest, switch_wallpaper_content,
};
use crate::image::prefetch;
use crate::image::runtime_cache::{
    LOW_POWER_IMAGE_PREFETCH_DEFER, begin_image_prefetch_generation,
    ordered_pending_content_switches, prepared_image_available_for_output,
    schedule_image_prefetch_plan,
};
use crate::main_loop::{
    CmdMsg, CommandContext, LoadedImage, MainLoopContext, log_slow_image_prepare,
};
use crate::queue;
use crate::renderer;
use crate::runtime::commands::handle_command;
use crate::runtime::timing::duration_ms;
use crate::video;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;
use tracing::{debug, info, trace, warn};

fn trace_video_drain_enabled() -> bool {
    if crate::observability::trace_all::trace_all_enabled() {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_TRACE_VIDEO_DRAIN")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

impl MainLoopContext {
    /// Process scheduled changes from MonitorManager::tick().
    pub fn process_scheduled(&mut self, loop_start: Instant) {
        if !self.monitor_manager.tick_due(loop_start) {
            return;
        }

        if self.defer_low_power_image_switch_for_prefetch(loop_start) {
            return;
        }

        let scheduled_changes = self.monitor_manager.tick();
        if !scheduled_changes.is_empty() {
            let batch_id = rand::random::<u64>();
            let ordered_changes =
                ordered_pending_content_switches(&self.renderers, scheduled_changes);
            for change in ordered_changes {
                switch_wallpaper_content(
                    ContentSwitchRequest {
                        name: change.name,
                        path: change.path,
                        content_type: change.content_type,
                        batch_id: Some(batch_id),
                        batch_trigger_time: Some(loop_start),
                        shared_image_target: change.shared_image_target,
                        log_prefix: "SCHEDULED",
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
        }
    }

    pub(crate) fn defer_low_power_image_switch_for_prefetch(
        &mut self,
        loop_start: Instant,
    ) -> bool {
        for name in self.monitor_manager.due_low_power_outputs(loop_start) {
            let Some(orchestrator) = self.monitor_manager.outputs.get(&name) else {
                continue;
            };
            let Some((path, queue::ContentType::Image)) = orchestrator.peek_next() else {
                continue;
            };
            let Some(renderer) = self.renderers.get(&name) else {
                continue;
            };
            if prepared_image_available_for_output(
                &path,
                renderer.config.width,
                renderer.config.height,
            ) {
                continue;
            }

            let generation = begin_image_prefetch_generation(&name);
            let prefetch_plan = prefetch::build_plan(&self.monitor_manager, &self.renderers, &name);
            if prefetch_plan.is_empty() {
                continue;
            }

            info!(
                "[PREFETCH] {}: Low-power deferring image switch by {:.0}ms until prepared cache warms ({})",
                name,
                LOW_POWER_IMAGE_PREFETCH_DEFER.as_secs_f64() * 1000.0,
                path.display()
            );
            schedule_image_prefetch_plan(&name, generation, prefetch_plan, self.metrics.clone());
            self.monitor_manager
                .defer_switch_deadline(&name, LOW_POWER_IMAGE_PREFETCH_DEFER);
            return true;
        }

        false
    }

    /// Process script tick.
    pub fn process_script_tick(&mut self) {
        if !self.script_manager.is_loaded() {
            return;
        }
        if self.last_script_tick.elapsed().as_secs() >= self.script_tick_interval {
            self.script_manager.tick();
            self.last_script_tick = Instant::now();
        }
    }

    /// Drain and handle all pending commands.
    pub async fn drain_commands(&mut self, cmd_buf: Option<CmdMsg>, loop_start: Instant) {
        let cmd_iter = std::iter::once(cmd_buf)
            .flatten()
            .chain(std::iter::from_fn(|| self.cmd_rx.try_recv().ok()));
        for (req, resp) in cmd_iter {
            let response = handle_command(
                req,
                CommandContext {
                    monitor_manager: &mut self.monitor_manager,
                    renderers: &mut self.renderers,
                    video_players: &mut self.video_players,
                    pending_video_switches: &mut self.pending_video_switches,
                    pending_image_video_stops: &mut self.pending_image_video_stops,
                    pending_video_sessions: &self.pending_video_sessions,
                    metrics: &self.metrics,
                    frame_mailbox: &self.latest_video_frames,
                    image_tx: &self.image_tx,
                    player_tx: &self.player_tx,
                    player_event_tx: &self.player_event_tx,
                    next_session_id: &mut self.next_session_id,
                    loop_start,
                    shutdown_flag: &self.shutdown_flag,
                },
            )
            .await;
            let _ = resp.send(response);
        }
    }

    /// Drain video frames from channel. Returns latest frame per source, plus stats.
    pub fn drain_frames(
        &mut self,
        should_check_mailbox: bool,
        hold_video_until_callback: bool,
    ) -> (HashMap<String, video::VideoFrame>, usize, usize) {
        let mut latest_frames: HashMap<String, video::VideoFrame> = HashMap::new();
        let mut frames_received = 0;
        let mut frames_discarded = 0;
        let mut stale_session_discards = 0;
        let superseded_source_discards = self.latest_video_frames.take_overwrite_count() as usize;

        if should_check_mailbox {
            self.latest_video_frames.clear_signal_pending();
            for source_id in self.latest_video_frames.pending_sources() {
                let frame_state = self.latest_video_frames.inspect_frame(&source_id, |frame| {
                    let renderer = self.renderers.get(source_id.as_str());
                    let should_accept = renderer.is_some_and(|r| {
                        should_accept_video_frame(
                            r.valid_content_type,
                            r.active_video_session_id,
                            frame.session_id,
                        )
                    });
                    let hold_for_callback = should_accept
                        && hold_video_until_callback
                        && renderer
                            .is_some_and(renderer::Renderer::should_hold_video_frame_for_callback);

                    (should_accept, hold_for_callback)
                });

                let Some((should_accept, hold_for_callback)) = frame_state else {
                    continue;
                };

                if hold_for_callback {
                    continue;
                }

                let Some(frame) = self.latest_video_frames.take_frame(&source_id) else {
                    continue;
                };
                frames_received += 1;
                self.metrics.record_video_frame_received();
                if !should_accept {
                    frames_discarded += 1;
                    stale_session_discards += 1;
                    self.metrics.record_video_frame_stale_skipped();
                } else {
                    latest_frames.insert(source_id, frame);
                }
            }
        }

        // Track frame channel usage for memory leak detection
        if frames_received > 0 || superseded_source_discards > 0 {
            self.metrics
                .record_frame_channel_size(frames_received + self.latest_video_frames.occupancy());
            if (frames_discarded > 0 || superseded_source_discards > 0)
                && trace_video_drain_enabled()
            {
                trace!(
                    "[VIDEO] Discarded {} frames (stale_session={}, superseded_by_newer_same_source={})",
                    frames_discarded + superseded_source_discards,
                    stale_session_discards,
                    superseded_source_discards
                );
            }
        }

        (latest_frames, frames_received, frames_discarded)
    }

    /// Drain images from channel, upload, and optionally render.
    ///
    /// The `render_fn` closure is called for each image that needs rendering.
    /// It receives (renderer, &name, loop_start) and should perform the
    /// backend-specific render call.
    pub fn drain_images<F>(
        &mut self,
        image_buf: Option<LoadedImage>,
        loop_start: Instant,
        mut render_fn: F,
    ) where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let mut images_received = 0;
        let mut pending_images = Vec::new();
        if let Some(msg) = image_buf {
            pending_images.push(msg);
        }
        while let Ok(msg) = self.image_rx.try_recv() {
            pending_images.push(msg);
        }

        for msg in pending_images {
            images_received += 1;
            let barrier_blocks = self.startup_barrier_blocks_output(&msg.name, loop_start);
            let mut release_pending_video = false;
            let mut startup_ready = false;
            let mut startup_failure_reason: Option<String> = None;
            debug!(
                "[IMAGE] Received image for {}: session={}, data={}, size={}x{}",
                msg.name,
                msg.session_id,
                msg.data.is_some(),
                msg.width,
                msg.height
            );
            if let Some(r) = self.renderers.get_mut(&msg.name) {
                if r.valid_content_type != crate::queue::ContentType::Image
                    || r.active_image_session_id != msg.session_id
                {
                    if let Some(profile) = &msg.profile {
                        debug!(
                            "[IMAGE] {}: stale image was prepared via {} in {:.1}ms (wait {:.1}ms, cpu {:.1}ms)",
                            msg.name,
                            profile.format,
                            duration_ms(profile.total_duration()),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.cpu_duration())
                        );
                    }
                    debug!(
                        "[IMAGE] Dropping stale image for {}: session={} active_session={} content_type={:?}",
                        msg.name, msg.session_id, r.active_image_session_id, r.valid_content_type
                    );
                    continue;
                }

                if let Some(data) = msg.data {
                    debug!(
                        "[IMAGE] Uploading image data for {}: {} bytes",
                        msg.name,
                        data.len()
                    );
                    let upload_start = Instant::now();
                    let _ = r.upload_image_data(data.as_ref(), msg.width, msg.height);
                    let upload_duration = upload_start.elapsed();
                    if let Some(profile) = &msg.profile {
                        self.metrics.record_image_stage_timings(
                            profile.permit_wait,
                            profile.decode,
                            profile.convert,
                            profile.resize,
                            profile.expand,
                            upload_duration,
                        );
                        log_slow_image_prepare(&msg.name, profile, upload_duration, &self.metrics);
                        debug!(
                            "[IMAGE] {}: prepared {} {}x{} -> {}x{} in {:.1}ms (wait {:.1}ms, decode {:.1}ms, convert {:.1}ms, resize {:.1}ms, expand {:.1}ms, upload {:.1}ms, filter={})",
                            msg.name,
                            profile.format,
                            profile.source_width,
                            profile.source_height,
                            msg.width,
                            msg.height,
                            duration_ms(profile.total_duration() + upload_duration),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.decode),
                            duration_ms(profile.convert),
                            duration_ms(profile.resize),
                            duration_ms(profile.expand),
                            duration_ms(upload_duration),
                            profile.resize_filter.as_deref().unwrap_or("none")
                        );
                    }
                    debug!(
                        "[IMAGE] Upload complete for {}: {:.1}ms",
                        msg.name,
                        duration_ms(upload_duration)
                    );
                    startup_ready = true;
                    if barrier_blocks {
                        debug!(
                            "[STARTUP] {}: First image ready, holding present for barrier release",
                            msg.name
                        );
                    } else {
                        debug!("[IMAGE] Rendering after upload for {}", msg.name);
                        render_fn(r, &msg.name, loop_start);
                        if !self.first_frame_recorded {
                            self.metrics.record_first_frame();
                            self.first_frame_recorded = true;
                        }
                        self.mark_output_presented_if_ready(&msg.name);
                    }
                    release_pending_video = true;
                } else {
                    r.abort_transition();
                    startup_failure_reason = Some("image_decode_failed".to_string());
                    release_pending_video = true;
                }
            } else {
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
            }
            if startup_ready {
                self.mark_startup_output_ready(&msg.name, loop_start);
            }
            if let Some(reason) = startup_failure_reason {
                self.handle_startup_content_failure(&msg.name, &reason, loop_start);
            }
            if release_pending_video {
                self.release_pending_image_video_stop(&msg.name);
            }
        }
        if images_received > 0 {
            self.metrics.record_image_channel_size(images_received);
        }
    }

    pub(crate) fn release_pending_image_video_stop(&mut self, name: &str) {
        if let Some(player) = self.pending_image_video_stops.remove(name) {
            stop_video_player_in_background(name.to_string(), player);
        }
    }
}
