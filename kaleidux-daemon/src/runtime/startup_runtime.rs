use crate::content::switch::{
    ContentSwitchContext, ContentSwitchRequest, switch_wallpaper_content,
};
use crate::image::runtime_cache::ordered_pending_content_switches;
use crate::main_loop::MainLoopContext;
use crate::renderer;
use crate::runtime::startup_barrier::{
    StartupOutputPhase, StartupOutputState, StartupPresentBarrier, startup_barrier_counts,
    startup_barrier_is_terminal, startup_barrier_release_candidate,
};
use crate::runtime::timing::duration_ms;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tracing::{info, warn};

impl MainLoopContext {
    pub fn initial_load(&mut self) {
        info!(
            "[STARTUP] Reached Initial Load section, renderers count: {}",
            self.renderers.len()
        );
        info!("[STARTUP] About to call monitor_manager.tick()");
        let initial_changes = self.monitor_manager.tick();
        info!(
            "[STARTUP] Initial changes: {} outputs",
            initial_changes.len()
        );
        for (name, (path, content_type)) in &initial_changes {
            info!(
                "[STARTUP] Change: {} -> {:?} ({:?})",
                name, path, content_type
            );
        }
        if initial_changes.is_empty() {
            warn!("[STARTUP] No initial content changes - wallpapers may not load!");
        }
        let batch_id = rand::random::<u64>();
        let mut startup_outputs = Vec::new();
        let ordered_changes = ordered_pending_content_switches(&self.renderers, initial_changes);
        for change in ordered_changes {
            if !self.renderers.contains_key(&change.name) {
                warn!(
                    "[STARTUP] Skipping initial content for {} - renderer does not exist",
                    change.name
                );
                continue;
            }
            startup_outputs.push(change.name.clone());
            switch_wallpaper_content(
                ContentSwitchRequest {
                    name: change.name,
                    path: change.path,
                    content_type: change.content_type,
                    batch_id: Some(batch_id),
                    batch_trigger_time: None,
                    shared_image_target: change.shared_image_target,
                    log_prefix: "STARTUP",
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
        if startup_outputs.len() > 1 {
            self.arm_startup_present_barrier(batch_id, startup_outputs);
        }
    }

    pub fn arm_startup_present_barrier(&mut self, batch_id: u64, outputs: Vec<String>) {
        let output_states: HashMap<_, _> = outputs
            .into_iter()
            .filter(|name| {
                self.renderers
                    .get(name)
                    .is_some_and(|renderer| !renderer.has_any_content())
            })
            .map(|name| (name, StartupOutputState::pending()))
            .collect();
        if output_states.len() <= 1 {
            return;
        }

        let now = Instant::now();
        self.startup_present_barrier = Some(StartupPresentBarrier {
            batch_id,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: output_states,
        });
        info!(
            "[STARTUP] First-present barrier armed for {} outputs (batch {:x})",
            self.startup_present_barrier
                .as_ref()
                .map_or(0, |b| b.outputs.len()),
            batch_id
        );
    }

    pub fn startup_barrier_blocks_output(&self, name: &str, now: Instant) -> bool {
        let Some(barrier) = &self.startup_present_barrier else {
            return false;
        };

        let Some(state) = barrier.outputs.get(name) else {
            return false;
        };

        if !state.can_block {
            return false;
        }

        startup_barrier_release_candidate(barrier, now).is_none()
    }

    pub fn release_startup_present_barrier<F>(&mut self, loop_start: Instant, mut render_fn: F)
    where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let Some(reason) = self
            .startup_present_barrier
            .as_ref()
            .and_then(|barrier| startup_barrier_release_candidate(barrier, loop_start))
        else {
            return;
        };

        if let Some(barrier) = self.startup_present_barrier.as_mut() {
            if barrier.release_reason.is_none() {
                let (pending, ready, failed) = startup_barrier_counts(barrier);
                barrier.release_reason = Some(reason);
                info!(
                    "[STARTUP] First-present barrier released for batch {:x} after {:.1}ms reason={} pending={} ready={} failed={}",
                    barrier.batch_id,
                    duration_ms(loop_start.saturating_duration_since(barrier.armed_at)),
                    reason,
                    pending,
                    ready,
                    failed
                );
            }
        }

        let outputs_to_release: Vec<String> = self
            .startup_present_barrier
            .as_ref()
            .map(|barrier| {
                barrier
                    .outputs
                    .iter()
                    .filter_map(|(name, state)| {
                        if state.can_block && state.phase == StartupOutputPhase::Ready {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        for name in outputs_to_release {
            if let Some(r) = self.renderers.get_mut(&name) {
                self.metrics.record_startup_release_present();
                render_fn(r, &name, loop_start);
                if !self.first_frame_recorded {
                    self.metrics.record_first_frame();
                    self.first_frame_recorded = true;
                }
                self.mark_output_presented_if_ready(&name);
            }
        }

        self.maybe_clear_startup_present_barrier();
    }

    pub(crate) fn mark_startup_output_ready(&mut self, name: &str, now: Instant) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if state.phase == StartupOutputPhase::Presented {
            return;
        }

        if state.first_ready_at.is_none() {
            state.first_ready_at = Some(now);
            info!(
                "[STARTUP] {} first-ready {:.1}ms (batch {:x})",
                name,
                duration_ms(now.saturating_duration_since(barrier.armed_at)),
                barrier.batch_id
            );
        }

        if barrier.first_ready_at.is_none() {
            barrier.first_ready_at = Some(now);
        }

        state.phase = StartupOutputPhase::Ready;
    }

    pub(crate) fn mark_startup_output_failed(
        &mut self,
        name: &str,
        reason: &str,
        failed_path: Option<&Path>,
    ) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if let Some(path) = failed_path {
            state.failed_paths.insert(path.to_path_buf());
        }
        state.phase = StartupOutputPhase::Failed;
        state.can_block = false;

        info!(
            "[STARTUP] {} failed after {:.1}ms reason={} retries={} batch {:x}",
            name,
            duration_ms(Instant::now().saturating_duration_since(barrier.armed_at)),
            reason,
            state.retry_count,
            barrier.batch_id
        );
    }

    pub(crate) fn mark_startup_output_presented(&mut self, name: &str, now: Instant) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if state.first_present_at.is_none() {
            state.first_present_at = Some(now);
            info!(
                "[STARTUP] {} first-present {:.1}ms (batch {:x})",
                name,
                duration_ms(now.saturating_duration_since(barrier.armed_at)),
                barrier.batch_id
            );
        }

        state.phase = StartupOutputPhase::Presented;
        state.can_block = false;
    }

    pub(crate) fn maybe_clear_startup_present_barrier(&mut self) {
        if self
            .startup_present_barrier
            .as_ref()
            .is_some_and(startup_barrier_is_terminal)
        {
            self.startup_present_barrier = None;
        }
    }
}
