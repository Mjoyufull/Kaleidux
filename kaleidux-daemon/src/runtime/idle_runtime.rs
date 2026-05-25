use crate::main_loop::MainLoopContext;
use crate::observability::wake::{DeadlineReason, WakeReason};
use crate::renderer;
use crate::runtime::idle_wait::next_idle_wake_deadline;
use crate::runtime::startup_barrier::startup_barrier_next_deadline;
use crate::runtime::timing::{
    IdleWaitResult, RendererActivitySnapshot, min_deadline_with_reason, min_instant,
    sleep_until_optional,
};
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;

impl MainLoopContext {
    pub fn any_active(&self) -> bool {
        self.renderers
            .values()
            .any(|r| r.transition_active || r.needs_redraw)
    }

    pub fn wayland_hot_loop_active(&self) -> bool {
        self.renderers
            .values()
            .any(renderer::Renderer::needs_wayland_immediate_work)
    }

    pub fn renderer_activity_snapshot(&self) -> RendererActivitySnapshot {
        let mut snapshot = RendererActivitySnapshot::default();
        for renderer in self.renderers.values() {
            snapshot.any_active |= renderer.transition_active || renderer.needs_redraw;
            snapshot.wayland_hot |= renderer.needs_wayland_immediate_work();
            snapshot.next_wayland_retry_deadline = min_instant(
                snapshot.next_wayland_retry_deadline,
                renderer.next_wayland_retry_deadline(Duration::from_millis(500)),
            );
        }
        snapshot
    }

    pub fn next_common_idle_deadline(&self, now: Instant) -> Option<(Instant, DeadlineReason)> {
        let mut deadline = self
            .monitor_manager
            .next_switch_deadline()
            .map(|deadline| (deadline, DeadlineReason::ContentSwitch));
        if self.script_manager.is_loaded() {
            let script_deadline = (
                self.last_script_tick + Duration::from_secs(self.script_tick_interval),
                DeadlineReason::ScriptTick,
            );
            deadline = min_deadline_with_reason(deadline, Some(script_deadline));
        }
        deadline = min_deadline_with_reason(
            deadline,
            self.startup_present_barrier
                .as_ref()
                .and_then(|barrier| startup_barrier_next_deadline(barrier, now))
                .map(|deadline| (deadline, DeadlineReason::StartupBarrier)),
        );
        deadline
    }

    pub fn next_wayland_idle_deadline(&self, now: Instant) -> Option<(Instant, DeadlineReason)> {
        self.next_wayland_idle_deadline_from_snapshot(now, self.renderer_activity_snapshot())
    }

    pub fn next_wayland_idle_deadline_from_snapshot(
        &self,
        now: Instant,
        snapshot: RendererActivitySnapshot,
    ) -> Option<(Instant, DeadlineReason)> {
        let deadline = self.next_common_idle_deadline(now);
        min_deadline_with_reason(
            deadline,
            snapshot
                .next_wayland_retry_deadline
                .map(|deadline| (deadline, DeadlineReason::WaylandRetry)),
        )
    }

    /// Idle-wait using `tokio::select!` until any event source fires.
    /// Returns buffered messages from whichever branch triggered.
    pub async fn idle_wait(
        &mut self,
        fd: &AsyncFd<RawFd>,
        wake_deadline: Option<(Instant, DeadlineReason)>,
    ) -> IdleWaitResult {
        let mut cmd_buf = None;
        let mut frame_ready = false;
        let mut fd_ready = false;
        let mut image_buf = None;
        let mut player_buf = None;
        let mut player_event_buf = None;

        let now = Instant::now();
        if self.latest_video_frames.has_signal_pending() {
            let result = IdleWaitResult::immediate_video_frame();
            self.metrics.record_loop_wake(
                result.wake_reason,
                result.requested_sleep,
                result.actual_sleep,
            );
            if result.requested_sleep != Duration::ZERO {
                self.metrics.record_deadline_reason(result.deadline_reason);
            }
            return result;
        }
        let wake_deadline = wake_deadline.map(|(deadline, reason)| {
            if deadline <= now {
                self.metrics.record_wayland_expired_deadline_wake();
                (now + std::time::Duration::from_millis(1), reason)
            } else {
                (deadline, reason)
            }
        });
        let selected_deadline = next_idle_wake_deadline(
            now,
            Some(crate::observability::trace_all::trace_idle_poll_interval()),
            wake_deadline.map(|(deadline, _reason)| deadline),
        );
        let deadline_reason = selected_deadline
            .and_then(|selected| {
                wake_deadline
                    .filter(|(deadline, _reason)| *deadline == selected)
                    .map(|(_deadline, reason)| reason)
            })
            .unwrap_or(DeadlineReason::PeriodicFallback);
        let requested_sleep = selected_deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(Duration::MAX);
        let wait_started = Instant::now();
        let mut wake_reason = WakeReason::Deadline;

        tokio::select! {
            cmd = self.cmd_rx.recv() => {
                if let Some(c) = cmd {
                    wake_reason = WakeReason::Command;
                    cmd_buf = Some(c);
                }
            }
            _ = self.latest_video_frames.notified() => {
                wake_reason = WakeReason::VideoFrame;
                frame_ready = true;
            }
            image = self.image_rx.recv() => {
                if let Some(i) = image {
                    wake_reason = WakeReason::Image;
                    image_buf = Some(i);
                }
            }
            player = self.player_rx.recv() => {
                if let Some(p) = player {
                    wake_reason = WakeReason::PlayerReady;
                    player_buf = Some(p);
                }
            }
            player_event = self.player_event_rx.recv() => {
                if let Some(event) = player_event {
                    wake_reason = WakeReason::PlayerEvent;
                    player_event_buf = Some(event);
                }
            }
            result = fd.readable() => {
                if let Ok(mut guard) = result {
                    guard.clear_ready();
                    wake_reason = WakeReason::WaylandFd;
                    fd_ready = true;
                }
            }
            _ = sleep_until_optional(selected_deadline) => {
                wake_reason = WakeReason::Deadline;
            }
        }

        let actual_sleep = wait_started.elapsed();
        if crate::observability::trace_all::trace_all_enabled() {
            tracing::trace!(
                "[TRACE5][WAKE] reason={} deadline={} requested_ms={:.3} actual_ms={:.3} frame_signal={} fd_ready={} cmd={} image={} player={} player_event={}",
                wake_reason.label(),
                deadline_reason.label(),
                requested_sleep.as_secs_f64() * 1000.0,
                actual_sleep.as_secs_f64() * 1000.0,
                frame_ready,
                fd_ready,
                cmd_buf.is_some(),
                image_buf.is_some(),
                player_buf.is_some(),
                player_event_buf.is_some()
            );
        }
        self.metrics
            .record_loop_wake(wake_reason, requested_sleep, actual_sleep);
        if selected_deadline.is_some() || wake_reason == WakeReason::Deadline {
            self.metrics.record_deadline_reason(deadline_reason);
        }

        IdleWaitResult {
            cmd: cmd_buf,
            frame_ready,
            fd_ready,
            image: image_buf,
            player: player_buf,
            player_event: player_event_buf,
            wake_reason,
            requested_sleep,
            deadline_reason,
            actual_sleep,
        }
    }
}
