use crate::content::sessions::VideoPlayerResult;
use crate::main_loop::{CmdMsg, LoadedImage, PlayerEventMsg};
use crate::observability::wake::{DeadlineReason, WakeReason};
use std::time::{Duration, Instant};

pub struct IdleWaitResult {
    pub cmd: Option<CmdMsg>,
    pub frame_ready: bool,
    pub fd_ready: bool,
    pub image: Option<LoadedImage>,
    pub player: Option<VideoPlayerResult>,
    pub player_event: Option<PlayerEventMsg>,
    pub wake_reason: WakeReason,
    pub deadline_reason: DeadlineReason,
    pub requested_sleep: Duration,
    pub actual_sleep: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RendererActivitySnapshot {
    pub any_active: bool,
    pub wayland_hot: bool,
    pub next_wayland_retry_deadline: Option<Instant>,
    pub next_direct_heartbeat_deadline: Option<Instant>,
}

impl IdleWaitResult {
    pub(crate) fn immediate_video_frame() -> Self {
        Self {
            cmd: None,
            frame_ready: true,
            fd_ready: false,
            image: None,
            player: None,
            player_event: None,
            wake_reason: WakeReason::VideoFrame,
            deadline_reason: DeadlineReason::PeriodicFallback,
            requested_sleep: Duration::ZERO,
            actual_sleep: Duration::ZERO,
        }
    }
}

pub(crate) fn min_deadline_with_reason(
    left: Option<(Instant, DeadlineReason)>,
    right: Option<(Instant, DeadlineReason)>,
) -> Option<(Instant, DeadlineReason)> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

pub(crate) fn min_instant(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

pub(crate) async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending::<()>().await,
    }
}

pub(crate) fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
