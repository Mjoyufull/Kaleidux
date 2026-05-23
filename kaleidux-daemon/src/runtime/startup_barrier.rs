use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub const STARTUP_BARRIER_SKEW_RELEASE: Duration = Duration::from_millis(150);
pub const STARTUP_BARRIER_TIMEOUT: Duration = Duration::from_millis(1000);
pub const STARTUP_RETRY_LIMIT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupOutputPhase {
    Pending,
    Ready,
    Failed,
    Presented,
}

#[derive(Debug, Clone)]
pub struct StartupOutputState {
    pub phase: StartupOutputPhase,
    pub first_ready_at: Option<Instant>,
    pub first_present_at: Option<Instant>,
    pub retry_count: u8,
    pub can_block: bool,
    pub failed_paths: HashSet<PathBuf>,
}

impl StartupOutputState {
    pub fn pending() -> Self {
        Self {
            phase: StartupOutputPhase::Pending,
            first_ready_at: None,
            first_present_at: None,
            retry_count: 0,
            can_block: true,
            failed_paths: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupPresentBarrier {
    pub batch_id: u64,
    pub armed_at: Instant,
    pub first_ready_at: Option<Instant>,
    pub release_reason: Option<&'static str>,
    pub outputs: HashMap<String, StartupOutputState>,
}

pub fn min_optional_deadline(
    current: Option<Instant>,
    candidate: Option<Instant>,
) -> Option<Instant> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

pub fn startup_barrier_counts(barrier: &StartupPresentBarrier) -> (usize, usize, usize) {
    let mut pending = 0usize;
    let mut ready = 0usize;
    let mut failed = 0usize;

    for state in barrier.outputs.values() {
        match state.phase {
            StartupOutputPhase::Pending if state.can_block => pending += 1,
            StartupOutputPhase::Ready | StartupOutputPhase::Presented if state.can_block => {
                ready += 1;
            }
            StartupOutputPhase::Failed => failed += 1,
            _ => {}
        }
    }

    (pending, ready, failed)
}

pub fn startup_barrier_release_candidate(
    barrier: &StartupPresentBarrier,
    now: Instant,
) -> Option<&'static str> {
    if let Some(reason) = barrier.release_reason {
        return Some(reason);
    }

    let (pending, _ready, failed) = startup_barrier_counts(barrier);
    if pending == 0 {
        return Some(if failed > 0 {
            "failed_outputs"
        } else {
            "all_ready"
        });
    }
    if let Some(first_ready_at) = barrier.first_ready_at
        && now >= first_ready_at + STARTUP_BARRIER_SKEW_RELEASE
    {
        return Some("bounded_skew");
    }
    if now >= barrier.armed_at + STARTUP_BARRIER_TIMEOUT {
        return Some("timeout");
    }

    None
}

pub fn startup_barrier_next_deadline(
    barrier: &StartupPresentBarrier,
    now: Instant,
) -> Option<Instant> {
    if startup_barrier_release_candidate(barrier, now).is_some() {
        return Some(now);
    }

    let mut deadline = Some(barrier.armed_at + STARTUP_BARRIER_TIMEOUT);
    if let Some(first_ready_at) = barrier.first_ready_at {
        deadline = min_optional_deadline(
            deadline,
            Some(first_ready_at + STARTUP_BARRIER_SKEW_RELEASE),
        );
    }
    deadline
}

pub fn startup_barrier_is_terminal(barrier: &StartupPresentBarrier) -> bool {
    barrier.outputs.values().all(|state| match state.phase {
        StartupOutputPhase::Presented => true,
        StartupOutputPhase::Failed => !state.can_block && state.retry_count >= STARTUP_RETRY_LIMIT,
        StartupOutputPhase::Pending | StartupOutputPhase::Ready => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_barrier_releases_after_bounded_skew() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: Some(now),
            release_reason: None,
            outputs: HashMap::from([
                (
                    String::from("DP-2"),
                    StartupOutputState {
                        phase: StartupOutputPhase::Ready,
                        first_ready_at: Some(now),
                        first_present_at: None,
                        retry_count: 0,
                        can_block: true,
                        failed_paths: HashSet::new(),
                    },
                ),
                (String::from("DP-3"), StartupOutputState::pending()),
            ]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + Duration::from_millis(100)),
            None
        );
        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + STARTUP_BARRIER_SKEW_RELEASE),
            Some("bounded_skew")
        );
    }

    #[test]
    fn startup_barrier_releases_failed_outputs_without_waiting() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: HashMap::from([(
                String::from("DP-2"),
                StartupOutputState {
                    phase: StartupOutputPhase::Failed,
                    first_ready_at: None,
                    first_present_at: None,
                    retry_count: STARTUP_RETRY_LIMIT,
                    can_block: false,
                    failed_paths: HashSet::new(),
                },
            )]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now),
            Some("failed_outputs")
        );
    }

    #[test]
    fn startup_barrier_times_out_after_one_second() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: HashMap::from([(String::from("DP-2"), StartupOutputState::pending())]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + STARTUP_BARRIER_TIMEOUT),
            Some("timeout")
        );
    }
}
