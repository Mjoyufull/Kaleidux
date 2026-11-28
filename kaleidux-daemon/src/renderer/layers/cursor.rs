//! Eases cursor-driven shaders and stops waking once the pointer settles.

use std::time::{Duration, Instant};

const REST: [f32; 3] = [0.5, 0.5, 0.0];

pub(super) struct CursorMotion {
    current: [f32; 3],
    target: [f32; 3],
    updated: Option<Instant>,
}

impl Default for CursorMotion {
    fn default() -> Self {
        Self {
            current: REST,
            target: REST,
            updated: None,
        }
    }
}

impl CursorMotion {
    pub(super) fn set(&mut self, pointer: [f32; 4], active: bool) -> bool {
        let target = if active {
            [pointer[0], pointer[1], 1.0]
        } else {
            REST
        };
        if self.target == target {
            return false;
        }
        self.target = target;
        true
    }

    pub(super) fn pending(&self) -> bool {
        self.current != self.target
    }

    pub(super) fn update(&mut self, now: Instant) -> [f32; 4] {
        if self.pending() {
            let seconds = self
                .updated
                .map_or(Duration::from_millis(16), |previous| {
                    now.saturating_duration_since(previous)
                        .min(Duration::from_millis(50))
                })
                .as_secs_f32();
            let weight = 1.0 - (-12.0 * seconds).exp();
            for (current, target) in self.current.iter_mut().zip(self.target) {
                let delta = target - *current;
                *current = if delta.abs() < 0.002 {
                    target
                } else {
                    *current + delta * weight
                };
            }
            self.updated = Some(now);
        } else {
            self.updated = None;
        }
        [self.current[0], self.current[1], self.current[2], 0.0]
    }
}
