//! Owns compact long-video plans and exact playback selections in browser history.
//! Duration discovery happens in decoder preparation; queue selection performs no I/O.

use super::SmartQueue;
use crate::media_source::PlaybackWindow;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
    pub path: PathBuf,
    pub window: Option<PlaybackWindow>,
}

impl From<PathBuf> for QueueEntry {
    fn from(path: PathBuf) -> Self {
        Self { path, window: None }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ExcerptPlan {
    duration_ns: u64,
    length_ns: u64,
    count: u64,
    cursor: u64,
    seed: u64,
}

impl ExcerptPlan {
    fn new(duration_ns: u64, length_ns: u64) -> Option<Self> {
        if length_ns == 0 || duration_ns <= length_ns.saturating_mul(2) {
            return None;
        }
        Some(Self {
            duration_ns,
            length_ns,
            count: duration_ns.div_ceil(length_ns).min(4096),
            cursor: 1,
            seed: rand::random(),
        })
    }

    fn window(&self, index: u64) -> PlaybackWindow {
        let base =
            (u128::from(self.duration_ns) * u128::from(index) / u128::from(self.count)) as u64;
        // Offsets are stable within a cycle; history retains the selected window
        // when a later cycle changes the seed. Adjacent bins remain ordered.
        let mut mixed = self
            .seed
            .wrapping_add(index.wrapping_mul(0x9e3779b97f4a7c15));
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        let radius = (self.length_ns / 4).min(45_000_000_000);
        let offset = mixed % radius.saturating_mul(2).saturating_add(1);
        let start_ns = if index == 0 {
            0
        } else {
            base.saturating_sub(radius)
                .saturating_add(offset)
                .min(self.duration_ns - self.length_ns)
        };
        PlaybackWindow {
            start_ns,
            end_ns: start_ns + self.length_ns,
        }
    }
}

impl SmartQueue {
    pub(crate) fn configure_excerpts(&mut self, length: Duration, enabled: bool) {
        let length_ns = length.as_nanos().min(u128::from(u64::MAX)) as u64;
        let desired = (enabled && self.pool.len() > 1).then_some(length_ns);
        if self.excerpt_length_ns != desired {
            self.excerpt_length_ns = desired;
            self.excerpt_plans.clear();
        }
    }

    pub(crate) fn register_video_duration(&mut self, path: &Path, duration_ns: u64) {
        let Some(length_ns) = self.excerpt_length_ns else {
            return;
        };
        if let Some(plan) = self.excerpt_plans.get_mut(path) {
            if let Some(current) = self
                .history
                .back_mut()
                .filter(|entry| entry.path == path && entry.window.is_none())
            {
                current.window = Some(plan.window(0));
                plan.cursor = 1;
            }
            return;
        }
        if self.excerpt_plans.len() >= 5000 {
            return;
        }
        let Some(plan) = ExcerptPlan::new(duration_ns, length_ns) else {
            return;
        };
        if let Some(current) = self.history.back_mut().filter(|entry| entry.path == path) {
            current.window = Some(plan.window(0));
        }
        if matches!(
            self.strategy,
            crate::orchestration::SortingStrategy::Ascending
                | crate::orchestration::SortingStrategy::Descending
        ) && self.history.back().is_some_and(|entry| entry.path == path)
            && let Some(index) = self.pool.iter().position(|candidate| candidate == path)
        {
            self.current_index = index;
        }
        self.excerpt_plans.insert(path.to_path_buf(), plan);
    }

    pub(crate) fn current_window(&self) -> Option<PlaybackWindow> {
        self.history.back().and_then(|entry| entry.window)
    }

    pub(super) fn next_entry(&mut self, path: &Path) -> QueueEntry {
        let window = self.excerpt_plans.get_mut(path).map(|plan| {
            let window = plan.window(plan.cursor);
            plan.cursor = (plan.cursor + 1) % plan.count;
            if plan.cursor == 0 {
                plan.seed = rand::random();
            }
            if plan.cursor != 0
                && matches!(
                    self.strategy,
                    crate::orchestration::SortingStrategy::Ascending
                        | crate::orchestration::SortingStrategy::Descending
                )
                && let Some(index) = self.pool.iter().position(|candidate| candidate == path)
            {
                self.current_index = index;
            }
            window
        });
        QueueEntry {
            path: path.to_path_buf(),
            window,
        }
    }

    pub(super) fn excerpt_weight(&self, path: &Path) -> f64 {
        self.excerpt_plans
            .get(path)
            .map_or(1.0, |plan| plan.count as f64)
    }

    pub(super) fn has_remaining_excerpts(&self, path: &Path) -> bool {
        self.excerpt_plans
            .get(path)
            .is_some_and(|plan| plan.cursor != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::ExcerptPlan;
    #[test]
    fn long_video_windows_are_fixed_length_ordered_and_bounded() {
        for length in [1, 180_000_000_000, u64::MAX / 10] {
            let duration = length * 9 + length / 3;
            let plan = ExcerptPlan::new(duration, length).unwrap();
            assert_eq!(plan.window(0).start_ns, 0);
            let mut previous = 0;
            for index in 0..plan.count {
                let window = plan.window(index);
                assert!(window.start_ns >= previous);
                assert_eq!(window.end_ns - window.start_ns, length);
                assert!(window.end_ns <= duration);
                previous = window.start_ns;
            }
        }
        assert!(ExcerptPlan::new(360, 180).is_none());
        assert!(ExcerptPlan::new(100, 0).is_none());
    }

    #[test]
    fn partial_final_interval_still_gets_a_full_length_excerpt() {
        let length = 180_000_000_000;
        let duration = 520_000_000_000;
        let plan = ExcerptPlan::new(duration, length).unwrap();
        assert_eq!(plan.count, 3);
        let last = plan.window(plan.count - 1);
        assert_eq!(last.end_ns - last.start_ns, length);
        assert!(last.end_ns > length * 2);
        assert!(last.end_ns <= duration);
    }
}
