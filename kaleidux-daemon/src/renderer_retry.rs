use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
struct RetryEntry {
    failures: u32,
    not_before: Option<Instant>,
}

/// Per-output exponential backoff for renderer/WSI initialization.
///
/// A broken connector or lost device must not turn topology reconciliation
/// into an unbounded create/destroy loop. Entries remain while an attempt is
/// in flight so repeated failures continue the same bounded backoff series.
#[derive(Debug, Default)]
pub(crate) struct RendererRetryBackoff {
    entries: HashMap<String, RetryEntry>,
}

impl RendererRetryBackoff {
    pub(crate) fn can_start(&self, name: &str, now: Instant) -> bool {
        self.entries
            .get(name)
            .and_then(|entry| entry.not_before)
            .is_none_or(|deadline| now >= deadline)
    }

    pub(crate) fn mark_started(&mut self, name: &str) {
        if let Some(entry) = self.entries.get_mut(name) {
            entry.not_before = None;
        }
    }

    pub(crate) fn record_failure(&mut self, name: &str, now: Instant) -> Duration {
        let entry = self.entries.entry(name.to_string()).or_insert(RetryEntry {
            failures: 0,
            not_before: None,
        });
        entry.failures = entry.failures.saturating_add(1);
        let shift = entry.failures.saturating_sub(1).min(5);
        let delay = Duration::from_secs((1_u64 << shift).min(30));
        entry.not_before = Some(now + delay);
        delay
    }

    pub(crate) fn record_success(&mut self, name: &str) {
        self.entries.remove(name);
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.entries
            .values()
            .filter_map(|entry| entry.not_before)
            .min()
    }

    pub(crate) fn retain_outputs(&mut self, live: &HashSet<String>) {
        self.entries.retain(|name, _| live.contains(name));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_back_off_and_cap_at_thirty_seconds() {
        let now = Instant::now();
        let mut retry = RendererRetryBackoff::default();
        for expected in [1, 2, 4, 8, 16, 30, 30] {
            let delay = retry.record_failure("DP-1", now);
            assert_eq!(delay, Duration::from_secs(expected));
            assert!(!retry.can_start("DP-1", now));
            assert!(retry.can_start("DP-1", now + delay));
            retry.mark_started("DP-1");
        }
        assert!(retry.next_deadline().is_none());
        retry.record_success("DP-1");
        assert!(retry.can_start("DP-1", now));
    }

    #[test]
    fn disconnected_outputs_drop_retry_state() {
        let now = Instant::now();
        let mut retry = RendererRetryBackoff::default();
        retry.record_failure("DP-1", now);
        retry.retain_outputs(&HashSet::new());
        assert!(retry.next_deadline().is_none());
    }
}
