//! Freezes independent output timers while their compositor output is powered off.

use super::MonitorManager;
use crate::orchestration::MonitorBehavior;
use std::time::Instant;

impl MonitorManager {
    pub(crate) fn set_output_power_suspended(&mut self, name: &str, off: bool, now: Instant) {
        if !self.outputs.contains_key(name) {
            return;
        }
        if off {
            self.output_power_paused
                .entry(name.to_owned())
                .or_insert(now);
            return;
        }
        let Some(start) = self.output_power_paused.remove(name) else {
            return;
        };
        let independent = matches!(
            self.config.global.monitor_behavior,
            MonitorBehavior::Independent
        ) || (matches!(
            self.config.global.monitor_behavior,
            MonitorBehavior::Grouped(_)
        ) && !self.output_groups.contains_key(name));
        if independent && let Some(output) = self.outputs.get_mut(name) {
            let suspended = now.saturating_duration_since(start);
            if let Some(display_start) = output.display_start_time.as_mut() {
                *display_start += suspended;
            }
            if let Some(next_change) = output.next_change.as_mut() {
                *next_change += suspended;
            }
        }
    }
}
