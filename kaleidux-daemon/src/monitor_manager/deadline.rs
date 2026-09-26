use super::MonitorManager;
use crate::orchestration::{MonitorBehavior, PerformanceProfile};
use kaleidux_common::{MAX_INHIBITORS, validate_inhibit_reason};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::info;

impl MonitorManager {
    fn reset_display_timers(&mut self) {
        let now = Instant::now();
        for orch in self.outputs.values_mut() {
            orch.display_start_time = Some(now);
            orch.next_change = Some(now + orch.cycle_duration());
        }
        self.shared_display_start_time = Some(now);
        for start in self.group_display_start_times.values_mut() {
            *start = now;
        }
    }

    pub fn set_paused(&mut self, paused: bool) -> bool {
        let was_effective_paused = self.is_paused();
        self.manual_paused = paused;
        let is_effective_paused = self.is_paused();
        if !was_effective_paused && is_effective_paused {
            info!("[MONITOR_MANAGER] Wallpaper cycling paused (manual)");
            true
        } else if was_effective_paused && !is_effective_paused {
            // When resuming, reset timers so content doesn't immediately switch
            self.reset_display_timers();
            info!("[MONITOR_MANAGER] Wallpaper cycling resumed (manual, timers reset)");
            true
        } else {
            false
        }
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.manual_paused || !self.pause_reasons.is_empty()
    }

    #[allow(dead_code)]
    pub(crate) fn is_manual_paused(&self) -> bool {
        self.manual_paused
    }

    pub fn inhibit(&mut self, reason: String) -> Result<bool, &'static str> {
        validate_inhibit_reason(&reason)?;
        if !self.pause_reasons.contains(&reason) && self.pause_reasons.len() >= MAX_INHIBITORS {
            return Err("maximum inhibitor capacity reached");
        }
        let was_effective_paused = self.is_paused();
        self.pause_reasons.insert(reason);
        let is_effective_paused = self.is_paused();
        if !was_effective_paused && is_effective_paused {
            info!("[MONITOR_MANAGER] Wallpaper cycling paused (inhibited)");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn uninhibit(&mut self, reason: &str) -> Result<bool, &'static str> {
        validate_inhibit_reason(reason)?;
        let was_effective_paused = self.is_paused();
        self.pause_reasons.remove(reason);
        let is_effective_paused = self.is_paused();
        if was_effective_paused && !is_effective_paused {
            self.reset_display_timers();
            info!("[MONITOR_MANAGER] Wallpaper cycling resumed (inhibitor cleared, timers reset)");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn inhibitors(&self) -> Vec<String> {
        self.pause_reasons.iter().cloned().collect()
    }

    pub(crate) fn set_power_suspended(&mut self, suspended: bool) {
        if self.power_suspended == suspended {
            return;
        }
        self.power_suspended = suspended;
        if suspended {
            info!("[MONITOR_MANAGER] Wallpaper cycling suspended for display power");
        } else {
            self.reset_display_timers();
            info!("[MONITOR_MANAGER] Wallpaper cycling resumed after display power (timers reset)");
        }
    }

    pub fn next_switch_deadline(&self) -> Option<Instant> {
        if self.is_paused() || self.power_suspended {
            return None;
        }

        let mut next_deadline = None;

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                for orch in self.outputs.values() {
                    Self::earlier_deadline(&mut next_deadline, orch.next_deadline());
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(shared_start) = self.shared_display_start_time {
                    if let Some(first_orch) = self.outputs.values().next() {
                        next_deadline = Some(shared_start + first_orch.config.duration);
                    }
                } else if let Some(first_orch) = self.outputs.values().next() {
                    next_deadline = if let Some(display_start) = first_orch.display_start_time {
                        Some(display_start + first_orch.config.duration)
                    } else {
                        first_orch.next_change
                    };
                }
            }
            MonitorBehavior::Grouped(_) => {
                let mut groups_to_check: HashMap<usize, Vec<String>> = HashMap::new();
                for (name, gid) in &self.output_groups {
                    groups_to_check.entry(*gid).or_default().push(name.clone());
                }

                for (gid, output_names) in groups_to_check {
                    if let Some(group_start) = self.group_display_start_times.get(&gid) {
                        if let Some(first_name) = output_names.first() {
                            if let Some(orch) = self.outputs.get(first_name) {
                                Self::earlier_deadline(
                                    &mut next_deadline,
                                    Some(*group_start + orch.config.duration),
                                );
                            }
                        }
                    } else if let Some(first_name) = output_names.first() {
                        if let Some(orch) = self.outputs.get(first_name) {
                            let candidate = if let Some(display_start) = orch.display_start_time {
                                Some(display_start + orch.config.duration)
                            } else {
                                orch.next_change
                            };
                            Self::earlier_deadline(&mut next_deadline, candidate);
                        }
                    }
                }

                for (name, orch) in &self.outputs {
                    if !self.output_groups.contains_key(name) {
                        Self::earlier_deadline(&mut next_deadline, orch.next_deadline());
                    }
                }
            }
        }

        next_deadline
    }

    pub fn tick_due(&self, now: Instant) -> bool {
        if self.is_paused() || self.power_suspended {
            return false;
        }

        if self
            .outputs
            .values()
            .any(|orch| orch.current_path.is_none())
        {
            return true;
        }

        match self.next_switch_deadline() {
            Some(deadline) => deadline <= now,
            None => self
                .outputs
                .values()
                .any(|orch| orch.current_path.is_none()),
        }
    }

    pub fn due_low_power_outputs(&self, now: Instant) -> Vec<String> {
        if self.is_paused() || self.power_suspended {
            return Vec::new();
        }

        self.outputs
            .iter()
            .filter_map(|(name, orch)| {
                let low_power = orch.config.performance == PerformanceProfile::LowPower;
                let due = orch.current_path.is_none()
                    || orch.next_deadline().is_some_and(|deadline| deadline <= now);
                if low_power && due {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn defer_switch_deadline(&mut self, name: &str, defer: Duration) {
        let deadline = Instant::now() + defer;
        match &self.config.global.monitor_behavior {
            MonitorBehavior::Synchronized => {
                self.shared_display_start_time = None;
                for orch in self.outputs.values_mut() {
                    orch.display_start_time = None;
                    orch.next_change = Some(deadline);
                }
            }
            MonitorBehavior::Grouped(_) => {
                if let Some(group_id) = self.output_groups.get(name).copied() {
                    self.group_display_start_times.remove(&group_id);
                    for (output_name, output_group_id) in &self.output_groups {
                        if *output_group_id == group_id {
                            if let Some(orch) = self.outputs.get_mut(output_name) {
                                orch.display_start_time = None;
                                orch.next_change = Some(deadline);
                            }
                        }
                    }
                } else if let Some(orch) = self.outputs.get_mut(name) {
                    orch.display_start_time = None;
                    orch.next_change = Some(deadline);
                }
            }
            MonitorBehavior::Independent => {
                if let Some(orch) = self.outputs.get_mut(name) {
                    orch.display_start_time = None;
                    orch.next_change = Some(deadline);
                }
            }
        }
    }
}
