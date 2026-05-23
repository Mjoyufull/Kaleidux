use super::MonitorManager;
use crate::orchestration::{MonitorBehavior, PerformanceProfile};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::info;

impl MonitorManager {
    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
        if paused {
            info!("[MONITOR_MANAGER] Wallpaper cycling paused");
        } else {
            // When resuming, reset timers so content doesn't immediately switch
            let now = Instant::now();
            for orch in self.outputs.values_mut() {
                orch.display_start_time = Some(now);
                orch.next_change = Some(now + orch.cycle_duration());
            }
            self.shared_display_start_time = Some(now);
            for start in self.group_display_start_times.values_mut() {
                *start = now;
            }
            info!("[MONITOR_MANAGER] Wallpaper cycling resumed (timers reset)");
        }
    }

    pub fn next_switch_deadline(&self) -> Option<Instant> {
        if self.paused {
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
        if self.paused {
            return false;
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
        if self.paused {
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
