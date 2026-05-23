use super::{MonitorManager, content_load_timeout};
use crate::orchestration::MonitorBehavior;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

impl MonitorManager {
    pub fn tick(&mut self) -> HashMap<String, (PathBuf, crate::queue::ContentType)> {
        let mut changes = HashMap::new();
        // Don't cycle wallpapers when paused
        if self.paused {
            return changes;
        }
        let now = Instant::now();

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                for (name, orch) in &mut self.outputs {
                    if let Some(res) = orch.tick() {
                        changes.insert(name.clone(), res);
                    }
                }
            }
            MonitorBehavior::Synchronized => {
                let mut should_change = false;
                // Use shared display start time for synchronized outputs
                if let Some(shared_start) = self.shared_display_start_time {
                    let elapsed = now.saturating_duration_since(shared_start);
                    if let Some(first_orch) = self.outputs.values().next() {
                        if elapsed >= first_orch.config.duration {
                            should_change = true;
                        }
                    }
                } else if let Some(first_orch) = self.outputs.values().next() {
                    // Fallback: check individual times if shared time not set yet
                    if let Some(display_start) = first_orch.display_start_time {
                        let elapsed = now.saturating_duration_since(display_start);
                        if elapsed >= first_orch.config.duration {
                            should_change = true;
                        }
                    } else if let Some(next) = first_orch.next_change {
                        if now >= next {
                            should_change = true;
                        }
                    } else if first_orch.current_path.is_none() {
                        should_change = true;
                    }
                }

                if should_change {
                    if let Some(queue) = &mut self.shared_queue {
                        if let Some(path) = queue.pick_next() {
                            let Some(content_type) =
                                Self::resolve_content_type(&path, "synchronized tick")
                            else {
                                return changes;
                            };

                            // Pre-buffer next content
                            let (next_p, next_t) = if let Some((np, nt)) = queue.peek_next() {
                                (Some(np), Some(nt))
                            } else {
                                (None, None)
                            };

                            // Reset shared display start time for next cycle
                            self.shared_display_start_time = None;
                            for (name, orch) in &mut self.outputs {
                                orch.current_path = Some(path.clone());
                                orch.display_start_time = None;
                                orch.next_change =
                                    Some(now + content_load_timeout(orch.config.duration));

                                orch.next_path = next_p.clone();
                                orch.next_content_type = next_t;

                                changes.insert(name.clone(), (path.clone(), content_type));
                            }
                        }
                    }
                }
            }
            MonitorBehavior::Grouped(_) => {
                // Check each group independently
                let mut groups_to_tick: HashMap<usize, Vec<String>> = HashMap::new();

                for (name, gid) in &self.output_groups {
                    groups_to_tick.entry(*gid).or_default().push(name.clone());
                }

                for (gid, output_names) in groups_to_tick {
                    // Check if any output in this group needs a change
                    let mut should_change = false;
                    // Use group display start time if available
                    if let Some(group_start) = self.group_display_start_times.get(&gid) {
                        let elapsed = now.saturating_duration_since(*group_start);
                        if let Some(first_name) = output_names.first() {
                            if let Some(orch) = self.outputs.get(first_name) {
                                if elapsed >= orch.config.duration {
                                    should_change = true;
                                }
                            }
                        }
                    } else if let Some(first_name) = output_names.first() {
                        // Fallback: check individual times if group time not set yet
                        if let Some(orch) = self.outputs.get(first_name) {
                            if let Some(display_start) = orch.display_start_time {
                                let elapsed = now.saturating_duration_since(display_start);
                                if elapsed >= orch.config.duration {
                                    should_change = true;
                                }
                            } else if let Some(next) = orch.next_change {
                                if now >= next {
                                    should_change = true;
                                }
                            } else if orch.current_path.is_none() {
                                should_change = true;
                            }
                        }
                    }

                    if should_change {
                        if let Some(queue) = self.group_queues.get_mut(&gid) {
                            if let Some(path) = queue.pick_next() {
                                let Some(content_type) =
                                    Self::resolve_content_type(&path, "grouped tick")
                                else {
                                    continue;
                                };

                                // Pre-buffer next content
                                let (next_p, next_t) = if let Some((np, nt)) = queue.peek_next() {
                                    (Some(np), Some(nt))
                                } else {
                                    (None, None)
                                };

                                // Reset group display start time for next cycle
                                self.group_display_start_times.remove(&gid);
                                for name in &output_names {
                                    if let Some(orch) = self.outputs.get_mut(name) {
                                        orch.current_path = Some(path.clone());
                                        orch.display_start_time = None;
                                        orch.next_change =
                                            Some(now + content_load_timeout(orch.config.duration));

                                        orch.next_path = next_p.clone();
                                        orch.next_content_type = next_t;

                                        changes.insert(name.clone(), (path.clone(), content_type));
                                    }
                                }
                            }
                        }
                    }
                }

                // Also tick independent outputs (not in any group)
                for (name, orch) in &mut self.outputs {
                    if !self.output_groups.contains_key(name) {
                        if let Some(res) = orch.tick() {
                            changes.insert(name.clone(), res);
                        }
                    }
                }
            }
        }

        changes
    }

    pub fn pick_startup_replacement(
        &mut self,
        output_name: &str,
        excluded: &HashSet<PathBuf>,
    ) -> HashMap<String, (PathBuf, crate::queue::ContentType)> {
        let mut changes = HashMap::new();
        let now = Instant::now();

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                if let Some(orch) = self.outputs.get_mut(output_name) {
                    if let Some(res) = orch.pick_next_excluding(excluded) {
                        changes.insert(output_name.to_string(), res);
                    }
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(queue) = &mut self.shared_queue {
                    if let Some(path) = queue.pick_next_excluding(excluded) {
                        let Some(content_type) =
                            Self::resolve_content_type(&path, "synchronized startup replacement")
                        else {
                            return changes;
                        };
                        let (next_p, next_t) = if let Some((np, nt)) = queue.peek_next() {
                            (Some(np), Some(nt))
                        } else {
                            (None, None)
                        };

                        self.shared_display_start_time = None;
                        for (name, orch) in &mut self.outputs {
                            orch.current_path = Some(path.clone());
                            orch.display_start_time = None;
                            orch.next_change =
                                Some(now + content_load_timeout(orch.config.duration));
                            orch.next_path = next_p.clone();
                            orch.next_content_type = next_t;
                            changes.insert(name.clone(), (path.clone(), content_type));
                        }
                    }
                }
            }
            MonitorBehavior::Grouped(_) => {
                if let Some(gid) = self.output_groups.get(output_name).copied() {
                    if let Some(queue) = self.group_queues.get_mut(&gid) {
                        if let Some(path) = queue.pick_next_excluding(excluded) {
                            let Some(content_type) =
                                Self::resolve_content_type(&path, "grouped startup replacement")
                            else {
                                return changes;
                            };
                            let (next_p, next_t) = if let Some((np, nt)) = queue.peek_next() {
                                (Some(np), Some(nt))
                            } else {
                                (None, None)
                            };

                            self.group_display_start_times.remove(&gid);
                            for (name, orch_gid) in &self.output_groups {
                                if *orch_gid == gid {
                                    if let Some(orch) = self.outputs.get_mut(name) {
                                        orch.current_path = Some(path.clone());
                                        orch.display_start_time = None;
                                        orch.next_change =
                                            Some(now + content_load_timeout(orch.config.duration));
                                        orch.next_path = next_p.clone();
                                        orch.next_content_type = next_t;
                                        changes.insert(name.clone(), (path.clone(), content_type));
                                    }
                                }
                            }
                        }
                    }
                } else if let Some(orch) = self.outputs.get_mut(output_name) {
                    if let Some(res) = orch.pick_next_excluding(excluded) {
                        changes.insert(output_name.to_string(), res);
                    }
                }
            }
        }

        changes
    }
}
