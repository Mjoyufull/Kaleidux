use super::{MonitorManager, content_load_timeout};
use crate::orchestration::MonitorBehavior;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

impl MonitorManager {
    /// Validate every target before changing any queue. None selects jump/set by root.
    pub(crate) fn prepare_image_selection(
        &mut self,
        path: &str,
        output: &Option<String>,
        jump: Option<bool>,
    ) -> anyhow::Result<()> {
        let path = std::fs::canonicalize(path)?;
        anyhow::ensure!(path.is_file(), "Image path is not a file");
        anyhow::ensure!(
            Self::resolve_content_type(&path, "image selection")
                == Some(crate::queue::ContentType::Image),
            "Path is not a supported image"
        );
        if let Some(name) = output {
            anyhow::ensure!(self.outputs.contains_key(name), "Unknown output: {name}");
        }
        let mut queues = Vec::new();
        match &self.config.global.monitor_behavior {
            MonitorBehavior::Synchronized => {
                queues.push(
                    self.shared_queue
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("No slideshow queue"))?,
                );
            }
            MonitorBehavior::Independent | MonitorBehavior::Grouped(_) => {
                let grouped = matches!(
                    self.config.global.monitor_behavior,
                    MonitorBehavior::Grouped(_)
                );
                let mut groups = std::collections::HashSet::new();
                for (name, orch) in &mut self.outputs {
                    if output.as_ref().is_some_and(|target| target != name) {
                        continue;
                    }
                    if grouped && let Some(gid) = self.output_groups.get(name) {
                        groups.insert(*gid);
                    } else {
                        queues.push(
                            orch.queue
                                .as_mut()
                                .ok_or_else(|| anyhow::anyhow!("No slideshow queue for {name}"))?,
                        );
                    }
                }
                for (gid, queue) in &mut self.group_queues {
                    if groups.remove(gid) {
                        queues.push(queue);
                    }
                }
                anyhow::ensure!(groups.is_empty(), "Missing group slideshow queue");
            }
        }
        anyhow::ensure!(!queues.is_empty(), "No slideshow outputs");
        for queue in &queues {
            let in_root =
                std::fs::canonicalize(&queue.root_path).is_ok_and(|root| path.starts_with(root));
            anyhow::ensure!(
                jump != Some(true) || in_root,
                "Image is outside the slideshow directory"
            );
            anyhow::ensure!(
                !queue.stats.blacklist.contains(&path),
                "Image is blacklisted"
            );
        }
        for queue in queues {
            queue.enqueue_selected_image(path.clone());
        }
        Ok(())
    }

    pub fn handle_next(
        &mut self,
        output_name: Option<String>,
    ) -> HashMap<String, (PathBuf, crate::queue::ContentType)> {
        let mut changes = HashMap::new();
        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                if let Some(name) = output_name {
                    if let Some(orch) = self.outputs.get_mut(&name) {
                        if let Some(res) = orch.pick_next() {
                            changes.insert(name, res);
                        }
                    }
                } else {
                    for (name, orch) in &mut self.outputs {
                        if let Some(res) = orch.pick_next() {
                            changes.insert(name.clone(), res);
                        }
                    }
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(queue) = &mut self.shared_queue {
                    if let Some(path) = queue.pick_next() {
                        let Some(content_type) =
                            Self::resolve_content_type(&path, "synchronized next")
                        else {
                            return changes;
                        };
                        let now = Instant::now();

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
            MonitorBehavior::Grouped(_) => {
                if let Some(target_name) = output_name {
                    // If target is in a group, advance all in group
                    if let Some(gid) = self.output_groups.get(&target_name).copied() {
                        if let Some(queue) = self.group_queues.get_mut(&gid) {
                            if let Some(path) = queue.pick_next() {
                                let Some(content_type) =
                                    Self::resolve_content_type(&path, "grouped next")
                                else {
                                    return changes;
                                };

                                // Pre-buffer next content
                                let (next_p, next_t) = if let Some((np, nt)) = queue.peek_next() {
                                    (Some(np), Some(nt))
                                } else {
                                    (None, None)
                                };

                                // Reset group display start time for next cycle
                                self.group_display_start_times.remove(&gid);
                                for (name, orch_gid) in &self.output_groups {
                                    if *orch_gid == gid {
                                        if let Some(orch) = self.outputs.get_mut(name) {
                                            orch.current_path = Some(path.clone());
                                            orch.display_start_time = None;
                                            orch.next_change = Some(
                                                Instant::now()
                                                    + content_load_timeout(orch.config.duration),
                                            );

                                            orch.next_path = next_p.clone();
                                            orch.next_content_type = next_t;

                                            changes
                                                .insert(name.clone(), (path.clone(), content_type));
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Not in a group, treat as independent
                        if let Some(orch) = self.outputs.get_mut(&target_name) {
                            if let Some(res) = orch.pick_next() {
                                changes.insert(target_name, res);
                            }
                        }
                    }
                } else {
                    // No target specified, advance all groups and independents
                    let mut advanced_groups = std::collections::HashSet::new();
                    for gid in self.output_groups.values() {
                        if !advanced_groups.contains(gid) {
                            if let Some(queue) = self.group_queues.get_mut(gid) {
                                if let Some(path) = queue.pick_next() {
                                    let Some(content_type) =
                                        Self::resolve_content_type(&path, "grouped next-all")
                                    else {
                                        continue;
                                    };
                                    // Reset group display start time for next cycle
                                    self.group_display_start_times.remove(gid);
                                    for (n, og) in &self.output_groups {
                                        if og == gid {
                                            if let Some(orch) = self.outputs.get_mut(n) {
                                                orch.current_path = Some(path.clone());
                                                orch.display_start_time = None;
                                                orch.next_change = Some(
                                                    Instant::now()
                                                        + content_load_timeout(
                                                            orch.config.duration,
                                                        ),
                                                );
                                                changes.insert(
                                                    n.clone(),
                                                    (path.clone(), content_type),
                                                );
                                            }
                                        }
                                    }
                                    advanced_groups.insert(*gid);
                                }
                            }
                        }
                    }
                    // Also handle ungrouped outputs
                    for (name, orch) in &mut self.outputs {
                        if !self.output_groups.contains_key(name) {
                            if let Some(res) = orch.pick_next() {
                                changes.insert(name.clone(), res);
                            }
                        }
                    }
                }
            }
        }
        changes
    }

    pub fn handle_prev(
        &mut self,
        output_name: Option<String>,
    ) -> HashMap<String, (PathBuf, crate::queue::ContentType)> {
        let mut changes = HashMap::new();
        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                if let Some(name) = output_name {
                    if let Some(orch) = self.outputs.get_mut(&name) {
                        if let Some(res) = orch.pick_prev() {
                            changes.insert(name, res);
                        }
                    }
                } else {
                    for (name, orch) in &mut self.outputs {
                        if let Some(res) = orch.pick_prev() {
                            changes.insert(name.clone(), res);
                        }
                    }
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(queue) = &mut self.shared_queue {
                    if let Some(path) = queue.pick_prev() {
                        let Some(content_type) =
                            Self::resolve_content_type(&path, "synchronized prev")
                        else {
                            return changes;
                        };
                        let now = Instant::now();
                        // Reset shared display start time for next cycle
                        self.shared_display_start_time = None;
                        let next = queue.peek_next();
                        for (name, orch) in &mut self.outputs {
                            orch.current_path = Some(path.clone());
                            orch.display_start_time = None;
                            orch.next_change =
                                Some(now + content_load_timeout(orch.config.duration));
                            orch.next_path = next.as_ref().map(|(path, _)| path.clone());
                            orch.next_content_type = next.as_ref().map(|(_, kind)| *kind);
                            changes.insert(name.clone(), (path.clone(), content_type));
                        }
                    }
                }
            }
            MonitorBehavior::Grouped(_) => {
                if let Some(target_name) = output_name {
                    if let Some(gid) = self.output_groups.get(&target_name).copied() {
                        if let Some(queue) = self.group_queues.get_mut(&gid) {
                            if let Some(path) = queue.pick_prev() {
                                let Some(content_type) =
                                    Self::resolve_content_type(&path, "grouped prev")
                                else {
                                    return changes;
                                };
                                // Reset group display start time for next cycle
                                self.group_display_start_times.remove(&gid);
                                let next = queue.peek_next();
                                for (name, og) in &self.output_groups {
                                    if og == &gid {
                                        if let Some(orch) = self.outputs.get_mut(name) {
                                            orch.current_path = Some(path.clone());
                                            orch.next_path =
                                                next.as_ref().map(|(path, _)| path.clone());
                                            orch.next_content_type =
                                                next.as_ref().map(|(_, kind)| *kind);
                                            orch.display_start_time = None;
                                            orch.next_change = Some(
                                                Instant::now()
                                                    + content_load_timeout(orch.config.duration),
                                            );
                                            changes
                                                .insert(name.clone(), (path.clone(), content_type));
                                        }
                                    }
                                }
                            }
                        }
                    } else if let Some(orch) = self.outputs.get_mut(&target_name) {
                        if let Some(res) = orch.pick_prev() {
                            changes.insert(target_name, res);
                        }
                    }
                } else {
                    let mut prev_groups = std::collections::HashSet::new();
                    for gid in self.output_groups.values() {
                        if !prev_groups.contains(gid) {
                            if let Some(queue) = self.group_queues.get_mut(gid) {
                                if let Some(path) = queue.pick_prev() {
                                    let Some(content_type) =
                                        Self::resolve_content_type(&path, "grouped prev-all")
                                    else {
                                        continue;
                                    };
                                    // Reset group display start time for next cycle
                                    self.group_display_start_times.remove(gid);
                                    let next = queue.peek_next();
                                    for (n, og) in &self.output_groups {
                                        if og == gid {
                                            if let Some(orch) = self.outputs.get_mut(n) {
                                                orch.current_path = Some(path.clone());
                                                orch.next_path =
                                                    next.as_ref().map(|(path, _)| path.clone());
                                                orch.next_content_type =
                                                    next.as_ref().map(|(_, kind)| *kind);
                                                orch.display_start_time = None;
                                                orch.next_change = Some(
                                                    Instant::now()
                                                        + content_load_timeout(
                                                            orch.config.duration,
                                                        ),
                                                );
                                                changes.insert(
                                                    n.clone(),
                                                    (path.clone(), content_type),
                                                );
                                            }
                                        }
                                    }
                                    prev_groups.insert(*gid);
                                }
                            }
                        }
                    }
                    for (name, orch) in &mut self.outputs {
                        if !self.output_groups.contains_key(name) {
                            if let Some(res) = orch.pick_prev() {
                                changes.insert(name.clone(), res);
                            }
                        }
                    }
                }
            }
        }
        changes
    }
}
