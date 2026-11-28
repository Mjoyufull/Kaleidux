//! Snapshots slideshow selection and restores its elapsed time after a preview.
//! Manual inhibitors and output power remain live while the slideshow is frozen.

use super::{MonitorManager, OutputOrchestrator};
use crate::queue::SmartQueue;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub(crate) struct QueueSnapshot {
    outputs: HashMap<String, OutputOrchestrator>,
    shared: Option<SmartQueue>,
    groups: HashMap<usize, SmartQueue>,
    shared_start: Option<Instant>,
    group_starts: HashMap<usize, Instant>,
    output_groups: HashMap<String, usize>,
    power_pauses: HashMap<String, Instant>,
    targets: HashSet<String>,
    captured: Instant,
}

impl QueueSnapshot {
    pub(crate) fn retire_output(&mut self, name: &str) {
        self.outputs.remove(name);
        self.power_pauses.remove(name);
        self.targets.remove(name);
        if self.outputs.is_empty() {
            self.shared = None;
            self.shared_start = None;
        }
        if let Some(group) = self.output_groups.remove(name)
            && !self.output_groups.values().any(|id| *id == group)
        {
            self.groups.remove(&group);
            self.group_starts.remove(&group);
        }
    }

    pub(crate) fn set_targets(&mut self, names: impl Iterator<Item = String>) {
        self.targets = names.collect();
    }
}

impl MonitorManager {
    pub(crate) fn snapshot_queues(&self) -> QueueSnapshot {
        self.snapshot_queues_at(Instant::now())
    }

    pub(super) fn snapshot_queues_at(&self, captured: Instant) -> QueueSnapshot {
        QueueSnapshot {
            outputs: self.outputs.clone(),
            shared: self.shared_queue.clone(),
            groups: self.group_queues.clone(),
            shared_start: self.shared_display_start_time,
            group_starts: self.group_display_start_times.clone(),
            output_groups: self.output_groups.clone(),
            power_pauses: self.output_power_paused.clone(),
            targets: HashSet::new(),
            captured,
        }
    }

    pub(crate) fn begin_preview(&mut self) {
        self.preview_active = true;
    }

    pub(crate) fn end_preview(&mut self, snapshot: QueueSnapshot, apply: bool) {
        self.end_preview_at(snapshot, apply, Instant::now());
    }

    pub(super) fn end_preview_at(
        &mut self,
        mut snapshot: QueueSnapshot,
        apply: bool,
        now: Instant,
    ) {
        self.preview_active = false;
        let elapsed = now.saturating_duration_since(snapshot.captured);
        let surviving: HashSet<_> = snapshot
            .outputs
            .keys()
            .filter(|name| self.outputs.contains_key(*name))
            .cloned()
            .collect();
        for (name, mut saved) in std::mem::take(&mut snapshot.outputs) {
            if let Some(output) = self.outputs.get_mut(&name) {
                if apply && snapshot.targets.contains(&name) {
                    if let Some(pause) = self.output_power_paused.get_mut(&name) {
                        *pause = now;
                    }
                    if output.current_path.is_some() {
                        output.display_start_time = Some(now);
                        output.next_change = Some(now + output.cycle_duration());
                    }
                    continue;
                }
                // Credit any power pause before preview separately. Preview
                // already covers all overlapping time; future wake adds only
                // the off time after this restoration.
                let before_preview = snapshot
                    .power_pauses
                    .get(&name)
                    .map_or(Duration::ZERO, |pause| {
                        snapshot.captured.saturating_duration_since(*pause)
                    });
                let frozen = elapsed + before_preview;
                if let Some(pause) = self.output_power_paused.get_mut(&name) {
                    *pause = now;
                }
                saved.display_start_time = saved.display_start_time.map(|start| start + frozen);
                saved.next_change = saved.next_change.map(|deadline| deadline + frozen);
                *output = saved;
            }
        }
        self.finish_preview_cohorts(snapshot, &surviving, apply, now);
        // A temporary preview queue may have suppressed discovery when a new
        // peer connected. Restoring queue absence must admit that work again.
        self.start_missing_output_queues();
    }

    fn finish_preview_cohorts(
        &mut self,
        mut snapshot: QueueSnapshot,
        surviving: &HashSet<String>,
        apply: bool,
        now: Instant,
    ) {
        let elapsed = now.saturating_duration_since(snapshot.captured);
        if !surviving.is_empty()
            && matches!(
                self.config.global.monitor_behavior,
                crate::orchestration::MonitorBehavior::Synchronized
            )
        {
            if apply && snapshot.targets.iter().any(|name| surviving.contains(name)) {
                self.shared_display_start_time = Some(now);
            } else {
                // None is part of the snapshot too. Do not keep a candidate
                // queue created while previewing a previously blank cohort.
                self.shared_queue = snapshot.shared;
                self.shared_display_start_time = snapshot.shared_start.map(|start| start + elapsed);
            }
        }
        let groups: HashSet<_> = surviving
            .iter()
            .filter_map(|name| snapshot.output_groups.get(name).copied())
            .collect();
        for group in groups {
            let applied = apply
                && snapshot.targets.iter().any(|name| {
                    surviving.contains(name) && snapshot.output_groups.get(name) == Some(&group)
                });
            if applied {
                self.group_display_start_times.insert(group, now);
                continue;
            }
            if let Some(queue) = snapshot.groups.remove(&group) {
                self.group_queues.insert(group, queue);
            } else {
                self.group_queues.remove(&group);
            }
            if let Some(start) = snapshot.group_starts.get(&group) {
                self.group_display_start_times
                    .insert(group, *start + elapsed);
            } else {
                self.group_display_start_times.remove(&group);
            }
        }
    }
}
