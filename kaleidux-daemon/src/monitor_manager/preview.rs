//! Snapshots slideshow selection and restores its elapsed time after a preview.
//! Manual inhibitors and output power remain live while the slideshow is frozen.

use super::{MonitorManager, OutputOrchestrator};
use crate::queue::SmartQueue;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub(crate) struct QueueSnapshot {
    outputs: HashMap<String, OutputOrchestrator>,
    shared: Option<SmartQueue>,
    groups: HashMap<usize, SmartQueue>,
    shared_start: Option<Instant>,
    group_starts: HashMap<usize, Instant>,
    output_groups: HashMap<String, usize>,
    power_pauses: HashMap<String, Instant>,
    captured: Instant,
}

impl QueueSnapshot {
    pub(crate) fn retire_output(&mut self, name: &str) {
        self.outputs.remove(name);
        self.power_pauses.remove(name);
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
            captured,
        }
    }

    pub(crate) fn begin_preview(&mut self) {
        self.preview_active = true;
    }

    pub(crate) fn end_preview(&mut self, snapshot: QueueSnapshot, apply: bool) {
        self.end_preview_at(snapshot, apply, Instant::now());
    }

    pub(super) fn end_preview_at(&mut self, snapshot: QueueSnapshot, apply: bool, now: Instant) {
        self.preview_active = false;
        if apply {
            for pause in self.output_power_paused.values_mut() {
                *pause = now;
            }
            for output in self.outputs.values_mut() {
                if output.current_path.is_some() {
                    output.display_start_time = Some(now);
                }
            }
            self.shared_display_start_time = self.shared_display_start_time.map(|_| now);
            for start in self.group_display_start_times.values_mut() {
                *start = now;
            }
            return;
        }
        let elapsed = now.saturating_duration_since(snapshot.captured);
        for (name, mut saved) in snapshot.outputs {
            if let Some(output) = self.outputs.get_mut(&name) {
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
        if let Some(shared) = snapshot.shared {
            self.shared_queue = Some(shared);
            self.shared_display_start_time = snapshot.shared_start.map(|start| start + elapsed);
        }
        for (group, queue) in snapshot.groups {
            self.group_queues.insert(group, queue);
            if let Some(start) = snapshot.group_starts.get(&group) {
                self.group_display_start_times
                    .insert(group, *start + elapsed);
            }
        }
    }
}
