//! Snapshots slideshow selection and restores its elapsed time after a preview.
//! Manual inhibitors and output power remain live while the slideshow is frozen.

use super::{MonitorManager, OutputOrchestrator};
use crate::queue::SmartQueue;
use std::collections::HashMap;
use std::time::Instant;

pub(crate) struct QueueSnapshot {
    outputs: HashMap<String, OutputOrchestrator>,
    shared: Option<SmartQueue>,
    groups: HashMap<usize, SmartQueue>,
    shared_start: Option<Instant>,
    group_starts: HashMap<usize, Instant>,
    captured: Instant,
}

impl MonitorManager {
    pub(crate) fn snapshot_queues(&self) -> QueueSnapshot {
        QueueSnapshot {
            outputs: self.outputs.clone(),
            shared: self.shared_queue.clone(),
            groups: self.group_queues.clone(),
            shared_start: self.shared_display_start_time,
            group_starts: self.group_display_start_times.clone(),
            captured: Instant::now(),
        }
    }

    pub(crate) fn begin_preview(&mut self) {
        self.preview_active = true;
    }

    pub(crate) fn end_preview(&mut self, snapshot: QueueSnapshot, apply: bool) {
        let now = Instant::now();
        self.preview_active = false;
        if apply {
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
                saved.display_start_time = saved.display_start_time.map(|start| start + elapsed);
                saved.next_change = saved.next_change.map(|deadline| deadline + elapsed);
                *output = saved;
            }
        }
        self.shared_queue = snapshot.shared;
        self.group_queues = snapshot.groups;
        self.shared_display_start_time = snapshot.shared_start.map(|start| start + elapsed);
        self.group_display_start_times = snapshot
            .group_starts
            .into_iter()
            .map(|(group, start)| (group, start + elapsed))
            .collect();
    }
}
