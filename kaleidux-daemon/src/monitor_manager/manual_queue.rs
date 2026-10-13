//! Validates manual queue targets and installs missing queues only after all checks succeed.

use super::MonitorManager;
use crate::orchestration::MonitorBehavior;
use crate::queue::SmartQueue;
use std::path::Path;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum ManualQueueTarget {
    Shared,
    Group(usize),
    Output(String),
}

impl MonitorManager {
    pub(super) fn manual_queue_targets(
        &self,
        output: &Option<String>,
    ) -> anyhow::Result<Vec<ManualQueueTarget>> {
        let names: Vec<_> = self
            .outputs
            .keys()
            .filter(|name| output.as_ref().is_none_or(|target| target == *name))
            .cloned()
            .collect();
        anyhow::ensure!(!names.is_empty(), "No slideshow outputs");
        if matches!(
            self.config.global.monitor_behavior,
            MonitorBehavior::Synchronized
        ) {
            return Ok(vec![ManualQueueTarget::Shared]);
        }
        let mut targets = Vec::new();
        for name in names {
            let target = match self.output_groups.get(&name) {
                Some(group) => ManualQueueTarget::Group(*group),
                None => ManualQueueTarget::Output(name),
            };
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        Ok(targets)
    }

    fn manual_queue(&self, target: &ManualQueueTarget) -> Option<&SmartQueue> {
        match target {
            ManualQueueTarget::Shared => self.shared_queue.as_ref(),
            ManualQueueTarget::Group(group) => self.group_queues.get(group),
            ManualQueueTarget::Output(name) => self.outputs.get(name)?.queue.as_ref(),
        }
    }

    pub(super) fn validate_manual_queues(
        &self,
        targets: &[ManualQueueTarget],
        path: &Path,
        jump: Option<bool>,
    ) -> anyhow::Result<()> {
        let global_blacklist = self.cache.get_all_blacklisted()?;
        anyhow::ensure!(
            !global_blacklist
                .iter()
                .any(|blocked| same_path(blocked, path)),
            "Wallpaper is blacklisted"
        );
        for target in targets {
            let Some(queue) = self.manual_queue(target) else {
                anyhow::ensure!(jump != Some(true), "No slideshow queue");
                continue;
            };
            let in_root =
                std::fs::canonicalize(&queue.root_path).is_ok_and(|root| path.starts_with(root));
            anyhow::ensure!(
                jump != Some(true) || in_root,
                "Wallpaper is outside the slideshow directory"
            );
            anyhow::ensure!(
                !queue
                    .stats
                    .blacklist
                    .iter()
                    .any(|blocked| same_path(blocked, path)),
                "Wallpaper is blacklisted"
            );
        }
        Ok(())
    }

    pub(super) fn enqueue_manual_selection(
        &mut self,
        targets: Vec<ManualQueueTarget>,
        path: &Path,
    ) -> anyhow::Result<()> {
        let mut missing = Vec::new();
        for target in &targets {
            if self.manual_queue(target).is_some() {
                continue;
            }
            let representative = match target {
                ManualQueueTarget::Output(name) => self.outputs.get(name),
                ManualQueueTarget::Shared => self.outputs.values().next(),
                ManualQueueTarget::Group(group) => self
                    .output_groups
                    .iter()
                    .find(|(_, id)| *id == group)
                    .and_then(|(name, _)| self.outputs.get(name)),
            }
            .ok_or_else(|| anyhow::anyhow!("Missing slideshow output"))?;
            let root = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Missing media directory"))?;
            let queue = SmartQueue::new_manual_from_pool(
                root,
                vec![path.to_path_buf()],
                representative.config.video_ratio,
                representative.config.sorting,
                self.cache.clone(),
            )?;
            missing.push((target.clone(), queue));
        }
        // Construction can fail; make every target concrete before mutating queues.
        for (target, queue) in missing {
            match target {
                ManualQueueTarget::Shared => self.shared_queue = Some(queue),
                ManualQueueTarget::Group(group) => {
                    self.group_queues.insert(group, queue);
                }
                ManualQueueTarget::Output(name) => {
                    self.outputs.get_mut(&name).expect("validated output").queue = Some(queue);
                }
            }
        }
        for target in targets {
            let queue = match target {
                ManualQueueTarget::Shared => self.shared_queue.as_mut(),
                ManualQueueTarget::Group(group) => self.group_queues.get_mut(&group),
                ManualQueueTarget::Output(name) => self
                    .outputs
                    .get_mut(&name)
                    .and_then(|output| output.queue.as_mut()),
            }
            .expect("validated manual queue");
            queue.enqueue_selected_image(path.to_path_buf());
        }
        Ok(())
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    left == right || std::fs::canonicalize(left).is_ok_and(|resolved| resolved == right)
}
