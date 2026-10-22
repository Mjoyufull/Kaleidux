//! Owns hotplug queue workers; finished queues are adopted without awaiting filesystem work.

use super::manual_queue::ManualQueueTarget;
use super::{MonitorManager, OutputOrchestrator};
use crate::orchestration::{MonitorBehavior, OutputConfig};
use crate::queue::SmartQueue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

struct PendingQueue {
    config: OutputConfig,
    behavior: MonitorBehavior,
    owner_name: String,
    owner_description: String,
    task: tokio::task::JoinHandle<anyhow::Result<(SmartQueue, Vec<PathBuf>)>>,
}

struct QueueRetry {
    due: Instant,
    delay: Duration,
}

#[derive(Default)]
pub(super) struct PendingOutputQueues {
    tasks: HashMap<ManualQueueTarget, PendingQueue>,
    retries: HashMap<ManualQueueTarget, QueueRetry>,
}

impl Drop for PendingOutputQueues {
    fn drop(&mut self) {
        for pending in self.tasks.values() {
            pending.task.abort();
        }
    }
}

impl MonitorManager {
    pub(crate) fn add_output_deferred(&mut self, name: &str, description: &str) {
        let config = self.config.get_config_for_output(name, description);
        if let MonitorBehavior::Grouped(groups) = &self.config.global.monitor_behavior
            && let Some(group) = groups
                .iter()
                .position(|members| members.iter().any(|member| member == name))
        {
            self.output_groups.insert(name.to_owned(), group);
        }
        self.outputs.insert(
            name.to_owned(),
            OutputOrchestrator::without_queue(name.to_owned(), description.to_owned(), config),
        );
        let target = self.queue_target(name);
        self.start_output_queue(target);
    }

    fn queue_target(&self, name: &str) -> ManualQueueTarget {
        match &self.config.global.monitor_behavior {
            MonitorBehavior::Synchronized => ManualQueueTarget::Shared,
            MonitorBehavior::Grouped(_) if self.output_groups.contains_key(name) => {
                ManualQueueTarget::Group(self.output_groups[name])
            }
            _ => ManualQueueTarget::Output(name.to_owned()),
        }
    }

    pub(super) fn start_missing_output_queues(&mut self) {
        let targets: std::collections::HashSet<_> = self
            .outputs
            .keys()
            .map(|name| self.queue_target(name))
            .collect();
        for target in targets {
            self.start_output_queue(target);
        }
    }

    fn queue_owner(&self, target: &ManualQueueTarget) -> Option<&OutputOrchestrator> {
        if matches!(target, ManualQueueTarget::Shared)
            && !matches!(
                self.config.global.monitor_behavior,
                MonitorBehavior::Synchronized
            )
            || matches!(target, ManualQueueTarget::Group(_))
                && !matches!(
                    self.config.global.monitor_behavior,
                    MonitorBehavior::Grouped(_)
                )
        {
            return None;
        }
        self.outputs
            .values()
            .filter(|output| {
                output.config.path.is_some()
                    && match target {
                        ManualQueueTarget::Output(name) => &output._name == name,
                        ManualQueueTarget::Shared => true,
                        ManualQueueTarget::Group(group) => {
                            self.output_groups.get(&output._name) == Some(group)
                        }
                    }
            })
            .min_by(|left, right| left._name.cmp(&right._name))
    }

    fn queue_exists(&self, target: &ManualQueueTarget) -> bool {
        match target {
            ManualQueueTarget::Output(name) => self
                .outputs
                .get(name)
                .is_some_and(|output| output.queue.is_some()),
            ManualQueueTarget::Shared => self.shared_queue.is_some(),
            ManualQueueTarget::Group(group) => self.group_queues.contains_key(group),
        }
    }

    fn start_output_queue(&mut self, target: ManualQueueTarget) {
        if self.queue_exists(&target) {
            self.pending_output_queues.retries.remove(&target);
            return;
        }
        if self.pending_output_queues.tasks.contains_key(&target)
            || self
                .pending_output_queues
                .retries
                .get(&target)
                .is_some_and(|retry| retry.due > Instant::now())
        {
            return;
        }
        let Some(owner) = self.queue_owner(&target) else {
            return;
        };
        let owner_name = owner._name.clone();
        let owner_description = owner.description.clone();
        let config = owner.config.clone();
        let Some(path) = config.path.clone() else {
            return;
        };
        let cached = self.discovered_files_cache.get(&path).cloned();
        let cache = self.cache.clone();
        let metrics = self.metrics.clone();
        let task_config = config.clone();
        let task = tokio::spawn(async move {
            let mut queue = match cached {
                Some(pool) => SmartQueue::new_from_pool(
                    &path,
                    pool.as_ref().clone(),
                    task_config.video_ratio,
                    task_config.sorting,
                    cache,
                )?,
                None => {
                    SmartQueue::new_with_cache(
                        &path,
                        task_config.video_ratio,
                        task_config.sorting,
                        cache,
                        metrics,
                    )
                    .await?
                }
            };
            if let Some(playlist) = task_config.default_playlist {
                if let Err(error) = queue.set_playlist(Some(playlist)) {
                    tracing::warn!(
                        "[HOTPLUG-QUEUE] Default playlist unavailable: {error}; retaining root queue"
                    );
                }
            }
            let snapshot = queue.root_pool_snapshot();
            Ok((queue, snapshot))
        });
        self.pending_output_queues.tasks.insert(
            target,
            PendingQueue {
                config,
                behavior: self.config.global.monitor_behavior.clone(),
                owner_name,
                owner_description,
                task,
            },
        );
    }

    pub(crate) async fn poll_output_queues(&mut self) {
        let finished: Vec<_> = self
            .pending_output_queues
            .tasks
            .iter()
            .filter(|(_, pending)| pending.task.is_finished())
            .map(|(target, _)| target.clone())
            .collect();
        for target in finished {
            let pending = self
                .pending_output_queues
                .tasks
                .remove(&target)
                .expect("finished queue task");
            if self.config.global.monitor_behavior != pending.behavior {
                pending.task.abort();
                let names: Vec<_> = self.outputs.keys().cloned().collect();
                for name in names {
                    self.start_output_queue(self.queue_target(&name));
                }
                continue;
            }
            if self.queue_owner(&target).is_none() {
                continue;
            }
            let current = self
                .config
                .get_config_for_output(&pending.owner_name, &pending.owner_description);
            if Self::queue_config_changed(&pending.config, &current) {
                pending.task.abort();
                self.start_output_queue(target);
                continue;
            }
            // is_finished was observed before await; no scan is awaited here.
            let (queue, snapshot) = match pending.task.await {
                Ok(Ok(prepared)) => prepared,
                Ok(Err(error)) => {
                    tracing::warn!(
                        "[HOTPLUG-QUEUE] Preparing {:?} failed: {error}",
                        pending.config.path
                    );
                    self.defer_queue_retry(target);
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        "[HOTPLUG-QUEUE] Preparing {:?} task failed: {error}",
                        pending.config.path
                    );
                    self.defer_queue_retry(target);
                    continue;
                }
            };
            self.pending_output_queues.retries.remove(&target);
            if let Some(path) = pending.config.path {
                self.discovered_files_cache
                    .insert(path, std::sync::Arc::new(snapshot));
            }
            if self.queue_exists(&target) {
                continue;
            } // A manual command initialized it meanwhile.
            match target {
                ManualQueueTarget::Output(name) => {
                    self.outputs.get_mut(&name).expect("live output").queue = Some(queue)
                }
                ManualQueueTarget::Shared => self.shared_queue = Some(queue),
                ManualQueueTarget::Group(group) => {
                    self.group_queues.insert(group, queue);
                }
            }
        }
        let due: Vec<_> = self
            .pending_output_queues
            .retries
            .iter()
            .filter(|(_, retry)| retry.due <= Instant::now())
            .map(|(target, _)| target.clone())
            .collect();
        for target in due {
            if self.queue_owner(&target).is_none() {
                self.pending_output_queues.retries.remove(&target);
            } else {
                self.start_output_queue(target);
            }
        }
    }

    fn defer_queue_retry(&mut self, target: ManualQueueTarget) {
        let delay = self
            .pending_output_queues
            .retries
            .get(&target)
            .map_or(Duration::from_millis(250), |previous| {
                (previous.delay * 2).min(Duration::from_secs(30))
            });
        self.pending_output_queues.retries.insert(
            target,
            QueueRetry {
                due: Instant::now() + delay,
                delay,
            },
        );
    }

    pub(super) fn note_output_queue_events(&self, events: &[crate::cache::PoolEvent]) {
        let roots: std::collections::HashSet<_> = self
            .pending_output_queues
            .tasks
            .values()
            .filter_map(|pending| pending.config.path.as_ref())
            .collect();
        for root in roots {
            let canonical = root.canonicalize().unwrap_or_else(|_| root.clone());
            if events.iter().any(|event| {
                let path = match event {
                    crate::cache::PoolEvent::Added(path)
                    | crate::cache::PoolEvent::Removed(path)
                    | crate::cache::PoolEvent::Modified(path)
                    | crate::cache::PoolEvent::Rescan(path) => path,
                };
                path.starts_with(root)
                    || path.starts_with(&canonical)
                    || canonical.starts_with(path)
            }) {
                SmartQueue::schedule_root_rescan_for(
                    crate::queue::media_index::shared_root_index(root),
                    root.clone(),
                    self.cache.clone(),
                );
            }
        }
    }

    pub(crate) fn output_queue_deadline(&self, now: Instant) -> Option<Instant> {
        let active =
            (!self.pending_output_queues.tasks.is_empty()).then(|| now + Duration::from_millis(50));
        let retry = self
            .pending_output_queues
            .retries
            .iter()
            .filter(|(target, _)| !self.pending_output_queues.tasks.contains_key(*target))
            .map(|(_, retry)| retry.due)
            .min();
        active.into_iter().chain(retry).min()
    }

    pub(super) fn cancel_output_queue(&mut self, name: &str) {
        let targets: Vec<_> = self
            .pending_output_queues
            .tasks
            .iter()
            .filter(|(_, pending)| pending.owner_name == name)
            .map(|(target, _)| target.clone())
            .collect();
        let abandoned: Vec<_> = self
            .pending_output_queues
            .retries
            .keys()
            .filter(|target| self.queue_owner(target).is_none())
            .cloned()
            .collect();
        for target in abandoned {
            self.pending_output_queues.retries.remove(&target);
        }
        for target in targets {
            let mut pending = self
                .pending_output_queues
                .tasks
                .remove(&target)
                .expect("pending queue");
            let Some(owner) = self.queue_owner(&target) else {
                pending.task.abort();
                continue;
            };
            if Self::queue_config_changed(&pending.config, &owner.config) {
                pending.task.abort();
                self.start_output_queue(target);
            } else {
                // Identical queue work survives unplug, but validation must use a live owner.
                pending.owner_name = owner._name.clone();
                pending.owner_description = owner.description.clone();
                pending.config = owner.config.clone();
                self.pending_output_queues.tasks.insert(target, pending);
            }
        }
    }
}

#[cfg(test)]
#[path = "deferred_output_tests.rs"]
mod tests;
