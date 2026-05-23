use super::{MonitorManager, OutputOrchestrator, independent_phase_offset};
use crate::orchestration::MonitorBehavior;
use crate::queue::SmartQueue;
use tracing::info;

impl MonitorManager {
    pub async fn add_output(&mut self, name: &str, description: &str) {
        let output_config = self.config.get_config_for_output(name, description);
        info!(
            "[ADD_OUTPUT] {}: path={:?}, behavior={:?}",
            name, output_config.path, self.config.global.monitor_behavior
        );

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                info!("[ADD_OUTPUT] {}: Creating independent queue", name);
                // Check if we already discovered files for this path (avoids re-scanning)
                let cached_path = output_config.path.clone();
                let orch = if let Some(path) = &cached_path {
                    if let Some(cached_files) = self.discovered_files_cache.get(path) {
                        info!(
                            "[ADD_OUTPUT] {}: Reusing cached file list ({} files) for {:?}",
                            name,
                            cached_files.len(),
                            path
                        );
                        let queue = SmartQueue::new_from_pool(
                            path,
                            cached_files.clone(),
                            output_config.video_ratio,
                            output_config.sorting,
                            self.cache.clone(),
                        )
                        .ok()
                        .map(|mut q| {
                            if let Some(pl_name) = &output_config.default_playlist {
                                let _ = q.set_playlist(Some(pl_name.clone()));
                            }
                            q
                        });
                        OutputOrchestrator {
                            _name: name.to_string(),
                            description: description.to_string(),
                            phase_offset: independent_phase_offset(name, output_config.duration),
                            config: output_config,
                            queue,
                            current_path: None,
                            next_path: None,
                            next_content_type: None,
                            next_change: None,
                            display_start_time: None,
                        }
                    } else {
                        let orch = OutputOrchestrator::new(
                            name.to_string(),
                            description.to_string(),
                            output_config,
                            self.cache.clone(),
                            self.metrics.clone(),
                        )
                        .await;
                        // Cache the discovered file list for subsequent outputs with the same path
                        if let Some(q) = &orch.queue {
                            self.discovered_files_cache
                                .insert(path.clone(), q.pool.clone());
                        }
                        orch
                    }
                } else {
                    OutputOrchestrator::new(
                        name.to_string(),
                        description.to_string(),
                        output_config,
                        self.cache.clone(),
                        self.metrics.clone(),
                    )
                    .await
                };
                info!(
                    "[ADD_OUTPUT] {}: Queue created: {}",
                    name,
                    orch.queue.is_some()
                );
                self.outputs.insert(name.to_string(), orch);
            }
            MonitorBehavior::Synchronized => {
                if self.shared_queue.is_none() {
                    if let Some(path) = &output_config.path {
                        if let Ok(mut q) = SmartQueue::new_with_cache(
                            path,
                            output_config.video_ratio,
                            output_config.sorting,
                            self.cache.clone(),
                            self.metrics.clone(),
                        )
                        .await
                        {
                            if let Some(pl_name) = &output_config.default_playlist {
                                let _ = q.set_playlist(Some(pl_name.clone()));
                            }
                            self.shared_queue = Some(q);
                        }
                    }
                }
                let mut orch = OutputOrchestrator::new(
                    name.to_string(),
                    description.to_string(),
                    output_config,
                    self.cache.clone(),
                    self.metrics.clone(),
                )
                .await;
                orch.queue = None; // Will use shared queue
                self.outputs.insert(name.to_string(), orch);
            }
            MonitorBehavior::Grouped(groups) => {
                // Find which group this output belongs to
                let mut group_id: Option<usize> = None;
                for (idx, group) in groups.iter().enumerate() {
                    if group.contains(&name.to_string()) {
                        group_id = Some(idx);
                        break;
                    }
                }

                if let Some(gid) = group_id {
                    self.output_groups.insert(name.to_string(), gid);

                    // Initialize group queue if needed
                    if !self.group_queues.contains_key(&gid) {
                        if let Some(path) = &output_config.path {
                            if let Ok(mut q) = SmartQueue::new_with_cache(
                                path,
                                output_config.video_ratio,
                                output_config.sorting,
                                self.cache.clone(),
                                self.metrics.clone(),
                            )
                            .await
                            {
                                if let Some(pl_name) = &output_config.default_playlist {
                                    let _ = q.set_playlist(Some(pl_name.clone()));
                                }
                                self.group_queues.insert(gid, q);
                            }
                        }
                    }

                    let mut orch = OutputOrchestrator::new(
                        name.to_string(),
                        description.to_string(),
                        output_config,
                        self.cache.clone(),
                        self.metrics.clone(),
                    )
                    .await;
                    orch.queue = None; // Will use group queue
                    self.outputs.insert(name.to_string(), orch);
                } else {
                    // Output not in any group, treat as independent
                    info!("Output {} not in any group, treating as independent", name);
                    let orch = OutputOrchestrator::new(
                        name.to_string(),
                        description.to_string(),
                        output_config,
                        self.cache.clone(),
                        self.metrics.clone(),
                    )
                    .await;
                    self.outputs.insert(name.to_string(), orch);
                }
            }
        }
    }
}
