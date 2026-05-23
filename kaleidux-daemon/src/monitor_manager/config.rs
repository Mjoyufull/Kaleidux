use super::MonitorManager;
use crate::cache::FileCache;
use crate::metrics::PerformanceMetrics;
use crate::orchestration::{Config, MonitorBehavior, OutputConfig};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, warn};

impl MonitorManager {
    pub(super) fn resolve_content_type(
        path: &Path,
        context: &str,
    ) -> Option<crate::queue::ContentType> {
        match crate::queue::SmartQueue::get_content_type(path) {
            Some(content_type) => Some(content_type),
            None => {
                warn!(
                    "[PICK] {}: Skipping path with unknown content type: {}",
                    context,
                    path.display()
                );
                None
            }
        }
    }

    fn updated_config_for<'a>(
        updated_configs: &'a HashMap<String, OutputConfig>,
        name: &str,
        context: &str,
    ) -> Option<&'a OutputConfig> {
        match updated_configs.get(name) {
            Some(config) => Some(config),
            None => {
                error!(
                    "[CONFIG] Missing refreshed output config for {} while {}",
                    name, context
                );
                None
            }
        }
    }

    fn flush_all_queue_stats(&mut self) {
        for (name, orch) in &mut self.outputs {
            if let Some(queue) = &mut orch.queue {
                Self::flush_queue_stats(queue, name);
            }
        }
        if let Some(queue) = &mut self.shared_queue {
            Self::flush_queue_stats(queue, "synchronized queue");
        }
        for (gid, queue) in &mut self.group_queues {
            Self::flush_queue_stats(queue, &format!("group queue {}", gid));
        }
    }

    fn rebuild_all_queues_for_behavior_change(
        &mut self,
        updated_configs: &HashMap<String, OutputConfig>,
    ) {
        let cache = self.cache.clone();
        let metrics = self.metrics.clone();
        let mut output_names: Vec<String> = updated_configs.keys().cloned().collect();
        output_names.sort();

        self.flush_all_queue_stats();
        self.shared_queue = None;
        self.group_queues.clear();
        self.output_groups.clear();
        self.shared_display_start_time = None;
        self.group_display_start_times.clear();

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                for name in &output_names {
                    if let Some(orch) = self.outputs.get_mut(name) {
                        let Some(output_config) =
                            Self::updated_config_for(updated_configs, name, "rebuilding queues")
                        else {
                            continue;
                        };
                        orch.queue = Self::build_refreshed_queue(
                            cache.clone(),
                            metrics.clone(),
                            name,
                            output_config,
                        );
                        Self::reset_output_after_queue_refresh(orch);
                        tracing::info!(
                            "[CONFIG] {}: Monitor behavior changed to independent; queue rebuilt",
                            name
                        );
                    }
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(representative_name) = output_names.first() {
                    if let Some(representative_config) = Self::updated_config_for(
                        updated_configs,
                        representative_name,
                        "rebuilding synchronized queue",
                    ) {
                        self.shared_queue = Self::build_refreshed_queue(
                            cache.clone(),
                            metrics.clone(),
                            representative_name,
                            representative_config,
                        );
                    }
                }

                for name in &output_names {
                    if let Some(orch) = self.outputs.get_mut(name) {
                        orch.queue = None;
                        Self::reset_output_after_queue_refresh(orch);
                        tracing::info!(
                            "[CONFIG] {}: Monitor behavior changed to synchronized; shared queue rebuilt",
                            name
                        );
                    }
                }
            }
            MonitorBehavior::Grouped(groups) => {
                for name in &output_names {
                    if let Some(gid) = groups
                        .iter()
                        .position(|group| group.iter().any(|member| member == name))
                    {
                        self.output_groups.insert(name.clone(), gid);
                    }
                }

                for (gid, group) in groups.iter().enumerate() {
                    let Some(representative_name) = group
                        .iter()
                        .find(|name| updated_configs.contains_key(*name))
                    else {
                        continue;
                    };
                    let Some(representative_config) = Self::updated_config_for(
                        updated_configs,
                        representative_name,
                        "rebuilding grouped queue",
                    ) else {
                        continue;
                    };
                    if let Some(queue) = Self::build_refreshed_queue(
                        cache.clone(),
                        metrics.clone(),
                        representative_name,
                        representative_config,
                    ) {
                        self.group_queues.insert(gid, queue);
                    }
                }

                for name in &output_names {
                    if let Some(orch) = self.outputs.get_mut(name) {
                        if self.output_groups.contains_key(name) {
                            orch.queue = None;
                            Self::reset_output_after_queue_refresh(orch);
                            tracing::info!(
                                "[CONFIG] {}: Monitor behavior changed to grouped; group queue rebuilt",
                                name
                            );
                        } else {
                            let Some(output_config) = Self::updated_config_for(
                                updated_configs,
                                name,
                                "rebuilding independent queue after grouping change",
                            ) else {
                                continue;
                            };
                            orch.queue = Self::build_refreshed_queue(
                                cache.clone(),
                                metrics.clone(),
                                name,
                                output_config,
                            );
                            Self::reset_output_after_queue_refresh(orch);
                            tracing::info!(
                                "[CONFIG] {}: Monitor behavior changed to grouped; output remains independent",
                                name
                            );
                        }
                    }
                }
            }
        }
    }

    fn first_changed_name(
        changed_names: &[String],
        predicate: impl Fn(&str) -> bool,
    ) -> Option<&str> {
        changed_names
            .iter()
            .find(|name| predicate(name))
            .map(String::as_str)
    }

    pub(super) fn earlier_deadline(current: &mut Option<Instant>, candidate: Option<Instant>) {
        if let Some(candidate) = candidate {
            match current {
                Some(current_deadline) if *current_deadline <= candidate => {}
                _ => *current = Some(candidate),
            }
        }
    }

    #[allow(dead_code)]
    pub fn new(config: Config) -> Result<Self> {
        Self::new_with_metrics(config, None)
    }

    pub fn get_cache(&self) -> Arc<FileCache> {
        self.cache.clone()
    }

    pub fn new_with_metrics(
        config: Config,
        metrics: Option<Arc<PerformanceMetrics>>,
    ) -> Result<Self> {
        // Create shared cache instance once for all queues
        let cache = Arc::new(FileCache::new()?);

        Ok(Self {
            config,
            outputs: HashMap::new(),
            shared_queue: None,
            group_queues: HashMap::new(),
            output_groups: HashMap::new(),
            shared_display_start_time: None,
            group_display_start_times: HashMap::new(),
            cache,
            metrics,
            paused: false,
            discovered_files_cache: HashMap::new(),
        })
    }

    #[allow(dead_code)]
    pub fn update_config(&mut self, config: Config) {
        let previous_behavior = self.config.global.monitor_behavior.clone();
        self.config = config;
        let cache = self.cache.clone();
        let metrics = self.metrics.clone();
        let mut updated_configs = HashMap::new();
        let mut changed_names = Vec::new();
        let behavior_changed = previous_behavior != self.config.global.monitor_behavior;

        for (name, orch) in &self.outputs {
            let output_config = self.config.get_config_for_output(name, &orch.description);
            if Self::queue_config_changed(&orch.config, &output_config) {
                changed_names.push(name.clone());
            }
            updated_configs.insert(name.clone(), output_config);
        }
        changed_names.sort();

        if behavior_changed {
            self.rebuild_all_queues_for_behavior_change(&updated_configs);
            for (name, orch) in &mut self.outputs {
                if let Some(output_config) = updated_configs.remove(name) {
                    orch.apply_config(output_config);
                } else {
                    error!(
                        "[CONFIG] Missing refreshed output config for {} after behavior change",
                        name
                    );
                }
            }
            return;
        }

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                for name in &changed_names {
                    if let Some(orch) = self.outputs.get_mut(name) {
                        let Some(output_config) = Self::updated_config_for(
                            &updated_configs,
                            name,
                            "refreshing independent queue",
                        ) else {
                            continue;
                        };
                        let path_changed = orch.config.path != output_config.path;
                        if let Some(queue) = &mut orch.queue {
                            Self::flush_queue_stats(queue, name);
                        }
                        orch.queue = Self::build_refreshed_queue(
                            cache.clone(),
                            metrics.clone(),
                            name,
                            output_config,
                        );
                        Self::reset_output_after_queue_refresh(orch);
                        tracing::info!(
                            "[CONFIG] {}: {} config changed, queue refreshed",
                            name,
                            if path_changed {
                                "Path"
                            } else {
                                "Queue-affecting"
                            }
                        );
                    }
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(representative_name) = changed_names.first().map(String::as_str) {
                    if let Some(queue) = &mut self.shared_queue {
                        Self::flush_queue_stats(queue, "synchronized queue");
                    }
                    if let Some(representative_config) = Self::updated_config_for(
                        &updated_configs,
                        representative_name,
                        "refreshing synchronized queue",
                    ) {
                        self.shared_queue = Self::build_refreshed_queue(
                            cache.clone(),
                            metrics.clone(),
                            representative_name,
                            representative_config,
                        );
                    }
                    self.shared_display_start_time = None;
                    for (name, orch) in &mut self.outputs {
                        orch.queue = None;
                        Self::reset_output_after_queue_refresh(orch);
                        tracing::info!(
                            "[CONFIG] {}: Shared queue config changed, queue refreshed",
                            name
                        );
                    }
                }
            }
            MonitorBehavior::Grouped(_) => {
                let mut changed_group_ids: Vec<usize> = self
                    .output_groups
                    .iter()
                    .filter_map(|(name, gid)| changed_names.binary_search(name).ok().map(|_| *gid))
                    .collect();
                changed_group_ids.sort_unstable();
                changed_group_ids.dedup();

                for gid in changed_group_ids {
                    let Some(representative_name) =
                        Self::first_changed_name(&changed_names, |name| {
                            self.output_groups.get(name).copied() == Some(gid)
                        })
                    else {
                        continue;
                    };
                    if let Some(queue) = self.group_queues.get_mut(&gid) {
                        Self::flush_queue_stats(queue, &format!("group queue {}", gid));
                    }
                    let Some(representative_config) = Self::updated_config_for(
                        &updated_configs,
                        representative_name,
                        "refreshing grouped queue",
                    ) else {
                        continue;
                    };
                    let refreshed_queue = Self::build_refreshed_queue(
                        cache.clone(),
                        metrics.clone(),
                        representative_name,
                        representative_config,
                    );
                    match refreshed_queue {
                        Some(queue) => {
                            self.group_queues.insert(gid, queue);
                        }
                        None => {
                            self.group_queues.remove(&gid);
                        }
                    }
                    self.group_display_start_times.remove(&gid);
                    for (name, orch_gid) in &self.output_groups {
                        if *orch_gid == gid {
                            if let Some(orch) = self.outputs.get_mut(name) {
                                orch.queue = None;
                                Self::reset_output_after_queue_refresh(orch);
                                tracing::info!(
                                    "[CONFIG] {}: Group queue config changed, queue refreshed",
                                    name
                                );
                            }
                        }
                    }
                }

                for name in &changed_names {
                    if self.output_groups.contains_key(name) {
                        continue;
                    }
                    if let Some(orch) = self.outputs.get_mut(name) {
                        let Some(output_config) = Self::updated_config_for(
                            &updated_configs,
                            name,
                            "refreshing ungrouped queue in grouped mode",
                        ) else {
                            continue;
                        };
                        let path_changed = orch.config.path != output_config.path;
                        if let Some(queue) = &mut orch.queue {
                            Self::flush_queue_stats(queue, name);
                        }
                        orch.queue = Self::build_refreshed_queue(
                            cache.clone(),
                            metrics.clone(),
                            name,
                            output_config,
                        );
                        Self::reset_output_after_queue_refresh(orch);
                        tracing::info!(
                            "[CONFIG] {}: {} config changed, queue refreshed",
                            name,
                            if path_changed {
                                "Path"
                            } else {
                                "Queue-affecting"
                            }
                        );
                    }
                }
            }
        }

        for (name, orch) in &mut self.outputs {
            let output_config = updated_configs
                .remove(name)
                .expect("updated config should exist for every output");
            orch.apply_config(output_config);
        }
    }
}
