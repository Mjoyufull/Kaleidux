use crate::cache::FileCache;
use crate::metrics::PerformanceMetrics;
use crate::orchestration::{Config, MonitorBehavior, OutputConfig};
use crate::queue::Playlist;
use crate::queue::SmartQueue;
use anyhow::Result;
use kaleidux_common::{BlacklistCommand, KEntry, PlaylistCommand, Response};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

pub struct OutputOrchestrator {
    pub _name: String,
    #[allow(dead_code)]
    pub description: String,
    pub config: OutputConfig,
    pub queue: Option<SmartQueue>,
    pub current_path: Option<PathBuf>,
    #[allow(dead_code)]
    pub next_path: Option<PathBuf>, // Pre-buffered next content path
    #[allow(dead_code)]
    pub next_content_type: Option<crate::queue::ContentType>, // Type of next content
    pub next_change: Option<Instant>,
    pub display_start_time: Option<Instant>, // When content actually started displaying
}

impl OutputOrchestrator {
    pub async fn new(
        name: String,
        description: String,
        config: OutputConfig,
        cache: Arc<FileCache>,
        metrics: Option<Arc<PerformanceMetrics>>,
    ) -> Self {
        let queue = if let Some(path) = &config.path {
            info!("[QUEUE] {}: Initializing queue for path: {:?}", name, path);
            match SmartQueue::new_with_cache(
                path,
                config.video_ratio,
                config.sorting,
                cache,
                metrics.clone(),
            )
            .await
            {
                Ok(mut q) => {
                    info!("[QUEUE] {}: Queue initialized successfully", name);
                    if let Some(pl_name) = &config.default_playlist {
                        if let Err(e) = q.set_playlist(Some(pl_name.clone())) {
                            error!(
                                "Failed to set default playlist '{}' for {}: {}",
                                pl_name, name, e
                            );
                        }
                    }
                    Some(q)
                }
                Err(e) => {
                    error!("[QUEUE] {}: Failed to initialize queue: {}", name, e);
                    None
                }
            }
        } else {
            warn!("[QUEUE] {}: No path configured, queue will be None", name);
            None
        };

        Self {
            _name: name,
            description,
            config,
            queue,
            current_path: None,
            next_path: None,
            next_content_type: None,
            next_change: None,
            display_start_time: None,
        }
    }

    pub fn tick(&mut self) -> Option<(PathBuf, crate::queue::ContentType)> {
        let now = Instant::now();

        // If content is displaying, check if duration has elapsed based on actual display start time
        if let Some(display_start) = self.display_start_time {
            let elapsed = now.saturating_duration_since(display_start);
            if elapsed >= self.config.duration {
                debug!(
                    "Duration expired for {}: {} elapsed (target: {:?})",
                    self._name,
                    format!("{:.2}s", elapsed.as_secs_f64()),
                    self.config.duration
                );
                let result = self.pick_next();
                return result;
            }
        } else if let Some(next) = self.next_change {
            // Fallback: if display_start_time not set yet, use scheduled time
            // This handles the case where content hasn't loaded yet
            if now >= next {
                debug!(
                    "Timer expired for {}: Switching now (next was {:?})",
                    self._name, next
                );
                let result = self.pick_next();
                return result;
            }
        } else if self.current_path.is_none() {
            if self.queue.is_none() {
                warn!("[TICK] {}: Queue is None, cannot pick content", self._name);
                return None;
            }
            info!(
                "[TICK] {}: Initial tick - picking first content (queue exists)",
                self._name
            );
            let result = self.pick_next();
            return result;
        }
        None
    }

    pub fn pick_next(&mut self) -> Option<(PathBuf, crate::queue::ContentType)> {
        if let Some(queue) = &mut self.queue {
            info!("[PICK] {}: Calling queue.pick_next()", self._name);
            if let Some(path) = queue.pick_next() {
                info!("[PICK] {}: Selected path: {:?}", self._name, path);
                let content_type = crate::queue::SmartQueue::get_content_type(&path).unwrap(); // Already validated in discovery
                self.current_path = Some(path.clone());
                // Reset display start time - will be set when content actually starts displaying
                // Reset display start time - will be set when content actually starts displaying
                self.display_start_time = None;
                // Set next_change as fallback (in case content never loads)
                self.next_change =
                    Some(Instant::now() + self.config.duration + std::time::Duration::from_secs(5)); // Add 5s buffer for loading

                // Pre-buffer next content
                if let Some((next_p, next_t)) = self.peek_next() {
                    self.next_path = Some(next_p);
                    self.next_content_type = Some(next_t);
                } else {
                    self.next_path = None;
                    self.next_content_type = None;
                }

                debug!(
                    "Scheduled next change for {} in {:?} (path: {})",
                    self._name,
                    self.config.duration,
                    path.display()
                );
                return Some((path, content_type));
            }
        }
        None
    }

    /// Get the next content path without consuming it (for pre-buffering)
    pub fn peek_next(&self) -> Option<(PathBuf, crate::queue::ContentType)> {
        if let Some(queue) = &self.queue {
            return queue.peek_next();
        }
        None
    }

    /// Mark that transition has completed and content is now displaying (called when transition progress >= 1.0)
    pub fn mark_transition_completed(&mut self) {
        if self.display_start_time.is_none() && self.current_path.is_some() {
            self.display_start_time = Some(Instant::now());
            debug!("Transition completed for {} - duration timer now active (2s of content display starts now)", self._name);
        }
    }

    pub fn pick_prev(&mut self) -> Option<(PathBuf, crate::queue::ContentType)> {
        if let Some(queue) = &mut self.queue {
            if let Some(path) = queue.pick_prev() {
                let content_type = crate::queue::SmartQueue::get_content_type(&path).unwrap();
                self.current_path = Some(path.clone());
                // Reset display start time - will be set when content actually starts displaying
                self.display_start_time = None;
                // Set next_change as fallback (in case content never loads)
                self.next_change =
                    Some(Instant::now() + self.config.duration + std::time::Duration::from_secs(5)); // Add 5s buffer for loading
                return Some((path, content_type));
            }
        }
        None
    }
}

pub struct MonitorManager {
    config: Config,
    pub outputs: HashMap<String, OutputOrchestrator>,
    shared_queue: Option<SmartQueue>,
    group_queues: HashMap<usize, SmartQueue>, // Per-group queues
    output_groups: HashMap<String, usize>,    // output_name -> group_id
    shared_display_start_time: Option<Instant>, // For synchronized outputs - shared display start time
    group_display_start_times: HashMap<usize, Instant>, // For grouped outputs - per-group display start time
    cache: Arc<FileCache>,                              // Shared cache instance for all queues
    metrics: Option<Arc<PerformanceMetrics>>,           // Shared metrics instance
}

impl MonitorManager {
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
        })
    }

    #[allow(dead_code)]
    pub fn update_config(&mut self, config: Config) {
        self.config = config;

        // Refresh all output configurations
        for (name, orch) in &mut self.outputs {
            // Re-match config for this output using its stored description
            let output_config = self.config.get_config_for_output(name, &orch.description);
            orch.config = output_config;

            // TODO: Full queue refresh if path changes.
        }
    }

    pub async fn add_output(&mut self, name: &str, description: &str) {
        let output_config = self.config.get_config_for_output(name, description);
        info!(
            "[ADD_OUTPUT] {}: path={:?}, behavior={:?}",
            name, output_config.path, self.config.global.monitor_behavior
        );

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                info!("[ADD_OUTPUT] {}: Creating independent queue", name);
                let orch = OutputOrchestrator::new(
                    name.to_string(),
                    description.to_string(),
                    output_config,
                    self.cache.clone(),
                    self.metrics.clone(),
                )
                .await;
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

    pub fn tick(&mut self) -> HashMap<String, (PathBuf, crate::queue::ContentType)> {
        let mut changes = HashMap::new();
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
                            let content_type =
                                crate::queue::SmartQueue::get_content_type(&path).unwrap();

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
                                orch.next_change = Some(
                                    now + orch.config.duration + std::time::Duration::from_secs(5),
                                );

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
                                let content_type =
                                    crate::queue::SmartQueue::get_content_type(&path).unwrap();

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
                                        orch.next_change = Some(
                                            now + orch.config.duration
                                                + std::time::Duration::from_secs(5),
                                        );

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
                        let content_type =
                            crate::queue::SmartQueue::get_content_type(&path).unwrap();
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
                            orch.next_change = Some(
                                now + orch.config.duration + std::time::Duration::from_secs(5),
                            );

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
                                let content_type =
                                    crate::queue::SmartQueue::get_content_type(&path).unwrap();

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
                                                    + orch.config.duration
                                                    + std::time::Duration::from_secs(5),
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
                                    let content_type =
                                        crate::queue::SmartQueue::get_content_type(&path).unwrap();
                                    // Reset group display start time for next cycle
                                    self.group_display_start_times.remove(gid);
                                    for (n, og) in &self.output_groups {
                                        if og == gid {
                                            if let Some(orch) = self.outputs.get_mut(n) {
                                                orch.current_path = Some(path.clone());
                                                orch.display_start_time = None;
                                                orch.next_change = Some(
                                                    Instant::now()
                                                        + orch.config.duration
                                                        + std::time::Duration::from_secs(5),
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
                        let content_type =
                            crate::queue::SmartQueue::get_content_type(&path).unwrap();
                        let now = Instant::now();
                        // Reset shared display start time for next cycle
                        self.shared_display_start_time = None;
                        for (name, orch) in &mut self.outputs {
                            orch.current_path = Some(path.clone());
                            orch.display_start_time = None;
                            orch.next_change = Some(
                                now + orch.config.duration + std::time::Duration::from_secs(5),
                            );
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
                                let content_type =
                                    crate::queue::SmartQueue::get_content_type(&path).unwrap();
                                // Reset group display start time for next cycle
                                self.group_display_start_times.remove(&gid);
                                for (name, og) in &self.output_groups {
                                    if og == &gid {
                                        if let Some(orch) = self.outputs.get_mut(name) {
                                            orch.current_path = Some(path.clone());
                                            orch.display_start_time = None;
                                            orch.next_change = Some(
                                                Instant::now()
                                                    + orch.config.duration
                                                    + std::time::Duration::from_secs(5),
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
                                    let content_type =
                                        crate::queue::SmartQueue::get_content_type(&path).unwrap();
                                    // Reset group display start time for next cycle
                                    self.group_display_start_times.remove(gid);
                                    for (n, og) in &self.output_groups {
                                        if og == gid {
                                            if let Some(orch) = self.outputs.get_mut(n) {
                                                orch.current_path = Some(path.clone());
                                                orch.display_start_time = None;
                                                orch.next_change = Some(
                                                    Instant::now()
                                                        + orch.config.duration
                                                        + std::time::Duration::from_secs(5),
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

    pub fn love_file(&mut self, path: String, multiplier: f32) -> Result<()> {
        let path = PathBuf::from(path);
        if let Some(queue) = &mut self.shared_queue {
            queue.love_file(path.clone(), multiplier)?;
        }
        for queue in self.group_queues.values_mut() {
            queue.love_file(path.clone(), multiplier)?;
        }
        for orch in self.outputs.values_mut() {
            if let Some(queue) = &mut orch.queue {
                queue.love_file(path.clone(), multiplier)?;
            }
        }
        Ok(())
    }

    pub fn unlove_file(&mut self, path: String) -> Result<()> {
        self.love_file(path, 1.0)
    }

    pub fn get_loveitlist(&self) -> Vec<KEntry> {
        let mut list = HashMap::new();
        // Consolidate from all queues
        if let Some(queue) = &self.shared_queue {
            for (path, stats) in &queue.stats.files {
                if stats.love_multiplier > 1.0 {
                    list.insert(
                        path.to_string_lossy().to_string(),
                        (stats.love_multiplier, stats.count),
                    );
                }
            }
        }
        for queue in self.group_queues.values() {
            for (path, stats) in &queue.stats.files {
                if stats.love_multiplier > 1.0 {
                    list.insert(
                        path.to_string_lossy().to_string(),
                        (stats.love_multiplier, stats.count),
                    );
                }
            }
        }
        for orch in self.outputs.values() {
            if let Some(queue) = &orch.queue {
                for (path, stats) in &queue.stats.files {
                    if stats.love_multiplier > 1.0 {
                        list.insert(
                            path.to_string_lossy().to_string(),
                            (stats.love_multiplier, stats.count),
                        );
                    }
                }
            }
        }
        list.into_iter()
            .map(|(path, (multiplier, count))| KEntry {
                path,
                multiplier,
                count,
            })
            .collect()
    }

    pub fn get_output_config(&self, name: &str) -> Option<&OutputConfig> {
        self.outputs.get(name).map(|o| &o.config)
    }

    /// Mark that transition has completed for an output (called when transition progress >= 1.0)
    /// For synchronized mode, uses shared display start time (first output to complete)
    /// For grouped mode, uses group display start time (first output in group to complete)
    /// For independent mode, each output has its own display start time
    pub fn mark_transition_completed(&mut self, name: &str) {
        let now = Instant::now();

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Synchronized => {
                // For synchronized mode, use shared display start time
                // Set it when first output completes transition
                if self.shared_display_start_time.is_none() {
                    self.shared_display_start_time = Some(now);
                    debug!("Synchronized mode: First output ({}) completed transition - shared display timer started", name);
                }
                // All synchronized outputs use the shared time
                if let Some(orch) = self.outputs.get_mut(name) {
                    orch.display_start_time = self.shared_display_start_time;
                }
            }
            MonitorBehavior::Grouped(_) => {
                // For grouped mode, use per-group display start time
                if let Some(gid) = self.output_groups.get(name) {
                    // Set group time when first output in group completes transition
                    if !self.group_display_start_times.contains_key(gid) {
                        self.group_display_start_times.insert(*gid, now);
                        debug!("Group {}: First output ({}) completed transition - group display timer started", gid, name);
                    }
                    // All outputs in group use the group time
                    if let Some(orch) = self.outputs.get_mut(name) {
                        orch.display_start_time = self.group_display_start_times.get(gid).copied();
                    }
                } else {
                    // Not in a group, treat as independent
                    if let Some(orch) = self.outputs.get_mut(name) {
                        orch.mark_transition_completed();
                    }
                }
            }
            MonitorBehavior::Independent => {
                // For independent mode, each output has its own timer
                if let Some(orch) = self.outputs.get_mut(name) {
                    orch.mark_transition_completed();
                }
            }
        }
    }

    pub fn handle_playlist_command(&mut self, cmd: PlaylistCommand) -> Response {
        match cmd {
            PlaylistCommand::Create { name } => {
                let playlist = Playlist {
                    paths: Vec::new(),
                    strategy: crate::orchestration::SortingStrategy::Loveit,
                    enabled: true,
                };
                self.apply_to_all_queues(|q| {
                    q.stats.playlists.insert(name.clone(), playlist.clone());
                    q.save_stats()
                });
                Response::Ok
            }
            PlaylistCommand::Delete { name } => {
                self.apply_to_all_queues(|q| {
                    q.stats.playlists.remove(&name);
                    q.save_stats()
                });
                Response::Ok
            }
            PlaylistCommand::Add { name, path } => {
                let path_buf = PathBuf::from(path);
                self.apply_to_all_queues(|q| {
                    if let Some(pl) = q.stats.playlists.get_mut(&name) {
                        if !pl.paths.contains(&path_buf) {
                            pl.paths.push(path_buf.clone());
                        }
                    }
                    q.save_stats()
                });
                Response::Ok
            }
            PlaylistCommand::Remove { name, path } => {
                let path_buf = PathBuf::from(path);
                self.apply_to_all_queues(|q| {
                    if let Some(pl) = q.stats.playlists.get_mut(&name) {
                        pl.paths.retain(|p| p != &path_buf);
                    }
                    q.save_stats()
                });
                Response::Ok
            }
            PlaylistCommand::Load { name } => {
                let mut error = None;
                self.apply_to_all_queues(|q| {
                    if let Err(e) = q.set_playlist(name.clone()) {
                        error = Some(e.to_string());
                        let _ = q.save_stats(); // Save active playlist state? SmartQueue doesn't persist active_playlist yet
                        Err(e)
                    } else {
                        q.save_stats()
                    }
                });
                if let Some(e) = error {
                    Response::Error(e)
                } else {
                    Response::Ok
                }
            }
            PlaylistCommand::List => {
                // Get from first available queue
                if let Some(q) = self.get_any_queue() {
                    let names: Vec<String> = q.stats.playlists.keys().cloned().collect();
                    Response::Playlists(names)
                } else {
                    Response::Playlists(Vec::new())
                }
            }
        }
    }

    pub fn handle_blacklist_command(&mut self, cmd: BlacklistCommand) -> Response {
        match cmd {
            BlacklistCommand::Add { path } => {
                let path_buf = PathBuf::from(path);
                self.apply_to_all_queues(|q| q.blacklist_file(path_buf.clone()));
                Response::Ok
            }
            BlacklistCommand::Remove { path } => {
                let path_buf = PathBuf::from(path);
                self.apply_to_all_queues(|q| q.unblacklist_file(path_buf.clone()));
                Response::Ok
            }
            BlacklistCommand::List => {
                if let Some(q) = self.get_any_queue() {
                    let paths: Vec<String> = q
                        .stats
                        .blacklist
                        .iter()
                        .map(|p| p.to_string_lossy().to_string())
                        .collect();
                    Response::Blacklist(paths)
                } else {
                    Response::Blacklist(Vec::new())
                }
            }
        }
    }

    fn apply_to_all_queues<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut SmartQueue) -> Result<()>,
    {
        if let Some(q) = &mut self.shared_queue {
            let _ = f(q);
        }
        for q in self.group_queues.values_mut() {
            let _ = f(q);
        }
        for orch in self.outputs.values_mut() {
            if let Some(q) = &mut orch.queue {
                let _ = f(q);
            }
        }
    }

    /// Flush pending stats updates from all queues (batched write)
    pub fn flush_all_stats(&mut self) -> Result<()> {
        if let Some(q) = &mut self.shared_queue {
            let _ = q.flush_stats();
        }
        for q in self.group_queues.values_mut() {
            let _ = q.flush_stats();
        }
        for orch in self.outputs.values_mut() {
            if let Some(q) = &mut orch.queue {
                let _ = q.flush_stats();
            }
        }
        Ok(())
    }

    fn get_any_queue(&self) -> Option<&SmartQueue> {
        if let Some(q) = &self.shared_queue {
            return Some(q);
        }
        if let Some(q) = self.group_queues.values().next() {
            return Some(q);
        }
        for orch in self.outputs.values() {
            if let Some(q) = &orch.queue {
                return Some(q);
            }
        }
        None
    }

    pub fn get_history(&self, output_name: Option<String>) -> Vec<String> {
        let history = Vec::new();
        let to_strings = |paths: &[PathBuf]| -> Vec<String> {
            paths
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect()
        };

        if let Some(name) = output_name {
            // Specific output requested
            if let Some(gid) = self.output_groups.get(&name) {
                if let Some(q) = self.group_queues.get(gid) {
                    return to_strings(&q.history);
                }
            }
            if let Some(orch) = self.outputs.get(&name) {
                if let Some(q) = &orch.queue {
                    return to_strings(&q.history);
                }
                // If orch exists but no queue (synchronized?), check shared
                if self.shared_queue.is_some() {
                    if let Some(q) = &self.shared_queue {
                        return to_strings(&q.history);
                    }
                }
            }
        } else {
            // General request
            if let Some(q) = &self.shared_queue {
                return to_strings(&q.history);
            }
            // Try to find a group queue
            if let Some(q) = self.group_queues.values().next() {
                return to_strings(&q.history);
            }
            // Try to find any independent queue
            for orch in self.outputs.values() {
                if let Some(q) = &orch.queue {
                    return to_strings(&q.history);
                }
            }
        }
        history
    }
}
