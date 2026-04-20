use crate::cache::FileCache;
use crate::metrics::PerformanceMetrics;
use crate::orchestration::{Config, MonitorBehavior, OutputConfig};
use crate::queue::Playlist;
use crate::queue::SmartQueue;
use anyhow::Result;
use kaleidux_common::{BlacklistCommand, KEntry, PlaylistCommand, Response};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

const MIN_CONTENT_LOAD_TIMEOUT: Duration = Duration::from_secs(15);
const CONTENT_LOAD_GRACE: Duration = Duration::from_secs(5);

fn content_load_timeout(display_duration: Duration) -> Duration {
    (display_duration + CONTENT_LOAD_GRACE).max(MIN_CONTENT_LOAD_TIMEOUT)
}

fn stable_output_hash(name: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn independent_phase_offset(name: &str, base_duration: Duration) -> Duration {
    let cap = (base_duration / 12).min(Duration::from_millis(120));
    if cap < Duration::from_millis(8) {
        return Duration::ZERO;
    }

    let cap_nanos = cap.as_nanos();
    let offset_nanos = u128::from(stable_output_hash(name)) % (cap_nanos + 1);
    Duration::from_nanos(offset_nanos as u64)
}

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
    pub phase_offset: Duration,
}

impl OutputOrchestrator {
    fn cycle_duration(&self) -> Duration {
        self.config.duration.saturating_add(self.phase_offset)
    }

    fn apply_config(&mut self, config: OutputConfig) {
        self.phase_offset = independent_phase_offset(&self._name, config.duration);
        self.config = config;
    }

    fn next_deadline(&self) -> Option<Instant> {
        if let Some(display_start) = self.display_start_time {
            Some(display_start + self.cycle_duration())
        } else {
            self.next_change
        }
    }

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

        let phase_offset = independent_phase_offset(&name, config.duration);

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
            phase_offset,
        }
    }

    pub fn tick(&mut self) -> Option<(PathBuf, crate::queue::ContentType)> {
        let now = Instant::now();

        // If content is displaying, check if duration has elapsed based on actual display start time
        if let Some(display_start) = self.display_start_time {
            let elapsed = now.saturating_duration_since(display_start);
            if elapsed >= self.cycle_duration() {
                debug!(
                    "Duration expired for {}: {} elapsed (target: {:?})",
                    self._name,
                    format!("{:.2}s", elapsed.as_secs_f64()),
                    self.cycle_duration()
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
                return self.apply_selected_path(path);
            }
        }
        None
    }

    pub fn pick_next_excluding(
        &mut self,
        excluded: &HashSet<PathBuf>,
    ) -> Option<(PathBuf, crate::queue::ContentType)> {
        if let Some(queue) = &mut self.queue {
            info!(
                "[PICK] {}: Calling queue.pick_next_excluding() with {} excluded",
                self._name,
                excluded.len()
            );
            if let Some(path) = queue.pick_next_excluding(excluded) {
                return self.apply_selected_path(path);
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
            debug!(
                "Transition completed for {} - duration timer now active ({:.2}s of content display starts now)",
                self._name,
                self.cycle_duration().as_secs_f64()
            );
        }
    }

    pub fn pick_prev(&mut self) -> Option<(PathBuf, crate::queue::ContentType)> {
        if let Some(queue) = &mut self.queue {
            if let Some(path) = queue.pick_prev() {
                let content_type = crate::queue::SmartQueue::get_content_type(&path).unwrap();
                self.current_path = Some(path.clone());
                // Reset display start time - will be set when content actually starts displaying
                self.display_start_time = None;
                self.next_change =
                    Some(Instant::now() + content_load_timeout(self.cycle_duration()));
                return Some((path, content_type));
            }
        }
        None
    }

    fn apply_selected_path(
        &mut self,
        path: PathBuf,
    ) -> Option<(PathBuf, crate::queue::ContentType)> {
        info!("[PICK] {}: Selected path: {:?}", self._name, path);
        let content_type = crate::queue::SmartQueue::get_content_type(&path).unwrap();
        self.current_path = Some(path.clone());
        self.display_start_time = None;
        self.next_change = Some(Instant::now() + content_load_timeout(self.cycle_duration()));

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
            self.cycle_duration(),
            path.display()
        );
        Some((path, content_type))
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
    paused: bool,                                       // Global pause state for wallpaper cycling
    // In-memory cache of discovered file lists per directory path.
    // Avoids re-scanning the same directory when multiple outputs share the same path.
    discovered_files_cache: HashMap<PathBuf, Vec<PathBuf>>,
}

impl MonitorManager {
    fn queue_config_changed(old: &OutputConfig, new: &OutputConfig) -> bool {
        old.path != new.path
            || old.video_ratio != new.video_ratio
            || old.sorting != new.sorting
            || old.default_playlist != new.default_playlist
    }

    fn build_refreshed_queue(
        cache: Arc<FileCache>,
        metrics: Option<Arc<PerformanceMetrics>>,
        name: &str,
        output_config: &OutputConfig,
    ) -> Option<SmartQueue> {
        let path = output_config.path.as_ref()?;
        let blacklist = cache.get_all_blacklisted().unwrap_or_else(|e| {
            tracing::warn!(
                "[CONFIG] Failed to read blacklist while refreshing queue: {}",
                e
            );
            HashSet::new()
        });
        let mut pool = cache
            .get_cached_pool(path)
            .ok()
            .flatten()
            .unwrap_or_default();
        pool.retain(|p| p.exists() && !blacklist.contains(p));
        if pool.is_empty() {
            pool = match SmartQueue::discover_content(path, &blacklist, cache.clone(), metrics) {
                Ok((p, _)) => p,
                Err(e) => {
                    tracing::warn!(
                        "[CONFIG] Could not discover files for refreshed path {:?}: {}",
                        path,
                        e
                    );
                    return None;
                }
            };
        }

        match SmartQueue::new_from_pool(
            path,
            pool,
            output_config.video_ratio,
            output_config.sorting,
            cache,
        ) {
            Ok(mut q) => {
                if let Some(pl_name) = &output_config.default_playlist {
                    if let Err(e) = q.set_playlist(Some(pl_name.clone())) {
                        tracing::warn!(
                            "[CONFIG] Failed to set default playlist '{}' for {} during reload: {}",
                            pl_name,
                            name,
                            e
                        );
                    }
                }
                Some(q)
            }
            Err(e) => {
                tracing::warn!("[CONFIG] Failed to refresh queue for {}: {}", name, e);
                None
            }
        }
    }

    fn flush_queue_stats(queue: &mut SmartQueue, label: &str) {
        if let Err(e) = queue.flush_stats() {
            tracing::warn!("[CONFIG] Failed to flush queue stats for {}: {}", label, e);
        }
    }

    fn reset_output_after_queue_refresh(orch: &mut OutputOrchestrator) {
        orch.current_path = None;
        orch.next_path = None;
        orch.next_content_type = None;
        orch.display_start_time = None;
        orch.next_change = None;
    }

    fn first_changed_name<'a>(
        changed_names: &'a [String],
        predicate: impl Fn(&str) -> bool,
    ) -> Option<&'a str> {
        changed_names
            .iter()
            .find(|name| predicate(name))
            .map(String::as_str)
    }

    fn earlier_deadline(current: &mut Option<Instant>, candidate: Option<Instant>) {
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
        self.config = config;
        let cache = self.cache.clone();
        let metrics = self.metrics.clone();
        let mut updated_configs = HashMap::new();
        let mut changed_names = Vec::new();

        for (name, orch) in &self.outputs {
            let output_config = self.config.get_config_for_output(name, &orch.description);
            if Self::queue_config_changed(&orch.config, &output_config) {
                changed_names.push(name.clone());
            }
            updated_configs.insert(name.clone(), output_config);
        }
        changed_names.sort();

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                for name in &changed_names {
                    if let Some(orch) = self.outputs.get_mut(name) {
                        let output_config = updated_configs
                            .get(name)
                            .expect("updated config should exist for changed output");
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
                    let representative_config = updated_configs
                        .get(representative_name)
                        .expect("updated config should exist for synchronized output");
                    self.shared_queue = Self::build_refreshed_queue(
                        cache.clone(),
                        metrics.clone(),
                        representative_name,
                        representative_config,
                    );
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
                    let representative_config = updated_configs
                        .get(representative_name)
                        .expect("updated config should exist for grouped output");
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
                        let output_config = updated_configs
                            .get(name)
                            .expect("updated config should exist for changed ungrouped output");
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

    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
        if paused {
            info!("[MONITOR_MANAGER] Wallpaper cycling paused");
        } else {
            // When resuming, reset timers so content doesn't immediately switch
            let now = Instant::now();
            for orch in self.outputs.values_mut() {
                orch.display_start_time = Some(now);
                orch.next_change = Some(now + orch.cycle_duration());
            }
            self.shared_display_start_time = Some(now);
            for start in self.group_display_start_times.values_mut() {
                *start = now;
            }
            info!("[MONITOR_MANAGER] Wallpaper cycling resumed (timers reset)");
        }
    }

    pub fn next_switch_deadline(&self) -> Option<Instant> {
        if self.paused {
            return None;
        }

        let mut next_deadline = None;

        match &self.config.global.monitor_behavior {
            MonitorBehavior::Independent => {
                for orch in self.outputs.values() {
                    Self::earlier_deadline(&mut next_deadline, orch.next_deadline());
                }
            }
            MonitorBehavior::Synchronized => {
                if let Some(shared_start) = self.shared_display_start_time {
                    if let Some(first_orch) = self.outputs.values().next() {
                        next_deadline = Some(shared_start + first_orch.config.duration);
                    }
                } else if let Some(first_orch) = self.outputs.values().next() {
                    next_deadline = if let Some(display_start) = first_orch.display_start_time {
                        Some(display_start + first_orch.config.duration)
                    } else {
                        first_orch.next_change
                    };
                }
            }
            MonitorBehavior::Grouped(_) => {
                let mut groups_to_check: HashMap<usize, Vec<String>> = HashMap::new();
                for (name, gid) in &self.output_groups {
                    groups_to_check.entry(*gid).or_default().push(name.clone());
                }

                for (gid, output_names) in groups_to_check {
                    if let Some(group_start) = self.group_display_start_times.get(&gid) {
                        if let Some(first_name) = output_names.first() {
                            if let Some(orch) = self.outputs.get(first_name) {
                                Self::earlier_deadline(
                                    &mut next_deadline,
                                    Some(*group_start + orch.config.duration),
                                );
                            }
                        }
                    } else if let Some(first_name) = output_names.first() {
                        if let Some(orch) = self.outputs.get(first_name) {
                            let candidate = if let Some(display_start) = orch.display_start_time {
                                Some(display_start + orch.config.duration)
                            } else {
                                orch.next_change
                            };
                            Self::earlier_deadline(&mut next_deadline, candidate);
                        }
                    }
                }

                for (name, orch) in &self.outputs {
                    if !self.output_groups.contains_key(name) {
                        Self::earlier_deadline(&mut next_deadline, orch.next_deadline());
                    }
                }
            }
        }

        next_deadline
    }

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
                        let content_type =
                            crate::queue::SmartQueue::get_content_type(&path).unwrap();
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
                            let content_type =
                                crate::queue::SmartQueue::get_content_type(&path).unwrap();
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
                        let content_type =
                            crate::queue::SmartQueue::get_content_type(&path).unwrap();
                        let now = Instant::now();
                        // Reset shared display start time for next cycle
                        self.shared_display_start_time = None;
                        for (name, orch) in &mut self.outputs {
                            orch.current_path = Some(path.clone());
                            orch.display_start_time = None;
                            orch.next_change =
                                Some(now + content_load_timeout(orch.config.duration));
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
                    debug!(
                        "Synchronized mode: First output ({}) completed transition - shared display timer started",
                        name
                    );
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
                        debug!(
                            "Group {}: First output ({}) completed transition - group display timer started",
                            gid, name
                        );
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
                let errors = self.apply_to_all_queues(|q| {
                    q.stats.playlists.insert(name.clone(), playlist.clone());
                    q.save_stats()
                });
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
                } else {
                    Response::Ok
                }
            }
            PlaylistCommand::Delete { name } => {
                let errors = self.apply_to_all_queues(|q| {
                    q.stats.playlists.remove(&name);
                    q.save_stats()
                });
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
                } else {
                    Response::Ok
                }
            }
            PlaylistCommand::Add { name, path } => {
                let path_buf = PathBuf::from(path);
                let errors = self.apply_to_all_queues(|q| {
                    if let Some(pl) = q.stats.playlists.get_mut(&name) {
                        if !pl.paths.contains(&path_buf) {
                            pl.paths.push(path_buf.clone());
                        }
                    }
                    q.save_stats()
                });
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
                } else {
                    Response::Ok
                }
            }
            PlaylistCommand::Remove { name, path } => {
                let path_buf = PathBuf::from(path);
                let errors = self.apply_to_all_queues(|q| {
                    if let Some(pl) = q.stats.playlists.get_mut(&name) {
                        pl.paths.retain(|p| p != &path_buf);
                    }
                    q.save_stats()
                });
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
                } else {
                    Response::Ok
                }
            }
            PlaylistCommand::Load { name } => {
                let errors = self.apply_to_all_queues(|q| {
                    q.set_playlist(name.clone())?;
                    q.save_stats()
                });
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
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
                let errors = self.apply_to_all_queues(|q| q.blacklist_file(path_buf.clone()));
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
                } else {
                    Response::Ok
                }
            }
            BlacklistCommand::Remove { path } => {
                let path_buf = PathBuf::from(path);
                let errors = self.apply_to_all_queues(|q| q.unblacklist_file(path_buf.clone()));
                if let Some(e) = errors.first() {
                    Response::Error(e.to_string())
                } else {
                    Response::Ok
                }
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

    fn apply_to_all_queues<F>(&mut self, mut f: F) -> Vec<anyhow::Error>
    where
        F: FnMut(&mut SmartQueue) -> Result<()>,
    {
        let mut errors = Vec::new();
        if let Some(q) = &mut self.shared_queue {
            if let Err(e) = f(q) {
                errors.push(e);
            }
        }
        for q in self.group_queues.values_mut() {
            if let Err(e) = f(q) {
                errors.push(e);
            }
        }
        for orch in self.outputs.values_mut() {
            if let Some(q) = &mut orch.queue {
                if let Err(e) = f(q) {
                    errors.push(e);
                }
            }
        }
        errors
    }

    /// Flush pending stats updates from all queues (batched write)
    pub fn flush_all_stats(&mut self) -> Result<()> {
        let mut first_err = None;
        if let Some(q) = &mut self.shared_queue {
            if let Err(e) = q.flush_stats() {
                first_err = first_err.or(Some(e));
            }
        }
        for q in self.group_queues.values_mut() {
            if let Err(e) = q.flush_stats() {
                first_err = first_err.or(Some(e));
            }
        }
        for orch in self.outputs.values_mut() {
            if let Some(q) = &mut orch.queue {
                if let Err(e) = q.flush_stats() {
                    first_err = first_err.or(Some(e));
                }
            }
        }

        if let Some(e) = first_err {
            Err(e)
        } else {
            Ok(())
        }
    }

    /// Forward filesystem watcher events to all active queues for incremental pool updates
    pub fn apply_pool_events(&mut self, events: Vec<crate::cache::PoolEvent>) {
        if events.is_empty() {
            return;
        }

        // Process events to invalidate our internal cache
        for event in &events {
            match event {
                crate::cache::PoolEvent::Added(path)
                | crate::cache::PoolEvent::Removed(path)
                | crate::cache::PoolEvent::Modified(path) => {
                    self.invalidate_cache(path);
                }
            }
        }

        if let Some(q) = &mut self.shared_queue {
            q.apply_pool_events(events.clone());
        }
        for q in self.group_queues.values_mut() {
            q.apply_pool_events(events.clone());
        }
        for orch in self.outputs.values_mut() {
            if let Some(q) = &mut orch.queue {
                q.apply_pool_events(events.clone());
            }
        }
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

    pub fn invalidate_cache(&mut self, path: &PathBuf) {
        // Invalidate entry for the file itself (if it was cached directly)
        self.discovered_files_cache.remove(path);
        // Also invalidate the parent directory as the list of files has changed
        if let Some(parent) = path.parent() {
            self.discovered_files_cache.remove(&parent.to_path_buf());
        }
    }

    pub fn get_history(&self, output_name: Option<String>) -> Vec<String> {
        let history = Vec::new();
        let to_strings = |paths: &std::collections::VecDeque<PathBuf>| -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::{Layer, PartialOutputConfig, SortingStrategy};
    use crate::queue::Playlist;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_output_config(duration: Duration) -> OutputConfig {
        OutputConfig {
            path: None,
            duration,
            video_ratio: 50,
            transition: crate::shaders::Transition::Fade,
            transition_time: 1000,
            volume: 100,
            sorting: SortingStrategy::Loveit,
            layer: Layer::Background,
            default_playlist: None,
        }
    }

    fn output_partial(config: &OutputConfig) -> PartialOutputConfig {
        PartialOutputConfig {
            path: config.path.clone(),
            duration: Some(config.duration),
            video_ratio: Some(config.video_ratio),
            transition: Some(config.transition.clone()),
            transition_time: Some(config.transition_time),
            volume: Some(config.volume),
            sorting: Some(config.sorting),
            layer: Some(config.layer.clone()),
            default_playlist: config.default_playlist.clone(),
        }
    }

    fn config_for_output_with_behavior(
        name: &str,
        config: &OutputConfig,
        monitor_behavior: MonitorBehavior,
    ) -> Config {
        let mut outputs = HashMap::new();
        outputs.insert(name.to_string(), output_partial(config));
        Config {
            global: crate::orchestration::GlobalConfig {
                monitor_behavior,
                ..Default::default()
            },
            any: PartialOutputConfig::default(),
            outputs,
        }
    }

    fn config_for_output(name: &str, config: &OutputConfig) -> Config {
        config_for_output_with_behavior(name, config, MonitorBehavior::Independent)
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kaleidux-monitor-test-{}-{}-{}",
            name,
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("test dir should be created");
        dir
    }

    fn write_test_image(dir: &std::path::Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let mut bytes = vec![0u8; 16];
        bytes[..4].copy_from_slice(&[0x89, b'P', b'N', b'G']);
        std::fs::write(&path, bytes).expect("test image should be written");
        path
    }

    fn make_test_queue(
        cache: Arc<FileCache>,
        dir: &std::path::Path,
        pool: Vec<PathBuf>,
        config: &OutputConfig,
    ) -> SmartQueue {
        let mut queue =
            SmartQueue::new_from_pool(dir, pool, config.video_ratio, config.sorting, cache)
                .expect("test queue should be created");
        if let Some(pl_name) = &config.default_playlist {
            queue
                .set_playlist(Some(pl_name.clone()))
                .expect("default playlist should be applied");
        }
        queue
    }

    fn make_test_manager(
        name: &str,
        cache: Arc<FileCache>,
        orch: OutputOrchestrator,
        config: Config,
    ) -> MonitorManager {
        let mut outputs = HashMap::new();
        outputs.insert(name.to_string(), orch);
        MonitorManager {
            config,
            outputs,
            shared_queue: None,
            group_queues: HashMap::new(),
            output_groups: HashMap::new(),
            shared_display_start_time: None,
            group_display_start_times: HashMap::new(),
            cache,
            metrics: None,
            paused: false,
            discovered_files_cache: HashMap::new(),
        }
    }

    #[test]
    fn independent_phase_offset_is_stable_for_same_output_name() {
        let base = Duration::from_secs(60);
        let first = independent_phase_offset("DP-2", base);
        let second = independent_phase_offset("DP-2", base);

        assert_eq!(first, second);
    }

    #[test]
    fn independent_phase_offset_stays_within_cap() {
        let base = Duration::from_secs(120);
        let offset = independent_phase_offset("HDMI-A-1", base);

        assert!(offset <= Duration::from_millis(120));
        assert!(offset <= base / 12);
    }

    #[test]
    fn independent_phase_offset_is_zero_for_short_durations() {
        assert_eq!(
            independent_phase_offset("DP-3", Duration::from_millis(80)),
            Duration::ZERO
        );
    }

    #[test]
    fn applying_updated_config_recomputes_phase_offset() {
        let old_duration = Duration::from_secs(1);
        let new_duration = Duration::from_secs(2);
        let old_offset = independent_phase_offset("DP-2", old_duration);
        let new_offset = independent_phase_offset("DP-2", new_duration);

        assert_ne!(
            old_offset, new_offset,
            "test requires durations with distinct phase offsets"
        );

        let mut orch = OutputOrchestrator {
            _name: "DP-2".to_string(),
            description: "DisplayPort-2".to_string(),
            config: test_output_config(old_duration),
            queue: None,
            current_path: None,
            next_path: None,
            next_content_type: None,
            next_change: None,
            display_start_time: None,
            phase_offset: old_offset,
        };

        orch.apply_config(test_output_config(new_duration));

        assert_eq!(orch.phase_offset, new_offset);
        assert_eq!(orch.config.duration, new_duration);
    }

    #[test]
    fn update_config_rebuilds_same_path_queue_for_sorting_and_playlist_changes() {
        let temp = unique_test_dir("same-path-reload");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let path = temp.join("wallpapers");
        std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
        let first = write_test_image(&path, "a.png");
        let second = write_test_image(&path, "b.png");
        cache
            .set_cached_pool(&path, &[first.clone(), second.clone()])
            .expect("cached pool should be stored");
        cache
            .set_playlist(
                "favorites",
                &Playlist {
                    paths: vec![second.clone()],
                    strategy: SortingStrategy::Descending,
                    enabled: true,
                },
            )
            .expect("playlist should be stored");

        let mut old_config = test_output_config(Duration::from_secs(60));
        old_config.path = Some(path.clone());
        let old_queue = make_test_queue(
            cache.clone(),
            &path,
            vec![first.clone(), second.clone()],
            &old_config,
        );
        let orch = OutputOrchestrator {
            _name: "DP-1".to_string(),
            description: "DisplayPort-1".to_string(),
            phase_offset: independent_phase_offset("DP-1", old_config.duration),
            config: old_config.clone(),
            queue: Some(old_queue),
            current_path: Some(first.clone()),
            next_path: Some(second.clone()),
            next_content_type: Some(crate::queue::ContentType::Image),
            next_change: Some(Instant::now()),
            display_start_time: Some(Instant::now()),
        };
        let mut manager = make_test_manager(
            "DP-1",
            cache.clone(),
            orch,
            config_for_output("DP-1", &old_config),
        );

        let mut new_config = old_config.clone();
        new_config.sorting = SortingStrategy::Ascending;
        new_config.default_playlist = Some("favorites".to_string());

        manager.update_config(config_for_output("DP-1", &new_config));

        let orch = manager.outputs.get("DP-1").expect("output should exist");
        let queue = orch.queue.as_ref().expect("queue should be rebuilt");
        assert_eq!(queue.strategy, SortingStrategy::Ascending);
        assert_eq!(queue.active_playlist.as_deref(), Some("favorites"));
        assert_eq!(queue.pool, vec![second.clone()]);
        assert!(orch.current_path.is_none());
        assert!(orch.next_path.is_none());
        assert!(orch.display_start_time.is_none());
        assert!(orch.next_change.is_none());
    }

    #[test]
    fn update_config_rebuilds_synchronized_shared_queue() {
        let temp = unique_test_dir("sync-reload");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let path = temp.join("wallpapers");
        std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
        let first = write_test_image(&path, "a.png");
        let second = write_test_image(&path, "b.png");
        cache
            .set_cached_pool(&path, &[first.clone(), second.clone()])
            .expect("cached pool should be stored");
        cache
            .set_playlist(
                "favorites",
                &Playlist {
                    paths: vec![second.clone()],
                    strategy: SortingStrategy::Descending,
                    enabled: true,
                },
            )
            .expect("playlist should be stored");

        let mut old_config = test_output_config(Duration::from_secs(60));
        old_config.path = Some(path.clone());
        let shared_queue = make_test_queue(
            cache.clone(),
            &path,
            vec![first.clone(), second.clone()],
            &old_config,
        );
        let orch = OutputOrchestrator {
            _name: "DP-1".to_string(),
            description: "DisplayPort-1".to_string(),
            phase_offset: independent_phase_offset("DP-1", old_config.duration),
            config: old_config.clone(),
            queue: None,
            current_path: Some(first.clone()),
            next_path: Some(second.clone()),
            next_content_type: Some(crate::queue::ContentType::Image),
            next_change: Some(Instant::now()),
            display_start_time: Some(Instant::now()),
        };
        let mut manager = make_test_manager(
            "DP-1",
            cache.clone(),
            orch,
            config_for_output_with_behavior("DP-1", &old_config, MonitorBehavior::Synchronized),
        );
        manager.shared_queue = Some(shared_queue);
        manager.shared_display_start_time = Some(Instant::now());

        let mut new_config = old_config.clone();
        new_config.sorting = SortingStrategy::Ascending;
        new_config.default_playlist = Some("favorites".to_string());

        manager.update_config(config_for_output_with_behavior(
            "DP-1",
            &new_config,
            MonitorBehavior::Synchronized,
        ));

        let shared_queue = manager
            .shared_queue
            .as_ref()
            .expect("shared queue should be rebuilt");
        assert_eq!(shared_queue.strategy, SortingStrategy::Ascending);
        assert_eq!(shared_queue.active_playlist.as_deref(), Some("favorites"));
        assert_eq!(shared_queue.pool, vec![second.clone()]);
        assert!(manager.shared_display_start_time.is_none());

        let orch = manager.outputs.get("DP-1").expect("output should exist");
        assert!(orch.queue.is_none());
        assert!(orch.current_path.is_none());
        assert!(orch.next_path.is_none());
        assert!(orch.display_start_time.is_none());
        assert!(orch.next_change.is_none());
    }

    #[test]
    fn update_config_rebuilds_group_queue() {
        let temp = unique_test_dir("group-reload");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let path = temp.join("wallpapers");
        std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
        let first = write_test_image(&path, "a.png");
        let second = write_test_image(&path, "b.png");
        cache
            .set_cached_pool(&path, &[first.clone(), second.clone()])
            .expect("cached pool should be stored");
        cache
            .set_playlist(
                "favorites",
                &Playlist {
                    paths: vec![second.clone()],
                    strategy: SortingStrategy::Descending,
                    enabled: true,
                },
            )
            .expect("playlist should be stored");

        let mut old_config = test_output_config(Duration::from_secs(60));
        old_config.path = Some(path.clone());
        let group_queue = make_test_queue(
            cache.clone(),
            &path,
            vec![first.clone(), second.clone()],
            &old_config,
        );
        let orch = OutputOrchestrator {
            _name: "DP-1".to_string(),
            description: "DisplayPort-1".to_string(),
            phase_offset: independent_phase_offset("DP-1", old_config.duration),
            config: old_config.clone(),
            queue: None,
            current_path: Some(first.clone()),
            next_path: Some(second.clone()),
            next_content_type: Some(crate::queue::ContentType::Image),
            next_change: Some(Instant::now()),
            display_start_time: Some(Instant::now()),
        };
        let mut manager = make_test_manager(
            "DP-1",
            cache.clone(),
            orch,
            config_for_output_with_behavior(
                "DP-1",
                &old_config,
                MonitorBehavior::Grouped(vec![vec!["DP-1".to_string()]]),
            ),
        );
        manager.output_groups.insert("DP-1".to_string(), 0);
        manager.group_queues.insert(0, group_queue);
        manager.group_display_start_times.insert(0, Instant::now());

        let mut new_config = old_config.clone();
        new_config.sorting = SortingStrategy::Ascending;
        new_config.default_playlist = Some("favorites".to_string());

        manager.update_config(config_for_output_with_behavior(
            "DP-1",
            &new_config,
            MonitorBehavior::Grouped(vec![vec!["DP-1".to_string()]]),
        ));

        let group_queue = manager
            .group_queues
            .get(&0)
            .expect("group queue should be rebuilt");
        assert_eq!(group_queue.strategy, SortingStrategy::Ascending);
        assert_eq!(group_queue.active_playlist.as_deref(), Some("favorites"));
        assert_eq!(group_queue.pool, vec![second.clone()]);
        assert!(!manager.group_display_start_times.contains_key(&0));

        let orch = manager.outputs.get("DP-1").expect("output should exist");
        assert!(orch.queue.is_none());
        assert!(orch.current_path.is_none());
        assert!(orch.next_path.is_none());
        assert!(orch.display_start_time.is_none());
        assert!(orch.next_change.is_none());
    }

    #[test]
    fn update_config_flushes_pending_stats_before_replacing_queue() {
        let temp = unique_test_dir("flush-stats");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let path = temp.join("wallpapers");
        std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
        let file = write_test_image(&path, "a.png");
        cache
            .set_cached_pool(&path, std::slice::from_ref(&file))
            .expect("cached pool should be stored");

        let mut old_config = test_output_config(Duration::from_secs(60));
        old_config.path = Some(path.clone());
        old_config.sorting = SortingStrategy::Ascending;
        let mut old_queue = make_test_queue(cache.clone(), &path, vec![file.clone()], &old_config);
        assert_eq!(old_queue.pick_next(), Some(file.clone()));

        let orch = OutputOrchestrator {
            _name: "DP-1".to_string(),
            description: "DisplayPort-1".to_string(),
            phase_offset: independent_phase_offset("DP-1", old_config.duration),
            config: old_config.clone(),
            queue: Some(old_queue),
            current_path: Some(file.clone()),
            next_path: None,
            next_content_type: Some(crate::queue::ContentType::Image),
            next_change: Some(Instant::now()),
            display_start_time: Some(Instant::now()),
        };
        let mut manager = make_test_manager(
            "DP-1",
            cache.clone(),
            orch,
            config_for_output("DP-1", &old_config),
        );

        let mut new_config = old_config.clone();
        new_config.default_playlist = Some("missing".to_string());

        manager.update_config(config_for_output("DP-1", &new_config));

        let stats = cache
            .get_all_file_stats()
            .expect("file stats should be readable");
        let stat = stats
            .get(&file)
            .expect("picked file stats should be flushed");
        assert_eq!(stat.count, 1);
        assert!(stat.last_seen.is_some());
    }

    #[test]
    fn update_config_clears_stale_queue_when_refresh_fails() {
        let temp = unique_test_dir("refresh-failure");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let old_path = temp.join("old");
        std::fs::create_dir_all(&old_path).expect("old dir should be created");
        let file = write_test_image(&old_path, "old.png");
        cache
            .set_cached_pool(&old_path, std::slice::from_ref(&file))
            .expect("cached pool should be stored");

        let mut old_config = test_output_config(Duration::from_secs(60));
        old_config.path = Some(old_path.clone());
        let old_queue = make_test_queue(cache.clone(), &old_path, vec![file.clone()], &old_config);
        let orch = OutputOrchestrator {
            _name: "DP-1".to_string(),
            description: "DisplayPort-1".to_string(),
            phase_offset: independent_phase_offset("DP-1", old_config.duration),
            config: old_config.clone(),
            queue: Some(old_queue),
            current_path: Some(file.clone()),
            next_path: None,
            next_content_type: Some(crate::queue::ContentType::Image),
            next_change: Some(Instant::now()),
            display_start_time: Some(Instant::now()),
        };
        let mut manager = make_test_manager(
            "DP-1",
            cache.clone(),
            orch,
            config_for_output("DP-1", &old_config),
        );

        let bad_stats_path = temp.join("bad-stats");
        cache
            .insert_invalid_file_stats_bytes(&bad_stats_path, &[0xff, 0x00, 0x01])
            .expect("invalid stats bytes should be inserted");

        let new_path = temp.join("new");
        std::fs::create_dir_all(&new_path).expect("new dir should be created");
        let new_file = write_test_image(&new_path, "new.png");
        cache
            .set_cached_pool(&new_path, std::slice::from_ref(&new_file))
            .expect("new cached pool should be stored");

        let mut new_config = old_config.clone();
        new_config.path = Some(new_path.clone());

        manager.update_config(config_for_output("DP-1", &new_config));

        let orch = manager.outputs.get("DP-1").expect("output should exist");
        assert!(
            orch.queue.is_none(),
            "stale queue should be cleared on refresh failure"
        );
        assert_eq!(orch.config.path, Some(new_path));
        assert!(orch.current_path.is_none());
        assert!(orch.display_start_time.is_none());
        assert!(orch.next_change.is_none());
    }
}
