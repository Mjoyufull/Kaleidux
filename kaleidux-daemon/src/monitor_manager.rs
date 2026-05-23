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
use tracing::debug;

mod add_output;
mod config;
mod config_queue;
mod deadline;
mod manual_selection;
mod output;
mod schedule;

pub use output::OutputOrchestrator;
use output::{content_load_timeout, independent_phase_offset};
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
#[path = "monitor_manager/tests.rs"]
mod tests;
