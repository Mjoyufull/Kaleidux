use super::{MonitorManager, OutputOrchestrator};
use crate::cache::FileCache;
use crate::metrics::PerformanceMetrics;
use crate::orchestration::OutputConfig;
use crate::queue::SmartQueue;
use std::sync::Arc;

impl MonitorManager {
    pub(super) fn queue_config_changed(old: &OutputConfig, new: &OutputConfig) -> bool {
        old.path != new.path
            || old.video_ratio != new.video_ratio
            || old.sorting != new.sorting
            || old.default_playlist != new.default_playlist
    }

    fn build_refreshed_queue_sync(
        cache: Arc<FileCache>,
        metrics: Option<Arc<PerformanceMetrics>>,
        name: &str,
        output_config: &OutputConfig,
    ) -> Option<SmartQueue> {
        let path = output_config.path.as_ref()?;
        let blacklist = match cache.get_all_blacklisted() {
            Ok(blacklist) => blacklist,
            Err(e) => {
                tracing::error!(
                    "[CONFIG] Failed to read blacklist while refreshing queue for {}: {}",
                    name,
                    e
                );
                return None;
            }
        };
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

    pub(super) fn build_refreshed_queue(
        cache: Arc<FileCache>,
        metrics: Option<Arc<PerformanceMetrics>>,
        name: &str,
        output_config: &OutputConfig,
    ) -> Option<SmartQueue> {
        let name_owned = name.to_string();
        let output_config_owned = output_config.clone();

        Self::build_refreshed_queue_sync(cache, metrics, &name_owned, &output_config_owned)
    }

    pub(super) fn flush_queue_stats(queue: &mut SmartQueue, label: &str) {
        if let Err(e) = queue.flush_stats() {
            tracing::warn!("[CONFIG] Failed to flush queue stats for {}: {}", label, e);
        }
    }

    pub(super) fn reset_output_after_queue_refresh(orch: &mut OutputOrchestrator) {
        orch.current_path = None;
        orch.next_path = None;
        orch.next_content_type = None;
        orch.display_start_time = None;
        orch.next_change = None;
    }
}
