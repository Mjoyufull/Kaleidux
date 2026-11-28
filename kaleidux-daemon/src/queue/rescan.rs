//! Coordinates full discovery with watcher generations; blocking scans retain ownership until done.

use super::media_index::{RootMediaIndex, shared_root_index};
use super::{ContentType, SmartQueue};
use crate::background::{self, BackgroundWorkKind};
use crate::cache::FileCache;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct ScanLease {
    index: Arc<RootMediaIndex>,
    generation: u64,
    finished: bool,
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        if !self.finished {
            self.index.cancel_rescan();
        }
    }
}

impl SmartQueue {
    pub(super) async fn full_discovery(
        path: &Path,
        blacklist: &HashSet<PathBuf>,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> anyhow::Result<(Vec<PathBuf>, HashMap<PathBuf, ContentType>)> {
        let index = shared_root_index(path);
        let Some(generation) = index.request_rescan() else {
            // Another owner includes this request in its final scan. Waiting
            // occurs in the queue worker, never in either display event loop.
            while index.rescan_running() {
                anyhow::ensure!(
                    background::is_accepting_new_work(),
                    "Directory discovery cancelled during shutdown"
                );
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            let snapshot = index.snapshot();
            let identity = super::BlacklistIdentity::new(blacklist);
            let types: HashMap<_, _> = snapshot
                .entries
                .iter()
                .filter(|entry| !identity.contains(&entry.path))
                .map(|entry| (entry.path.clone(), entry.content_type))
                .collect();
            anyhow::ensure!(!types.is_empty(), "No supported media found in {path:?}");
            return Ok((types.keys().cloned().collect(), types));
        };
        let lease = ScanLease {
            index,
            generation,
            finished: false,
        };
        let root = path.to_path_buf();
        let excluded = blacklist.clone();
        let handle = background::spawn_blocking_tracked_wait(
            BackgroundWorkKind::QueueDiscovery,
            move || {
                let mut owned = lease;
                loop {
                    let result =
                        Self::discover_content(&root, &excluded, cache.clone(), metrics.clone());
                    anyhow::ensure!(
                        background::is_accepting_new_work(),
                        "Directory discovery cancelled during shutdown"
                    );
                    if let Some(next) = owned.index.finish_rescan(owned.generation) {
                        owned.generation = next;
                    } else {
                        owned.finished = true;
                        return result;
                    }
                }
            },
        )
        .await;
        let Some(handle) = handle else {
            anyhow::bail!("Directory discovery rejected during shutdown");
        };
        handle.await?
    }
}
