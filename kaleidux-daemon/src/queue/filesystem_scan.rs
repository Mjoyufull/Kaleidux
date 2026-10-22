//! Walks directory metadata on the bounded discovery pool and publishes to its owning index.

use super::{ContentType, SmartQueue};
use crate::cache::FileCache;
use anyhow::Result;
use jwalk::WalkDir;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl SmartQueue {
    pub(crate) fn discover_content(
        path: &Path,
        blacklist: &std::collections::HashSet<PathBuf>,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> Result<(Vec<PathBuf>, HashMap<PathBuf, ContentType>)> {
        Self::discover_content_into_index(
            path,
            blacklist,
            cache,
            metrics,
            &super::media_index::shared_root_index(path),
        )
    }

    pub(super) fn discover_content_into_index(
        path: &Path,
        blacklist: &std::collections::HashSet<PathBuf>,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
        index: &super::media_index::RootMediaIndex,
    ) -> Result<(Vec<PathBuf>, HashMap<PathBuf, ContentType>)> {
        let discovery_start = std::time::Instant::now();
        let blacklist = super::BlacklistIdentity::new(blacklist);
        let mut files = Vec::new();
        let mut ct_cache: HashMap<PathBuf, ContentType> = HashMap::new();
        let mut cache_updates: Vec<(PathBuf, crate::cache::FileMetadata)> = Vec::new();

        // Reuse one small process-wide pool. Creating a Rayon pool for every
        // discovery burst caused thread churn and defeated the thread bound.
        let walk_dir = WalkDir::new(path).follow_links(true).parallelism(
            jwalk::Parallelism::RayonExistingPool {
                pool: DIRECTORY_POOL.clone(),
                busy_timeout: Some(Duration::from_secs(1)),
            },
        );

        // Consume the walker directly into the metadata needed by the final
        // index. Keeping `DirEntry` objects around duplicates path and walk
        // state for large roots.
        let candidates: Vec<_> = walk_dir
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .filter_map(|entry| {
                let path = entry.path();
                if blacklist.contains(path.as_path()) {
                    return None;
                }
                let metadata = entry.metadata().ok()?;
                let modified = metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .unwrap_or_default();
                Some((
                    path,
                    metadata.len(),
                    modified.as_secs(),
                    modified.subsec_nanos(),
                ))
            })
            .collect();

        let candidate_paths: Vec<PathBuf> = candidates
            .iter()
            .map(|(path, _size, _secs, _nanos)| path.clone())
            .collect();
        let cached_metadata = cache.batch_get_file_metadata(&candidate_paths)?;

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        for (p, fs_size, fs_mtime, fs_mtime_nanos) in candidates {
            let (content_type, needs_cache_update) = match cached_metadata.get(&p) {
                Some(cached) => {
                    if cached.mtime == fs_mtime
                        && cached.mtime_nanos == fs_mtime_nanos
                        && cached.size == fs_size
                    {
                        // Cache hit — mtime matches, file unchanged
                        if let Some(m) = &metrics {
                            m.record_cache_hit();
                        }
                        let ct = match cached.content_type {
                            0 => Some(ContentType::Image),
                            1 => Some(ContentType::Video),
                            _ => None,
                        };
                        (ct, false)
                    } else {
                        // Cache stale — file changed since last discovery
                        if let Some(m) = &metrics {
                            m.record_cache_miss();
                        }
                        (Self::get_content_type(&p), true)
                    }
                }
                _ => {
                    // Not in cache — first time seeing this file
                    if let Some(m) = &metrics {
                        m.record_cache_miss();
                    }
                    (Self::get_content_type(&p), true)
                }
            };

            if let Some(ct) = content_type.filter(|ct| ct.supported()) {
                files.push(p.clone());
                ct_cache.insert(p.clone(), ct);

                // Only build cache update for new/changed entries (reuse fs_meta from above)
                if needs_cache_update {
                    cache_updates.push((
                        p,
                        crate::cache::FileMetadata {
                            mtime: fs_mtime,
                            mtime_nanos: fs_mtime_nanos,
                            size: fs_size,
                            content_type: match ct {
                                ContentType::Image => 0,
                                ContentType::Video => 1,
                            },
                            discovered_at: now_secs,
                        },
                    ));
                }
            }
        }

        // Single batched redb write transaction for all metadata updates
        if let Err(e) = cache.batch_set_file_metadata(&cache_updates) {
            tracing::warn!("[QUEUE] Failed to batch-update file cache: {}", e);
        }

        // Persist the pool for instant reload on next startup
        if let Err(e) = cache.set_cached_pool(path, &files) {
            tracing::warn!("[QUEUE] Failed to cache file pool: {}", e);
        }

        // Manual queues retain a private index. Publish into the scan owner's
        // index, including empty results, so removed paths cannot remain active.
        index.replace(&files, &ct_cache);

        if files.is_empty() {
            anyhow::bail!("No supported images or videos found in {:?}", path);
        }

        // Record file discovery CPU time
        if let Some(m) = &metrics {
            let discovery_duration = discovery_start.elapsed();
            m.record_file_discovery_cpu_time(discovery_duration);
            tracing::info!(
                "[QUEUE] Discovery completed: {} files, {} cache updates, {:.1}ms",
                files.len(),
                cache_updates.len(),
                discovery_duration.as_secs_f64() * 1000.0
            );
        }

        Ok((files, ct_cache))
    }
}
static DIRECTORY_POOL: LazyLock<Arc<jwalk::rayon::ThreadPool>> = LazyLock::new(|| {
    Arc::new(
        jwalk::rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|index| format!("kld-directory-{index}"))
            .build()
            .expect("bounded directory worker pool should initialize"),
    )
});
