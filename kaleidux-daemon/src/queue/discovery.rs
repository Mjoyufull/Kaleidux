use super::{ContentType, SmartQueue};
use crate::background::{self, BackgroundWorkKind};
use crate::cache::FileCache;
use anyhow::Result;
use jwalk::WalkDir;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl SmartQueue {
    fn root_index_fields(
        path: &Path,
        pool: &[PathBuf],
        content_types: &HashMap<PathBuf, ContentType>,
    ) -> (Arc<super::media_index::RootMediaIndex>, u64) {
        let index = super::media_index::shared_root_index(path);
        let snapshot = index.install_if_empty(pool, content_types);
        (index, snapshot.generation)
    }

    fn content_type_from_metadata(meta: crate::cache::FileMetadata) -> ContentType {
        if meta.content_type == 0 {
            ContentType::Image
        } else {
            ContentType::Video
        }
    }

    fn populate_content_type_cache_from_pool(
        cache: &Arc<FileCache>,
        pool: &[PathBuf],
    ) -> HashMap<PathBuf, ContentType> {
        let mut init_map = HashMap::with_capacity(pool.len());
        let cached_metadata = cache.batch_get_file_metadata(pool).unwrap_or_else(|error| {
            tracing::warn!("[QUEUE] Failed to batch-read cached media types: {error}");
            HashMap::new()
        });
        for path in pool {
            let content_type = cached_metadata
                .get(path)
                .cloned()
                .map(Self::content_type_from_metadata)
                .or_else(|| Self::get_content_type(path));
            if let Some(content_type) = content_type {
                init_map.insert(path.clone(), content_type);
            }
        }
        init_map
    }

    pub(super) fn fallback_current_index(
        strategy: crate::orchestration::SortingStrategy,
        pool_len: usize,
    ) -> usize {
        if strategy == crate::orchestration::SortingStrategy::Descending {
            pool_len.saturating_sub(1)
        } else {
            0
        }
    }

    pub async fn new_with_cache(
        path: &Path,
        video_ratio: u8,
        strategy: crate::orchestration::SortingStrategy,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> Result<Self> {
        tracing::info!("[QUEUE] new_with_cache called for path: {:?}", path);
        let stats = Self::load_stats_from_cache(&cache)?;
        tracing::info!(
            "[QUEUE] Loaded stats, blacklist size: {}",
            stats.blacklist.len()
        );

        let shared_index = super::media_index::shared_root_index(path);
        let shared_snapshot = shared_index.snapshot();
        if !shared_snapshot.entries.is_empty() {
            let mut pool = Vec::with_capacity(shared_snapshot.entries.len());
            let mut content_type_cache = HashMap::with_capacity(shared_snapshot.entries.len());
            for entry in shared_snapshot.entries.iter() {
                if !stats.blacklist.contains(&entry.path) {
                    pool.push(entry.path.clone());
                    content_type_cache.insert(entry.path.clone(), entry.content_type);
                }
            }
            pool.sort();
            let current_index = Self::fallback_current_index(strategy, pool.len());
            tracing::info!(
                "[QUEUE] Reusing shared root media index generation {} ({} files) for {:?}",
                shared_snapshot.generation,
                pool.len(),
                path
            );
            return Ok(Self {
                pool,
                stats,
                video_ratio,
                strategy,
                current_index,
                planned_sequential_type: None,
                history: VecDeque::new(),
                root_path: path.to_path_buf(),
                active_playlist: None,
                cache,
                pending_stats_updates: HashMap::new(),
                content_type_cache,
                root_index: shared_index,
                root_generation: shared_snapshot.generation,
            });
        }

        // Try loading cached pool from redb first (near-instant)
        let ct_cache_init: Option<HashMap<PathBuf, ContentType>>;
        let pool = match cache.get_cached_pool(path) {
            Ok(Some(cached_pool)) => {
                // Validate cached pool: filter out files that no longer exist
                let valid_pool = {
                    let cp = cached_pool.clone();
                    let bl = stats.blacklist.clone();
                    let Some(handle) = background::spawn_blocking_tracked_wait(
                        BackgroundWorkKind::QueueDiscovery,
                        move || {
                            cp.into_iter()
                                .filter(|p| p.exists() && !bl.contains(p))
                                .collect::<Vec<PathBuf>>()
                        },
                    )
                    .await
                    else {
                        tracing::warn!(
                            "[QUEUE] Skipping cached pool validation for {:?}: shutdown in progress",
                            path
                        );
                        let root_index = super::media_index::shared_root_index(path);
                        let root_generation = root_index.snapshot().generation;
                        return Ok(Self {
                            pool: Vec::new(),
                            stats,
                            video_ratio,
                            strategy,
                            current_index: Self::fallback_current_index(strategy, 0),
                            planned_sequential_type: None,
                            history: VecDeque::new(),
                            root_path: path.to_path_buf(),
                            active_playlist: None,
                            cache,
                            pending_stats_updates: HashMap::new(),
                            content_type_cache: HashMap::new(),
                            root_index,
                            root_generation,
                        });
                    };
                    handle.await?
                };

                if !valid_pool.is_empty() {
                    tracing::info!(
                        "[QUEUE] Loaded cached pool: {} files for {:?}",
                        valid_pool.len(),
                        path
                    );

                    // Spawn background full discovery to refresh the pool and metadata cache
                    let bg_path = path.to_path_buf();
                    let bg_blacklist = stats.blacklist.clone();
                    let bg_cache = cache.clone();
                    let bg_metrics = metrics.clone();

                    if background::spawn_blocking_tracked(
                        BackgroundWorkKind::QueueDiscovery,
                        move || match Self::discover_content(
                            &bg_path,
                            &bg_blacklist,
                            bg_cache,
                            bg_metrics,
                        ) {
                            Ok((pool, _)) => {
                                tracing::info!(
                                    "[QUEUE] Background pool refresh finished ({} files) for {:?}",
                                    pool.len(),
                                    bg_path
                                );
                            }
                            Err(e) => {
                                tracing::warn!("[QUEUE] Background pool refresh failed: {}", e);
                            }
                        },
                    )
                    .is_none()
                    {
                        tracing::debug!(
                            "[QUEUE] Skipping background pool refresh for {:?}: shutdown in progress",
                            path
                        );
                    }

                    ct_cache_init = Some(Self::populate_content_type_cache_from_pool(
                        &cache,
                        &valid_pool,
                    ));

                    valid_pool
                } else {
                    tracing::info!("[QUEUE] Cached pool was stale, running full discovery");
                    let (pool, ct) = Self::full_discovery(
                        path,
                        &stats.blacklist,
                        cache.clone(),
                        metrics.clone(),
                    )
                    .await?;
                    ct_cache_init = Some(ct);
                    pool
                }
            }
            _ => {
                tracing::info!("[QUEUE] No cached pool found, running full discovery");
                let (pool, ct) =
                    Self::full_discovery(path, &stats.blacklist, cache.clone(), metrics.clone())
                        .await?;
                ct_cache_init = Some(ct);
                pool
            }
        };

        tracing::info!(
            "[QUEUE] File discovery completed, found {} files",
            pool.len()
        );

        let mut pool = pool;
        // Sort the pool initially for sequential strategies
        pool.sort();

        let current_index = Self::fallback_current_index(strategy, pool.len());

        let content_type_cache = ct_cache_init.unwrap_or_default();
        let (root_index, root_generation) =
            Self::root_index_fields(path, &pool, &content_type_cache);
        Ok(Self {
            pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            planned_sequential_type: None,
            history: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            content_type_cache,
            root_index,
            root_generation,
        })
    }

    /// Run full file discovery on a blocking thread
    async fn full_discovery(
        path: &Path,
        blacklist: &std::collections::HashSet<PathBuf>,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> Result<(Vec<PathBuf>, HashMap<PathBuf, ContentType>)> {
        let path_buf = path.to_path_buf();
        let blacklist_clone = blacklist.clone();
        let Some(handle) = background::spawn_blocking_tracked_wait(
            BackgroundWorkKind::QueueDiscovery,
            move || Self::discover_content(&path_buf, &blacklist_clone, cache, metrics),
        )
        .await
        else {
            return Ok((Vec::new(), HashMap::new()));
        };
        handle.await?
    }

    /// Create a queue from a pre-discovered file list (avoids re-scanning the directory)
    pub fn new_from_pool(
        path: &Path,
        pool: Vec<PathBuf>,
        video_ratio: u8,
        strategy: crate::orchestration::SortingStrategy,
        cache: Arc<FileCache>,
    ) -> Result<Self> {
        let stats = Self::load_stats_from_cache(&cache)?;
        let mut pool = pool;
        pool.sort();

        let current_index = Self::fallback_current_index(strategy, pool.len());

        let content_type_cache = Self::populate_content_type_cache_from_pool(&cache, &pool);
        let (root_index, root_generation) =
            Self::root_index_fields(path, &pool, &content_type_cache);
        Ok(Self {
            content_type_cache,
            pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            planned_sequential_type: None,
            history: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            root_index,
            root_generation,
        })
    }

    #[inline]
    pub fn get_content_type(path: &Path) -> Option<ContentType> {
        use std::io::Read;
        let mut file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return None,
        };
        let mut buffer = [0u8; 16];
        if file.read_exact(&mut buffer).is_err() {
            return None;
        }

        // JPEG: FF D8 FF
        if buffer[0..3] == [0xFF, 0xD8, 0xFF] {
            return Some(ContentType::Image);
        }
        // PNG: 89 50 4E 47
        if buffer[0..4] == [0x89, 0x50, 0x4E, 0x47] {
            return Some(ContentType::Image);
        }
        // GIF: GIF8
        if buffer[0..4] == *b"GIF8" {
            return Some(ContentType::Image);
        }
        // WebP: RIFF .... WEBP
        if &buffer[0..4] == b"RIFF" && &buffer[8..12] == b"WEBP" {
            return Some(ContentType::Image);
        }
        // EBML (MKV/WebM): 1A 45 DF A3
        if buffer[0..4] == [0x1A, 0x45, 0xDF, 0xA3] {
            return Some(ContentType::Video);
        }
        // ISO BMFF container: ....ftyp (shared by MP4/MOV video and AVIF/HEIF images)
        if &buffer[4..8] == b"ftyp" {
            // AVIF images use brands: avif, avis, mif1
            if &buffer[8..12] == b"avif" || &buffer[8..12] == b"avis" || &buffer[8..12] == b"mif1" {
                return Some(ContentType::Image);
            }
            return Some(ContentType::Video);
        }

        None
    }

    pub(crate) fn discover_content(
        path: &Path,
        blacklist: &std::collections::HashSet<PathBuf>,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> Result<(Vec<PathBuf>, HashMap<PathBuf, ContentType>)> {
        let discovery_start = std::time::Instant::now();
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

            if let Some(ct) = content_type {
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

        super::media_index::shared_root_index(path).replace(&files, &ct_cache);

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
