//! Builds queue state from shared snapshots, persistent pools or full discovery.

use super::{ContentType, SmartQueue};
use crate::background::{self, BackgroundWorkKind};
use crate::cache::FileCache;
use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
        if !shared_snapshot.entries.is_empty() && !shared_index.rescan_running() {
            let blacklist = super::BlacklistIdentity::new(&stats.blacklist);
            let mut pool = Vec::with_capacity(shared_snapshot.entries.len());
            let mut content_type_cache = HashMap::with_capacity(shared_snapshot.entries.len());
            for entry in shared_snapshot.entries.iter() {
                if entry.content_type.supported() && !blacklist.contains(&entry.path) {
                    pool.push(entry.path.clone());
                    content_type_cache.insert(entry.path.clone(), entry.content_type);
                }
            }
            pool.sort();
            drop(blacklist);
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
                forward_history: VecDeque::new(),
                selected_images: VecDeque::new(),
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
                            let bl = super::BlacklistIdentity::new(&bl);
                            cp.into_iter()
                                .filter(|p| {
                                    p.exists()
                                        && !bl.contains(p)
                                        && (ContentType::Video.supported()
                                            || Self::get_content_type(p)
                                                == Some(ContentType::Image))
                                })
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
                            forward_history: VecDeque::new(),
                            selected_images: VecDeque::new(),
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

                    Self::schedule_root_rescan_for(
                        super::media_index::shared_root_index(path),
                        path.to_path_buf(),
                        cache.clone(),
                    );

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
            forward_history: VecDeque::new(),
            selected_images: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            content_type_cache,
            root_index,
            root_generation,
        })
    }

    /// Create a queue from a pre-discovered file list (avoids re-scanning the directory)
    pub fn new_from_pool(
        path: &Path,
        pool: Vec<PathBuf>,
        video_ratio: u8,
        strategy: crate::orchestration::SortingStrategy,
        cache: Arc<FileCache>,
    ) -> Result<Self> {
        Self::from_pool(
            path,
            pool,
            video_ratio,
            strategy,
            cache,
            super::media_index::shared_root_index(path),
        )
    }

    /// Manual selections represent a partial pool and must not seed directory discovery.
    pub(crate) fn new_manual_from_pool(
        path: &Path,
        pool: Vec<PathBuf>,
        video_ratio: u8,
        strategy: crate::orchestration::SortingStrategy,
        cache: Arc<FileCache>,
    ) -> Result<Self> {
        Self::from_pool(
            path,
            pool,
            video_ratio,
            strategy,
            cache,
            super::media_index::RootMediaIndex::local(),
        )
    }

    fn from_pool(
        path: &Path,
        pool: Vec<PathBuf>,
        video_ratio: u8,
        strategy: crate::orchestration::SortingStrategy,
        cache: Arc<FileCache>,
        root_index: Arc<super::media_index::RootMediaIndex>,
    ) -> Result<Self> {
        let stats = Self::load_stats_from_cache(&cache)?;
        let mut pool = pool;
        let blacklist = super::BlacklistIdentity::new(&stats.blacklist);
        pool.retain(|path| {
            !blacklist.contains(path)
                && (ContentType::Video.supported()
                    || Self::get_content_type(path) == Some(ContentType::Image))
        });
        drop(blacklist);
        pool.sort();

        let seeded_types = Self::populate_content_type_cache_from_pool(&cache, &pool);
        let snapshot = root_index.install_if_empty(&pool, &seeded_types);
        // The shared root may have advanced while a cached pool was prepared.
        // Adopt its entries together with its generation, never tag stale data
        // with the newer generation and thereby miss later synchronization.
        let identity = super::BlacklistIdentity::new(&stats.blacklist);
        let content_type_cache: HashMap<_, _> = snapshot
            .entries
            .iter()
            .filter(|entry| entry.content_type.supported() && !identity.contains(&entry.path))
            .map(|entry| (entry.path.clone(), entry.content_type))
            .collect();
        let mut current_pool: Vec<_> = content_type_cache.keys().cloned().collect();
        current_pool.sort();
        let current_index = Self::fallback_current_index(strategy, current_pool.len());
        let root_generation = snapshot.generation;
        Ok(Self {
            content_type_cache,
            pool: current_pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            planned_sequential_type: None,
            history: VecDeque::new(),
            forward_history: VecDeque::new(),
            selected_images: VecDeque::new(),
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
        super::media_probe::probe(path)
    }
}
