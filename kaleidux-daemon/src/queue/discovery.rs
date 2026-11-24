//! Builds queue state from shared snapshots, persistent pools or full discovery.

use super::{ContentType, SmartQueue};
use crate::background::{self, BackgroundWorkKind};
use crate::cache::FileCache;
use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl SmartQueue {
    fn indexed_pool(
        snapshot: &super::media_index::RootMediaSnapshot,
        blacklist: &super::BlacklistIdentity<'_>,
    ) -> (Vec<PathBuf>, HashMap<PathBuf, ContentType>) {
        let mut pool = Vec::with_capacity(snapshot.entries.len());
        let mut content_types = HashMap::with_capacity(snapshot.entries.len());
        for entry in snapshot.entries.iter() {
            if entry.content_type.supported() && !blacklist.contains(&entry.path) {
                pool.push(entry.path.clone());
                content_types.insert(entry.path.clone(), entry.content_type);
            }
        }
        pool.sort_unstable();
        (pool, content_types)
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
        let source = crate::media_source::MediaSource::parse(&path.to_string_lossy())?;
        if !matches!(source, crate::media_source::MediaSource::File(_)) {
            return Self::new_manual_from_pool(
                path,
                vec![path.to_path_buf()],
                video_ratio,
                strategy,
                cache,
            );
        }
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
            let (pool, content_type_cache) = Self::indexed_pool(&shared_snapshot, &blacklist);
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
                excerpt_length_ns: None,
                excerpt_plans: HashMap::new(),
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
                                    (p.exists() || crate::media_source::MediaSource::is_virtual(p))
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
                            excerpt_length_ns: None,
                            excerpt_plans: HashMap::new(),
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
                        shared_index.clone(),
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

        let snapshot = shared_index.install_if_empty(&pool, &ct_cache_init.unwrap_or_default());
        let blacklist = super::BlacklistIdentity::new(&stats.blacklist);
        let (pool, content_type_cache) = Self::indexed_pool(&snapshot, &blacklist);
        let current_index = Self::fallback_current_index(strategy, pool.len());
        Ok(Self {
            pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            planned_sequential_type: None,
            history: VecDeque::new(),
            forward_history: VecDeque::new(),
            excerpt_length_ns: None,
            excerpt_plans: HashMap::new(),
            selected_images: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            content_type_cache,
            root_index: shared_index,
            root_generation: snapshot.generation,
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
        let blacklist = super::BlacklistIdentity::new(&stats.blacklist);
        let (pool, content_type_cache) = Self::indexed_pool(&snapshot, &blacklist);
        let current_index = Self::fallback_current_index(strategy, pool.len());
        Ok(Self {
            content_type_cache,
            pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            planned_sequential_type: None,
            history: VecDeque::new(),
            forward_history: VecDeque::new(),
            excerpt_length_ns: None,
            excerpt_plans: HashMap::new(),
            selected_images: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            root_index,
            root_generation: snapshot.generation,
        })
    }

    #[inline]
    pub fn get_content_type(path: &Path) -> Option<ContentType> {
        match crate::media_source::MediaSource::parse(&path.to_string_lossy()).ok()? {
            crate::media_source::MediaSource::Network(_) => {
                return ContentType::Video.supported().then_some(ContentType::Video);
            }
            crate::media_source::MediaSource::Colour(_) => return Some(ContentType::Image),
            crate::media_source::MediaSource::File(_) => {}
        }
        if crate::renderer::is_shader_file(path) || crate::scene::is_scene(path) {
            return Some(ContentType::Image);
        }
        if crate::video::is_web_path(path) {
            return cfg!(feature = "wallpaper-web").then_some(ContentType::Video);
        }
        super::media_probe::probe(path)
    }
}
