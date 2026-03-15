use crate::cache::FileCache;
use anyhow::Result;
use chrono::{DateTime, Utc};
use jwalk::WalkDir;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Stats capacity for LRU eviction — entries beyond this are auto-evicted
const STATS_LRU_CAP: usize = 5000;

#[derive(Debug)]
pub struct LoveitData {
    pub files: lru::LruCache<PathBuf, FileStats>,
    pub playlists: HashMap<String, Playlist>,
    pub blacklist: std::collections::HashSet<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Playlist {
    pub paths: Vec<PathBuf>,
    pub strategy: crate::orchestration::SortingStrategy,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileStats {
    pub count: u32,
    pub last_seen: Option<DateTime<Utc>>,
    pub love_multiplier: f32, // 1.0 = normal, 2.0 = double chance, etc.
}

pub struct SmartQueue {
    pub pool: Vec<PathBuf>,
    pub stats: LoveitData,
    pub video_ratio: u8,
    pub strategy: crate::orchestration::SortingStrategy,
    pub current_index: usize,
    pub history: VecDeque<PathBuf>,
    pub root_path: PathBuf,
    pub active_playlist: Option<String>,
    pub cache: Arc<FileCache>,
    pending_stats_updates: HashMap<PathBuf, FileStats>,
    // In-memory cache of content types to avoid file I/O on every pick (P-01)
    content_type_cache: HashMap<PathBuf, ContentType>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContentType {
    Image,
    Video,
}

impl SmartQueue {
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

        // Try loading cached pool from redb first (near-instant)
        let mut ct_cache_init: Option<HashMap<PathBuf, ContentType>>;
        let pool = match cache.get_cached_pool(path) {
            Ok(Some(cached_pool)) => {
                // Validate cached pool: filter out files that no longer exist
                let valid_pool = {
                    let cp = cached_pool.clone();
                    let bl = stats.blacklist.clone();
                    tokio::task::spawn_blocking(move || {
                        cp.into_iter()
                            .filter(|p| p.exists() && !bl.contains(p))
                            .collect::<Vec<PathBuf>>()
                    })
                    .await?
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
                    let (tx, _rx) = tokio::sync::mpsc::channel(1);

                    tokio::task::spawn_blocking(move || {
                        match Self::discover_content(&bg_path, &bg_blacklist, bg_cache, bg_metrics)
                        {
                            Ok(res) => {
                                let _ = tx.blocking_send(res);
                            }
                            Err(e) => {
                                tracing::warn!("[QUEUE] Background pool refresh failed: {}", e);
                            }
                        }
                    });

                    // We wrap the receiver so we can periodically check it or use it in an event loop
                    // But for the initial creation, we can't easily block here if we want to stay async.
                    // Instead, idk we'll let the monitor_manager handle the swap when it receives the update.
                    // For now, let's at least populate the initial cache from disk to avoid the I/O bottleneck.

                    let mut init_map = HashMap::new();
                    for path in &valid_pool {
                        if let Ok(Some(meta)) = cache.get_file_metadata(path) {
                            let ct = if meta.content_type == 0 {
                                ContentType::Image
                            } else {
                                ContentType::Video
                            };
                            init_map.insert(path.clone(), ct);
                        }
                    }
                    ct_cache_init = Some(init_map);

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

        let current_index = if strategy == crate::orchestration::SortingStrategy::Descending {
            pool.len().saturating_sub(1)
        } else {
            0
        };

        Ok(Self {
            pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            history: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            content_type_cache: ct_cache_init.unwrap_or_default(),
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
        tokio::task::spawn_blocking(move || {
            Self::discover_content(&path_buf, &blacklist_clone, cache, metrics)
        })
        .await?
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

        let current_index = if strategy == crate::orchestration::SortingStrategy::Descending {
            pool.len().saturating_sub(1)
        } else {
            0
        };

        Ok(Self {
            pool,
            stats,
            video_ratio,
            strategy,
            current_index,
            history: VecDeque::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
            pending_stats_updates: HashMap::new(),
            content_type_cache: HashMap::new(),
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

    fn discover_content(
        path: &Path,
        blacklist: &std::collections::HashSet<PathBuf>,
        cache: Arc<FileCache>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> Result<(Vec<PathBuf>, HashMap<PathBuf, ContentType>)> {
        let discovery_start = std::time::Instant::now();
        let mut files = Vec::new();
        let mut ct_cache: HashMap<PathBuf, ContentType> = HashMap::new();
        let mut cache_updates: Vec<(PathBuf, crate::cache::FileMetadata)> = Vec::new();

        // Use jwalk for parallel directory traversal
        let walk_dir = WalkDir::new(path)
            .follow_links(true)
            .parallelism(jwalk::Parallelism::RayonNewPool(0));

        // Collect entries in parallel
        let entries: Vec<_> = walk_dir
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .collect();

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        for entry in entries {
            let p = entry.path().to_path_buf();
            if blacklist.contains(&p) {
                continue;
            }

            // Single fs::metadata call per file — reused for both validation and cache update
            let fs_meta = match std::fs::metadata(&p) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let fs_mtime = fs_meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);

            // Check cache: single redb read, inline validity check using fs_mtime
            let (content_type, needs_cache_update) = match cache.get_file_metadata(&p) {
                Ok(Some(cached)) => {
                    if cached.mtime == fs_mtime {
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
                            size: fs_meta.len(),
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

    /// Look up a path's ContentType from the in-memory cache (no file I/O fallback).
    /// All files in the pool are pre-cached during discover_content, so a miss
    /// means the file was added after discovery (handled on next refresh).
    fn cached_content_type(&self, path: &Path) -> Option<ContentType> {
        self.content_type_cache.get(path).copied()
    }

    /// Apply incremental pool events from the filesystem watcher
    pub fn apply_pool_events(&mut self, events: Vec<crate::cache::PoolEvent>) {
        use crate::cache::PoolEvent;

        let mut added = 0usize;
        let mut removed = 0usize;

        for event in events {
            match event {
                PoolEvent::Added(path) => {
                    // Only add if it's a supported media file and not blacklisted
                    if self.stats.blacklist.contains(&path) {
                        continue;
                    }
                    if let Some(ct) = Self::get_content_type(&path) {
                        if !self.pool.contains(&path) {
                            self.pool.push(path.clone());
                            self.content_type_cache.insert(path.clone(), ct);
                            added += 1;

                            // Update cache metadata
                            if let Ok(meta) = std::fs::metadata(&path) {
                                let mtime = meta
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let now_secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let _ = self.cache.set_file_metadata(
                                    &path,
                                    &crate::cache::FileMetadata {
                                        mtime,
                                        size: meta.len(),
                                        content_type: match ct {
                                            ContentType::Image => 0,
                                            ContentType::Video => 1,
                                        },
                                        discovered_at: now_secs,
                                    },
                                );
                            }
                        }
                    }
                }
                PoolEvent::Removed(path) => {
                    let before = self.pool.len();
                    self.pool.retain(|p| p != &path);
                    if self.pool.len() < before {
                        removed += 1;
                        self.content_type_cache.remove(&path);
                        // Clamp current_index if it's now out of bounds
                        if !self.pool.is_empty() {
                            self.current_index = self.current_index.min(self.pool.len() - 1);
                        }
                    }
                }
                PoolEvent::Modified(path) => {
                    // File content may have changed — re-check if it's still valid media
                    if let Some(ct) = Self::get_content_type(&path) {
                        self.content_type_cache.insert(path.clone(), ct);
                        // Still valid, cache metadata was already invalidated by the watcher
                        if let Ok(meta) = std::fs::metadata(&path) {
                            let mtime = meta
                                .modified()
                                .ok()
                                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let now_secs = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let _ = self.cache.set_file_metadata(
                                &path,
                                &crate::cache::FileMetadata {
                                    mtime,
                                    size: meta.len(),
                                    content_type: match ct {
                                        ContentType::Image => 0,
                                        ContentType::Video => 1,
                                    },
                                    discovered_at: now_secs,
                                },
                            );
                        }
                    } else {
                        // No longer valid media, remove from pool
                        self.pool.retain(|p| p != &path);
                        self.content_type_cache.remove(&path);
                        removed += 1;

                        // Clamp index to avoid panics if we removed the last item
                        self.current_index =
                            self.current_index.min(self.pool.len().saturating_sub(1));
                    }
                }
            }
        }

        if added > 0 || removed > 0 {
            self.pool.sort();
            // Update the cached pool
            let _ = self.cache.set_cached_pool(&self.root_path, &self.pool);
            tracing::info!(
                "[QUEUE] Pool updated: +{} added, -{} removed, {} total",
                added,
                removed,
                self.pool.len()
            );
        }
    }

    #[inline]
    pub fn pick_next(&mut self) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let picked = match self.strategy {
            crate::orchestration::SortingStrategy::Loveit => self.pick_loveit(),
            crate::orchestration::SortingStrategy::Random => self.pick_random(),
            crate::orchestration::SortingStrategy::Ascending => self.pick_sequential(false),
            crate::orchestration::SortingStrategy::Descending => self.pick_sequential(true),
        };

        if let Some(ref p) = picked {
            self.update_stats(p);
            // Add to history (limit to 50 items)
            if self.history.back() != Some(p) {
                self.history.push_back(p.clone());
                if self.history.len() > 50 {
                    self.history.pop_front();
                }
            }
        }

        picked
    }

    /// Get the next content path without consuming it (for pre-buffering)
    pub fn peek_next(&self) -> Option<(PathBuf, ContentType)> {
        // For sequential strategies, we can peek at the next index
        match self.strategy {
            crate::orchestration::SortingStrategy::Ascending
            | crate::orchestration::SortingStrategy::Descending => {
                let next_idx = match self.strategy {
                    crate::orchestration::SortingStrategy::Ascending => {
                        (self.current_index + 1) % self.pool.len()
                    }
                    crate::orchestration::SortingStrategy::Descending => {
                        if self.current_index == 0 {
                            self.pool.len().saturating_sub(1)
                        } else {
                            self.current_index - 1
                        }
                    }
                    _ => return None,
                };

                if next_idx < self.pool.len() {
                    let path = self.pool[next_idx].clone();
                    if let Some(content_type) = Self::get_content_type(&path) {
                        return Some((path, content_type));
                    }
                }
            }
            _ => {
                // For Random/Loveit, can't predict next easily without pre-rolling
            }
        }
        None
    }

    #[inline]
    pub fn pick_prev(&mut self) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let picked = match self.strategy {
            crate::orchestration::SortingStrategy::Ascending
            | crate::orchestration::SortingStrategy::Descending => {
                let descending = matches!(
                    self.strategy,
                    crate::orchestration::SortingStrategy::Descending
                );
                self.pick_sequential(!descending) // Reversed
            }
            _ => {
                // For non-sequential, use history
                if self.history.len() > 1 {
                    self.history.pop_back(); // Remove current
                    self.history.back().cloned()
                } else {
                    None
                }
            }
        };

        if let Some(ref p) = picked {
            self.update_stats(p);
        }

        picked
    }

    fn pick_random(&mut self) -> Option<PathBuf> {
        let mut rng = rand::thread_rng();
        let is_video_cycle = rng.gen_range(0..100) < self.video_ratio;

        let sub_pool: Vec<&PathBuf> = self
            .pool
            .iter()
            .filter(|p| {
                let is_video = matches!(self.cached_content_type(p), Some(ContentType::Video));
                is_video == is_video_cycle
            })
            .collect();

        let active_pool = if sub_pool.is_empty() {
            self.pool.iter().collect::<Vec<_>>()
        } else {
            sub_pool
        };

        let idx = rng.gen_range(0..active_pool.len());
        Some(active_pool[idx].clone())
    }

    fn pick_sequential(&mut self, descending: bool) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let pool_len = self.pool.len();
        let picked = self.pool[self.current_index].clone();

        if descending {
            if self.current_index == 0 {
                self.current_index = pool_len - 1;
            } else {
                self.current_index -= 1;
            }
        } else {
            self.current_index = (self.current_index + 1) % pool_len;
        }

        Some(picked)
    }

    fn pick_loveit(&mut self) -> Option<PathBuf> {
        let mut rng = rand::thread_rng();

        // 1. Filter by video_ratio probability
        let is_video_cycle = rng.gen_range(0..100) < self.video_ratio;

        let sub_pool: Vec<&PathBuf> = self
            .pool
            .iter()
            .filter(|p| {
                let content_type = self.cached_content_type(p);
                let is_video = matches!(content_type, Some(ContentType::Video));
                is_video == is_video_cycle
            })
            .collect();

        // Fallback if sub_pool is empty
        let active_pool = if sub_pool.is_empty() {
            self.pool.iter().collect::<Vec<_>>()
        } else {
            sub_pool
        };

        // 2. Weighted Random Selection (Loveit + Recency)
        let mut weights = Vec::new();
        let now = Utc::now();

        for path in &active_pool {
            let stat = self.stats.files.peek(*path).cloned().unwrap_or_default();

            // Score = LoveMultiplier / (1 + Count) * RecencyFactor
            let count_score = 100.0 / (stat.count as f32 + 1.0);

            let recency_factor = if let Some(last) = stat.last_seen {
                let hours_since = (now - last).num_hours() as f32;
                // Favor items not seen in a long time
                (hours_since / 24.0).clamp(1.0, 10.0)
            } else {
                10.0 // Never seen is high priority
            };

            let love_weight = if stat.love_multiplier > 0.0 {
                stat.love_multiplier
            } else {
                1.0
            };

            let weight = count_score * recency_factor * love_weight;
            weights.push(weight);
        }

        let total_weight: f32 = weights.iter().sum();
        let mut choice = rng.gen_range(0.0..total_weight);

        for (i, weight) in weights.iter().enumerate() {
            choice -= weight;
            if choice <= 0.0 {
                return Some(active_pool[i].clone());
            }
        }

        Some(active_pool[0].clone())
    }

    fn update_stats(&mut self, path: &Path) {
        let now = Utc::now();
        // get_or_insert_mut promotes existing entries and auto-evicts oldest when at capacity
        let stat = self
            .stats
            .files
            .get_or_insert_mut(path.to_path_buf(), || FileStats {
                count: 0,
                last_seen: None,
                love_multiplier: 1.0,
            });
        stat.count += 1;
        stat.last_seen = Some(now);

        // Add to pending updates for batched write
        self.pending_stats_updates
            .insert(path.to_path_buf(), stat.clone());
        // No manual eviction needed — LruCache handles it automatically
    }

    /// Flush pending stats updates to cache in a batch
    pub fn flush_stats(&mut self) -> Result<()> {
        if self.pending_stats_updates.is_empty() {
            return Ok(());
        }

        let updates: Vec<_> = self.pending_stats_updates.drain().collect();

        self.cache.batch_set_file_stats(&updates)?;
        Ok(())
    }

    fn load_stats_from_cache(cache: &FileCache) -> Result<LoveitData> {
        // Load from redb cache
        let files_map = cache.get_all_file_stats()?;
        let playlists = cache.get_all_playlists()?;
        let blacklist = cache.get_all_blacklisted()?;

        // Build LruCache from loaded HashMap
        let cap = NonZeroUsize::new(STATS_LRU_CAP).unwrap();
        let mut files = lru::LruCache::new(cap);
        for (path, stat) in files_map {
            files.put(path, stat);
        }

        Ok(LoveitData {
            files,
            playlists,
            blacklist,
        })
    }

    pub fn save_stats(&mut self) -> Result<()> {
        // Flush any pending stats updates first
        self.flush_stats()?;

        // Save playlists
        for (name, playlist) in &self.stats.playlists {
            let _ = self.cache.set_playlist(name, playlist);
        }

        // Save blacklist
        for path in &self.stats.blacklist {
            let _ = self.cache.set_blacklisted(path, true);
        }

        Ok(())
    }

    pub fn love_file(&mut self, path: PathBuf, multiplier: f32) -> Result<()> {
        let stat = self
            .stats
            .files
            .get_or_insert_mut(path, || FileStats::default());
        stat.love_multiplier = multiplier;
        self.save_stats()
    }

    pub fn set_playlist(&mut self, name: Option<String>) -> Result<()> {
        if let Some(ref n) = name {
            if let Some(playlist) = self.stats.playlists.get(n) {
                if !playlist.enabled {
                    anyhow::bail!("Playlist '{}' is disabled", n);
                }
                // Filter playlist paths against blacklist
                self.pool = playlist
                    .paths
                    .iter()
                    .filter(|p| !self.stats.blacklist.contains(*p))
                    .cloned()
                    .collect();
                // If playlist has a strategy, use it? Or keep global?
                // For now, let's stick to global strategy unless we want to override it.
            } else {
                anyhow::bail!("Playlist '{}' not found", n);
            }
        } else {
            // Reset to full discovery (no metrics available in this context)
            let (pool, ct_cache) = Self::discover_content(
                &self.root_path,
                &self.stats.blacklist,
                self.cache.clone(),
                None,
            )?;
            self.pool = pool;
            self.content_type_cache = ct_cache;
        }

        self.active_playlist = name;
        self.pool.sort(); // Always sort generic pool
        self.current_index = 0; // Reset index
        Ok(())
    }

    pub fn blacklist_file(&mut self, path: PathBuf) -> Result<()> {
        self.stats.blacklist.insert(path.clone());
        self.pool.retain(|p| p != &path);
        self.save_stats()
    }

    pub fn unblacklist_file(&mut self, path: PathBuf) -> Result<()> {
        if self.stats.blacklist.remove(&path) {
            // If we are currently in "All" mode (no playlist), add it back if it exists in root
            // If we are in a playlist, add it back if it's in the playlist
            // Simplest way is just to reload the current playlist/root
            self.set_playlist(self.active_playlist.clone())?;
            self.save_stats()?;
        }
        Ok(())
    }
}
