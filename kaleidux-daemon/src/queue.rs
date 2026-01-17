use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use jwalk::WalkDir;
use rand::Rng;
use chrono::{DateTime, Utc};
use anyhow::Result;
use std::sync::Arc;
use crate::cache::FileCache;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoveitData {
    pub files: HashMap<PathBuf, FileStats>,
    #[serde(default)]
    pub playlists: HashMap<String, Playlist>,
    #[serde(default)]
    pub blacklist: std::collections::HashSet<PathBuf>,
}

impl Default for LoveitData {
    fn default() -> Self {
        Self {
            files: HashMap::new(),
            playlists: HashMap::new(),
            blacklist: std::collections::HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Playlist {
    pub paths: Vec<PathBuf>,
    pub strategy: crate::orchestration::SortingStrategy,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool { true }

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
    pub history: Vec<PathBuf>,
    pub root_path: PathBuf,
    pub active_playlist: Option<String>,
    pub cache: Arc<FileCache>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContentType {
    Image,
    Video,
}

impl SmartQueue {
    pub fn new(path: &Path, video_ratio: u8, strategy: crate::orchestration::SortingStrategy) -> Result<Self> {
        let cache = Arc::new(FileCache::new()?);
        Self::new_with_cache(path, video_ratio, strategy, cache)
    }
    
    pub fn new_with_cache(path: &Path, video_ratio: u8, strategy: crate::orchestration::SortingStrategy, cache: Arc<FileCache>) -> Result<Self> {
        let stats = Self::load_stats_from_cache(&cache)?;
        
        // Run file discovery in blocking task to avoid blocking main thread
        let path_buf = path.to_path_buf();
        let blacklist_clone = stats.blacklist.clone();
        let cache_clone = cache.clone();
        
        // Use tokio::task::spawn_blocking for CPU-intensive work
        let pool = tokio::task::block_in_place(|| {
            Self::discover_content(&path_buf, &blacklist_clone, cache_clone)
        })?;
        
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
            history: Vec::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
        })
    }
    
    /// Async version that can be spawned in background
    pub async fn new_async(path: &Path, video_ratio: u8, strategy: crate::orchestration::SortingStrategy) -> Result<Self> {
        let cache = Arc::new(FileCache::new()?);
        let stats = Self::load_stats_from_cache(&cache)?;
        
        // Run file discovery in blocking task
        let path_buf = path.to_path_buf();
        let blacklist_clone = stats.blacklist.clone();
        let cache_clone = cache.clone();
        
        let pool = tokio::task::spawn_blocking(move || {
            Self::discover_content(&path_buf, &blacklist_clone, cache_clone)
        }).await??;
        
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
            history: Vec::new(),
            root_path: path.to_path_buf(),
            active_playlist: None,
            cache,
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
        if buffer[0..3] == [0xFF, 0xD8, 0xFF] { return Some(ContentType::Image); }
        // PNG: 89 50 4E 47
        if buffer[0..4] == [0x89, 0x50, 0x4E, 0x47] { return Some(ContentType::Image); }
        // GIF: GIF8
        if buffer[0..4] == *b"GIF8" { return Some(ContentType::Image); }
        // WebP: RIFF .... WEBP
        if &buffer[0..4] == b"RIFF" && &buffer[8..12] == b"WEBP" { return Some(ContentType::Image); }
        // EBML (MKV/WebM): 1A 45 DF A3
        if buffer[0..4] == [0x1A, 0x45, 0xDF, 0xA3] { return Some(ContentType::Video); }
        // MP4: ....ftyp
        if &buffer[4..8] == b"ftyp" { return Some(ContentType::Video); }

        None
    }

    fn discover_content(
        path: &Path, 
        blacklist: &std::collections::HashSet<PathBuf>,
        cache: Arc<FileCache>
    ) -> Result<Vec<PathBuf>> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut files = Vec::new();
        let mut cache_updates = Vec::new();

        // Use jwalk for parallel directory traversal
        let walk_dir = WalkDir::new(path)
            .follow_links(true)
            .parallelism(jwalk::Parallelism::RayonNewPool(0)); // 0 = auto-detect CPU count

        // Collect entries in parallel
        let entries: Vec<_> = walk_dir
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .collect();

        for entry in entries {
            let p = entry.path().to_path_buf();
            if blacklist.contains(&p) { continue; }
            
            // Check cache first
            let content_type = if let Ok(Some(metadata)) = cache.get_file_metadata(&p) {
                // Check if file is still valid (mtime matches)
                if let Ok(valid) = cache.is_file_valid(&p) {
                    if valid {
                        // Use cached content type
                        match metadata.content_type {
                            0 => Some(ContentType::Image),
                            1 => Some(ContentType::Video),
                            _ => None,
                        }
                    } else {
                        // File changed, re-check
                        Self::get_content_type(&p)
                    }
                } else {
                    Self::get_content_type(&p)
                }
            } else {
                // Not in cache, check and cache it
                Self::get_content_type(&p)
            };

            if let Some(ct) = content_type {
                files.push(p.clone());
                
                // Update cache with file metadata
                if let Ok(metadata) = std::fs::metadata(&p) {
                    if let Ok(mtime) = metadata.modified()
                        .and_then(|t| t.duration_since(UNIX_EPOCH).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
                        .map(|d| d.as_secs())
                    {
                        let size = metadata.len();
                        if let Ok(discovered_at) = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()) {
                            let file_metadata = crate::cache::FileMetadata {
                                mtime,
                                size,
                                content_type: match ct {
                                    ContentType::Image => 0,
                                    ContentType::Video => 1,
                                },
                                discovered_at,
                            };
                            cache_updates.push((p, file_metadata));
                        }
                    }
                }
            }
        }

        // Batch update cache
        for (path, metadata) in cache_updates {
            let _ = cache.set_file_metadata(&path, &metadata);
        }

        if files.is_empty() {
            anyhow::bail!("No supported images or videos found in {:?}", path);
        }

        Ok(files)
    }

    #[inline]
    pub fn pick_next(&mut self) -> Option<PathBuf> {
        if self.pool.is_empty() { return None; }

        let picked = match self.strategy {
            crate::orchestration::SortingStrategy::Loveit => self.pick_loveit(),
            crate::orchestration::SortingStrategy::Random => self.pick_random(),
            crate::orchestration::SortingStrategy::Ascending => self.pick_sequential(false),
            crate::orchestration::SortingStrategy::Descending => self.pick_sequential(true),
        };

        if let Some(ref p) = picked {
            self.update_stats(p);
            // Add to history (limit to 50 items)
            if self.history.last() != Some(p) {
                self.history.push(p.clone());
                if self.history.len() > 50 {
                    self.history.remove(0);
                }
            }
        }

        picked
    }

    #[inline]
    pub fn pick_prev(&mut self) -> Option<PathBuf> {
        if self.pool.is_empty() { return None; }

        let picked = match self.strategy {
            crate::orchestration::SortingStrategy::Ascending | crate::orchestration::SortingStrategy::Descending => {
                let descending = matches!(self.strategy, crate::orchestration::SortingStrategy::Descending);
                self.pick_sequential(!descending) // Reversed
            },
            _ => {
                // For non-sequential, use history
                if self.history.len() > 1 {
                    self.history.pop(); // Remove current
                    self.history.last().cloned()
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
        
        let sub_pool: Vec<&PathBuf> = self.pool.iter().filter(|p| {
            let is_video = matches!(Self::get_content_type(p), Some(ContentType::Video));
            is_video == is_video_cycle
        }).collect();

        let active_pool = if sub_pool.is_empty() {
            self.pool.iter().collect::<Vec<_>>()
        } else {
            sub_pool
        };
        
        let idx = rng.gen_range(0..active_pool.len());
        Some(active_pool[idx].clone())
    }

    fn pick_sequential(&mut self, descending: bool) -> Option<PathBuf> {
        if self.pool.is_empty() { return None; }
        
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
        
        let sub_pool: Vec<&PathBuf> = self.pool.iter().filter(|p| {
            let content_type = Self::get_content_type(p);
            let is_video = matches!(content_type, Some(ContentType::Video));
            is_video == is_video_cycle
        }).collect();

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
            let stat = self.stats.files.get(*path).cloned().unwrap_or_default();
            
            // Score = LoveMultiplier / (1 + Count) * RecencyFactor
            let count_score = 100.0 / (stat.count as f32 + 1.0);
            
            let recency_factor = if let Some(last) = stat.last_seen {
                let hours_since = (now - last).num_hours() as f32;
                // Favor items not seen in a long time
                (hours_since / 24.0).min(10.0).max(1.0)
            } else {
                10.0 // Never seen is high priority
            };

            let love_weight = if stat.love_multiplier > 0.0 { stat.love_multiplier } else { 1.0 };
            
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

    fn update_stats(&mut self, path: &PathBuf) {
        // Update the stat
        {
            let stat = self.stats.files.entry(path.clone()).or_insert_with(|| FileStats {
                count: 0,
                last_seen: None,
                love_multiplier: 1.0,
            });
            stat.count += 1;
            stat.last_seen = Some(Utc::now());
        }
        
        // Limit stats growth (LRU-ish removal) - done after dropping the borrow
        if self.stats.files.len() > 5000 {
            let oldest = self.stats.files.iter()
                .min_by_key(|(_, s)| s.last_seen.map(|d| d.timestamp()).unwrap_or(0))
                .map(|(p, _)| p.clone());
            if let Some(p) = oldest {
                self.stats.files.remove(&p);
            }
        }
        
        // Save to redb cache (non-blocking, batched)
        if let Some(stat) = self.stats.files.get(path) {
            let _ = self.cache.set_file_stats(path, stat);
        }
    }

    fn load_stats_from_cache(cache: &FileCache) -> Result<LoveitData> {
        // Load from redb cache
        let files = cache.get_all_file_stats()?;
        let playlists = cache.get_all_playlists()?;
        let blacklist = cache.get_all_blacklisted()?;
        
        Ok(LoveitData {
            files,
            playlists,
            blacklist,
        })
    }

    pub fn save_stats(&self) -> Result<()> {
        // Batch save all stats to redb
        let updates: Vec<_> = self.stats.files.iter()
            .map(|(path, stats)| (path.clone(), stats.clone()))
            .collect();
        self.cache.batch_set_file_stats(&updates)?;
        
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
        let stat = self.stats.files.entry(path).or_default();
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
                self.pool = playlist.paths.iter()
                    .filter(|p| !self.stats.blacklist.contains(*p))
                    .cloned()
                    .collect();
                // If playlist has a strategy, use it? Or keep global?
                // For now, let's stick to global strategy unless we want to override it.
            } else {
                anyhow::bail!("Playlist '{}' not found", n);
            }
        } else {
            // Reset to full discovery
            self.pool = Self::discover_content(&self.root_path, &self.stats.blacklist, self.cache.clone())?;
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
