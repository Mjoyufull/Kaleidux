use std::path::{Path, PathBuf};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use walkdir::WalkDir;
use rand::Rng;
use chrono::{DateTime, Utc};
use anyhow::{Result, Context};

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
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContentType {
    Image,
    Video,
}

impl SmartQueue {
    pub fn new(path: &Path, video_ratio: u8, strategy: crate::orchestration::SortingStrategy) -> Result<Self> {
        let stats = Self::load_stats()?;
        let mut pool = Self::discover_content(path, &stats.blacklist)?;
        
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

    fn discover_content(path: &Path, blacklist: &std::collections::HashSet<PathBuf>) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        for entry in WalkDir::new(path).follow_links(true).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let p = entry.path().to_path_buf();
                if blacklist.contains(&p) { continue; }
                if Self::get_content_type(&p).is_some() {
                    files.push(p);
                }
            }
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
        let stat = self.stats.files.entry(path.clone()).or_insert_with(|| FileStats {
            count: 0,
            last_seen: None,
            love_multiplier: 1.0,
        });
        stat.count += 1;
        stat.last_seen = Some(Utc::now());

        // Limit stats growth (LRU-ish removal)
        if self.stats.files.len() > 5000 {
            let oldest = self.stats.files.iter()
                .min_by_key(|(_, s)| s.last_seen.map(|d| d.timestamp()).unwrap_or(0))
                .map(|(p, _)| p.clone());
            if let Some(p) = oldest {
                self.stats.files.remove(&p);
            }
        }
        
        let _ = self.save_stats();
    }

    fn load_stats() -> Result<LoveitData> {
        let path = Self::stats_path()?;
        if !path.exists() {
            return Ok(LoveitData::default());
        }
        let content = std::fs::read_to_string(&path)?;
        match serde_json::from_str(&content) {
            Ok(data) => Ok(data),
            Err(e) => {
                tracing::warn!("Failed to parse statistics file {}: {}. Using empty stats.", path.display(), e);
                Ok(LoveitData::default())
            }
        }
    }

    pub fn save_stats(&self) -> Result<()> {
        let path = Self::stats_path()?;
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let content = serde_json::to_string_pretty(&self.stats)?;
        std::fs::write(&path, content)?;
        Ok(())
    }

    fn stats_path() -> Result<PathBuf> {
        Ok(dirs::state_dir()
            .context("Failed to get state directory")?
            .join("kaleidux")
            .join("loveit.json"))
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
            self.pool = Self::discover_content(&self.root_path, &self.stats.blacklist)?;
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
