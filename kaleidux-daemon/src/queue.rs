use crate::cache::FileCache;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

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
    planned_sequential_type: Option<ContentType>,
    pub history: VecDeque<PathBuf>,
    pub root_path: PathBuf,
    pub active_playlist: Option<String>,
    pub cache: Arc<FileCache>,
    pending_stats_updates: HashMap<PathBuf, FileStats>,
    // In-memory cache of content types to avoid file I/O on every pick (P-01)
    content_type_cache: HashMap<PathBuf, ContentType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Image,
    Video,
}

impl ContentType {
    fn other(self) -> Self {
        match self {
            Self::Image => Self::Video,
            Self::Video => Self::Image,
        }
    }
}

mod discovery;
mod picking;
mod pool_events;
mod stats;

#[cfg(test)]
#[path = "queue/tests.rs"]
mod tests;
