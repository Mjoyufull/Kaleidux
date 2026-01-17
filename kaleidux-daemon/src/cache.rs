use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};
use redb::{Database, ReadableTable, TableDefinition};
use notify::{Watcher, RecommendedWatcher, RecursiveMode, Event, EventKind};
use std::sync::Arc;
use tokio::sync::mpsc;

// Table definitions for redb
const FILE_CACHE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_cache");
const FILE_STATS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_stats");
const PLAYLISTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("playlists");
const BLACKLIST_TABLE: TableDefinition<&[u8], bool> = TableDefinition::new("blacklist");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub mtime: u64, // Unix timestamp
    pub size: u64,
    pub content_type: u8, // 0 = Image, 1 = Video
    pub discovered_at: u64, // Unix timestamp
}

pub struct FileCache {
    db: Database,
}

impl FileCache {
    pub fn new() -> Result<Self> {
        let cache_dir = dirs::cache_dir()
            .context("Failed to get cache directory")?
            .join("kaleidux");
        std::fs::create_dir_all(&cache_dir)?;
        
        let db_path = cache_dir.join("cache.redb");
        let db = Database::create(&db_path)?;
        
        // Initialize tables
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(FILE_CACHE_TABLE)?;
            let _ = write_txn.open_table(FILE_STATS_TABLE)?;
            let _ = write_txn.open_table(PLAYLISTS_TABLE)?;
            let _ = write_txn.open_table(BLACKLIST_TABLE)?;
        }
        write_txn.commit()?;
        
        Ok(Self { db })
    }

    pub fn get_file_metadata(&self, path: &Path) -> Result<Option<FileMetadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_CACHE_TABLE)?;
        
        let path_str = path.to_string_lossy();
        let path_bytes = path_str.as_bytes();
        if let Some(data) = table.get(path_bytes)? {
            let metadata: FileMetadata = bincode::deserialize(data.value())?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    pub fn set_file_metadata(&self, path: &Path, metadata: &FileMetadata) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            let path_str = path.to_string_lossy();
            let path_bytes = path_str.as_bytes();
            let data = bincode::serialize(metadata)?;
            table.insert(path_bytes, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn is_file_valid(&self, path: &Path) -> Result<bool> {
        let metadata = std::fs::metadata(path)?;
        let mtime = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs();
        
        if let Some(cached) = self.get_file_metadata(path)? {
            Ok(cached.mtime == mtime)
        } else {
            Ok(false)
        }
    }

    pub fn get_file_stats(&self, path: &Path) -> Result<Option<crate::queue::FileStats>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_STATS_TABLE)?;
        
        let path_str = path.to_string_lossy();
        let path_bytes = path_str.as_bytes();
        if let Some(data) = table.get(path_bytes)? {
            let stats: crate::queue::FileStats = bincode::deserialize(data.value())?;
            Ok(Some(stats))
        } else {
            Ok(None)
        }
    }

    pub fn set_file_stats(&self, path: &Path, stats: &crate::queue::FileStats) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            let path_str = path.to_string_lossy();
            let path_bytes = path_str.as_bytes();
            let data = bincode::serialize(stats)?;
            table.insert(path_bytes, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn batch_set_file_stats(&self, updates: &[(PathBuf, crate::queue::FileStats)]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            for (path, stats) in updates {
                let path_str = path.to_string_lossy();
                let path_bytes = path_str.as_bytes();
                let data = bincode::serialize(stats)?;
                table.insert(path_bytes, data.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_all_file_stats(&self) -> Result<std::collections::HashMap<PathBuf, crate::queue::FileStats>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_STATS_TABLE)?;
        let mut stats = std::collections::HashMap::new();
        
        for item in table.iter()? {
            let (key, value) = item?;
            let path = PathBuf::from(String::from_utf8_lossy(key.value()).to_string());
            let file_stats: crate::queue::FileStats = bincode::deserialize(value.value())?;
            stats.insert(path, file_stats);
        }
        
        Ok(stats)
    }

    pub fn get_playlist(&self, name: &str) -> Result<Option<crate::queue::Playlist>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PLAYLISTS_TABLE)?;
        
        if let Some(data) = table.get(name)? {
            let playlist: crate::queue::Playlist = bincode::deserialize(data.value())?;
            Ok(Some(playlist))
        } else {
            Ok(None)
        }
    }

    pub fn set_playlist(&self, name: &str, playlist: &crate::queue::Playlist) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PLAYLISTS_TABLE)?;
            let data = bincode::serialize(playlist)?;
            table.insert(name, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_all_playlists(&self) -> Result<std::collections::HashMap<String, crate::queue::Playlist>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PLAYLISTS_TABLE)?;
        let mut playlists = std::collections::HashMap::new();
        
        for item in table.iter()? {
            let (key, value) = item?;
            let name = key.value().to_string();
            let playlist: crate::queue::Playlist = bincode::deserialize(value.value())?;
            playlists.insert(name, playlist);
        }
        
        Ok(playlists)
    }

    pub fn delete_playlist(&self, name: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PLAYLISTS_TABLE)?;
            table.remove(name)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn is_blacklisted(&self, path: &Path) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLACKLIST_TABLE)?;
        
        let path_str = path.to_string_lossy();
        let path_bytes = path_str.as_bytes();
        Ok(table.get(path_bytes)?.is_some())
    }

    pub fn set_blacklisted(&self, path: &Path, blacklisted: bool) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLACKLIST_TABLE)?;
            let path_str = path.to_string_lossy();
            let path_bytes = path_str.as_bytes();
            if blacklisted {
                table.insert(path_bytes, true)?;
            } else {
                table.remove(path_bytes)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_all_blacklisted(&self) -> Result<std::collections::HashSet<PathBuf>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLACKLIST_TABLE)?;
        let mut blacklist = std::collections::HashSet::new();
        
        for item in table.iter()? {
            let (key, _) = item?;
            let path = PathBuf::from(String::from_utf8_lossy(key.value()).to_string());
            blacklist.insert(path);
        }
        
        Ok(blacklist)
    }

    pub fn clear_file_cache(&self) -> Result<()> {
        // Clear cache by removing all entries
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            // Collect all keys first, then remove them
            let keys: Vec<Vec<u8>> = {
                let read_txn = self.db.begin_read()?;
                let read_table = read_txn.open_table(FILE_CACHE_TABLE)?;
                read_table.iter()?
                    .filter_map(|item| {
                        item.ok().map(|(key, _)| key.value().to_vec())
                    })
                    .collect()
            };
            for key in keys {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
    
    /// Invalidate cache entry for a specific file
    pub fn invalidate_file(&self, path: &Path) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            let path_str = path.to_string_lossy();
            let path_bytes = path_str.as_bytes();
            table.remove(path_bytes)?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

/// Directory watcher for cache invalidation
pub struct DirectoryWatcher {
    watcher: RecommendedWatcher,
    event_rx: mpsc::Receiver<notify::Result<Event>>,
    cache: Arc<FileCache>,
    watched_dirs: Vec<PathBuf>,
}

impl DirectoryWatcher {
    pub fn new(cache: Arc<FileCache>) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::channel(100);
        
        let watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            let _ = event_tx.blocking_send(res);
        })?;
        
        Ok(Self {
            watcher,
            event_rx,
            cache,
            watched_dirs: Vec::new(),
        })
    }
    
    /// Watch a directory for changes
    pub fn watch(&mut self, path: &Path) -> Result<()> {
        if path.exists() && path.is_dir() {
            self.watcher.watch(path, RecursiveMode::Recursive)?;
            self.watched_dirs.push(path.to_path_buf());
            tracing::info!("[CACHE] Watching directory for changes: {}", path.display());
        }
        Ok(())
    }
    
    /// Process file system events and invalidate cache entries
    pub async fn process_events(&mut self) {
        while let Ok(Ok(event)) = self.event_rx.try_recv() {
            match event.kind {
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_) => {
                    for path in event.paths {
                        if path.is_file() {
                            // Invalidate cache entry for this file
                            if let Err(e) = self.cache.invalidate_file(&path) {
                                tracing::warn!("[CACHE] Failed to invalidate cache for {}: {}", path.display(), e);
                            } else {
                                tracing::debug!("[CACHE] Invalidated cache for: {}", path.display());
                            }
                        } else if path.is_dir() {
                            // Directory changed - mark all files in this directory as dirty
                            // For now, we'll just log it. Full directory invalidation could be added later.
                            tracing::debug!("[CACHE] Directory changed: {}", path.display());
                        }
                    }
                }
                _ => {}
            }
        }
    }
}
