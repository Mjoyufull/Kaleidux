use anyhow::{Context, Result, bail};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

// Table definitions for redb
const FILE_CACHE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_cache");
const FILE_STATS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_stats");
const PLAYLISTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("playlists");
const BLACKLIST_TABLE: TableDefinition<&[u8], bool> = TableDefinition::new("blacklist");
const HISTORY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("history");
const POOL_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pool_cache");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");

const CACHE_VERSION: u64 = 5;

fn path_from_redb_key(key: &[u8]) -> Option<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Some(PathBuf::from(std::ffi::OsStr::from_bytes(key)))
    }
    #[cfg(not(unix))]
    {
        std::str::from_utf8(key).ok().map(PathBuf::from)
    }
}

/// Filesystem events that affect the active file pool
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolEvent {
    /// A new file was created in a watched directory
    Added(PathBuf),
    /// A file was removed from a watched directory
    Removed(PathBuf),
    /// A file was modified in a watched directory
    Modified(PathBuf),
    /// The bounded watcher queue overflowed; rebuild this root from disk.
    Rescan(PathBuf),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub mtime: u64, // Whole Unix timestamp seconds
    pub mtime_nanos: u32,
    pub size: u64,
    pub content_type: u8,   // 0 = Image, 1 = Video
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
        Self::new_in_dir(cache_dir)
    }

    pub fn new_in_dir<P: AsRef<Path>>(cache_dir: P) -> Result<Self> {
        let cache_dir = cache_dir.as_ref();
        std::fs::create_dir_all(cache_dir)?;
        Self::open_at(&cache_dir.join("cache.redb"))
    }

    fn open_at(db_path: &Path) -> Result<Self> {
        let db_preexisting = std::fs::metadata(db_path)
            .map(|meta| meta.len() > 0)
            .unwrap_or(false);
        let mut db = Database::create(db_path)?;

        // Check version
        let mut needs_wipe = false;
        if db_preexisting {
            let read_txn = db.begin_read()?;
            if let Ok(table) = read_txn.open_table(META_TABLE) {
                if let Some(v) = table.get("version")? {
                    if v.value() != CACHE_VERSION {
                        needs_wipe = true;
                    }
                } else {
                    needs_wipe = true;
                }
            } else {
                needs_wipe = true;
            }
        }

        if needs_wipe {
            tracing::info!("[CACHE] Cache version mismatch or missing, wiping database...");
            drop(db);
            match std::fs::remove_file(db_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e).context("Failed to remove stale cache database before recreate");
                }
            }
            if std::fs::metadata(db_path).is_ok() {
                bail!(
                    "Stale cache file {:?} still exists after remove_file; refusing to recreate to avoid corruption",
                    db_path
                );
            }
            db = Database::create(db_path)?;
        }

        // Initialize tables
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(FILE_CACHE_TABLE)?;
            let _ = write_txn.open_table(FILE_STATS_TABLE)?;
            let _ = write_txn.open_table(PLAYLISTS_TABLE)?;
            let _ = write_txn.open_table(BLACKLIST_TABLE)?;
            let _ = write_txn.open_table(HISTORY_TABLE)?;
            let _ = write_txn.open_table(POOL_TABLE)?;
            let mut meta = write_txn.open_table(META_TABLE)?;
            meta.insert("version", CACHE_VERSION)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    #[cfg(test)]
    pub(crate) fn new_test(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self::open_at(db_path)
    }

    #[cfg(test)]
    pub(crate) fn insert_invalid_file_stats_bytes(&self, path: &Path, raw: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            let path_bytes = path.as_os_str().as_encoded_bytes();
            table.insert(path_bytes, raw)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_file_metadata(&self, path: &Path) -> Result<Option<FileMetadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_CACHE_TABLE)?;

        let path_bytes = path.as_os_str().as_encoded_bytes();
        match table.get(path_bytes)? {
            Some(data) => {
                let metadata: FileMetadata = postcard::from_bytes(data.value())?;
                Ok(Some(metadata))
            }
            _ => Ok(None),
        }
    }

    /// Read all requested metadata rows under one redb snapshot transaction.
    /// Missing paths are omitted from the returned map.
    pub fn batch_get_file_metadata(
        &self,
        paths: &[PathBuf],
    ) -> Result<HashMap<PathBuf, FileMetadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_CACHE_TABLE)?;
        let mut metadata = HashMap::with_capacity(paths.len());
        for path in paths {
            let path_bytes = path.as_os_str().as_encoded_bytes();
            if let Some(data) = table.get(path_bytes)? {
                match postcard::from_bytes(data.value()) {
                    Ok(value) => {
                        metadata.insert(path.clone(), value);
                    }
                    Err(error) => tracing::warn!(
                        "[CACHE] Ignoring corrupt metadata row for {}: {error}",
                        path.display()
                    ),
                }
            }
        }
        Ok(metadata)
    }

    pub fn set_file_metadata(&self, path: &Path, metadata: &FileMetadata) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            let path_bytes = path.as_os_str().as_encoded_bytes();
            let data = postcard::to_allocvec(metadata)?;
            table.insert(path_bytes, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Write multiple file metadata entries in a single redb transaction
    pub fn batch_set_file_metadata(&self, updates: &[(PathBuf, FileMetadata)]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            for (path, metadata) in updates {
                let path_bytes = path.as_os_str().as_encoded_bytes();
                let data = postcard::to_allocvec(metadata)?;
                table.insert(path_bytes, data.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Persist a discovered file pool for a directory (keyed by directory path)
    pub fn set_cached_pool(&self, dir: &Path, pool: &[PathBuf]) -> Result<()> {
        let encoded: Vec<Vec<u8>> = pool
            .iter()
            .map(|p| p.as_os_str().as_encoded_bytes().to_vec())
            .collect();
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(POOL_TABLE)?;
            let key = dir.as_os_str().as_encoded_bytes();
            let data = postcard::to_allocvec(&encoded)?;
            table.insert(key, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Load a previously cached file pool for a directory
    pub fn get_cached_pool(&self, dir: &Path) -> Result<Option<Vec<PathBuf>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(POOL_TABLE)?;
        let key = dir.as_os_str().as_encoded_bytes();
        match table.get(key)? {
            Some(data) => {
                let bytes = data.value();
                let paths = if let Ok(encoded) = postcard::from_bytes::<Vec<Vec<u8>>>(bytes) {
                    encoded
                        .into_iter()
                        .filter_map(|b| path_from_redb_key(&b))
                        .collect()
                } else {
                    match postcard::from_bytes::<Vec<PathBuf>>(bytes) {
                        Ok(legacy) => legacy,
                        Err(e) => {
                            tracing::warn!(
                                "[CACHE] Failed to deserialize cached pool entry for {:?} ({} bytes): {}",
                                dir,
                                bytes.len(),
                                e
                            );
                            Vec::new()
                        }
                    }
                };
                Ok(Some(paths))
            }
            _ => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn is_file_valid(&self, path: &Path) -> Result<bool> {
        let metadata = std::fs::metadata(path)?;
        let modified = metadata.modified()?.duration_since(UNIX_EPOCH)?;

        if let Some(cached) = self.get_file_metadata(path)? {
            Ok(cached.mtime == modified.as_secs()
                && cached.mtime_nanos == modified.subsec_nanos()
                && cached.size == metadata.len())
        } else {
            Ok(false)
        }
    }

    #[allow(dead_code)]
    pub fn get_file_stats(&self, path: &Path) -> Result<Option<crate::queue::FileStats>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_STATS_TABLE)?;

        let path_bytes = path.as_os_str().as_encoded_bytes();
        match table.get(path_bytes)? {
            Some(data) => {
                let stats: crate::queue::FileStats = postcard::from_bytes(data.value())?;
                Ok(Some(stats))
            }
            _ => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn set_file_stats(&self, path: &Path, stats: &crate::queue::FileStats) -> Result<()> {
        self.batch_set_file_stats(&[(path.to_path_buf(), stats.clone())])
    }

    pub fn batch_set_file_stats(
        &self,
        updates: &[(PathBuf, crate::queue::FileStats)],
    ) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            for (path, stats) in updates {
                let path_bytes = path.as_os_str().as_encoded_bytes();
                let data = postcard::to_allocvec(stats)?;
                table.insert(path_bytes, data.as_slice())?;
            }
        }
        write_txn.commit()?;
        self.prune_file_stats(crate::queue::STATS_LRU_CAP)?;
        Ok(())
    }

    fn prune_file_stats(&self, limit: usize) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            let len = usize::try_from(table.len()?).unwrap_or(usize::MAX);
            if len > limit {
                let mut rows = Vec::with_capacity(len);
                for item in table.iter()? {
                    let (key, value) = item?;
                    let last_seen = postcard::from_bytes::<crate::queue::FileStats>(value.value())
                        .ok()
                        .and_then(|stats| stats.last_seen)
                        .map_or(i64::MIN, |timestamp| timestamp.timestamp_millis());
                    rows.push((last_seen, key.value().to_vec()));
                }
                rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                for (_, key) in rows.into_iter().take(len.saturating_sub(limit)) {
                    table.remove(key.as_slice())?;
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_all_file_stats(
        &self,
    ) -> Result<std::collections::HashMap<PathBuf, crate::queue::FileStats>> {
        self.prune_file_stats(crate::queue::STATS_LRU_CAP)?;
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_STATS_TABLE)?;
        let mut stats = std::collections::HashMap::new();

        for item in table.iter()? {
            let (key, value) = item?;
            let Some(path) = path_from_redb_key(key.value()) else {
                tracing::warn!("[CACHE] Skipping file_stats row with invalid path encoding");
                continue;
            };
            let file_stats: crate::queue::FileStats = postcard::from_bytes(value.value())?;
            stats.insert(path, file_stats);
        }

        Ok(stats)
    }

    #[allow(dead_code)]
    pub fn get_playlist(&self, name: &str) -> Result<Option<crate::queue::Playlist>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PLAYLISTS_TABLE)?;

        match table.get(name)? {
            Some(data) => {
                let playlist: crate::queue::Playlist = postcard::from_bytes(data.value())?;
                Ok(Some(playlist))
            }
            _ => Ok(None),
        }
    }

    pub fn set_playlist(&self, name: &str, playlist: &crate::queue::Playlist) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PLAYLISTS_TABLE)?;
            let data = postcard::to_allocvec(playlist)?;
            table.insert(name, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_all_playlists(
        &self,
    ) -> Result<std::collections::HashMap<String, crate::queue::Playlist>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PLAYLISTS_TABLE)?;
        let mut playlists = std::collections::HashMap::new();

        for item in table.iter()? {
            let (key, value) = item?;
            let name = key.value().to_string();
            let playlist: crate::queue::Playlist = postcard::from_bytes(value.value())?;
            playlists.insert(name, playlist);
        }

        Ok(playlists)
    }

    #[allow(dead_code)]
    pub fn delete_playlist(&self, name: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PLAYLISTS_TABLE)?;
            table.remove(name)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn is_blacklisted(&self, path: &Path) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLACKLIST_TABLE)?;

        let path_bytes = path.as_os_str().as_encoded_bytes();
        Ok(table.get(path_bytes)?.is_some())
    }

    pub fn set_blacklisted(&self, path: &Path, blacklisted: bool) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLACKLIST_TABLE)?;
            let path_bytes = path.as_os_str().as_encoded_bytes();
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
            let Some(path) = path_from_redb_key(key.value()) else {
                tracing::warn!("[CACHE] Skipping blacklist row with invalid path encoding");
                continue;
            };
            blacklist.insert(path);
        }

        Ok(blacklist)
    }

    #[allow(dead_code)]
    pub fn set_history(&self, output_name: &str, history: &[PathBuf]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(HISTORY_TABLE)?;
            let data = postcard::to_allocvec(&history)?;
            table.insert(output_name, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_history(&self, output_name: &str) -> Result<Vec<PathBuf>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(HISTORY_TABLE)?;
        match table.get(output_name)? {
            Some(data) => {
                let paths: Vec<PathBuf> = postcard::from_bytes(data.value())?;
                Ok(paths)
            }
            _ => Ok(Vec::new()),
        }
    }

    #[allow(dead_code)]
    pub fn clear_file_cache(&self) -> Result<()> {
        // Clear cache atomically using a single write transaction
        // This avoids race conditions where entries added between read and write would be missed
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            // Collect keys within write transaction to ensure atomicity
            let keys: Vec<Vec<u8>> = table
                .iter()?
                .filter_map(|item| item.ok().map(|(key, _)| key.value().to_vec()))
                .collect();
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
            table.remove(path.as_os_str().as_encoded_bytes())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn batch_invalidate_files(&self, paths: &[PathBuf]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_CACHE_TABLE)?;
            for path in paths {
                let path_bytes = path.as_os_str().as_encoded_bytes();
                table.remove(path_bytes)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
}

/// Directory watcher for cache invalidation
mod watcher;
pub use watcher::DirectoryWatcher;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_cache(label: &str) -> FileCache {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kaleidux-redb-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        FileCache::new_test(&dir.join("cache.redb")).expect("test cache")
    }

    #[test]
    fn persisted_file_stats_prune_oldest_rows() {
        let cache = test_cache("stats-bound");
        for second in 1..=3 {
            cache
                .set_file_stats(
                    Path::new(&format!("/media/{second}")),
                    &crate::queue::FileStats {
                        count: second,
                        last_seen: Utc.timestamp_opt(second.into(), 0).single(),
                        love_multiplier: 1.0,
                    },
                )
                .expect("insert stats");
        }
        cache.prune_file_stats(2).expect("prune stats");
        assert!(
            cache
                .get_file_stats(Path::new("/media/1"))
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .get_file_stats(Path::new("/media/2"))
                .unwrap()
                .is_some()
        );
        assert!(
            cache
                .get_file_stats(Path::new("/media/3"))
                .unwrap()
                .is_some()
        );
    }
}
