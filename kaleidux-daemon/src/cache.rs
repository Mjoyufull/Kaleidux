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
const STATS_PRUNE_TRIGGER: usize = crate::queue::STATS_LRU_CAP + crate::queue::STATS_LRU_CAP / 10;

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
        let db = Database::create(db_path)?;

        let stored_version = if db_preexisting {
            let read_txn = db.begin_read()?;
            if let Ok(table) = read_txn.open_table(META_TABLE) {
                table.get("version")?.map(|version| version.value())
            } else {
                None
            }
        } else {
            None
        };

        if stored_version.is_some_and(|version| version > CACHE_VERSION) {
            bail!(
                "Cache database version {} is newer than supported version {}",
                stored_version.unwrap_or_default(),
                CACHE_VERSION
            );
        }

        // Schema v5 changed only transient discovery metadata and pool rows. Keep
        // user history, playlists, blacklist entries, and learned file stats.
        let write_txn = db.begin_write()?;
        {
            let mut file_cache = write_txn.open_table(FILE_CACHE_TABLE)?;
            if stored_version != Some(CACHE_VERSION) {
                let keys = file_cache
                    .iter()?
                    .map(|entry| entry.map(|(key, _)| key.value().to_vec()))
                    .collect::<Result<Vec<_>, _>>()?;
                for key in keys {
                    file_cache.remove(key.as_slice())?;
                }
            }
        }
        {
            let _ = write_txn.open_table(FILE_STATS_TABLE)?;
            let _ = write_txn.open_table(PLAYLISTS_TABLE)?;
            let _ = write_txn.open_table(BLACKLIST_TABLE)?;
            let _ = write_txn.open_table(HISTORY_TABLE)?;
        }
        {
            let mut pools = write_txn.open_table(POOL_TABLE)?;
            if stored_version != Some(CACHE_VERSION) {
                let keys = pools
                    .iter()?
                    .map(|entry| entry.map(|(key, _)| key.value().to_vec()))
                    .collect::<Result<Vec<_>, _>>()?;
                for key in keys {
                    pools.remove(key.as_slice())?;
                }
            }
        }
        {
            let mut meta = write_txn.open_table(META_TABLE)?;
            meta.insert("version", CACHE_VERSION)?;
        }
        write_txn.commit()?;

        if stored_version != Some(CACHE_VERSION) && db_preexisting {
            tracing::info!(
                "[CACHE] Migrated cache schema from {:?} to {} while preserving durable user data",
                stored_version,
                CACHE_VERSION
            );
        }

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
        let decoded = table
            .get(path_bytes)?
            .map(|data| postcard::from_bytes(data.value()))
            .transpose();
        drop(table);
        drop(read_txn);

        match decoded {
            Ok(stats) => Ok(stats),
            Err(error) => {
                tracing::warn!(
                    "[CACHE] Removing corrupt file_stats row for {}: {error}",
                    path.display()
                );
                let write_txn = self.db.begin_write()?;
                {
                    let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
                    table.remove(path_bytes)?;
                }
                write_txn.commit()?;
                Ok(None)
            }
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
        if updates.is_empty() {
            return Ok(());
        }
        let write_txn = self.db.begin_write()?;
        let should_prune = {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            for (path, stats) in updates {
                let path_bytes = path.as_os_str().as_encoded_bytes();
                let data = postcard::to_allocvec(stats)?;
                table.insert(path_bytes, data.as_slice())?;
            }
            usize::try_from(table.len()?).unwrap_or(usize::MAX) > STATS_PRUNE_TRIGGER
        };
        write_txn.commit()?;
        if should_prune {
            self.prune_file_stats(crate::queue::STATS_LRU_CAP)?;
        }
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
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILE_STATS_TABLE)?;
        let mut stats = std::collections::HashMap::new();
        let mut corrupt_keys = Vec::new();

        for item in table.iter()? {
            let (key, value) = item?;
            let Some(path) = path_from_redb_key(key.value()) else {
                tracing::warn!("[CACHE] Skipping file_stats row with invalid path encoding");
                corrupt_keys.push(key.value().to_vec());
                continue;
            };
            match postcard::from_bytes(value.value()) {
                Ok(file_stats) => {
                    stats.insert(path, file_stats);
                }
                Err(error) => {
                    tracing::warn!(
                        "[CACHE] Removing corrupt file_stats row for {}: {error}",
                        path.display()
                    );
                    corrupt_keys.push(key.value().to_vec());
                }
            }
        }
        drop(table);
        drop(read_txn);

        if !corrupt_keys.is_empty() {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
                for key in corrupt_keys {
                    table.remove(key.as_slice())?;
                }
            }
            write_txn.commit()?;
        }

        Ok(stats)
    }

    #[allow(dead_code)]
    pub fn get_playlist(&self, name: &str) -> Result<Option<crate::queue::Playlist>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PLAYLISTS_TABLE)?;
        let decoded = match table.get(name)? {
            Some(data) => postcard::from_bytes(data.value()).map(Some),
            None => Ok(None),
        };
        drop(table);
        drop(read_txn);

        match decoded {
            Ok(playlist) => Ok(playlist),
            Err(error) => {
                tracing::warn!("[CACHE] Removing corrupt playlist row {name:?}: {error}");
                let write_txn = self.db.begin_write()?;
                {
                    let mut table = write_txn.open_table(PLAYLISTS_TABLE)?;
                    table.remove(name)?;
                }
                write_txn.commit()?;
                Ok(None)
            }
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
        let mut corrupt_names = Vec::new();

        for item in table.iter()? {
            let (key, value) = item?;
            let name = key.value().to_string();
            match postcard::from_bytes(value.value()) {
                Ok(playlist) => {
                    playlists.insert(name, playlist);
                }
                Err(error) => {
                    tracing::warn!("[CACHE] Removing corrupt playlist row {name:?}: {error}");
                    corrupt_names.push(name);
                }
            }
        }
        drop(table);
        drop(read_txn);

        if !corrupt_names.is_empty() {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(PLAYLISTS_TABLE)?;
                for name in corrupt_names {
                    table.remove(name.as_str())?;
                }
            }
            write_txn.commit()?;
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
        let decoded = match table.get(output_name)? {
            Some(data) => postcard::from_bytes(data.value()),
            None => Ok(Vec::new()),
        };
        drop(table);
        drop(read_txn);

        match decoded {
            Ok(paths) => Ok(paths),
            Err(error) => {
                tracing::warn!("[CACHE] Removing corrupt history row {output_name:?}: {error}");
                let write_txn = self.db.begin_write()?;
                {
                    let mut table = write_txn.open_table(HISTORY_TABLE)?;
                    table.remove(output_name)?;
                }
                write_txn.commit()?;
                Ok(Vec::new())
            }
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

    fn test_cache_path(label: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(10_000);
        std::env::temp_dir()
            .join(format!(
                "kaleidux-redb-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .join("cache.redb")
    }

    #[test]
    fn schema_migration_preserves_durable_user_tables() {
        let db_path = test_cache_path("migration");
        std::fs::create_dir_all(db_path.parent().expect("cache parent")).unwrap();
        let db = Database::create(&db_path).expect("create old cache");
        let write_txn = db.begin_write().unwrap();
        {
            write_txn
                .open_table(FILE_CACHE_TABLE)
                .unwrap()
                .insert(b"/media/a".as_slice(), b"metadata".as_slice())
                .unwrap();
            write_txn
                .open_table(POOL_TABLE)
                .unwrap()
                .insert(b"/media".as_slice(), b"pool".as_slice())
                .unwrap();
            write_txn
                .open_table(FILE_STATS_TABLE)
                .unwrap()
                .insert(b"/media/a".as_slice(), b"stats".as_slice())
                .unwrap();
            write_txn
                .open_table(PLAYLISTS_TABLE)
                .unwrap()
                .insert("favorites", b"playlist".as_slice())
                .unwrap();
            write_txn
                .open_table(BLACKLIST_TABLE)
                .unwrap()
                .insert(b"/media/b".as_slice(), true)
                .unwrap();
            write_txn
                .open_table(HISTORY_TABLE)
                .unwrap()
                .insert("DP-1", b"history".as_slice())
                .unwrap();
            write_txn
                .open_table(META_TABLE)
                .unwrap()
                .insert("version", 4)
                .unwrap();
        }
        write_txn.commit().unwrap();
        drop(db);

        let migrated = FileCache::new_test(&db_path).expect("migrate cache");
        let read_txn = migrated.db.begin_read().unwrap();
        assert!(
            read_txn
                .open_table(FILE_CACHE_TABLE)
                .unwrap()
                .get(b"/media/a".as_slice())
                .unwrap()
                .is_none()
        );
        assert!(
            read_txn
                .open_table(POOL_TABLE)
                .unwrap()
                .get(b"/media".as_slice())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_txn
                .open_table(FILE_STATS_TABLE)
                .unwrap()
                .len()
                .unwrap(),
            1
        );
        assert_eq!(
            read_txn.open_table(PLAYLISTS_TABLE).unwrap().len().unwrap(),
            1
        );
        assert_eq!(
            read_txn.open_table(BLACKLIST_TABLE).unwrap().len().unwrap(),
            1
        );
        assert_eq!(
            read_txn.open_table(HISTORY_TABLE).unwrap().len().unwrap(),
            1
        );
    }

    #[test]
    fn corrupt_file_stats_are_skipped_and_removed() {
        let cache = test_cache("corrupt-stats");
        let path = Path::new("/media/corrupt");
        cache
            .insert_invalid_file_stats_bytes(path, &[0xff, 0x00, 0x01])
            .unwrap();

        assert!(cache.get_file_stats(path).unwrap().is_none());
        cache
            .insert_invalid_file_stats_bytes(path, &[0xff, 0x00, 0x01])
            .unwrap();
        assert!(cache.get_all_file_stats().unwrap().is_empty());
        assert!(cache.get_file_stats(path).unwrap().is_none());
    }

    #[test]
    fn corrupt_playlist_and_history_rows_are_skipped_and_removed() {
        let cache = test_cache("corrupt-durable-rows");
        let write_txn = cache.db.begin_write().unwrap();
        {
            let mut playlists = write_txn.open_table(PLAYLISTS_TABLE).unwrap();
            playlists.insert("one", &[0xff][..]).unwrap();
            playlists.insert("two", &[0xfe][..]).unwrap();
            let mut history = write_txn.open_table(HISTORY_TABLE).unwrap();
            history.insert("DP-1", &[0xfd][..]).unwrap();
        }
        write_txn.commit().unwrap();

        assert!(cache.get_playlist("one").unwrap().is_none());
        assert!(cache.get_all_playlists().unwrap().is_empty());
        assert!(cache.get_history("DP-1").unwrap().is_empty());

        let read_txn = cache.db.begin_read().unwrap();
        assert_eq!(
            read_txn.open_table(PLAYLISTS_TABLE).unwrap().len().unwrap(),
            0
        );
        assert_eq!(
            read_txn.open_table(HISTORY_TABLE).unwrap().len().unwrap(),
            0
        );
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
