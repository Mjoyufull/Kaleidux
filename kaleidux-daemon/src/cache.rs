use anyhow::{Context, Result, bail};
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tokio::sync::mpsc;

// Table definitions for redb
const FILE_CACHE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_cache");
const FILE_STATS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_stats");
const PLAYLISTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("playlists");
const BLACKLIST_TABLE: TableDefinition<&[u8], bool> = TableDefinition::new("blacklist");
const HISTORY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("history");
const POOL_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pool_cache");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");

const CACHE_VERSION: u64 = 4;

fn path_from_redb_key(key: &[u8]) -> Option<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        return Some(PathBuf::from(std::ffi::OsStr::from_bytes(key)));
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub mtime: u64, // Unix timestamp
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
            match std::fs::remove_file(&db_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e).context("Failed to remove stale cache database before recreate");
                }
            }
            if std::fs::metadata(&db_path).is_ok() {
                bail!(
                    "Stale cache file {:?} still exists after remove_file; refusing to recreate to avoid corruption",
                    db_path
                );
            }
            db = Database::create(&db_path)?;
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
                } else if let Ok(legacy) = postcard::from_bytes::<Vec<PathBuf>>(bytes) {
                    legacy
                } else {
                    Vec::new()
                };
                Ok(Some(paths))
            }
            _ => Ok(None),
        }
    }

    pub fn is_file_valid(&self, path: &Path) -> Result<bool> {
        let metadata = std::fs::metadata(path)?;
        let mtime = metadata.modified()?.duration_since(UNIX_EPOCH)?.as_secs();

        if let Some(cached) = self.get_file_metadata(path)? {
            Ok(cached.mtime == mtime)
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
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(FILE_STATS_TABLE)?;
            let path_bytes = path.as_os_str().as_encoded_bytes();
            let data = postcard::to_allocvec(stats)?;
            table.insert(path_bytes, data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
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
        Ok(())
    }

    pub fn get_all_file_stats(
        &self,
    ) -> Result<std::collections::HashMap<PathBuf, crate::queue::FileStats>> {
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
            let path_bytes = path.as_os_str().as_encoded_bytes();
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
    known_files: HashSet<PathBuf>,
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
            known_files: HashSet::new(),
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

    fn is_known_file(&self, path: &Path) -> bool {
        self.known_files.contains(path) || matches!(self.cache.get_file_metadata(path), Ok(Some(_)))
    }

    fn emit_added_file(&mut self, path: PathBuf, pool_events: &mut Vec<PoolEvent>) {
        if path.is_file() {
            self.known_files.insert(path.clone());
            tracing::debug!("[CACHE] File created: {}", path.display());
            pool_events.push(PoolEvent::Added(path));
        }
    }

    fn emit_modified_file(&mut self, path: PathBuf, pool_events: &mut Vec<PoolEvent>) {
        if path.is_file() {
            self.known_files.insert(path.clone());
            if let Err(e) = self.cache.invalidate_file(&path) {
                tracing::warn!(
                    "[CACHE] Failed to invalidate cache for {}: {}",
                    path.display(),
                    e
                );
            }
            pool_events.push(PoolEvent::Modified(path));
        }
    }

    fn emit_removed_file(&mut self, path: PathBuf, pool_events: &mut Vec<PoolEvent>) {
        if self.known_files.remove(&path) || self.is_known_file(&path) {
            if let Err(e) = self.cache.invalidate_file(&path) {
                tracing::debug!(
                    "[CACHE] Invalidation for removed path {}: {}",
                    path.display(),
                    e
                );
            }
            pool_events.push(PoolEvent::Removed(path));
        }
    }

    fn process_rename_event(
        &mut self,
        rename_mode: RenameMode,
        paths: Vec<PathBuf>,
        pool_events: &mut Vec<PoolEvent>,
    ) {
        match (rename_mode, paths.as_slice()) {
            (RenameMode::Both, [from, to]) => {
                self.emit_removed_file(from.clone(), pool_events);
                self.emit_added_file(to.clone(), pool_events);
                return;
            }
            (RenameMode::From, [from]) => {
                self.emit_removed_file(from.clone(), pool_events);
                return;
            }
            (RenameMode::To, [to]) => {
                self.emit_added_file(to.clone(), pool_events);
                return;
            }
            _ => {}
        }

        for path in paths {
            if path.is_file() {
                self.emit_added_file(path, pool_events);
            } else {
                self.emit_removed_file(path, pool_events);
            }
        }
    }

    fn process_notify_event(&mut self, event: Event, pool_events: &mut Vec<PoolEvent>) {
        let Event { kind, paths, .. } = event;
        match kind {
            EventKind::Create(_) => {
                for path in paths {
                    self.emit_added_file(path, pool_events);
                }
            }
            EventKind::Modify(ModifyKind::Name(rename_mode)) => {
                self.process_rename_event(rename_mode, paths, pool_events);
            }
            EventKind::Modify(_) => {
                for path in paths {
                    self.emit_modified_file(path, pool_events);
                }
            }
            EventKind::Remove(_) => {
                for path in paths {
                    self.emit_removed_file(path, pool_events);
                }
            }
            _ => {}
        }
    }

    /// Process file system events, invalidate cache entries, and return pool-affecting events
    pub async fn process_events(&mut self) -> Vec<PoolEvent> {
        let mut pool_events = Vec::new();

        loop {
            match self.event_rx.try_recv() {
                Ok(Ok(event)) => {
                    self.process_notify_event(event, &mut pool_events);
                }
                Ok(Err(e)) => {
                    tracing::error!("[CACHE] Watcher error: {}", e);
                }
                Err(_) => break, // Empty or disconnected
            }
        }

        pool_events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::RemoveKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn sample_metadata() -> FileMetadata {
        FileMetadata {
            mtime: 1,
            size: 2,
            content_type: 0,
            discovered_at: 3,
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kaleidux-cache-test-{}-{}-{}",
            name,
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("test dir should be created");
        dir
    }

    #[test]
    fn remove_event_uses_cached_metadata_for_extensionless_files() {
        let temp = unique_test_dir("remove-extensionless");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let mut watcher =
            DirectoryWatcher::new(cache.clone()).expect("directory watcher should be created");
        let path = temp.join("LICENSE");

        cache
            .set_file_metadata(&path, &sample_metadata())
            .expect("metadata should be stored");

        let mut pool_events = Vec::new();
        watcher.process_notify_event(
            Event::new(EventKind::Remove(notify::event::RemoveKind::File)).add_path(path.clone()),
            &mut pool_events,
        );

        assert_eq!(pool_events, vec![PoolEvent::Removed(path.clone())]);
        assert!(
            cache
                .get_file_metadata(&path)
                .expect("metadata lookup should succeed")
                .is_none()
        );
    }

    #[test]
    fn rename_event_removes_old_path_and_adds_new_path() {
        let temp = unique_test_dir("rename");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let mut watcher =
            DirectoryWatcher::new(cache.clone()).expect("directory watcher should be created");
        let old_path = temp.join("old_name");
        let new_path = temp.join("new_name");

        cache
            .set_file_metadata(&old_path, &sample_metadata())
            .expect("old metadata should be stored");
        std::fs::write(&new_path, b"new").expect("new file should be created");

        let mut pool_events = Vec::new();
        watcher.process_notify_event(
            Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
                .add_path(old_path.clone())
                .add_path(new_path.clone()),
            &mut pool_events,
        );

        assert_eq!(
            pool_events,
            vec![
                PoolEvent::Removed(old_path.clone()),
                PoolEvent::Added(new_path.clone())
            ]
        );
        assert!(
            cache
                .get_file_metadata(&old_path)
                .expect("old metadata lookup should succeed")
                .is_none()
        );
    }

    #[test]
    fn remove_event_after_modify_uses_known_file_tracking() {
        let temp = unique_test_dir("remove-after-modify");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let mut watcher =
            DirectoryWatcher::new(cache.clone()).expect("directory watcher should be created");
        let path = temp.join("clip.mp4");
        std::fs::write(&path, b"clip").expect("media file should be created");
        cache
            .set_file_metadata(&path, &sample_metadata())
            .expect("metadata should be stored");

        let mut pool_events = Vec::new();
        watcher.process_notify_event(
            Event::new(EventKind::Modify(ModifyKind::Data(
                notify::event::DataChange::Content,
            )))
            .add_path(path.clone()),
            &mut pool_events,
        );

        std::fs::remove_file(&path).expect("media file should be removed");
        watcher.process_notify_event(
            Event::new(EventKind::Remove(RemoveKind::File)).add_path(path.clone()),
            &mut pool_events,
        );

        assert_eq!(
            pool_events,
            vec![PoolEvent::Modified(path.clone()), PoolEvent::Removed(path)]
        );
    }
}
