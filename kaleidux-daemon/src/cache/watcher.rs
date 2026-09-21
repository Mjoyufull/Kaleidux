use super::{FileCache, PoolEvent};
use anyhow::Result;
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tokio::sync::mpsc;

const MAX_KNOWN_FILES: usize = 262_144;

pub struct DirectoryWatcher {
    watcher: RecommendedWatcher,
    event_rx: mpsc::Receiver<notify::Result<Event>>,
    event_ready: Arc<Notify>,
    overflowed: Arc<AtomicBool>,
    cache: Arc<FileCache>,
    watched_dirs: Vec<PathBuf>,
    known_files: HashSet<PathBuf>,
}

impl DirectoryWatcher {
    pub fn new(cache: Arc<FileCache>) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::channel(100);
        let event_ready = Arc::new(Notify::new());
        let callback_ready = event_ready.clone();
        let overflowed = Arc::new(AtomicBool::new(false));
        let callback_overflowed = overflowed.clone();

        let watcher =
            notify::recommended_watcher(move |res: notify::Result<Event>| {
                match event_tx.try_send(res) {
                    Ok(()) => callback_ready.notify_one(),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        callback_overflowed.store(true, Ordering::Release);
                        callback_ready.notify_one();
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {}
                }
            })?;

        Ok(Self {
            watcher,
            event_rx,
            event_ready,
            overflowed,
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
            if self.known_files.len() >= MAX_KNOWN_FILES && !self.known_files.contains(&path) {
                self.known_files.clear();
                tracing::warn!(
                    "[CACHE] Watcher file identity cache reached {MAX_KNOWN_FILES} entries; resetting it"
                );
            }
            self.known_files.insert(path.clone());
            tracing::debug!("[CACHE] File created: {}", path.display());
            // Modified is deliberately upsert-like in SmartQueue: it refreshes
            // an existing path and adds a genuinely new one. This remains
            // correct even when a prior batch invalidated the metadata cache.
            pool_events.push(PoolEvent::Modified(path));
        }
    }

    fn emit_modified_file(&mut self, path: PathBuf, pool_events: &mut Vec<PoolEvent>) {
        if path.is_file() {
            self.known_files.insert(path.clone());
            pool_events.push(PoolEvent::Modified(path));
        }
    }

    fn emit_removed_file(&mut self, path: PathBuf, pool_events: &mut Vec<PoolEvent>) {
        self.known_files.remove(&path);
        // Watchers are root-scoped and queues independently reject paths
        // outside their root, so an unconditional remove is safe and avoids
        // losing deletes after an earlier metadata invalidation.
        pool_events.push(PoolEvent::Removed(path));
    }

    fn process_rename_event(
        &mut self,
        rename_mode: RenameMode,
        paths: Vec<PathBuf>,
        pool_events: &mut Vec<PoolEvent>,
    ) {
        match (rename_mode, paths.as_slice()) {
            (RenameMode::Both, [from, to]) => {
                if to.is_dir() {
                    self.emit_root_rescans(from, pool_events);
                    self.emit_root_rescans(to, pool_events);
                    return;
                }
                self.emit_removed_file(from.clone(), pool_events);
                self.emit_added_file(to.clone(), pool_events);
                return;
            }
            (RenameMode::From, [from]) => {
                if self.is_known_file(from) {
                    self.emit_removed_file(from.clone(), pool_events);
                } else {
                    self.emit_root_rescans(from, pool_events);
                }
                return;
            }
            (RenameMode::To, [to]) => {
                if to.is_dir() {
                    self.emit_root_rescans(to, pool_events);
                } else {
                    self.emit_added_file(to.clone(), pool_events);
                }
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

    fn emit_root_rescans(&self, path: &Path, pool_events: &mut Vec<PoolEvent>) {
        pool_events.extend(
            self.watched_dirs
                .iter()
                .filter(|root| path.starts_with(root.as_path()) || root.starts_with(path))
                .cloned()
                .map(PoolEvent::Rescan),
        );
    }

    fn process_notify_event(&mut self, event: Event, pool_events: &mut Vec<PoolEvent>) {
        let Event { kind, paths, .. } = event;
        match kind {
            EventKind::Create(CreateKind::Folder) => {
                for path in paths {
                    self.emit_root_rescans(&path, pool_events);
                }
            }
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
            EventKind::Remove(RemoveKind::Folder) => {
                for path in paths {
                    self.emit_root_rescans(&path, pool_events);
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
    pub fn event_ready_handle(&self) -> Arc<Notify> {
        self.event_ready.clone()
    }

    pub fn process_events(&mut self) -> Vec<PoolEvent> {
        if self.overflowed.swap(false, Ordering::AcqRel) {
            while self.event_rx.try_recv().is_ok() {}
            self.known_files.clear();
            tracing::warn!(
                "[CACHE] Watcher event queue overflowed; scheduling full watched-root rescan"
            );
            return self
                .watched_dirs
                .iter()
                .cloned()
                .map(PoolEvent::Rescan)
                .collect();
        }
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

        self.finalize_pool_events(pool_events)
    }

    fn finalize_pool_events(&mut self, pool_events: Vec<PoolEvent>) -> Vec<PoolEvent> {
        let pool_events = coalesce_events(pool_events);
        let invalidated: Vec<PathBuf> = pool_events
            .iter()
            .filter_map(|event| match event {
                PoolEvent::Modified(path) | PoolEvent::Removed(path) => Some(path.clone()),
                PoolEvent::Added(_) | PoolEvent::Rescan(_) => None,
            })
            .collect();
        if let Err(error) = self.cache.batch_invalidate_files(&invalidated) {
            tracing::warn!("[CACHE] Failed to batch-invalidate watcher events: {error}");
        }
        pool_events
    }
}

fn coalesce_events(events: Vec<PoolEvent>) -> Vec<PoolEvent> {
    let mut coalesced = Vec::with_capacity(events.len());
    let mut positions = HashMap::<PathBuf, usize>::with_capacity(events.len());
    for event in events {
        let path = match &event {
            PoolEvent::Added(path)
            | PoolEvent::Removed(path)
            | PoolEvent::Modified(path)
            | PoolEvent::Rescan(path) => path.clone(),
        };
        if let Some(position) = positions.get(&path).copied() {
            let replacement = match (&coalesced[position], event) {
                (PoolEvent::Added(_), PoolEvent::Modified(path)) => PoolEvent::Added(path),
                (PoolEvent::Removed(_), PoolEvent::Added(path)) => PoolEvent::Modified(path),
                (_, latest) => latest,
            };
            coalesced[position] = replacement;
        } else {
            positions.insert(path, coalesced.len());
            coalesced.push(event);
        }
    }
    coalesced
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::FileMetadata;
    use notify::event::RemoveKind;
    use notify::event::{ModifyKind, RenameMode};
    use notify::{Event, EventKind};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn sample_metadata() -> FileMetadata {
        FileMetadata {
            mtime: 1,
            mtime_nanos: 0,
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
        let pool_events = watcher.finalize_pool_events(pool_events);

        assert_eq!(pool_events, vec![PoolEvent::Removed(path.clone())]);
        assert!(
            cache
                .get_file_metadata(&path)
                .expect("metadata lookup should succeed")
                .is_none()
        );
    }

    #[test]
    fn rename_event_removes_old_path_and_upserts_new_path() {
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
        let pool_events = watcher.finalize_pool_events(pool_events);

        assert_eq!(
            pool_events,
            vec![
                PoolEvent::Removed(old_path.clone()),
                PoolEvent::Modified(new_path.clone())
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

    #[test]
    fn watcher_overflow_requests_full_root_rescan() {
        let temp = unique_test_dir("overflow");
        let cache = Arc::new(
            FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
        );
        let mut watcher = DirectoryWatcher::new(cache).expect("directory watcher");
        watcher.watched_dirs.push(temp.clone());
        watcher.overflowed.store(true, Ordering::Release);

        assert_eq!(watcher.process_events(), vec![PoolEvent::Rescan(temp)]);
    }

    #[test]
    fn watcher_storm_coalesces_repeated_path_events() {
        let path = PathBuf::from("/tmp/kaleidux-watch-storm.png");
        let mut events = Vec::with_capacity(10_000);
        events.push(PoolEvent::Added(path.clone()));
        events.extend((1..10_000).map(|_| PoolEvent::Modified(path.clone())));

        assert_eq!(coalesce_events(events), vec![PoolEvent::Added(path)]);
    }

    #[test]
    fn create_for_existing_path_is_a_modification() {
        let temp = unique_test_dir("replace-existing");
        let cache = Arc::new(FileCache::new_test(&temp.join("cache.redb")).unwrap());
        let mut watcher = DirectoryWatcher::new(cache.clone()).unwrap();
        let path = temp.join("wallpaper.png");
        std::fs::write(&path, b"replacement").unwrap();
        cache.set_file_metadata(&path, &sample_metadata()).unwrap();

        let mut events = Vec::new();
        watcher.process_notify_event(
            Event::new(EventKind::Create(CreateKind::File)).add_path(path.clone()),
            &mut events,
        );

        assert_eq!(
            watcher.finalize_pool_events(events),
            vec![PoolEvent::Modified(path)]
        );
    }

    #[test]
    fn directory_rename_requests_a_root_rescan() {
        let temp = unique_test_dir("rename-directory");
        let cache = Arc::new(FileCache::new_test(&temp.join("cache.redb")).unwrap());
        let mut watcher = DirectoryWatcher::new(cache).unwrap();
        watcher.watched_dirs.push(temp.clone());
        let from = temp.join("old");
        let to = temp.join("new");
        std::fs::create_dir_all(&to).unwrap();

        let mut events = Vec::new();
        watcher.process_notify_event(
            Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
                .add_path(from)
                .add_path(to),
            &mut events,
        );

        assert_eq!(
            watcher.finalize_pool_events(events),
            vec![PoolEvent::Rescan(temp)]
        );
    }
}
