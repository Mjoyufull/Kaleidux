use super::{FileCache, PoolEvent};
use anyhow::Result;
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

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
