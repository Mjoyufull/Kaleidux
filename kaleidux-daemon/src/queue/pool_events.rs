use super::{ContentType, SmartQueue};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

impl SmartQueue {
    /// Look up a path's ContentType from the in-memory cache first.
    /// On a cache miss this falls back to `Self::get_content_type(path)`, which
    /// may open and read the file to detect its media type.
    pub(super) fn cached_content_type(&self, path: &Path) -> Option<ContentType> {
        self.content_type_cache
            .get(path)
            .copied()
            .or_else(|| self.root_index.content_type(path))
            .or_else(|| Self::get_content_type(path))
    }

    pub(super) fn sync_root_index_if_needed(&mut self) {
        let snapshot = self.root_index.snapshot();
        if snapshot.generation == self.root_generation {
            return;
        }
        let playlist = self
            .active_playlist
            .as_ref()
            .and_then(|name| self.stats.playlists.get(name));
        self.pool = snapshot
            .entries
            .iter()
            .filter(|entry| {
                !self.stats.blacklist.contains(&entry.path)
                    && playlist.is_none_or(|playlist| playlist.paths.contains(&entry.path))
            })
            .map(|entry| entry.path.clone())
            .collect();
        self.content_type_cache = snapshot
            .entries
            .iter()
            .map(|entry| (entry.path.clone(), entry.content_type))
            .collect();
        self.root_generation = snapshot.generation;
        self.current_index = self.current_index.min(self.pool.len().saturating_sub(1));
        self.planned_sequential_type = None;
    }

    /// Apply incremental pool events from the filesystem watcher
    pub fn apply_pool_events(&mut self, events: Vec<crate::cache::PoolEvent>) {
        use crate::cache::PoolEvent;

        self.sync_root_index_if_needed();

        let mut added = 0usize;
        let mut removed = 0usize;
        let mut index_changed = false;
        let mut root_changes = Vec::new();
        let mut cache_updates = Vec::new();
        let active_playlist_paths = self.active_playlist.as_ref().and_then(|name| {
            self.stats.playlists.get(name).map(|playlist| {
                playlist
                    .paths
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>()
            })
        });

        for event in events {
            match event {
                PoolEvent::Added(path) => {
                    if !path.starts_with(&self.root_path) {
                        continue;
                    }
                    let content_type = if self.stats.blacklist.contains(&path) {
                        None
                    } else {
                        Self::get_content_type(&path)
                    };
                    root_changes.push((path.clone(), content_type));
                    if active_playlist_paths
                        .as_ref()
                        .is_some_and(|paths| !paths.contains(&path))
                    {
                        continue;
                    }
                    // Only add if it's a supported media file and not blacklisted
                    if let Some(ct) = content_type {
                        if !self.pool.contains(&path) {
                            self.pool.push(path.clone());
                            self.content_type_cache.insert(path.clone(), ct);
                            added += 1;
                            index_changed = true;

                            // Update cache metadata
                            if let Ok(meta) = std::fs::metadata(&path) {
                                let modified = meta
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                    .unwrap_or_default();
                                let now_secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                cache_updates.push((
                                    path.clone(),
                                    crate::cache::FileMetadata {
                                        mtime: modified.as_secs(),
                                        mtime_nanos: modified.subsec_nanos(),
                                        size: meta.len(),
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
                }
                PoolEvent::Removed(path) => {
                    if !path.starts_with(&self.root_path) {
                        continue;
                    }
                    root_changes.push((path.clone(), None));
                    let before = self.pool.len();
                    self.pool.retain(|p| p != &path);
                    if self.pool.len() < before {
                        removed += 1;
                        self.content_type_cache.remove(&path);
                        index_changed = true;
                        // Clamp current_index if it's now out of bounds
                        if !self.pool.is_empty() {
                            self.current_index = self.current_index.min(self.pool.len() - 1);
                        }
                    }
                }
                PoolEvent::Modified(path) => {
                    if !path.starts_with(&self.root_path) {
                        continue;
                    }
                    let content_type = if self.stats.blacklist.contains(&path) {
                        None
                    } else {
                        Self::get_content_type(&path)
                    };
                    root_changes.push((path.clone(), content_type));
                    if active_playlist_paths
                        .as_ref()
                        .is_some_and(|paths| !paths.contains(&path))
                    {
                        continue;
                    }
                    // File content may have changed — re-check if it's still valid media
                    if let Some(ct) = content_type {
                        if !self.pool.contains(&path) && !self.stats.blacklist.contains(&path) {
                            self.pool.push(path.clone());
                            added += 1;
                            index_changed = true;
                        }
                        index_changed |=
                            self.content_type_cache.insert(path.clone(), ct) != Some(ct);
                        // Still valid, cache metadata was already invalidated by the watcher
                        if let Ok(meta) = std::fs::metadata(&path) {
                            let modified = meta
                                .modified()
                                .ok()
                                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                .unwrap_or_default();
                            let now_secs = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            cache_updates.push((
                                path.clone(),
                                crate::cache::FileMetadata {
                                    mtime: modified.as_secs(),
                                    mtime_nanos: modified.subsec_nanos(),
                                    size: meta.len(),
                                    content_type: match ct {
                                        ContentType::Image => 0,
                                        ContentType::Video => 1,
                                    },
                                    discovered_at: now_secs,
                                },
                            ));
                        }
                    } else {
                        // No longer valid media, remove from pool
                        let before = self.pool.len();
                        self.pool.retain(|p| p != &path);
                        self.content_type_cache.remove(&path);
                        if self.pool.len() < before {
                            removed += 1;
                            index_changed = true;
                        }

                        // Clamp index to avoid panics if we removed the last item
                        self.current_index =
                            self.current_index.min(self.pool.len().saturating_sub(1));
                    }
                }
                PoolEvent::Rescan(root) => {
                    if root != self.root_path {
                        continue;
                    }
                    match Self::discover_content(
                        &self.root_path,
                        &self.stats.blacklist,
                        self.cache.clone(),
                        None,
                    ) {
                        Ok((pool, content_types)) => {
                            let snapshot = self.root_index.replace(&pool, &content_types);
                            root_changes.clear();
                            self.root_generation = snapshot.generation;
                            let _ = self.cache.set_cached_pool(&self.root_path, &pool);
                            if let Some(playlist_paths) = active_playlist_paths.as_ref() {
                                self.pool = pool
                                    .into_iter()
                                    .filter(|path| playlist_paths.contains(path))
                                    .collect();
                                self.content_type_cache = content_types;
                            } else {
                                self.pool = pool;
                                self.content_type_cache = content_types;
                            }
                            self.current_index =
                                self.current_index.min(self.pool.len().saturating_sub(1));
                            self.planned_sequential_type = None;
                        }
                        Err(error) => tracing::warn!(
                            "[QUEUE] Failed full rescan for {} after watcher overflow: {}",
                            self.root_path.display(),
                            error
                        ),
                    }
                }
            }
        }

        if let Err(error) = self.cache.batch_set_file_metadata(&cache_updates) {
            tracing::warn!("[QUEUE] Failed to batch-update watcher metadata: {error}");
        }

        if added > 0 || removed > 0 {
            self.pool.sort();
            self.current_index = self.current_index.min(self.pool.len().saturating_sub(1));
            self.planned_sequential_type = None;
            // Update the cached pool
            if self.active_playlist.is_none() {
                let _ = self.cache.set_cached_pool(&self.root_path, &self.pool);
            }
            tracing::info!(
                "[QUEUE] Pool updated: +{} added, -{} removed, {} total",
                added,
                removed,
                self.pool.len()
            );
        }
        if self.active_playlist.is_none() {
            if index_changed {
                let snapshot = self
                    .root_index
                    .replace(&self.pool, &self.content_type_cache);
                self.root_generation = snapshot.generation;
            }
        } else if !root_changes.is_empty() {
            let snapshot = self.root_index.apply_changes(&root_changes);
            self.root_generation = snapshot.generation;
        }
    }
}
