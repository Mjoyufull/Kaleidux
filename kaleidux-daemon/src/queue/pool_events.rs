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
            .or_else(|| Self::get_content_type(path))
    }

    /// Apply incremental pool events from the filesystem watcher
    pub fn apply_pool_events(&mut self, events: Vec<crate::cache::PoolEvent>) {
        use crate::cache::PoolEvent;

        let mut added = 0usize;
        let mut removed = 0usize;

        for event in events {
            match event {
                PoolEvent::Added(path) => {
                    // Only add if it's a supported media file and not blacklisted
                    if self.stats.blacklist.contains(&path) {
                        continue;
                    }
                    if let Some(ct) = Self::get_content_type(&path) {
                        if !self.pool.contains(&path) {
                            self.pool.push(path.clone());
                            self.content_type_cache.insert(path.clone(), ct);
                            added += 1;

                            // Update cache metadata
                            if let Ok(meta) = std::fs::metadata(&path) {
                                let mtime = meta
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let now_secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let _ = self.cache.set_file_metadata(
                                    &path,
                                    &crate::cache::FileMetadata {
                                        mtime,
                                        size: meta.len(),
                                        content_type: match ct {
                                            ContentType::Image => 0,
                                            ContentType::Video => 1,
                                        },
                                        discovered_at: now_secs,
                                    },
                                );
                            }
                        }
                    }
                }
                PoolEvent::Removed(path) => {
                    let before = self.pool.len();
                    self.pool.retain(|p| p != &path);
                    if self.pool.len() < before {
                        removed += 1;
                        self.content_type_cache.remove(&path);
                        // Clamp current_index if it's now out of bounds
                        if !self.pool.is_empty() {
                            self.current_index = self.current_index.min(self.pool.len() - 1);
                        }
                    }
                }
                PoolEvent::Modified(path) => {
                    // File content may have changed — re-check if it's still valid media
                    if let Some(ct) = Self::get_content_type(&path) {
                        self.content_type_cache.insert(path.clone(), ct);
                        // Still valid, cache metadata was already invalidated by the watcher
                        if let Ok(meta) = std::fs::metadata(&path) {
                            let mtime = meta
                                .modified()
                                .ok()
                                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let now_secs = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let _ = self.cache.set_file_metadata(
                                &path,
                                &crate::cache::FileMetadata {
                                    mtime,
                                    size: meta.len(),
                                    content_type: match ct {
                                        ContentType::Image => 0,
                                        ContentType::Video => 1,
                                    },
                                    discovered_at: now_secs,
                                },
                            );
                        }
                    } else {
                        // No longer valid media, remove from pool
                        self.pool.retain(|p| p != &path);
                        self.content_type_cache.remove(&path);
                        removed += 1;

                        // Clamp index to avoid panics if we removed the last item
                        self.current_index =
                            self.current_index.min(self.pool.len().saturating_sub(1));
                    }
                }
            }
        }

        if added > 0 || removed > 0 {
            self.pool.sort();
            self.current_index = self.current_index.min(self.pool.len().saturating_sub(1));
            self.planned_sequential_type = None;
            // Update the cached pool
            let _ = self.cache.set_cached_pool(&self.root_path, &self.pool);
            tracing::info!(
                "[QUEUE] Pool updated: +{} added, -{} removed, {} total",
                added,
                removed,
                self.pool.len()
            );
        }
    }
}
