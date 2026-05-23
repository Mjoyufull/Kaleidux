use super::{FileStats, LoveitData, STATS_LRU_CAP, SmartQueue};
use crate::cache::FileCache;
use anyhow::Result;
use chrono::Utc;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

impl SmartQueue {
    pub(super) fn update_stats(&mut self, path: &Path) {
        let now = Utc::now();
        // get_or_insert_mut promotes existing entries and auto-evicts oldest when at capacity
        let stat = self
            .stats
            .files
            .get_or_insert_mut(path.to_path_buf(), || FileStats {
                count: 0,
                last_seen: None,
                love_multiplier: 1.0,
            });
        stat.count += 1;
        stat.last_seen = Some(now);

        // Add to pending updates for batched write
        self.pending_stats_updates
            .insert(path.to_path_buf(), stat.clone());
        // No manual eviction needed — LruCache handles it automatically
    }

    /// Flush pending stats updates to cache in a batch
    pub fn flush_stats(&mut self) -> Result<()> {
        if self.pending_stats_updates.is_empty() {
            return Ok(());
        }

        let updates: Vec<_> = self.pending_stats_updates.drain().collect();

        self.cache.batch_set_file_stats(&updates)?;
        Ok(())
    }

    pub(super) fn load_stats_from_cache(cache: &FileCache) -> Result<LoveitData> {
        // Load from redb cache
        let files_map = cache.get_all_file_stats()?;
        let playlists = cache.get_all_playlists()?;
        let blacklist = cache.get_all_blacklisted()?;

        let cap = NonZeroUsize::new(STATS_LRU_CAP).unwrap();
        let mut files = lru::LruCache::new(cap);
        let mut entries: Vec<(PathBuf, FileStats)> = files_map.into_iter().collect();
        entries.sort_by(|a, b| match (&a.1.last_seen, &b.1.last_seen) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(aa), Some(bb)) => aa.cmp(bb),
        });
        for (path, stat) in entries {
            files.put(path, stat);
        }

        Ok(LoveitData {
            files,
            playlists,
            blacklist,
        })
    }

    pub fn save_stats(&mut self) -> Result<()> {
        // Flush any pending stats updates first
        self.flush_stats()?;

        // Save playlists
        for (name, playlist) in &self.stats.playlists {
            let _ = self.cache.set_playlist(name, playlist);
        }

        // Save blacklist
        for path in &self.stats.blacklist {
            let _ = self.cache.set_blacklisted(path, true);
        }

        Ok(())
    }

    pub fn love_file(&mut self, path: PathBuf, multiplier: f32) -> Result<()> {
        let stat = self.stats.files.get_or_insert_mut(path, FileStats::default);
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
                self.pool = playlist
                    .paths
                    .iter()
                    .filter(|p| !self.stats.blacklist.contains(*p))
                    .cloned()
                    .collect();
                // If playlist has a strategy, use it? Or keep global?
                // For now, let's stick to global strategy unless we want to override it.
            } else {
                anyhow::bail!("Playlist '{}' not found", n);
            }
        } else {
            // Reset to full discovery (no metrics available in this context)
            let (pool, ct_cache) = Self::discover_content(
                &self.root_path,
                &self.stats.blacklist,
                self.cache.clone(),
                None,
            )?;
            self.pool = pool;
            self.content_type_cache = ct_cache;
        }

        self.active_playlist = name;
        self.pool.sort(); // Always sort generic pool
        self.current_index = Self::fallback_current_index(self.strategy, self.pool.len());
        self.planned_sequential_type = None;
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
