use super::ContentType;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};

#[derive(Debug, Clone)]
pub(crate) struct MediaEntry {
    pub(crate) path: PathBuf,
    pub(crate) content_type: ContentType,
}

#[derive(Debug, Clone)]
pub(crate) struct RootMediaSnapshot {
    pub(crate) generation: u64,
    pub(crate) entries: Arc<[MediaEntry]>,
}

#[derive(Debug, Default)]
struct RootMediaState {
    generation: u64,
    entries: Arc<[MediaEntry]>,
    content_types: HashMap<PathBuf, ContentType>,
}

#[derive(Debug)]
pub(crate) struct RootMediaIndex {
    state: RwLock<RootMediaState>,
    rescan: Mutex<RescanState>,
}

#[derive(Debug, Default)]
struct RescanState {
    running: bool,
    generation: u64,
}

static ROOT_INDEXES: LazyLock<Mutex<HashMap<PathBuf, Weak<RootMediaIndex>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn canonical_root(root: &Path) -> PathBuf {
    root.canonicalize().unwrap_or_else(|_| root.to_path_buf())
}

pub(crate) fn shared_root_index(root: &Path) -> Arc<RootMediaIndex> {
    let root = canonical_root(root);
    let mut indexes = ROOT_INDEXES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    indexes.retain(|_, index| index.strong_count() > 0);
    if let Some(index) = indexes.get(&root).and_then(Weak::upgrade) {
        return index;
    }
    let index = Arc::new(RootMediaIndex {
        state: RwLock::new(RootMediaState::default()),
        rescan: Mutex::new(RescanState::default()),
    });
    indexes.insert(root, Arc::downgrade(&index));
    index
}

impl RootMediaIndex {
    pub(crate) fn request_rescan(&self) -> Option<u64> {
        let mut state = self
            .rescan
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.generation = state.generation.wrapping_add(1);
        if state.running {
            None
        } else {
            state.running = true;
            Some(state.generation)
        }
    }

    pub(crate) fn finish_rescan(&self, generation: u64) -> Option<u64> {
        let mut state = self
            .rescan
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.generation != generation {
            Some(state.generation)
        } else {
            state.running = false;
            None
        }
    }

    pub(crate) fn install_if_empty(
        &self,
        pool: &[PathBuf],
        content_types: &HashMap<PathBuf, ContentType>,
    ) -> RootMediaSnapshot {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.entries.is_empty() {
            replace_locked(&mut state, pool, content_types);
        }
        snapshot_locked(&state)
    }

    pub(crate) fn replace(
        &self,
        pool: &[PathBuf],
        content_types: &HashMap<PathBuf, ContentType>,
    ) -> RootMediaSnapshot {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        replace_locked(&mut state, pool, content_types);
        snapshot_locked(&state)
    }

    pub(crate) fn snapshot(&self) -> RootMediaSnapshot {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        snapshot_locked(&state)
    }

    pub(crate) fn apply_changes(
        &self,
        changes: &[(PathBuf, Option<ContentType>)],
    ) -> RootMediaSnapshot {
        if !changes.is_empty() {
            let mut rescan = self
                .rescan
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if rescan.running {
                rescan.generation = rescan.generation.wrapping_add(1);
            }
        }
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut changed = false;
        for (path, content_type) in changes {
            match content_type {
                Some(content_type) => {
                    changed |= state.content_types.insert(path.clone(), *content_type)
                        != Some(*content_type);
                }
                None => changed |= state.content_types.remove(path).is_some(),
            }
        }
        if changed {
            let mut entries = state
                .content_types
                .iter()
                .map(|(path, content_type)| MediaEntry {
                    path: path.clone(),
                    content_type: *content_type,
                })
                .collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.path.cmp(&right.path));
            state.entries = entries.into();
            state.generation = state.generation.saturating_add(1);
        }
        snapshot_locked(&state)
    }

    pub(crate) fn content_type(&self, path: &Path) -> Option<ContentType> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        state.content_types.get(path).copied()
    }
}

impl super::SmartQueue {
    pub(crate) fn root_pool_snapshot(&self) -> Vec<PathBuf> {
        self.root_index
            .snapshot()
            .entries
            .iter()
            .map(|entry| entry.path.clone())
            .collect()
    }
}

fn replace_locked(
    state: &mut RootMediaState,
    pool: &[PathBuf],
    content_types: &HashMap<PathBuf, ContentType>,
) {
    let entries: Vec<MediaEntry> = pool
        .iter()
        .filter_map(|path| {
            content_types
                .get(path)
                .copied()
                .map(|content_type| MediaEntry {
                    path: path.clone(),
                    content_type,
                })
        })
        .collect();
    state.entries = entries.into();
    state.content_types = content_types.clone();
    state.generation = state.generation.saturating_add(1);
}

fn snapshot_locked(state: &RootMediaState) -> RootMediaSnapshot {
    RootMediaSnapshot {
        generation: state.generation,
        entries: state.entries.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rescan_requests_coalesce_without_losing_changes_during_a_scan() {
        let index = shared_root_index(&PathBuf::from(format!("rescan-{}", rand::random::<u64>())));
        let first = index.request_rescan().unwrap();
        assert!(index.request_rescan().is_none());
        index.apply_changes(&[(PathBuf::from("new.png"), Some(ContentType::Image))]);
        let next = index
            .finish_rescan(first)
            .expect("requests during scan require a rerun");
        assert!(index.finish_rescan(next).is_none());
        assert!(index.request_rescan().is_some());
    }

    #[test]
    fn root_snapshot_storage_is_shared_and_generation_tagged() {
        let root = std::env::temp_dir().join(format!(
            "kaleidux-root-index-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).expect("root");
        let path = root.join("image.png");
        let types = HashMap::from([(path.clone(), ContentType::Image)]);
        let index = shared_root_index(&root);
        let first = index.replace(std::slice::from_ref(&path), &types);
        let second = shared_root_index(&root).snapshot();
        assert_eq!(first.generation, second.generation);
        assert!(Arc::ptr_eq(&first.entries, &second.entries));
    }

    #[test]
    fn incremental_changes_preserve_unrelated_root_entries() {
        let root = std::env::temp_dir().join(format!(
            "kaleidux-root-index-changes-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).expect("root");
        let first = root.join("first.png");
        let second = root.join("second.mp4");
        let third = root.join("third.png");
        let index = shared_root_index(&root);
        index.replace(
            &[first.clone(), second.clone()],
            &HashMap::from([
                (first.clone(), ContentType::Image),
                (second.clone(), ContentType::Video),
            ]),
        );

        let snapshot = index.apply_changes(&[
            (first.clone(), None),
            (third.clone(), Some(ContentType::Image)),
        ]);

        let entries = snapshot
            .entries
            .iter()
            .map(|entry| (entry.path.clone(), entry.content_type))
            .collect::<HashMap<_, _>>();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.get(&second), Some(&ContentType::Video));
        assert_eq!(entries.get(&third), Some(&ContentType::Image));
    }
}
