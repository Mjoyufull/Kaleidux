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
    });
    indexes.insert(root, Arc::downgrade(&index));
    index
}

impl RootMediaIndex {
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

    pub(crate) fn content_type(&self, path: &Path) -> Option<ContentType> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        state.content_types.get(path).copied()
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
}
