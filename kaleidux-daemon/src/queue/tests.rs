use super::*;
use std::fs;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

fn empty_stats() -> LoveitData {
    LoveitData {
        files: lru::LruCache::new(NonZeroUsize::new(STATS_LRU_CAP).unwrap()),
        playlists: HashMap::new(),
        blacklist: std::collections::HashSet::new(),
    }
}

fn make_test_queue(
    pool: Vec<PathBuf>,
    strategy: crate::orchestration::SortingStrategy,
    video_ratio: u8,
    content_type_cache: HashMap<PathBuf, ContentType>,
) -> SmartQueue {
    let root_path = PathBuf::from(format!(
        "/tmp/kaleidux-queue-unit-root-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let root_index = media_index::shared_root_index(&root_path);
    let root_generation = root_index
        .install_if_empty(&pool, &content_type_cache)
        .generation;
    SmartQueue {
        current_index: SmartQueue::fallback_current_index(strategy, pool.len()),
        planned_sequential_type: None,
        pool,
        stats: empty_stats(),
        video_ratio,
        strategy,
        history: VecDeque::new(),
        root_path,
        active_playlist: None,
        cache: test_cache(),
        pending_stats_updates: HashMap::new(),
        content_type_cache,
        root_index,
        root_generation,
    }
}

fn unique_test_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kaleidux-queue-test-{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn test_cache() -> Arc<FileCache> {
    let db_path = unique_test_dir("cache-db").join("cache.redb");
    Arc::new(FileCache::new_test(&db_path).unwrap())
}

#[test]
#[ignore = "explicit 100k-file Phase 7 discovery gate"]
fn discovery_handles_100k_files_with_one_root_index() {
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let root = unique_test_dir("100k-discovery");
    let _cleanup = Cleanup(root.clone());
    let header = [0x89, b'P', b'N', b'G', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for directory in 0..100_u32 {
        let shard = root.join(format!("{directory:03}"));
        std::fs::create_dir(&shard).expect("shard directory");
        for file in 0..1_000_u32 {
            std::fs::write(shard.join(format!("{file:04}.png")), header).expect("fixture file");
        }
    }

    let root_index = media_index::shared_root_index(&root);
    let cache = test_cache();
    let (pool, content_types) =
        SmartQueue::discover_content(&root, &std::collections::HashSet::new(), cache, None)
            .expect("100k discovery should complete");

    assert_eq!(pool.len(), 100_000);
    assert_eq!(content_types.len(), 100_000);
    let snapshot = root_index.snapshot();
    assert_eq!(snapshot.entries.len(), 100_000);
}

#[test]
fn new_from_pool_populates_content_type_cache_for_reused_pool() {
    let dir = unique_test_dir("content-cache");
    let image = dir.join("a.jpg");
    let video = dir.join("b.mp4");

    let mut jpeg = [0u8; 16];
    jpeg[..3].copy_from_slice(&[0xFF, 0xD8, 0xFF]);
    let mut mp4 = [0u8; 16];
    mp4[4..8].copy_from_slice(b"ftyp");
    mp4[8..12].copy_from_slice(b"isom");
    fs::write(&image, jpeg).unwrap();
    fs::write(&video, mp4).unwrap();

    let queue = SmartQueue::new_from_pool(
        &dir,
        vec![image.clone(), video.clone()],
        75,
        crate::orchestration::SortingStrategy::Loveit,
        test_cache(),
    )
    .unwrap();

    assert_eq!(
        queue.content_type_cache.get(&image),
        Some(&ContentType::Image)
    );
    assert_eq!(
        queue.content_type_cache.get(&video),
        Some(&ContentType::Video)
    );
}

#[test]
fn ascending_sequential_honors_images_only_ratio() {
    let img_a = PathBuf::from("a.jpg");
    let img_b = PathBuf::from("b.jpg");
    let vid_a = PathBuf::from("c.mp4");
    let vid_b = PathBuf::from("d.mp4");
    let pool = vec![img_a.clone(), img_b.clone(), vid_a, vid_b];
    let content_type_cache = HashMap::from([
        (img_a.clone(), ContentType::Image),
        (img_b.clone(), ContentType::Image),
        (PathBuf::from("c.mp4"), ContentType::Video),
        (PathBuf::from("d.mp4"), ContentType::Video),
    ]);
    let mut queue = make_test_queue(
        pool,
        crate::orchestration::SortingStrategy::Ascending,
        0,
        content_type_cache,
    );

    assert_eq!(queue.pick_next(), Some(img_a));
    assert_eq!(queue.pick_next(), Some(img_b));
}

#[test]
fn ascending_sequential_honors_videos_only_ratio() {
    let pool = vec![
        PathBuf::from("a.jpg"),
        PathBuf::from("b.jpg"),
        PathBuf::from("c.mp4"),
        PathBuf::from("d.mp4"),
    ];
    let content_type_cache = HashMap::from([
        (PathBuf::from("a.jpg"), ContentType::Image),
        (PathBuf::from("b.jpg"), ContentType::Image),
        (PathBuf::from("c.mp4"), ContentType::Video),
        (PathBuf::from("d.mp4"), ContentType::Video),
    ]);
    let mut queue = make_test_queue(
        pool,
        crate::orchestration::SortingStrategy::Ascending,
        100,
        content_type_cache,
    );

    assert_eq!(queue.pick_next(), Some(PathBuf::from("c.mp4")));
    assert_eq!(queue.pick_next(), Some(PathBuf::from("d.mp4")));
}

#[test]
fn descending_sequential_honors_videos_only_ratio() {
    let pool = vec![
        PathBuf::from("a.jpg"),
        PathBuf::from("b.jpg"),
        PathBuf::from("c.mp4"),
        PathBuf::from("d.mp4"),
    ];
    let content_type_cache = HashMap::from([
        (PathBuf::from("a.jpg"), ContentType::Image),
        (PathBuf::from("b.jpg"), ContentType::Image),
        (PathBuf::from("c.mp4"), ContentType::Video),
        (PathBuf::from("d.mp4"), ContentType::Video),
    ]);
    let mut queue = make_test_queue(
        pool,
        crate::orchestration::SortingStrategy::Descending,
        100,
        content_type_cache,
    );

    assert_eq!(queue.pick_next(), Some(PathBuf::from("d.mp4")));
    assert_eq!(queue.pick_next(), Some(PathBuf::from("c.mp4")));
}

#[test]
fn sequential_peek_upcoming_images_skips_videos_and_wraps() {
    let img_a = PathBuf::from("a.jpg");
    let img_b = PathBuf::from("b.jpg");
    let img_c = PathBuf::from("c.jpg");
    let vid_a = PathBuf::from("d.mp4");
    let pool = vec![img_a.clone(), vid_a.clone(), img_b.clone(), img_c.clone()];
    let content_type_cache = HashMap::from([
        (img_a.clone(), ContentType::Image),
        (img_b.clone(), ContentType::Image),
        (img_c.clone(), ContentType::Image),
        (vid_a, ContentType::Video),
    ]);
    let queue = make_test_queue(
        pool,
        crate::orchestration::SortingStrategy::Ascending,
        50,
        content_type_cache,
    );

    assert_eq!(queue.peek_upcoming_images(3), vec![img_a, img_b, img_c]);
}

#[test]
fn non_sequential_peek_upcoming_images_returns_no_deterministic_lookahead() {
    let img = PathBuf::from("a.jpg");
    let content_type_cache = HashMap::from([(img.clone(), ContentType::Image)]);
    let queue = make_test_queue(
        vec![img.clone()],
        crate::orchestration::SortingStrategy::Random,
        0,
        content_type_cache,
    );

    assert!(queue.peek_upcoming_images(4).is_empty());
}

#[test]
fn loveit_peek_upcoming_images_prioritizes_high_weight_images() {
    let img_a = PathBuf::from("a.jpg");
    let img_b = PathBuf::from("b.jpg");
    let img_c = PathBuf::from("c.jpg");
    let vid_a = PathBuf::from("d.mp4");
    let content_type_cache = HashMap::from([
        (img_a.clone(), ContentType::Image),
        (img_b.clone(), ContentType::Image),
        (img_c.clone(), ContentType::Image),
        (vid_a.clone(), ContentType::Video),
    ]);
    let mut queue = make_test_queue(
        vec![img_a.clone(), img_b.clone(), img_c.clone(), vid_a],
        crate::orchestration::SortingStrategy::Loveit,
        50,
        content_type_cache,
    );

    queue.stats.files.put(
        img_a.clone(),
        FileStats {
            count: 25,
            last_seen: Some(Utc::now()),
            love_multiplier: 1.0,
        },
    );
    queue.stats.files.put(
        img_c.clone(),
        FileStats {
            count: 0,
            last_seen: Some(Utc::now()),
            love_multiplier: 5.0,
        },
    );

    assert_eq!(queue.peek_upcoming_images(2), vec![img_b, img_c]);
}

#[test]
fn sequential_previous_uses_display_history_and_preserves_next_position() {
    for strategy in [
        crate::orchestration::SortingStrategy::Ascending,
        crate::orchestration::SortingStrategy::Descending,
    ] {
        let paths = ["a.jpg", "b.jpg", "c.jpg"].map(PathBuf::from).to_vec();
        let content_types = paths
            .iter()
            .cloned()
            .map(|path| (path, ContentType::Image))
            .collect();
        let mut queue = make_test_queue(paths, strategy, 0, content_types);
        let first = queue.pick_next().unwrap();
        let second = queue.pick_next().unwrap();

        assert_eq!(queue.pick_prev(), Some(first));
        assert_eq!(queue.pick_next(), Some(second));
    }
}

#[test]
fn sequential_previous_skips_history_entries_removed_from_the_pool() {
    let first = PathBuf::from("a.jpg");
    let removed = PathBuf::from("b.jpg");
    let current = PathBuf::from("c.jpg");
    let content_types = [first.clone(), removed.clone(), current.clone()]
        .into_iter()
        .map(|path| (path, ContentType::Image))
        .collect();
    let mut queue = make_test_queue(
        vec![first.clone(), removed.clone(), current.clone()],
        crate::orchestration::SortingStrategy::Ascending,
        0,
        content_types,
    );
    queue.history = VecDeque::from([first.clone(), removed.clone(), current]);
    queue.pool.retain(|path| path != &removed);

    assert_eq!(queue.pick_prev(), Some(first));
}

#[test]
fn playlist_events_do_not_replace_the_shared_root_index() {
    let root = unique_test_dir("playlist-root-index");
    let first = root.join("a.jpg");
    let second = root.join("b.jpg");
    let third = root.join("c.jpg");
    for path in [&first, &second, &third] {
        let mut bytes = [0_u8; 16];
        bytes[..4].copy_from_slice(&[0xff, 0xd8, 0xff, 0xd9]);
        std::fs::write(path, bytes).unwrap();
    }
    let cache = test_cache();
    let mut unfiltered = SmartQueue::new_from_pool(
        &root,
        vec![first.clone(), second.clone()],
        0,
        crate::orchestration::SortingStrategy::Ascending,
        cache.clone(),
    )
    .unwrap();
    let mut filtered = SmartQueue::new_from_pool(
        &root,
        vec![first.clone(), second.clone()],
        0,
        crate::orchestration::SortingStrategy::Ascending,
        cache,
    )
    .unwrap();
    filtered.stats.playlists.insert(
        "only-a".into(),
        Playlist {
            paths: vec![first.clone()],
            strategy: crate::orchestration::SortingStrategy::Ascending,
            enabled: true,
        },
    );
    filtered.set_playlist(Some("only-a".into())).unwrap();

    filtered.apply_pool_events(vec![crate::cache::PoolEvent::Modified(first.clone())]);
    unfiltered.sync_root_index_if_needed();

    assert_eq!(unfiltered.pool, vec![first.clone(), second.clone()]);

    filtered.apply_pool_events(vec![crate::cache::PoolEvent::Modified(third.clone())]);
    unfiltered.sync_root_index_if_needed();
    assert_eq!(filtered.pool, vec![first.clone()]);
    assert_eq!(
        unfiltered.pool,
        vec![first.clone(), second.clone(), third.clone()]
    );

    filtered.blacklist_file(first.clone()).unwrap();
    unfiltered.sync_root_index_if_needed();
    assert_eq!(unfiltered.pool, vec![second.clone(), third.clone()]);

    filtered.unblacklist_file(first.clone()).unwrap();
    unfiltered.sync_root_index_if_needed();
    assert_eq!(filtered.pool, vec![first.clone()]);
    assert_eq!(unfiltered.pool, vec![first, second, third]);
}
