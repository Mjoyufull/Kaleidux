use super::*;
use std::collections::HashSet;
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

#[cfg(not(any(
    feature = "backend-ffmpeg",
    feature = "backend-mpv",
    feature = "backend-appsink"
)))]
#[test]
fn minimal_static_filters_video_from_discovery_and_seeded_pools() {
    let dir = unique_test_dir("minimal-static");
    let image = dir.join("image.png");
    let video = dir.join("video.webm");
    let mut bytes = [0_u8; 16];
    bytes[..4].copy_from_slice(&[0x89, 0x50, 0x4e, 0x47]);
    fs::write(&image, bytes).unwrap();
    bytes[..4].copy_from_slice(&[0x1a, 0x45, 0xdf, 0xa3]);
    fs::write(&video, bytes).unwrap();
    let cache = test_cache();
    let (pool, _) =
        SmartQueue::discover_content(&dir, &HashSet::new(), cache.clone(), None).unwrap();
    assert_eq!(pool, vec![image.clone()]);
    let queue = SmartQueue::new_from_pool(
        &dir,
        vec![image.clone(), video],
        50,
        crate::orchestration::SortingStrategy::Random,
        cache,
    )
    .unwrap();
    assert_eq!(queue.pool, vec![image]);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn selected_image_overrides_random_and_preserves_back_forward_history() {
    let dir = unique_test_dir("selected-history");
    let a = dir.join("a.png");
    let b = dir.join("b.png");
    let external = dir.join("external.png");
    for path in [&a, &b, &external] {
        fs::write(path, b"fixture").unwrap();
    }
    for strategy in [
        crate::orchestration::SortingStrategy::Random,
        crate::orchestration::SortingStrategy::Ascending,
    ] {
        let mut queue = make_test_queue(
            vec![a.clone(), b.clone()],
            strategy,
            0,
            HashMap::from([
                (a.clone(), ContentType::Image),
                (b.clone(), ContentType::Image),
            ]),
        );
        queue.enqueue_selected_image(a.clone());
        assert_eq!(queue.pick_next(), Some(a.clone()));
        queue.enqueue_selected_image(b.clone());
        assert_eq!(queue.pick_next(), Some(b.clone()));
        assert_eq!(queue.pool.len(), 2);
        queue.enqueue_selected_image(external.clone());
        assert_eq!(
            queue.peek_next(),
            Some((external.clone(), ContentType::Image))
        );
        assert_eq!(queue.pick_next(), Some(external.clone()));
        assert_eq!(queue.pick_prev(), Some(b.clone()));
        queue.root_index.replace(
            &[a.clone(), b.clone()],
            &HashMap::from([
                (a.clone(), ContentType::Image),
                (b.clone(), ContentType::Image),
            ]),
        );
        assert_eq!(queue.pick_next(), Some(external.clone()));
        assert_eq!(queue.pool.len(), 3);
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn selected_images_respect_alias_blacklists_and_missing_files_on_refresh() {
    let dir = unique_test_dir("selected-blacklist");
    let path = dir.join("image.png");
    let alias = dir.join("alias.png");
    fs::write(&path, b"fixture").unwrap();
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    let mut queue = make_test_queue(
        Vec::new(),
        crate::orchestration::SortingStrategy::Random,
        0,
        HashMap::new(),
    );
    queue.enqueue_selected_image(path.clone());
    queue.stats.blacklist.insert(alias);
    queue.root_index.replace(
        std::slice::from_ref(&path),
        &HashMap::from([(path.clone(), ContentType::Image)]),
    );
    assert_eq!(queue.pick_next(), None);
    queue.enqueue_selected_image(path.clone());
    assert!(queue.selected_images.is_empty());
    queue.stats.blacklist.clear();
    queue.enqueue_selected_image(path.clone());
    fs::remove_file(&path).unwrap();
    queue.root_index.replace(&[], &HashMap::new());
    assert_eq!(queue.pick_next(), None);
    assert!(queue.selected_images.is_empty());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn missing_active_playlist_does_not_expand_to_root_after_refresh() {
    let path = PathBuf::from("/a.png");
    let types = HashMap::from([(path.clone(), ContentType::Image)]);
    let mut queue = make_test_queue(
        vec![path.clone()],
        crate::orchestration::SortingStrategy::Random,
        0,
        types.clone(),
    );
    queue.active_playlist = Some("deleted".into());
    queue.root_index.replace(&[path], &types);
    assert_eq!(queue.pick_next(), None);
    assert!(queue.pool.is_empty());
}

#[test]
fn selected_image_retention_deduplicates_and_evicts_oldest_on_refresh() {
    let dir = unique_test_dir("selected-cap");
    let paths: Vec<_> = (0..51).map(|i| dir.join(format!("{i:02}.png"))).collect();
    let mut queue = make_test_queue(
        Vec::new(),
        crate::orchestration::SortingStrategy::Ascending,
        0,
        HashMap::new(),
    );
    for path in &paths {
        fs::write(path, b"fixture").unwrap();
        queue.enqueue_selected_image(path.clone());
    }
    assert_eq!(queue.selected_images.len(), 50);
    assert!(!queue.selected_images.contains(&paths[0]));
    queue.enqueue_selected_image(paths[1].clone());
    assert_eq!(queue.selected_images.len(), 50);
    assert_eq!(queue.selected_images.back(), Some(&paths[1]));
    queue.root_index.replace(&[], &HashMap::new());
    queue.sync_root_index_if_needed();
    assert_eq!(queue.pool.len(), 50);
    assert!(!queue.pool.contains(&paths[0]));
    assert!(queue.pool.windows(2).all(|pair| pair[0] < pair[1]));
    fs::remove_dir_all(dir).unwrap();
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
        forward_history: VecDeque::new(),
        selected_images: VecDeque::new(),
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
fn non_sequential_previous_preserves_forward_navigation() {
    for strategy in [
        crate::orchestration::SortingStrategy::Random,
        crate::orchestration::SortingStrategy::Loveit,
    ] {
        let paths = ["a.jpg", "b.jpg", "c.jpg"].map(PathBuf::from).to_vec();
        let content_types = paths
            .iter()
            .cloned()
            .map(|path| (path, ContentType::Image))
            .collect();
        let mut queue = make_test_queue(paths, strategy, 0, content_types);
        let first = PathBuf::from("a.jpg");
        let second = PathBuf::from("b.jpg");
        let third = PathBuf::from("c.jpg");
        queue.history = VecDeque::from([first.clone(), second.clone(), third.clone()]);

        assert_eq!(queue.pick_prev(), Some(second.clone()));
        assert_eq!(queue.peek_next(), Some((third.clone(), ContentType::Image)));
        assert_eq!(queue.pick_prev(), Some(first));
        assert_eq!(
            queue.peek_upcoming_images(2),
            vec![second.clone(), third.clone()]
        );
        assert_eq!(queue.pick_next(), Some(second));
        assert_eq!(queue.pick_next(), Some(third));
    }
}

#[test]
fn forward_navigation_skips_removed_and_excluded_entries() {
    let first = PathBuf::from("a.jpg");
    let second = PathBuf::from("b.jpg");
    let third = PathBuf::from("c.jpg");
    let content_types = [first.clone(), second.clone(), third.clone()]
        .into_iter()
        .map(|path| (path, ContentType::Image))
        .collect();
    let mut queue = make_test_queue(
        vec![first.clone(), second.clone(), third.clone()],
        crate::orchestration::SortingStrategy::Random,
        0,
        content_types,
    );
    queue.history = VecDeque::from([first.clone(), second.clone(), third.clone()]);

    assert_eq!(queue.pick_prev(), Some(second.clone()));
    assert_eq!(queue.pick_prev(), Some(first.clone()));
    queue.pool.retain(|path| path != &second);
    let excluded = HashSet::from([third.clone()]);

    assert_eq!(queue.pick_next_excluding(&excluded), Some(first));
    assert_eq!(queue.pick_next(), Some(third));
}

#[test]
fn forward_peek_does_not_skip_an_unknown_first_entry() {
    let unknown = PathBuf::from("unknown");
    let image = PathBuf::from("a.jpg");
    let mut queue = make_test_queue(
        vec![unknown.clone(), image.clone()],
        crate::orchestration::SortingStrategy::Random,
        0,
        HashMap::from([(image.clone(), ContentType::Image)]),
    );
    queue.forward_history = VecDeque::from([unknown.clone(), image]);
    assert_eq!(queue.peek_next(), None);
    assert_eq!(queue.pick_next(), Some(unknown));
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
