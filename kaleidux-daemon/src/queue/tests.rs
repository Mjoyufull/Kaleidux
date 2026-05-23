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
    SmartQueue {
        current_index: SmartQueue::fallback_current_index(strategy, pool.len()),
        planned_sequential_type: None,
        pool,
        stats: empty_stats(),
        video_ratio,
        strategy,
        history: VecDeque::new(),
        root_path: PathBuf::from("/tmp"),
        active_playlist: None,
        cache: test_cache(),
        pending_stats_updates: HashMap::new(),
        content_type_cache,
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
