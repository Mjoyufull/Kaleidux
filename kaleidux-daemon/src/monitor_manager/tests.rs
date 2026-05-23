use super::*;
use crate::orchestration::{Layer, PartialOutputConfig, SortingStrategy};
use crate::queue::Playlist;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn test_output_config(duration: Duration) -> OutputConfig {
    OutputConfig {
        path: None,
        duration,
        video_ratio: 50,
        transition: crate::shaders::Transition::Fade,
        transition_time: 1000,
        volume: 100,
        sorting: SortingStrategy::Loveit,
        layer: Layer::Background,
        default_playlist: None,
        performance: crate::orchestration::PerformanceProfile::Balanced,
        video_fps: crate::orchestration::VideoFpsProfile::Unlimited,
        frame_latency: None,
    }
}

fn output_partial(config: &OutputConfig) -> PartialOutputConfig {
    PartialOutputConfig {
        path: config.path.clone(),
        duration: Some(config.duration),
        video_ratio: Some(config.video_ratio),
        transition: Some(config.transition.clone()),
        transition_time: Some(config.transition_time),
        volume: Some(config.volume),
        sorting: Some(config.sorting),
        layer: Some(config.layer.clone()),
        default_playlist: config.default_playlist.clone(),
        performance: Some(config.performance),
        video_fps: Some(config.video_fps),
        frame_latency: config.frame_latency,
    }
}

fn config_for_output_with_behavior(
    name: &str,
    config: &OutputConfig,
    monitor_behavior: MonitorBehavior,
) -> Config {
    let mut outputs = HashMap::new();
    outputs.insert(name.to_string(), output_partial(config));
    Config::from_parts(
        crate::orchestration::GlobalConfig {
            monitor_behavior,
            ..Default::default()
        },
        PartialOutputConfig::default(),
        outputs,
    )
}

fn config_for_output(name: &str, config: &OutputConfig) -> Config {
    config_for_output_with_behavior(name, config, MonitorBehavior::Independent)
}

fn config_for_outputs_with_behavior(
    outputs_with_config: &[(&str, &OutputConfig)],
    monitor_behavior: MonitorBehavior,
) -> Config {
    let mut outputs = HashMap::new();
    for (name, config) in outputs_with_config {
        outputs.insert((*name).to_string(), output_partial(config));
    }
    Config::from_parts(
        crate::orchestration::GlobalConfig {
            monitor_behavior,
            ..Default::default()
        },
        PartialOutputConfig::default(),
        outputs,
    )
}

fn unique_test_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kaleidux-monitor-test-{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("test dir should be created");
    dir
}

#[test]
fn apply_selected_path_skips_unknown_content_type_without_mutating_state() {
    let temp = unique_test_dir("unknown-content");
    let path = temp.join("mystery.bin");
    std::fs::write(&path, b"short").expect("test file should be written");

    let mut orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        config: test_output_config(Duration::from_secs(60)),
        queue: None,
        current_path: Some(PathBuf::from("/tmp/original")),
        next_path: Some(PathBuf::from("/tmp/next")),
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
        phase_offset: Duration::ZERO,
    };

    assert!(orch.apply_selected_path(path).is_none());
    assert_eq!(orch.current_path, Some(PathBuf::from("/tmp/original")));
    assert_eq!(orch.next_path, Some(PathBuf::from("/tmp/next")));
    assert_eq!(
        orch.next_content_type,
        Some(crate::queue::ContentType::Image)
    );
    assert!(orch.display_start_time.is_some());
    assert!(orch.next_change.is_some());
}

fn write_test_image(dir: &std::path::Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let mut bytes = vec![0u8; 16];
    bytes[..4].copy_from_slice(&[0x89, b'P', b'N', b'G']);
    std::fs::write(&path, bytes).expect("test image should be written");
    path
}

fn make_test_queue(
    cache: Arc<FileCache>,
    dir: &std::path::Path,
    pool: Vec<PathBuf>,
    config: &OutputConfig,
) -> SmartQueue {
    let mut queue = SmartQueue::new_from_pool(dir, pool, config.video_ratio, config.sorting, cache)
        .expect("test queue should be created");
    if let Some(pl_name) = &config.default_playlist {
        queue
            .set_playlist(Some(pl_name.clone()))
            .expect("default playlist should be applied");
    }
    queue
}

fn make_test_manager(
    name: &str,
    cache: Arc<FileCache>,
    orch: OutputOrchestrator,
    config: Config,
) -> MonitorManager {
    let mut outputs = HashMap::new();
    outputs.insert(name.to_string(), orch);
    MonitorManager {
        config,
        outputs,
        shared_queue: None,
        group_queues: HashMap::new(),
        output_groups: HashMap::new(),
        shared_display_start_time: None,
        group_display_start_times: HashMap::new(),
        cache,
        metrics: None,
        paused: false,
        discovered_files_cache: HashMap::new(),
    }
}

#[test]
fn independent_phase_offset_is_stable_for_same_output_name() {
    let base = Duration::from_secs(60);
    let first = independent_phase_offset("DP-2", base);
    let second = independent_phase_offset("DP-2", base);

    assert_eq!(first, second);
}

#[test]
fn independent_phase_offset_stays_within_cap() {
    let base = Duration::from_secs(120);
    let offset = independent_phase_offset("HDMI-A-1", base);

    assert!(offset <= Duration::from_millis(120));
    assert!(offset <= base / 12);
}

#[test]
fn independent_phase_offset_is_zero_for_short_durations() {
    assert_eq!(
        independent_phase_offset("DP-3", Duration::from_millis(80)),
        Duration::ZERO
    );
}

#[test]
fn applying_updated_config_recomputes_phase_offset() {
    let old_duration = Duration::from_secs(1);
    let new_duration = Duration::from_secs(2);
    let old_offset = independent_phase_offset("DP-2", old_duration);
    let new_offset = independent_phase_offset("DP-2", new_duration);

    assert_ne!(
        old_offset, new_offset,
        "test requires durations with distinct phase offsets"
    );

    let mut orch = OutputOrchestrator {
        _name: "DP-2".to_string(),
        description: "DisplayPort-2".to_string(),
        config: test_output_config(old_duration),
        queue: None,
        current_path: None,
        next_path: None,
        next_content_type: None,
        next_change: None,
        display_start_time: None,
        phase_offset: old_offset,
    };

    orch.apply_config(test_output_config(new_duration));

    assert_eq!(orch.phase_offset, new_offset);
    assert_eq!(orch.config.duration, new_duration);
}

#[path = "tests/update_config.rs"]
mod update_config;

#[test]
fn synchronized_tick_skips_unknown_content_type_without_mutating_outputs() {
    let temp = unique_test_dir("sync-unknown-tick");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let unknown = path.join("mystery.bin");
    std::fs::write(&unknown, b"short").expect("test file should be written");

    let mut output_config = test_output_config(Duration::from_secs(60));
    output_config.path = Some(path.clone());
    let shared_queue = make_test_queue(cache.clone(), &path, vec![unknown], &output_config);

    let mut outputs = HashMap::new();
    for name in ["DP-1", "DP-2"] {
        outputs.insert(
            name.to_string(),
            OutputOrchestrator {
                _name: name.to_string(),
                description: format!("{name} description"),
                phase_offset: independent_phase_offset(name, output_config.duration),
                config: output_config.clone(),
                queue: None,
                current_path: None,
                next_path: None,
                next_content_type: None,
                next_change: None,
                display_start_time: None,
            },
        );
    }

    let mut manager = MonitorManager {
        config: config_for_outputs_with_behavior(
            &[("DP-1", &output_config), ("DP-2", &output_config)],
            MonitorBehavior::Synchronized,
        ),
        outputs,
        shared_queue: Some(shared_queue),
        group_queues: HashMap::new(),
        output_groups: HashMap::new(),
        shared_display_start_time: None,
        group_display_start_times: HashMap::new(),
        cache,
        metrics: None,
        paused: false,
        discovered_files_cache: HashMap::new(),
    };

    let changes = manager.tick();

    assert!(changes.is_empty());
    for name in ["DP-1", "DP-2"] {
        let orch = manager.outputs.get(name).expect("output should exist");
        assert!(orch.current_path.is_none());
        assert!(orch.next_path.is_none());
        assert!(orch.next_content_type.is_none());
        assert!(orch.display_start_time.is_none());
        assert!(orch.next_change.is_none());
    }
}

#[test]
fn grouped_handle_next_skips_unknown_content_type_without_mutating_group() {
    let temp = unique_test_dir("group-unknown-next");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let unknown = path.join("mystery.bin");
    std::fs::write(&unknown, b"short").expect("test file should be written");

    let mut output_config = test_output_config(Duration::from_secs(60));
    output_config.path = Some(path.clone());
    let group_queue = make_test_queue(cache.clone(), &path, vec![unknown], &output_config);

    let mut outputs = HashMap::new();
    for name in ["DP-1", "DP-2"] {
        outputs.insert(
            name.to_string(),
            OutputOrchestrator {
                _name: name.to_string(),
                description: format!("{name} description"),
                phase_offset: independent_phase_offset(name, output_config.duration),
                config: output_config.clone(),
                queue: None,
                current_path: Some(PathBuf::from(format!("/tmp/{name}-current"))),
                next_path: Some(PathBuf::from(format!("/tmp/{name}-next"))),
                next_content_type: Some(crate::queue::ContentType::Image),
                next_change: Some(Instant::now()),
                display_start_time: Some(Instant::now()),
            },
        );
    }

    let mut manager = MonitorManager {
        config: config_for_outputs_with_behavior(
            &[("DP-1", &output_config), ("DP-2", &output_config)],
            MonitorBehavior::Grouped(vec![vec!["DP-1".to_string(), "DP-2".to_string()]]),
        ),
        outputs,
        shared_queue: None,
        group_queues: HashMap::from([(0, group_queue)]),
        output_groups: HashMap::from([("DP-1".to_string(), 0), ("DP-2".to_string(), 0)]),
        shared_display_start_time: None,
        group_display_start_times: HashMap::new(),
        cache,
        metrics: None,
        paused: false,
        discovered_files_cache: HashMap::new(),
    };

    let changes = manager.handle_next(Some("DP-1".to_string()));

    assert!(changes.is_empty());
    for name in ["DP-1", "DP-2"] {
        let orch = manager.outputs.get(name).expect("output should exist");
        assert_eq!(
            orch.current_path,
            Some(PathBuf::from(format!("/tmp/{name}-current")))
        );
        assert_eq!(
            orch.next_path,
            Some(PathBuf::from(format!("/tmp/{name}-next")))
        );
        assert_eq!(
            orch.next_content_type,
            Some(crate::queue::ContentType::Image)
        );
        assert!(orch.display_start_time.is_some());
        assert!(orch.next_change.is_some());
    }
}
