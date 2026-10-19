//! Exercises asynchronous output admission, cancellation and manual-selection preservation.

use super::*;
use std::sync::Arc;

fn fixture() -> (MonitorManager, PathBuf, PathBuf) {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let root = std::env::temp_dir().join(format!(
        "kld-deferred-output-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let image = root.join("initial.png");
    image::RgbaImage::new(2, 2).save(&image).unwrap();
    let config = toml::from_str(&format!("[any]\npath = {:?}\n", root.to_str().unwrap())).unwrap();
    let cache = Arc::new(crate::cache::FileCache::new_test(&root.join("stats.redb")).unwrap());
    let manager = MonitorManager {
        config,
        cache,
        outputs: HashMap::new(),
        shared_queue: None,
        group_queues: HashMap::new(),
        output_groups: HashMap::new(),
        shared_display_start_time: None,
        group_display_start_times: HashMap::new(),
        metrics: None,
        manual_paused: false,
        pause_reasons: Default::default(),
        power_suspended: false,
        discovered_files_cache: HashMap::new(),
        pending_output_queues: Default::default(),
        shared_join_retries: Default::default(),
        output_power_paused: Default::default(),
    };
    (manager, root, image)
}

fn hold_queue(
    manager: &mut MonitorManager,
    name: &str,
    image: PathBuf,
) -> tokio::sync::oneshot::Sender<()> {
    manager.add_output_deferred(name, "test output");
    let target = manager.queue_target(name);
    let pending = manager
        .pending_output_queues
        .tasks
        .get_mut(&target)
        .unwrap();
    pending.task.abort();
    let config = pending.config.clone();
    let cache = manager.cache.clone();
    let (release, waiting) = tokio::sync::oneshot::channel();
    pending.task = tokio::spawn(async move {
        waiting.await?;
        let path = config.path.unwrap();
        let queue = SmartQueue::new_from_pool(
            &path,
            vec![image],
            config.video_ratio,
            config.sorting,
            cache,
        )?;
        let snapshot = queue.root_pool_snapshot();
        Ok((queue, snapshot))
    });
    release
}

async fn finish(manager: &mut MonitorManager) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !manager.pending_output_queues.tasks.is_empty() {
            tokio::task::yield_now().await;
            manager.poll_output_queues().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn incomplete_discovery_does_not_block_output_registration_or_polling() {
    let (mut manager, root, image) = fixture();
    let release = hold_queue(&mut manager, "DP-1", image.clone());
    assert!(manager.outputs.contains_key("DP-1"));
    assert!(manager.outputs["DP-1"].queue.is_none());
    tokio::time::timeout(Duration::from_millis(25), manager.poll_output_queues())
        .await
        .unwrap();
    assert!(manager.output_queue_deadline(Instant::now()).is_some());
    release.send(()).unwrap();
    finish(&mut manager).await;
    assert_eq!(manager.handle_next(None)["DP-1"].0, image);
    assert!(manager.output_queue_deadline(Instant::now()).is_none());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn removed_output_cancels_its_waiting_discovery() {
    let (mut manager, root, image) = fixture();
    let mut release = hold_queue(&mut manager, "DP-1", image);
    manager.remove_output("DP-1");
    tokio::time::timeout(Duration::from_millis(100), release.closed())
        .await
        .unwrap();
    assert!(manager.pending_output_queues.tasks.is_empty());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn completed_discovery_preserves_a_manual_queue_created_while_waiting() {
    let (mut manager, root, image) = fixture();
    let release = hold_queue(&mut manager, "DP-1", image);
    let selected = root.join("selected.png");
    image::RgbaImage::new(3, 2).save(&selected).unwrap();
    manager
        .prepare_media_selection(selected.to_str().unwrap(), &None, Some(false))
        .unwrap();
    release.send(()).unwrap();
    finish(&mut manager).await;
    assert_eq!(manager.handle_next(None)["DP-1"].0, selected);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn synchronized_hotplug_outputs_share_one_discovery_task() {
    let (mut manager, root, image) = fixture();
    manager.config.global.monitor_behavior = MonitorBehavior::Synchronized;
    let release = hold_queue(&mut manager, "DP-1", image.clone());
    manager.add_output_deferred("DP-2", "second output");
    assert_eq!(manager.pending_output_queues.tasks.len(), 1);
    release.send(()).unwrap();
    finish(&mut manager).await;
    let changes = manager.handle_next(None);
    assert_eq!(changes["DP-1"].0, image);
    assert_eq!(changes["DP-2"].0, image);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn shared_discovery_survives_one_unplug_and_cancels_after_the_last() {
    let (mut manager, root, image) = fixture();
    manager.config.global.monitor_behavior = MonitorBehavior::Synchronized;
    let mut release = hold_queue(&mut manager, "DP-1", image);
    manager.add_output_deferred("DP-2", "second output");
    manager.remove_output("DP-1");
    assert_eq!(manager.pending_output_queues.tasks.len(), 1);
    manager.remove_output("DP-2");
    tokio::time::timeout(Duration::from_millis(100), release.closed())
        .await
        .unwrap();
    assert!(manager.pending_output_queues.tasks.is_empty());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn shared_queue_uses_a_configured_owner_after_a_pathless_output() {
    for grouped in [false, true] {
        let (mut manager, root, image) = fixture();
        manager.config.any.path = None;
        manager.config.outputs.insert(
            "DP-2".into(),
            crate::orchestration::PartialOutputConfig {
                path: Some(root.clone()),
                ..Default::default()
            },
        );
        manager.config.global.monitor_behavior = if grouped {
            MonitorBehavior::Grouped(vec![vec!["DP-1".into(), "DP-2".into()]])
        } else {
            MonitorBehavior::Synchronized
        };
        manager.add_output_deferred("DP-1", "pathless output");
        assert!(manager.pending_output_queues.tasks.is_empty());
        manager.add_output_deferred("DP-2", "configured output");
        assert_eq!(manager.pending_output_queues.tasks.len(), 1);
        finish(&mut manager).await;
        let changes = manager.handle_next(None);
        assert_eq!(changes["DP-1"].0, image);
        assert_eq!(changes["DP-2"].0, image);
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn unplugging_owner_restarts_discovery_with_survivors_configuration() {
    let (mut manager, root, image) = fixture();
    let survivor_root = root.join("survivor");
    std::fs::create_dir(&survivor_root).unwrap();
    let survivor_image = survivor_root.join("survivor.png");
    image::RgbaImage::new(2, 2).save(&survivor_image).unwrap();
    manager.config.outputs.insert(
        "DP-2".into(),
        crate::orchestration::PartialOutputConfig {
            path: Some(survivor_root),
            ..Default::default()
        },
    );
    manager.config.global.monitor_behavior = MonitorBehavior::Synchronized;
    let mut release = hold_queue(&mut manager, "DP-1", image);
    manager.add_output_deferred("DP-2", "survivor");
    manager.remove_output("DP-1");
    tokio::time::timeout(Duration::from_millis(100), release.closed())
        .await
        .unwrap();
    finish(&mut manager).await;
    assert_eq!(manager.handle_next(None)["DP-2"].0, survivor_image);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn only_synchronized_or_grouped_peers_share_playback() {
    let (mut manager, root, _) = fixture();
    assert!(!manager.shares_playback_with("DP-1", "DP-2"));
    manager.config.global.monitor_behavior = MonitorBehavior::Synchronized;
    assert!(manager.shares_playback_with("DP-1", "DP-2"));
    manager.config.global.monitor_behavior =
        MonitorBehavior::Grouped(vec![vec!["DP-1".into(), "DP-2".into()]]);
    manager.add_output_deferred("DP-1", "first");
    manager.add_output_deferred("DP-2", "second");
    manager.add_output_deferred("DP-3", "independent");
    assert!(manager.shares_playback_with("DP-1", "DP-2"));
    assert!(!manager.shares_playback_with("DP-1", "DP-3"));
    assert!(!manager.shares_playback_with("DP-3", "DP-4"));
    finish(&mut manager).await;
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn unavailable_hotplug_root_retries_and_recovers_without_replug() {
    let (mut manager, root, image) = fixture();
    std::fs::remove_file(&image).unwrap();
    manager.add_output_deferred("DP-1", "empty root");
    finish(&mut manager).await;
    assert!(manager.outputs["DP-1"].queue.is_none());
    assert_eq!(manager.pending_output_queues.retries.len(), 1);
    assert!(manager.output_queue_deadline(Instant::now()).is_some());
    image::RgbaImage::new(2, 2).save(&image).unwrap();
    tokio::time::sleep(Duration::from_millis(260)).await;
    manager.poll_output_queues().await;
    finish(&mut manager).await;
    assert_eq!(manager.handle_next(None)["DP-1"].0, image);
    assert!(manager.pending_output_queues.retries.is_empty());
    assert!(manager.output_queue_deadline(Instant::now()).is_none());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn unplug_cancels_deferred_retry() {
    let (mut manager, root, image) = fixture();
    std::fs::remove_file(image).unwrap();
    manager.add_output_deferred("DP-1", "empty root");
    finish(&mut manager).await;
    assert!(!manager.pending_output_queues.retries.is_empty());
    manager.remove_output("DP-1");
    assert!(manager.pending_output_queues.retries.is_empty());
    assert!(manager.output_queue_deadline(Instant::now()).is_none());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn manual_selection_does_not_replace_a_later_hotplug_directory_pool() {
    let (mut manager, root, image) = fixture();
    manager.config.any.path = None;
    manager.add_output_deferred("DP-1", "manual output");
    manager
        .prepare_media_selection(image.to_str().unwrap(), &Some("DP-1".into()), Some(false))
        .unwrap();
    let second = root.join("second.png");
    image::RgbaImage::new(2, 2).save(&second).unwrap();
    manager.config.outputs.insert(
        "DP-2".into(),
        crate::orchestration::PartialOutputConfig {
            path: Some(root.clone()),
            ..Default::default()
        },
    );
    manager.add_output_deferred("DP-2", "directory output");
    finish(&mut manager).await;
    let pool = &manager.outputs["DP-2"].queue.as_ref().unwrap().pool;
    assert_eq!(pool.len(), 2);
    assert!(pool.contains(&image) && pool.contains(&second));
    assert_eq!(
        manager.outputs["DP-1"].queue.as_ref().unwrap().pool,
        vec![image]
    );
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn pending_hotplug_discovery_does_not_lose_watcher_additions() {
    let (mut manager, root, image) = fixture();
    let release = hold_queue(&mut manager, "DP-1", image.clone());
    let added = root.join("added.png");
    image::RgbaImage::new(2, 2).save(&added).unwrap();
    manager.apply_pool_events(vec![crate::cache::PoolEvent::Added(added.clone())]);
    let index = crate::queue::media_index::shared_root_index(&root);
    tokio::time::timeout(Duration::from_secs(2), async {
        while index.rescan_running() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    finish(&mut manager).await;
    let pool = &manager.outputs["DP-1"].queue.as_ref().unwrap().pool;
    assert_eq!(pool.len(), 2);
    assert!(pool.contains(&image) && pool.contains(&added));
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}
