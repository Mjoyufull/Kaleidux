//! Verifies per-output power pauses without stopping an independent visible peer.

use super::*;

fn manager_with_visible_peer(behavior: MonitorBehavior) -> (MonitorManager, PathBuf) {
    let root = unique_test_dir("output-power");
    let cache = Arc::new(FileCache::new_test(&root.join("cache.redb")).unwrap());
    let image = write_test_image(&root, "current.png");
    let mut config = test_output_config(Duration::from_secs(600));
    config.performance = crate::orchestration::PerformanceProfile::LowPower;
    let mut output =
        OutputOrchestrator::without_queue("DP-1".into(), String::new(), config.clone());
    output.queue = Some(
        SmartQueue::new_manual_from_pool(
            &root,
            vec![image.clone()],
            0,
            SortingStrategy::Ascending,
            cache.clone(),
        )
        .unwrap(),
    );
    output.current_path = Some(image.clone());
    output.display_start_time = Some(Instant::now());
    output.next_change = Some(Instant::now() + config.duration);
    let mut manager = make_test_manager(
        "DP-1",
        cache,
        output,
        config_for_output_with_behavior("DP-1", &config, behavior),
    );
    let mut peer = OutputOrchestrator::without_queue("DP-2".into(), String::new(), config);
    peer.current_path = Some(image);
    peer.display_start_time = Some(Instant::now());
    peer.next_change = Some(Instant::now() + Duration::from_secs(600));
    manager.outputs.insert("DP-2".into(), peer);
    manager.output_groups.insert("DP-2".into(), 0);
    (manager, root)
}

#[test]
fn powered_off_independent_and_ungrouped_outputs_do_not_advance_or_wake() {
    for behavior in [
        MonitorBehavior::Independent,
        MonitorBehavior::Grouped(vec![vec!["DP-2".into()]]),
    ] {
        let (mut manager, root) = manager_with_visible_peer(behavior);
        let now = Instant::now();
        let output = manager.outputs.get_mut("DP-1").unwrap();
        output.display_start_time = Some(now - Duration::from_secs(601));
        output.next_change = Some(now - Duration::from_secs(1));
        manager.set_output_power_suspended("DP-1", true, now);
        assert!(!manager.tick_due(now));
        assert!(manager.next_switch_deadline().unwrap() > now);
        assert!(manager.due_low_power_outputs(now).is_empty());
        assert!(manager.tick().is_empty());
        let blank = manager.outputs.get_mut("DP-1").unwrap();
        blank.current_path = None;
        blank.display_start_time = None;
        blank.next_change = None;
        assert!(!manager.tick_due(now));
        assert!(
            manager.tick().is_empty(),
            "off output must not prepare initial content"
        );
        manager.set_output_power_suspended("DP-1", false, now + Duration::from_secs(60));
        assert!(manager.tick_due(Instant::now()));
        assert!(manager.tick().contains_key("DP-1"));
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn individual_resume_preserves_remaining_duration_and_manual_reset_does_not_double_count() {
    let (mut manager, root) = manager_with_visible_peer(MonitorBehavior::Independent);
    let now = Instant::now();
    let before = manager.outputs["DP-1"].display_start_time.unwrap();
    let peer_before = manager.outputs["DP-2"].display_start_time;
    manager.set_output_power_suspended("DP-1", true, now);
    manager.set_output_power_suspended("DP-1", false, now + Duration::from_secs(60));
    assert_eq!(
        manager.outputs["DP-1"].display_start_time,
        Some(before + Duration::from_secs(60))
    );
    assert_eq!(manager.outputs["DP-2"].display_start_time, peer_before);
    manager.set_output_power_suspended("DP-1", true, now - Duration::from_secs(10));
    manager.set_paused(true);
    manager.set_paused(false);
    let reset_start = manager.outputs["DP-1"].display_start_time.unwrap();
    assert_eq!(manager.output_power_paused["DP-1"], reset_start);
    manager.set_output_power_suspended("DP-1", false, reset_start + Duration::from_secs(30));
    assert_eq!(
        manager.outputs["DP-1"].display_start_time,
        Some(reset_start + Duration::from_secs(30))
    );
    manager.remove_output("DP-1");
    assert!(!manager.output_power_paused.contains_key("DP-1"));
    std::fs::remove_dir_all(root).unwrap();
}
