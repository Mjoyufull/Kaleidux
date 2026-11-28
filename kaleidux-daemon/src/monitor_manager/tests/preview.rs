//! Verifies preview cancellation preserves slideshow progress and live pause reasons.

use super::*;

#[test]
fn preview_cancel_restores_selection_and_remaining_time_without_clearing_inhibitors() {
    let root = unique_test_dir("preview-cancel");
    let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
    let config = test_output_config(Duration::from_secs(60));
    let mut output =
        OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
    output.current_path = Some(PathBuf::from("original.png"));
    output.display_start_time = Some(Instant::now() - Duration::from_secs(20));
    let mut manager = make_test_manager("DP-1", cache, output, config_for_output("DP-1", &config));
    let snapshot = manager.snapshot_queues();
    manager.begin_preview();
    manager.outputs.get_mut("DP-1").unwrap().current_path = Some(PathBuf::from("candidate.png"));
    assert!(manager.tick().is_empty());
    assert!(manager.next_switch_deadline().is_none());
    manager.inhibit("gaming".into()).unwrap();
    manager.end_preview(snapshot, false);
    assert_eq!(
        manager.outputs["DP-1"].current_path,
        Some(PathBuf::from("original.png"))
    );
    assert!(manager.is_paused());
    let elapsed = manager.outputs["DP-1"]
        .display_start_time
        .unwrap()
        .elapsed();
    assert!((elapsed.as_secs_f64() - 20.0).abs() < 0.1);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn preview_cancel_does_not_resurrect_unplugged_outputs() {
    let root = unique_test_dir("preview-unplug");
    let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
    let config = test_output_config(Duration::from_secs(60));
    let output = OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
    let mut manager = make_test_manager("DP-1", cache, output, config_for_output("DP-1", &config));
    let snapshot = manager.snapshot_queues();
    manager.begin_preview();
    manager.remove_output("DP-1");
    manager.end_preview(snapshot, false);
    assert!(manager.outputs.is_empty());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn preview_and_output_power_freeze_only_the_union_of_their_intervals() {
    for (off_second, wake_second, expected_shift) in
        [(5, 40, 35), (15, 40, 30), (5, 20, 25), (35, 40, 25)]
    {
        let root = unique_test_dir("preview-power-union");
        let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
        let config = test_output_config(Duration::from_secs(60));
        let origin = Instant::now();
        let at = |second| origin + Duration::from_secs(second);
        let mut output =
            OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
        output.current_path = Some(PathBuf::from("original.png"));
        output.display_start_time = Some(origin);
        let mut manager =
            make_test_manager("DP-1", cache, output, config_for_output("DP-1", &config));
        if off_second < 10 {
            manager.set_output_power_suspended("DP-1", true, at(off_second));
        }
        let snapshot = manager.snapshot_queues_at(at(10));
        manager.begin_preview();
        if (10..30).contains(&off_second) {
            manager.set_output_power_suspended("DP-1", true, at(off_second));
        }
        if wake_second < 30 {
            manager.set_output_power_suspended("DP-1", false, at(wake_second));
        }
        manager.end_preview_at(snapshot, false, at(30));
        if off_second >= 30 {
            manager.set_output_power_suspended("DP-1", true, at(off_second));
        }
        if wake_second >= 30 {
            manager.set_output_power_suspended("DP-1", false, at(wake_second));
        }
        assert_eq!(
            manager.outputs["DP-1"].display_start_time,
            Some(at(expected_shift))
        );
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn preview_retirement_keeps_a_same_name_replacement_selection() {
    let root = unique_test_dir("preview-replacement");
    let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
    let config = test_output_config(Duration::from_secs(60));
    let output = OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
    let mut manager = make_test_manager("DP-1", cache, output, config_for_output("DP-1", &config));
    let mut snapshot = manager.snapshot_queues();
    manager.begin_preview();
    manager.remove_output("DP-1");
    snapshot.retire_output("DP-1");
    let mut replacement = OutputOrchestrator::without_queue("DP-1".into(), "new".into(), config);
    replacement.current_path = Some(PathBuf::from("replacement.png"));
    manager.outputs.insert("DP-1".into(), replacement);
    manager.end_preview(snapshot, false);
    assert_eq!(
        manager.outputs["DP-1"].current_path,
        Some(PathBuf::from("replacement.png"))
    );
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}
