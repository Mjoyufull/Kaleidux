//! Covers bounded retries for retained content that disappears before a cohort join.

use super::*;

#[test]
fn missing_shared_content_defers_paused_joins_and_retries_without_spinning() {
    for behavior in [
        MonitorBehavior::Synchronized,
        MonitorBehavior::Grouped(vec![vec!["DP-1".into(), "DP-2".into()]]),
    ] {
        for power_suspended in [false, true] {
            let root = unique_test_dir("missing-shared-join");
            let cache = Arc::new(FileCache::new_test(&root.join("cache.redb")).unwrap());
            let config = test_output_config(Duration::from_secs(600));
            let mut manager = make_test_manager(
                "DP-1",
                cache,
                OutputOrchestrator::without_queue("DP-1".into(), String::new(), config.clone()),
                config_for_output_with_behavior("DP-1", &config, behavior.clone()),
            );
            manager.outputs.insert(
                "DP-2".into(),
                OutputOrchestrator::without_queue("DP-2".into(), String::new(), config),
            );
            manager.output_groups = HashMap::from([("DP-1".into(), 0), ("DP-2".into(), 0)]);
            manager.set_paused(!power_suspended);
            manager.set_power_suspended(power_suspended);
            let missing = root.join("missing.png");
            manager.outputs.get_mut("DP-1").unwrap().current_path = Some(missing.clone());
            assert!(manager.tick_due(Instant::now()));
            assert!(manager.tick().is_empty());
            let retry = manager.next_switch_deadline().unwrap();
            assert!(retry > Instant::now() + Duration::from_secs(4));
            for _ in 0..100 {
                assert!(!manager.tick_due(Instant::now()));
                assert!(manager.tick().is_empty());
                assert_eq!(manager.next_switch_deadline(), Some(retry));
            }

            image::RgbaImage::new(2, 2).save(&missing).unwrap();
            manager.shared_join_retries.get_mut("DP-2").unwrap().1 = Instant::now();
            assert!(manager.tick_due(Instant::now()));
            assert_eq!(manager.tick()["DP-2"].0, missing);
            assert!(manager.shared_join_retries.is_empty());
            assert!(manager.next_switch_deadline().is_none());
            manager.remove_output("DP-2");
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
