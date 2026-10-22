//! Checks blank-cohort rollback and targeted apply without extending other timers.

use super::*;

#[test]
fn cancelling_a_blank_cohort_removes_its_candidate_queue() {
    for grouped in [false, true] {
        let root = unique_test_dir("preview-blank-cohort");
        let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
        let config = test_output_config(Duration::from_secs(60));
        let behavior = if grouped {
            MonitorBehavior::Grouped(vec![vec!["DP-1".into()]])
        } else {
            MonitorBehavior::Synchronized
        };
        let output =
            OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
        let mut manager = make_test_manager(
            "DP-1",
            cache,
            output,
            config_for_output_with_behavior("DP-1", &config, behavior),
        );
        if grouped {
            manager.output_groups.insert("DP-1".into(), 0);
        }
        let snapshot = manager.snapshot_queues();
        manager.begin_preview();
        manager
            .prepare_media_selection("#112233", &Some("DP-1".into()), Some(false))
            .unwrap();
        assert!(!manager.handle_next(Some("DP-1".into())).is_empty());
        manager.end_preview(snapshot, false);
        assert!(manager.outputs["DP-1"].current_path.is_none());
        assert!(manager.shared_queue.is_none());
        assert!(manager.group_queues.is_empty());
        assert!(
            manager.tick().is_empty(),
            "cancelled colour was selected again"
        );
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn applying_one_preview_preserves_other_outputs_remaining_duration() {
    for grouped in [false, true] {
        let root = unique_test_dir("preview-targeted-apply");
        let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
        let config = test_output_config(Duration::from_secs(60));
        let behavior = if grouped {
            MonitorBehavior::Grouped(vec![vec!["DP-1".into()], vec!["DP-2".into()]])
        } else {
            MonitorBehavior::Independent
        };
        let origin = Instant::now();
        let at = |seconds| origin + Duration::from_secs(seconds);
        let mut output =
            OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
        output.current_path = Some(PathBuf::from("original.png"));
        output.display_start_time = Some(origin);
        output.phase_offset = Duration::ZERO;
        let mut manager = make_test_manager(
            "DP-1",
            cache,
            output.clone(),
            config_for_output_with_behavior("DP-1", &config, behavior),
        );
        manager.outputs.insert("DP-2".into(), output);
        if grouped {
            manager
                .output_groups
                .extend([("DP-1".into(), 0), ("DP-2".into(), 1)]);
            manager
                .group_display_start_times
                .extend([(0, origin), (1, origin)]);
        }
        let mut snapshot = manager.snapshot_queues_at(at(40));
        snapshot.set_targets(std::iter::once("DP-1".into()));
        manager.begin_preview();
        manager.outputs.get_mut("DP-1").unwrap().current_path =
            Some(PathBuf::from("candidate.png"));
        manager.end_preview_at(snapshot, true, at(60));
        assert_eq!(
            manager.outputs["DP-1"].current_path,
            Some(PathBuf::from("candidate.png"))
        );
        assert_eq!(manager.outputs["DP-1"].display_start_time, Some(at(60)));
        assert_eq!(manager.outputs["DP-2"].display_start_time, Some(at(20)));
        assert_eq!(manager.next_switch_deadline(), Some(at(80)));
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn cancellation_preserves_a_new_cohort_after_the_original_disconnected() {
    for grouped in [false, true] {
        let root = unique_test_dir("preview-new-cohort");
        let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
        let config = test_output_config(Duration::from_secs(60));
        let behavior = if grouped {
            MonitorBehavior::Grouped(vec![vec!["DP-1".into()]])
        } else {
            MonitorBehavior::Synchronized
        };
        let output =
            OutputOrchestrator::without_queue("DP-1".into(), "test".into(), config.clone());
        let mut manager = make_test_manager(
            "DP-1",
            cache,
            output.clone(),
            config_for_output_with_behavior("DP-1", &config, behavior),
        );
        if grouped {
            manager.output_groups.insert("DP-1".into(), 0);
        }
        let mut snapshot = manager.snapshot_queues();
        manager.begin_preview();
        manager.remove_output("DP-1");
        snapshot.retire_output("DP-1");
        manager.outputs.insert("DP-1".into(), output);
        if grouped {
            manager.output_groups.insert("DP-1".into(), 0);
        }
        manager
            .prepare_media_selection("#112233", &Some("DP-1".into()), Some(false))
            .unwrap();
        assert!(!manager.handle_next(Some("DP-1".into())).is_empty());
        let selected = manager.outputs["DP-1"].current_path.clone();
        manager.end_preview(snapshot, false);
        assert_eq!(manager.outputs["DP-1"].current_path, selected);
        assert!(if grouped {
            manager.group_queues.contains_key(&0)
        } else {
            manager.shared_queue.is_some()
        });
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}
