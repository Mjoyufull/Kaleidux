use super::*;

#[test]
fn update_config_rebuilds_same_path_queue_for_sorting_and_playlist_changes() {
    let temp = unique_test_dir("same-path-reload");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let first = write_test_image(&path, "a.png");
    let second = write_test_image(&path, "b.png");
    cache
        .set_cached_pool(&path, &[first.clone(), second.clone()])
        .expect("cached pool should be stored");
    cache
        .set_playlist(
            "favorites",
            &Playlist {
                paths: vec![second.clone()],
                strategy: SortingStrategy::Descending,
                enabled: true,
            },
        )
        .expect("playlist should be stored");

    let mut old_config = test_output_config(Duration::from_secs(60));
    old_config.path = Some(path.clone());
    let old_queue = make_test_queue(
        cache.clone(),
        &path,
        vec![first.clone(), second.clone()],
        &old_config,
    );
    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", old_config.duration),
        config: old_config.clone(),
        queue: Some(old_queue),
        current_path: Some(first.clone()),
        next_path: Some(second.clone()),
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output("DP-1", &old_config),
    );

    let mut new_config = old_config.clone();
    new_config.sorting = SortingStrategy::Ascending;
    new_config.default_playlist = Some("favorites".to_string());

    manager.update_config(config_for_output("DP-1", &new_config));

    let orch = manager.outputs.get("DP-1").expect("output should exist");
    let queue = orch.queue.as_ref().expect("queue should be rebuilt");
    assert_eq!(queue.strategy, SortingStrategy::Ascending);
    assert_eq!(queue.active_playlist.as_deref(), Some("favorites"));
    assert_eq!(queue.pool, vec![second.clone()]);
    assert!(orch.current_path.is_none());
    assert!(orch.next_path.is_none());
    assert!(orch.display_start_time.is_none());
    assert!(orch.next_change.is_none());
}

#[test]
fn update_config_rebuilds_synchronized_shared_queue() {
    let temp = unique_test_dir("sync-reload");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let first = write_test_image(&path, "a.png");
    let second = write_test_image(&path, "b.png");
    cache
        .set_cached_pool(&path, &[first.clone(), second.clone()])
        .expect("cached pool should be stored");
    cache
        .set_playlist(
            "favorites",
            &Playlist {
                paths: vec![second.clone()],
                strategy: SortingStrategy::Descending,
                enabled: true,
            },
        )
        .expect("playlist should be stored");

    let mut old_config = test_output_config(Duration::from_secs(60));
    old_config.path = Some(path.clone());
    let shared_queue = make_test_queue(
        cache.clone(),
        &path,
        vec![first.clone(), second.clone()],
        &old_config,
    );
    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", old_config.duration),
        config: old_config.clone(),
        queue: None,
        current_path: Some(first.clone()),
        next_path: Some(second.clone()),
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output_with_behavior("DP-1", &old_config, MonitorBehavior::Synchronized),
    );
    manager.shared_queue = Some(shared_queue);
    manager.shared_display_start_time = Some(Instant::now());

    let mut new_config = old_config.clone();
    new_config.sorting = SortingStrategy::Ascending;
    new_config.default_playlist = Some("favorites".to_string());

    manager.update_config(config_for_output_with_behavior(
        "DP-1",
        &new_config,
        MonitorBehavior::Synchronized,
    ));

    let shared_queue = manager
        .shared_queue
        .as_ref()
        .expect("shared queue should be rebuilt");
    assert_eq!(shared_queue.strategy, SortingStrategy::Ascending);
    assert_eq!(shared_queue.active_playlist.as_deref(), Some("favorites"));
    assert_eq!(shared_queue.pool, vec![second.clone()]);
    assert!(manager.shared_display_start_time.is_none());

    let orch = manager.outputs.get("DP-1").expect("output should exist");
    assert!(orch.queue.is_none());
    assert!(orch.current_path.is_none());
    assert!(orch.next_path.is_none());
    assert!(orch.display_start_time.is_none());
    assert!(orch.next_change.is_none());
}

#[test]
fn synchronized_queue_ignores_non_representative_output_queue_changes() {
    let temp = unique_test_dir("sync-non-representative-change");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let primary_path = temp.join("primary");
    let secondary_path = temp.join("secondary");
    std::fs::create_dir_all(&primary_path).expect("primary dir should be created");
    std::fs::create_dir_all(&secondary_path).expect("secondary dir should be created");
    let primary_file = write_test_image(&primary_path, "primary.png");
    let secondary_file = write_test_image(&secondary_path, "secondary.png");
    cache
        .set_cached_pool(&primary_path, std::slice::from_ref(&primary_file))
        .expect("primary pool should be stored");
    cache
        .set_cached_pool(&secondary_path, std::slice::from_ref(&secondary_file))
        .expect("secondary pool should be stored");

    let mut primary_config = test_output_config(Duration::from_secs(60));
    primary_config.path = Some(primary_path.clone());
    primary_config.sorting = SortingStrategy::Ascending;
    let mut secondary_config = test_output_config(Duration::from_secs(60));
    secondary_config.path = Some(secondary_path);
    secondary_config.sorting = SortingStrategy::Descending;
    let shared_queue = make_test_queue(
        cache.clone(),
        &primary_path,
        vec![primary_file.clone()],
        &primary_config,
    );
    let primary = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "Primary".to_string(),
        phase_offset: Duration::ZERO,
        config: primary_config.clone(),
        queue: None,
        current_path: Some(primary_file),
        next_path: None,
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let secondary = OutputOrchestrator {
        _name: "DP-2".to_string(),
        description: "Secondary".to_string(),
        phase_offset: Duration::ZERO,
        config: secondary_config.clone(),
        queue: None,
        current_path: Some(secondary_file),
        next_path: None,
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let initial_config = config_for_outputs_with_behavior(
        &[("DP-1", &primary_config), ("DP-2", &secondary_config)],
        MonitorBehavior::Synchronized,
    );
    let mut manager = make_test_manager("DP-1", cache, primary, initial_config);
    manager.outputs.insert("DP-2".to_string(), secondary);
    manager.shared_queue = Some(shared_queue);
    let original_start = Instant::now();
    manager.shared_display_start_time = Some(original_start);

    let mut changed_secondary = secondary_config;
    changed_secondary.sorting = SortingStrategy::Random;
    manager.update_config(config_for_outputs_with_behavior(
        &[("DP-1", &primary_config), ("DP-2", &changed_secondary)],
        MonitorBehavior::Synchronized,
    ));

    let shared_queue = manager.shared_queue.as_ref().expect("shared queue remains");
    assert_eq!(shared_queue.root_path, primary_path);
    assert_eq!(shared_queue.strategy, SortingStrategy::Ascending);
    assert_eq!(manager.shared_display_start_time, Some(original_start));
    assert_eq!(
        manager.outputs["DP-2"].config.sorting,
        SortingStrategy::Random
    );
}

#[test]
fn update_config_rebuilds_group_queue() {
    let temp = unique_test_dir("group-reload");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let first = write_test_image(&path, "a.png");
    let second = write_test_image(&path, "b.png");
    cache
        .set_cached_pool(&path, &[first.clone(), second.clone()])
        .expect("cached pool should be stored");
    cache
        .set_playlist(
            "favorites",
            &Playlist {
                paths: vec![second.clone()],
                strategy: SortingStrategy::Descending,
                enabled: true,
            },
        )
        .expect("playlist should be stored");

    let mut old_config = test_output_config(Duration::from_secs(60));
    old_config.path = Some(path.clone());
    let group_queue = make_test_queue(
        cache.clone(),
        &path,
        vec![first.clone(), second.clone()],
        &old_config,
    );
    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", old_config.duration),
        config: old_config.clone(),
        queue: None,
        current_path: Some(first.clone()),
        next_path: Some(second.clone()),
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output_with_behavior(
            "DP-1",
            &old_config,
            MonitorBehavior::Grouped(vec![vec!["DP-1".to_string()]]),
        ),
    );
    manager.output_groups.insert("DP-1".to_string(), 0);
    manager.group_queues.insert(0, group_queue);
    manager.group_display_start_times.insert(0, Instant::now());

    let mut new_config = old_config.clone();
    new_config.sorting = SortingStrategy::Ascending;
    new_config.default_playlist = Some("favorites".to_string());

    manager.update_config(config_for_output_with_behavior(
        "DP-1",
        &new_config,
        MonitorBehavior::Grouped(vec![vec!["DP-1".to_string()]]),
    ));

    let group_queue = manager
        .group_queues
        .get(&0)
        .expect("group queue should be rebuilt");
    assert_eq!(group_queue.strategy, SortingStrategy::Ascending);
    assert_eq!(group_queue.active_playlist.as_deref(), Some("favorites"));
    assert_eq!(group_queue.pool, vec![second.clone()]);
    assert!(!manager.group_display_start_times.contains_key(&0));

    let orch = manager.outputs.get("DP-1").expect("output should exist");
    assert!(orch.queue.is_none());
    assert!(orch.current_path.is_none());
    assert!(orch.next_path.is_none());
    assert!(orch.display_start_time.is_none());
    assert!(orch.next_change.is_none());
}

#[test]
fn update_config_flushes_pending_stats_before_replacing_queue() {
    let temp = unique_test_dir("flush-stats");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let file = write_test_image(&path, "a.png");
    cache
        .set_cached_pool(&path, std::slice::from_ref(&file))
        .expect("cached pool should be stored");

    let mut old_config = test_output_config(Duration::from_secs(60));
    old_config.path = Some(path.clone());
    old_config.sorting = SortingStrategy::Ascending;
    let mut old_queue = make_test_queue(cache.clone(), &path, vec![file.clone()], &old_config);
    assert_eq!(old_queue.pick_next(), Some(file.clone()));

    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", old_config.duration),
        config: old_config.clone(),
        queue: Some(old_queue),
        current_path: Some(file.clone()),
        next_path: None,
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output("DP-1", &old_config),
    );

    let mut new_config = old_config.clone();
    new_config.default_playlist = Some("missing".to_string());

    manager.update_config(config_for_output("DP-1", &new_config));

    let stats = cache
        .get_all_file_stats()
        .expect("file stats should be readable");
    let stat = stats
        .get(&file)
        .expect("picked file stats should be flushed");
    assert_eq!(stat.count, 1);
    assert!(stat.last_seen.is_some());
}

#[test]
fn update_config_ignores_corrupt_stats_and_refreshes_queue() {
    let temp = unique_test_dir("refresh-failure");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let old_path = temp.join("old");
    std::fs::create_dir_all(&old_path).expect("old dir should be created");
    let file = write_test_image(&old_path, "old.png");
    cache
        .set_cached_pool(&old_path, std::slice::from_ref(&file))
        .expect("cached pool should be stored");

    let mut old_config = test_output_config(Duration::from_secs(60));
    old_config.path = Some(old_path.clone());
    let old_queue = make_test_queue(cache.clone(), &old_path, vec![file.clone()], &old_config);
    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", old_config.duration),
        config: old_config.clone(),
        queue: Some(old_queue),
        current_path: Some(file.clone()),
        next_path: None,
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output("DP-1", &old_config),
    );

    let bad_stats_path = temp.join("bad-stats");
    cache
        .insert_invalid_file_stats_bytes(&bad_stats_path, &[0xff, 0x00, 0x01])
        .expect("invalid stats bytes should be inserted");

    let new_path = temp.join("new");
    std::fs::create_dir_all(&new_path).expect("new dir should be created");
    let new_file = write_test_image(&new_path, "new.png");
    cache
        .set_cached_pool(&new_path, std::slice::from_ref(&new_file))
        .expect("new cached pool should be stored");

    let mut new_config = old_config.clone();
    new_config.path = Some(new_path.clone());

    manager.update_config(config_for_output("DP-1", &new_config));

    let orch = manager.outputs.get("DP-1").expect("output should exist");
    assert!(
        orch.queue.is_some(),
        "new queue should still be constructed"
    );
    assert_eq!(orch.config.path, Some(new_path));
    assert!(orch.current_path.is_none());
    assert!(orch.display_start_time.is_none());
    assert!(orch.next_change.is_none());
    assert!(
        cache
            .get_file_stats(&bad_stats_path)
            .expect("corrupt stats lookup")
            .is_none()
    );
}

#[test]
fn update_config_switches_from_synchronized_to_independent() {
    let temp = unique_test_dir("sync-to-independent");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let first = write_test_image(&path, "a.png");
    let second = write_test_image(&path, "b.png");
    cache
        .set_cached_pool(&path, &[first.clone(), second.clone()])
        .expect("cached pool should be stored");

    let mut output_config = test_output_config(Duration::from_secs(60));
    output_config.path = Some(path.clone());
    let shared_queue = make_test_queue(
        cache.clone(),
        &path,
        vec![first.clone(), second.clone()],
        &output_config,
    );
    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", output_config.duration),
        config: output_config.clone(),
        queue: None,
        current_path: Some(first),
        next_path: Some(second),
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output_with_behavior("DP-1", &output_config, MonitorBehavior::Synchronized),
    );
    manager.shared_queue = Some(shared_queue);
    manager.shared_display_start_time = Some(Instant::now());

    manager.update_config(config_for_output_with_behavior(
        "DP-1",
        &output_config,
        MonitorBehavior::Independent,
    ));

    assert!(manager.shared_queue.is_none());
    assert!(manager.group_queues.is_empty());
    let orch = manager.outputs.get("DP-1").expect("output should exist");
    assert!(
        orch.queue.is_some(),
        "independent mode should rebuild per-output queue"
    );
    assert!(orch.current_path.is_none());
    assert!(orch.next_path.is_none());
    assert!(orch.display_start_time.is_none());
}

#[test]
fn update_config_switches_from_independent_to_synchronized() {
    let temp = unique_test_dir("independent-to-sync");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let path = temp.join("wallpapers");
    std::fs::create_dir_all(&path).expect("wallpaper dir should be created");
    let first = write_test_image(&path, "a.png");
    let second = write_test_image(&path, "b.png");
    cache
        .set_cached_pool(&path, &[first.clone(), second.clone()])
        .expect("cached pool should be stored");

    let mut output_config = test_output_config(Duration::from_secs(60));
    output_config.path = Some(path.clone());
    let queue = make_test_queue(
        cache.clone(),
        &path,
        vec![first.clone(), second.clone()],
        &output_config,
    );
    let orch = OutputOrchestrator {
        _name: "DP-1".to_string(),
        description: "DisplayPort-1".to_string(),
        phase_offset: independent_phase_offset("DP-1", output_config.duration),
        config: output_config.clone(),
        queue: Some(queue),
        current_path: Some(first),
        next_path: Some(second),
        next_content_type: Some(crate::queue::ContentType::Image),
        next_change: Some(Instant::now()),
        display_start_time: Some(Instant::now()),
    };
    let mut manager = make_test_manager(
        "DP-1",
        cache.clone(),
        orch,
        config_for_output("DP-1", &output_config),
    );

    manager.update_config(config_for_output_with_behavior(
        "DP-1",
        &output_config,
        MonitorBehavior::Synchronized,
    ));

    assert!(
        manager.shared_queue.is_some(),
        "synchronized mode should rebuild shared queue"
    );
    assert!(manager.group_queues.is_empty());
    let orch = manager.outputs.get("DP-1").expect("output should exist");
    assert!(orch.queue.is_none());
    assert!(orch.current_path.is_none());
    assert!(orch.next_path.is_none());
    assert!(orch.display_start_time.is_none());
}
