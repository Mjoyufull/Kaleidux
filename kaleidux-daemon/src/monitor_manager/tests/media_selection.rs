use super::*;

fn selection_manager(label: &str) -> (MonitorManager, PathBuf, PathBuf) {
    let root = unique_test_dir(label);
    let original = write_test_image(&root, "a.png");
    let cache = Arc::new(FileCache::new_test(&root.join("stats.redb")).unwrap());
    let cfg = test_output_config(Duration::from_secs(60));
    let mut orch = OutputOrchestrator::without_queue("DP-1".into(), "test".into(), cfg.clone());
    orch.queue = Some(make_test_queue(
        cache.clone(),
        &root,
        vec![original.clone()],
        &cfg,
    ));
    (
        make_test_manager("DP-1", cache, orch, config_for_output("DP-1", &cfg)),
        root,
        original,
    )
}

#[test]
fn mislabeled_images_are_selected_and_corrupt_headers_leave_history_unchanged() {
    let (mut manager, root, original) = selection_manager("sniff-command");
    let renamed = root.join("renamed.png");
    image::RgbImage::new(3, 2)
        .save_with_format(&renamed, image::ImageFormat::Jpeg)
        .unwrap();
    let corrupt = root.join("corrupt.png");
    std::fs::write(&corrupt, b"broken PNG").unwrap();
    manager
        .prepare_media_selection(original.to_str().unwrap(), &None, None)
        .unwrap();
    manager.handle_next(None);
    manager
        .prepare_media_selection(renamed.to_str().unwrap(), &None, None)
        .unwrap();
    assert_eq!(manager.handle_next(None)["DP-1"].0, renamed);
    assert!(
        manager
            .prepare_media_selection(corrupt.to_str().unwrap(), &None, None)
            .is_err()
    );
    assert_eq!(
        manager.outputs["DP-1"].current_path.as_ref(),
        Some(&renamed)
    );
    assert_eq!(manager.handle_prev(None)["DP-1"].0, original);
}

#[cfg(any(
    feature = "backend-ffmpeg",
    feature = "backend-mpv",
    feature = "backend-appsink"
))]
#[test]
fn manual_video_selection_preserves_video_type_and_forward_history() {
    let (mut manager, root, original) = selection_manager("video-command");
    let video = root.join("b.mp4");
    std::fs::write(&video, b"\0\0\0\x18ftypisom\0\0\0\0").unwrap();
    manager
        .prepare_media_selection(original.to_str().unwrap(), &None, None)
        .unwrap();
    manager.handle_next(None);
    manager
        .prepare_media_selection(video.to_str().unwrap(), &None, Some(false))
        .unwrap();
    assert_eq!(
        manager.handle_next(None)["DP-1"],
        (video.clone(), crate::queue::ContentType::Video)
    );
    assert_eq!(manager.handle_prev(None)["DP-1"].0, original);
    assert_eq!(manager.handle_next(None)["DP-1"].0, video);
}

#[test]
fn main_monitor_defaults_are_deterministic_and_allow_explicit_all() {
    let (mut manager, _root, _) = selection_manager("main-monitor");
    manager.outputs.insert(
        "HDMI-A-1".into(),
        OutputOrchestrator::without_queue(
            "HDMI-A-1".into(),
            "test".into(),
            test_output_config(Duration::from_secs(60)),
        ),
    );
    let names = ["HDMI-A-1".to_string(), "DP-1".to_string()];
    let outputs = || names.iter().map(|name| (name, 1920, 1080));
    assert_eq!(
        manager.command_output(None, outputs()).unwrap().as_deref(),
        Some("DP-1")
    );
    assert_eq!(
        manager
            .command_output(Some("all".into()), outputs())
            .unwrap(),
        None
    );
    assert!(
        manager
            .command_output(Some("missing".into()), outputs())
            .is_err()
    );
    manager.config.global.main_monitor = Some("HDMI-A-1".into());
    assert_eq!(
        manager.command_output(None, outputs()).unwrap().as_deref(),
        Some("HDMI-A-1")
    );
    manager.config.global.main_monitor = Some("disconnected".into());
    assert_eq!(
        manager.command_output(None, outputs()).unwrap().as_deref(),
        Some("DP-1")
    );
    assert_eq!(
        manager
            .command_output(
                None,
                names
                    .iter()
                    .map(|name| (name, if name == "HDMI-A-1" { 2560 } else { 1920 }, 1080))
            )
            .unwrap()
            .as_deref(),
        Some("HDMI-A-1")
    );
}
