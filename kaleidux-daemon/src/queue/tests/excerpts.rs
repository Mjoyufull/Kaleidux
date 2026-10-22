//! Checks excerpt identity through sequential selection and browser navigation.

use super::*;

#[test]
fn excerpts_are_distinct_entries_and_back_forward_restores_the_exact_window() {
    let video = PathBuf::from("a.mp4");
    let image = PathBuf::from("z.png");
    let mut queue = make_test_queue(
        vec![video.clone(), image.clone()],
        crate::orchestration::SortingStrategy::Ascending,
        100,
        HashMap::from([
            (video.clone(), ContentType::Video),
            (image, ContentType::Image),
        ]),
    );
    queue.configure_excerpts(std::time::Duration::from_secs(3), true);
    assert_eq!(queue.pick_next(), Some(video.clone()));
    queue.register_video_duration(&video, 9_000_000_000);
    let first = queue.current_window().unwrap();
    assert_eq!(first.start_ns, 0);
    assert_eq!(queue.pick_next(), Some(video.clone()));
    let second = queue.current_window().unwrap();
    assert!(second.start_ns > 0);
    assert_eq!(queue.history.len(), 2);
    assert_eq!(queue.pick_prev(), Some(video.clone()));
    assert_eq!(queue.current_window(), Some(first));
    assert_eq!(queue.pick_next(), Some(video.clone()));
    assert_eq!(queue.current_window(), Some(second));
    assert_eq!(queue.pick_next(), Some(video));
    assert!(queue.current_window().unwrap().start_ns > second.start_ns);
    assert_eq!(queue.history.len(), 3);
}

#[test]
fn pinned_or_disabled_sources_do_not_create_excerpt_entries() {
    let video = PathBuf::from("a.mp4");
    let mut queue = make_test_queue(
        vec![video.clone()],
        crate::orchestration::SortingStrategy::Ascending,
        100,
        HashMap::from([(video.clone(), ContentType::Video)]),
    );
    queue.configure_excerpts(std::time::Duration::from_secs(3), true);
    queue.pick_next();
    queue.register_video_duration(&video, 60_000_000_000);
    assert_eq!(queue.current_window(), None);
    queue.pool.push(PathBuf::from("other.mp4"));
    queue.configure_excerpts(std::time::Duration::from_secs(3), false);
    queue.register_video_duration(&video, 60_000_000_000);
    assert_eq!(queue.current_window(), None);
}

#[test]
fn history_navigation_preserves_sequential_excerpt_continuation_with_multiple_videos() {
    use crate::orchestration::SortingStrategy;
    for strategy in [SortingStrategy::Ascending, SortingStrategy::Descending] {
        let paths = vec![PathBuf::from("a.mp4"), PathBuf::from("z.mp4")];
        let mut queue = make_test_queue(
            paths.clone(),
            strategy,
            100,
            paths
                .iter()
                .cloned()
                .map(|path| (path, ContentType::Video))
                .collect(),
        );
        queue.configure_excerpts(std::time::Duration::from_secs(3), true);
        let first_path = queue.pick_next().unwrap();
        queue.register_video_duration(&first_path, 9_000_000_000);
        assert_eq!(queue.pick_next(), Some(first_path.clone()));
        let second = queue.current_window().unwrap();
        queue.pick_prev();
        assert_eq!(queue.pick_next(), Some(first_path.clone()));
        assert_eq!(queue.current_window(), Some(second));
        assert_eq!(queue.pick_next(), Some(first_path.clone()));
        assert!(queue.current_window().unwrap().start_ns > second.start_ns);
        assert_ne!(queue.pick_next(), Some(first_path));
    }
}
