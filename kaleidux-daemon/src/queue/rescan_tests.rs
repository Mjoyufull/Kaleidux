//! Tests full refresh of private manual pools through real asynchronous watcher work.

use super::*;

async fn wait_for_rescan(queue: &SmartQueue) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while queue.root_index.rescan_running() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background refresh should finish");
}

#[tokio::test]
async fn manual_queue_rescan_replaces_deleted_selection_and_accepts_empty_results() {
    let root = unique_test_dir("manual-rescan");
    let selected = root.join("selected.png");
    let added = root.join("added.png");
    image::RgbaImage::new(2, 2).save(&selected).unwrap();
    let shared = media_index::shared_root_index(&root);
    let mut queue = SmartQueue::new_manual_from_pool(
        &root,
        vec![selected.clone()],
        0,
        crate::orchestration::SortingStrategy::Ascending,
        test_cache(),
    )
    .unwrap();
    assert!(
        shared.snapshot().entries.is_empty(),
        "partial pool is private"
    );
    fs::remove_file(selected).unwrap();
    image::RgbaImage::new(2, 2).save(&added).unwrap();
    queue.apply_pool_events(vec![crate::cache::PoolEvent::Rescan(root.clone())]);
    wait_for_rescan(&queue).await;
    assert_eq!(queue.pick_next(), Some(added.clone()));
    assert_eq!(queue.pool, vec![added.clone()]);

    fs::remove_file(added).unwrap();
    queue.apply_pool_events(vec![crate::cache::PoolEvent::Rescan(root.clone())]);
    wait_for_rescan(&queue).await;
    assert_eq!(queue.pick_next(), None);
    assert!(queue.pool.is_empty());
    fs::remove_dir_all(root).unwrap();
}
