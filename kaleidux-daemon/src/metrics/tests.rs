use super::*;

#[test]
fn recent_file_discovery_avg_ignores_stale_samples() {
    let metrics = PerformanceMetrics::new();
    {
        let mut samples = metrics.file_discovery_samples.lock();
        samples.push_back((std::time::Instant::now() - Duration::from_secs(30), 111.0));
    }
    assert_eq!(metrics.get_recent_avg_file_discovery_cpu_time_ms(), 0.0);
}

#[test]
fn image_stage_timings_are_averaged() {
    let metrics = PerformanceMetrics::new();
    metrics.record_image_stage_timings(
        Duration::from_millis(10),
        Duration::from_millis(20),
        Duration::from_millis(30),
        Duration::from_millis(40),
        Duration::from_millis(50),
        Duration::from_millis(60),
    );

    assert_eq!(metrics.get_recent_avg_image_wait_ms(), 10.0);
    assert_eq!(metrics.get_recent_avg_image_decode_ms(), 20.0);
    assert_eq!(metrics.get_recent_avg_image_convert_ms(), 30.0);
    assert_eq!(metrics.get_recent_avg_image_resize_ms(), 40.0);
    assert_eq!(metrics.get_recent_avg_image_expand_ms(), 50.0);
    assert_eq!(metrics.get_recent_avg_image_upload_ms(), 60.0);
    assert_eq!(metrics.get_recent_avg_image_total_ms(), 210.0);
}
