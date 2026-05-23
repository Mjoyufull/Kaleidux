use crate::observability::cpu_threads::ThreadCpuSnapshot;
use crate::observability::present::{FRAME_CALLBACK_KIND_COUNT, RENDERER_PRESENT_KIND_COUNT};
use crate::observability::video_backend::VIDEO_BACKEND_METRIC_KIND_COUNT;
use crate::observability::wake::{DEADLINE_REASON_COUNT, WAKE_REASON_COUNT};

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct MonitorStageTimings {
    pub refresh_cpu: Duration,
    pub refresh_memory: Duration,
    pub process_refresh: Duration,
    pub gpu_query: Duration,
    pub logging: Duration,
}

/// Performance metrics for monitoring
pub struct PerformanceMetrics {
    // Frame timing
    pub frame_times: Arc<parking_lot::Mutex<VecDeque<f64>>>, // Last 100 frame times in ms
    pub avg_frame_time: Arc<AtomicU64>,                      // Average in microseconds

    // Texture pool stats
    pub texture_pool_hits: Arc<AtomicU64>,
    pub texture_pool_misses: Arc<AtomicU64>,

    // Transition stats
    pub transition_count: Arc<AtomicU64>,
    pub transition_times: Arc<parking_lot::Mutex<VecDeque<f64>>>, // Last 50 transition times in ms

    // Video stats
    pub video_first_frame_times: Arc<parking_lot::Mutex<VecDeque<f64>>>, // Last 20 first frame times in ms

    // Cache stats
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,

    // Resource leak detection
    pub texture_count_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, usize)>>>, // (timestamp, count)
    pub pipeline_count_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, usize)>>>, // (timestamp, count)

    // Channel buffer tracking for memory leak detection
    pub frame_channel_size_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, usize)>>>, // (timestamp, frames in channel)
    pub image_channel_size_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, usize)>>>, // (timestamp, images in channel)
    pub texture_pool_size_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, usize)>>>, // (timestamp, textures in pool)

    // Uptime tracking
    start_time: std::time::Instant,

    // Startup metrics
    startup_metrics: Arc<parking_lot::Mutex<StartupMetrics>>,

    // Memory usage over time
    memory_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, f64)>>>, // (timestamp, MB)

    // GPU utilization over time
    gpu_util_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, f64)>>>, // (timestamp, %)

    // Error tracking
    error_count: Arc<AtomicU64>,
    error_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, String)>>>, // (timestamp, error_type)

    // Wayland pacing diagnostics
    wayland_idle_loops: Arc<AtomicU64>,
    wayland_hot_loops: Arc<AtomicU64>,
    wayland_callback_wakes: Arc<AtomicU64>,
    wayland_expired_deadline_wakes: Arc<AtomicU64>,
    wake_reasons: Arc<[AtomicU64; WAKE_REASON_COUNT]>,
    deadline_reasons: Arc<[AtomicU64; DEADLINE_REASON_COUNT]>,
    wake_requested_sleep_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    wake_actual_sleep_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,

    // Present source diagnostics
    static_image_presents: Arc<AtomicU64>,
    video_frame_presents: Arc<AtomicU64>,
    startup_release_presents: Arc<AtomicU64>,
    present_kinds: Arc<[AtomicU64; RENDERER_PRESENT_KIND_COUNT]>,
    frame_callback_kinds: Arc<[AtomicU64; FRAME_CALLBACK_KIND_COUNT]>,
    frame_callback_full_damage: Arc<AtomicU64>,
    frame_callback_minimal_damage: Arc<AtomicU64>,

    // Image cache diagnostics
    image_prepared_memory_hits: Arc<AtomicU64>,
    image_prepared_compatible_hits: Arc<AtomicU64>,
    image_prepared_disk_hits: Arc<AtomicU64>,
    image_prepared_misses: Arc<AtomicU64>,
    image_source_memory_hits: Arc<AtomicU64>,
    image_source_decode_misses: Arc<AtomicU64>,
    image_shared_waits: Arc<AtomicU64>,
    image_slow_prepares: Arc<AtomicU64>,

    // Video pacing diagnostics
    video_frames_received: Arc<AtomicU64>,
    video_frames_uploaded: Arc<AtomicU64>,
    video_frames_presented: Arc<AtomicU64>,
    video_frames_stale_skipped: Arc<AtomicU64>,
    shared_broker_hits: Arc<AtomicU64>,
    shared_broker_misses: Arc<AtomicU64>,
    video_backend_metrics: Arc<[AtomicU64; VIDEO_BACKEND_METRIC_KIND_COUNT]>,

    // Component CPU tracking (time spent in each component in milliseconds)
    renderer_cpu_time: Arc<AtomicU64>, // Total CPU time in microseconds
    video_cpu_time: Arc<AtomicU64>,    // Total CPU time in microseconds
    file_discovery_cpu_time: Arc<AtomicU64>, // Total CPU time in microseconds
    shader_compile_cpu_time: Arc<AtomicU64>, // Total CPU time in microseconds

    // Component operation counts
    renderer_ops: Arc<AtomicU64>,
    video_ops: Arc<AtomicU64>,
    file_discovery_ops: Arc<AtomicU64>,
    shader_compile_ops: Arc<AtomicU64>,

    // Component CPU time samples (for averaging)
    renderer_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>, // Last 100 renderer times in ms
    video_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,    // Last 100 video times in ms
    file_discovery_samples: Arc<parking_lot::Mutex<VecDeque<(std::time::Instant, f64)>>>, // Last 20 file discovery times in ms
    shader_compile_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>, // Last 50 shader compile times in ms
    image_total_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    image_wait_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    image_decode_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    image_convert_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    image_resize_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    image_expand_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    image_upload_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    video_cuda_map_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    video_cuda_copy_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    video_cuda_sync_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    video_cuda_convert_submit_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    video_cuda_total_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    monitor_refresh_cpu_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    monitor_refresh_memory_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    monitor_process_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    monitor_gpu_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    monitor_logging_samples: Arc<parking_lot::Mutex<VecDeque<f64>>>,
    previous_thread_cpu_snapshot: Arc<parking_lot::Mutex<Option<ThreadCpuSnapshot>>>,
}

#[derive(Debug, Clone)]
pub struct StartupMetrics {
    pub startup_start: Option<std::time::Instant>,
    pub gstreamer_init_duration: Option<Duration>,
    pub wgpu_init_duration: Option<Duration>,
    pub file_discovery_duration: Option<Duration>,
    pub time_to_first_frame: Option<Duration>,
    pub time_to_full_init: Option<Duration>,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            frame_times: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            avg_frame_time: Arc::new(AtomicU64::new(0)),
            texture_pool_hits: Arc::new(AtomicU64::new(0)),
            texture_pool_misses: Arc::new(AtomicU64::new(0)),
            transition_count: Arc::new(AtomicU64::new(0)),
            transition_times: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(50))),
            video_first_frame_times: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(20))),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            texture_count_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            pipeline_count_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            frame_channel_size_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            image_channel_size_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            texture_pool_size_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            start_time: std::time::Instant::now(),
            startup_metrics: Arc::new(parking_lot::Mutex::new(StartupMetrics {
                startup_start: Some(std::time::Instant::now()),
                gstreamer_init_duration: None,
                wgpu_init_duration: None,
                file_discovery_duration: None,
                time_to_first_frame: None,
                time_to_full_init: None,
            })),
            memory_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            gpu_util_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            error_count: Arc::new(AtomicU64::new(0)),
            error_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            wayland_idle_loops: Arc::new(AtomicU64::new(0)),
            wayland_hot_loops: Arc::new(AtomicU64::new(0)),
            wayland_callback_wakes: Arc::new(AtomicU64::new(0)),
            wayland_expired_deadline_wakes: Arc::new(AtomicU64::new(0)),
            wake_reasons: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            deadline_reasons: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            wake_requested_sleep_samples: Arc::new(parking_lot::Mutex::new(
                VecDeque::with_capacity(100),
            )),
            wake_actual_sleep_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            static_image_presents: Arc::new(AtomicU64::new(0)),
            video_frame_presents: Arc::new(AtomicU64::new(0)),
            startup_release_presents: Arc::new(AtomicU64::new(0)),
            present_kinds: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            frame_callback_kinds: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            frame_callback_full_damage: Arc::new(AtomicU64::new(0)),
            frame_callback_minimal_damage: Arc::new(AtomicU64::new(0)),
            image_prepared_memory_hits: Arc::new(AtomicU64::new(0)),
            image_prepared_compatible_hits: Arc::new(AtomicU64::new(0)),
            image_prepared_disk_hits: Arc::new(AtomicU64::new(0)),
            image_prepared_misses: Arc::new(AtomicU64::new(0)),
            image_source_memory_hits: Arc::new(AtomicU64::new(0)),
            image_source_decode_misses: Arc::new(AtomicU64::new(0)),
            image_shared_waits: Arc::new(AtomicU64::new(0)),
            image_slow_prepares: Arc::new(AtomicU64::new(0)),
            video_frames_received: Arc::new(AtomicU64::new(0)),
            video_frames_uploaded: Arc::new(AtomicU64::new(0)),
            video_frames_presented: Arc::new(AtomicU64::new(0)),
            video_frames_stale_skipped: Arc::new(AtomicU64::new(0)),
            shared_broker_hits: Arc::new(AtomicU64::new(0)),
            shared_broker_misses: Arc::new(AtomicU64::new(0)),
            video_backend_metrics: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            renderer_cpu_time: Arc::new(AtomicU64::new(0)),
            video_cpu_time: Arc::new(AtomicU64::new(0)),
            file_discovery_cpu_time: Arc::new(AtomicU64::new(0)),
            shader_compile_cpu_time: Arc::new(AtomicU64::new(0)),
            renderer_ops: Arc::new(AtomicU64::new(0)),
            video_ops: Arc::new(AtomicU64::new(0)),
            file_discovery_ops: Arc::new(AtomicU64::new(0)),
            shader_compile_ops: Arc::new(AtomicU64::new(0)),
            renderer_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            video_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            file_discovery_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(20))),
            shader_compile_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(50))),
            image_total_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            image_wait_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            image_decode_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            image_convert_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            image_resize_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            image_expand_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            image_upload_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            video_cuda_map_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(100))),
            video_cuda_copy_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            video_cuda_sync_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            video_cuda_convert_submit_samples: Arc::new(parking_lot::Mutex::new(
                VecDeque::with_capacity(100),
            )),
            video_cuda_total_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(
                100,
            ))),
            monitor_refresh_cpu_samples: Arc::new(parking_lot::Mutex::new(
                VecDeque::with_capacity(20),
            )),
            monitor_refresh_memory_samples: Arc::new(parking_lot::Mutex::new(
                VecDeque::with_capacity(20),
            )),
            monitor_process_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(20))),
            monitor_gpu_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(20))),
            monitor_logging_samples: Arc::new(parking_lot::Mutex::new(VecDeque::with_capacity(20))),
            previous_thread_cpu_snapshot: Arc::new(parking_lot::Mutex::new(None)),
        }
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

mod baseline;
mod diagnostics;
mod summary;
mod system;
mod timing;

#[cfg(test)]
#[path = "metrics/tests.rs"]
mod tests;
