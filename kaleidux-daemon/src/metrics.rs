use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
        }
    }

    fn push_sample(samples: &parking_lot::Mutex<VecDeque<f64>>, value_ms: f64, capacity: usize) {
        let mut samples = samples.lock();
        samples.push_back(value_ms);
        if samples.len() > capacity {
            samples.pop_front();
        }
    }

    pub fn record_error(&self, error_type: &str) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let mut samples = self.error_samples.lock();
        samples.push_back((std::time::Instant::now(), error_type.to_string()));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn get_error_rate(&self) -> f64 {
        let samples = self.error_samples.lock();
        if samples.len() < 2 {
            return 0.0;
        }
        if let (Some(first), Some(last)) = (samples.front(), samples.back()) {
            let duration_secs = last.0.duration_since(first.0).as_secs_f64();
            if duration_secs > 0.0 {
                samples.len() as f64 / duration_secs // errors per second
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub fn get_error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    pub fn record_gpu_utilization(&self, percent: f64) {
        let mut samples = self.gpu_util_samples.lock();
        samples.push_back((std::time::Instant::now(), percent));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn get_avg_gpu_utilization(&self) -> Option<f64> {
        let samples = self.gpu_util_samples.lock();
        if samples.is_empty() {
            return None;
        }
        let sum: f64 = samples.iter().map(|(_, p)| *p).sum();
        Some(sum / samples.len() as f64)
    }

    pub fn record_memory_usage(&self, mb: f64) {
        let mut samples = self.memory_samples.lock();
        samples.push_back((std::time::Instant::now(), mb));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn get_memory_growth_rate(&self) -> Option<f64> {
        let samples = self.memory_samples.lock();
        if samples.len() < 2 {
            return None;
        }
        if let (Some(first), Some(last)) = (samples.front(), samples.back()) {
            let growth = last.1 - first.1;
            let duration_secs = last.0.duration_since(first.0).as_secs_f64();
            if duration_secs > 0.0 {
                Some(growth / duration_secs) // MB per second
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn get_current_memory(&self) -> Option<f64> {
        self.memory_samples.lock().back().map(|(_, mb)| *mb)
    }

    pub fn get_peak_memory(&self) -> Option<f64> {
        self.memory_samples
            .lock()
            .iter()
            .filter_map(|(_, mb)| if mb.is_finite() { Some(*mb) } else { None })
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    pub fn record_startup_start(&self) {
        let mut metrics = self.startup_metrics.lock();
        metrics.startup_start = Some(std::time::Instant::now());
    }

    pub fn record_gstreamer_init(&self, duration: Duration) {
        let mut metrics = self.startup_metrics.lock();
        metrics.gstreamer_init_duration = Some(duration);
    }

    pub fn record_wgpu_init(&self, duration: Duration) {
        let mut metrics = self.startup_metrics.lock();
        metrics.wgpu_init_duration = Some(duration);
    }

    /// Record CPU time spent in renderer operations
    pub fn record_renderer_cpu_time(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.renderer_cpu_time.fetch_add(us, Ordering::Relaxed);
        self.renderer_ops.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_secs_f64() * 1000.0;
        let mut samples = self.renderer_samples.lock();
        samples.push_back(ms);
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    /// Record CPU time spent in video operations
    pub fn record_video_cpu_time(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.video_cpu_time.fetch_add(us, Ordering::Relaxed);
        self.video_ops.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_secs_f64() * 1000.0;
        let mut samples = self.video_samples.lock();
        samples.push_back(ms);
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    /// Record CPU time spent in file discovery
    pub fn record_file_discovery_cpu_time(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.file_discovery_cpu_time
            .fetch_add(us, Ordering::Relaxed);
        self.file_discovery_ops.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_secs_f64() * 1000.0;
        let mut samples = self.file_discovery_samples.lock();
        samples.push_back((std::time::Instant::now(), ms));
        if samples.len() > 20 {
            samples.pop_front();
        }
    }

    /// Record CPU time spent in shader compilation
    pub fn record_shader_compile_cpu_time(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.shader_compile_cpu_time
            .fetch_add(us, Ordering::Relaxed);
        self.shader_compile_ops.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_secs_f64() * 1000.0;
        let mut samples = self.shader_compile_samples.lock();
        samples.push_back(ms);
        if samples.len() > 50 {
            samples.pop_front();
        }
    }

    pub fn record_image_stage_timings(
        &self,
        permit_wait: Duration,
        decode: Duration,
        convert: Duration,
        resize: Duration,
        expand: Duration,
    ) {
        let total = permit_wait + decode + convert + resize + expand;
        Self::push_sample(&self.image_total_samples, total.as_secs_f64() * 1000.0, 100);
        Self::push_sample(
            &self.image_wait_samples,
            permit_wait.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.image_decode_samples,
            decode.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.image_convert_samples,
            convert.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.image_resize_samples,
            resize.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.image_expand_samples,
            expand.as_secs_f64() * 1000.0,
            100,
        );
    }

    pub fn record_image_upload_cpu_time(&self, duration: Duration) {
        Self::push_sample(
            &self.image_upload_samples,
            duration.as_secs_f64() * 1000.0,
            100,
        );
    }

    /// Get average CPU time per renderer operation (in ms)
    #[allow(dead_code)]
    pub fn get_avg_renderer_cpu_time_ms(&self) -> f64 {
        let ops = self.renderer_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0.0;
        }
        let total_us = self.renderer_cpu_time.load(Ordering::Relaxed);
        (total_us as f64 / ops as f64) / 1000.0
    }

    /// Get average CPU time per video operation (in ms)
    #[allow(dead_code)]
    pub fn get_avg_video_cpu_time_ms(&self) -> f64 {
        let ops = self.video_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0.0;
        }
        let total_us = self.video_cpu_time.load(Ordering::Relaxed);
        (total_us as f64 / ops as f64) / 1000.0
    }

    /// Get average CPU time per file discovery operation (in ms)
    pub fn get_avg_file_discovery_cpu_time_ms(&self) -> f64 {
        let ops = self.file_discovery_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0.0;
        }
        let total_us = self.file_discovery_cpu_time.load(Ordering::Relaxed);
        (total_us as f64 / ops as f64) / 1000.0
    }

    /// Get recent average file discovery CPU time from samples (in ms).
    /// Startup discovery is a one-shot event, so stale samples should age out
    /// instead of showing up forever as active steady-state CPU.
    pub fn get_recent_avg_file_discovery_cpu_time_ms(&self) -> f64 {
        let cutoff = std::time::Instant::now() - Duration::from_secs(15);
        let samples = self.file_discovery_samples.lock();
        let mut sum = 0.0;
        let mut count = 0usize;
        for (ts, ms) in samples.iter() {
            if *ts >= cutoff {
                sum += *ms;
                count += 1;
            }
        }
        if count == 0 { 0.0 } else { sum / count as f64 }
    }

    /// Get average CPU time per shader compile operation (in ms)
    pub fn get_avg_shader_compile_cpu_time_ms(&self) -> f64 {
        let ops = self.shader_compile_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0.0;
        }
        let total_us = self.shader_compile_cpu_time.load(Ordering::Relaxed);
        (total_us as f64 / ops as f64) / 1000.0
    }

    /// Get recent average renderer CPU time from samples (in ms)
    pub fn get_recent_avg_renderer_cpu_time_ms(&self) -> f64 {
        let samples = self.renderer_samples.lock();
        if samples.is_empty() {
            return 0.0;
        }
        samples.iter().sum::<f64>() / samples.len() as f64
    }

    /// Get recent average video CPU time from samples (in ms)
    pub fn get_recent_avg_video_cpu_time_ms(&self) -> f64 {
        let samples = self.video_samples.lock();
        if samples.is_empty() {
            return 0.0;
        }
        samples.iter().sum::<f64>() / samples.len() as f64
    }

    fn average_samples(samples: &parking_lot::Mutex<VecDeque<f64>>) -> f64 {
        let samples = samples.lock();
        if samples.is_empty() {
            0.0
        } else {
            samples.iter().sum::<f64>() / samples.len() as f64
        }
    }

    pub fn get_recent_avg_image_total_ms(&self) -> f64 {
        Self::average_samples(&self.image_total_samples)
    }

    pub fn get_recent_avg_image_wait_ms(&self) -> f64 {
        Self::average_samples(&self.image_wait_samples)
    }

    pub fn get_recent_avg_image_decode_ms(&self) -> f64 {
        Self::average_samples(&self.image_decode_samples)
    }

    pub fn get_recent_avg_image_convert_ms(&self) -> f64 {
        Self::average_samples(&self.image_convert_samples)
    }

    pub fn get_recent_avg_image_resize_ms(&self) -> f64 {
        Self::average_samples(&self.image_resize_samples)
    }

    pub fn get_recent_avg_image_expand_ms(&self) -> f64 {
        Self::average_samples(&self.image_expand_samples)
    }

    pub fn get_recent_avg_image_upload_ms(&self) -> f64 {
        Self::average_samples(&self.image_upload_samples)
    }

    pub fn record_first_frame(&self) {
        let mut metrics = self.startup_metrics.lock();
        if let Some(start) = metrics.startup_start {
            if metrics.time_to_first_frame.is_none() {
                metrics.time_to_first_frame = Some(start.elapsed());
            }
        }
    }

    pub fn record_full_init(&self) {
        let mut metrics = self.startup_metrics.lock();
        if let Some(start) = metrics.startup_start {
            metrics.time_to_full_init = Some(start.elapsed());
        }
    }

    pub fn log_startup_summary(&self) {
        let metrics = self.startup_metrics.lock();
        let mut parts = Vec::new();

        if let Some(d) = metrics.gstreamer_init_duration {
            parts.push(format!("GStreamer: {:.2}ms", d.as_secs_f64() * 1000.0));
        }
        if let Some(d) = metrics.wgpu_init_duration {
            parts.push(format!("WGPU: {:.2}ms", d.as_secs_f64() * 1000.0));
        }
        if let Some(d) = metrics.file_discovery_duration {
            parts.push(format!("File discovery: {:.2}ms", d.as_secs_f64() * 1000.0));
        }
        if let Some(d) = metrics.time_to_first_frame {
            parts.push(format!("First frame: {:.2}ms", d.as_secs_f64() * 1000.0));
        }
        if let Some(d) = metrics.time_to_full_init {
            parts.push(format!("Full init: {:.2}ms", d.as_secs_f64() * 1000.0));
        }

        if !parts.is_empty() {
            tracing::info!("[STARTUP] {}", parts.join(" | "));
        }
    }

    pub fn get_memory_info(&self) -> String {
        let current = self
            .get_current_memory()
            .map(|m| format!("{:.1}MB", m))
            .unwrap_or_else(|| "N/A".to_string());
        let peak = self
            .get_peak_memory()
            .map(|m| format!("{:.1}MB", m))
            .unwrap_or_else(|| "N/A".to_string());
        let growth = self
            .get_memory_growth_rate()
            .map(|r| format!("{:.2}MB/s", r))
            .unwrap_or_else(|| "N/A".to_string());
        format!("current={} peak={} growth={}", current, peak, growth)
    }

    pub fn get_uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    pub fn get_uptime_seconds(&self) -> u64 {
        self.get_uptime().as_secs()
    }

    pub fn record_frame_time(&self, duration: std::time::Duration) {
        let ms = duration.as_secs_f64() * 1000.0;
        let mut times = self.frame_times.lock();
        times.push_back(ms);
        if times.len() > 100 {
            times.pop_front();
        }

        // Update average (in microseconds)
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        self.avg_frame_time
            .store((avg * 1000.0) as u64, Ordering::Relaxed);
    }

    pub fn record_texture_pool_hit(&self) {
        self.texture_pool_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_texture_pool_miss(&self) {
        self.texture_pool_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_transition(&self, duration: std::time::Duration) {
        self.transition_count.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_secs_f64() * 1000.0;
        let mut times = self.transition_times.lock();
        times.push_back(ms);
        if times.len() > 50 {
            times.pop_front();
        }
    }

    pub fn record_video_first_frame(&self, duration: std::time::Duration) {
        let ms = duration.as_secs_f64() * 1000.0;
        let mut times = self.video_first_frame_times.lock();
        times.push_back(ms);
        if times.len() > 20 {
            times.pop_front();
        }
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_texture_pool_hit_rate(&self) -> f64 {
        let hits = self.texture_pool_hits.load(Ordering::Relaxed) as f64;
        let misses = self.texture_pool_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 { 0.0 } else { hits / total }
    }

    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 { 0.0 } else { hits / total }
    }

    pub fn get_avg_frame_time_ms(&self) -> f64 {
        self.avg_frame_time.load(Ordering::Relaxed) as f64 / 1000.0
    }

    fn get_percentile(&self, percentile: f64) -> f64 {
        let times = self.frame_times.lock();
        if times.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<f64> = times.iter().filter(|t| t.is_finite()).copied().collect();
        if sorted.is_empty() {
            return 0.0;
        }
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = (sorted.len() as f64 * percentile) as usize;
        sorted
            .get(idx.min(sorted.len() - 1))
            .copied()
            .unwrap_or(0.0)
    }

    pub fn get_p50_frame_time_ms(&self) -> f64 {
        self.get_percentile(0.50)
    }

    pub fn get_p95_frame_time_ms(&self) -> f64 {
        self.get_percentile(0.95)
    }

    pub fn get_p99_frame_time_ms(&self) -> f64 {
        self.get_percentile(0.99)
    }

    pub fn record_texture_count(&self, count: usize) {
        let mut samples = self.texture_count_samples.lock();
        samples.push_back((std::time::Instant::now(), count));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn record_pipeline_count(&self, count: usize) {
        let mut samples = self.pipeline_count_samples.lock();
        samples.push_back((std::time::Instant::now(), count));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn record_frame_channel_size(&self, size: usize) {
        let mut samples = self.frame_channel_size_samples.lock();
        samples.push_back((std::time::Instant::now(), size));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn record_image_channel_size(&self, size: usize) {
        let mut samples = self.image_channel_size_samples.lock();
        samples.push_back((std::time::Instant::now(), size));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn record_texture_pool_size(&self, size: usize) {
        let mut samples = self.texture_pool_size_samples.lock();
        samples.push_back((std::time::Instant::now(), size));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn check_resource_leaks(&self) -> Option<String> {
        let texture_samples = self.texture_count_samples.lock();
        let pipeline_samples = self.pipeline_count_samples.lock();
        let frame_channel_samples = self.frame_channel_size_samples.lock();
        let image_channel_samples = self.image_channel_size_samples.lock();
        let texture_pool_samples = self.texture_pool_size_samples.lock();

        let mut warnings = Vec::new();

        // Check texture count growth
        if texture_samples.len() >= 10 {
            if let (Some(first), Some(last)) = (texture_samples.front(), texture_samples.back()) {
                let growth = last.1.saturating_sub(first.1);
                let duration = last.0.duration_since(first.0).as_secs();
                if growth > 50 && duration > 60 {
                    let growth_rate = growth as f64 / duration as f64;
                    warnings.push(format!(
                        "Texture count grew by {} over {}s ({:.2}/s)",
                        growth, duration, growth_rate
                    ));
                }
            }
        }

        // Check pipeline count growth
        if pipeline_samples.len() >= 10 {
            if let (Some(first), Some(last)) = (pipeline_samples.front(), pipeline_samples.back()) {
                let growth = last.1.saturating_sub(first.1);
                let duration = last.0.duration_since(first.0).as_secs();
                if growth > 20 && duration > 60 {
                    let growth_rate = growth as f64 / duration as f64;
                    warnings.push(format!(
                        "Pipeline count grew by {} over {}s ({:.2}/s)",
                        growth, duration, growth_rate
                    ));
                }
            }
        }

        // Check frame channel accumulation
        if frame_channel_samples.len() >= 10 {
            if let Some((_, last_size)) = frame_channel_samples.back() {
                if *last_size > 20 {
                    warnings.push(format!(
                        "Frame channel has {} frames queued (potential backpressure issue)",
                        last_size
                    ));
                }
            }
        }

        // Check image channel accumulation
        if image_channel_samples.len() >= 10 {
            if let Some((_, last_size)) = image_channel_samples.back() {
                if *last_size > 4 {
                    warnings.push(format!(
                        "Image channel has {} images queued (potential backpressure issue)",
                        last_size
                    ));
                }
            }
        }

        // Check texture pool size
        if texture_pool_samples.len() >= 10 {
            if let Some((_, last_size)) = texture_pool_samples.back() {
                if *last_size > 40 {
                    warnings.push(format!(
                        "Texture pool has {} textures (approaching limit of 50)",
                        last_size
                    ));
                }
            }
        }

        if warnings.is_empty() {
            None
        } else {
            Some(warnings.join("; "))
        }
    }

    pub fn log_summary(&self) {
        let leak_warning = self.check_resource_leaks();
        let leak_msg = leak_warning
            .map(|w| format!(" | LEAK WARNING: {}", w))
            .unwrap_or_default();

        let uptime_secs = self.get_uptime_seconds();
        let uptime_str = if uptime_secs < 60 {
            format!("{}s", uptime_secs)
        } else if uptime_secs < 3600 {
            format!("{}m{}s", uptime_secs / 60, uptime_secs % 60)
        } else {
            format!(
                "{}h{}m{}s",
                uptime_secs / 3600,
                (uptime_secs % 3600) / 60,
                uptime_secs % 60
            )
        };

        let memory_info = self.get_memory_info();
        let gpu_info = self
            .get_avg_gpu_utilization()
            .map(|g| format!("{:.1}%", g))
            .unwrap_or_else(|| "N/A".to_string());
        let error_info = format!(
            "count={} rate={:.3}/s",
            self.get_error_count(),
            self.get_error_rate()
        );

        // Component CPU stats
        let renderer_avg = self.get_recent_avg_renderer_cpu_time_ms();
        let video_avg = self.get_recent_avg_video_cpu_time_ms();
        let file_disc_avg = self.get_recent_avg_file_discovery_cpu_time_ms();
        let shader_avg = self.get_avg_shader_compile_cpu_time_ms();
        let image_avg = self.get_recent_avg_image_total_ms();
        let component_cpu = format!(
            "renderer={:.2}ms video={:.2}ms image={:.2}ms file_disc={:.2}ms shader={:.2}ms",
            renderer_avg, video_avg, image_avg, file_disc_avg, shader_avg
        );

        let hits = self.texture_pool_hits.load(Ordering::Relaxed);
        let misses = self.texture_pool_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let texture_hit_rate = if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        };

        let c_hits = self.cache_hits.load(Ordering::Relaxed);
        let c_misses = self.cache_misses.load(Ordering::Relaxed);
        let c_total = c_hits + c_misses;
        let cache_hit_rate = if c_total == 0 {
            0.0
        } else {
            c_hits as f64 / c_total as f64
        };

        tracing::info!(
            "[METRICS] Uptime: {} | Memory: {} | GPU: {} | Errors: {} | Frame time: avg={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms | Texture pool: hit_rate={:.1}% ({}/{}) | Cache: hit_rate={:.1}% ({}/{}) | Transitions: {} | Component CPU: {}{}",
            uptime_str,
            memory_info,
            gpu_info,
            error_info,
            self.get_avg_frame_time_ms(),
            self.get_p50_frame_time_ms(),
            self.get_p95_frame_time_ms(),
            self.get_p99_frame_time_ms(),
            texture_hit_rate * 100.0,
            hits,
            total,
            cache_hit_rate * 100.0,
            c_hits,
            c_total,
            self.transition_count.load(Ordering::Relaxed),
            component_cpu,
            leak_msg
        );

        if image_avg > 0.0 || self.get_recent_avg_image_upload_ms() > 0.0 {
            tracing::info!(
                "[METRICS] Image stages: total={:.2}ms wait={:.2}ms decode={:.2}ms convert={:.2}ms resize={:.2}ms expand={:.2}ms upload={:.2}ms",
                image_avg,
                self.get_recent_avg_image_wait_ms(),
                self.get_recent_avg_image_decode_ms(),
                self.get_recent_avg_image_convert_ms(),
                self.get_recent_avg_image_resize_ms(),
                self.get_recent_avg_image_expand_ms(),
                self.get_recent_avg_image_upload_ms(),
            );
        }
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
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
        );
        metrics.record_image_upload_cpu_time(Duration::from_millis(60));

        assert_eq!(metrics.get_recent_avg_image_wait_ms(), 10.0);
        assert_eq!(metrics.get_recent_avg_image_decode_ms(), 20.0);
        assert_eq!(metrics.get_recent_avg_image_convert_ms(), 30.0);
        assert_eq!(metrics.get_recent_avg_image_resize_ms(), 40.0);
        assert_eq!(metrics.get_recent_avg_image_expand_ms(), 50.0);
        assert_eq!(metrics.get_recent_avg_image_upload_ms(), 60.0);
        assert_eq!(metrics.get_recent_avg_image_total_ms(), 150.0);
    }
}
