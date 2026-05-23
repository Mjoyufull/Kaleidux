use super::PerformanceMetrics;
use std::sync::atomic::Ordering;

impl PerformanceMetrics {
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

    #[allow(dead_code)]
    pub fn get_texture_pool_hit_rate(&self) -> f64 {
        let hits = self.texture_pool_hits.load(Ordering::Relaxed) as f64;
        let misses = self.texture_pool_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 { 0.0 } else { hits / total }
    }

    #[allow(dead_code)]
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
}
