use super::PerformanceMetrics;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Duration;

impl PerformanceMetrics {
    pub(super) fn push_sample(
        samples: &parking_lot::Mutex<VecDeque<f64>>,
        value_ms: f64,
        capacity: usize,
    ) {
        let mut samples = samples.lock();
        samples.push_back(value_ms);
        if samples.len() > capacity {
            samples.pop_front();
        }
    }

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
        upload: Duration,
    ) {
        let total = permit_wait + decode + convert + resize + expand + upload;
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
        Self::push_sample(
            &self.image_upload_samples,
            upload.as_secs_f64() * 1000.0,
            100,
        );
    }

    #[allow(dead_code)]
    pub fn record_image_upload_cpu_time(&self, duration: Duration) {
        Self::push_sample(
            &self.image_upload_samples,
            duration.as_secs_f64() * 1000.0,
            100,
        );
    }

    pub fn record_video_cuda_upload_stages(
        &self,
        map: Duration,
        copy: Duration,
        sync: Duration,
        convert_submit: Duration,
        total: Duration,
    ) {
        Self::push_sample(
            &self.video_cuda_map_samples,
            map.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.video_cuda_copy_samples,
            copy.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.video_cuda_sync_samples,
            sync.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.video_cuda_convert_submit_samples,
            convert_submit.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.video_cuda_total_samples,
            total.as_secs_f64() * 1000.0,
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
    #[allow(dead_code)]
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

    pub(super) fn average_samples(samples: &parking_lot::Mutex<VecDeque<f64>>) -> f64 {
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

    pub fn get_recent_avg_video_cuda_map_ms(&self) -> f64 {
        Self::average_samples(&self.video_cuda_map_samples)
    }

    pub fn get_recent_avg_video_cuda_copy_ms(&self) -> f64 {
        Self::average_samples(&self.video_cuda_copy_samples)
    }

    pub fn get_recent_avg_video_cuda_sync_ms(&self) -> f64 {
        Self::average_samples(&self.video_cuda_sync_samples)
    }

    pub fn get_recent_avg_video_cuda_convert_submit_ms(&self) -> f64 {
        Self::average_samples(&self.video_cuda_convert_submit_samples)
    }

    pub fn get_recent_avg_video_cuda_total_ms(&self) -> f64 {
        Self::average_samples(&self.video_cuda_total_samples)
    }
}
