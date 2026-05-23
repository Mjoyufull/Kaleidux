use super::{MonitorStageTimings, PerformanceMetrics};
use std::time::Duration;

impl PerformanceMetrics {
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

    pub fn record_monitor_stage_timings(&self, timings: MonitorStageTimings) {
        Self::push_sample(
            &self.monitor_refresh_cpu_samples,
            timings.refresh_cpu.as_secs_f64() * 1000.0,
            20,
        );
        Self::push_sample(
            &self.monitor_refresh_memory_samples,
            timings.refresh_memory.as_secs_f64() * 1000.0,
            20,
        );
        Self::push_sample(
            &self.monitor_process_samples,
            timings.process_refresh.as_secs_f64() * 1000.0,
            20,
        );
        Self::push_sample(
            &self.monitor_gpu_samples,
            timings.gpu_query.as_secs_f64() * 1000.0,
            20,
        );
        Self::push_sample(
            &self.monitor_logging_samples,
            timings.logging.as_secs_f64() * 1000.0,
            20,
        );
    }

    pub fn get_monitor_stage_summary(&self) -> String {
        format!(
            "refresh_cpu={:.2}ms refresh_memory={:.2}ms process={:.2}ms gpu={:.2}ms logging={:.2}ms",
            Self::average_samples(&self.monitor_refresh_cpu_samples),
            Self::average_samples(&self.monitor_refresh_memory_samples),
            Self::average_samples(&self.monitor_process_samples),
            Self::average_samples(&self.monitor_gpu_samples),
            Self::average_samples(&self.monitor_logging_samples)
        )
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
}
