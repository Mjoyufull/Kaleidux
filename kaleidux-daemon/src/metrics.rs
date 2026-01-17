use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::VecDeque;

/// Performance metrics for monitoring
pub struct PerformanceMetrics {
    // Frame timing
    pub frame_times: Arc<parking_lot::Mutex<VecDeque<f64>>>, // Last 100 frame times in ms
    pub avg_frame_time: Arc<AtomicU64>, // Average in microseconds
    
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
        }
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
        self.avg_frame_time.store((avg * 1000.0) as u64, Ordering::Relaxed);
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
        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }
    
    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }
    
    pub fn get_avg_frame_time_ms(&self) -> f64 {
        self.avg_frame_time.load(Ordering::Relaxed) as f64 / 1000.0
    }
    
    pub fn get_p95_frame_time_ms(&self) -> f64 {
        let times = self.frame_times.lock();
        if times.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<f64> = times.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (sorted.len() as f64 * 0.95) as usize;
        sorted.get(idx.min(sorted.len() - 1)).copied().unwrap_or(0.0)
    }
    
    pub fn log_summary(&self) {
        tracing::info!(
            "[METRICS] Frame time: avg={:.2}ms p95={:.2}ms | Texture pool: hit_rate={:.1}% ({}/{}) | Cache: hit_rate={:.1}% ({}/{}) | Transitions: {}",
            self.get_avg_frame_time_ms(),
            self.get_p95_frame_time_ms(),
            self.get_texture_pool_hit_rate() * 100.0,
            self.texture_pool_hits.load(Ordering::Relaxed),
            self.texture_pool_hits.load(Ordering::Relaxed) + self.texture_pool_misses.load(Ordering::Relaxed),
            self.get_cache_hit_rate() * 100.0,
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed) + self.cache_misses.load(Ordering::Relaxed),
            self.transition_count.load(Ordering::Relaxed)
        );
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
