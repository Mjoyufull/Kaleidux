use super::PerformanceMetrics;
use crate::observability::cpu_threads::ThreadCpuSnapshot;
use crate::observability::present::{FRAME_CALLBACK_KINDS, RENDERER_PRESENT_KINDS};
use crate::observability::video_backend::VIDEO_BACKEND_METRIC_KINDS;
use crate::observability::wake::{DEADLINE_REASONS, WAKE_REASONS};
use std::sync::atomic::Ordering;

impl PerformanceMetrics {
    pub fn perf_snapshot(&self) -> String {
        [
            format!("uptime_s={}", self.get_uptime_seconds()),
            format!("memory={}", self.get_memory_info()),
            format!("component_cpu={}", self.component_cpu_summary()),
            format!("video_upload={}", self.video_upload_summary()),
            format!("video={}", self.video_pacing_summary()),
            format!("video_backend={}", self.video_backend_summary()),
            format!("present={}", self.present_source_summary()),
            format!("image_cache={}", self.image_cache_summary()),
            format!(
                "wake={} {}",
                self.wake_reason_summary(),
                self.wake_sleep_summary()
            ),
            format!("monitor_self_cost={}", self.get_monitor_stage_summary()),
            format!("thread_cpu={}", self.thread_cpu_summary()),
        ]
        .join("\n")
    }

    fn component_cpu_summary(&self) -> String {
        format!(
            "renderer={:.2}ms video={:.2}ms image={:.2}ms file_disc={:.2}ms shader={:.2}ms",
            self.get_recent_avg_renderer_cpu_time_ms(),
            self.get_recent_avg_video_cpu_time_ms(),
            self.get_recent_avg_image_total_ms(),
            self.get_recent_avg_file_discovery_cpu_time_ms(),
            self.get_avg_shader_compile_cpu_time_ms()
        )
    }

    fn video_upload_summary(&self) -> String {
        format!(
            "cuda_total={:.2}ms map={:.2}ms copy={:.2}ms sync={:.2}ms convert_submit={:.2}ms",
            self.get_recent_avg_video_cuda_total_ms(),
            self.get_recent_avg_video_cuda_map_ms(),
            self.get_recent_avg_video_cuda_copy_ms(),
            self.get_recent_avg_video_cuda_sync_ms(),
            self.get_recent_avg_video_cuda_convert_submit_ms(),
        )
    }

    fn video_pacing_summary(&self) -> String {
        format!(
            "recv={} upload={} present={} stale={}",
            self.video_frames_received.load(Ordering::Relaxed),
            self.video_frames_uploaded.load(Ordering::Relaxed),
            self.video_frames_presented.load(Ordering::Relaxed),
            self.video_frames_stale_skipped.load(Ordering::Relaxed)
        )
    }

    fn video_backend_summary(&self) -> String {
        VIDEO_BACKEND_METRIC_KINDS
            .iter()
            .map(|kind| {
                format!(
                    "{}={}",
                    kind.label(),
                    self.video_backend_metrics[kind.as_index()].load(Ordering::Relaxed)
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn present_source_summary(&self) -> String {
        let counts = RENDERER_PRESENT_KINDS
            .iter()
            .map(|kind| {
                format!(
                    "{}={}",
                    kind.label(),
                    self.present_kinds[kind.as_index()].load(Ordering::Relaxed)
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let callback_counts = FRAME_CALLBACK_KINDS
            .iter()
            .map(|kind| {
                format!(
                    "{}={}",
                    kind.label(),
                    self.frame_callback_kinds[kind.as_index()].load(Ordering::Relaxed)
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "{} {} callback_full_damage={} callback_minimal_damage={} legacy_static={} legacy_video={} legacy_startup_release={}",
            counts,
            callback_counts,
            self.frame_callback_full_damage.load(Ordering::Relaxed),
            self.frame_callback_minimal_damage.load(Ordering::Relaxed),
            self.static_image_presents.load(Ordering::Relaxed),
            self.video_frame_presents.load(Ordering::Relaxed),
            self.startup_release_presents.load(Ordering::Relaxed)
        )
    }

    fn image_cache_summary(&self) -> String {
        format!(
            "prepared_memory={} compatible={} disk={} miss={} source_memory={} source_decode={} shared_wait={} slow_prepare={}",
            self.image_prepared_memory_hits.load(Ordering::Relaxed),
            self.image_prepared_compatible_hits.load(Ordering::Relaxed),
            self.image_prepared_disk_hits.load(Ordering::Relaxed),
            self.image_prepared_misses.load(Ordering::Relaxed),
            self.image_source_memory_hits.load(Ordering::Relaxed),
            self.image_source_decode_misses.load(Ordering::Relaxed),
            self.image_shared_waits.load(Ordering::Relaxed),
            self.image_slow_prepares.load(Ordering::Relaxed)
        )
    }

    pub fn log_summary(&self) {
        let thread_cpu_info = self.thread_cpu_summary();
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
        let image_avg = self.get_recent_avg_image_total_ms();
        let component_cpu = self.component_cpu_summary();
        let video_pacing = self.video_pacing_summary();
        let video_backend_info = self.video_backend_summary();
        let wayland_info = format!(
            " wayland=idle:{} hot:{} callbacks:{} expired:{}",
            self.wayland_idle_loops.load(Ordering::Relaxed),
            self.wayland_hot_loops.load(Ordering::Relaxed),
            self.wayland_callback_wakes.load(Ordering::Relaxed),
            self.wayland_expired_deadline_wakes.load(Ordering::Relaxed)
        );
        let wake_info = self.wake_reason_summary();
        let present_info = self.present_source_summary();
        let image_cache_info = self.image_cache_summary();
        let shared_broker_info = format!(
            " shared_broker=hit:{} miss:{}",
            self.shared_broker_hits.load(Ordering::Relaxed),
            self.shared_broker_misses.load(Ordering::Relaxed)
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
            "[METRICS] Uptime: {} | Memory: {} | GPU: {} | Errors: {} | Frame time: avg={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms | Texture pool: hit_rate={:.1}% ({}/{}) | Cache: hit_rate={:.1}% ({}/{}) | Transitions: {} | Component CPU: {}{}{}",
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
            leak_msg,
            wayland_info
        );
        tracing::info!(
            "[METRICS] Video pacing: {}{}{}",
            video_pacing,
            "",
            shared_broker_info
        );
        tracing::info!("[METRICS] Video backend: {}", video_backend_info);
        tracing::info!("[VIDEO-BACKEND] {}", video_backend_info);
        tracing::info!("[METRICS] CPU threads: {}", thread_cpu_info);
        tracing::info!("[CPU-THREADS] {}", thread_cpu_info);
        tracing::info!(
            "[METRICS] Wake reasons: {} | deadlines: {} | {}",
            wake_info,
            self.deadline_reason_summary(),
            self.wake_sleep_summary()
        );
        tracing::info!(
            "[WAKE] {} deadlines={} {}",
            wake_info,
            self.deadline_reason_summary(),
            self.wake_sleep_summary()
        );
        tracing::info!("[METRICS] Present sources: {}", present_info);
        tracing::info!("[PRESENT] {}", present_info);
        tracing::info!("[METRICS] Image cache: {}", image_cache_info);
        tracing::info!("[IMAGE-CACHE] {}", image_cache_info);
        tracing::info!(
            "[METRICS] Monitor self-cost: {}",
            self.get_monitor_stage_summary()
        );
        tracing::info!("[MONITOR-COST] {}", self.get_monitor_stage_summary());

        let video_cuda_total = self.get_recent_avg_video_cuda_total_ms();
        if video_cuda_total > 0.0 {
            tracing::info!("[VIDEO-UPLOAD] {}", self.video_upload_summary());
        }

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

    fn thread_cpu_summary(&self) -> String {
        let current = ThreadCpuSnapshot::collect_current();
        let mut previous = self.previous_thread_cpu_snapshot.lock();
        let summary = current.format_top(previous.as_ref(), 8);
        *previous = Some(current);
        summary
    }

    fn wake_reason_summary(&self) -> String {
        WAKE_REASONS
            .iter()
            .map(|reason| {
                format!(
                    "{}={}",
                    reason.label(),
                    self.wake_reasons[reason.as_index()].load(Ordering::Relaxed)
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn deadline_reason_summary(&self) -> String {
        DEADLINE_REASONS
            .iter()
            .map(|reason| {
                format!(
                    "{}={}",
                    reason.label(),
                    self.deadline_reasons[reason.as_index()].load(Ordering::Relaxed)
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}
