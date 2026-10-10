use super::PerformanceMetrics;
use crate::observability::present::{FrameCallbackKind, RendererPresentKind};
use crate::observability::video_backend::VideoBackendMetricKind;
use crate::observability::wake::{DeadlineReason, WakeReason};
use std::sync::atomic::Ordering;
use std::time::Duration;

impl PerformanceMetrics {
    pub fn record_error(&self, error_type: &str) {
        if !self.detailed_enabled() {
            return;
        }
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let mut samples = self.error_samples.lock();
        samples.push_back((std::time::Instant::now(), error_type.to_string()));
        if samples.len() > 100 {
            samples.pop_front();
        }
    }

    pub fn record_wayland_idle_loop(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.wayland_idle_loops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wayland_hot_loop(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.wayland_hot_loops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn wayland_loop_counts(&self) -> (u64, u64) {
        (
            self.wayland_idle_loops.load(Ordering::Relaxed),
            self.wayland_hot_loops.load(Ordering::Relaxed),
        )
    }

    pub fn record_wayland_callback_wake(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.wayland_callback_wakes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wayland_expired_deadline_wake(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.wayland_expired_deadline_wakes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wake_reason(&self, reason: WakeReason) {
        if !self.detailed_enabled() {
            return;
        }
        self.wake_reasons[reason.as_index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_loop_wake(
        &self,
        reason: WakeReason,
        requested_sleep: Duration,
        actual_sleep: Duration,
    ) {
        if !self.detailed_enabled() {
            return;
        }
        self.record_wake_reason(reason);
        Self::push_sample(
            &self.wake_requested_sleep_samples,
            requested_sleep.as_secs_f64() * 1000.0,
            100,
        );
        Self::push_sample(
            &self.wake_actual_sleep_samples,
            actual_sleep.as_secs_f64() * 1000.0,
            100,
        );
    }

    pub fn record_deadline_reason(&self, reason: DeadlineReason) {
        if !self.detailed_enabled() {
            return;
        }
        self.deadline_reasons[reason.as_index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn wake_sleep_summary(&self) -> String {
        format!(
            "requested_avg={:.2}ms actual_avg={:.2}ms",
            Self::average_samples(&self.wake_requested_sleep_samples),
            Self::average_samples(&self.wake_actual_sleep_samples)
        )
    }

    pub fn record_static_image_present(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.record_renderer_present(RendererPresentKind::StaticImage);
        self.static_image_presents.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_video_frame_present_source(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.record_renderer_present(RendererPresentKind::AppsinkVideo);
        self.video_frame_presents.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_startup_release_present(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.record_renderer_present(RendererPresentKind::StartupRelease);
        self.startup_release_presents
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_renderer_present(&self, kind: RendererPresentKind) {
        if !self.detailed_enabled() {
            return;
        }
        self.present_kinds[kind.as_index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_frame_callback_request(&self, kind: FrameCallbackKind) {
        if !self.detailed_enabled() {
            return;
        }
        self.frame_callback_kinds[kind.as_index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_frame_callback_damage(&self, full_surface: bool) {
        if !self.detailed_enabled() {
            return;
        }
        if full_surface {
            self.frame_callback_full_damage
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.frame_callback_minimal_damage
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_image_prepared_memory_hit(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_prepared_memory_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_prepared_compatible_hit(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_prepared_compatible_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_prepared_disk_hit(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_prepared_disk_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_prepared_miss(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_prepared_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_source_memory_hit(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_source_memory_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_source_decode_miss(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_source_decode_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_shared_wait(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_shared_waits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_image_slow_prepare(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.image_slow_prepares.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_video_frame_received(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.video_frames_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_video_frame_uploaded(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.video_frames_uploaded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_video_frame_presented(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.video_frames_presented.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_video_frame_stale_skipped(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.video_frames_stale_skipped
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_shared_broker_hit(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.shared_broker_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_shared_broker_miss(&self) {
        if !self.detailed_enabled() {
            return;
        }
        self.shared_broker_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_video_backend_metric(&self, kind: VideoBackendMetricKind) {
        if !self.detailed_enabled() {
            return;
        }
        self.video_backend_metrics[kind.as_index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_native_sync_blocked(&self, duration: std::time::Duration) {
        if !self.detailed_enabled() {
            return;
        }
        self.native_sync_blocked_ns.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn record_video_backend_session(&self, backend: crate::video::VideoBackendKind) {
        let kind = match backend {
            crate::video::VideoBackendKind::Appsink => VideoBackendMetricKind::AppsinkSession,
            crate::video::VideoBackendKind::Mpv => VideoBackendMetricKind::MpvSession,
            crate::video::VideoBackendKind::Ffmpeg => VideoBackendMetricKind::NativeSession,
        };
        self.record_video_backend_metric(kind);
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
}
