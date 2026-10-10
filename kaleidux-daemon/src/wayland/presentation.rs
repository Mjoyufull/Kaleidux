use std::time::Instant;

use wayland_client::{Connection, Dispatch, QueueHandle, WEnum};
use wayland_protocols::wp::presentation_time::client::{
    wp_presentation::{self, WpPresentation},
    wp_presentation_feedback::{self, WpPresentationFeedback},
};

#[derive(Debug, Clone)]
pub(crate) struct FeedbackData {
    pub output: String,
    pub requested_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct PresentationTelemetry {
    pub clock_id: Option<u32>,
    pub requested: u64,
    pub presented: u64,
    pub discarded: u64,
    pub last_refresh_ns: Option<u32>,
    pub last_sequence: Option<u64>,
    pub last_timestamp_ns: Option<u128>,
    pub last_flags: u32,
    pub last_feedback_latency_us: Option<u64>,
}

impl PresentationTelemetry {
    pub fn summary(&self) -> String {
        format!(
            "clock={:?} requested={} presented={} discarded={} refresh_ns={:?} seq={:?} timestamp_ns={:?} flags={:#x} feedback_us={:?}",
            self.clock_id,
            self.requested,
            self.presented,
            self.discarded,
            self.last_refresh_ns,
            self.last_sequence,
            self.last_timestamp_ns,
            self.last_flags,
            self.last_feedback_latency_us,
        )
    }
}

impl Dispatch<WpPresentation, ()> for super::WaylandBackend {
    fn event(
        state: &mut Self,
        _proxy: &WpPresentation,
        event: wp_presentation::Event,
        _data: &(),
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        if let wp_presentation::Event::ClockId { clk_id } = event {
            state.presentation_telemetry.clock_id = Some(clk_id);
            tracing::info!("[WAYLAND-PRESENT] presentation clock id={clk_id}");
        }
    }
}

impl Dispatch<WpPresentationFeedback, FeedbackData> for super::WaylandBackend {
    fn event(
        state: &mut Self,
        _proxy: &WpPresentationFeedback,
        event: wp_presentation_feedback::Event,
        data: &FeedbackData,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        match event {
            wp_presentation_feedback::Event::Presented {
                tv_sec_hi,
                tv_sec_lo,
                tv_nsec,
                refresh,
                seq_hi,
                seq_lo,
                flags,
            } => {
                let seconds = (u64::from(tv_sec_hi) << 32) | u64::from(tv_sec_lo);
                let sequence = (u64::from(seq_hi) << 32) | u64::from(seq_lo);
                let flags = match flags {
                    WEnum::Value(value) => value.bits(),
                    WEnum::Unknown(value) => value,
                };
                let telemetry = &mut state.presentation_telemetry;
                telemetry.requested = telemetry.requested.saturating_add(1);
                telemetry.presented = telemetry.presented.saturating_add(1);
                telemetry.last_refresh_ns = Some(refresh);
                telemetry.last_sequence = Some(sequence);
                telemetry.last_timestamp_ns =
                    Some(u128::from(seconds) * 1_000_000_000 + u128::from(tv_nsec));
                telemetry.last_flags = flags;
                telemetry.last_feedback_latency_us = Some(
                    data.requested_at
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
                if crate::wayland::trace_frame_events_enabled() {
                    tracing::trace!(
                        "[WAYLAND-PRESENT] output={} refresh_ns={} seq={} flags={:#x} latency_us={:?}",
                        data.output,
                        refresh,
                        sequence,
                        flags,
                        telemetry.last_feedback_latency_us,
                    );
                }
            }
            wp_presentation_feedback::Event::Discarded => {
                state.presentation_telemetry.requested =
                    state.presentation_telemetry.requested.saturating_add(1);
                state.presentation_telemetry.discarded =
                    state.presentation_telemetry.discarded.saturating_add(1);
                tracing::debug!(
                    "[WAYLAND-PRESENT] output={} feedback discarded",
                    data.output
                );
            }
            wp_presentation_feedback::Event::SyncOutput { .. } => {}
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PresentationTelemetry;

    #[test]
    fn summary_names_clock_refresh_and_visibility_outcomes() {
        let telemetry = PresentationTelemetry {
            clock_id: Some(1),
            requested: 4,
            presented: 3,
            discarded: 1,
            last_refresh_ns: Some(16_666_667),
            ..Default::default()
        };
        let summary = telemetry.summary();
        assert!(summary.contains("clock=Some(1)"));
        assert!(summary.contains("refresh_ns=Some(16666667)"));
        assert!(summary.contains("discarded=1"));
    }
}
