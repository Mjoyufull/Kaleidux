//! X11 Present telemetry shared by the WSI baseline and any future custom DRI3 tier.
//!
//! Present `Flip` is deliberately reported as a Present mode, never as proof of
//! compositor bypass or KMS direct scanout.

use x11rb::protocol::present::{CompleteMode, CompleteNotifyEvent, IdleNotifyEvent};

#[derive(Debug, Default)]
pub struct X11PresentTelemetry {
    pub wsi_submitted: u64,
    pub complete: u64,
    pub idle: u64,
    pub copy: u64,
    pub flip: u64,
    pub skip: u64,
    pub suboptimal_copy: u64,
    pub last_ust: u64,
    pub last_msc: u64,
    pub event_batches: u64,
    pub event_batch_events: u64,
    pub event_batch_high_water: u64,
    pub randr_events: u64,
    pub randr_reconciles: u64,
    pub slot_wait_ns: u64,
}

impl X11PresentTelemetry {
    pub fn record_wsi_submit(&mut self) {
        self.wsi_submitted = self.wsi_submitted.saturating_add(1);
    }

    pub fn record_complete(&mut self, event: &CompleteNotifyEvent) {
        self.complete = self.complete.saturating_add(1);
        self.last_ust = event.ust;
        self.last_msc = event.msc;
        if event.mode == CompleteMode::COPY {
            self.copy = self.copy.saturating_add(1);
        } else if event.mode == CompleteMode::FLIP {
            self.flip = self.flip.saturating_add(1);
        } else if event.mode == CompleteMode::SKIP {
            self.skip = self.skip.saturating_add(1);
        } else if event.mode == CompleteMode::SUBOPTIMAL_COPY {
            self.suboptimal_copy = self.suboptimal_copy.saturating_add(1);
        }
    }

    pub fn record_idle(&mut self, _event: &IdleNotifyEvent) {
        self.idle = self.idle.saturating_add(1);
    }

    pub fn record_event_batch(&mut self, events: u64) {
        if events == 0 {
            return;
        }
        self.event_batches = self.event_batches.saturating_add(1);
        self.event_batch_events = self.event_batch_events.saturating_add(events);
        self.event_batch_high_water = self.event_batch_high_water.max(events);
    }

    pub fn event_batch_average(&self) -> f64 {
        if self.event_batches == 0 {
            0.0
        } else {
            self.event_batch_events as f64 / self.event_batches as f64
        }
    }

    pub fn format_summary(&self, server_kind: &str) -> String {
        format!(
            "server={} backend=wsi submitted={} complete={} idle={} copy={} flip={} skip={} suboptimal_copy={} last_ust={} last_msc={} slot_wait_ns={} event_batches={} event_batch_avg={:.2} event_batch_high_water={} randr_events={} randr_reconciles={}",
            server_kind,
            self.wsi_submitted,
            self.complete,
            self.idle,
            self.copy,
            self.flip,
            self.skip,
            self.suboptimal_copy,
            self.last_ust,
            self.last_msc,
            self.slot_wait_ns,
            self.event_batches,
            self.event_batch_average(),
            self.event_batch_high_water,
            self.randr_events,
            self.randr_reconciles,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_batch_average_ignores_empty_polls() {
        let mut telemetry = X11PresentTelemetry::default();
        telemetry.record_event_batch(0);
        telemetry.record_event_batch(2);
        telemetry.record_event_batch(4);
        assert_eq!(telemetry.event_batches, 2);
        assert_eq!(telemetry.event_batch_high_water, 4);
        assert_eq!(telemetry.event_batch_average(), 3.0);
    }
}
