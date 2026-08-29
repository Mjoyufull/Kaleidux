use super::VideoFrame;
#[cfg(test)]
use super::VideoFrameFormat;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Default)]
struct MailboxState {
    frames: HashMap<Arc<str>, VideoFrame>,
    pending_notifications: HashSet<Arc<str>>,
    pending_since: HashMap<Arc<str>, Instant>,
}

#[derive(Clone, Default)]
pub struct LatestFrameMailbox {
    state: Arc<parking_lot::Mutex<MailboxState>>,
    overwrite_count: Arc<AtomicU64>,
    signal_pending: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl LatestFrameMailbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish_frame(&self, source_id: &str, frame: VideoFrame) {
        self.publish_frame_key(Arc::from(source_id), frame);
    }

    pub(crate) fn publish_frame_key(&self, source_id: Arc<str>, frame: VideoFrame) {
        let mut state = self.state.lock();
        if state.frames.insert(source_id.clone(), frame).is_some() {
            self.overwrite_count.fetch_add(1, Ordering::Relaxed);
        }
        state
            .pending_since
            .insert(source_id.clone(), Instant::now());
        let should_signal = state.pending_notifications.insert(source_id);
        drop(state);

        if should_signal {
            self.signal_pending.store(true, Ordering::Release);
            self.notify.notify_one();
        }
    }

    /// Publish synchronized output frames with one lock sequence and at most
    /// one runtime wake.
    #[cfg_attr(not(feature = "backend-ffmpeg"), allow(dead_code))]
    pub(crate) fn publish_frame_batch(&self, frames_to_publish: Vec<(Arc<str>, VideoFrame)>) {
        if frames_to_publish.is_empty() {
            return;
        }
        let published_at = Instant::now();
        let mut should_signal = false;
        {
            let mut state = self.state.lock();
            for (source_id, frame) in frames_to_publish {
                if state.frames.insert(source_id.clone(), frame).is_some() {
                    self.overwrite_count.fetch_add(1, Ordering::Relaxed);
                }
                state.pending_since.insert(source_id.clone(), published_at);
                should_signal |= state.pending_notifications.insert(source_id);
            }
        }
        if should_signal {
            self.signal_pending.store(true, Ordering::Release);
            self.notify.notify_one();
        }
    }

    #[cfg_attr(not(feature = "backend-ffmpeg"), allow(dead_code))]
    pub(crate) fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    pub fn take_frame(&self, source_id: &str) -> Option<VideoFrame> {
        let mut state = self.state.lock();
        let frame = state.frames.remove(source_id);
        state.pending_notifications.remove(source_id);
        state.pending_since.remove(source_id);
        frame
    }

    pub fn defer_notification(&self, source_id: &str) {
        self.state.lock().pending_notifications.remove(source_id);
    }

    pub fn inspect_frame<R, F>(&self, source_id: &str, inspect: F) -> Option<R>
    where
        F: FnOnce(&VideoFrame) -> R,
    {
        let state = self.state.lock();
        state.frames.get(source_id).map(inspect)
    }

    pub fn has_pending_frame(&self, source_id: &str) -> bool {
        self.state.lock().frames.contains_key(source_id)
    }

    pub fn pending_frame_age(&self, source_id: &str) -> Option<Duration> {
        let state = self.state.lock();
        if !state.frames.contains_key(source_id) {
            return None;
        }
        state.pending_since.get(source_id).map(Instant::elapsed)
    }

    pub fn clear_source(&self, source_id: &str) {
        let mut state = self.state.lock();
        state.frames.remove(source_id);
        state.pending_notifications.remove(source_id);
        state.pending_since.remove(source_id);
    }

    pub fn take_overwrite_count(&self) -> u64 {
        self.overwrite_count.swap(0, Ordering::Relaxed)
    }

    pub fn has_signal_pending(&self) -> bool {
        self.signal_pending.load(Ordering::Acquire)
    }

    pub fn clear_signal_pending(&self) {
        self.signal_pending.store(false, Ordering::Release);
    }

    pub fn pending_sources(&self) -> Vec<String> {
        self.state
            .lock()
            .pending_notifications
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Inspect and consume all currently notified frames under one mailbox
    /// lock. The source key already owned by the notification set becomes the
    /// returned key, avoiding a per-frame clone in the main loop.
    pub(crate) fn drain_pending_frames<F>(&self, mut decide: F) -> Vec<(Arc<str>, VideoFrame, bool)>
    where
        F: FnMut(&str, &VideoFrame) -> (bool, bool),
    {
        let mut state = self.state.lock();
        let pending = std::mem::take(&mut state.pending_notifications);
        let mut drained = Vec::with_capacity(pending.len());
        for source_id in pending {
            let Some(frame) = state.frames.get(source_id.as_ref()) else {
                state.pending_since.remove(source_id.as_ref());
                continue;
            };
            let (should_accept, hold_for_callback) = decide(&source_id, frame);
            if hold_for_callback {
                continue;
            }
            if let Some(frame) = state.frames.remove(source_id.as_ref()) {
                state.pending_since.remove(source_id.as_ref());
                drained.push((source_id, frame, should_accept));
            }
        }
        drained
    }

    pub fn notified(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.notify.notified()
    }

    pub fn occupancy(&self) -> usize {
        self.state.lock().frames.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_frame(session_id: u64) -> VideoFrame {
        let _ = gstreamer::init();
        let buffer = gstreamer::Buffer::with_size(4).expect("buffer allocation should succeed");
        VideoFrame {
            storage: buffer.into(),
            width: 1,
            height: 1,
            stride: 4,
            format: VideoFrameFormat::Rgba,
            session_id,
            pts_ns: None,
            duration_ns: None,
            color: Default::default(),
            geometry: crate::video::VideoGeometry::for_dimensions(1, 1),
        }
    }

    #[test]
    fn pending_frame_state_tracks_source_presence() {
        let mailbox = LatestFrameMailbox::new();
        assert!(!mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_frame_age("HDMI-A-1").is_none());
        mailbox.publish_frame("HDMI-A-1", test_frame(7));
        assert!(mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_frame_age("HDMI-A-1").is_some());
        assert!(!mailbox.has_pending_frame("DP-2"));
        let _ = mailbox.take_frame("HDMI-A-1");
        assert!(!mailbox.has_pending_frame("HDMI-A-1"));
    }

    #[test]
    fn deferred_notification_keeps_frame_and_allows_resignal() {
        let mailbox = LatestFrameMailbox::new();
        mailbox.publish_frame("HDMI-A-1", test_frame(7));
        mailbox.clear_signal_pending();
        mailbox.defer_notification("HDMI-A-1");
        assert!(mailbox.has_pending_frame("HDMI-A-1"));
        assert!(mailbox.pending_sources().is_empty());
        mailbox.publish_frame("HDMI-A-1", test_frame(8));
        assert!(mailbox.has_signal_pending());
        assert_eq!(mailbox.pending_sources(), vec!["HDMI-A-1".to_string()]);
        assert_eq!(
            mailbox.take_frame("HDMI-A-1").map(|frame| frame.session_id),
            Some(8)
        );
    }

    #[test]
    fn batch_publish_queues_all_outputs_behind_one_signal_state() {
        let mailbox = LatestFrameMailbox::new();
        let same_mailbox = mailbox.clone();
        mailbox.publish_frame_batch(vec![
            (Arc::from("eDP-1"), test_frame(11)),
            (Arc::from("HEADLESS-1"), test_frame(12)),
        ]);
        assert!(mailbox.shares_storage_with(&same_mailbox));
        assert!(mailbox.has_signal_pending());
        let mut pending = mailbox.pending_sources();
        pending.sort();
        assert_eq!(pending, ["HEADLESS-1", "eDP-1"]);
        assert_eq!(
            mailbox.take_frame("eDP-1").map(|frame| frame.session_id),
            Some(11)
        );
        assert_eq!(
            mailbox
                .take_frame("HEADLESS-1")
                .map(|frame| frame.session_id),
            Some(12)
        );
    }
}
