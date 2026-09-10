use super::control::NativePlaybackControl;
use super::decode::{self, NativeDecodeConfig};
use crate::metrics::PerformanceMetrics;
use crate::observability::video_backend::VideoBackendMetricKind;
use crate::video::{
    LatestFrameMailbox, PlayerEvent, PlayerEventKind, VideoBackendKind, VideoFrame,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::Instant;
use tracing::{debug, info};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SharedDecodeKey {
    uri: String,
    group_id: u64,
    max_publish_fps: Option<u32>,
}

static SHARED_DECODERS: Lazy<Mutex<HashMap<SharedDecodeKey, Weak<SharedDecodeSession>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(super) struct NativeSubscriber {
    source_id: Arc<String>,
    source_key: Arc<str>,
    session_id: u64,
    mailbox: LatestFrameMailbox,
    event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
    metrics: Arc<PerformanceMetrics>,
    first_frame_tx: SyncSender<VideoFrame>,
    first_frame_sent: AtomicBool,
    active: AtomicBool,
    creation_start: Instant,
}

struct FanoutState {
    subscribers: HashMap<u64, Arc<NativeSubscriber>>,
    latest_frame: Option<VideoFrame>,
}

pub(super) struct NativeDecodeFanout {
    state: Mutex<FanoutState>,
    primary: Arc<NativeSubscriber>,
    subscriber_count: AtomicUsize,
    latest_initialized: AtomicBool,
}

pub(super) struct SharedDecodeSession {
    key: SharedDecodeKey,
    pub(super) control: Arc<NativePlaybackControl>,
    fanout: Arc<NativeDecodeFanout>,
    worker: Mutex<Option<JoinHandle<()>>>,
    stopping: AtomicBool,
    finished: Arc<AtomicBool>,
}

pub(super) struct AcquireRequest {
    pub uri: String,
    pub source_id: Arc<String>,
    pub session_id: u64,
    pub group_id: u64,
    pub mailbox: LatestFrameMailbox,
    pub event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
    pub metrics: Arc<PerformanceMetrics>,
    pub max_publish_fps: Option<u32>,
    pub creation_start: Instant,
    pub worker_name: String,
}

impl SharedDecodeSession {
    pub(super) fn acquire(
        request: AcquireRequest,
    ) -> anyhow::Result<(Arc<Self>, Receiver<VideoFrame>)> {
        let (first_frame_tx, first_frame_rx) = std::sync::mpsc::sync_channel(1);
        let subscriber = Arc::new(NativeSubscriber {
            source_id: request.source_id.clone(),
            source_key: Arc::from(request.source_id.as_str()),
            session_id: request.session_id,
            mailbox: request.mailbox,
            event_tx: request.event_tx,
            metrics: request.metrics.clone(),
            first_frame_tx,
            first_frame_sent: AtomicBool::new(false),
            active: AtomicBool::new(true),
            creation_start: request.creation_start,
        });
        let key = SharedDecodeKey {
            uri: request.uri.clone(),
            group_id: request.group_id,
            max_publish_fps: request.max_publish_fps,
        };

        let mut registry = SHARED_DECODERS.lock();
        registry.retain(|_, session| session.strong_count() > 0);
        if let Some(session) = registry.get(&key).and_then(Weak::upgrade)
            && !session.stopping.load(Ordering::Acquire)
            && !session.finished.load(Ordering::Acquire)
        {
            session.fanout.add_subscriber(subscriber);
            info!(
                "[NATIVE-SHARED] {} session={} joined decode group {:x}; subscribers={}",
                request.source_id,
                request.session_id,
                request.group_id,
                session.fanout.subscriber_count()
            );
            return Ok((session, first_frame_rx));
        }

        let control = Arc::new(NativePlaybackControl::new());
        let fanout = Arc::new(NativeDecodeFanout::new(subscriber));
        let finished = Arc::new(AtomicBool::new(false));
        let session = Arc::new(Self {
            key: key.clone(),
            control: control.clone(),
            fanout: fanout.clone(),
            worker: Mutex::new(None),
            stopping: AtomicBool::new(false),
            finished: finished.clone(),
        });
        let config = NativeDecodeConfig {
            uri: request.uri,
            source_id: request.source_id,
            session_id: request.session_id,
            fanout,
            metrics: request.metrics,
            control,
            max_publish_fps: request.max_publish_fps,
            creation_start: request.creation_start,
        };
        let worker = std::thread::Builder::new()
            .name(request.worker_name)
            .spawn(move || {
                decode::run(config);
                finished.store(true, Ordering::Release);
            })?;
        *session.worker.lock() = Some(worker);
        registry.insert(key, Arc::downgrade(&session));
        debug!(
            "[NATIVE-SHARED] created decode group {:x} for {}",
            request.group_id, session.key.uri
        );
        Ok((session, first_frame_rx))
    }

    pub(super) fn release(self: &Arc<Self>, session_id: u64) -> anyhow::Result<()> {
        let mut registry = SHARED_DECODERS.lock();
        if !self.fanout.remove_subscriber(session_id) || self.fanout.subscriber_count() != 0 {
            return Ok(());
        }
        if self.stopping.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if registry
            .get(&self.key)
            .and_then(Weak::upgrade)
            .is_some_and(|registered| Arc::ptr_eq(&registered, self))
        {
            registry.remove(&self.key);
        }
        drop(registry);
        self.control.stop();
        if let Some(worker) = self.worker.lock().take() {
            worker.join().map_err(|panic| {
                anyhow::anyhow!("shared native decoder thread panicked: {panic:?}")
            })?;
        }
        Ok(())
    }
}

impl NativeDecodeFanout {
    fn new(first: Arc<NativeSubscriber>) -> Self {
        Self {
            state: Mutex::new(FanoutState {
                subscribers: HashMap::from([(first.session_id, first.clone())]),
                latest_frame: None,
            }),
            primary: first,
            subscriber_count: AtomicUsize::new(1),
            latest_initialized: AtomicBool::new(false),
        }
    }

    fn add_subscriber(&self, subscriber: Arc<NativeSubscriber>) {
        let latest = {
            let mut state = self.state.lock();
            let inserted_new = state
                .subscribers
                .insert(subscriber.session_id, subscriber.clone())
                .is_none();
            if inserted_new {
                self.subscriber_count.fetch_add(1, Ordering::Release);
            }
            state
                .latest_frame
                .as_ref()
                .and_then(|frame| clone_for_session(frame, subscriber.session_id))
        };
        if let Some(frame) = latest {
            subscriber.deliver(frame);
        }
    }

    fn remove_subscriber(&self, session_id: u64) -> bool {
        let removed = self.state.lock().subscribers.remove(&session_id);
        if let Some(subscriber) = removed.as_ref() {
            subscriber.active.store(false, Ordering::Release);
            self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
        }
        removed.is_some()
    }

    fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Acquire)
    }

    pub(super) fn publish_frame(&self, frame: VideoFrame) {
        if self.subscriber_count.load(Ordering::Acquire) == 1
            && self.primary.active.load(Ordering::Acquire)
        {
            if !self.latest_initialized.load(Ordering::Acquire)
                && self
                    .latest_initialized
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.state.lock().latest_frame = frame.try_clone();
            }
            let mut frame = frame;
            frame.session_id = self.primary.session_id;
            self.primary.deliver(frame);
            return;
        }
        let (single_subscriber, subscribers) = {
            let mut state = self.state.lock();
            // A late subscriber only needs a valid preroll surface. Retaining
            // the first frame avoids an AVFrame ref/unref pair on every steady
            // frame; the next publication immediately brings it current.
            if state.latest_frame.is_none() {
                state.latest_frame = frame.try_clone();
                self.latest_initialized.store(true, Ordering::Release);
            }
            if state.subscribers.len() == 1 {
                (state.subscribers.values().next().cloned(), Vec::new())
            } else {
                (
                    None,
                    state.subscribers.values().cloned().collect::<Vec<_>>(),
                )
            }
        };
        if let Some(subscriber) = single_subscriber {
            let mut frame = frame;
            frame.session_id = subscriber.session_id;
            subscriber.deliver(frame);
            return;
        }
        let Some(first_subscriber) = subscribers.first() else {
            return;
        };
        let primary_index = subscribers
            .iter()
            .position(|subscriber| subscriber.session_id == frame.session_id)
            .unwrap_or(0);
        let mut deliveries = subscribers
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != primary_index)
            .filter_map(|(_, subscriber)| {
                clone_for_session(&frame, subscriber.session_id)
                    .map(|frame| (subscriber.clone(), frame))
            })
            .collect::<Vec<_>>();
        let mut primary_frame = frame;
        primary_frame.session_id = subscribers[primary_index].session_id;
        deliveries.push((subscribers[primary_index].clone(), primary_frame));

        if subscribers.iter().all(|subscriber| {
            subscriber
                .mailbox
                .shares_storage_with(&first_subscriber.mailbox)
        }) {
            let deliveries = deliveries
                .into_iter()
                .map(|(subscriber, frame)| subscriber.prepare_delivery(frame))
                .collect();
            first_subscriber.mailbox.publish_frame_batch(deliveries);
            return;
        }
        for (subscriber, frame) in deliveries {
            subscriber.deliver(frame);
        }
    }

    pub(super) fn report_fatal(&self, reason: String) {
        let subscribers = self
            .state
            .lock()
            .subscribers
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for subscriber in subscribers {
            subscriber
                .metrics
                .record_video_backend_metric(VideoBackendMetricKind::NativeDecodeError);
            let _ = subscriber.event_tx.blocking_send(PlayerEvent {
                source_id: subscriber.source_id.to_string(),
                session_id: subscriber.session_id,
                backend_kind: VideoBackendKind::Ffmpeg,
                kind: PlayerEventKind::Error,
                reason: reason.clone(),
            });
        }
    }
}

impl NativeSubscriber {
    fn deliver(&self, frame: VideoFrame) {
        let (source_id, frame) = self.prepare_delivery(frame);
        self.mailbox.publish_frame_key(source_id, frame);
    }

    fn prepare_delivery(&self, frame: VideoFrame) -> (Arc<str>, VideoFrame) {
        if !self.first_frame_sent.load(Ordering::Acquire)
            && self
                .first_frame_sent
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            if let Some(first_frame) = frame.try_clone() {
                let _ = self.first_frame_tx.try_send(first_frame);
            }
            info!(
                "[ASSET] {}: First native video frame produced in {:.3}ms",
                self.source_id,
                self.creation_start.elapsed().as_secs_f64() * 1000.0
            );
        }
        self.metrics
            .record_video_backend_metric(VideoBackendMetricKind::NativeFramePublished);
        (self.source_key.clone(), frame)
    }
}

fn clone_for_session(frame: &VideoFrame, session_id: u64) -> Option<VideoFrame> {
    let mut cloned = frame.try_clone()?;
    cloned.session_id = session_id;
    Some(cloned)
}
