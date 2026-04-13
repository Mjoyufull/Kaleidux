use once_cell::sync::Lazy;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundWorkKind {
    VideoPrepare,
    PlayerStop,
    ImageDecode,
    ImagePrefetch,
    QueueDiscovery,
    RendererInit,
    CudaWarmup,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackgroundWorkSnapshot {
    pub total_in_flight: u64,
    pub video_prepare: u64,
    pub player_stop: u64,
    pub image_decode: u64,
    pub image_prefetch: u64,
    pub queue_discovery: u64,
    pub renderer_init: u64,
    pub cuda_warmup: u64,
}

impl BackgroundWorkSnapshot {
    pub fn format_compact(self) -> String {
        format!(
            "total={} video_prepare={} player_stop={} image_decode={} image_prefetch={} queue_discovery={} renderer_init={} cuda_warmup={}",
            self.total_in_flight,
            self.video_prepare,
            self.player_stop,
            self.image_decode,
            self.image_prefetch,
            self.queue_discovery,
            self.renderer_init,
            self.cuda_warmup
        )
    }
}

#[derive(Default)]
struct BackgroundWorkCounters {
    total_in_flight: AtomicU64,
    video_prepare: AtomicU64,
    player_stop: AtomicU64,
    image_decode: AtomicU64,
    image_prefetch: AtomicU64,
    queue_discovery: AtomicU64,
    renderer_init: AtomicU64,
    cuda_warmup: AtomicU64,
}

impl BackgroundWorkCounters {
    fn counter(&self, kind: BackgroundWorkKind) -> &AtomicU64 {
        match kind {
            BackgroundWorkKind::VideoPrepare => &self.video_prepare,
            BackgroundWorkKind::PlayerStop => &self.player_stop,
            BackgroundWorkKind::ImageDecode => &self.image_decode,
            BackgroundWorkKind::ImagePrefetch => &self.image_prefetch,
            BackgroundWorkKind::QueueDiscovery => &self.queue_discovery,
            BackgroundWorkKind::RendererInit => &self.renderer_init,
            BackgroundWorkKind::CudaWarmup => &self.cuda_warmup,
        }
    }

    fn snapshot(&self) -> BackgroundWorkSnapshot {
        BackgroundWorkSnapshot {
            total_in_flight: self.total_in_flight.load(Ordering::Relaxed),
            video_prepare: self.video_prepare.load(Ordering::Relaxed),
            player_stop: self.player_stop.load(Ordering::Relaxed),
            image_decode: self.image_decode.load(Ordering::Relaxed),
            image_prefetch: self.image_prefetch.load(Ordering::Relaxed),
            queue_discovery: self.queue_discovery.load(Ordering::Relaxed),
            renderer_init: self.renderer_init.load(Ordering::Relaxed),
            cuda_warmup: self.cuda_warmup.load(Ordering::Relaxed),
        }
    }
}

struct BackgroundWorkInner {
    accepting_new: AtomicBool,
    counters: BackgroundWorkCounters,
    notify: Notify,
}

#[derive(Clone)]
pub struct BackgroundWorkRegistry {
    inner: Arc<BackgroundWorkInner>,
}

impl BackgroundWorkRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BackgroundWorkInner {
                accepting_new: AtomicBool::new(true),
                counters: BackgroundWorkCounters::default(),
                notify: Notify::new(),
            }),
        }
    }

    pub fn is_accepting_new(&self) -> bool {
        self.inner.accepting_new.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        self.inner.accepting_new.store(false, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    pub fn snapshot(&self) -> BackgroundWorkSnapshot {
        self.inner.counters.snapshot()
    }

    fn try_begin(&self, kind: BackgroundWorkKind) -> Option<BackgroundWorkGuard> {
        if !self.is_accepting_new() {
            return None;
        }

        self.inner
            .counters
            .total_in_flight
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .counters
            .counter(kind)
            .fetch_add(1, Ordering::SeqCst);

        if self.is_accepting_new() {
            Some(BackgroundWorkGuard {
                registry: self.clone(),
                kind,
            })
        } else {
            self.finish(kind);
            None
        }
    }

    fn finish(&self, kind: BackgroundWorkKind) {
        self.inner
            .counters
            .counter(kind)
            .fetch_sub(1, Ordering::SeqCst);
        self.inner
            .counters
            .total_in_flight
            .fetch_sub(1, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    pub async fn wait_for_quiet(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.snapshot().total_in_flight == 0 {
                return true;
            }

            let notified = self.inner.notify.notified();
            if self.snapshot().total_in_flight == 0 {
                return true;
            }

            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.snapshot().total_in_flight == 0;
            }
        }
    }
}

impl Default for BackgroundWorkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

struct BackgroundWorkGuard {
    registry: BackgroundWorkRegistry,
    kind: BackgroundWorkKind,
}

impl Drop for BackgroundWorkGuard {
    fn drop(&mut self) {
        self.registry.finish(self.kind);
    }
}

static GLOBAL_BACKGROUND_WORK: Lazy<BackgroundWorkRegistry> =
    Lazy::new(BackgroundWorkRegistry::new);

pub fn global_registry() -> BackgroundWorkRegistry {
    GLOBAL_BACKGROUND_WORK.clone()
}

pub fn is_accepting_new_work() -> bool {
    global_registry().is_accepting_new()
}

pub fn close_global_work() {
    global_registry().close();
}

pub async fn wait_for_global_quiet(timeout: Duration) -> bool {
    global_registry().wait_for_quiet(timeout).await
}

pub fn snapshot() -> BackgroundWorkSnapshot {
    global_registry().snapshot()
}

pub fn spawn_blocking_tracked<T, F>(kind: BackgroundWorkKind, work: F) -> Option<JoinHandle<T>>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let registry = global_registry();
    let guard = registry.try_begin(kind)?;
    Some(tokio::task::spawn_blocking(move || {
        let _guard = guard;
        work()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_waits_for_tracked_work_to_finish() {
        let registry = BackgroundWorkRegistry::new();
        let guard = registry
            .try_begin(BackgroundWorkKind::ImageDecode)
            .expect("registry should accept work");
        let waiter = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.wait_for_quiet(Duration::from_millis(250)).await })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(guard);

        assert!(waiter.await.expect("wait task should complete"));
        assert_eq!(registry.snapshot().total_in_flight, 0);
    }

    #[test]
    fn closing_registry_rejects_new_work() {
        let registry = BackgroundWorkRegistry::new();
        registry.close();
        assert!(
            registry
                .try_begin(BackgroundWorkKind::VideoPrepare)
                .is_none()
        );
    }
}
