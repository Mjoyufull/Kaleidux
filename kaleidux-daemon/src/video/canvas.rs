//! Shares preroll, pause and frame delivery for software-rendered canvases.

use super::{LatestFrameMailbox, PlayerEvent, PlayerEventKind, VideoBackendKind, VideoFrame};
use anyhow::{Context, Result};
use parking_lot::{Condvar, Mutex};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) struct CanvasPlayer {
    control: Arc<Clock>,
    preroll: Option<Receiver<Result<VideoFrame>>>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy)]
pub(crate) enum CanvasSource {
    Gif(super::gif::GifOptions),
    Web,
}

impl CanvasSource {
    pub(super) fn backend(self) -> VideoBackendKind {
        match self {
            Self::Gif(_) => VideoBackendKind::Gif,
            Self::Web => VideoBackendKind::Web,
        }
    }
}

pub(super) struct FrameStream {
    clock: Arc<Clock>,
    first: Option<SyncSender<Result<VideoFrame>>>,
    mailbox: LatestFrameMailbox,
    source: Arc<str>,
}

#[cfg(feature = "wallpaper-web")]
#[derive(Default, serde::Serialize)]
pub(super) struct WebInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pointer: Option<[f32; 4]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<Vec<f32>>,
}

#[cfg(feature = "wallpaper-web")]
pub(super) enum WebWake {
    Frame,
    Paused(bool),
    Stopped,
}

impl FrameStream {
    pub(super) fn publish(&mut self, frame: VideoFrame) -> bool {
        if let Some(send) = self.first.take() {
            return send.send(Ok(frame)).is_ok();
        }
        self.clock
            .position_ns
            .store(frame.pts_ns.unwrap_or(0), Ordering::Relaxed);
        self.mailbox.publish_frame_key(self.source.clone(), frame);
        true
    }

    pub(super) fn wait(&self, duration: Duration) -> bool {
        self.clock.wait_playing(duration)
    }

    pub(super) fn stopped(&self) -> bool {
        self.clock.state.lock().stopped
    }

    #[cfg(feature = "wallpaper-web")]
    pub(super) fn take_web_inputs(&self) -> Option<WebInputs> {
        let mut state = self.clock.state.lock();
        let pending = &mut state.web_inputs;
        if pending.pointer.is_none() && pending.audio.is_none() {
            None
        } else {
            Some(std::mem::take(pending))
        }
    }

    #[cfg(feature = "wallpaper-web")]
    pub(super) fn wait_web(&self, mut remaining: Duration, notified_paused: bool) -> WebWake {
        let mut state = self.clock.state.lock();
        loop {
            if state.stopped {
                return WebWake::Stopped;
            }
            if state.paused != notified_paused {
                return WebWake::Paused(state.paused);
            }
            if state.paused {
                self.clock.changed.wait(&mut state);
            } else if remaining.is_zero() {
                return WebWake::Frame;
            } else {
                let started = Instant::now();
                self.clock.changed.wait_for(&mut state, remaining);
                remaining = remaining.saturating_sub(started.elapsed());
            }
        }
    }
}

struct Clock {
    state: Mutex<Playback>,
    changed: Condvar,
    position_ns: AtomicU64,
}

struct Playback {
    paused: bool,
    stopped: bool,
    #[cfg(feature = "wallpaper-web")]
    web_inputs: WebInputs,
}

impl CanvasPlayer {
    pub(super) fn open(
        path: PathBuf,
        source: Arc<str>,
        session: u64,
        mailbox: LatestFrameMailbox,
        kind: CanvasSource,
        size: Option<(u32, u32)>,
        fps: Option<u32>,
        event_sender: tokio::sync::mpsc::Sender<PlayerEvent>,
    ) -> Result<Self> {
        let (send, receive) = mpsc::sync_channel(1);
        let control = Arc::new(Clock {
            state: Mutex::new(Playback {
                paused: true,
                stopped: false,
                #[cfg(feature = "wallpaper-web")]
                web_inputs: WebInputs::default(),
            }),
            changed: Condvar::new(),
            position_ns: AtomicU64::new(0),
        });
        let clock = control.clone();
        let worker = std::thread::Builder::new()
            .name("kld-canvas".into())
            .spawn(move || {
                let mut stream = FrameStream {
                    clock,
                    first: Some(send),
                    mailbox,
                    source,
                };
                let result = match kind {
                    CanvasSource::Gif(settings) => {
                        super::gif::decode(&path, settings, session, &mut stream)
                    }
                    CanvasSource::Web => super::web::run(&path, session, size, fps, &mut stream),
                };
                if let Err(error) = result {
                    if let Some(send) = stream.first.take() {
                        let _ = send.send(Err(error));
                    } else if !stream.stopped() {
                        let _ = event_sender.blocking_send(PlayerEvent {
                            source_id: stream.source.to_string(),
                            session_id: session,
                            backend_kind: kind.backend(),
                            kind: PlayerEventKind::Error,
                            reason: format!("Canvas playback stopped: {error:#}"),
                        });
                    }
                }
            })?;
        Ok(Self {
            control,
            preroll: Some(receive),
            worker: Some(worker),
        })
    }

    pub(super) fn prebuffer(&mut self, abort: impl Fn() -> bool) -> Result<VideoFrame> {
        let receive = self
            .preroll
            .take()
            .context("Canvas first frame already read")?;
        let end = Instant::now() + Duration::from_secs(7);
        while !abort() && Instant::now() < end {
            match receive.recv_timeout(Duration::from_millis(30)) {
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                result => {
                    return result.context("Canvas producer closed before its first frame")?;
                }
            }
        }
        anyhow::bail!("Canvas frame preparation expired or was superseded")
    }

    pub(super) fn set_paused(&self, paused: bool) {
        self.control.state.lock().paused = paused;
        self.control.changed.notify_one();
    }

    #[cfg(feature = "wallpaper-web")]
    pub(super) fn apply_web_inputs(&self, input: &kaleidux_common::LayerInputs) {
        let mut state = self.control.state.lock();
        if input.pointer.is_some() {
            state.web_inputs.pointer = input.pointer;
        }
        if let Some(audio) = &input.audio {
            match &mut state.web_inputs.audio {
                Some(pending) => pending.clone_from(audio),
                None => state.web_inputs.audio = Some(audio.clone()),
            }
        }
    }

    pub(super) fn paused(&self) -> bool {
        self.control.state.lock().paused
    }

    pub(super) fn position_ns(&self) -> u64 {
        self.control.position_ns.load(Ordering::Relaxed)
    }

    pub(super) fn stop(&mut self) {
        self.control.state.lock().stopped = true;
        self.control.changed.notify_one();
        // The decoder owns its file, timers and canvas. A slow filesystem must
        // not make a display-thread shutdown wait for that file to wake up.
        if let Some(worker) = self.worker.take().filter(|worker| worker.is_finished()) {
            let _ = worker.join();
        }
    }
}

impl Drop for CanvasPlayer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Clock {
    fn wait_playing(&self, mut remaining: Duration) -> bool {
        let mut state = self.state.lock();
        while !state.stopped {
            if state.paused {
                self.changed.wait(&mut state);
            } else if remaining.is_zero() {
                return true;
            } else {
                let started = Instant::now();
                self.changed.wait_for(&mut state, remaining);
                remaining = remaining.saturating_sub(started.elapsed());
            }
        }
        false
    }
}
