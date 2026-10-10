use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackState {
    Paused,
    Playing,
    Stopped,
}

impl PlaybackState {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Paused => 0,
            Self::Playing => 1,
            Self::Stopped => 2,
        }
    }
}

#[derive(Debug)]
struct ControlState {
    playback: PlaybackState,
    seek_ns: Option<u64>,
    frame_demanded: bool,
}

pub struct NativePlaybackControl {
    state: Mutex<ControlState>,
    wake: Condvar,
    playback: AtomicU8,
    seek_pending: AtomicBool,
    position_ns: AtomicU64,
    play_epoch: AtomicU64,
    volume: AtomicU64,
    audio_clock: Mutex<Option<(u64, Instant, u64)>>,
}

impl NativePlaybackControl {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(ControlState {
                playback: PlaybackState::Paused,
                seek_ns: None,
                // One credit permits prebuffering before playback starts.
                frame_demanded: true,
            }),
            wake: Condvar::new(),
            playback: AtomicU8::new(PlaybackState::Paused.as_u8()),
            seek_pending: AtomicBool::new(false),
            position_ns: AtomicU64::new(0),
            play_epoch: AtomicU64::new(0),
            volume: AtomicU64::new(0.0f64.to_bits()),
            audio_clock: Mutex::new(None),
        }
    }

    pub fn play(&self) {
        let mut state = self.state.lock();
        if state.playback != PlaybackState::Playing {
            self.play_epoch.fetch_add(1, Ordering::Relaxed);
        }
        state.playback = PlaybackState::Playing;
        // Preroll consumes the initial credit. Starting/resuming must wake the
        // demand-driven decoder even before a compositor callback arrives.
        state.frame_demanded = true;
        self.playback
            .store(PlaybackState::Playing.as_u8(), Ordering::Release);
        drop(state);
        self.wake.notify_all();
    }

    pub fn pause(&self) {
        let mut state = self.state.lock();
        state.playback = PlaybackState::Paused;
        self.playback
            .store(PlaybackState::Paused.as_u8(), Ordering::Release);
        drop(state);
        self.wake.notify_all();
    }

    pub fn stop(&self) {
        let mut state = self.state.lock();
        state.playback = PlaybackState::Stopped;
        self.playback
            .store(PlaybackState::Stopped.as_u8(), Ordering::Release);
        drop(state);
        self.wake.notify_all();
    }

    pub fn seek(&self, position_ns: u64) {
        let mut state = self.state.lock();
        state.seek_ns = Some(position_ns);
        state.frame_demanded = true;
        // Audio observes the epoch independently of the video worker. Publish
        // the requested position before waking either decoder.
        self.set_position_ns(position_ns);
        self.seek_pending.store(true, Ordering::Release);
        self.play_epoch.fetch_add(1, Ordering::Relaxed);
        drop(state);
        self.wake.notify_all();
    }

    pub fn request_frame(&self) {
        let mut state = self.state.lock();
        state.frame_demanded = true;
        drop(state);
        self.wake.notify_all();
    }

    pub fn consume_frame_demand(&self) {
        self.state.lock().frame_demanded = false;
    }

    pub fn wait_for_frame_demand(&self) -> bool {
        let mut state = self.state.lock();
        while !state.frame_demanded
            && state.seek_ns.is_none()
            && state.playback != PlaybackState::Stopped
        {
            self.wake.wait(&mut state);
        }
        state.playback != PlaybackState::Stopped
    }

    pub fn take_seek(&self) -> Option<u64> {
        let mut state = self.state.lock();
        let seek = state.seek_ns.take();
        self.seek_pending.store(false, Ordering::Release);
        drop(state);
        seek
    }

    pub fn has_pending_seek(&self) -> bool {
        self.seek_pending.load(Ordering::Acquire)
    }

    pub fn is_stopped(&self) -> bool {
        self.playback.load(Ordering::Acquire) == PlaybackState::Stopped.as_u8()
    }

    pub fn wait_until_playing(&self) -> bool {
        if self.playback.load(Ordering::Acquire) == PlaybackState::Playing.as_u8()
            && !self.seek_pending.load(Ordering::Acquire)
        {
            return true;
        }
        let mut state = self.state.lock();
        while state.playback == PlaybackState::Paused && state.seek_ns.is_none() {
            self.wake.wait(&mut state);
        }
        state.playback == PlaybackState::Playing && state.seek_ns.is_none()
    }

    pub fn wait_until(&self, deadline: Instant) -> bool {
        let mut state = self.state.lock();
        loop {
            if state.playback == PlaybackState::Stopped {
                return false;
            }
            if state.playback == PlaybackState::Paused || state.seek_ns.is_some() {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return true;
            }
            self.wake.wait_for(&mut state, deadline - now);
        }
    }

    pub fn is_playing(&self) -> bool {
        self.playback.load(Ordering::Acquire) == PlaybackState::Playing.as_u8()
    }

    pub fn set_position_ns(&self, position_ns: u64) {
        self.position_ns.store(position_ns, Ordering::Relaxed);
    }

    pub fn position_ns(&self) -> u64 {
        self.position_ns.load(Ordering::Relaxed)
    }

    pub fn play_epoch(&self) -> u64 {
        self.play_epoch.load(Ordering::Relaxed)
    }

    pub fn volume(&self) -> f64 {
        f64::from_bits(self.volume.load(Ordering::Acquire))
    }

    pub fn set_volume(&self, volume: f64) {
        self.volume
            .store(volume.clamp(0.0, 1.0).to_bits(), Ordering::Release);
        self.notify_audio();
    }

    pub fn notify_audio(&self) {
        // Pair notification with the waiter's state lock to avoid lost wakes.
        let guard = self.state.lock();
        self.wake.notify_all();
        drop(guard);
    }

    pub fn wait_for_audio(&self, stopped: &AtomicBool) -> bool {
        let mut state = self.state.lock();
        while (state.playback != PlaybackState::Playing || self.volume() == 0.0)
            && state.playback != PlaybackState::Stopped
            && !stopped.load(Ordering::Acquire)
        {
            self.wake.wait(&mut state);
        }
        state.playback != PlaybackState::Stopped && !stopped.load(Ordering::Acquire)
    }

    pub fn looped(&self) {
        self.set_position_ns(0);
        self.play_epoch.fetch_add(1, Ordering::Release);
        self.notify_audio();
    }

    pub fn update_audio_clock(&self, position: Option<u64>) {
        *self.audio_clock.lock() =
            position.map(|position| (position, Instant::now(), self.play_epoch()));
    }

    pub fn audio_position_ns(&self) -> Option<u64> {
        let clock = self.audio_clock.lock();
        let (position, observed, epoch) = (*clock)?;
        let age = observed.elapsed();
        (epoch == self.play_epoch()
            && self.is_playing()
            && self.volume() > 0.0
            && age < std::time::Duration::from_millis(200))
        .then(|| position.saturating_add(age.as_nanos().min(u64::MAX as u128) as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::NativePlaybackControl;

    #[test]
    fn audio_clock_is_ignored_while_muted_paused_or_after_seek() {
        let control = NativePlaybackControl::new();
        control.play();
        control.update_audio_clock(Some(1_000_000_000));
        assert!(control.audio_position_ns().is_none());
        control.set_volume(0.5);
        assert!(control.audio_position_ns().unwrap() >= 1_000_000_000);
        control.seek(4_000_000_000);
        assert_eq!(control.position_ns(), 4_000_000_000);
        assert!(control.audio_position_ns().is_none());
        control.update_audio_clock(Some(4_000_000_000));
        control.pause();
        assert!(control.audio_position_ns().is_none());
    }

    #[test]
    fn audio_wait_ends_on_retirement_without_stopping_video() {
        let control = std::sync::Arc::new(NativePlaybackControl::new());
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_control = control.clone();
        let worker_stop = stopped.clone();
        let worker = std::thread::spawn(move || worker_control.wait_for_audio(&worker_stop));
        stopped.store(true, std::sync::atomic::Ordering::Release);
        control.notify_audio();
        assert!(!worker.join().unwrap());
        assert!(!control.is_stopped());
    }

    #[test]
    fn atomic_playback_cache_tracks_control_changes() {
        let control = NativePlaybackControl::new();
        assert!(!control.is_playing());
        assert!(!control.is_stopped());
        control.play();
        assert!(control.is_playing());
        assert!(control.wait_until_playing());
        control.pause();
        assert!(!control.is_playing());
        control.stop();
        assert!(control.is_stopped());
        assert!(!control.wait_until_playing());
    }

    #[test]
    fn atomic_seek_cache_clears_when_seek_is_taken() {
        let control = NativePlaybackControl::new();
        control.seek(42);
        assert!(control.has_pending_seek());
        assert_eq!(control.take_seek(), Some(42));
        assert!(!control.has_pending_seek());
    }

    #[test]
    fn paused_seek_wakes_pacing_without_publishing_the_old_frame() {
        let control = NativePlaybackControl::new();
        control.seek(42);
        assert!(!control.wait_until_playing());
    }

    #[test]
    fn frame_demand_is_coalesced_and_consumed() {
        let control = NativePlaybackControl::new();
        assert!(control.wait_for_frame_demand());
        control.consume_frame_demand();
        control.request_frame();
        control.request_frame();
        assert!(control.wait_for_frame_demand());
        control.consume_frame_demand();
        control.stop();
        assert!(!control.wait_for_frame_demand());
    }

    #[test]
    fn start_and_resume_replenish_consumed_preroll_demand() {
        let control = NativePlaybackControl::new();
        control.consume_frame_demand();
        control.play();
        assert!(control.wait_for_frame_demand());
        control.consume_frame_demand();
        control.pause();
        control.play();
        assert!(control.wait_for_frame_demand());
    }
}
