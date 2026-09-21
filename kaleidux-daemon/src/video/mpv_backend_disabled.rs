use super::{
    LatestFrameMailbox, MpvComposedVideoTarget, MpvNativeVideoTarget, PlayerEvent, VideoFrame,
};
use crate::metrics::PerformanceMetrics;
use std::sync::Arc;
use std::time::Instant;

pub struct MpvPlayer;

impl MpvPlayer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _uri: &str,
        _source_id: Arc<String>,
        _session_id: u64,
        _volume: f64,
        _frame_mailbox: LatestFrameMailbox,
        _player_event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
        _metrics: Arc<PerformanceMetrics>,
        _max_publish_fps: Option<u32>,
        _render_size: Option<(u32, u32)>,
        _native_target: Option<MpvNativeVideoTarget>,
        _composed_target: Option<MpvComposedVideoTarget>,
        _start_time: Instant,
    ) -> anyhow::Result<Self> {
        anyhow::bail!("mpv backend is disabled in this build; rebuild with --features backend-mpv")
    }

    pub fn prebuffer<F>(&mut self, _should_abort: F) -> anyhow::Result<Option<VideoFrame>>
    where
        F: Fn() -> bool,
    {
        anyhow::bail!("mpv backend is disabled in this build")
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn renders_natively(&self) -> bool {
        false
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn set_volume(&self, _volume: f64) {}

    pub fn pause(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn current_position_ns(&self) -> Option<u64> {
        None
    }

    pub fn seek_to_position_ns(&self, _position_ns: u64) -> anyhow::Result<()> {
        Ok(())
    }
}
