use crate::metrics::PerformanceMetrics;
use crate::video::{LatestFrameMailbox, PlayerEvent, VideoFrame};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePathTier {
    SurfaceZeroCopy,
    SingleGpuCopy,
    HardwareTransfer,
    SoftwareDecode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeDecoderApi {
    VulkanVideo,
    Vaapi,
    Nvdec,
    Qsv,
    Amf,
    VideoToolbox,
    D3d12,
    D3d11,
    MediaCodec,
}

pub(crate) fn report_native_surface_import_failure() -> bool {
    false
}

pub struct NativePlayer;

impl NativePlayer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _uri: &str,
        _source_id: Arc<String>,
        _session_id: u64,
        _volume: f64,
        _mailbox: LatestFrameMailbox,
        _event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
        _metrics: Arc<PerformanceMetrics>,
        _decode_group_id: Option<u64>,
        _max_publish_fps: Option<u32>,
        _creation_start: Instant,
    ) -> anyhow::Result<Self> {
        anyhow::bail!(
            "FFmpeg backend is disabled in this build; rebuild with --features backend-ffmpeg"
        )
    }

    pub fn prebuffer<F>(&mut self, _should_abort: F) -> anyhow::Result<Option<VideoFrame>>
    where
        F: Fn() -> bool,
    {
        anyhow::bail!("FFmpeg backend is disabled in this build")
    }

    pub fn start(&self) {}

    pub fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn pause(&self) {}
    pub fn resume(&self) {}
    pub fn seek_to_position_ns(&self, _position_ns: u64) {}
    pub fn current_position_ns(&self) -> u64 {
        0
    }
    pub fn request_frame(&self) {}
    pub fn set_volume(&mut self, _volume: f64) {}
}
