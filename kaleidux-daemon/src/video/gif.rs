//! Decodes GIF disposal and timing natively from a bounded canvas.

use super::{VideoFrame, VideoFrameFormat, VideoFrameStorage, VideoGeometry};
use anyhow::Result;
use image::{AnimationDecoder, ImageDecoder, codecs::gif::GifDecoder};
use std::io::BufReader;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn is_gif_path(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gif"))
        && !crate::media_source::MediaSource::is_virtual(path)
}

#[derive(Clone, Copy)]
pub(crate) struct GifOptions {
    pub speed: f32,
    pub repeat: bool,
}

pub(super) fn decode(
    path: &Path,
    settings: GifOptions,
    session_id: u64,
    stream: &mut super::canvas::FrameStream,
) -> Result<()> {
    loop {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)?;
        anyhow::ensure!(file.metadata()?.is_file(), "GIF must be a regular file");
        let mut decoder = GifDecoder::new(BufReader::new(file))?;
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(8192);
        limits.max_image_height = Some(8192);
        limits.max_alloc = Some(96 * 1024 * 1024);
        decoder.set_limits(limits)?;
        let mut position_ns = 0_u64;
        let mut frame_count = 0;
        for decoded in decoder.into_frames() {
            let decoded = decoded?;
            let (numerator, denominator) = decoded.delay().numer_denom_ms();
            let delay = Duration::from_secs_f64(
                (f64::from(numerator) / f64::from(denominator) / 1000.0).max(0.010)
                    / f64::from(settings.speed),
            );
            let duration_ns = delay.as_nanos() as u64;
            let rgba = decoded.into_buffer();
            let (width, height) = rgba.dimensions();
            let frame = VideoFrame {
                storage: VideoFrameStorage::Cpu(Arc::from(rgba.into_raw())),
                width,
                height,
                stride: width * 4,
                format: VideoFrameFormat::Rgba,
                session_id,
                pts_ns: Some(position_ns),
                duration_ns: Some(duration_ns),
                color: Default::default(),
                geometry: VideoGeometry::for_dimensions(width, height),
            };
            if !stream.publish(frame) {
                return Ok(());
            }
            frame_count += 1;
            position_ns = position_ns.saturating_add(duration_ns);
            if !stream.wait(delay) {
                return Ok(());
            }
        }
        anyhow::ensure!(frame_count != 0, "GIF has no frames");
        if !settings.repeat || frame_count == 1 {
            return Ok(());
        }
    }
}
