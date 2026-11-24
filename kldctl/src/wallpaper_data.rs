//! Parses frame export arguments and writes the daemon's PNG response.

use anyhow::{Context, Result};
use kaleidux_common::{FrameRequest, MAX_FRAME_PNG_BYTES, Response};
use std::{io::Write, path::PathBuf, time::Duration};
use tokio::{io::AsyncReadExt, net::UnixStream};

#[derive(clap::Args)]
pub(crate) struct FrameArgs {
    #[arg(short, long)]
    output: Option<String>,
    /// Decode the first frame of the current scene
    #[arg(long)]
    first: bool,
    /// Decode a local media file before selecting it as wallpaper
    #[arg(long, requires = "first", conflicts_with = "output")]
    source: Option<PathBuf>,
    /// Decode this many seconds into the scene or explicit source
    #[arg(long, requires = "first")]
    at: Option<f64>,
    /// Limit the longest edge in pixels
    #[arg(long)]
    max_size: Option<u32>,
    /// Write the PNG to this file instead of stdout
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
}

impl FrameArgs {
    pub(crate) fn request(self) -> Result<FrameRequest> {
        let at_ns = self
            .at
            .map(|seconds| {
                let duration =
                    Duration::try_from_secs_f64(seconds).context("Invalid frame timestamp")?;
                u64::try_from(duration.as_nanos()).context("Frame timestamp is too large")
            })
            .transpose()?;
        Ok(FrameRequest {
            output: self.output,
            first: self.first,
            source: self.source.map(super::media_locator).transpose()?,
            at_ns,
            max_size: self.max_size,
        })
    }
}

pub(crate) async fn read_png(stream: UnixStream, file: Option<PathBuf>) -> Result<()> {
    let mut bytes = Vec::new();
    stream
        .take((MAX_FRAME_PNG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    anyhow::ensure!(
        bytes.len() <= MAX_FRAME_PNG_BYTES,
        "Frame PNG exceeds the export limit"
    );
    if !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        if let Ok(Response::Error(error)) = serde_json::from_slice(&bytes) {
            anyhow::bail!(error);
        }
        anyhow::bail!("Daemon returned no frame PNG");
    }
    anyhow::ensure!(
        bytes.ends_with(b"IEND\xae\x42\x60\x82"),
        "Incomplete frame PNG"
    );
    if let Some(path) = file {
        std::fs::write(path, bytes)?;
    } else {
        std::io::stdout().lock().write_all(&bytes)?;
    }
    Ok(())
}
