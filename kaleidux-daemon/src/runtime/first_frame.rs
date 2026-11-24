//! Decodes a bounded local frame before starting or seeking any player.

use crate::media_source::MediaSource;
use anyhow::{Context, Result};
use kaleidux_common::MAX_FRAME_PNG_BYTES;
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::io::AsyncReadExt;

pub(super) async fn first_png(source: String, at_ns: u64, max_size: u32) -> Result<Vec<u8>> {
    match MediaSource::parse(&source)? {
        MediaSource::File(path) => tokio::time::timeout(
            Duration::from_secs(8),
            decode_local_frame(path, at_ns, max_size),
        )
        .await
        .context("First-frame decode timed out")?,
        MediaSource::Colour(rgba) => {
            use image::ImageEncoder;
            anyhow::ensure!(at_ns == 0, "A solid colour has no timeline");
            let mut png = Vec::new();
            image::codecs::png::PngEncoder::new(&mut png).write_image(
                &rgba,
                1,
                1,
                image::ExtendedColorType::Rgba8,
            )?;
            Ok(png)
        }
        MediaSource::Network(_) => anyhow::bail!("First-frame export requires a local source"),
    }
}

async fn decode_local_frame(path: PathBuf, at_ns: u64, max_size: u32) -> Result<Vec<u8>> {
    let absolute = tokio::fs::canonicalize(path)
        .await
        .context("Frame source is unavailable")?;
    anyhow::ensure!(
        tokio::fs::metadata(&absolute).await?.is_file(),
        "Frame source is not a regular file"
    );
    let start = format!("{}.{:09}", at_ns / 1_000_000_000, at_ns % 1_000_000_000);
    let scale = format!(
        "scale=min(iw\\,{max_size}):min(ih\\,{max_size}):force_original_aspect_ratio=decrease"
    );
    let mut child = tokio::process::Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-threads",
            "2",
            "-max_pixels",
            "67108864",
            "-protocol_whitelist",
            "file,pipe",
            "-ss",
            &start,
            "-i",
        ])
        .arg(absolute)
        .args([
            "-map",
            "0:v:0",
            "-frames:v",
            "1",
            "-filter_threads",
            "1",
            "-vf",
            &scale,
            "-threads",
            "1",
            "-c:v",
            "png",
            "-f",
            "image2pipe",
            "pipe:1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("First-frame export requires ffmpeg on PATH")?;
    let mut bytes = Vec::new();
    child
        .stdout
        .take()
        .context("Missing FFmpeg frame pipe")?
        .take((MAX_FRAME_PNG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    anyhow::ensure!(
        bytes.len() <= MAX_FRAME_PNG_BYTES,
        "Frame PNG exceeds the export limit"
    );
    anyhow::ensure!(
        child.wait().await?.success() && bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "FFmpeg could not decode a frame at that position"
    );
    Ok(bytes)
}
