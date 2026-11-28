//! Prepares bounded RGBA assets and uses the system's Pango font resolver for labels.

use anyhow::{Context, Result};
use image::{DynamicImage, ImageReader, RgbaImage};
use std::io::BufReader;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub(super) fn read(path: &Path) -> Result<RgbaImage> {
    Ok(decode(path)?.into_rgba8())
}

pub(super) fn depth(path: &Path) -> Result<RgbaImage> {
    let depth = decode(path)?.into_luma16();
    // Bilinear sampling of both bytes preserves all 16 bits in the result.
    Ok(RgbaImage::from_fn(depth.width(), depth.height(), |x, y| {
        let [high, low] = depth.get_pixel(x, y).0[0].to_be_bytes();
        image::Rgba([high, low, 0, 255])
    }))
}

fn decode(path: &Path) -> Result<DynamicImage> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= 64 * 1024 * 1024,
        "Layer asset must be a regular file no larger than 64 MiB"
    );
    let mut reader = ImageReader::new(BufReader::new(file)).with_guessed_format()?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(4096);
    limits.max_image_height = Some(4096);
    limits.max_alloc = Some(64 * 1024 * 1024);
    reader.limits(limits);
    Ok(reader.decode()?)
}

pub(super) fn label(text: &str, font: &str, colour: &str) -> Result<RgbaImage> {
    anyhow::ensure!(
        !text.is_empty() && text.len() <= 2048 && !font.is_empty() && font.len() <= 192,
        "Text or font description exceeds the layer limit"
    );
    anyhow::ensure!(
        crate::media_source::parse_colour(colour).is_some(),
        "Text colour must use #RRGGBB or #RRGGBBAA"
    );
    let scratch = Scratch::new()?;
    let output = scratch.0.join("label.png");
    let mut child =
        Command::new(std::env::var_os("KLD_PANGO_VIEW").unwrap_or_else(|| "pango-view".into()))
            .args(["--no-display", "--margin=0", "--background=transparent"])
            .arg(format!("--font={font}"))
            .arg(format!("--foreground={colour}"))
            .arg(format!("--text={text}"))
            .arg("--output")
            .arg(&output)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("System font morphs require pango-view")?;
    let deadline = Instant::now() + Duration::from_secs(4);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("System font rasterization took too long");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    anyhow::ensure!(status.success(), "Pango rejected the font or label");
    read(&output)
}

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        use std::os::unix::fs::DirBuilderExt;
        let path = std::env::temp_dir().join(format!("kld-layer-{:016x}", rand::random::<u64>()));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
