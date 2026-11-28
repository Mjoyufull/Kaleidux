//! Reads explicitly listed shaders, scenes, local media and streams during discovery.

use crate::media_source::MediaSource;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub(crate) fn is_list(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                "kaleidux-wallpapers.txt" | "kaleidux advanced wallpapers.txt"
            )
        })
}

pub(super) fn entries(file: &Path) -> anyhow::Result<Vec<PathBuf>> {
    const MAX_BYTES: u64 = 256 * 1024;
    let input = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(file)?;
    let metadata = input.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= MAX_BYTES,
        "Wallpaper list exceeds 256 KiB"
    );
    let root = file.parent().unwrap_or_else(|| Path::new("."));
    let mut sources = Vec::new();
    for line in BufReader::new(input.take(MAX_BYTES)).lines() {
        let line = line?;
        let text = line.trim();
        if text.is_empty()
            || (text.starts_with('#') && crate::media_source::parse_colour(text).is_none())
        {
            continue;
        }
        match MediaSource::parse(text) {
            Ok(MediaSource::File(path)) => {
                let path = path
                    .strip_prefix("~")
                    .ok()
                    .and_then(|relative| dirs::home_dir().map(|home| home.join(relative)))
                    .unwrap_or(path);
                let path = root.join(path);
                let entry = if path.is_dir()
                    || path.file_name().is_some_and(|name| name == "project.json")
                {
                    crate::scene::workshop_path(&path)
                } else {
                    path.canonicalize().map_err(anyhow::Error::from)
                };
                if let Ok(absolute) = entry
                    && absolute != file
                    && !is_list(&absolute)
                {
                    sources.push(absolute);
                }
            }
            Ok(source) => sources.push(source.locator()),
            Err(error) => tracing::warn!("[QUEUE] Invalid wallpaper list entry: {error:#}"),
        }
    }
    Ok(sources)
}
