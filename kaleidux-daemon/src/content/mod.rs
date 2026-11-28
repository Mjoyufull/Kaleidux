pub mod sessions;
pub(crate) mod switch;
pub(crate) mod video_start;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

// Resolve symlinks only when opening the chosen media on a blocking worker.
fn check_playlist_blacklist(path: &Path, blacklist: &HashSet<PathBuf>) -> anyhow::Result<()> {
    anyhow::ensure!(
        !crate::queue::BlacklistIdentity::new(blacklist).contains(path),
        "Wallpaper is blacklisted"
    );
    Ok(())
}
