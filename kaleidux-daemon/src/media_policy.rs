//! Immutable launch policy shared by media workers and their process-wide caches.

use std::sync::OnceLock;

static STREAMED: OnceLock<bool> = OnceLock::new();

/// Select the experimental memory policy before creating media workers.
pub fn initialize(streamed: bool) -> anyhow::Result<()> {
    STREAMED
        .set(streamed)
        .map_err(|_| anyhow::anyhow!("media launch policy was already initialized"))
}

pub(crate) fn streamed() -> bool {
    STREAMED.get().copied().unwrap_or(false)
}
