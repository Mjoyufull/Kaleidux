//! Offloads descriptor I/O to tracked blocking work under the image semaphore.

use super::types::ImageSourceDescriptor;
use crate::background::BackgroundWorkKind;
use std::path::Path;
use std::sync::Arc;

pub(crate) async fn probe_descriptor(
    path: &Path,
    work_kind: BackgroundWorkKind,
) -> anyhow::Result<Option<Arc<ImageSourceDescriptor>>> {
    let path = path.to_path_buf();
    let permit = super::runtime_cache::acquire_image_work_permit(work_kind, "probe").await?;
    let handle = super::runtime_request::spawn_image_blocking(work_kind, move || {
        let _permit = permit;
        super::runtime_cache::load_image_source_descriptor(&path)
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("image probe skipped during shutdown or capacity exhaustion"))?;
    handle
        .await
        .map_err(|error| anyhow::anyhow!("image probe task panicked: {error}"))
}
