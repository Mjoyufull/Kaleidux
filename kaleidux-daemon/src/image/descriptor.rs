use crate::image::types::{DecodedSourceImage, ImageSourceDescriptor, ImageSourceIdentity};
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

pub(crate) fn source_identity(path: &Path) -> Option<ImageSourceIdentity> {
    if crate::media_source::parse_colour(&path.to_string_lossy()).is_some() {
        return Some(ImageSourceIdentity {
            path: path.to_path_buf(),
            file_len: 4,
            modified_secs: 0,
            modified_nanos: 0,
        });
    }
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;

    Some(ImageSourceIdentity {
        path: path.to_path_buf(),
        file_len: meta.len(),
        modified_secs: modified.as_secs(),
        modified_nanos: modified.subsec_nanos(),
    })
}

pub(crate) fn from_decoded_source(
    identity: ImageSourceIdentity,
    source: &DecodedSourceImage,
) -> Arc<ImageSourceDescriptor> {
    Arc::new(ImageSourceDescriptor::from_decoded_source(identity, source))
}

pub(crate) fn from_dimensions(
    identity: ImageSourceIdentity,
    width: u32,
    height: u32,
) -> Arc<ImageSourceDescriptor> {
    Arc::new(ImageSourceDescriptor::from_dimensions(
        identity, width, height,
    ))
}
