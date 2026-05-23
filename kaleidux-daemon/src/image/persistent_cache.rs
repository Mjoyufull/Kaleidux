use crate::image::types::{DecodedImagePayload, ImageLoadProfile, PreparedImageKey};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Duration;

const CACHE_MAGIC: &[u8; 8] = b"KDXIMG03";
const MAX_STORED_FORMAT_LEN: usize = 512;

#[cfg(test)]
fn root_dir() -> Option<PathBuf> {
    Some(std::env::temp_dir().join("kaleidux-test-cache"))
}

#[cfg(not(test))]
fn root_dir() -> Option<PathBuf> {
    Some(dirs::cache_dir()?.join("kaleidux"))
}

fn cache_dir() -> Option<PathBuf> {
    let dir = root_dir()?.join("prepared-images");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

fn cache_key(key: &PreparedImageKey) -> String {
    let source = &key.source;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    source.path.as_os_str().as_encoded_bytes().hash(&mut hasher);
    source.file_len.hash(&mut hasher);
    source.modified_secs.hash(&mut hasher);
    source.modified_nanos.hash(&mut hasher);
    key.target_width.hash(&mut hasher);
    key.target_height.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub(crate) fn path_for_key(key: &PreparedImageKey) -> Option<PathBuf> {
    let dir = cache_dir()?;
    Some(dir.join(format!("{}.rgba", cache_key(key))))
}

pub(crate) fn load_by_key(key: &PreparedImageKey) -> Option<DecodedImagePayload> {
    let cache_path = path_for_key(key)?;
    let bytes = std::fs::read(cache_path).ok()?;
    if bytes.len() < CACHE_MAGIC.len() + (5 * 4) {
        return None;
    }
    if &bytes[..CACHE_MAGIC.len()] != CACHE_MAGIC {
        return None;
    }

    let mut cursor = CACHE_MAGIC.len();
    let width = read_u32(&bytes, &mut cursor)?;
    let height = read_u32(&bytes, &mut cursor)?;
    let source_width = read_u32(&bytes, &mut cursor)?;
    let source_height = read_u32(&bytes, &mut cursor)?;
    let format_len = read_u32(&bytes, &mut cursor)? as usize;
    if format_len > MAX_STORED_FORMAT_LEN {
        return None;
    }
    let format_bytes = bytes.get(cursor..cursor + format_len)?;
    cursor += format_len;
    let source_format = std::str::from_utf8(format_bytes).ok()?.to_string();
    let data = bytes.get(cursor..)?.to_vec();
    let expected_len = image_len(width, height)?;
    if data.len() != expected_len {
        return None;
    }

    Some(DecodedImagePayload {
        data: data.into(),
        width,
        height,
        profile: ImageLoadProfile {
            format: format!("prepared-cache via {source_format}"),
            source_width,
            source_height,
            permit_wait: Duration::ZERO,
            decode: Duration::ZERO,
            convert: Duration::ZERO,
            resize: Duration::ZERO,
            expand: Duration::ZERO,
            resize_filter: None,
        },
    })
}

pub(crate) fn store_by_key(key: &PreparedImageKey, payload: &DecodedImagePayload) {
    let Some(cache_path) = path_for_key(key) else {
        return;
    };

    let Some(expected_len) = image_len(payload.width, payload.height) else {
        return;
    };
    if payload.data.len() != expected_len {
        return;
    }
    let source_format = payload.profile.format.as_bytes();
    if source_format.len() > MAX_STORED_FORMAT_LEN {
        return;
    }

    let mut bytes =
        Vec::with_capacity(CACHE_MAGIC.len() + (5 * 4) + source_format.len() + payload.data.len());
    bytes.extend_from_slice(CACHE_MAGIC);
    bytes.extend_from_slice(&payload.width.to_le_bytes());
    bytes.extend_from_slice(&payload.height.to_le_bytes());
    bytes.extend_from_slice(&payload.profile.source_width.to_le_bytes());
    bytes.extend_from_slice(&payload.profile.source_height.to_le_bytes());
    bytes.extend_from_slice(&(source_format.len() as u32).to_le_bytes());
    bytes.extend_from_slice(source_format);
    bytes.extend_from_slice(payload.data.as_ref());

    let tmp_path = cache_path.with_extension("rgba.tmp");
    if std::fs::write(&tmp_path, &bytes).is_ok() {
        let _ = std::fs::rename(tmp_path, cache_path);
    }
}

fn image_len(width: u32, height: u32) -> Option<usize> {
    (width as usize)
        .checked_mul(height as usize)?
        .checked_mul(4)
}

fn read_u32(buf: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = *cursor + 4;
    let slice = buf.get(*cursor..end)?;
    *cursor = end;
    Some(u32::from_le_bytes(slice.try_into().ok()?))
}
