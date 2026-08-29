use crate::image::types::{DecodedImagePayload, ImageLoadProfile, PreparedImageKey};
use std::collections::HashMap;
use std::ffi::CString;
use std::fs::{File, FileTimes, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

const CACHE_MAGIC: &[u8; 8] = b"KDXIMG04";
const FIXED_HEADER_LEN: u64 = 8 + (5 * 4) + 8;
const MAX_STORED_FORMAT_LEN: usize = 512;
const DEFAULT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_MAX_ENTRIES: usize = 4096;
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(180 * 24 * 60 * 60);
const DEFAULT_MIN_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const PRUNE_BATCH: usize = 64;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static POLICY: OnceLock<CachePolicy> = OnceLock::new();
static CACHE_INDEX: LazyLock<Mutex<CacheIndex>> =
    LazyLock::new(|| Mutex::new(CacheIndex::default()));

#[derive(Debug, Clone, Copy)]
struct CachePolicy {
    max_bytes: Option<u64>,
    max_entries: Option<usize>,
    max_age: Option<Duration>,
    min_free_bytes: u64,
    fsync: bool,
}

impl CachePolicy {
    fn from_env() -> Self {
        if env_bool("KALEIDUX_IMAGE_CACHE_UNLIMITED") {
            return Self {
                max_bytes: None,
                max_entries: None,
                max_age: None,
                min_free_bytes: 0,
                fsync: env_bool("KALEIDUX_IMAGE_CACHE_FSYNC"),
            };
        }
        Self {
            max_bytes: env_mib("KALEIDUX_IMAGE_CACHE_MAX_MIB", DEFAULT_MAX_BYTES),
            max_entries: env_usize("KALEIDUX_IMAGE_CACHE_MAX_ENTRIES", DEFAULT_MAX_ENTRIES),
            max_age: env_days("KALEIDUX_IMAGE_CACHE_MAX_AGE_DAYS", DEFAULT_MAX_AGE),
            min_free_bytes: env_mib("KALEIDUX_IMAGE_CACHE_MIN_FREE_MIB", DEFAULT_MIN_FREE_BYTES)
                .unwrap_or(0),
            fsync: env_bool("KALEIDUX_IMAGE_CACHE_FSYNC"),
        }
    }
}

#[derive(Debug, Clone)]
struct CacheIndexEntry {
    bytes: u64,
    last_access: SystemTime,
}

#[derive(Default)]
struct CacheIndex {
    root: Option<PathBuf>,
    entries: HashMap<PathBuf, CacheIndexEntry>,
    bytes: u64,
    evictions: u64,
    corruptions: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PersistentCacheSnapshot {
    pub(crate) bytes: u64,
    pub(crate) count: usize,
    pub(crate) evictions: u64,
    pub(crate) corruptions: u64,
}

#[derive(Debug)]
struct CacheHeader {
    width: u32,
    height: u32,
    source_width: u32,
    source_height: u32,
    format_len: usize,
    payload_len: usize,
}

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

fn policy() -> CachePolicy {
    *POLICY.get_or_init(CachePolicy::from_env)
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
}

fn env_mib(name: &str, default_bytes: u64) -> Option<u64> {
    match std::env::var(name).ok() {
        Some(value) => value
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|mib| (mib != 0).then(|| mib.saturating_mul(1024 * 1024))),
        None => Some(default_bytes),
    }
}

fn env_usize(name: &str, default: usize) -> Option<usize> {
    match std::env::var(name).ok() {
        Some(value) => value
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|count| (count != 0).then_some(count)),
        None => Some(default),
    }
}

fn env_days(name: &str, default: Duration) -> Option<Duration> {
    match std::env::var(name).ok() {
        Some(value) => value
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|days| (days != 0).then(|| Duration::from_secs(days * 24 * 60 * 60))),
        None => Some(default),
    }
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

pub(crate) fn probe_by_key(key: &PreparedImageKey) -> bool {
    let Some(cache_path) = path_for_key(key) else {
        return false;
    };
    match File::open(&cache_path).and_then(|file| validate_file_header(file).map(|_| ())) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => {
            discard_corrupt(&cache_path);
            false
        }
    }
}

pub(crate) fn load_by_key(key: &PreparedImageKey) -> Option<DecodedImagePayload> {
    let cache_path = path_for_key(key)?;
    match load_path(&cache_path) {
        Ok(payload) => {
            touch_access_time(&cache_path);
            Some(payload)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => {
            discard_corrupt(&cache_path);
            None
        }
    }
}

fn load_path(cache_path: &Path) -> std::io::Result<DecodedImagePayload> {
    let file = File::open(cache_path)?;
    let (mut reader, header) = validate_file_header(file)?;
    let mut format_bytes = vec![0; header.format_len];
    reader.read_exact(&mut format_bytes)?;
    let source_format = std::str::from_utf8(&format_bytes)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?
        .to_string();
    let mut data = Vec::new();
    data.try_reserve_exact(header.payload_len)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    data.resize(header.payload_len, 0);
    reader.read_exact(&mut data)?;

    Ok(DecodedImagePayload {
        data: data.into(),
        width: header.width,
        height: header.height,
        profile: ImageLoadProfile {
            format: format!("prepared-cache via {source_format}"),
            source_width: header.source_width,
            source_height: header.source_height,
            permit_wait: Duration::ZERO,
            decode: Duration::ZERO,
            convert: Duration::ZERO,
            resize: Duration::ZERO,
            expand: Duration::ZERO,
            resize_filter: None,
        },
    })
}

fn validate_file_header(file: File) -> std::io::Result<(BufReader<File>, CacheHeader)> {
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut magic = [0; 8];
    reader.read_exact(&mut magic)?;
    if &magic != CACHE_MAGIC {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
    }
    let width = read_u32(&mut reader)?;
    let height = read_u32(&mut reader)?;
    let source_width = read_u32(&mut reader)?;
    let source_height = read_u32(&mut reader)?;
    let format_len = read_u32(&mut reader)? as usize;
    let payload_len_u64 = read_u64(&mut reader)?;
    if format_len > MAX_STORED_FORMAT_LEN
        || payload_len_u64 != image_len(width, height).unwrap_or(u64::MAX)
        || file_len
            != FIXED_HEADER_LEN
                .saturating_add(format_len as u64)
                .saturating_add(payload_len_u64)
    {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
    }
    let payload_len = usize::try_from(payload_len_u64)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
    Ok((
        reader,
        CacheHeader {
            width,
            height,
            source_width,
            source_height,
            format_len,
            payload_len,
        },
    ))
}

pub(crate) fn store_by_key(key: &PreparedImageKey, payload: &DecodedImagePayload) {
    let Some(cache_path) = path_for_key(key) else {
        return;
    };
    let Some(expected_len) = image_len(payload.width, payload.height) else {
        return;
    };
    if payload.data.len() as u64 != expected_len {
        return;
    }
    let source_format = payload.profile.format.as_bytes();
    if source_format.len() > MAX_STORED_FORMAT_LEN {
        return;
    }
    let total_len = FIXED_HEADER_LEN
        .saturating_add(source_format.len() as u64)
        .saturating_add(expected_len);
    let Some(dir) = cache_path.parent() else {
        return;
    };
    if !ensure_capacity(dir, &cache_path, total_len, policy()) {
        tracing::warn!(
            "[IMAGE] Persistent cache budget/free-space floor rejected {} bytes",
            total_len
        );
        return;
    }

    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let tmp_path =
        cache_path.with_extension(format!("rgba.{}.{}.tmp", std::process::id(), sequence));
    let result = write_streamed(
        &tmp_path,
        payload,
        source_format,
        expected_len,
        policy().fsync,
    )
    .and_then(|()| std::fs::rename(&tmp_path, &cache_path));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
        return;
    }
    update_index_entry(&cache_path, total_len, SystemTime::now());
}

fn write_streamed(
    path: &Path,
    payload: &DecodedImagePayload,
    source_format: &[u8],
    payload_len: u64,
    fsync: bool,
) -> std::io::Result<()> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(CACHE_MAGIC)?;
    writer.write_all(&payload.width.to_le_bytes())?;
    writer.write_all(&payload.height.to_le_bytes())?;
    writer.write_all(&payload.profile.source_width.to_le_bytes())?;
    writer.write_all(&payload.profile.source_height.to_le_bytes())?;
    writer.write_all(&(source_format.len() as u32).to_le_bytes())?;
    writer.write_all(&payload_len.to_le_bytes())?;
    writer.write_all(source_format)?;
    writer.write_all(payload.data.as_ref())?;
    writer.flush()?;
    if fsync {
        writer.get_ref().sync_data()?;
    }
    Ok(())
}

fn touch_access_time(path: &Path) {
    let now = SystemTime::now();
    if let Ok(file) = OpenOptions::new().write(true).open(path) {
        let _ = file.set_times(FileTimes::new().set_modified(now));
    }
    if let Ok(bytes) = std::fs::metadata(path).map(|metadata| metadata.len()) {
        update_index_entry(path, bytes, now);
    }
}

fn discard_corrupt(path: &Path) {
    let removed = std::fs::remove_file(path).is_ok();
    let mut index = CACHE_INDEX
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    initialize_index(&mut index, path.parent().unwrap_or_else(|| Path::new(".")));
    remove_index_entry(&mut index, path);
    if removed {
        index.corruptions = index.corruptions.saturating_add(1);
    }
}

fn ensure_capacity(dir: &Path, path: &Path, incoming: u64, policy: CachePolicy) -> bool {
    let mut index = CACHE_INDEX
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    initialize_index(&mut index, dir);
    let replaced = index.entries.get(path).map_or(0, |entry| entry.bytes);
    let mut available = available_space(dir).unwrap_or(u64::MAX);
    let now = SystemTime::now();
    let mut candidates: Vec<_> = index
        .entries
        .iter()
        .filter(|(candidate, _)| candidate.as_path() != path)
        .map(|(path, entry)| (path.clone(), entry.last_access, entry.bytes))
        .collect();
    candidates.sort_by_key(|(_, access, _)| *access);

    let mut removed = 0;
    for (candidate, access, bytes) in candidates {
        let projected_bytes = index
            .bytes
            .saturating_sub(replaced)
            .saturating_add(incoming);
        let projected_count = index.entries.len() + usize::from(replaced == 0);
        let too_old = policy
            .max_age
            .is_some_and(|max_age| now.duration_since(access).is_ok_and(|age| age > max_age));
        let over_bytes = policy.max_bytes.is_some_and(|max| projected_bytes > max);
        let over_count = policy.max_entries.is_some_and(|max| projected_count > max);
        let low_space = available < policy.min_free_bytes.saturating_add(incoming);
        if !(too_old || over_bytes || over_count || low_space) || removed >= PRUNE_BATCH {
            break;
        }
        if std::fs::remove_file(&candidate).is_ok() {
            remove_index_entry(&mut index, &candidate);
            index.evictions = index.evictions.saturating_add(1);
            available = available.saturating_add(bytes);
            removed += 1;
        }
    }

    let projected_bytes = index
        .bytes
        .saturating_sub(replaced)
        .saturating_add(incoming);
    let projected_count = index.entries.len() + usize::from(replaced == 0);
    policy.max_bytes.is_none_or(|max| projected_bytes <= max)
        && policy.max_entries.is_none_or(|max| projected_count <= max)
        && available >= policy.min_free_bytes.saturating_add(incoming)
}

fn initialize_index(index: &mut CacheIndex, dir: &Path) {
    if index.root.as_deref() == Some(dir) {
        return;
    }
    index.root = Some(dir.to_path_buf());
    index.entries.clear();
    index.bytes = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rgba") {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let bytes = metadata.len();
        index.bytes = index.bytes.saturating_add(bytes);
        index.entries.insert(
            path,
            CacheIndexEntry {
                bytes,
                last_access: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            },
        );
    }
}

fn update_index_entry(path: &Path, bytes: u64, last_access: SystemTime) {
    let mut index = CACHE_INDEX
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    initialize_index(&mut index, path.parent().unwrap_or_else(|| Path::new(".")));
    if let Some(old) = index
        .entries
        .insert(path.to_path_buf(), CacheIndexEntry { bytes, last_access })
    {
        index.bytes = index.bytes.saturating_sub(old.bytes);
    }
    index.bytes = index.bytes.saturating_add(bytes);
}

fn remove_index_entry(index: &mut CacheIndex, path: &Path) {
    if let Some(entry) = index.entries.remove(path) {
        index.bytes = index.bytes.saturating_sub(entry.bytes);
    }
}

pub(crate) fn snapshot() -> PersistentCacheSnapshot {
    let Some(dir) = cache_dir() else {
        return PersistentCacheSnapshot::default();
    };
    let mut index = CACHE_INDEX
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    initialize_index(&mut index, &dir);
    PersistentCacheSnapshot {
        bytes: index.bytes,
        count: index.entries.len(),
        evictions: index.evictions,
        corruptions: index.corruptions,
    }
}

fn available_space(path: &Path) -> Option<u64> {
    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` points to writable storage.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: statvfs returned success and initialized the structure.
    let stats = unsafe { stats.assume_init() };
    Some(stats.f_bavail.saturating_mul(stats.f_frsize))
}

fn image_len(width: u32, height: u32) -> Option<u64> {
    u64::from(width)
        .checked_mul(u64::from(height))?
        .checked_mul(4)
}

fn read_u32(reader: &mut impl Read) -> std::io::Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::types::ImageSourceIdentity;

    fn unique_key(label: &str) -> PreparedImageKey {
        PreparedImageKey {
            source: ImageSourceIdentity {
                path: PathBuf::from(format!(
                    "/test/{label}-{}",
                    TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                )),
                file_len: 1,
                modified_secs: 2,
                modified_nanos: 3,
            },
            target_width: 1,
            target_height: 1,
        }
    }

    #[test]
    fn corrupt_entry_is_deleted_by_header_probe() {
        let key = unique_key("corrupt");
        let path = path_for_key(&key).expect("test cache path");
        std::fs::write(&path, b"not-a-valid-cache-entry").expect("write corrupt entry");
        assert!(!probe_by_key(&key));
        assert!(!path.exists());
    }

    #[test]
    fn capacity_rejects_an_entry_larger_than_the_byte_budget() {
        let key = unique_key("budget");
        let path = path_for_key(&key).expect("test cache path");
        let dir = path.parent().expect("cache directory");
        let tiny = CachePolicy {
            max_bytes: Some(3),
            max_entries: Some(1),
            max_age: None,
            min_free_bytes: 0,
            fsync: false,
        };
        assert!(!ensure_capacity(dir, &path, 4, tiny));
    }
}
