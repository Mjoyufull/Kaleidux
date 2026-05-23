use crate::background::BackgroundWorkKind;
use crate::image as image_pipeline;
use crate::image::types::{
    DecodedImagePayload, DecodedSourceImage, ImageSourceDescriptor, ImageSourceIdentity,
    PreparedImageEntry, PreparedImageKey, SizedLruCache,
};
use parking_lot::Mutex as ParkingMutex;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

// Global semaphore to limit concurrent image decode tasks (prevents CPU/memory spikes).
// Defaults to 1 worker for smoother multi-output transitions; override with
// KLD_IMAGE_DECODE_WORKERS when higher throughput is preferred.
static IMAGE_DECODE_SEMAPHORE: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| {
        let workers = std::env::var("KLD_IMAGE_DECODE_WORKERS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .map(|v| v.clamp(1, 8))
            .unwrap_or(1);
        Arc::new(Semaphore::new(workers))
    });

const PREPARED_IMAGE_MEMORY_CACHE_ENTRIES: usize = 48;
const PREPARED_IMAGE_MEMORY_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;
const SOURCE_IMAGE_MEMORY_CACHE_ENTRIES: usize = 12;
const SOURCE_IMAGE_MEMORY_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;
const SOURCE_IMAGE_DESCRIPTOR_CACHE_ENTRIES: usize = 256;
const SOURCE_IMAGE_DESCRIPTOR_CACHE_MAX_BYTES: usize = 1024 * 1024;
pub(crate) const SLOW_IMAGE_PREPARE_MS: f64 = 100.0;
pub(crate) const LOW_POWER_IMAGE_PREFETCH_DEFER: Duration = Duration::from_millis(250);

static PREPARED_IMAGE_MEMORY_CACHE: once_cell::sync::Lazy<
    ParkingMutex<SizedLruCache<PreparedImageKey, Arc<PreparedImageEntry>>>,
> = once_cell::sync::Lazy::new(|| {
    ParkingMutex::new(SizedLruCache::new(
        PREPARED_IMAGE_MEMORY_CACHE_ENTRIES,
        PREPARED_IMAGE_MEMORY_CACHE_MAX_BYTES,
    ))
});

static SOURCE_IMAGE_MEMORY_CACHE: once_cell::sync::Lazy<
    ParkingMutex<SizedLruCache<ImageSourceIdentity, Arc<DecodedSourceImage>>>,
> = once_cell::sync::Lazy::new(|| {
    ParkingMutex::new(SizedLruCache::new(
        SOURCE_IMAGE_MEMORY_CACHE_ENTRIES,
        SOURCE_IMAGE_MEMORY_CACHE_MAX_BYTES,
    ))
});

static SOURCE_IMAGE_DESCRIPTOR_CACHE: once_cell::sync::Lazy<
    ParkingMutex<SizedLruCache<ImageSourceIdentity, Arc<ImageSourceDescriptor>>>,
> = once_cell::sync::Lazy::new(|| {
    ParkingMutex::new(SizedLruCache::new(
        SOURCE_IMAGE_DESCRIPTOR_CACHE_ENTRIES,
        SOURCE_IMAGE_DESCRIPTOR_CACHE_MAX_BYTES,
    ))
});

pub(crate) fn select_compatible_prepared_key<'a, I>(
    keys: I,
    identity: &ImageSourceIdentity,
    target_width: u32,
    target_height: u32,
) -> Option<PreparedImageKey>
where
    I: IntoIterator<Item = &'a PreparedImageKey>,
{
    keys.into_iter()
        .filter_map(|key| {
            if &key.source != identity
                || key.target_width < target_width
                || key.target_height < target_height
            {
                return None;
            }

            Some((
                key.clone(),
                u64::from(key.target_width) * u64::from(key.target_height),
            ))
        })
        .min_by_key(|(_key, area)| *area)
        .map(|(key, _area)| key)
}

pub(crate) fn prepared_image_key_for_identity(
    source: ImageSourceIdentity,
    target_width: u32,
    target_height: u32,
) -> PreparedImageKey {
    PreparedImageKey {
        source,
        target_width,
        target_height,
    }
}

#[cfg(test)]
pub(crate) fn prepared_image_cache_lookup_key(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<PreparedImageKey> {
    let descriptor = load_image_source_descriptor(path)?;
    Some(prepared_image_key_for_identity(
        descriptor.identity.clone(),
        target_width,
        target_height,
    ))
}

#[cfg(test)]
pub(crate) fn prepared_image_cache_path(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<PathBuf> {
    let key = prepared_image_cache_lookup_key(path, target_width, target_height)?;
    image_pipeline::persistent_cache::path_for_key(&key)
}

pub(crate) fn try_load_prepared_image_cache_by_key(
    key: &PreparedImageKey,
) -> Option<DecodedImagePayload> {
    image_pipeline::persistent_cache::load_by_key(key)
}

#[cfg(test)]
pub(crate) fn try_load_prepared_image_cache(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<DecodedImagePayload> {
    let key = prepared_image_cache_lookup_key(path, target_width, target_height)?;
    try_load_prepared_image_cache_by_key(&key)
}

pub(crate) fn store_prepared_image_cache_by_key(
    key: &PreparedImageKey,
    payload: &DecodedImagePayload,
) {
    image_pipeline::persistent_cache::store_by_key(key, payload);
}

#[cfg(test)]
pub(crate) fn store_prepared_image_cache(
    path: &Path,
    target_width: u32,
    target_height: u32,
    payload: &DecodedImagePayload,
) {
    let Some(key) = prepared_image_cache_lookup_key(path, target_width, target_height) else {
        return;
    };
    store_prepared_image_cache_by_key(&key, payload);
}

pub(crate) fn try_load_prepared_image_memory(
    key: &PreparedImageKey,
) -> Option<DecodedImagePayload> {
    PREPARED_IMAGE_MEMORY_CACHE
        .lock()
        .get_cloned(key)
        .map(|entry| entry.to_payload("prepared-memory-cache"))
}

pub(crate) fn try_load_compatible_prepared_image_memory(
    identity: &ImageSourceIdentity,
    target_width: u32,
    target_height: u32,
) -> Option<DecodedImagePayload> {
    let candidate_key = {
        let cache = PREPARED_IMAGE_MEMORY_CACHE.lock();
        select_compatible_prepared_key(
            cache.lru.iter().map(|(key, _entry)| key),
            identity,
            target_width,
            target_height,
        )
    }?;

    PREPARED_IMAGE_MEMORY_CACHE
        .lock()
        .get_cloned(&candidate_key)
        .map(|entry| entry.to_payload("prepared-memory-compatible"))
}

pub(crate) fn prepared_image_available_for_output(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> bool {
    let Some(descriptor) = load_image_source_descriptor(path) else {
        return false;
    };
    let (prepared_width, prepared_height) =
        prepared_target_dimensions_from_descriptor(&descriptor, target_width, target_height);
    let key = prepared_image_key_for_identity(
        descriptor.identity.clone(),
        prepared_width,
        prepared_height,
    );

    try_load_prepared_image_memory(&key).is_some()
        || try_load_compatible_prepared_image_memory(
            &descriptor.identity,
            prepared_width,
            prepared_height,
        )
        .is_some()
        || try_load_prepared_image_cache_by_key(&key).is_some()
}

pub(crate) fn store_prepared_image_memory(key: PreparedImageKey, payload: &DecodedImagePayload) {
    PREPARED_IMAGE_MEMORY_CACHE
        .lock()
        .put(key, Arc::new(PreparedImageEntry::from_payload(payload)));
}

pub(crate) fn try_load_decoded_source_memory(
    identity: &ImageSourceIdentity,
) -> Option<Arc<DecodedSourceImage>> {
    SOURCE_IMAGE_MEMORY_CACHE.lock().get_cloned(identity)
}

pub(crate) fn try_load_source_descriptor_memory(
    identity: &ImageSourceIdentity,
) -> Option<Arc<ImageSourceDescriptor>> {
    SOURCE_IMAGE_DESCRIPTOR_CACHE.lock().get_cloned(identity)
}

pub(crate) fn store_decoded_source_memory(
    identity: ImageSourceIdentity,
    source: Arc<DecodedSourceImage>,
) {
    SOURCE_IMAGE_MEMORY_CACHE.lock().put(identity, source);
}

pub(crate) fn store_source_descriptor_memory(descriptor: Arc<ImageSourceDescriptor>) {
    SOURCE_IMAGE_DESCRIPTOR_CACHE
        .lock()
        .put(descriptor.identity.clone(), descriptor);
}

pub(crate) fn load_image_source_descriptor(path: &Path) -> Option<Arc<ImageSourceDescriptor>> {
    let identity = image_pipeline::descriptor::source_identity(path)?;
    if let Some(descriptor) = try_load_source_descriptor_memory(&identity) {
        return Some(descriptor);
    }

    if let Some(source) = try_load_decoded_source_memory(&identity) {
        let descriptor = Arc::new(ImageSourceDescriptor::from_decoded_source(
            identity, &source,
        ));
        store_source_descriptor_memory(descriptor.clone());
        return Some(descriptor);
    }

    let (width, height) = image::image_dimensions(path).ok()?;
    let descriptor = image_pipeline::descriptor::from_dimensions(identity, width, height);
    store_source_descriptor_memory(descriptor.clone());
    Some(descriptor)
}

#[cfg(test)]
pub(crate) fn prepared_target_dimensions_from_path(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<(u32, u32)> {
    let descriptor = load_image_source_descriptor(path)?;
    Some(prepared_target_dimensions_from_descriptor(
        &descriptor,
        target_width,
        target_height,
    ))
}

pub(crate) fn prepared_target_dimensions_from_descriptor(
    descriptor: &ImageSourceDescriptor,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    image_pipeline::prepare::prepared_target_dimensions_from_dimensions(
        descriptor.width,
        descriptor.height,
        target_width,
        target_height,
    )
}

pub(crate) async fn acquire_image_work_permit(
    work_kind: BackgroundWorkKind,
    stage_name: &str,
) -> anyhow::Result<tokio::sync::OwnedSemaphorePermit> {
    let semaphore = IMAGE_DECODE_SEMAPHORE.clone();
    if matches!(work_kind, BackgroundWorkKind::ImagePrefetch) {
        return semaphore.try_acquire_owned().map_err(|_| {
            anyhow::anyhow!(
                "image prefetch {} skipped because decode workers are busy",
                stage_name
            )
        });
    }

    semaphore
        .acquire_owned()
        .await
        .map_err(|_| anyhow::anyhow!("image {} semaphore closed", stage_name))
}

pub(crate) fn decode_source_image(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    image_pipeline::decode::decode_source_image(path)
}

pub(crate) fn prepare_source_image_for_output(
    source: &DecodedSourceImage,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    image_pipeline::decode::prepare_source_image_for_output(source, target_width, target_height)
}

pub(crate) fn prepare_image_for_output_uncached(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    image_pipeline::decode::prepare_image_for_output_uncached(path, target_width, target_height)
}

pub(crate) use super::runtime_prefetch::{
    begin_image_prefetch_generation, schedule_image_prefetch_plan,
};
pub(crate) use super::runtime_request::request_prepared_image_payload;
pub(crate) use super::runtime_switch::ordered_pending_content_switches;
