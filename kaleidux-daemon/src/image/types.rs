use parking_lot::Mutex as ParkingMutex;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct ImageLoadProfile {
    pub format: String,
    pub source_width: u32,
    pub source_height: u32,
    pub permit_wait: Duration,
    pub decode: Duration,
    pub convert: Duration,
    pub resize: Duration,
    pub expand: Duration,
    pub resize_filter: Option<String>,
}

impl ImageLoadProfile {
    pub(crate) fn cpu_duration(&self) -> Duration {
        self.decode + self.convert + self.resize + self.expand
    }

    pub(crate) fn total_duration(&self) -> Duration {
        self.permit_wait + self.cpu_duration()
    }
}

#[derive(Debug)]
pub(crate) struct DecodedImagePayload {
    pub(crate) data: Arc<[u8]>,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) profile: ImageLoadProfile,
}

pub(crate) trait CacheSized {
    fn cache_size_bytes(&self) -> usize;
}

impl<T: CacheSized> CacheSized for Arc<T> {
    fn cache_size_bytes(&self) -> usize {
        self.as_ref().cache_size_bytes()
    }
}

pub(crate) struct SizedLruCache<K, V> {
    pub(crate) lru: lru::LruCache<K, V>,
    total_bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl<K: Hash + Eq, V: CacheSized> SizedLruCache<K, V> {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            lru: lru::LruCache::unbounded(),
            total_bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    pub(crate) fn get_cloned(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.lru.get(key).cloned()
    }

    pub(crate) fn put(&mut self, key: K, value: V) {
        let value_size = value.cache_size_bytes();
        if let Some(old) = self.lru.put(key, value) {
            self.total_bytes = self.total_bytes.saturating_sub(old.cache_size_bytes());
        }
        self.total_bytes = self.total_bytes.saturating_add(value_size);

        while self.lru.len() > self.max_entries || self.total_bytes > self.max_bytes {
            let Some((_evicted_key, evicted_value)) = self.lru.pop_lru() else {
                break;
            };
            self.total_bytes = self
                .total_bytes
                .saturating_sub(evicted_value.cache_size_bytes());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ImageSourceIdentity {
    pub(crate) path: PathBuf,
    pub(crate) file_len: u64,
    pub(crate) modified_secs: u64,
    pub(crate) modified_nanos: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct ImageSourceDescriptor {
    pub(crate) identity: ImageSourceIdentity,
    pub(crate) width: u32,
    pub(crate) height: u32,
}

impl CacheSized for ImageSourceDescriptor {
    fn cache_size_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.identity.path.as_os_str().as_encoded_bytes().len()
    }
}

impl ImageSourceDescriptor {
    pub(crate) fn from_dimensions(identity: ImageSourceIdentity, width: u32, height: u32) -> Self {
        Self {
            identity,
            width,
            height,
        }
    }

    pub(crate) fn from_decoded_source(
        identity: ImageSourceIdentity,
        source: &DecodedSourceImage,
    ) -> Self {
        Self::from_dimensions(identity, source.width, source.height)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PreparedImageKey {
    pub(crate) source: ImageSourceIdentity,
    pub(crate) target_width: u32,
    pub(crate) target_height: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedImageEntry {
    data: Arc<[u8]>,
    width: u32,
    height: u32,
    source_width: u32,
    source_height: u32,
    format: String,
    resize_filter: Option<String>,
}

impl CacheSized for PreparedImageEntry {
    fn cache_size_bytes(&self) -> usize {
        self.data.len()
    }
}

impl PreparedImageEntry {
    pub(crate) fn from_payload(payload: &DecodedImagePayload) -> Self {
        Self {
            data: payload.data.clone(),
            width: payload.width,
            height: payload.height,
            source_width: payload.profile.source_width,
            source_height: payload.profile.source_height,
            format: payload.profile.format.clone(),
            resize_filter: payload.profile.resize_filter.clone(),
        }
    }

    pub(crate) fn to_payload(&self, format: &str) -> DecodedImagePayload {
        DecodedImagePayload {
            data: self.data.clone(),
            width: self.width,
            height: self.height,
            profile: ImageLoadProfile {
                format: format!("{} via {}", format, self.format),
                source_width: self.source_width,
                source_height: self.source_height,
                permit_wait: Duration::ZERO,
                decode: Duration::ZERO,
                convert: Duration::ZERO,
                resize: Duration::ZERO,
                expand: Duration::ZERO,
                resize_filter: self.resize_filter.clone(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedSourcePixels {
    Rgb(Arc<[u8]>),
    Rgba(Arc<[u8]>),
    Luma(Arc<[u8]>),
    LumaA(Arc<[u8]>),
}

impl DecodedSourcePixels {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Rgb(bytes) | Self::Rgba(bytes) | Self::Luma(bytes) | Self::LumaA(bytes) => {
                bytes.len()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedSourceImage {
    pub(crate) pixels: DecodedSourcePixels,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) format: String,
    pub(crate) decode: Duration,
    pub(crate) convert: Duration,
}

impl CacheSized for DecodedSourceImage {
    fn cache_size_bytes(&self) -> usize {
        self.pixels.len()
    }
}

#[derive(Debug)]
pub(crate) struct InFlightSharedResult<T> {
    pub(crate) notify: tokio::sync::Notify,
    pub(crate) result: ParkingMutex<Option<Result<Arc<T>, String>>>,
}

impl<T> Default for InFlightSharedResult<T> {
    fn default() -> Self {
        Self {
            notify: tokio::sync::Notify::new(),
            result: ParkingMutex::new(None),
        }
    }
}
