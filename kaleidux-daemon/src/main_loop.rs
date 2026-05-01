//! Shared main loop context and helpers used by both Wayland and X11 backends.
//!
//! This module contains the `MainLoopContext` struct which owns all state shared
//! between backend loops, along with helper methods that deduplicate the
//! channel-drain, scheduling, command-handling, and housekeeping logic.

use crate::background::{self, BackgroundWorkKind};
use crate::cache;
use crate::metrics;
use crate::monitor;
use crate::monitor_manager;
use crate::orchestration;
use crate::queue;
use crate::renderer;
use crate::scripting;
use crate::video;

use gstreamer as gst;
use kaleidux_common::{Request, Response, Transition};
use parking_lot::Mutex as ParkingMutex;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace, warn};
use zune_core::bytestream::ZCursor;
use zune_core::colorspace::ColorSpace;
use zune_core::options::DecoderOptions;

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
const IMAGE_PREFETCH_LOOKAHEAD_IMAGES: usize = 3;
const IMAGE_PREFETCH_MAX_REQUESTS: usize = 6;

async fn read_ipc_request_line(
    stream: &mut tokio::net::UnixStream,
    max_message_size: usize,
) -> Option<String> {
    let mut message = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        match stream.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                let chunk = &chunk[..n];
                let bytes_to_take = chunk.iter().position(|&b| b == b'\n').unwrap_or(n);
                if message.len() + bytes_to_take > max_message_size {
                    warn!(
                        "[IPC] Dropping oversized request (>{} bytes) from control socket",
                        max_message_size
                    );
                    return None;
                }
                message.extend_from_slice(&chunk[..bytes_to_take]);
                if bytes_to_take != n {
                    break;
                }
            }
            Err(e) => {
                warn!("[IPC] Failed reading request from control socket: {}", e);
                return None;
            }
        }
    }

    if message.is_empty() {
        return None;
    }

    match String::from_utf8(message) {
        Ok(message) => Some(message),
        Err(e) => {
            warn!("[IPC] Received non-UTF8 request on control socket: {}", e);
            None
        }
    }
}
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

static PREPARED_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<PreparedImageKey, Arc<InFlightSharedResult<PreparedImageEntry>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

static SOURCE_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<ImageSourceIdentity, Arc<InFlightSharedResult<DecodedSourceImage>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

static IMAGE_PREFETCH_GENERATIONS: once_cell::sync::Lazy<ParkingMutex<HashMap<String, u64>>> =
    once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

type PendingVideoSessions = Arc<Mutex<HashMap<String, u64>>>;

#[derive(Debug, Clone)]
pub struct LoadedImage {
    pub name: String,
    pub session_id: u64,
    pub data: Option<Arc<[u8]>>,
    pub width: u32,
    pub height: u32,
    pub profile: Option<ImageLoadProfile>,
    pub _path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp_png(width: u32, height: u32) -> PathBuf {
        let unique = format!(
            "kaleidux-image-test-{}-{}.png",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        let image = image::RgbaImage::from_pixel(width, height, image::Rgba([10, 20, 30, 255]));
        image.save(&path).expect("temp png should save");
        path
    }

    #[test]
    fn upload_resize_does_not_touch_normal_images() {
        assert_eq!(
            compute_upload_downscale_dimensions(1280, 720, 1920, 1080),
            None
        );
    }

    #[test]
    fn upload_resize_downscales_only_oversized_sources() {
        assert_eq!(
            compute_upload_downscale_dimensions(
                MAX_IMAGE_UPLOAD_DIMENSION * 2,
                4000,
                MAX_IMAGE_UPLOAD_DIMENSION * 2,
                4000,
            ),
            Some((MAX_IMAGE_UPLOAD_DIMENSION, 2000))
        );
    }

    #[test]
    fn cover_target_downscales_to_minimum_cover_size() {
        assert_eq!(
            compute_upload_downscale_dimensions(6000, 4000, 1920, 1080),
            Some((1920, 1280))
        );
        assert_eq!(
            compute_upload_downscale_dimensions(3000, 4500, 1920, 1080),
            Some((1920, 2880))
        );
    }

    #[test]
    fn rgb_prep_expands_to_rgba_without_resize_when_not_needed() {
        let rgb = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 2, 2, 3840, 2160).expect("rgb prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 20, 30, 255, 40, 50, 60, 255, 70, 80, 90, 255, 100, 110, 120, 255,
            ]
        );
    }

    #[test]
    fn rgb_prep_keeps_source_dimensions_even_when_target_is_smaller() {
        let rgb = vec![64; 4 * 3];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 4, 1, 1, 1).expect("rgb prep should succeed");

        assert_eq!((width, height), (4, 1));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(rgba.len(), 4 * 4);
    }

    #[test]
    fn luma_prep_expands_to_rgba_without_resize_when_not_needed() {
        let luma = vec![10, 40, 70, 100];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_luma_image(luma, 2, 2, 3840, 2160).expect("luma prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 10, 10, 255, 40, 40, 40, 255, 70, 70, 70, 255, 100, 100, 100, 255,
            ]
        );
    }

    #[test]
    fn lumaa_prep_preserves_alpha() {
        let lumaa = vec![10, 11, 40, 41, 70, 71, 100, 101];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_lumaa_image(lumaa, 2, 2, 3840, 2160).expect("lumaa prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 10, 10, 11, 40, 40, 40, 41, 70, 70, 70, 71, 100, 100, 100, 101,
            ]
        );
    }

    #[test]
    fn prepared_image_cache_roundtrip_preserves_rgba_payload() {
        let source_path = write_temp_png(1, 1);

        let payload = DecodedImagePayload {
            data: vec![1, 2, 3, 4, 5, 6, 7, 8].into(),
            width: 1,
            height: 2,
            profile: ImageLoadProfile {
                format: "png-fast".to_string(),
                source_width: 100,
                source_height: 200,
                permit_wait: Duration::ZERO,
                decode: Duration::from_millis(1),
                convert: Duration::ZERO,
                resize: Duration::from_millis(2),
                expand: Duration::ZERO,
                resize_filter: Some("bilinear".to_string()),
            },
        };

        store_prepared_image_cache(&source_path, 1920, 1080, &payload);
        let cached = try_load_prepared_image_cache(&source_path, 1920, 1080)
            .expect("prepared cache should load");

        assert_eq!(cached.width, payload.width);
        assert_eq!(cached.height, payload.height);
        assert_eq!(cached.data, payload.data);
        assert_eq!(cached.profile.source_width, payload.profile.source_width);
        assert_eq!(cached.profile.source_height, payload.profile.source_height);
        assert_eq!(cached.profile.format, "prepared-cache");

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1920, 1080) {
            let _ = std::fs::remove_file(cache_path);
        }
        let _ = std::fs::remove_file(source_path);
    }

    #[test]
    fn prepared_image_cache_is_scoped_to_output_target() {
        let source_path = write_temp_png(1, 1);

        let payload = DecodedImagePayload {
            data: vec![9, 8, 7, 6].into(),
            width: 1,
            height: 1,
            profile: ImageLoadProfile {
                format: "png-fast".to_string(),
                source_width: 3840,
                source_height: 2160,
                permit_wait: Duration::ZERO,
                decode: Duration::from_millis(1),
                convert: Duration::ZERO,
                resize: Duration::ZERO,
                expand: Duration::ZERO,
                resize_filter: None,
            },
        };

        store_prepared_image_cache(&source_path, 1920, 1080, &payload);
        assert!(try_load_prepared_image_cache(&source_path, 1366, 768).is_none());

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1920, 1080) {
            let _ = std::fs::remove_file(cache_path);
        }
        let _ = std::fs::remove_file(source_path);
    }

    #[test]
    fn prepared_target_dimensions_do_not_upscale_small_sources() {
        let path = write_temp_png(800, 600);

        assert_eq!(
            prepared_target_dimensions_from_path(&path, 1920, 1080),
            Some((800, 600))
        );
        assert_eq!(
            prepared_target_dimensions_from_path(&path, 2560, 1440),
            Some((800, 600))
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn prepared_image_memory_cache_reuses_small_source_for_larger_outputs() {
        let path = write_temp_png(800, 600);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        let first = runtime
            .block_on(request_prepared_image_payload(
                &path,
                1920,
                1080,
                BackgroundWorkKind::ImageDecode,
            ))
            .expect("first prepared image should load");
        let second = runtime
            .block_on(request_prepared_image_payload(
                &path,
                2560,
                1440,
                BackgroundWorkKind::ImageDecode,
            ))
            .expect("second prepared image should reuse cache");

        assert_eq!((first.width, first.height), (800, 600));
        assert_eq!((second.width, second.height), (800, 600));
        assert!(Arc::ptr_eq(&first.data, &second.data));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn compatible_prepared_image_memory_reuses_larger_variant_for_smaller_target() {
        let path = std::env::temp_dir().join(format!(
            "kaleidux-compatible-prepared-test-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"prepared").expect("temp source should be writable");

        let identity = image_source_identity(&path).expect("temp source should have metadata");
        let key = prepared_image_key_for_identity(identity.clone(), 2560, 1440);
        let payload = DecodedImagePayload {
            data: vec![42; 16].into(),
            width: 2560,
            height: 1440,
            profile: ImageLoadProfile {
                format: "prepared-test".to_string(),
                source_width: 3840,
                source_height: 2160,
                permit_wait: Duration::ZERO,
                decode: Duration::ZERO,
                convert: Duration::ZERO,
                resize: Duration::ZERO,
                expand: Duration::ZERO,
                resize_filter: None,
            },
        };
        store_prepared_image_memory(key, &payload);

        let compatible = try_load_compatible_prepared_image_memory(&identity, 1920, 1080)
            .expect("smaller target should reuse larger prepared payload");
        assert_eq!((compatible.width, compatible.height), (2560, 1440));
        assert_eq!(compatible.data, payload.data);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn compatible_prepared_key_prefers_smallest_larger_variant() {
        let identity = ImageSourceIdentity {
            path: PathBuf::from("/tmp/shared.png"),
            file_len: 10,
            modified_secs: 20,
            modified_nanos: 30,
        };
        let keys = vec![
            PreparedImageKey {
                source: identity.clone(),
                target_width: 2560,
                target_height: 1440,
            },
            PreparedImageKey {
                source: identity.clone(),
                target_width: 1920,
                target_height: 1080,
            },
            PreparedImageKey {
                source: identity.clone(),
                target_width: 3840,
                target_height: 2160,
            },
        ];

        let selected = select_compatible_prepared_key(keys.iter(), &identity, 1600, 900)
            .expect("a larger compatible prepared key should be selected");
        assert_eq!(selected.target_width, 1920);
        assert_eq!(selected.target_height, 1080);
    }

    #[test]
    fn pending_image_switches_sort_larger_shared_targets_first() {
        let mut pending = vec![
            PendingContentSwitch {
                name: "DP-2".to_string(),
                path: PathBuf::from("/tmp/shared.jpg"),
                content_type: queue::ContentType::Image,
                target_area: 1280 * 720,
            },
            PendingContentSwitch {
                name: "HDMI-A-1".to_string(),
                path: PathBuf::from("/tmp/shared.jpg"),
                content_type: queue::ContentType::Image,
                target_area: 1920 * 1080,
            },
            PendingContentSwitch {
                name: "DP-3".to_string(),
                path: PathBuf::from("/tmp/video.mp4"),
                content_type: queue::ContentType::Video,
                target_area: 0,
            },
        ];

        sort_pending_content_switches(&mut pending);

        assert_eq!(pending[0].name, "HDMI-A-1");
        assert_eq!(pending[1].name, "DP-2");
        assert_eq!(pending[2].name, "DP-3");
    }

    #[test]
    fn source_descriptor_reuses_decoded_source_dimensions_without_header_parse() {
        let path = std::env::temp_dir().join(format!(
            "kaleidux-image-descriptor-test-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"not-an-image").expect("temp source should be writable");

        let identity = image_source_identity(&path).expect("temp source should have metadata");
        store_decoded_source_memory(
            identity.clone(),
            Arc::new(DecodedSourceImage {
                pixels: DecodedSourcePixels::Rgb(vec![0; 2 * 2 * 3].into()),
                width: 2,
                height: 2,
                format: "test".to_string(),
                decode: Duration::ZERO,
                convert: Duration::ZERO,
            }),
        );

        let descriptor =
            load_image_source_descriptor(&path).expect("decoded source cache should provide dims");
        assert_eq!(descriptor.identity, identity);
        assert_eq!((descriptor.width, descriptor.height), (2, 2));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn prefetch_plan_sorts_immediate_requests_before_lookahead_and_by_size() {
        let mut requests = vec![
            ImagePrefetchRequest {
                target_output: "DP-1".to_string(),
                path: PathBuf::from("/tmp/shared.png"),
                target_width: 1920,
                target_height: 1080,
                reason: "shared-next",
            },
            ImagePrefetchRequest {
                target_output: "DP-1".to_string(),
                path: PathBuf::from("/tmp/lookahead.png"),
                target_width: 2560,
                target_height: 1440,
                reason: "lookahead",
            },
            ImagePrefetchRequest {
                target_output: "DP-2".to_string(),
                path: PathBuf::from("/tmp/shared.png"),
                target_width: 2560,
                target_height: 1440,
                reason: "next",
            },
        ];
        requests.sort_by(|left, right| {
            let left_priority = match left.reason {
                "next" | "shared-next" => 0u8,
                _ => 1u8,
            };
            let right_priority = match right.reason {
                "next" | "shared-next" => 0u8,
                _ => 1u8,
            };
            let left_area = u64::from(left.target_width) * u64::from(left.target_height);
            let right_area = u64::from(right.target_width) * u64::from(right.target_height);

            left_priority
                .cmp(&right_priority)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| right_area.cmp(&left_area))
                .then_with(|| left.target_output.cmp(&right.target_output))
        });

        assert_eq!(requests[0].reason, "next");
        assert_eq!(
            (requests[0].target_width, requests[0].target_height),
            (2560, 1440)
        );
        assert_eq!(requests[1].reason, "shared-next");
        assert_eq!(requests[2].reason, "lookahead");
    }

    #[test]
    fn pending_video_session_state_replaces_and_clears() {
        let sessions: PendingVideoSessions = Arc::new(Mutex::new(HashMap::new()));

        set_pending_video_session(&sessions, "DP-2", Some(7));
        assert!(pending_video_session_matches(&sessions, "DP-2", 7));
        assert!(!pending_video_session_matches(&sessions, "DP-2", 8));

        set_pending_video_session(&sessions, "DP-2", Some(9));
        assert!(!pending_video_session_matches(&sessions, "DP-2", 7));
        assert!(pending_video_session_matches(&sessions, "DP-2", 9));

        set_pending_video_session(&sessions, "DP-2", None);
        assert!(!pending_video_session_matches(&sessions, "DP-2", 9));
    }

    #[test]
    fn video_frames_are_rejected_for_non_video_outputs() {
        assert!(!should_accept_video_frame(
            queue::ContentType::Image,
            42,
            42
        ));
    }

    #[test]
    fn video_frames_are_rejected_for_stale_sessions() {
        assert!(!should_accept_video_frame(
            queue::ContentType::Video,
            42,
            41
        ));
    }

    #[test]
    fn video_frames_are_accepted_for_active_sessions() {
        assert!(should_accept_video_frame(queue::ContentType::Video, 42, 42));
    }

    #[test]
    fn next_idle_wake_prefers_pending_switch_deadline() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);
        let switch_deadline = now + Duration::from_millis(250);

        assert_eq!(
            next_idle_wake_deadline(now, periodic, Some(switch_deadline)),
            switch_deadline
        );
    }

    #[test]
    fn next_idle_wake_falls_back_to_periodic_when_switch_is_later() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);
        let switch_deadline = now + Duration::from_secs(3);

        assert_eq!(
            next_idle_wake_deadline(now, periodic, Some(switch_deadline)),
            now + periodic
        );
    }

    #[test]
    fn next_idle_wake_uses_periodic_when_no_switch_is_pending() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);

        assert_eq!(next_idle_wake_deadline(now, periodic, None), now + periodic);
    }

    #[test]
    fn startup_barrier_releases_after_bounded_skew() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: Some(now),
            release_reason: None,
            outputs: HashMap::from([
                (
                    String::from("DP-2"),
                    StartupOutputState {
                        phase: StartupOutputPhase::Ready,
                        first_ready_at: Some(now),
                        first_present_at: None,
                        retry_count: 0,
                        can_block: true,
                        failed_paths: HashSet::new(),
                    },
                ),
                (String::from("DP-3"), StartupOutputState::pending()),
            ]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + Duration::from_millis(100)),
            None
        );
        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + STARTUP_BARRIER_SKEW_RELEASE),
            Some("bounded_skew")
        );
    }

    #[test]
    fn startup_barrier_releases_failed_outputs_without_waiting() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: HashMap::from([(
                String::from("DP-2"),
                StartupOutputState {
                    phase: StartupOutputPhase::Failed,
                    first_ready_at: None,
                    first_present_at: None,
                    retry_count: STARTUP_RETRY_LIMIT,
                    can_block: false,
                    failed_paths: HashSet::new(),
                },
            )]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now),
            Some("failed_outputs")
        );
    }

    #[test]
    fn startup_barrier_times_out_after_one_second() {
        let now = Instant::now();
        let barrier = StartupPresentBarrier {
            batch_id: 1,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: HashMap::from([(String::from("DP-2"), StartupOutputState::pending())]),
        };

        assert_eq!(
            startup_barrier_release_candidate(&barrier, now + STARTUP_BARRIER_TIMEOUT),
            Some("timeout")
        );
    }
}

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
    fn cpu_duration(&self) -> Duration {
        self.decode + self.convert + self.resize + self.expand
    }

    fn total_duration(&self) -> Duration {
        self.permit_wait + self.cpu_duration()
    }
}

#[derive(Debug)]
struct DecodedImagePayload {
    data: Arc<[u8]>,
    width: u32,
    height: u32,
    profile: ImageLoadProfile,
}

trait CacheSized {
    fn cache_size_bytes(&self) -> usize;
}

impl<T: CacheSized> CacheSized for Arc<T> {
    fn cache_size_bytes(&self) -> usize {
        self.as_ref().cache_size_bytes()
    }
}

struct SizedLruCache<K, V> {
    lru: lru::LruCache<K, V>,
    total_bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl<K: Hash + Eq, V: CacheSized> SizedLruCache<K, V> {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            lru: lru::LruCache::unbounded(),
            total_bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get_cloned(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.lru.get(key).cloned()
    }

    fn put(&mut self, key: K, value: V) {
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
struct ImageSourceIdentity {
    path: PathBuf,
    file_len: u64,
    modified_secs: u64,
    modified_nanos: u32,
}

#[derive(Debug, Clone)]
struct ImageSourceDescriptor {
    identity: ImageSourceIdentity,
    width: u32,
    height: u32,
}

impl CacheSized for ImageSourceDescriptor {
    fn cache_size_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.identity.path.as_os_str().as_encoded_bytes().len()
    }
}

impl ImageSourceDescriptor {
    fn from_dimensions(identity: ImageSourceIdentity, width: u32, height: u32) -> Self {
        Self {
            identity,
            width,
            height,
        }
    }

    fn from_decoded_source(identity: ImageSourceIdentity, source: &DecodedSourceImage) -> Self {
        Self::from_dimensions(identity, source.width, source.height)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PreparedImageKey {
    source: ImageSourceIdentity,
    target_width: u32,
    target_height: u32,
}

#[derive(Debug, Clone)]
struct PreparedImageEntry {
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
    fn from_payload(payload: &DecodedImagePayload) -> Self {
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

    fn to_payload(&self, format: &str) -> DecodedImagePayload {
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
struct PendingContentSwitch {
    name: String,
    path: PathBuf,
    content_type: queue::ContentType,
    target_area: u64,
}

#[derive(Debug, Clone)]
enum DecodedSourcePixels {
    Rgb(Arc<[u8]>),
    Rgba(Arc<[u8]>),
    Luma(Arc<[u8]>),
    LumaA(Arc<[u8]>),
}

impl DecodedSourcePixels {
    fn len(&self) -> usize {
        match self {
            Self::Rgb(bytes) | Self::Rgba(bytes) | Self::Luma(bytes) | Self::LumaA(bytes) => {
                bytes.len()
            }
        }
    }
}

#[derive(Debug, Clone)]
struct DecodedSourceImage {
    pixels: DecodedSourcePixels,
    width: u32,
    height: u32,
    format: String,
    decode: Duration,
    convert: Duration,
}

impl CacheSized for DecodedSourceImage {
    fn cache_size_bytes(&self) -> usize {
        self.pixels.len()
    }
}

#[derive(Debug)]
struct InFlightSharedResult<T> {
    notify: tokio::sync::Notify,
    result: ParkingMutex<Option<Result<Arc<T>, String>>>,
}

impl<T> Default for InFlightSharedResult<T> {
    fn default() -> Self {
        Self {
            notify: tokio::sync::Notify::new(),
            result: ParkingMutex::new(None),
        }
    }
}

#[derive(Debug, Clone)]
struct ImagePrefetchRequest {
    target_output: String,
    path: PathBuf,
    target_width: u32,
    target_height: u32,
    reason: &'static str,
}

pub enum VideoPlayerResult {
    Success(String, u64, video::VideoPlayer, Option<video::VideoFrame>),
    Failure(String, u64),
}

#[derive(Debug, Clone)]
pub struct PendingVideoSwitch {
    pub session_id: u64,
    pub batch_id: Option<u64>,
    pub batch_trigger_time: Option<Instant>,
    pub transition: Transition,
}

pub(crate) struct PendingDirectTakeover {
    session_id: u64,
    player: video::VideoPlayer,
    armed_at: Instant,
    last_rendered: u64,
}

const DIRECT_HANDOFF_MIN_RENDERED_FRAMES: u64 = 4;
const DIRECT_HANDOFF_TIMEOUT: Duration = Duration::from_millis(1500);
const DIRECT_HANDOFF_COOLDOWN_BASE: Duration = Duration::from_secs(10);
const DIRECT_HANDOFF_COOLDOWN_MAX: Duration = Duration::from_secs(90);
const STARTUP_BARRIER_SKEW_RELEASE: Duration = Duration::from_millis(150);
const STARTUP_BARRIER_TIMEOUT: Duration = Duration::from_millis(1000);
const STARTUP_RETRY_LIMIT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupOutputPhase {
    Pending,
    Ready,
    Failed,
    Presented,
}

#[derive(Debug, Clone)]
pub struct StartupOutputState {
    pub phase: StartupOutputPhase,
    pub first_ready_at: Option<Instant>,
    pub first_present_at: Option<Instant>,
    pub retry_count: u8,
    pub can_block: bool,
    pub failed_paths: HashSet<PathBuf>,
}

impl StartupOutputState {
    fn pending() -> Self {
        Self {
            phase: StartupOutputPhase::Pending,
            first_ready_at: None,
            first_present_at: None,
            retry_count: 0,
            can_block: true,
            failed_paths: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupPresentBarrier {
    pub batch_id: u64,
    pub armed_at: Instant,
    pub first_ready_at: Option<Instant>,
    pub release_reason: Option<&'static str>,
    pub outputs: HashMap<String, StartupOutputState>,
}

pub(crate) fn stop_video_player_in_background(name: String, mut player: video::VideoPlayer) {
    let _ = player.request_stop();
    if let Some(handle) =
        background::spawn_blocking_tracked(BackgroundWorkKind::PlayerStop, move || {
            debug!("[VIDEO] {}: Finalizing player stop on blocking pool", name);
            let _ = player.stop();
        })
    {
        drop(handle);
    }
}

pub(crate) fn should_accept_video_frame(
    valid_content_type: queue::ContentType,
    active_video_session_id: u64,
    frame_session_id: u64,
) -> bool {
    valid_content_type == queue::ContentType::Video
        && active_video_session_id != 0
        && active_video_session_id == frame_session_id
}

fn next_idle_wake_deadline(
    now: Instant,
    periodic_interval: Duration,
    switch_deadline: Option<Instant>,
) -> Instant {
    let periodic_deadline = now + periodic_interval;
    switch_deadline
        .map(|deadline| deadline.min(periodic_deadline))
        .unwrap_or(periodic_deadline)
}

fn select_compatible_prepared_key<'a, I>(
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

fn min_optional_deadline(current: Option<Instant>, candidate: Option<Instant>) -> Option<Instant> {
    match (current, candidate) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn startup_barrier_counts(barrier: &StartupPresentBarrier) -> (usize, usize, usize) {
    let mut pending = 0usize;
    let mut ready = 0usize;
    let mut failed = 0usize;

    for state in barrier.outputs.values() {
        match state.phase {
            StartupOutputPhase::Pending if state.can_block => pending += 1,
            StartupOutputPhase::Ready | StartupOutputPhase::Presented if state.can_block => {
                ready += 1;
            }
            StartupOutputPhase::Failed => failed += 1,
            _ => {}
        }
    }

    (pending, ready, failed)
}

fn startup_barrier_release_candidate(
    barrier: &StartupPresentBarrier,
    now: Instant,
) -> Option<&'static str> {
    if let Some(reason) = barrier.release_reason {
        return Some(reason);
    }

    let (pending, _ready, failed) = startup_barrier_counts(barrier);
    if pending == 0 {
        return Some(if failed > 0 {
            "failed_outputs"
        } else {
            "all_ready"
        });
    }
    if let Some(first_ready_at) = barrier.first_ready_at
        && now >= first_ready_at + STARTUP_BARRIER_SKEW_RELEASE
    {
        return Some("bounded_skew");
    }
    if now >= barrier.armed_at + STARTUP_BARRIER_TIMEOUT {
        return Some("timeout");
    }

    None
}

fn startup_barrier_next_deadline(barrier: &StartupPresentBarrier, now: Instant) -> Option<Instant> {
    if startup_barrier_release_candidate(barrier, now).is_some() {
        return Some(now);
    }

    let mut deadline = Some(barrier.armed_at + STARTUP_BARRIER_TIMEOUT);
    if let Some(first_ready_at) = barrier.first_ready_at {
        deadline = min_optional_deadline(
            deadline,
            Some(first_ready_at + STARTUP_BARRIER_SKEW_RELEASE),
        );
    }
    deadline
}

fn startup_barrier_is_terminal(barrier: &StartupPresentBarrier) -> bool {
    barrier.outputs.values().all(|state| match state.phase {
        StartupOutputPhase::Presented => true,
        StartupOutputPhase::Failed => !state.can_block && state.retry_count >= STARTUP_RETRY_LIMIT,
        StartupOutputPhase::Pending | StartupOutputPhase::Ready => false,
    })
}

/// Type aliases to reduce verbosity in signatures
pub type CmdMsg = (Request, tokio::sync::oneshot::Sender<Response>);
pub type PlayerEventMsg = video::PlayerEvent;

/// Shared state for both Wayland and X11 main loops.
pub struct MainLoopContext {
    pub metrics: Arc<metrics::PerformanceMetrics>,
    pub monitor_manager: monitor_manager::MonitorManager,
    pub renderers: HashMap<String, renderer::Renderer>,
    pub video_players: HashMap<String, video::VideoPlayer>,
    pub pending_video_switches: HashMap<String, PendingVideoSwitch>,
    pub pending_image_video_stops: HashMap<String, video::VideoPlayer>,
    pending_direct_takeovers: HashMap<String, PendingDirectTakeover>,
    pub pending_direct_handoffs: HashMap<String, u64>,
    blocked_direct_handoff_sessions: HashMap<String, u64>,
    direct_handoff_cooldown_until: HashMap<String, Instant>,
    direct_handoff_failure_streak: HashMap<String, u32>,
    pub pending_video_sessions: PendingVideoSessions,
    pub wgpu_ctx: Option<Arc<renderer::WgpuContext>>,
    pub startup_present_barrier: Option<StartupPresentBarrier>,
    pub latest_video_frames: video::LatestFrameMailbox,
    pub native_video_targets: HashMap<String, video::NativeWaylandVideoTarget>,

    pub cmd_rx: tokio::sync::mpsc::UnboundedReceiver<CmdMsg>,
    pub image_rx: tokio::sync::mpsc::Receiver<LoadedImage>,
    pub image_tx: tokio::sync::mpsc::Sender<LoadedImage>,
    pub player_rx: tokio::sync::mpsc::UnboundedReceiver<VideoPlayerResult>,
    pub player_tx: tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    pub player_event_rx: tokio::sync::mpsc::UnboundedReceiver<PlayerEventMsg>,
    pub player_event_tx: tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,

    pub dir_watcher: Option<cache::DirectoryWatcher>,
    pub script_manager: scripting::ScriptManager,
    pub shutdown_flag: Arc<AtomicBool>,

    pub next_session_id: u64,
    pub first_frame_recorded: bool,
    pub last_metrics_log: Instant,
    pub last_stats_flush: Instant,
    pub last_pool_cleanup: Instant,
    pub last_dir_watch_poll: Instant,
    pub last_device_poll: Instant,
    pub last_script_tick: Instant,
    pub script_tick_interval: u64,
    pub target_frame_time: std::time::Duration,
}

impl MainLoopContext {
    /// Create a new `MainLoopContext` with all shared state initialized.
    /// This is the common pre-loop setup for both Wayland and X11.
    pub async fn new(
        config: orchestration::Config,
        log_level: Option<u8>,
        gstreamer_duration: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let script_path = config.global.script_path.clone();
        let script_tick_interval = config.global.script_tick_interval;
        let metrics = Arc::new(metrics::PerformanceMetrics::new());
        metrics.record_startup_start();
        metrics.record_gstreamer_init(gstreamer_duration);

        // Start resource monitor with metrics
        let sys_monitor = monitor::SystemMonitor::new_with_metrics(Some(metrics.clone()));
        tokio::spawn(async move {
            sys_monitor.run().await;
        });

        let monitor_manager = monitor_manager::MonitorManager::new_with_metrics(
            config.clone(),
            Some(metrics.clone()),
        )?;

        // Initialize directory watcher for cache invalidation
        let cache = monitor_manager.get_cache();
        let dir_watcher = match cache::DirectoryWatcher::new(cache.clone()) {
            Ok(mut watcher) => {
                for output_config in config.outputs.values() {
                    if let Some(path) = &output_config.path {
                        if let Err(e) = watcher.watch(path) {
                            warn!(
                                "[CACHE] Failed to watch directory {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
                Some(watcher)
            }
            Err(e) => {
                warn!("[CACHE] Failed to create directory watcher: {}", e);
                None
            }
        };

        // Log metrics immediately for DEBUG (3) and TRACE (4) levels
        if log_level.map(|l| l >= 3).unwrap_or(false) {
            metrics.log_summary();
        }

        // Create channels
        let latest_video_frames = video::LatestFrameMailbox::new();
        // Image channel: bounded to prevent memory spikes from large images accumulating
        let (image_tx, image_rx) = tokio::sync::mpsc::channel::<LoadedImage>(16);
        let (player_tx, player_rx) = tokio::sync::mpsc::unbounded_channel::<VideoPlayerResult>();
        let (player_event_tx, player_event_rx) =
            tokio::sync::mpsc::unbounded_channel::<PlayerEventMsg>();
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<CmdMsg>();

        // IPC Socket Setup
        info!("[STARTUP] Setting up IPC socket");
        let socket_path = dirs::runtime_dir()
            .map(|d| d.join("kaleidux.sock"))
            .unwrap_or_else(|| {
                let uid = std::env::var("USER").unwrap_or_else(|_| "kaleidux".to_string());
                std::path::PathBuf::from(format!("/tmp/kaleidux-{}.sock", uid))
            });

        info!("[STARTUP] IPC socket path: {:?}", socket_path);
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)?;
        info!("[STARTUP] IPC socket bound successfully");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = std::fs::metadata(&socket_path) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o600);
                let _ = std::fs::set_permissions(&socket_path, perms);
            }
        }

        // Spawn IPC Listener
        let cmd_tx_clone = cmd_tx.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let cmd_tx = cmd_tx_clone.clone();
                    tokio::spawn(async move {
                        const MAX_MESSAGE_SIZE: usize = 8192;
                        let Some(req_str) =
                            read_ipc_request_line(&mut stream, MAX_MESSAGE_SIZE).await
                        else {
                            return;
                        };
                        let Ok(req) = serde_json::from_str::<Request>(req_str.trim()) else {
                            warn!("[IPC] Failed to parse control request JSON");
                            return;
                        };
                        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                        if cmd_tx.send((req, resp_tx)).is_ok()
                            && let Ok(response) = resp_rx.await
                            && let Ok(json) = serde_json::to_string(&response)
                        {
                            let _ = stream.write_all(json.as_bytes()).await;
                        }
                    });
                }
            }
        });

        // Shutdown signal handler
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown_flag.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            warn!("Received shutdown signal, cleaning up...");
            shutdown_clone.store(true, Ordering::SeqCst);
        });

        // Script manager
        info!("[STARTUP] Creating script manager");
        let script_cmd_tx = cmd_tx.clone();
        let mut script_manager = scripting::ScriptManager::new(script_cmd_tx);
        if let Some(path) = &script_path {
            info!("[STARTUP] Loading script from: {:?}", path);
            let _ = script_manager.load(path).await;
        }
        info!("[STARTUP] Script manager initialized");

        let now = Instant::now();

        Ok(MainLoopContext {
            metrics,
            monitor_manager,
            renderers: HashMap::new(),
            video_players: HashMap::new(),
            pending_video_switches: HashMap::new(),
            pending_image_video_stops: HashMap::new(),
            pending_direct_takeovers: HashMap::new(),
            pending_direct_handoffs: HashMap::new(),
            blocked_direct_handoff_sessions: HashMap::new(),
            direct_handoff_cooldown_until: HashMap::new(),
            direct_handoff_failure_streak: HashMap::new(),
            pending_video_sessions: Arc::new(Mutex::new(HashMap::new())),
            wgpu_ctx: None,
            startup_present_barrier: None,
            latest_video_frames,
            native_video_targets: HashMap::new(),
            cmd_rx,
            image_rx,
            image_tx,
            player_rx,
            player_tx,
            player_event_rx,
            player_event_tx,
            dir_watcher,
            script_manager,
            shutdown_flag,
            next_session_id: 1,
            first_frame_recorded: false,
            last_metrics_log: now,
            last_stats_flush: now,
            last_pool_cleanup: now,
            last_dir_watch_poll: now,
            last_device_poll: now,
            last_script_tick: now,
            script_tick_interval,
            target_frame_time: std::time::Duration::from_micros(16667), // ~60 FPS
        })
    }

    // ─── Idle wait ──────────────────────────────────────────────────────

    /// Returns true if any renderer is actively transitioning or has pending redraw work.
    pub fn any_active(&self) -> bool {
        self.renderers
            .values()
            .any(|r| r.transition_active || r.needs_redraw)
    }

    pub fn wayland_hot_loop_active(&self) -> bool {
        self.renderers
            .values()
            .any(renderer::Renderer::needs_wayland_immediate_work)
    }

    pub fn next_common_idle_deadline(&self, now: Instant) -> Option<Instant> {
        let mut deadline = self.monitor_manager.next_switch_deadline();
        if self.script_manager.is_loaded() {
            let script_deadline =
                Some(self.last_script_tick + Duration::from_secs(self.script_tick_interval));
            deadline = min_optional_deadline(deadline, script_deadline);
        }
        deadline = min_optional_deadline(
            deadline,
            self.startup_present_barrier
                .as_ref()
                .and_then(|barrier| startup_barrier_next_deadline(barrier, now)),
        );
        deadline
    }

    pub fn next_wayland_idle_deadline(&self, now: Instant) -> Option<Instant> {
        let mut deadline = self.next_common_idle_deadline(now);
        for renderer in self.renderers.values() {
            deadline = min_optional_deadline(
                deadline,
                renderer.next_wayland_retry_deadline(Duration::from_millis(500)),
            );
            deadline = min_optional_deadline(
                deadline,
                renderer.next_direct_video_heartbeat_deadline(
                    renderer::DIRECT_VIDEO_PARENT_HEARTBEAT_INTERVAL,
                ),
            );
        }
        deadline
    }

    /// Idle-wait using `tokio::select!` until any event source fires.
    /// Returns buffered messages from whichever branch triggered.
    pub async fn idle_wait(
        &mut self,
        fd: &AsyncFd<RawFd>,
        wake_deadline: Option<Instant>,
    ) -> (
        Option<CmdMsg>,
        bool,
        bool,
        Option<LoadedImage>,
        Option<VideoPlayerResult>,
        Option<PlayerEventMsg>,
    ) {
        let mut cmd_buf = None;
        let mut frame_ready = false;
        let mut fd_ready = false;
        let mut image_buf = None;
        let mut player_buf = None;
        let mut player_event_buf = None;

        let now = Instant::now();
        if self.latest_video_frames.has_signal_pending() {
            return (
                cmd_buf,
                true,
                fd_ready,
                image_buf,
                player_buf,
                player_event_buf,
            );
        }
        let wake_deadline = wake_deadline.map(|deadline| {
            if deadline <= now {
                self.metrics.record_wayland_expired_deadline_wake();
                now + std::time::Duration::from_millis(1)
            } else {
                deadline
            }
        });
        let wake_deadline =
            next_idle_wake_deadline(now, std::time::Duration::from_secs(1), wake_deadline);
        let wake_deadline = tokio::time::Instant::from_std(wake_deadline);

        tokio::select! {
            cmd = self.cmd_rx.recv() => { if let Some(c) = cmd { cmd_buf = Some(c); } }
            _ = self.latest_video_frames.notified() => { frame_ready = true; }
            image = self.image_rx.recv() => { if let Some(i) = image { image_buf = Some(i); } }
            player = self.player_rx.recv() => { if let Some(p) = player { player_buf = Some(p); } }
            player_event = self.player_event_rx.recv() => {
                if let Some(event) = player_event {
                    player_event_buf = Some(event);
                }
            }
            result = fd.readable() => {
                if let Ok(mut guard) = result {
                    guard.clear_ready();
                    fd_ready = true;
                }
            }
            _ = tokio::time::sleep_until(wake_deadline) => {}
        }

        (
            cmd_buf,
            frame_ready,
            fd_ready,
            image_buf,
            player_buf,
            player_event_buf,
        )
    }

    // ─── Channel draining ───────────────────────────────────────────────

    /// Process scheduled changes from MonitorManager::tick().
    pub fn process_scheduled(&mut self, loop_start: Instant) {
        if !self.monitor_manager.tick_due(loop_start) {
            return;
        }

        let scheduled_changes = self.monitor_manager.tick();
        if !scheduled_changes.is_empty() {
            let batch_id = rand::random::<u64>();
            let ordered_changes =
                ordered_pending_content_switches(&self.renderers, scheduled_changes);
            for change in ordered_changes {
                switch_wallpaper_content(
                    &change.name,
                    &change.path,
                    change.content_type,
                    &self.metrics,
                    &mut self.next_session_id,
                    &self.latest_video_frames,
                    &self.monitor_manager,
                    &mut self.renderers,
                    &mut self.video_players,
                    &mut self.pending_video_switches,
                    &mut self.pending_image_video_stops,
                    &mut self.pending_direct_takeovers,
                    &mut self.pending_direct_handoffs,
                    &mut self.blocked_direct_handoff_sessions,
                    &self.pending_video_sessions,
                    Some(batch_id),
                    Some(loop_start),
                    &self.image_tx,
                    &self.player_tx,
                    &self.player_event_tx,
                    self.native_video_targets.get(&change.name).cloned(),
                    &self.shutdown_flag,
                    "SCHEDULED",
                );
            }
        }
    }

    /// Process script tick.
    pub fn process_script_tick(&mut self) {
        if !self.script_manager.is_loaded() {
            return;
        }
        if self.last_script_tick.elapsed().as_secs() >= self.script_tick_interval {
            self.script_manager.tick();
            self.last_script_tick = Instant::now();
        }
    }

    /// Drain and handle all pending commands.
    pub async fn drain_commands(&mut self, cmd_buf: Option<CmdMsg>, loop_start: Instant) {
        let cmd_iter = std::iter::once(cmd_buf)
            .flatten()
            .chain(std::iter::from_fn(|| self.cmd_rx.try_recv().ok()));
        for (req, resp) in cmd_iter {
            let response = handle_command(
                req,
                &mut self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
                &mut self.pending_video_switches,
                &mut self.pending_image_video_stops,
                &mut self.pending_direct_takeovers,
                &mut self.pending_direct_handoffs,
                &mut self.blocked_direct_handoff_sessions,
                &mut self.direct_handoff_cooldown_until,
                &mut self.direct_handoff_failure_streak,
                &self.pending_video_sessions,
                &self.metrics,
                &self.latest_video_frames,
                &self.image_tx,
                &self.player_tx,
                &self.player_event_tx,
                &self.native_video_targets,
                &mut self.next_session_id,
                loop_start,
                &self.shutdown_flag,
            )
            .await;
            let _ = resp.send(response);
        }
    }

    /// Drain video frames from channel. Returns latest frame per source, plus stats.
    pub fn drain_frames(
        &mut self,
        should_check_mailbox: bool,
        hold_video_until_callback: bool,
    ) -> (HashMap<String, video::VideoFrame>, usize, usize) {
        let mut latest_frames: HashMap<String, video::VideoFrame> = HashMap::new();
        let mut frames_received = 0;
        let mut frames_discarded = 0;
        let mut stale_session_discards = 0;
        let superseded_source_discards = self.latest_video_frames.take_overwrite_count() as usize;

        if should_check_mailbox {
            self.latest_video_frames.clear_signal_pending();
            for source_id in self.latest_video_frames.pending_sources() {
                let frame_state = self.latest_video_frames.inspect_frame(&source_id, |frame| {
                    let renderer = self.renderers.get(source_id.as_str());
                    let should_accept = renderer.is_some_and(|r| {
                        should_accept_video_frame(
                            r.valid_content_type,
                            r.active_video_session_id,
                            frame.session_id,
                        )
                    });
                    let hold_for_callback = should_accept
                        && hold_video_until_callback
                        && renderer
                            .is_some_and(renderer::Renderer::should_hold_video_frame_for_callback);

                    (should_accept, hold_for_callback)
                });

                let Some((should_accept, hold_for_callback)) = frame_state else {
                    continue;
                };

                if hold_for_callback {
                    continue;
                }

                let Some(frame) = self.latest_video_frames.take_frame(&source_id) else {
                    continue;
                };
                frames_received += 1;
                self.metrics.record_video_frame_received();
                if !should_accept {
                    frames_discarded += 1;
                    stale_session_discards += 1;
                    self.metrics.record_video_frame_stale_skipped();
                } else {
                    latest_frames.insert(source_id, frame);
                }
            }
        }

        // Track frame channel usage for memory leak detection
        if frames_received > 0 || superseded_source_discards > 0 {
            self.metrics
                .record_frame_channel_size(frames_received + self.latest_video_frames.occupancy());
            if frames_discarded > 0 || superseded_source_discards > 0 {
                trace!(
                    "[VIDEO] Discarded {} frames (stale_session={}, superseded_by_newer_same_source={})",
                    frames_discarded + superseded_source_discards,
                    stale_session_discards,
                    superseded_source_discards
                );
            }
        }

        (latest_frames, frames_received, frames_discarded)
    }

    /// Drain images from channel, upload, and optionally render.
    ///
    /// The `render_fn` closure is called for each image that needs rendering.
    /// It receives (renderer, &name, loop_start) and should perform the
    /// backend-specific render call.
    pub fn drain_images<F>(
        &mut self,
        image_buf: Option<LoadedImage>,
        loop_start: Instant,
        mut render_fn: F,
    ) where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let mut images_received = 0;
        let mut pending_images = Vec::new();
        if let Some(msg) = image_buf {
            pending_images.push(msg);
        }
        while let Ok(msg) = self.image_rx.try_recv() {
            pending_images.push(msg);
        }

        for msg in pending_images {
            images_received += 1;
            let barrier_blocks = self.startup_barrier_blocks_output(&msg.name, loop_start);
            let mut release_pending_video = false;
            let mut startup_ready = false;
            let mut startup_failure_reason: Option<String> = None;
            debug!(
                "[IMAGE] Received image for {}: session={}, data={}, size={}x{}",
                msg.name,
                msg.session_id,
                msg.data.is_some(),
                msg.width,
                msg.height
            );
            if let Some(r) = self.renderers.get_mut(&msg.name) {
                if r.valid_content_type != crate::queue::ContentType::Image
                    || r.active_image_session_id != msg.session_id
                {
                    if let Some(profile) = &msg.profile {
                        debug!(
                            "[IMAGE] {}: stale image was prepared via {} in {:.1}ms (wait {:.1}ms, cpu {:.1}ms)",
                            msg.name,
                            profile.format,
                            duration_ms(profile.total_duration()),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.cpu_duration())
                        );
                    }
                    debug!(
                        "[IMAGE] Dropping stale image for {}: session={} active_session={} content_type={:?}",
                        msg.name, msg.session_id, r.active_image_session_id, r.valid_content_type
                    );
                    continue;
                }

                if let Some(data) = msg.data {
                    debug!(
                        "[IMAGE] Uploading image data for {}: {} bytes",
                        msg.name,
                        data.len()
                    );
                    let upload_start = Instant::now();
                    let _ = r.upload_image_data(data.as_ref(), msg.width, msg.height);
                    let upload_duration = upload_start.elapsed();
                    if let Some(profile) = &msg.profile {
                        self.metrics.record_image_stage_timings(
                            profile.permit_wait,
                            profile.decode,
                            profile.convert,
                            profile.resize,
                            profile.expand,
                            upload_duration,
                        );
                        debug!(
                            "[IMAGE] {}: prepared {} {}x{} -> {}x{} in {:.1}ms (wait {:.1}ms, decode {:.1}ms, convert {:.1}ms, resize {:.1}ms, expand {:.1}ms, upload {:.1}ms, filter={})",
                            msg.name,
                            profile.format,
                            profile.source_width,
                            profile.source_height,
                            msg.width,
                            msg.height,
                            duration_ms(profile.total_duration() + upload_duration),
                            duration_ms(profile.permit_wait),
                            duration_ms(profile.decode),
                            duration_ms(profile.convert),
                            duration_ms(profile.resize),
                            duration_ms(profile.expand),
                            duration_ms(upload_duration),
                            profile.resize_filter.as_deref().unwrap_or("none")
                        );
                    }
                    debug!(
                        "[IMAGE] Upload complete for {}: {:.1}ms",
                        msg.name,
                        duration_ms(upload_duration)
                    );
                    startup_ready = true;
                    if barrier_blocks {
                        debug!(
                            "[STARTUP] {}: First image ready, holding present for barrier release",
                            msg.name
                        );
                    } else {
                        debug!("[IMAGE] Rendering after upload for {}", msg.name);
                        render_fn(r, &msg.name, loop_start);
                        if !self.first_frame_recorded {
                            self.metrics.record_first_frame();
                            self.first_frame_recorded = true;
                        }
                        self.mark_output_presented_if_ready(&msg.name);
                    }
                    release_pending_video = true;
                } else {
                    r.abort_transition();
                    startup_failure_reason = Some("image_decode_failed".to_string());
                    release_pending_video = true;
                }
            } else {
                warn!(
                    "[IMAGE] {}: Renderer not found, dropping image data to prevent memory leak",
                    msg.name
                );
            }
            if startup_ready {
                self.mark_startup_output_ready(&msg.name, loop_start);
            }
            if let Some(reason) = startup_failure_reason {
                self.handle_startup_content_failure(&msg.name, &reason, loop_start);
            }
            if release_pending_video {
                self.release_pending_image_video_stop(&msg.name);
            }
        }
        if images_received > 0 {
            self.metrics.record_image_channel_size(images_received);
        }
    }

    fn release_pending_image_video_stop(&mut self, name: &str) {
        if let Some(player) = self.pending_image_video_stops.remove(name) {
            stop_video_player_in_background(name.to_string(), player);
        }
    }

    pub fn mark_output_presented_if_ready(&mut self, name: &str) {
        let should_mark = self.renderers.get_mut(name).is_some_and(|renderer| {
            let ready = renderer.take_display_timer_ready();
            if ready {
                renderer.transition_just_completed = false;
            }
            ready
        });
        if should_mark {
            self.monitor_manager.mark_transition_completed(name);
            self.mark_startup_output_presented(name, Instant::now());
            self.maybe_clear_startup_present_barrier();
            self.maybe_schedule_direct_wayland_handoff(name);
        }
    }

    fn clear_pending_direct_handoff(&mut self, name: &str, session_id: u64) -> bool {
        if self.pending_direct_handoffs.get(name).copied() == Some(session_id) {
            self.pending_direct_handoffs.remove(name);
            return true;
        }

        false
    }

    fn direct_handoff_cooldown_active(&self, name: &str, now: Instant) -> Option<Duration> {
        let until = self.direct_handoff_cooldown_until.get(name).copied()?;
        if until <= now {
            return None;
        }
        Some(until.saturating_duration_since(now))
    }

    fn arm_direct_handoff_cooldown(&mut self, name: &str, session_id: u64, reason: &str) {
        let next_streak = self
            .direct_handoff_failure_streak
            .get(name)
            .copied()
            .unwrap_or(0)
            .saturating_add(1)
            .min(6);
        self.direct_handoff_failure_streak
            .insert(name.to_string(), next_streak);
        let scale = 1u32 << next_streak.saturating_sub(1);
        let cooldown = DIRECT_HANDOFF_COOLDOWN_BASE
            .saturating_mul(scale)
            .min(DIRECT_HANDOFF_COOLDOWN_MAX);
        let until = Instant::now() + cooldown;
        self.direct_handoff_cooldown_until
            .insert(name.to_string(), until);
        info!(
            "[VIDEO] {}: Direct handoff cooldown armed for {:.1}s after {} (session={}, streak={})",
            name,
            cooldown.as_secs_f64(),
            reason,
            session_id,
            next_streak
        );
    }

    fn clear_direct_handoff_backoff(&mut self, name: &str) {
        self.direct_handoff_failure_streak.remove(name);
        self.direct_handoff_cooldown_until.remove(name);
    }

    pub(crate) fn maybe_schedule_direct_wayland_handoff(&mut self, name: &str) {
        if !video::native_wayland_backend_enabled()
            || self.pending_video_switches.contains_key(name)
            || self.pending_direct_takeovers.contains_key(name)
        {
            return;
        }
        let now = Instant::now();
        if let Some(remaining) = self.direct_handoff_cooldown_active(name, now) {
            self.metrics.record_direct_handoff_cooldown_skip();
            debug!(
                "[VIDEO] {}: Skipping direct handoff while cooldown is active ({:.1}s remaining)",
                name,
                remaining.as_secs_f64()
            );
            return;
        }

        let Some(renderer) = self.renderers.get(name) else {
            return;
        };
        if renderer.valid_content_type != crate::queue::ContentType::Video
            || !renderer.has_any_content()
        {
            return;
        }

        let session_id = renderer.active_video_session_id;
        if session_id == 0
            || self.pending_direct_handoffs.get(name).copied() == Some(session_id)
            || self.blocked_direct_handoff_sessions.get(name).copied() == Some(session_id)
        {
            return;
        }

        let direct_decision = renderer.direct_wayland_presentation_decision();
        if direct_decision != renderer::DirectPresentationDecision::Compatible {
            info!(
                "[VIDEO] {}: Keeping appsink renderer path for session {} ({})",
                name,
                session_id,
                direct_decision.fallback_reason()
            );
            self.block_direct_handoff_session(name, session_id, direct_decision.fallback_reason());
            return;
        }

        let Some(native_target) = self.native_video_targets.get(name).cloned() else {
            return;
        };

        let Some(player) = self.video_players.get(name) else {
            return;
        };
        if !player.is_appsink_backend() || player.session_id() != session_id {
            return;
        }

        let Some(path) = self
            .monitor_manager
            .outputs
            .get(name)
            .and_then(|output| output.current_path.clone())
        else {
            return;
        };

        let volume = self
            .monitor_manager
            .outputs
            .get(name)
            .map(|output| output.config.volume as f64 / 100.0)
            .unwrap_or(0.0);
        let start_position_ns = player.current_position_ns();

        info!(
            "[VIDEO] {}: Scheduling same-session handoff from appsink renderer path to direct Wayland sink (session={} position_ms={:.1})",
            name,
            session_id,
            start_position_ns.unwrap_or(0) as f64 / 1_000_000.0
        );
        self.metrics.record_direct_handoff_attempt();

        self.pending_direct_handoffs
            .insert(name.to_string(), session_id);
        set_pending_video_session(&self.pending_video_sessions, name, Some(session_id));
        create_and_start_video_player(
            &path,
            name,
            session_id,
            volume,
            &self.latest_video_frames,
            &self.player_tx,
            &self.player_event_tx,
            Some(native_target),
            self.pending_video_sessions.clone(),
            self.shutdown_flag.clone(),
            video::VideoBackendRequest::ForceWaylandDirect,
            start_position_ns,
        );
    }

    /// Drain player results from channel, activate deferred video switches, and
    /// render immediately when a preroll frame is available.
    fn abandon_pending_direct_takeover(&mut self, name: &str, reason: &str) -> bool {
        let Some(mut pending) = self.pending_direct_takeovers.remove(name) else {
            return false;
        };

        self.pending_direct_handoffs.remove(name);
        if let Err(e) = pending.player.stop() {
            debug!(
                "[VIDEO] {}: Failed stopping pending direct takeover player after {}: {}",
                name, reason, e
            );
        }
        true
    }

    fn block_direct_handoff_session(&mut self, name: &str, session_id: u64, reason: &str) {
        if session_id == 0 {
            return;
        }

        self.pending_direct_handoffs.remove(name);
        let already_blocked =
            self.blocked_direct_handoff_sessions.get(name).copied() == Some(session_id);
        self.blocked_direct_handoff_sessions
            .insert(name.to_string(), session_id);
        self.metrics.record_direct_fallback_reason(reason);
        if !already_blocked {
            info!(
                "[VIDEO] {}: Direct Wayland handoff disabled for session {} ({})",
                name, session_id, reason
            );
        }
    }

    fn promote_pending_direct_takeover(&mut self, name: &str, rendered: u64, average_rate: f64) {
        let Some(pending) = self.pending_direct_takeovers.remove(name) else {
            return;
        };

        self.pending_direct_handoffs.remove(name);
        let old_player = self.video_players.remove(name);
        self.video_players.insert(name.to_string(), pending.player);
        if let Some(old) = old_player {
            stop_video_player_in_background(name.to_string(), old);
        }
        if let Some(renderer) = self.renderers.get_mut(name) {
            renderer.set_direct_video_presentation_active(true);
        }
        self.clear_direct_handoff_backoff(name);
        self.metrics.record_direct_handoff_promoted();
        info!(
            "[VIDEO] {}: Direct Wayland handoff promoted after {:.1}ms (rendered={} avg_rate={:.1})",
            name,
            pending.armed_at.elapsed().as_secs_f64() * 1000.0,
            rendered,
            average_rate
        );
    }

    pub fn service_pending_direct_takeovers(&mut self) {
        let names: Vec<String> = self.pending_direct_takeovers.keys().cloned().collect();
        for name in names {
            let Some(renderer_session_id) = self
                .renderers
                .get(&name)
                .map(|renderer| renderer.active_video_session_id)
            else {
                self.abandon_pending_direct_takeover(&name, "renderer_missing");
                continue;
            };

            let Some(active_player) = self.video_players.get(&name) else {
                self.abandon_pending_direct_takeover(&name, "active_player_missing");
                continue;
            };

            let Some(pending) = self.pending_direct_takeovers.get_mut(&name) else {
                continue;
            };

            if pending.session_id != renderer_session_id
                || pending.session_id != active_player.session_id()
                || !active_player.is_appsink_backend()
            {
                self.abandon_pending_direct_takeover(&name, "session_replaced");
                continue;
            }

            let stats = pending.player.direct_sink_stats().unwrap_or_default();
            if stats.rendered > pending.last_rendered {
                pending.last_rendered = stats.rendered;
            }

            let elapsed = pending.armed_at.elapsed();
            let rendered_enough =
                stats.rendered >= DIRECT_HANDOFF_MIN_RENDERED_FRAMES && stats.average_rate >= 6.0;
            let clearly_advancing = stats.rendered >= 4
                && stats.average_rate >= 4.0
                && stats.dropped == 0
                && elapsed >= Duration::from_millis(500);
            if rendered_enough || clearly_advancing {
                self.promote_pending_direct_takeover(&name, stats.rendered, stats.average_rate);
                continue;
            }

            if elapsed >= DIRECT_HANDOFF_TIMEOUT {
                warn!(
                    "[VIDEO] {}: Direct Wayland handoff never proved live playback after {:.1}ms (rendered={} dropped={} avg_rate={:.1}); keeping appsink path active",
                    name,
                    elapsed.as_secs_f64() * 1000.0,
                    stats.rendered,
                    stats.dropped,
                    stats.average_rate
                );
                let pending_session_id = pending.session_id;
                self.metrics.record_direct_handoff_timeout();
                self.arm_direct_handoff_cooldown(&name, pending_session_id, "promotion_timeout");
                self.block_direct_handoff_session(&name, pending_session_id, "promotion_timeout");
                self.abandon_pending_direct_takeover(&name, "promotion_timeout");
            }
        }
    }

    pub fn drain_players<F>(
        &mut self,
        player_buf: Option<VideoPlayerResult>,
        loop_start: Instant,
        mut render_fn: F,
    ) where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let mut pending_players = Vec::new();
        if let Some(res) = player_buf {
            pending_players.push(res);
        }
        while let Ok(res) = self.player_rx.try_recv() {
            pending_players.push(res);
        }

        for res in pending_players {
            match res {
                VideoPlayerResult::Success(name, session_id, mut player, preroll_frame) => {
                    let was_pending_direct_handoff =
                        self.clear_pending_direct_handoff(&name, session_id);
                    let barrier_blocks = self.startup_barrier_blocks_output(&name, loop_start);
                    let direct_surface_backend = player.is_direct_surface_backend();
                    let pending = self.pending_video_switches.get(&name).cloned();
                    if let Some(pending) = pending.filter(|p| p.session_id == session_id) {
                        self.pending_video_switches.remove(&name);

                        let mut should_render = false;
                        let mut startup_ready = false;
                        if let Some(r) = self.renderers.get_mut(&name) {
                            r.active_batch_id = pending.batch_id;
                            r.batch_start_time = pending.batch_trigger_time;
                            r.active_transition = pending.transition;
                            r.set_content_type(crate::queue::ContentType::Video);
                            r.active_image_session_id = 0;
                            r.active_video_session_id = session_id;
                            r.switch_content();

                            if direct_surface_backend {
                                r.set_direct_video_presentation_active(true);
                                startup_ready = true;
                            } else if let Some(frame) = preroll_frame.as_ref() {
                                let upload_start = Instant::now();
                                r.upload_frame(frame);
                                self.metrics.record_video_cpu_time(upload_start.elapsed());
                                self.metrics.record_video_frame_uploaded();
                                startup_ready = true;
                                should_render = true;
                            }
                        } else {
                            stop_video_player_in_background(name, player);
                            continue;
                        }
                        if startup_ready {
                            self.mark_startup_output_ready(&name, loop_start);
                        }

                        if let Err(e) = player.start() {
                            error!(
                                "[VIDEO] {}: Failed to start deferred video player: {}",
                                name, e
                            );
                            set_pending_video_session(&self.pending_video_sessions, &name, None);
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.abort_transition();
                            }
                            self.handle_startup_content_failure(
                                &name,
                                &format!("player_start: {}", e),
                                loop_start,
                            );
                            continue;
                        }

                        let old_player = self.video_players.remove(&name);
                        self.video_players.insert(name.clone(), player);
                        if let Some(old) = old_player {
                            stop_video_player_in_background(name.clone(), old);
                        }

                        if direct_surface_backend {
                            if barrier_blocks {
                                debug!(
                                    "[STARTUP] {}: Native Wayland direct video path bypasses renderer startup barrier",
                                    name
                                );
                            }
                            self.mark_output_presented_if_ready(&name);
                            if !self.first_frame_recorded {
                                self.metrics.record_first_frame();
                                self.first_frame_recorded = true;
                            }
                        } else if should_render {
                            if barrier_blocks {
                                debug!(
                                    "[STARTUP] {}: First video frame ready, holding present for barrier release",
                                    name
                                );
                            } else if let Some(r) = self.renderers.get_mut(&name) {
                                render_fn(r, &name, loop_start);
                                if !self.first_frame_recorded {
                                    self.metrics.record_first_frame();
                                    self.first_frame_recorded = true;
                                }
                            }
                            self.mark_output_presented_if_ready(&name);
                        }
                    } else if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Err(e) = player.start() {
                            if was_pending_direct_handoff {
                                warn!(
                                    "[VIDEO] {}: Direct Wayland handoff failed to start ({}); keeping appsink renderer path active",
                                    name, e
                                );
                                self.block_direct_handoff_session(
                                    &name,
                                    session_id,
                                    "player_start_failed",
                                );
                                let _ = player.stop();
                                continue;
                            }

                            error!("[VIDEO] {}: Failed to start video player: {}", name, e);
                            set_pending_video_session(&self.pending_video_sessions, &name, None);
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.abort_transition();
                            }
                            self.handle_startup_content_failure(
                                &name,
                                &format!("player_start: {}", e),
                                loop_start,
                            );
                            continue;
                        }

                        if was_pending_direct_handoff && direct_surface_backend {
                            if let Some(r) = self.renderers.get_mut(&name) {
                                r.set_direct_video_presentation_active(false);
                            }
                            if let Some(previous_pending) =
                                self.pending_direct_takeovers.remove(&name)
                            {
                                stop_video_player_in_background(
                                    name.clone(),
                                    previous_pending.player,
                                );
                            }
                            self.pending_direct_takeovers.insert(
                                name.clone(),
                                PendingDirectTakeover {
                                    session_id,
                                    player,
                                    armed_at: loop_start,
                                    last_rendered: 0,
                                },
                            );
                            debug!(
                                "[VIDEO] {}: Waiting for direct Wayland sink to prove live playback before replacing appsink path",
                                name
                            );
                            continue;
                        }

                        if let Some(r) = self.renderers.get_mut(&name) {
                            if direct_surface_backend {
                                r.set_direct_video_presentation_active(true);
                            } else {
                                r.set_direct_video_presentation_active(false);
                                if let Some(frame) = preroll_frame.as_ref() {
                                    let upload_start = Instant::now();
                                    r.upload_frame(frame);
                                    self.metrics.record_video_cpu_time(upload_start.elapsed());
                                    self.metrics.record_video_frame_uploaded();
                                    render_fn(r, &name, loop_start);
                                    if !self.first_frame_recorded {
                                        self.metrics.record_first_frame();
                                        self.first_frame_recorded = true;
                                    }
                                }
                            }
                        }

                        let old_player = self.video_players.remove(&name);
                        self.video_players.insert(name.clone(), player);
                        if let Some(old) = old_player {
                            stop_video_player_in_background(name.clone(), old);
                        }
                        self.mark_output_presented_if_ready(&name);
                    } else {
                        stop_video_player_in_background(name, player);
                    }
                }
                VideoPlayerResult::Failure(name, session_id) => {
                    if self.clear_pending_direct_handoff(&name, session_id) {
                        warn!(
                            "[VIDEO] {}: Direct Wayland handoff prepare failed for session {}; keeping appsink renderer path active",
                            name, session_id
                        );
                        self.block_direct_handoff_session(
                            &name,
                            session_id,
                            "player_prepare_failed",
                        );
                        continue;
                    }

                    if self
                        .pending_video_switches
                        .get(&name)
                        .is_some_and(|p| p.session_id == session_id)
                    {
                        self.pending_video_switches.remove(&name);
                        set_pending_video_session(&self.pending_video_sessions, &name, None);
                    }
                    if self.renderers.get(&name).map(|r| r.active_video_session_id)
                        == Some(session_id)
                    {
                        if let Some(r) = self.renderers.get_mut(&name) {
                            r.abort_transition();
                        }
                    }
                    self.handle_startup_content_failure(&name, "player_prepare_failed", loop_start);
                }
            }
        }
    }

    pub fn drain_player_events(
        &mut self,
        player_event_buf: Option<PlayerEventMsg>,
        loop_start: Instant,
    ) {
        let mut events = Vec::new();
        if let Some(event) = player_event_buf {
            events.push(event);
        }
        while let Ok(event) = self.player_event_rx.try_recv() {
            events.push(event);
        }

        for event in events {
            if self
                .pending_direct_takeovers
                .get(&event.source_id)
                .is_some_and(|pending| {
                    pending.session_id == event.session_id
                        && event.backend_kind == video::VideoBackendKind::WaylandDirect
                })
            {
                match event.kind {
                    video::PlayerEventKind::Eos => {
                        debug!(
                            "[VIDEO] {} session={} pending direct takeover reported EOS ({})",
                            event.source_id, event.session_id, event.reason
                        );
                    }
                    video::PlayerEventKind::Error | video::PlayerEventKind::FatalLifecycle => {
                        warn!(
                            "[VIDEO] {} session={} pending direct takeover {:?}: {}; keeping appsink renderer path active",
                            event.source_id, event.session_id, event.kind, event.reason
                        );
                        self.metrics.record_error("video_runtime");
                        self.block_direct_handoff_session(
                            &event.source_id,
                            event.session_id,
                            "pending_runtime_error",
                        );
                        self.abandon_pending_direct_takeover(&event.source_id, "runtime_error");
                    }
                }
                continue;
            }

            let is_pending = self
                .pending_video_switches
                .get(&event.source_id)
                .is_some_and(|pending| pending.session_id == event.session_id);
            let is_active = self
                .renderers
                .get(&event.source_id)
                .is_some_and(|renderer| renderer.active_video_session_id == event.session_id);

            if !is_pending && !is_active {
                debug!(
                    "[VIDEO] Ignoring stale player event {} session={} kind={:?} reason={}",
                    event.source_id, event.session_id, event.kind, event.reason
                );
                continue;
            }

            match event.kind {
                video::PlayerEventKind::Eos => {
                    debug!(
                        "[VIDEO] {} session={} reported EOS ({})",
                        event.source_id, event.session_id, event.reason
                    );
                }
                video::PlayerEventKind::Error | video::PlayerEventKind::FatalLifecycle => {
                    let direct_backend = self
                        .video_players
                        .get(&event.source_id)
                        .is_some_and(video::VideoPlayer::is_direct_surface_backend);

                    error!(
                        "[VIDEO] {} session={} runtime {:?}: {}",
                        event.source_id, event.session_id, event.kind, event.reason
                    );
                    self.metrics.record_error("video_runtime");
                    if direct_backend {
                        self.block_direct_handoff_session(
                            &event.source_id,
                            event.session_id,
                            "active_runtime_error",
                        );
                    } else {
                        self.pending_direct_handoffs.remove(&event.source_id);
                    }

                    self.pending_video_switches.remove(&event.source_id);
                    set_pending_video_session(&self.pending_video_sessions, &event.source_id, None);

                    if let Some(player) = self.video_players.remove(&event.source_id) {
                        stop_video_player_in_background(event.source_id.clone(), player);
                    }

                    if let Some(renderer) = self.renderers.get_mut(&event.source_id) {
                        renderer.set_direct_video_presentation_active(false);
                        if renderer.active_video_session_id == event.session_id && !direct_backend {
                            renderer.abort_transition();
                        }
                    }

                    self.handle_startup_content_failure(
                        &event.source_id,
                        &event.reason,
                        loop_start,
                    );
                }
            }
        }
    }

    fn reset_startup_output_pending(&mut self, name: &str) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };
        if state.can_block && state.phase != StartupOutputPhase::Presented {
            state.phase = StartupOutputPhase::Pending;
        }
    }

    fn handle_startup_content_failure(
        &mut self,
        name: &str,
        reason: &str,
        loop_start: Instant,
    ) -> bool {
        let tracked = self.startup_present_barrier.as_ref().and_then(|barrier| {
            barrier
                .outputs
                .get(name)
                .map(|state| (barrier.batch_id, state.phase))
        });
        let Some((batch_id, phase)) = tracked else {
            return false;
        };

        if phase == StartupOutputPhase::Presented {
            return false;
        }

        let failed_path = self
            .monitor_manager
            .outputs
            .get(name)
            .and_then(|orch| orch.current_path.clone());
        self.mark_startup_output_failed(name, reason, failed_path.as_deref());

        let retry_number = self
            .startup_present_barrier
            .as_mut()
            .and_then(|barrier| barrier.outputs.get_mut(name))
            .and_then(|state| {
                if state.retry_count >= STARTUP_RETRY_LIMIT {
                    None
                } else {
                    state.retry_count += 1;
                    Some(state.retry_count)
                }
            });

        let Some(retry_number) = retry_number else {
            warn!(
                "[STARTUP] {}: retries exhausted after failure ({})",
                name, reason
            );
            if let Some(state) = self
                .startup_present_barrier
                .as_mut()
                .and_then(|barrier| barrier.outputs.get_mut(name))
            {
                state.retry_count = STARTUP_RETRY_LIMIT;
            }
            self.maybe_clear_startup_present_barrier();
            return true;
        };

        let failed_paths = self
            .startup_present_barrier
            .as_ref()
            .and_then(|barrier| barrier.outputs.get(name))
            .map(|state| state.failed_paths.clone())
            .unwrap_or_default();
        let changes = self
            .monitor_manager
            .pick_startup_replacement(name, &failed_paths);

        if changes.is_empty() {
            warn!(
                "[STARTUP] {}: no replacement candidate after failure ({})",
                name, reason
            );
            if let Some(state) = self
                .startup_present_barrier
                .as_mut()
                .and_then(|barrier| barrier.outputs.get_mut(name))
            {
                state.retry_count = STARTUP_RETRY_LIMIT;
            }
            self.maybe_clear_startup_present_barrier();
            return true;
        }

        info!(
            "[STARTUP] {}: retry {}/{} after failure ({})",
            name, retry_number, STARTUP_RETRY_LIMIT, reason
        );

        for changed_name in changes.keys() {
            self.reset_startup_output_pending(changed_name);
        }

        for (changed_name, (path, content_type)) in changes {
            switch_wallpaper_content(
                &changed_name,
                &path,
                content_type,
                &self.metrics,
                &mut self.next_session_id,
                &self.latest_video_frames,
                &self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
                &mut self.pending_video_switches,
                &mut self.pending_image_video_stops,
                &mut self.pending_direct_takeovers,
                &mut self.pending_direct_handoffs,
                &mut self.blocked_direct_handoff_sessions,
                &self.pending_video_sessions,
                Some(batch_id),
                Some(loop_start),
                &self.image_tx,
                &self.player_tx,
                &self.player_event_tx,
                self.native_video_targets.get(&changed_name).cloned(),
                &self.shutdown_flag,
                "STARTUP-RETRY",
            );
        }

        true
    }

    // ─── Housekeeping ───────────────────────────────────────────────────

    /// Record frame time, clean up texture pool, flush stats, process dir watcher,
    /// log metrics summary. Called at the end of each loop iteration.
    pub async fn housekeeping(&mut self, loop_start: Instant, was_idle: bool) {
        // Skip recording frame time for iterations that entered idle_wait (P-26)
        if !was_idle {
            let frame_time = loop_start.elapsed();
            self.metrics.record_frame_time(frame_time);
        }

        self.service_pending_direct_takeovers();

        for renderer in self.renderers.values_mut() {
            renderer.trim_idle_retained_resources();
        }

        // Cleanup texture pool periodically (every 3 seconds)
        if self.last_pool_cleanup.elapsed().as_secs() >= 3 {
            if let Some(ctx) = &self.wgpu_ctx {
                ctx.cleanup_texture_pool(Some(&self.metrics));
            }
            self.last_pool_cleanup = Instant::now();
        }

        // Flush stats every 5 seconds (batched writes)
        if self.last_stats_flush.elapsed().as_secs() >= 5 {
            let _ = self.monitor_manager.flush_all_stats();
            self.last_stats_flush = Instant::now();
        }

        // Process directory watcher events and apply pool updates
        if self.last_dir_watch_poll.elapsed() >= Duration::from_millis(250) {
            if let Some(ref mut watcher) = self.dir_watcher {
                let pool_events = watcher.process_events().await;
                self.monitor_manager.apply_pool_events(pool_events);
            }
            self.last_dir_watch_poll = Instant::now();
        }

        // Log metrics summary every 10 seconds
        if self.last_metrics_log.elapsed().as_secs() >= 10 {
            if let Some(ctx) = &self.wgpu_ctx {
                let (texture_count, texture_pool_bytes) = ctx.texture_pool_stats();
                let pipeline_count = ctx.transition_pipelines.lock().len()
                    + ctx.blit_pipelines.lock().len()
                    + ctx.mipmap_pipelines.lock().len();
                self.metrics.record_texture_count(texture_count);
                self.metrics.record_pipeline_count(pipeline_count);
                let active_video_players = self.video_players.len();
                let active_direct_video_players = self
                    .video_players
                    .values()
                    .filter(|player| player.is_direct_surface_backend())
                    .count();
                let active_appsink_video_players =
                    active_video_players.saturating_sub(active_direct_video_players);
                let pending_video_stops = self.pending_image_video_stops.len();
                let pending_video_switches = self.pending_video_switches.len();
                let pending_direct_takeovers = self.pending_direct_takeovers.len();
                let pending_direct_handoffs = self.pending_direct_handoffs.len();
                let latest_frame_slots = self.latest_video_frames.occupancy();
                let background_snapshot = background::snapshot();
                let mut appsink_queue_levels = video::AppsinkQueueLevels::default();
                let mut appsink_queue_players = 0usize;
                let mut direct_sink_stats = video::DirectSinkStats::default();
                let mut direct_sink_players = 0usize;
                for player in self.video_players.values() {
                    if let Some(levels) = player.appsink_queue_levels() {
                        appsink_queue_players += 1;
                        appsink_queue_levels.buffers =
                            appsink_queue_levels.buffers.saturating_add(levels.buffers);
                        appsink_queue_levels.bytes =
                            appsink_queue_levels.bytes.saturating_add(levels.bytes);
                        appsink_queue_levels.time_ns =
                            appsink_queue_levels.time_ns.saturating_add(levels.time_ns);
                    }
                    if let Some(stats) = player.direct_sink_stats() {
                        direct_sink_players += 1;
                        direct_sink_stats.rendered =
                            direct_sink_stats.rendered.saturating_add(stats.rendered);
                        direct_sink_stats.dropped =
                            direct_sink_stats.dropped.saturating_add(stats.dropped);
                        direct_sink_stats.average_rate += stats.average_rate;
                        if stats.dropped > 0 {
                            debug!(
                                "[VIDEO] direct sink drops observed: rendered={} dropped={} avg_rate={:.2}",
                                stats.rendered, stats.dropped, stats.average_rate
                            );
                        }
                    }
                }

                let mut retained = renderer::RetainedTextureFootprint::default();
                let mut per_renderer = Vec::new();
                let to_mb = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
                for (name, r) in &self.renderers {
                    let fp = r.retained_texture_footprint();
                    retained.current_bytes =
                        retained.current_bytes.saturating_add(fp.current_bytes);
                    retained.prev_bytes = retained.prev_bytes.saturating_add(fp.prev_bytes);
                    retained.composition_bytes = retained
                        .composition_bytes
                        .saturating_add(fp.composition_bytes);
                    retained.video_aux_bytes =
                        retained.video_aux_bytes.saturating_add(fp.video_aux_bytes);
                    per_renderer.push(format!(
                        "{}={:.1}MB(c={:.1} p={:.1} comp={:.1} aux={:.1})",
                        name,
                        to_mb(fp.total_bytes()),
                        to_mb(fp.current_bytes),
                        to_mb(fp.prev_bytes),
                        to_mb(fp.composition_bytes),
                        to_mb(fp.video_aux_bytes)
                    ));
                }
                info!(
                    "[MEMORY] Renderer retained textures: total={:.1}MB current={:.1}MB prev={:.1}MB composition={:.1}MB video_aux={:.1}MB pool={:.1}MB | video_players={} direct={} appsink={} pending_switches={} pending_stops={} pending_handoffs={} pending_takeovers={} latest_frame_slots={} appsink={}q/{}b/{:.1}ms@{}p direct={}r/{}d/{:.1}fps@{}p | background={} | {}",
                    to_mb(retained.total_bytes()),
                    to_mb(retained.current_bytes),
                    to_mb(retained.prev_bytes),
                    to_mb(retained.composition_bytes),
                    to_mb(retained.video_aux_bytes),
                    to_mb(texture_pool_bytes),
                    active_video_players,
                    active_direct_video_players,
                    active_appsink_video_players,
                    pending_video_switches,
                    pending_video_stops,
                    pending_direct_handoffs,
                    pending_direct_takeovers,
                    latest_frame_slots,
                    appsink_queue_levels.buffers,
                    appsink_queue_levels.bytes,
                    appsink_queue_levels.time_ns as f64 / 1_000_000.0,
                    appsink_queue_players,
                    direct_sink_stats.rendered,
                    direct_sink_stats.dropped,
                    if direct_sink_players > 0 {
                        direct_sink_stats.average_rate / direct_sink_players as f64
                    } else {
                        0.0
                    },
                    direct_sink_players,
                    background_snapshot.format_compact(),
                    per_renderer.join(" | ")
                );
            }
            self.metrics.log_summary();
            self.last_metrics_log = Instant::now();
        }
    }

    /// Sleep at vsync rate if actively rendering, then poll device.
    pub async fn timing_and_poll(&mut self, any_active: bool, loop_start: Instant) {
        let elapsed = loop_start.elapsed();
        if any_active && elapsed < self.target_frame_time {
            tokio::time::sleep(self.target_frame_time - elapsed).await;
        }

        let poll_interval = if any_active {
            Duration::from_millis(16)
        } else {
            Duration::from_millis(100)
        };
        if self.last_device_poll.elapsed() < poll_interval {
            return;
        }

        if let Some(ctx) = &self.wgpu_ctx {
            ctx.device.poll(wgpu::Maintain::Poll);
        }
        self.last_device_poll = Instant::now();
    }

    /// Perform the initial content load.
    /// Calls `monitor_manager.tick()` and dispatches initial wallpaper content.
    pub fn initial_load(&mut self) {
        info!(
            "[STARTUP] Reached Initial Load section, renderers count: {}",
            self.renderers.len()
        );
        info!("[STARTUP] About to call monitor_manager.tick()");
        let initial_changes = self.monitor_manager.tick();
        info!(
            "[STARTUP] Initial changes: {} outputs",
            initial_changes.len()
        );
        for (name, (path, content_type)) in &initial_changes {
            info!(
                "[STARTUP] Change: {} -> {:?} ({:?})",
                name, path, content_type
            );
        }
        if initial_changes.is_empty() {
            warn!("[STARTUP] No initial content changes - wallpapers may not load!");
        }
        let batch_id = rand::random::<u64>();
        let mut startup_outputs = Vec::new();
        let ordered_changes = ordered_pending_content_switches(&self.renderers, initial_changes);
        for change in ordered_changes {
            if !self.renderers.contains_key(&change.name) {
                warn!(
                    "[STARTUP] Skipping initial content for {} - renderer does not exist",
                    change.name
                );
                continue;
            }
            startup_outputs.push(change.name.clone());
            switch_wallpaper_content(
                &change.name,
                &change.path,
                change.content_type,
                &self.metrics,
                &mut self.next_session_id,
                &self.latest_video_frames,
                &self.monitor_manager,
                &mut self.renderers,
                &mut self.video_players,
                &mut self.pending_video_switches,
                &mut self.pending_image_video_stops,
                &mut self.pending_direct_takeovers,
                &mut self.pending_direct_handoffs,
                &mut self.blocked_direct_handoff_sessions,
                &self.pending_video_sessions,
                Some(batch_id),
                None,
                &self.image_tx,
                &self.player_tx,
                &self.player_event_tx,
                self.native_video_targets.get(&change.name).cloned(),
                &self.shutdown_flag,
                "STARTUP",
            );
        }
        if startup_outputs.len() > 1 {
            self.arm_startup_present_barrier(batch_id, startup_outputs);
        }
    }

    pub fn arm_startup_present_barrier(&mut self, batch_id: u64, outputs: Vec<String>) {
        let output_states: HashMap<_, _> = outputs
            .into_iter()
            .filter(|name| {
                self.renderers
                    .get(name)
                    .is_some_and(|renderer| !renderer.has_any_content())
            })
            .map(|name| (name, StartupOutputState::pending()))
            .collect();
        if output_states.len() <= 1 {
            return;
        }

        let now = Instant::now();
        self.startup_present_barrier = Some(StartupPresentBarrier {
            batch_id,
            armed_at: now,
            first_ready_at: None,
            release_reason: None,
            outputs: output_states,
        });
        info!(
            "[STARTUP] First-present barrier armed for {} outputs (batch {:x})",
            self.startup_present_barrier
                .as_ref()
                .map_or(0, |b| b.outputs.len()),
            batch_id
        );
    }

    pub fn startup_barrier_blocks_output(&self, name: &str, now: Instant) -> bool {
        let Some(barrier) = &self.startup_present_barrier else {
            return false;
        };

        let Some(state) = barrier.outputs.get(name) else {
            return false;
        };

        if !state.can_block {
            return false;
        }

        startup_barrier_release_candidate(barrier, now).is_none()
    }

    pub fn release_startup_present_barrier<F>(&mut self, loop_start: Instant, mut render_fn: F)
    where
        F: FnMut(&mut renderer::Renderer, &str, Instant),
    {
        let Some(reason) = self
            .startup_present_barrier
            .as_ref()
            .and_then(|barrier| startup_barrier_release_candidate(barrier, loop_start))
        else {
            return;
        };

        if let Some(barrier) = self.startup_present_barrier.as_mut() {
            if barrier.release_reason.is_none() {
                let (pending, ready, failed) = startup_barrier_counts(barrier);
                barrier.release_reason = Some(reason);
                info!(
                    "[STARTUP] First-present barrier released for batch {:x} after {:.1}ms reason={} pending={} ready={} failed={}",
                    barrier.batch_id,
                    duration_ms(loop_start.saturating_duration_since(barrier.armed_at)),
                    reason,
                    pending,
                    ready,
                    failed
                );
            }
        }

        let outputs_to_release: Vec<String> = self
            .startup_present_barrier
            .as_ref()
            .map(|barrier| {
                barrier
                    .outputs
                    .iter()
                    .filter_map(|(name, state)| {
                        if state.can_block && state.phase == StartupOutputPhase::Ready {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        for name in outputs_to_release {
            if let Some(r) = self.renderers.get_mut(&name) {
                render_fn(r, &name, loop_start);
                if !self.first_frame_recorded {
                    self.metrics.record_first_frame();
                    self.first_frame_recorded = true;
                }
                self.mark_output_presented_if_ready(&name);
            }
        }

        self.maybe_clear_startup_present_barrier();
    }

    pub(crate) fn mark_startup_output_ready(&mut self, name: &str, now: Instant) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if state.phase == StartupOutputPhase::Presented {
            return;
        }

        if state.first_ready_at.is_none() {
            state.first_ready_at = Some(now);
            info!(
                "[STARTUP] {} first-ready {:.1}ms (batch {:x})",
                name,
                duration_ms(now.saturating_duration_since(barrier.armed_at)),
                barrier.batch_id
            );
        }

        if barrier.first_ready_at.is_none() {
            barrier.first_ready_at = Some(now);
        }

        state.phase = StartupOutputPhase::Ready;
    }

    fn mark_startup_output_failed(&mut self, name: &str, reason: &str, failed_path: Option<&Path>) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if let Some(path) = failed_path {
            state.failed_paths.insert(path.to_path_buf());
        }
        state.phase = StartupOutputPhase::Failed;
        state.can_block = false;

        info!(
            "[STARTUP] {} failed after {:.1}ms reason={} retries={} batch {:x}",
            name,
            duration_ms(Instant::now().saturating_duration_since(barrier.armed_at)),
            reason,
            state.retry_count,
            barrier.batch_id
        );
    }

    fn mark_startup_output_presented(&mut self, name: &str, now: Instant) {
        let Some(barrier) = self.startup_present_barrier.as_mut() else {
            return;
        };
        let Some(state) = barrier.outputs.get_mut(name) else {
            return;
        };

        if state.first_present_at.is_none() {
            state.first_present_at = Some(now);
            info!(
                "[STARTUP] {} first-present {:.1}ms (batch {:x})",
                name,
                duration_ms(now.saturating_duration_since(barrier.armed_at)),
                barrier.batch_id
            );
        }

        state.phase = StartupOutputPhase::Presented;
        state.can_block = false;
    }

    fn maybe_clear_startup_present_barrier(&mut self) {
        if self
            .startup_present_barrier
            .as_ref()
            .is_some_and(startup_barrier_is_terminal)
        {
            self.startup_present_barrier = None;
        }
    }

    /// Clean shutdown — stop all video players, quiesce background work, and save caches.
    pub async fn shutdown(&mut self) {
        let shutdown_start = Instant::now();
        self.pending_video_switches.clear();
        self.pending_direct_handoffs.clear();
        self.blocked_direct_handoff_sessions.clear();
        self.direct_handoff_cooldown_until.clear();
        self.direct_handoff_failure_streak.clear();
        for (_, mut pending) in self.pending_direct_takeovers.drain() {
            let _ = pending.player.request_stop();
        }
        if let Ok(mut sessions) = self.pending_video_sessions.lock() {
            sessions.clear();
        }

        background::close_global_work();

        let stop_players_start = Instant::now();
        for (_, mut player) in self.video_players.drain() {
            let _ = player.request_stop();
        }
        for (_, mut player) in self.pending_image_video_stops.drain() {
            let _ = player.request_stop();
        }
        let stop_players_duration = stop_players_start.elapsed();

        let background_wait_start = Instant::now();
        let background_quiet = background::wait_for_global_quiet(Duration::from_millis(250)).await;
        let background_wait_duration = background_wait_start.elapsed();

        let bus_shutdown_start = Instant::now();
        crate::video::shutdown_bus_dispatcher(Duration::from_millis(250));
        let bus_shutdown_duration = bus_shutdown_start.elapsed();

        let cache_start = Instant::now();
        // Drop renderer-owned wgpu surfaces while the backend connection still exists.
        self.renderers.clear();
        if let Some(ctx) = &self.wgpu_ctx {
            ctx.persist_pipeline_cache();
        }
        self.wgpu_ctx = None;
        // Persist WGSL cache to disk on shutdown (P-15 cache layer 1)
        if let Err(e) = crate::shaders::ShaderManager::save_cache() {
            warn!("[SHADER] Failed to save WGSL cache: {}", e);
        }

        info!(
            "[SHUTDOWN] stop_players={:.1}ms background_wait={:.1}ms background_quiet={} bus={:.1}ms caches={:.1}ms total={:.1}ms background={}",
            duration_ms(stop_players_duration),
            duration_ms(background_wait_duration),
            background_quiet,
            duration_ms(bus_shutdown_duration),
            duration_ms(cache_start.elapsed()),
            duration_ms(shutdown_start.elapsed()),
            background::snapshot().format_compact()
        );
    }
}

// ─── Standalone helpers ─────────────────────────────────────────────────────

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

pub(crate) fn set_pending_video_session(
    pending_video_sessions: &PendingVideoSessions,
    name: &str,
    session_id: Option<u64>,
) {
    let Ok(mut sessions) = pending_video_sessions.lock() else {
        return;
    };

    match session_id {
        Some(session_id) => {
            sessions.insert(name.to_string(), session_id);
        }
        None => {
            sessions.remove(name);
        }
    }
}

fn pending_video_session_matches(
    pending_video_sessions: &PendingVideoSessions,
    name: &str,
    session_id: u64,
) -> bool {
    pending_video_sessions
        .lock()
        .ok()
        .and_then(|sessions| sessions.get(name).copied())
        == Some(session_id)
}

fn image_format_label(format: Option<image::ImageFormat>, fast_path: bool) -> String {
    let label = match format {
        Some(image::ImageFormat::Avif) => "avif",
        Some(image::ImageFormat::Bmp) => "bmp",
        Some(image::ImageFormat::Gif) => "gif",
        Some(image::ImageFormat::Hdr) => "hdr",
        Some(image::ImageFormat::Ico) => "ico",
        Some(image::ImageFormat::Jpeg) => "jpeg",
        Some(image::ImageFormat::OpenExr) => "openexr",
        Some(image::ImageFormat::Png) => "png",
        Some(image::ImageFormat::Pnm) => "pnm",
        Some(image::ImageFormat::Qoi) => "qoi",
        Some(image::ImageFormat::Tga) => "tga",
        Some(image::ImageFormat::Tiff) => "tiff",
        Some(image::ImageFormat::WebP) => "webp",
        Some(image::ImageFormat::Dds) => "dds",
        Some(image::ImageFormat::Farbfeld) => "farbfeld",
        _ => "unknown",
    };

    if fast_path {
        format!("{}-fast", label)
    } else {
        label.to_string()
    }
}

const PREPARED_IMAGE_CACHE_MAGIC: &[u8; 8] = b"KDXIMG02";

fn prepared_image_cache_dir() -> Option<PathBuf> {
    let dir = dirs::cache_dir()?.join("kaleidux").join("prepared-images");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

fn image_source_identity(path: &Path) -> Option<ImageSourceIdentity> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;

    Some(ImageSourceIdentity {
        path: path.to_path_buf(),
        file_len: meta.len(),
        modified_secs: modified.as_secs(),
        modified_nanos: modified.subsec_nanos(),
    })
}

fn prepared_image_key_for_identity(
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

fn prepared_image_cache_key(key: &PreparedImageKey) -> String {
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

#[cfg(test)]
fn prepared_image_cache_lookup_key(
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

fn prepared_image_cache_path_for_key(key: &PreparedImageKey) -> Option<PathBuf> {
    let dir = prepared_image_cache_dir()?;
    Some(dir.join(format!("{}.rgba", prepared_image_cache_key(key))))
}

#[cfg(test)]
fn prepared_image_cache_path(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<PathBuf> {
    let key = prepared_image_cache_lookup_key(path, target_width, target_height)?;
    prepared_image_cache_path_for_key(&key)
}

fn try_load_prepared_image_cache_by_key(key: &PreparedImageKey) -> Option<DecodedImagePayload> {
    let cache_path = prepared_image_cache_path_for_key(key)?;
    let bytes = std::fs::read(cache_path).ok()?;
    if bytes.len() < PREPARED_IMAGE_CACHE_MAGIC.len() + (4 * 4) {
        return None;
    }
    if &bytes[..PREPARED_IMAGE_CACHE_MAGIC.len()] != PREPARED_IMAGE_CACHE_MAGIC {
        return None;
    }

    let mut cursor = PREPARED_IMAGE_CACHE_MAGIC.len();
    let read_u32 = |buf: &[u8], cursor: &mut usize| -> Option<u32> {
        let end = *cursor + 4;
        let slice = buf.get(*cursor..end)?;
        *cursor = end;
        Some(u32::from_le_bytes(slice.try_into().ok()?))
    };

    let width = read_u32(&bytes, &mut cursor)?;
    let height = read_u32(&bytes, &mut cursor)?;
    let source_width = read_u32(&bytes, &mut cursor)?;
    let source_height = read_u32(&bytes, &mut cursor)?;
    let data = bytes.get(cursor..)?.to_vec();
    if data.len() != (width as usize * height as usize * 4) {
        return None;
    }

    Some(DecodedImagePayload {
        data: data.into(),
        width,
        height,
        profile: ImageLoadProfile {
            format: "prepared-cache".to_string(),
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

#[cfg(test)]
fn try_load_prepared_image_cache(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> Option<DecodedImagePayload> {
    let key = prepared_image_cache_lookup_key(path, target_width, target_height)?;
    try_load_prepared_image_cache_by_key(&key)
}

fn store_prepared_image_cache_by_key(key: &PreparedImageKey, payload: &DecodedImagePayload) {
    let Some(cache_path) = prepared_image_cache_path_for_key(key) else {
        return;
    };

    let expected_len = payload.width as usize * payload.height as usize * 4;
    if payload.data.len() != expected_len {
        return;
    }

    let mut bytes =
        Vec::with_capacity(PREPARED_IMAGE_CACHE_MAGIC.len() + (4 * 4) + payload.data.len());
    bytes.extend_from_slice(PREPARED_IMAGE_CACHE_MAGIC);
    bytes.extend_from_slice(&payload.width.to_le_bytes());
    bytes.extend_from_slice(&payload.height.to_le_bytes());
    bytes.extend_from_slice(&payload.profile.source_width.to_le_bytes());
    bytes.extend_from_slice(&payload.profile.source_height.to_le_bytes());
    bytes.extend_from_slice(payload.data.as_ref());

    let tmp_path = cache_path.with_extension("rgba.tmp");
    if std::fs::write(&tmp_path, &bytes).is_ok() {
        let _ = std::fs::rename(tmp_path, cache_path);
    }
}

#[cfg(test)]
fn store_prepared_image_cache(
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

fn try_load_prepared_image_memory(key: &PreparedImageKey) -> Option<DecodedImagePayload> {
    PREPARED_IMAGE_MEMORY_CACHE
        .lock()
        .get_cloned(key)
        .map(|entry| entry.to_payload("prepared-memory-cache"))
}

fn try_load_compatible_prepared_image_memory(
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

fn find_compatible_prepared_in_flight_state(
    identity: &ImageSourceIdentity,
    target_width: u32,
    target_height: u32,
) -> Option<Arc<InFlightSharedResult<PreparedImageEntry>>> {
    let in_flight = PREPARED_IMAGE_IN_FLIGHT.lock();
    let candidate_key =
        select_compatible_prepared_key(in_flight.keys(), identity, target_width, target_height)?;
    in_flight.get(&candidate_key).cloned()
}

fn collect_pending_content_switches<I>(
    renderers: &HashMap<String, renderer::Renderer>,
    changes: I,
) -> Vec<PendingContentSwitch>
where
    I: IntoIterator<Item = (String, (PathBuf, queue::ContentType))>,
{
    let mut pending = Vec::new();
    for (name, (path, content_type)) in changes {
        let target_area = renderers
            .get(&name)
            .map(|renderer| u64::from(renderer.config.width) * u64::from(renderer.config.height))
            .unwrap_or(0);
        pending.push(PendingContentSwitch {
            name,
            path,
            content_type,
            target_area,
        });
    }
    pending
}

fn sort_pending_content_switches(pending: &mut [PendingContentSwitch]) {
    pending.sort_by(
        |left, right| match (left.content_type, right.content_type) {
            (queue::ContentType::Image, queue::ContentType::Image) => left
                .path
                .cmp(&right.path)
                .then_with(|| right.target_area.cmp(&left.target_area))
                .then_with(|| left.name.cmp(&right.name)),
            (queue::ContentType::Image, _) => std::cmp::Ordering::Less,
            (_, queue::ContentType::Image) => std::cmp::Ordering::Greater,
            _ => left.name.cmp(&right.name),
        },
    );
}

fn ordered_pending_content_switches<I>(
    renderers: &HashMap<String, renderer::Renderer>,
    changes: I,
) -> Vec<PendingContentSwitch>
where
    I: IntoIterator<Item = (String, (PathBuf, queue::ContentType))>,
{
    let mut pending = collect_pending_content_switches(renderers, changes);
    sort_pending_content_switches(&mut pending);
    pending
}

fn store_prepared_image_memory(key: PreparedImageKey, payload: &DecodedImagePayload) {
    PREPARED_IMAGE_MEMORY_CACHE
        .lock()
        .put(key, Arc::new(PreparedImageEntry::from_payload(payload)));
}

fn try_load_decoded_source_memory(
    identity: &ImageSourceIdentity,
) -> Option<Arc<DecodedSourceImage>> {
    SOURCE_IMAGE_MEMORY_CACHE.lock().get_cloned(identity)
}

fn try_load_source_descriptor_memory(
    identity: &ImageSourceIdentity,
) -> Option<Arc<ImageSourceDescriptor>> {
    SOURCE_IMAGE_DESCRIPTOR_CACHE.lock().get_cloned(identity)
}

fn store_decoded_source_memory(identity: ImageSourceIdentity, source: Arc<DecodedSourceImage>) {
    SOURCE_IMAGE_MEMORY_CACHE.lock().put(identity, source);
}

fn store_source_descriptor_memory(descriptor: Arc<ImageSourceDescriptor>) {
    SOURCE_IMAGE_DESCRIPTOR_CACHE
        .lock()
        .put(descriptor.identity.clone(), descriptor);
}

fn load_image_source_descriptor(path: &Path) -> Option<Arc<ImageSourceDescriptor>> {
    let identity = image_source_identity(path)?;
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
    let descriptor = Arc::new(ImageSourceDescriptor::from_dimensions(
        identity, width, height,
    ));
    store_source_descriptor_memory(descriptor.clone());
    Some(descriptor)
}

const MAX_IMAGE_UPLOAD_DIMENSION: u32 = 8192;

fn compute_cover_target_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    renderer::compute_cover_target_dimensions(
        source_width,
        source_height,
        target_width,
        target_height,
    )
}

fn apply_upload_dimension_clamp(source_width: u32, source_height: u32) -> Option<(u32, u32)> {
    if source_width <= MAX_IMAGE_UPLOAD_DIMENSION && source_height <= MAX_IMAGE_UPLOAD_DIMENSION {
        return None;
    }

    let longest_edge = source_width.max(source_height) as f32;
    let scale = MAX_IMAGE_UPLOAD_DIMENSION as f32 / longest_edge;
    let resized_width = ((source_width as f32 * scale).round() as u32).max(1);
    let resized_height = ((source_height as f32 * scale).round() as u32).max(1);
    Some((resized_width, resized_height))
}

fn compute_upload_downscale_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> Option<(u32, u32)> {
    let (cover_width, cover_height) =
        compute_cover_target_dimensions(source_width, source_height, target_width, target_height);
    let (prepared_width, prepared_height) = apply_upload_dimension_clamp(cover_width, cover_height)
        .unwrap_or((cover_width, cover_height));

    if prepared_width == source_width && prepared_height == source_height {
        None
    } else {
        Some((prepared_width, prepared_height))
    }
}

#[cfg(test)]
fn prepared_target_dimensions_from_path(
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

fn prepared_target_dimensions_from_descriptor(
    descriptor: &ImageSourceDescriptor,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    let source_width = descriptor.width;
    let source_height = descriptor.height;
    let (cover_width, cover_height) =
        compute_cover_target_dimensions(source_width, source_height, target_width, target_height);
    apply_upload_dimension_clamp(cover_width, cover_height).unwrap_or((cover_width, cover_height))
}

async fn acquire_image_work_permit(
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

fn select_resize_filter(
    source_width: u32,
    source_height: u32,
    resized_width: u32,
    resized_height: u32,
) -> fast_image_resize::FilterType {
    let width_ratio = source_width as f32 / resized_width as f32;
    let height_ratio = source_height as f32 / resized_height as f32;
    if width_ratio >= 2.0 || height_ratio >= 2.0 {
        fast_image_resize::FilterType::Bilinear
    } else {
        fast_image_resize::FilterType::CatmullRom
    }
}

fn resize_filter_label(filter: fast_image_resize::FilterType) -> &'static str {
    match filter {
        fast_image_resize::FilterType::Box => "box",
        fast_image_resize::FilterType::Bilinear => "bilinear",
        fast_image_resize::FilterType::Hamming => "hamming",
        fast_image_resize::FilterType::CatmullRom => "catmull-rom",
        fast_image_resize::FilterType::Mitchell => "mitchell",
        fast_image_resize::FilterType::Gaussian => "gaussian",
        fast_image_resize::FilterType::Lanczos3 => "lanczos3",
        fast_image_resize::FilterType::Custom(_) => "custom",
        _ => "unknown",
    }
}

fn resize_image_buffer(
    source_data: &[u8],
    source_width: u32,
    source_height: u32,
    resized_width: u32,
    resized_height: u32,
    pixel_type: fast_image_resize::PixelType,
    filter: fast_image_resize::FilterType,
) -> anyhow::Result<Vec<u8>> {
    use fast_image_resize as fr;

    let source = fr::images::ImageRef::new(source_width, source_height, source_data, pixel_type)
        .map_err(|e| anyhow::anyhow!("invalid source image buffer: {}", e))?;
    let mut resized = fr::images::Image::new(resized_width, resized_height, pixel_type);
    let mut resizer = fr::Resizer::new();
    resizer
        .resize(
            &source,
            &mut resized,
            Some(&fr::ResizeOptions::new().resize_alg(fr::ResizeAlg::Convolution(filter))),
        )
        .map_err(|e| anyhow::anyhow!("image resize failed: {}", e))?;
    Ok(resized.into_vec())
}

fn expand_rgb_to_rgba(rgb: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((rgb.len() / 3) * 4);
    for chunk in rgb.chunks_exact(3) {
        rgba.extend_from_slice(&[chunk[0], chunk[1], chunk[2], 255]);
    }
    rgba
}

fn expand_luma_to_rgba(luma: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity(luma.len() * 4);
    for value in luma {
        rgba.extend_from_slice(&[*value, *value, *value, 255]);
    }
    rgba
}

fn expand_lumaa_to_rgba(lumaa: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((lumaa.len() / 2) * 4);
    for chunk in lumaa.chunks_exact(2) {
        rgba.extend_from_slice(&[chunk[0], chunk[0], chunk[0], chunk[1]]);
    }
    rgba
}

fn prepare_rgb_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let pixels = pixels.as_ref();
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (rgba_data, width, height, expand_duration) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x3,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        let expand_start = Instant::now();
        let rgba = expand_rgb_to_rgba(&resized);
        (rgba, resized_width, resized_height, expand_start.elapsed())
    } else {
        let expand_start = Instant::now();
        let rgba = expand_rgb_to_rgba(pixels);
        (rgba, source_width, source_height, expand_start.elapsed())
    };

    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

fn prepare_rgba_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Option<String>)> {
    let pixels = pixels.as_ref();
    if let Some((resized_width, resized_height)) = compute_upload_downscale_dimensions(
        source_width,
        source_height,
        target_width,
        target_height,
    ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x4,
            filter,
        )?;
        return Ok((
            resized,
            resized_width,
            resized_height,
            resize_start.elapsed(),
            Some(resize_filter_label(filter).to_string()),
        ));
    }

    Ok((
        pixels.to_vec(),
        source_width,
        source_height,
        Duration::ZERO,
        None,
    ))
}

fn prepare_luma_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let pixels = pixels.as_ref();
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (rgba_data, width, height, expand_duration) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        let expand_start = Instant::now();
        let rgba = expand_luma_to_rgba(&resized);
        (rgba, resized_width, resized_height, expand_start.elapsed())
    } else {
        let expand_start = Instant::now();
        let rgba = expand_luma_to_rgba(pixels);
        (rgba, source_width, source_height, expand_start.elapsed())
    };

    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

fn prepare_lumaa_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, Duration, Duration, Option<String>)> {
    let pixels = pixels.as_ref();
    let mut resize_duration = Duration::ZERO;
    let mut resize_filter = None;
    let (rgba_data, width, height, expand_duration) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x2,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        let expand_start = Instant::now();
        let rgba = expand_lumaa_to_rgba(&resized);
        (rgba, resized_width, resized_height, expand_start.elapsed())
    } else {
        let expand_start = Instant::now();
        let rgba = expand_lumaa_to_rgba(pixels);
        (rgba, source_width, source_height, expand_start.elapsed())
    };

    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

fn decode_jpeg_source_fast(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    let decode_start = Instant::now();
    let encoded = std::fs::read(path)?;
    let options = DecoderOptions::new_fast()
        .set_strict_mode(false)
        .set_max_width(usize::MAX)
        .set_max_height(usize::MAX)
        .jpeg_set_out_colorspace(ColorSpace::RGB);
    let mut decoder =
        zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(encoded.as_slice()), options);
    decoder
        .decode_headers()
        .map_err(|e| anyhow::anyhow!("jpeg header decode failed: {}", e))?;
    let (source_width, source_height) = decoder
        .dimensions()
        .ok_or_else(|| anyhow::anyhow!("jpeg dimensions missing after header decode"))?;
    let source_width =
        u32::try_from(source_width).map_err(|_| anyhow::anyhow!("jpeg width is too large"))?;
    let source_height =
        u32::try_from(source_height).map_err(|_| anyhow::anyhow!("jpeg height is too large"))?;
    let decoded = decoder
        .decode()
        .map_err(|e| anyhow::anyhow!("jpeg decode failed: {}", e))?;
    Ok(DecodedSourceImage {
        pixels: DecodedSourcePixels::Rgb(decoded.into()),
        width: source_width,
        height: source_height,
        format: "jpeg-fast".to_string(),
        decode: decode_start.elapsed(),
        convert: Duration::ZERO,
    })
}

fn decode_png_source_fast(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    let decode_start = Instant::now();
    let encoded = std::fs::read(path)?;
    let options = DecoderOptions::default()
        .set_strict_mode(false)
        .set_max_width(usize::MAX)
        .set_max_height(usize::MAX)
        .png_set_strip_to_8bit(true);
    let mut decoder =
        zune_png::PngDecoder::new_with_options(ZCursor::new(encoded.as_slice()), options);
    decoder
        .decode_headers()
        .map_err(|e| anyhow::anyhow!("png header decode failed: {}", e))?;
    let (source_width, source_height) = decoder
        .dimensions()
        .ok_or_else(|| anyhow::anyhow!("png dimensions missing after header decode"))?;
    let source_width =
        u32::try_from(source_width).map_err(|_| anyhow::anyhow!("png width is too large"))?;
    let source_height =
        u32::try_from(source_height).map_err(|_| anyhow::anyhow!("png height is too large"))?;
    let colorspace = decoder
        .colorspace()
        .ok_or_else(|| anyhow::anyhow!("png colorspace missing after header decode"))?;
    let decoded = decoder
        .decode_raw()
        .map_err(|e| anyhow::anyhow!("png decode failed: {}", e))?;

    let pixels = match colorspace {
        ColorSpace::RGB => DecodedSourcePixels::Rgb(decoded.into()),
        ColorSpace::RGBA => DecodedSourcePixels::Rgba(decoded.into()),
        ColorSpace::Luma => DecodedSourcePixels::Luma(decoded.into()),
        ColorSpace::LumaA => DecodedSourcePixels::LumaA(decoded.into()),
        other => {
            return Err(anyhow::anyhow!(
                "unsupported fast png colorspace {:?} for {}",
                other,
                path.display()
            ));
        }
    };

    Ok(DecodedSourceImage {
        pixels,
        width: source_width,
        height: source_height,
        format: "png-fast".to_string(),
        decode: decode_start.elapsed(),
        convert: Duration::ZERO,
    })
}

fn decode_source_generic(
    path: &Path,
    format: Option<image::ImageFormat>,
) -> anyhow::Result<DecodedSourceImage> {
    let decode_start = Instant::now();
    let image = image::open(path)?;
    let decode_duration = decode_start.elapsed();
    let source_width = image.width();
    let source_height = image.height();

    if image.has_alpha() {
        let convert_start = Instant::now();
        let rgba = image.into_rgba8().into_raw();
        return Ok(DecodedSourceImage {
            pixels: DecodedSourcePixels::Rgba(rgba.into()),
            width: source_width,
            height: source_height,
            format: image_format_label(format, false),
            decode: decode_duration,
            convert: convert_start.elapsed(),
        });
    }

    let convert_start = Instant::now();
    let rgb = image.into_rgb8().into_raw();
    Ok(DecodedSourceImage {
        pixels: DecodedSourcePixels::Rgb(rgb.into()),
        width: source_width,
        height: source_height,
        format: image_format_label(format, false),
        decode: decode_duration,
        convert: convert_start.elapsed(),
    })
}

fn decode_source_image(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    let format = image::ImageFormat::from_path(path).ok();
    match format {
        Some(image::ImageFormat::Jpeg) => match decode_jpeg_source_fast(path) {
            Ok(source) => Ok(source),
            Err(e) => {
                warn!(
                    "[ASSET] {}: Fast JPEG decode failed, falling back to generic image path: {}",
                    path.display(),
                    e
                );
                decode_source_generic(path, format)
            }
        },
        Some(image::ImageFormat::Png) => match decode_png_source_fast(path) {
            Ok(source) => Ok(source),
            Err(e) => {
                warn!(
                    "[ASSET] {}: Fast PNG decode failed, falling back to generic image path: {}",
                    path.display(),
                    e
                );
                decode_source_generic(path, format)
            }
        },
        _ => decode_source_generic(path, format),
    }
}

fn prepare_source_image_for_output(
    source: &DecodedSourceImage,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    let (data, width, height, resize_duration, expand_duration, resize_filter) =
        match &source.pixels {
            DecodedSourcePixels::Rgb(pixels) => prepare_rgb_image(
                pixels,
                source.width,
                source.height,
                target_width,
                target_height,
            )?,
            DecodedSourcePixels::Rgba(pixels) => {
                let (prepared, width, height, resize_duration, resize_filter) = prepare_rgba_image(
                    pixels,
                    source.width,
                    source.height,
                    target_width,
                    target_height,
                )?;
                (
                    prepared,
                    width,
                    height,
                    resize_duration,
                    Duration::ZERO,
                    resize_filter,
                )
            }
            DecodedSourcePixels::Luma(pixels) => prepare_luma_image(
                pixels,
                source.width,
                source.height,
                target_width,
                target_height,
            )?,
            DecodedSourcePixels::LumaA(pixels) => prepare_lumaa_image(
                pixels,
                source.width,
                source.height,
                target_width,
                target_height,
            )?,
        };

    Ok(DecodedImagePayload {
        data: data.into(),
        width,
        height,
        profile: ImageLoadProfile {
            format: source.format.clone(),
            source_width: source.width,
            source_height: source.height,
            permit_wait: Duration::ZERO,
            decode: source.decode,
            convert: source.convert,
            resize: resize_duration,
            expand: expand_duration,
            resize_filter,
        },
    })
}

fn prepare_image_for_output_uncached(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    let source = decode_source_image(path)?;
    prepare_source_image_for_output(&source, target_width, target_height)
}

async fn wait_for_shared_result<T>(state: Arc<InFlightSharedResult<T>>) -> Result<Arc<T>, String> {
    loop {
        if let Some(result) = state.result.lock().clone() {
            return result;
        }
        state.notify.notified().await;
    }
}

fn publish_shared_result<T>(state: &Arc<InFlightSharedResult<T>>, result: Result<Arc<T>, String>) {
    *state.result.lock() = Some(result);
    state.notify.notify_waiters();
}

async fn request_decoded_source_image(
    path: &Path,
    identity: ImageSourceIdentity,
    work_kind: BackgroundWorkKind,
) -> anyhow::Result<Arc<DecodedSourceImage>> {
    if let Some(source) = try_load_decoded_source_memory(&identity) {
        return Ok(source);
    }

    let (state, leader) = {
        let mut in_flight = SOURCE_IMAGE_IN_FLIGHT.lock();
        if let Some(existing) = in_flight.get(&identity) {
            (existing.clone(), false)
        } else {
            let state = Arc::new(InFlightSharedResult::default());
            in_flight.insert(identity.clone(), state.clone());
            (state, true)
        }
    };

    if !leader {
        return wait_for_shared_result(state)
            .await
            .map_err(|e| anyhow::anyhow!(e));
    }

    let _permit = acquire_image_work_permit(work_kind, "decode").await?;
    let decode_path = path.to_path_buf();
    let Some(handle) =
        background::spawn_blocking_tracked(work_kind, move || decode_source_image(&decode_path))
    else {
        let msg = "image source decode skipped because shutdown is in progress".to_string();
        publish_shared_result(&state, Err(msg.clone()));
        SOURCE_IMAGE_IN_FLIGHT.lock().remove(&identity);
        return Err(anyhow::anyhow!(msg));
    };

    let result = match handle.await {
        Ok(Ok(source)) => Ok(Arc::new(source)),
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(format!("image source decode task panicked: {}", e)),
    };

    publish_shared_result(&state, result.clone());
    SOURCE_IMAGE_IN_FLIGHT.lock().remove(&identity);

    match result {
        Ok(source) => {
            let descriptor = Arc::new(ImageSourceDescriptor::from_decoded_source(
                identity.clone(),
                &source,
            ));
            store_decoded_source_memory(identity, source.clone());
            store_source_descriptor_memory(descriptor);
            Ok(source)
        }
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

async fn request_prepared_image_payload(
    path: &Path,
    target_width: u32,
    target_height: u32,
    work_kind: BackgroundWorkKind,
) -> anyhow::Result<DecodedImagePayload> {
    let Some(descriptor) = load_image_source_descriptor(path) else {
        let fallback_path = path.to_path_buf();
        let Some(handle) = background::spawn_blocking_tracked(work_kind, move || {
            prepare_image_for_output_uncached(&fallback_path, target_width, target_height)
        }) else {
            return Err(anyhow::anyhow!(
                "image prepare skipped because shutdown is in progress"
            ));
        };
        return handle
            .await
            .map_err(|e| anyhow::anyhow!("image prepare task panicked: {}", e))?;
    };

    let (prepared_width, prepared_height) =
        prepared_target_dimensions_from_descriptor(&descriptor, target_width, target_height);
    let key = prepared_image_key_for_identity(
        descriptor.identity.clone(),
        prepared_width,
        prepared_height,
    );

    if let Some(payload) = try_load_prepared_image_memory(&key) {
        return Ok(payload);
    }
    if let Some(payload) = try_load_compatible_prepared_image_memory(
        &descriptor.identity,
        prepared_width,
        prepared_height,
    ) {
        return Ok(payload);
    }

    if let Some(state) = find_compatible_prepared_in_flight_state(
        &descriptor.identity,
        prepared_width,
        prepared_height,
    ) {
        let entry = wait_for_shared_result(state)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        return Ok(entry.to_payload("prepared-shared-compatible"));
    }

    let (state, leader) = {
        let mut in_flight = PREPARED_IMAGE_IN_FLIGHT.lock();
        if let Some(existing) = in_flight.get(&key) {
            (existing.clone(), false)
        } else {
            let state = Arc::new(InFlightSharedResult::default());
            in_flight.insert(key.clone(), state.clone());
            (state, true)
        }
    };

    if !leader {
        let entry = wait_for_shared_result(state)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        return Ok(entry.to_payload("prepared-shared"));
    }

    if let Some(payload) = try_load_prepared_image_cache_by_key(&key) {
        store_prepared_image_memory(key.clone(), &payload);
        publish_shared_result(
            &state,
            Ok(Arc::new(PreparedImageEntry::from_payload(&payload))),
        );
        PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
        return Ok(payload);
    }

    let source = request_decoded_source_image(path, descriptor.identity.clone(), work_kind).await?;
    let _permit = acquire_image_work_permit(work_kind, "prepare").await?;
    let source_for_prepare = source.clone();
    let Some(handle) = background::spawn_blocking_tracked(work_kind, move || {
        prepare_source_image_for_output(&source_for_prepare, target_width, target_height)
    }) else {
        let msg = "image prepare skipped because shutdown is in progress".to_string();
        publish_shared_result(&state, Err(msg.clone()));
        PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
        return Err(anyhow::anyhow!(msg));
    };

    let payload = match handle.await {
        Ok(Ok(payload)) => payload,
        Ok(Err(e)) => {
            let msg = e.to_string();
            publish_shared_result(&state, Err(msg.clone()));
            PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
            return Err(anyhow::anyhow!(msg));
        }
        Err(e) => {
            let msg = format!("image prepare task panicked: {}", e);
            publish_shared_result(&state, Err(msg.clone()));
            PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
            return Err(anyhow::anyhow!(msg));
        }
    };

    store_prepared_image_cache_by_key(&key, &payload);
    store_prepared_image_memory(key.clone(), &payload);
    publish_shared_result(
        &state,
        Ok(Arc::new(PreparedImageEntry::from_payload(&payload))),
    );
    PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
    Ok(payload)
}

fn begin_image_prefetch_generation(name: &str) -> u64 {
    let mut generations = IMAGE_PREFETCH_GENERATIONS.lock();
    let next_generation = generations
        .get(name)
        .copied()
        .unwrap_or(0)
        .saturating_add(1);
    generations.insert(name.to_string(), next_generation);
    next_generation
}

fn image_prefetch_generation_matches(name: &str, generation: u64) -> bool {
    IMAGE_PREFETCH_GENERATIONS
        .lock()
        .get(name)
        .copied()
        .unwrap_or(0)
        == generation
}

fn collect_output_prefetch_candidates(
    orchestrator: &monitor_manager::OutputOrchestrator,
) -> Vec<(PathBuf, &'static str)> {
    let mut candidates = Vec::new();
    let mut seen_paths = HashSet::new();

    if orchestrator.next_content_type == Some(queue::ContentType::Image) {
        if let Some(next_path) = orchestrator.next_path.as_ref() {
            if seen_paths.insert(next_path.clone()) {
                candidates.push((next_path.clone(), "next"));
            }
        }
    }

    if let Some(queue) = orchestrator.queue.as_ref() {
        for path in queue.peek_upcoming_images(IMAGE_PREFETCH_LOOKAHEAD_IMAGES) {
            if seen_paths.insert(path.clone()) {
                candidates.push((path, "lookahead"));
            }
            if candidates.len() >= IMAGE_PREFETCH_LOOKAHEAD_IMAGES {
                break;
            }
        }
    }

    candidates
}

fn push_image_prefetch_request(
    requests: &mut Vec<ImagePrefetchRequest>,
    seen_keys: &mut HashSet<(PathBuf, u32, u32)>,
    target_output: &str,
    path: &Path,
    target_width: u32,
    target_height: u32,
    reason: &'static str,
) {
    let request_key = (path.to_path_buf(), target_width, target_height);
    if !seen_keys.insert(request_key) {
        return;
    }

    requests.push(ImagePrefetchRequest {
        target_output: target_output.to_string(),
        path: path.to_path_buf(),
        target_width,
        target_height,
        reason,
    });
}

fn build_image_prefetch_plan(
    monitor_manager: &monitor_manager::MonitorManager,
    renderers: &HashMap<String, renderer::Renderer>,
    trigger_output: &str,
) -> Vec<ImagePrefetchRequest> {
    let Some(orchestrator) = monitor_manager.outputs.get(trigger_output) else {
        return Vec::new();
    };
    let Some(renderer) = renderers.get(trigger_output) else {
        return Vec::new();
    };

    let candidates = collect_output_prefetch_candidates(orchestrator);
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut requests = Vec::new();
    let mut seen_keys = HashSet::new();
    for (path, reason) in &candidates {
        push_image_prefetch_request(
            &mut requests,
            &mut seen_keys,
            trigger_output,
            path,
            renderer.config.width,
            renderer.config.height,
            reason,
        );
    }

    let candidate_paths: HashSet<PathBuf> =
        candidates.into_iter().map(|(path, _reason)| path).collect();

    for (output_name, other_orchestrator) in &monitor_manager.outputs {
        if requests.len() >= IMAGE_PREFETCH_MAX_REQUESTS {
            break;
        }
        if output_name == trigger_output
            || other_orchestrator.next_content_type != Some(queue::ContentType::Image)
        {
            continue;
        }

        let Some(next_path) = other_orchestrator.next_path.as_ref() else {
            continue;
        };
        if !candidate_paths.contains(next_path) {
            continue;
        }

        let Some(other_renderer) = renderers.get(output_name) else {
            continue;
        };

        push_image_prefetch_request(
            &mut requests,
            &mut seen_keys,
            output_name,
            next_path,
            other_renderer.config.width,
            other_renderer.config.height,
            "shared-next",
        );
    }

    requests.truncate(IMAGE_PREFETCH_MAX_REQUESTS);
    requests.sort_by(|left, right| {
        let left_priority = match left.reason {
            "next" | "shared-next" => 0u8,
            _ => 1u8,
        };
        let right_priority = match right.reason {
            "next" | "shared-next" => 0u8,
            _ => 1u8,
        };
        let left_area = u64::from(left.target_width) * u64::from(left.target_height);
        let right_area = u64::from(right.target_width) * u64::from(right.target_height);

        left_priority
            .cmp(&right_priority)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| right_area.cmp(&left_area))
            .then_with(|| left.target_output.cmp(&right.target_output))
    });
    requests
}

fn schedule_image_prefetch_plan(
    trigger_output: &str,
    generation: u64,
    requests: Vec<ImagePrefetchRequest>,
) {
    if requests.is_empty() || !background::is_accepting_new_work() {
        return;
    }

    let output_name = trigger_output.to_string();
    tokio::spawn(async move {
        for request in requests {
            if !background::is_accepting_new_work() {
                return;
            }
            if !image_prefetch_generation_matches(&output_name, generation) {
                debug!(
                    "[PREFETCH] {}: Aborting superseded prefetch plan generation {}",
                    output_name, generation
                );
                return;
            }

            let prefetch_start = Instant::now();
            match request_prepared_image_payload(
                &request.path,
                request.target_width,
                request.target_height,
                BackgroundWorkKind::ImagePrefetch,
            )
            .await
            {
                Ok(payload) => debug!(
                    "[PREFETCH] {} -> {}: Warmed {} ({}) as {} {}x{} in {:.1}ms",
                    output_name,
                    request.target_output,
                    request.path.display(),
                    request.reason,
                    payload.profile.format,
                    payload.width,
                    payload.height,
                    duration_ms(prefetch_start.elapsed())
                ),
                Err(e) => debug!(
                    "[PREFETCH] {} -> {}: Failed to warm {} ({}): {}",
                    output_name,
                    request.target_output,
                    request.path.display(),
                    request.reason,
                    e
                ),
            }
        }
    });
}

fn resolve_transition_for_output(
    monitor_manager: &monitor_manager::MonitorManager,
    name: &str,
) -> Transition {
    monitor_manager
        .outputs
        .get(name)
        .map(|orchestrator| {
            if matches!(orchestrator.config.transition, Transition::Random) {
                let picked = crate::shaders::ShaderManager::pick_random_transition();
                debug!(
                    "[TRANSITION] {}: Resolved Random transition to: {}",
                    name,
                    picked.name()
                );
                picked
            } else {
                orchestrator.config.transition.clone()
            }
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoPolicyMode {
    DirectOnly,
    DirectPreferred,
    FallbackSafe,
}

fn current_video_policy_mode() -> VideoPolicyMode {
    match std::env::var("KLD_VIDEO_POLICY_MODE")
        .ok()
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("direct-only") => VideoPolicyMode::DirectOnly,
        Some("fallback-safe") => VideoPolicyMode::FallbackSafe,
        _ => VideoPolicyMode::DirectPreferred,
    }
}

/// Helper function to switch wallpaper content for an output.
#[allow(clippy::too_many_arguments)]
pub fn switch_wallpaper_content(
    name: &str,
    path: &Path,
    content_type: queue::ContentType,
    metrics: &Arc<metrics::PerformanceMetrics>,
    next_session_id: &mut u64,
    frame_mailbox: &video::LatestFrameMailbox,
    monitor_manager: &monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
    pending_video_switches: &mut HashMap<String, PendingVideoSwitch>,
    pending_image_video_stops: &mut HashMap<String, video::VideoPlayer>,
    pending_direct_takeovers: &mut HashMap<String, PendingDirectTakeover>,
    pending_direct_handoffs: &mut HashMap<String, u64>,
    blocked_direct_handoff_sessions: &mut HashMap<String, u64>,
    pending_video_sessions: &PendingVideoSessions,
    batch_id: Option<u64>,
    batch_trigger_time: Option<std::time::Instant>,
    image_tx: &tokio::sync::mpsc::Sender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    player_event_tx: &tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
    native_video_target: Option<video::NativeWaylandVideoTarget>,
    shutdown_flag: &Arc<AtomicBool>,
    log_prefix: &str,
) {
    info!("{}: {} -> {:?}", log_prefix, name, path.display());
    debug!(
        "[SWITCH] {}: content_type={:?}, renderer exists={}",
        name,
        content_type,
        renderers.contains_key(name)
    );

    let session_id = *next_session_id;
    *next_session_id += 1;

    frame_mailbox.clear_source(name);
    pending_direct_handoffs.remove(name);
    blocked_direct_handoff_sessions.remove(name);
    if let Some(mut pending_takeover) = pending_direct_takeovers.remove(name) {
        let _ = pending_takeover.player.stop();
    }
    if let Some(old_pending_stop) = pending_image_video_stops.remove(name) {
        stop_video_player_in_background(name.to_string(), old_pending_stop);
    }
    let mut native_video_target = native_video_target;
    let mut should_prepare_video = false;
    let mut backend_request = video::VideoBackendRequest::Auto;
    let mut broker_start_position_ns = None;
    if let Some(r) = renderers.get_mut(name) {
        let resolved_transition = resolve_transition_for_output(monitor_manager, name);
        native_video_target =
            native_video_target.map(|target| target.with_size(r.config.width, r.config.height));
        r.active_batch_id = batch_id;
        r.batch_start_time = batch_trigger_time;
        r.active_transition = resolved_transition.clone();

        if content_type == queue::ContentType::Image {
            let mut prior_video_player = video_players.remove(name);
            pending_video_switches.remove(name);
            set_pending_video_session(pending_video_sessions, name, None);
            r.set_content_type(content_type);
            r.active_image_session_id = session_id;
            r.active_video_session_id = 0;
            r.switch_content();
            let target_width = r.config.width;
            let target_height = r.config.height;

            let name_clone = name.to_string();
            let path_clone = path.to_path_buf();
            let tx = image_tx.clone();
            let image_session_id = session_id;
            let shutdown_flag = shutdown_flag.clone();
            if let Some(old_video_player) = prior_video_player.take() {
                if old_video_player.is_direct_surface_backend() {
                    stop_video_player_in_background(name.to_string(), old_video_player);
                } else {
                    pending_image_video_stops.insert(name.to_string(), old_video_player);
                }
            }

            debug!(
                "[ASSET] {}: Offloading image decode: {}",
                name,
                path.display()
            );
            tokio::spawn(async move {
                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Skipping image decode because shutdown is in progress",
                        name_clone
                    );
                    return;
                }

                let request_start = Instant::now();
                let decode_result = request_prepared_image_payload(
                    &path_clone,
                    target_width,
                    target_height,
                    BackgroundWorkKind::ImageDecode,
                )
                .await;

                if shutdown_flag.load(Ordering::SeqCst) || !background::is_accepting_new_work() {
                    debug!(
                        "[ASSET] {}: Discarding decoded image because shutdown is in progress",
                        name_clone
                    );
                    return;
                }

                // Send decoded image (or error) to channel
                match decode_result {
                    Ok(mut payload) => {
                        let observed_total = request_start.elapsed();
                        payload.profile.permit_wait =
                            observed_total.saturating_sub(payload.profile.cpu_duration());
                        if let Err(e) = tx
                            .send(LoadedImage {
                                name: name_clone.clone(),
                                session_id: image_session_id,
                                data: Some(payload.data),
                                width: payload.width,
                                height: payload.height,
                                profile: Some(payload.profile),
                                _path: path_clone,
                            })
                            .await
                        {
                            debug!(
                                "[ASSET] {}: Failed to send decoded image (channel closed): {}",
                                name_clone, e
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to decode image {}: {}", path_clone.display(), e);
                        let _ = tx
                            .send(LoadedImage {
                                name: name_clone,
                                session_id: image_session_id,
                                data: None,
                                width: 0,
                                height: 0,
                                profile: None,
                                _path: path_clone,
                            })
                            .await;
                    }
                }
            });
        } else {
            let policy_mode = current_video_policy_mode();
            if native_video_target.is_some() {
                backend_request = match policy_mode {
                    VideoPolicyMode::DirectOnly | VideoPolicyMode::DirectPreferred => {
                        video::VideoBackendRequest::ForceWaylandDirect
                    }
                    VideoPolicyMode::FallbackSafe => video::VideoBackendRequest::ForceAppsink,
                };
            } else if policy_mode == VideoPolicyMode::DirectOnly {
                warn!(
                    "[VIDEO] {}: direct-only policy requested but native target unavailable; using appsink fallback",
                    name
                );
                metrics.record_direct_fallback_reason("sink_unavailable");
            }
            if let Some((peer_name, peer_player)) = video_players
                .iter()
                .find(|(output, player)| {
                    output.as_str() != name
                        && monitor_manager
                            .outputs
                            .get(output.as_str())
                            .and_then(|o| o.current_path.as_deref())
                            .is_some_and(|p| p == path)
                        && player.current_position_ns().is_some()
                })
                .map(|(output, player)| (output.clone(), player))
            {
                broker_start_position_ns = peer_player.current_position_ns();
                metrics.record_shared_broker_hit();
                debug!(
                    "[VIDEO] {}: Shared broker prototype hit via peer {} (start_position_ms={:.1})",
                    name,
                    peer_name,
                    broker_start_position_ns.unwrap_or(0) as f64 / 1_000_000.0
                );
            } else {
                metrics.record_shared_broker_miss();
            }
            if video_players
                .get(name)
                .is_some_and(video::VideoPlayer::is_direct_surface_backend)
                && let Some(old_video_player) = video_players.remove(name)
            {
                stop_video_player_in_background(name.to_string(), old_video_player);
            }
            set_pending_video_session(pending_video_sessions, name, Some(session_id));
            pending_video_switches.insert(
                name.to_string(),
                PendingVideoSwitch {
                    session_id,
                    batch_id,
                    batch_trigger_time,
                    transition: resolved_transition,
                },
            );
            should_prepare_video = true;
        }
    } else {
        set_pending_video_session(pending_video_sessions, name, None);
        pending_video_switches.remove(name);
        if let Some(vp) = video_players.remove(name) {
            stop_video_player_in_background(name.to_string(), vp);
        }
        warn!(
            "[SWITCH] {}: Skipping content switch because renderer no longer exists",
            name
        );
    }

    if content_type == queue::ContentType::Video && should_prepare_video {
        debug!(
            "[TRANSITION] {}: Preparing deferred video player (session_id={})",
            name, session_id
        );
        create_and_start_video_player(
            path,
            name,
            session_id,
            monitor_manager
                .outputs
                .get(name)
                .map(|o| o.config.volume as f64 / 100.0)
                .unwrap_or_else(|| {
                    warn!(
                        "[VIDEO] {}: Missing output config while creating player; defaulting volume to 0",
                        name
                    );
                    0.0
                }),
            frame_mailbox,
            player_tx,
            player_event_tx,
            native_video_target,
            pending_video_sessions.clone(),
            shutdown_flag.clone(),
            backend_request,
            broker_start_position_ns,
        );
    }

    let prefetch_generation = begin_image_prefetch_generation(name);
    let prefetch_plan = build_image_prefetch_plan(monitor_manager, renderers, name);
    schedule_image_prefetch_plan(name, prefetch_generation, prefetch_plan);
}

fn create_and_start_video_player(
    path: &Path,
    name: &str,
    session_id: u64,
    volume: f64,
    frame_mailbox: &video::LatestFrameMailbox,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    player_event_tx: &tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
    native_video_target: Option<video::NativeWaylandVideoTarget>,
    pending_video_sessions: PendingVideoSessions,
    shutdown_flag: Arc<AtomicBool>,
    backend_request: video::VideoBackendRequest,
    start_position_ns: Option<u64>,
) {
    let path_str = path.to_string_lossy().into_owned();
    let name_arc = Arc::new(name.to_string());
    let name_str = name.to_string();
    let frame_mailbox_clone = frame_mailbox.clone();
    let player_tx_clone = player_tx.clone();
    let player_event_tx_clone = player_event_tx.clone();
    let Some(handle) = background::spawn_blocking_tracked(
        BackgroundWorkKind::VideoPrepare,
        move || {
            let name_for_panic = name_str.clone();
            let player_tx_panic = player_tx_clone.clone();
            let session_id_panic = session_id;
            let pending_video_sessions_for_task = pending_video_sessions.clone();
            let should_abort = || {
                shutdown_flag.load(Ordering::SeqCst)
                    || !pending_video_session_matches(
                        &pending_video_sessions_for_task,
                        &name_str,
                        session_id,
                    )
            };

            if should_abort() {
                debug!(
                    "[VIDEO] {}: Skipping superseded video prepare task for session {} before player creation",
                    name_str, session_id
                );
                return;
            }

            let prepare_start = Instant::now();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match video::VideoPlayer::new(
                    &path_str,
                    name_arc,
                    session_id,
                    volume,
                    frame_mailbox_clone,
                    player_event_tx_clone,
                    backend_request,
                    native_video_target.clone(),
                ) {
                    Ok(mut vp) => {
                        let create_duration = prepare_start.elapsed();
                        vp.set_volume(volume);
                        if should_abort() {
                            let _ = vp.stop();
                            return Ok(None);
                        }
                        let prebuffer_start = Instant::now();
                        let prebuffer = match vp.prebuffer(should_abort) {
                            Ok(result) => result,
                            Err(e) => {
                                if should_abort() {
                                    debug!(
                                        "[VIDEO] {}: Aborting pre-buffer for superseded/shutdown session {}",
                                        name_str, session_id
                                    );
                                    let _ = vp.stop();
                                    return Ok(None);
                                }
                                debug!(
                                    "[VIDEO] {}: Pre-buffering failed (non-fatal): {}",
                                    name_str, e
                                );
                                video::VideoPrebufferResult {
                                    frame: None,
                                    profile: video::VideoPrebufferProfile {
                                        set_state: Duration::ZERO,
                                        state_wait: Duration::ZERO,
                                        pull_preroll: Duration::ZERO,
                                        set_state_result: "error",
                                        state_wait_settled: false,
                                        current_state: gst::State::Null,
                                        pending_state: gst::State::VoidPending,
                                    },
                                }
                            }
                        };
                        let prebuffer_duration = prebuffer_start.elapsed();
                        if vp.is_direct_surface_backend()
                            && let Some(position_ns) = start_position_ns.filter(|pos| *pos > 0)
                        {
                            vp.set_start_position_ns(position_ns);
                        }
                        debug!(
                            "[VIDEO] {}: Player prepared in {:.1}ms (create {:.1}ms + prebuffer {:.1}ms, set_state {:.1}ms/{} + wait_state {:.1}ms settled={} current={:?} pending={:?} + pull_preroll {:.1}ms, preroll_frame={})",
                            name_str,
                            duration_ms(prepare_start.elapsed()),
                            duration_ms(create_duration),
                            duration_ms(prebuffer_duration),
                            duration_ms(prebuffer.profile.set_state),
                            prebuffer.profile.set_state_result,
                            duration_ms(prebuffer.profile.state_wait),
                            prebuffer.profile.state_wait_settled,
                            prebuffer.profile.current_state,
                            prebuffer.profile.pending_state,
                            duration_ms(prebuffer.profile.pull_preroll),
                            prebuffer.frame.is_some()
                        );
                        if should_abort() {
                            let _ = vp.stop();
                            Ok(None)
                        } else {
                            Ok(Some((vp, prebuffer.frame)))
                        }
                    }
                    Err(e) => {
                        error!("[VIDEO] {}: Failed to create video player: {}", name_str, e);
                        Err(e)
                    }
                }
            }));

            match result {
                Ok(Ok(Some((mut vp, preroll_frame)))) => {
                    if shutdown_flag.load(Ordering::SeqCst)
                        || !pending_video_session_matches(
                            &pending_video_sessions,
                            &name_str,
                            session_id,
                        )
                    {
                        debug!(
                            "[VIDEO] {}: Discarding superseded prepared player for session {}",
                            name_str, session_id
                        );
                        let _ = vp.stop();
                        return;
                    }
                    if let Err(e) = player_tx_clone.send(VideoPlayerResult::Success(
                        name_str,
                        session_id,
                        vp,
                        preroll_frame,
                    )) {
                        error!("[VIDEO] Failed to send video player back: {}", e);
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(_)) | Err(_) => {
                    if shutdown_flag.load(Ordering::SeqCst) {
                        return;
                    }
                    if result.is_err() {
                        error!("[VIDEO] {}: Video player task panicked!", name_for_panic);
                    }
                    let _ = player_tx_panic
                        .send(VideoPlayerResult::Failure(name_for_panic, session_id_panic));
                }
            }
        },
    ) else {
        debug!(
            "[VIDEO] {}: Skipping video prepare task because shutdown is in progress",
            name
        );
        return;
    };
    drop(handle);
}

/// Handle an IPC command request.
#[allow(clippy::too_many_arguments)]
pub async fn handle_command(
    req: Request,
    monitor_manager: &mut monitor_manager::MonitorManager,
    renderers: &mut HashMap<String, renderer::Renderer>,
    video_players: &mut HashMap<String, video::VideoPlayer>,
    pending_video_switches: &mut HashMap<String, PendingVideoSwitch>,
    pending_image_video_stops: &mut HashMap<String, video::VideoPlayer>,
    pending_direct_takeovers: &mut HashMap<String, PendingDirectTakeover>,
    pending_direct_handoffs: &mut HashMap<String, u64>,
    blocked_direct_handoff_sessions: &mut HashMap<String, u64>,
    direct_handoff_cooldown_until: &mut HashMap<String, Instant>,
    direct_handoff_failure_streak: &mut HashMap<String, u32>,
    pending_video_sessions: &PendingVideoSessions,
    metrics: &Arc<metrics::PerformanceMetrics>,
    frame_mailbox: &video::LatestFrameMailbox,
    image_tx: &tokio::sync::mpsc::Sender<LoadedImage>,
    player_tx: &tokio::sync::mpsc::UnboundedSender<VideoPlayerResult>,
    player_event_tx: &tokio::sync::mpsc::UnboundedSender<PlayerEventMsg>,
    native_video_targets: &HashMap<String, video::NativeWaylandVideoTarget>,
    next_session_id: &mut u64,
    loop_start: Instant,
    shutdown_flag: &Arc<AtomicBool>,
) -> Response {
    match req {
        Request::QueryOutputs => {
            let outputs = renderers
                .iter()
                .map(|(n, r)| kaleidux_common::OutputInfo {
                    name: n.clone(),
                    width: r.config.width,
                    height: r.config.height,
                    current_wallpaper: monitor_manager
                        .outputs
                        .get(n)
                        .and_then(|o| o.current_path.as_ref().map(|p| p.display().to_string())),
                })
                .collect();
            Response::OutputInfo(outputs)
        }
        Request::Next { output } => {
            let changes = monitor_manager.handle_next(output);
            let batch = rand::random::<u64>();
            let ordered_changes = ordered_pending_content_switches(renderers, changes);
            for change in ordered_changes {
                switch_wallpaper_content(
                    &change.name,
                    &change.path,
                    change.content_type,
                    metrics,
                    next_session_id,
                    frame_mailbox,
                    monitor_manager,
                    renderers,
                    video_players,
                    pending_video_switches,
                    pending_image_video_stops,
                    pending_direct_takeovers,
                    pending_direct_handoffs,
                    blocked_direct_handoff_sessions,
                    pending_video_sessions,
                    Some(batch),
                    Some(loop_start),
                    image_tx,
                    player_tx,
                    player_event_tx,
                    native_video_targets.get(&change.name).cloned(),
                    shutdown_flag,
                    "NEXT",
                );
            }
            Response::Ok
        }
        Request::Prev { output } => {
            let changes = monitor_manager.handle_prev(output);
            let batch = rand::random::<u64>();
            let ordered_changes = ordered_pending_content_switches(renderers, changes);
            for change in ordered_changes {
                switch_wallpaper_content(
                    &change.name,
                    &change.path,
                    change.content_type,
                    metrics,
                    next_session_id,
                    frame_mailbox,
                    monitor_manager,
                    renderers,
                    video_players,
                    pending_video_switches,
                    pending_image_video_stops,
                    pending_direct_takeovers,
                    pending_direct_handoffs,
                    blocked_direct_handoff_sessions,
                    pending_video_sessions,
                    Some(batch),
                    Some(loop_start),
                    image_tx,
                    player_tx,
                    player_event_tx,
                    native_video_targets.get(&change.name).cloned(),
                    shutdown_flag,
                    "PREV",
                );
            }
            Response::Ok
        }
        Request::Kill => {
            shutdown_flag.store(true, Ordering::SeqCst);
            Response::Ok
        }
        Request::Playlist(cmd) => monitor_manager.handle_playlist_command(cmd),
        Request::Blacklist(cmd) => monitor_manager.handle_blacklist_command(cmd),
        Request::LoveitList => Response::LoveitList(monitor_manager.get_loveitlist()),
        Request::Love { path, multiplier } => monitor_manager
            .love_file(path, multiplier)
            .map(|_| Response::Ok)
            .unwrap_or_else(|e| Response::Error(e.to_string())),
        Request::Unlove { path } => monitor_manager
            .unlove_file(path)
            .map(|_| Response::Ok)
            .unwrap_or_else(|e| Response::Error(e.to_string())),
        Request::History { output } => Response::History(monitor_manager.get_history(output)),
        Request::Reload => {
            info!("Reloading configuration...");
            match orchestration::Config::load().await {
                Ok(new_config) => {
                    monitor_manager.update_config(new_config);
                    for (name, r) in renderers.iter_mut() {
                        if let Some(cfg) = monitor_manager.get_output_config(name) {
                            r.apply_config(cfg);
                        }
                    }
                    info!("Configuration reloaded successfully");
                    Response::Ok
                }
                Err(e) => {
                    error!("Failed to reload config: {}", e);
                    Response::Error(format!("Failed to reload config: {}", e))
                }
            }
        }
        Request::Pause => {
            info!("[CMD] Pausing all video players and wallpaper cycling");
            for (name, player) in video_players.iter() {
                if let Err(e) = player.pause() {
                    error!("[CMD] Failed to pause video for {}: {}", name, e);
                }
            }
            monitor_manager.set_paused(true);
            Response::Ok
        }
        Request::Resume => {
            info!("[CMD] Resuming all video players and wallpaper cycling");
            for (name, player) in video_players.iter() {
                if let Err(e) = player.resume() {
                    error!("[CMD] Failed to resume video for {}: {}", name, e);
                }
            }
            monitor_manager.set_paused(false);
            Response::Ok
        }
        Request::Stop => {
            info!("[CMD] Stopping all video players");
            let names: Vec<String> = video_players.keys().cloned().collect();
            for name in names {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                pending_direct_handoffs.remove(&name);
                blocked_direct_handoff_sessions.remove(&name);
                direct_handoff_cooldown_until.remove(&name);
                direct_handoff_failure_streak.remove(&name);
                frame_mailbox.clear_source(&name);
                if let Some(player) = video_players.remove(&name) {
                    stop_video_player_in_background(name, player);
                }
            }
            Response::Ok
        }
        Request::Clear { output } => {
            info!("[CMD] Clearing output: {:?}", output);
            let targets: Vec<String> = match output {
                Some(ref name) => {
                    if renderers.contains_key(name) {
                        vec![name.clone()]
                    } else {
                        return Response::Error(format!("Output not found: {}", name));
                    }
                }
                None => renderers.keys().cloned().collect(),
            };
            for name in targets {
                set_pending_video_session(pending_video_sessions, &name, None);
                pending_video_switches.remove(&name);
                pending_direct_handoffs.remove(&name);
                blocked_direct_handoff_sessions.remove(&name);
                direct_handoff_cooldown_until.remove(&name);
                direct_handoff_failure_streak.remove(&name);
                frame_mailbox.clear_source(&name);
                if let Some(vp) = video_players.remove(&name) {
                    stop_video_player_in_background(name.clone(), vp);
                }
                if let Some(r) = renderers.get_mut(&name) {
                    r.clear();
                }
            }
            Response::Ok
        }
    }
}
