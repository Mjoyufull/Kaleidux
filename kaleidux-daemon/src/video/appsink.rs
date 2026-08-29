use gst::prelude::*;
use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tracing::{info, warn};

use crate::metrics::PerformanceMetrics;
use crate::observability::video_backend::VideoBackendMetricKind;

use super::appsink_pool::ImportableDmaBufPool;
use super::capabilities::CPU_VIDEO_FALLBACK_WARNED;
use super::dmabuf::{DmaBufDescriptorCache, linear_nv12_fourcc};
use super::{
    LatestFrameMailbox, VideoFrame, VideoFrameFormat, VideoMode, VideoPlayer, chroma_plane_extent,
    current_video_capabilities, get_video_mode, publish_interval_ns, should_abort_appsink_sample,
    should_publish_now,
};

fn color_metadata(
    video_info: &gst_video::VideoInfo,
    caps: &gst::CapsRef,
) -> super::VideoColorMetadata {
    let color = video_info.colorimetry();
    let matrix = match color.matrix() {
        gst_video::VideoColorMatrix::Bt601 => super::VideoColorMatrix::Bt601,
        gst_video::VideoColorMatrix::Bt2020 => super::VideoColorMatrix::Bt2020,
        _ => super::VideoColorMatrix::Bt709,
    };
    let range = match color.range() {
        gst_video::VideoColorRange::Range0_255 => super::VideoColorRange::Full,
        _ => super::VideoColorRange::Limited,
    };
    let transfer = match color.transfer() {
        gst_video::VideoTransferFunction::Smpte2084 => super::VideoTransfer::Pq,
        gst_video::VideoTransferFunction::AribStdB67 => super::VideoTransfer::Hlg,
        gst_video::VideoTransferFunction::Srgb => super::VideoTransfer::Srgb,
        gst_video::VideoTransferFunction::Bt709
        | gst_video::VideoTransferFunction::Bt202010
        | gst_video::VideoTransferFunction::Bt202012
        | gst_video::VideoTransferFunction::Bt601 => super::VideoTransfer::Bt709,
        _ => super::VideoTransfer::Bt1886,
    };
    let primaries = match color.primaries() {
        gst_video::VideoColorPrimaries::Bt2020 => super::VideoColorPrimaries::Bt2020,
        gst_video::VideoColorPrimaries::Bt470bg => super::VideoColorPrimaries::Bt601Pal,
        gst_video::VideoColorPrimaries::Smpte170m => super::VideoColorPrimaries::Bt601Ntsc,
        gst_video::VideoColorPrimaries::Smpteeg432 => super::VideoColorPrimaries::DisplayP3,
        gst_video::VideoColorPrimaries::Smpterp431 => super::VideoColorPrimaries::DciP3,
        _ => super::VideoColorPrimaries::Bt709,
    };
    let chroma_siting = match video_info.chroma_site() {
        site if site.contains(gst_video::VideoChromaSite::H_COSITED)
            && site.contains(gst_video::VideoChromaSite::V_COSITED) =>
        {
            super::VideoChromaSiting::TopLeft
        }
        site if site.contains(gst_video::VideoChromaSite::H_COSITED) => {
            super::VideoChromaSiting::Left
        }
        _ => super::VideoChromaSiting::Center,
    };
    let mastering = gst_video::VideoMasteringDisplayInfo::from_caps(caps)
        .ok()
        .map(|metadata| {
            let primaries = metadata.display_primaries();
            let white = metadata.white_point();
            super::VideoMasteringMetadata {
                primaries_xy: [
                    [primaries[0].x(), primaries[0].y()],
                    [primaries[1].x(), primaries[1].y()],
                    [primaries[2].x(), primaries[2].y()],
                    [white.x(), white.y()],
                ],
                min_luminance_nits: metadata.min_display_mastering_luminance() as f32 / 10_000.0,
                max_luminance_nits: metadata.max_display_mastering_luminance() as f32 / 10_000.0,
            }
        });
    let content_light = gst_video::VideoContentLightLevel::from_caps(caps)
        .map(|metadata| super::VideoContentLightMetadata {
            max_content_light_level: Some(u32::from(metadata.max_content_light_level())),
            max_frame_average_light_level: Some(u32::from(
                metadata.max_frame_average_light_level(),
            )),
        })
        .unwrap_or_default();
    super::VideoColorMetadata {
        matrix,
        range,
        transfer,
        primaries,
        chroma_siting,
        bit_depth: video_info.format_info().depth()[0] as u8,
        mastering,
        content_light,
    }
}

fn frame_geometry(
    video_info: &gst_video::VideoInfo,
    buffer: &gst::BufferRef,
) -> super::VideoGeometry {
    let width = video_info.width();
    let height = video_info.height();
    let mut geometry = super::VideoGeometry::for_dimensions(width, height);
    let par = video_info.par();
    geometry.sample_aspect_num = par.numer().max(1) as u32;
    geometry.sample_aspect_den = par.denom().max(1) as u32;
    if let Some(crop) = buffer.meta::<gst_video::VideoCropMeta>() {
        let (x, y, crop_width, crop_height) = crop.rect();
        geometry.crop = super::VideoCropRect {
            x,
            y,
            width: crop_width.max(1),
            height: crop_height.max(1),
        };
        geometry.display_width = crop_width.max(1);
        geometry.display_height = crop_height.max(1);
    }
    geometry
}
use super::{
    appsink_sync_enabled, cuda_layout_log_every_frame_enabled, prefer_videoinfo_cuda_layout_enabled,
};

fn env_i64_or_default(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(default)
}

pub(super) fn appsink_processing_deadline_ms() -> u64 {
    env_i64_or_default("KLD_APPSINK_PROCESSING_DEADLINE_MS", 20).max(0) as u64
}

pub(super) fn appsink_max_lateness_ms() -> i64 {
    env_i64_or_default("KLD_APPSINK_MAX_LATENESS_MS", -1)
}

pub(super) fn appsink_drop_if_mailbox_pending() -> bool {
    std::env::var("KLD_APPSINK_DROP_IF_MAILBOX_PENDING")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

const DEFAULT_CAPPED_PENDING_REFRESH_MS: i64 = 32;
const DEFAULT_UNCAPPED_PENDING_REFRESH_MS: i64 = 75;

#[derive(Clone)]
struct NegotiatedCaps {
    identity: usize,
    video_info: gst_video::VideoInfo,
    is_cuda: bool,
    drm: Option<(u32, u64)>,
}

#[derive(Default)]
pub(super) struct AppsinkNegotiatedState {
    caps: Option<NegotiatedCaps>,
    dmabuf_descriptors: DmaBufDescriptorCache,
    cuda_layout_logged: bool,
}

impl AppsinkNegotiatedState {
    fn negotiated_caps(&mut self, caps: &gst::CapsRef) -> Result<NegotiatedCaps, gst::FlowError> {
        let identity = caps.as_ptr() as usize;
        if let Some(cached) = self.caps.as_ref()
            && cached.identity == identity
        {
            return Ok(cached.clone());
        }

        let features = caps.features(0);
        let is_cuda = features.is_some_and(|value| value.contains("memory:CUDAMemory"));
        let is_dmabuf = features.is_some_and(|value| value.contains("memory:DMABuf"));
        let raw_info =
            gst_video::VideoInfo::from_caps(caps).map_err(|_| gst::FlowError::NotNegotiated)?;
        let (video_info, drm) = if is_dmabuf && raw_info.format() == gst_video::VideoFormat::DmaDrm
        {
            let drm_info = gst_video::VideoInfoDmaDrm::from_caps(caps)
                .map_err(|_| gst::FlowError::NotNegotiated)?;
            let pair = (drm_info.fourcc(), drm_info.modifier());
            let video_info = drm_info
                .to_video_info()
                .map_err(|_| gst::FlowError::NotNegotiated)?;
            (video_info, Some(pair))
        } else if is_dmabuf {
            // GStreamer's caps rules omit an explicit modifier for linear
            // layouts. Traditional NV12 + memory:DMABuf therefore means
            // DRM_FORMAT_NV12 with DRM_FORMAT_MOD_LINEAR.
            let pair =
                linear_nv12_fourcc(raw_info.format()).ok_or(gst::FlowError::NotNegotiated)?;
            (raw_info, Some(pair))
        } else {
            (raw_info, None)
        };
        let negotiated = NegotiatedCaps {
            identity,
            video_info,
            is_cuda,
            drm,
        };
        self.dmabuf_descriptors.clear();
        self.cuda_layout_logged = false;
        self.caps = Some(negotiated.clone());
        Ok(negotiated)
    }
}

pub(super) fn appsink_pending_refresh_interval(max_publish_fps: Option<u32>) -> Option<Duration> {
    let default_ms = if max_publish_fps.is_some() {
        DEFAULT_CAPPED_PENDING_REFRESH_MS
    } else {
        DEFAULT_UNCAPPED_PENDING_REFRESH_MS
    };
    let value = env_i64_or_default("KLD_APPSINK_PENDING_REFRESH_MS", default_ms);
    if value < 0 {
        return None;
    }
    Some(Duration::from_millis(value as u64))
}

fn trace_buffer_metadata_hash(buffer: &gst::Buffer, caps: &gst::CapsRef) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    caps.to_string().hash(&mut hasher);
    buffer.size().hash(&mut hasher);
    buffer.n_memory().hash(&mut hasher);
    buffer.pts().map(|value| value.nseconds()).hash(&mut hasher);
    buffer.dts().map(|value| value.nseconds()).hash(&mut hasher);
    buffer
        .duration()
        .map(|value| value.nseconds())
        .hash(&mut hasher);
    buffer.offset().hash(&mut hasher);
    buffer.offset_end().hash(&mut hasher);
    format!("{:?}", buffer.flags()).hash(&mut hasher);
    hasher.finish()
}

fn should_drop_for_pending_mailbox(
    mailbox: &LatestFrameMailbox,
    source_id: &str,
    drop_if_pending: bool,
    refresh_interval: Option<Duration>,
) -> bool {
    if !drop_if_pending {
        return false;
    }

    let Some(age) = mailbox.pending_frame_age(source_id) else {
        return false;
    };

    match refresh_interval {
        Some(interval) => age < interval,
        None => true,
    }
}

const APPSINK_POOL_MIN_BUFFERS: u32 = 2;
const APPSINK_POOL_MAX_BUFFERS: u32 = 6;

fn configure_appsink_queue(appsink: &gst_app::AppSink) {
    appsink.set_max_buffers(1);
    if appsink.find_property("leaky-type").is_some() {
        // GStreamer 1.28 replaced the deprecated drop boolean with an enum.
        appsink.set_property_from_str("leaky-type", "downstream");
    } else {
        appsink.set_drop(true);
    }
    if appsink.find_property("max-bytes").is_some() {
        appsink.set_property("max-bytes", 0u64);
    }
    if appsink.find_property("max-time").is_some() {
        appsink.set_property("max-time", 0u64);
    }
}

fn propose_appsink_allocation(
    query: &mut gst::query::Allocation,
    importable_pool: Option<&ImportableDmaBufPool>,
) -> bool {
    let (Some(caps), _) = query.get_owned() else {
        return false;
    };
    if query
        .find_allocation_meta::<gst_video::VideoMeta>()
        .is_none()
    {
        query.add_allocation_meta::<gst_video::VideoMeta>(None);
    }

    let is_dmabuf = caps
        .features(0)
        .is_some_and(|features| features.contains("memory:DMABuf"));
    if is_dmabuf {
        // Preserve the decoder/exporter's modifier-capable pool. If one was
        // already proposed, bound its live allocation count without replacing
        // it with a system-memory pool that would destroy DMA_DRM negotiation.
        for (index, (pool, size, min, max)) in query.allocation_pools().into_iter().enumerate() {
            let bounded_max = if max == 0 {
                APPSINK_POOL_MAX_BUFFERS
            } else {
                max.min(APPSINK_POOL_MAX_BUFFERS)
            };
            query.set_nth_allocation_pool(
                index as u32,
                pool.as_ref(),
                size,
                min.min(bounded_max),
                bounded_max,
            );
        }
        let is_linear_nv12 = gst_video::VideoInfo::from_caps(&caps)
            .is_ok_and(|info| info.format() == gst_video::VideoFormat::Nv12);
        if is_linear_nv12
            && let Some(pool) = importable_pool
            && let Ok(video_info) = gst_video::VideoInfo::from_caps(&caps)
            && let Ok(size) = u32::try_from(video_info.size())
        {
            let mut config = pool.config();
            config.set_params(
                Some(&caps),
                size,
                APPSINK_POOL_MIN_BUFFERS,
                APPSINK_POOL_MAX_BUFFERS,
            );
            config.add_option(gst_video::BUFFER_POOL_OPTION_VIDEO_META);
            if pool.set_config(config).is_ok() {
                query.add_allocation_pool(
                    Some(pool),
                    size,
                    APPSINK_POOL_MIN_BUFFERS,
                    APPSINK_POOL_MAX_BUFFERS,
                );
            }
        }
        return true;
    }

    let Ok(video_info) = gst_video::VideoInfo::from_caps(&caps) else {
        return false;
    };
    let Ok(size) = u32::try_from(video_info.size()) else {
        return false;
    };
    let pool = gst_video::VideoBufferPool::new();
    let mut config = pool.config();
    config.set_params(
        Some(&caps),
        size,
        APPSINK_POOL_MIN_BUFFERS,
        APPSINK_POOL_MAX_BUFFERS,
    );
    config.add_option(gst_video::BUFFER_POOL_OPTION_VIDEO_META);
    if pool.set_config(config).is_err() {
        return false;
    }
    query.add_allocation_pool(
        Some(&pool),
        size,
        APPSINK_POOL_MIN_BUFFERS,
        APPSINK_POOL_MAX_BUFFERS,
    );
    true
}

impl VideoPlayer {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn configure_appsink(
        pipeline: &gst::Element,
        source_id: &Arc<String>,
        session_id: u64,
        frame_mailbox: &LatestFrameMailbox,
        metrics: &Arc<PerformanceMetrics>,
        creation_start: std::time::Instant,
        caps: &gst::Caps,
        caps_ladder: &[&str],
        first_frame_logged: Arc<AtomicBool>,
        decode_path_logged: Arc<AtomicBool>,
        accept_samples: Arc<AtomicBool>,
        max_publish_fps: Option<u32>,
    ) -> anyhow::Result<gst_app::AppSink> {
        let appsink = gst::ElementFactory::make("appsink")
            .name("video-sink")
            .build()?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast to AppSink"))?;

        appsink.set_caps(Some(caps));
        let sink_sync_enabled = appsink_sync_enabled();
        appsink.set_sync(sink_sync_enabled);
        configure_appsink_queue(&appsink);
        appsink.set_property("enable-last-sample", false);
        appsink.set_property("wait-on-eos", false);
        appsink.set_property("qos", true);
        appsink.set_property(
            "processing-deadline",
            appsink_processing_deadline_ms().saturating_mul(1_000_000),
        );
        let max_lateness_ms = appsink_max_lateness_ms();
        if max_lateness_ms >= 0 {
            appsink.set_property("max-lateness", max_lateness_ms.saturating_mul(1_000_000));
        }

        let cb_source_id = source_id.clone();
        let frame_mailbox_clone = frame_mailbox.clone();
        let callback_metrics = metrics.clone();
        let callback_first_frame_logged = first_frame_logged.clone();
        let callback_decode_path_logged = decode_path_logged.clone();
        let callback_accept_samples = accept_samples.clone();
        let callback_stop_logged = Arc::new(AtomicBool::new(false));
        let callback_stop_logged_clone = callback_stop_logged.clone();
        let publish_interval_ns = publish_interval_ns(max_publish_fps);
        let callback_last_publish_ns = Arc::new(AtomicU64::new(super::NEVER_PUBLISHED_NS));
        let callback_last_publish_ns_clone = callback_last_publish_ns.clone();
        let drop_if_mailbox_pending = appsink_drop_if_mailbox_pending();
        let pending_refresh_interval = appsink_pending_refresh_interval(max_publish_fps);
        let mut negotiated_state = AppsinkNegotiatedState::default();
        let importable_pool = ImportableDmaBufPool::try_new();

        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .propose_allocation(move |_, query| {
                    propose_appsink_allocation(query, importable_pool.as_ref())
                })
                .new_sample(move |sink| {
                    callback_metrics
                        .record_video_backend_metric(VideoBackendMetricKind::AppsinkCallback);
                    let source_id = cb_source_id.clone();
                    let source_name = source_id.as_ref().as_str();

                    if should_abort_appsink_sample(
                        &callback_accept_samples,
                        &callback_stop_logged_clone,
                        source_name,
                    ) {
                        return Err(gst::FlowError::Flushing);
                    }

                    if !callback_first_frame_logged.load(Ordering::SeqCst) {
                        callback_first_frame_logged.store(true, Ordering::SeqCst);
                        let duration = creation_start.elapsed();
                        info!(
                            "[ASSET] {}: First video frame produced in {:.3}ms",
                            source_id,
                            duration.as_secs_f64() * 1000.0
                        );
                    }

                    let drop_pending_sample = should_drop_for_pending_mailbox(
                        &frame_mailbox_clone,
                        source_name,
                        drop_if_mailbox_pending,
                        pending_refresh_interval,
                    );
                    let sample = match sink.pull_sample() {
                        Ok(sample) => sample,
                        Err(_) => return Err(gst::FlowError::Error),
                    };

                    if drop_pending_sample {
                        callback_metrics.record_video_backend_metric(
                            VideoBackendMetricKind::AppsinkMailboxDropped,
                        );
                        return Ok(gst::FlowSuccess::Ok);
                    }

                    if should_abort_appsink_sample(
                        &callback_accept_samples,
                        &callback_stop_logged_clone,
                        source_name,
                    ) {
                        return Err(gst::FlowError::Flushing);
                    }

                    let elapsed_ns = creation_start.elapsed().as_nanos() as u64;
                    if !should_publish_now(
                        &callback_last_publish_ns_clone,
                        publish_interval_ns,
                        elapsed_ns,
                    ) {
                        callback_metrics.record_video_backend_metric(
                            VideoBackendMetricKind::AppsinkPublishCapped,
                        );
                        return Ok(gst::FlowSuccess::Ok);
                    }

                    let frame = sample_to_video_frame(
                        source_name,
                        sample,
                        session_id,
                        &mut negotiated_state,
                    )?;
                    maybe_log_decode_path(source_name, &frame, &callback_decode_path_logged);
                    frame_mailbox_clone.publish_frame(source_name, frame);
                    callback_metrics
                        .record_video_backend_metric(VideoBackendMetricKind::AppsinkFramePublished);

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        pipeline.set_property("video-sink", &appsink);

        info!(
            "[VIDEO] {}: VideoPlayer created with playbin + appsink (requested_mode={} sync={} processing_deadline_ms={} max_lateness_ms={} drop_if_mailbox_pending={} pending_refresh_ms={:?} max_publish_fps={:?} caps_ladder={:?} caps={})",
            source_id,
            get_video_mode().cli_label(),
            sink_sync_enabled,
            appsink_processing_deadline_ms(),
            max_lateness_ms,
            drop_if_mailbox_pending,
            pending_refresh_interval.map(|duration| duration.as_millis()),
            max_publish_fps,
            caps_ladder,
            caps
        );

        Ok(appsink)
    }
}

pub fn frame_decode_path_label(frame: &VideoFrame) -> &'static str {
    match frame.format {
        VideoFrameFormat::Rgba => "rgba",
        VideoFrameFormat::GlExternalRgba { .. } => "libmpv-gl-shared-rgba",
        VideoFrameFormat::Nv12 { .. } => "nv12",
        VideoFrameFormat::P010 { .. } => "p010",
        VideoFrameFormat::DmaBufNv12 { .. } => "dmabuf-nv12",
        VideoFrameFormat::NativeDmaBufNv12 { .. } => "native-dmabuf-nv12",
        VideoFrameFormat::CudaNv12 { .. } => "cuda-nv12",
        VideoFrameFormat::I420 { .. } => "i420",
    }
}

pub(super) fn should_warn_about_cpu_video_path(mode: VideoMode, format: &VideoFrameFormat) -> bool {
    matches!(
        (mode, format),
        (
            VideoMode::Auto | VideoMode::StrictCuda | VideoMode::ForceDmaBuf,
            VideoFrameFormat::Nv12 { .. }
                | VideoFrameFormat::P010 { .. }
                | VideoFrameFormat::I420 { .. }
                | VideoFrameFormat::Rgba
        )
    )
}

pub(super) fn maybe_log_decode_path(source_id: &str, frame: &VideoFrame, logged: &AtomicBool) {
    if !logged.swap(true, Ordering::SeqCst) {
        let actual_path = frame_decode_path_label(frame);
        info!(
            "[VIDEO] {}: Actual decode path={} frame={}x{} session={} color={:?} geometry={:?}",
            source_id,
            actual_path,
            frame.width,
            frame.height,
            frame.session_id,
            frame.color,
            frame.geometry,
        );

        let requested_mode = get_video_mode();
        if should_warn_about_cpu_video_path(requested_mode, &frame.format)
            && !CPU_VIDEO_FALLBACK_WARNED.swap(true, Ordering::SeqCst)
        {
            let capabilities = current_video_capabilities();
            warn!(
                "[VIDEO] {}: Falling back to CPU video path (actual={} requested_mode={} nvidia_driver={} vaapi={:?} nvcodec={:?} cuda_elements={:?}); this usually means hardware decode/zero-copy is unavailable and can cause high CPU usage",
                source_id,
                actual_path,
                requested_mode.cli_label(),
                capabilities.has_nvidia_driver,
                capabilities.vaapi_decoders,
                capabilities.nvcodec_decoders,
                capabilities.cuda_elements
            );
        }
    }
}

pub(super) fn sample_to_video_frame(
    source_name: &str,
    sample: gst::Sample,
    session_id: u64,
    negotiated_state: &mut AppsinkNegotiatedState,
) -> Result<VideoFrame, gst::FlowError> {
    let buffer = match sample.buffer() {
        Some(b) => b.to_owned(),
        None => return Err(gst::FlowError::Error),
    };

    let caps = match sample.caps() {
        Some(c) => c,
        None => return Err(gst::FlowError::Error),
    };

    let negotiated = negotiated_state.negotiated_caps(caps)?;
    let video_info = negotiated.video_info.clone();

    let width = video_info.width();
    let height = video_info.height();

    // Prefer GstVideoMeta stride/offset (reflects actual memory layout from
    // hardware decoders), fall back to VideoInfo when the meta is absent.
    // SAFETY: `buffer` is a live GStreamer buffer for this callback; if video meta exists,
    // GStreamer owns it for the buffer lifetime and we copy only fixed-size stride/offset arrays.
    let (meta_strides, meta_offsets, has_meta) = unsafe {
        let raw_meta =
            gst_video::ffi::gst_buffer_get_video_meta(buffer.as_ptr() as *mut gst::ffi::GstBuffer);
        if !raw_meta.is_null() {
            let meta = &*raw_meta;
            (meta.stride, meta.offset, true)
        } else {
            ([0i32; 4], [0usize; 4], false)
        }
    };
    let vi_strides = video_info.stride();
    let vi_offsets = video_info.offset();
    let mut vi_s = [0i32; 4];
    let mut vi_o = [0usize; 4];
    let n_planes = (video_info.n_planes() as usize).min(4);
    vi_s[..n_planes].copy_from_slice(&vi_strides[..n_planes]);
    vi_o[..n_planes].copy_from_slice(&vi_offsets[..n_planes]);
    let buffer_size = buffer.size();

    let is_cuda = negotiated.is_cuda;
    let (strides, offsets) = if is_cuda && prefer_videoinfo_cuda_layout_enabled() {
        (vi_s, vi_o)
    } else if has_meta {
        (meta_strides, meta_offsets)
    } else {
        (vi_s, vi_o)
    };
    let y_stride = strides[0] as u32;

    let format = match video_info.format() {
        gst_video::VideoFormat::Nv12 => {
            let (uv_width, uv_height) = chroma_plane_extent(width, height);
            let min_y_bytes = y_stride as usize * height as usize;
            let uv_offset = offsets[1];
            let uv_stride = strides[1].max(0) as usize;
            let min_uv_bytes = uv_stride.saturating_mul(uv_height as usize);
            let nv12_layout_invalid = strides[0] <= 0
                || strides[1] <= 0
                || y_stride < width
                || uv_stride < (uv_width.saturating_mul(2)) as usize
                || uv_offset > buffer_size
                || uv_offset.saturating_add(min_uv_bytes) > buffer_size
                || min_y_bytes > buffer_size;
            if nv12_layout_invalid {
                tracing::warn!(
                    "[VIDEO] Invalid NV12 layout detected: size={} frame={}x{} y_stride={} uv_offset={} uv_stride={} caps={}",
                    buffer_size,
                    width,
                    height,
                    strides[0],
                    offsets[1],
                    strides[1],
                    caps.to_string()
                );
            }
            if is_cuda {
                let should_log_layout =
                    cuda_layout_log_every_frame_enabled() || !negotiated_state.cuda_layout_logged;
                if should_log_layout {
                    negotiated_state.cuda_layout_logged = true;
                    tracing::debug!(
                        "[VIDEO] CUDA NV12 layout {}: y_stride={} uv_offset={} uv_stride={} frame={}x{} size={} has_meta={} vi_y_stride={} vi_uv_stride={} caps={}",
                        source_name,
                        strides[0],
                        offsets[1],
                        strides[1],
                        width,
                        height,
                        buffer_size,
                        has_meta,
                        vi_s[0],
                        vi_s[1],
                        caps.to_string()
                    );
                }
                VideoFrameFormat::CudaNv12 {
                    y_stride,
                    uv_offset: offsets[1] as u32,
                    uv_stride: strides[1] as u32,
                }
            } else {
                let drm = negotiated.drm.or_else(|| {
                    (buffer.n_memory() > 0
                        && buffer
                            .peek_memory(0)
                            .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
                            .is_some())
                    .then_some((super::dmabuf::DRM_FORMAT_NV12, 0))
                });
                if let Some((fourcc, modifier)) = drm {
                    negotiated_state
                        .dmabuf_descriptors
                        .synchronized_frame_format(
                            &buffer, width, height, fourcc, modifier, strides, offsets,
                        )
                        .map_err(|error| {
                            tracing::warn!(
                                "[VIDEO] {}: rejecting incompatible DMA_DRM frame: {error:#}",
                                source_name
                            );
                            gst::FlowError::NotNegotiated
                        })?
                } else {
                    VideoFrameFormat::Nv12 {
                        y_stride,
                        uv_offset: offsets[1] as u32,
                        uv_stride: strides[1] as u32,
                    }
                }
            }
        }
        gst_video::VideoFormat::P01010le => {
            let (uv_width, uv_height) = chroma_plane_extent(width, height);
            let uv_offset = offsets[1];
            let uv_stride = strides[1].max(0) as usize;
            let valid = strides[0] > 0
                && strides[1] > 0
                && y_stride >= width.saturating_mul(2)
                && uv_stride >= uv_width.saturating_mul(4) as usize
                && (y_stride as usize).saturating_mul(height as usize) <= buffer_size
                && uv_offset <= buffer_size
                && uv_offset.saturating_add(uv_stride.saturating_mul(uv_height as usize))
                    <= buffer_size;
            if !valid {
                tracing::error!(
                    "[VIDEO] Invalid P010 layout: size={} frame={}x{} y_stride={} uv_offset={} uv_stride={} caps={}",
                    buffer_size,
                    width,
                    height,
                    strides[0],
                    offsets[1],
                    strides[1],
                    caps
                );
                return Err(gst::FlowError::NotNegotiated);
            }
            VideoFrameFormat::P010 {
                y_stride,
                uv_offset: uv_offset as u32,
                uv_stride: uv_stride as u32,
            }
        }
        gst_video::VideoFormat::I420 => VideoFrameFormat::I420 {
            y_stride,
            u_offset: offsets[1] as u32,
            u_stride: strides[1] as u32,
            v_offset: offsets[2] as u32,
            v_stride: strides[2] as u32,
        },
        gst_video::VideoFormat::Rgba => VideoFrameFormat::Rgba,
        other => {
            tracing::error!("[VIDEO] Unsupported format {:?}, negotiation failed", other);
            return Err(gst::FlowError::NotNegotiated);
        }
    };

    let pts_ns = buffer.pts().map(|pts| pts.nseconds());
    let dts_ns = buffer.dts().map(|dts| dts.nseconds());
    let duration_ns = buffer.duration().map(|duration| duration.nseconds());
    let trace_hash = if crate::observability::trace_all::trace_all_enabled() {
        Some(trace_buffer_metadata_hash(&buffer, caps))
    } else {
        None
    };

    let frame = VideoFrame {
        storage: buffer.clone().into(),
        width,
        height,
        stride: y_stride,
        format,
        session_id,
        pts_ns,
        duration_ns,
        color: color_metadata(&video_info, caps),
        geometry: frame_geometry(&video_info, &buffer),
    };

    if let Some(trace_hash) = trace_hash {
        tracing::trace!(
            "[TRACE5][APPSINK-FRAME] output={} session={} hash={:016x} frame_hash={:016x} format={} size={}x{} stride={} pts_ns={:?} dts_ns={:?} duration_ns={:?} buffer_size={} memories={} offset={} offset_end={} flags={:?} caps={}",
            source_name,
            session_id,
            trace_hash,
            frame.trace_fingerprint(),
            frame_decode_path_label(&frame),
            width,
            height,
            y_stride,
            pts_ns,
            dts_ns,
            duration_ns,
            buffer.size(),
            buffer.n_memory(),
            buffer.offset(),
            buffer.offset_end(),
            buffer.flags(),
            caps
        );
    }

    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::{AppsinkNegotiatedState, appsink_pending_refresh_interval, color_metadata};
    use crate::video::test_support::{remove_env_var, set_env_var, with_video_env_test_lock};
    use crate::video::{VideoColorPrimaries, VideoTransfer};
    use gstreamer as gst;
    use gstreamer_video as gst_video;

    #[test]
    fn pending_refresh_accepts_env_overrides() {
        with_video_env_test_lock(|| {
            let old_refresh = std::env::var_os("KLD_APPSINK_PENDING_REFRESH_MS");
            remove_env_var("KLD_APPSINK_PENDING_REFRESH_MS");
            assert_eq!(
                appsink_pending_refresh_interval(Some(24))
                    .unwrap()
                    .as_millis(),
                32
            );
            assert_eq!(
                appsink_pending_refresh_interval(None).unwrap().as_millis(),
                75
            );

            set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", "0");
            assert_eq!(
                appsink_pending_refresh_interval(Some(24))
                    .unwrap()
                    .as_millis(),
                0
            );

            set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", "-1");
            assert!(appsink_pending_refresh_interval(None).is_none());

            match old_refresh {
                Some(value) => set_env_var("KLD_APPSINK_PENDING_REFRESH_MS", value),
                None => remove_env_var("KLD_APPSINK_PENDING_REFRESH_MS"),
            }
        });
    }

    #[test]
    fn dma_drm_caps_preserve_fourcc_and_modifier_in_negotiated_cache() {
        gst::init().expect("GStreamer should initialize");
        const INTEL_X_TILED: u64 = 0x0100_0000_0000_0001;
        let info = gst_video::VideoInfo::builder(gst_video::VideoFormat::Nv12, 64, 64)
            .build()
            .expect("valid NV12 video info");
        let drm_info = gst_video::VideoInfoDmaDrm::new(
            info,
            super::super::dmabuf::DRM_FORMAT_NV12,
            INTEL_X_TILED,
        );
        let caps = drm_info.to_caps().expect("DMA_DRM caps should serialize");
        let mut state = AppsinkNegotiatedState::default();
        let first = state
            .negotiated_caps(&caps)
            .expect("DMA_DRM caps should parse");
        let second = state
            .negotiated_caps(&caps)
            .expect("cached DMA_DRM caps should parse");
        assert_eq!(
            first.drm,
            Some((super::super::dmabuf::DRM_FORMAT_NV12, INTEL_X_TILED))
        );
        assert_eq!(first.identity, second.identity);
        assert_eq!(first.video_info.format(), gst_video::VideoFormat::Nv12);
    }

    #[test]
    fn hdr_transfer_and_mastering_luminance_are_preserved() {
        gst::init().expect("GStreamer should initialize");
        let coordinate = gst_video::VideoMasteringDisplayInfoCoordinate::new;
        let mastering = gst_video::VideoMasteringDisplayInfo::new(
            [
                coordinate(0.680, 0.320),
                coordinate(0.265, 0.690),
                coordinate(0.150, 0.060),
            ],
            coordinate(0.3127, 0.3290),
            10_000_000,
            50,
        );

        for (gst_transfer, expected_transfer) in [
            (
                gst_video::VideoTransferFunction::Smpte2084,
                VideoTransfer::Pq,
            ),
            (
                gst_video::VideoTransferFunction::AribStdB67,
                VideoTransfer::Hlg,
            ),
        ] {
            let colorimetry = gst_video::VideoColorimetry::new(
                gst_video::VideoColorRange::Range16_235,
                gst_video::VideoColorMatrix::Bt2020,
                gst_transfer,
                gst_video::VideoColorPrimaries::Bt2020,
            );
            let info = gst_video::VideoInfo::builder(gst_video::VideoFormat::P01010le, 1920, 1080)
                .colorimetry(&colorimetry)
                .build()
                .expect("valid HDR video info");
            let mut caps = info.to_caps().expect("HDR info should serialize to caps");
            mastering.add_to_caps(caps.make_mut());

            let metadata = color_metadata(&info, caps.as_ref());
            assert_eq!(metadata.transfer, expected_transfer);
            assert_eq!(metadata.primaries, VideoColorPrimaries::Bt2020);
            assert_eq!(metadata.bit_depth, 10);
            let mastering = metadata.mastering.expect("mastering metadata");
            assert!((mastering.max_luminance_nits - 1_000.0).abs() < f32::EPSILON);
            assert!((mastering.min_luminance_nits - 0.005).abs() < f32::EPSILON);
        }
    }
}
