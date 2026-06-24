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

use super::capabilities::{CPU_VIDEO_FALLBACK_WARNED, CUDA_LAYOUT_LOG_SIGNATURES};
use super::dmabuf::extract_dmabuf_nv12;
use super::{
    LatestFrameMailbox, VideoFrame, VideoFrameFormat, VideoMode, VideoPlayer, chroma_plane_extent,
    current_video_capabilities, get_video_mode, publish_interval_ns, should_abort_appsink_sample,
    should_publish_now,
};
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
        appsink.set_drop(true);
        appsink.set_max_buffers(1);
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
        appsink.set_property_from_str("leaky-type", "downstream");

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

        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
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

                    let frame = sample_to_video_frame(source_name, sample, session_id)?;
                    maybe_log_decode_path(source_name, &frame, &callback_decode_path_logged);
                    frame_mailbox_clone.publish_frame(source_name, frame);
                    callback_metrics
                        .record_video_backend_metric(VideoBackendMetricKind::AppsinkFramePublished);

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        appsink.set_property("drop", true);
        appsink.set_property("max-buffers", 1u32);
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
        #[cfg(feature = "mpv-backend")]
        VideoFrameFormat::GlExternalRgba { .. } => "libmpv-gl-shared-rgba",
        VideoFrameFormat::Nv12 { .. } => "nv12",
        VideoFrameFormat::DmaBufNv12 { .. } => "dmabuf-nv12",
        VideoFrameFormat::CudaNv12 { .. } => "cuda-nv12",
        VideoFrameFormat::I420 { .. } => "i420",
    }
}

pub(super) fn should_warn_about_cpu_video_path(mode: VideoMode, format: &VideoFrameFormat) -> bool {
    matches!(
        (mode, format),
        (
            VideoMode::Auto | VideoMode::StrictCuda | VideoMode::ForceDmaBuf,
            VideoFrameFormat::Nv12 { .. } | VideoFrameFormat::I420 { .. } | VideoFrameFormat::Rgba
        )
    )
}

pub(super) fn maybe_log_decode_path(source_id: &str, frame: &VideoFrame, logged: &AtomicBool) {
    if !logged.swap(true, Ordering::SeqCst) {
        let actual_path = frame_decode_path_label(frame);
        info!(
            "[VIDEO] {}: Actual decode path={} frame={}x{} session={}",
            source_id, actual_path, frame.width, frame.height, frame.session_id
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
) -> Result<VideoFrame, gst::FlowError> {
    let buffer = match sample.buffer() {
        Some(b) => b.to_owned(),
        None => return Err(gst::FlowError::Error),
    };

    let caps = match sample.caps() {
        Some(c) => c,
        None => return Err(gst::FlowError::Error),
    };

    let video_info = match gst_video::VideoInfo::from_caps(caps) {
        Ok(vi) => vi,
        Err(_) => return Err(gst::FlowError::Error),
    };

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

    let is_cuda = caps
        .features(0)
        .is_some_and(|f| f.contains("memory:CUDAMemory"));
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
                let key = format!("{source_name}:{session_id}");
                let mut should_log_layout = cuda_layout_log_every_frame_enabled();
                if !should_log_layout {
                    let mut signatures = CUDA_LAYOUT_LOG_SIGNATURES.lock();
                    if let std::collections::hash_map::Entry::Vacant(entry) = signatures.entry(key)
                    {
                        entry.insert(String::new());
                        should_log_layout = true;
                    }
                }
                if should_log_layout {
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
                let is_dmabuf = buffer.n_memory() > 0
                    && buffer
                        .peek_memory(0)
                        .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
                        .is_some();

                if is_dmabuf {
                    extract_dmabuf_nv12(&buffer, strides, offsets)
                } else {
                    VideoFrameFormat::Nv12 {
                        y_stride,
                        uv_offset: offsets[1] as u32,
                        uv_stride: strides[1] as u32,
                    }
                }
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
        buffer,
        width,
        height,
        stride: y_stride,
        format,
        session_id,
        pts_ns,
        duration_ns,
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
            frame.buffer.size(),
            frame.buffer.n_memory(),
            frame.buffer.offset(),
            frame.buffer.offset_end(),
            frame.buffer.flags(),
            caps
        );
    }

    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::appsink_pending_refresh_interval;
    use crate::video::test_support::{remove_env_var, set_env_var, with_video_env_test_lock};

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
}
