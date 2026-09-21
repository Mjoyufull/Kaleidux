use super::capabilities::{HardwareDecoderChoice, NativeDecoderApi, NativePathTier};
use super::control::NativePlaybackControl;
use super::decoder_open::DecoderInstance;
use super::frame_copy::NativeFrameConverter;
use crate::metrics::PerformanceMetrics;
use crate::observability::video_backend::VideoBackendMetricKind;
use crate::video::VideoFrame;
use ffmpeg_next as ffmpeg;
use ffmpeg_next::ffi;
use ffmpeg_next::packet::Mut as _;
use ffmpeg_next::util::frame::video::Video;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

pub struct NativeDecodeConfig {
    pub uri: String,
    pub source_id: Arc<String>,
    pub session_id: u64,
    pub fanout: Arc<super::shared_decode::NativeDecodeFanout>,
    pub metrics: Arc<PerformanceMetrics>,
    pub control: Arc<NativePlaybackControl>,
    pub max_publish_fps: Option<u32>,
    pub creation_start: Instant,
}

struct DecodeState {
    converter: NativeFrameConverter,
    surface_exporter: super::dmabuf::NativeSurfaceExporter,
    transfer_frame: Video,
    first_frame_decoded: bool,
    surface_export_warning_emitted: bool,
    timeline: PlaybackTimeline,
    last_publish_ns: AtomicU64,
}

#[derive(Default)]
struct PlaybackTimeline {
    epoch: u64,
    media_anchor_ns: u64,
    wall_anchor: Option<Instant>,
}

pub fn run(config: NativeDecodeConfig) {
    if let Err(error) = ffmpeg::init() {
        report_fatal(&config, format!("FFmpeg initialization failed: {error}"));
        return;
    }
    ffmpeg::util::log::set_level(ffmpeg::util::log::Level::Error);

    let hardware_result = run_session(&config, true);
    if hardware_result.is_ok() || config.control.is_stopped() {
        return;
    }

    let hardware_error = hardware_result.expect_err("checked above");
    warn!(
        "[NATIVE-VIDEO] {} session={} hardware path failed: {hardware_error:#}; reopening with software decode",
        config.source_id, config.session_id
    );
    config
        .metrics
        .record_video_backend_metric(VideoBackendMetricKind::NativeDecodeError);
    if let Err(error) = run_session(&config, false)
        && !config.control.is_stopped()
    {
        report_fatal(
            &config,
            format!("native FFmpeg decode failed after software fallback: {error:#}"),
        );
    }
}

fn run_session(config: &NativeDecodeConfig, allow_hardware: bool) -> anyhow::Result<()> {
    let mut input = ffmpeg::format::input(&config.uri)?;
    let (stream_index, stream_time_base, average_rate, parameters, codec_id) = {
        let stream = input
            .streams()
            .best(ffmpeg::media::Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)?;
        let parameters = stream.parameters().clone();
        let codec_id = parameters.id();
        (
            stream.index(),
            stream.time_base(),
            stream.avg_frame_rate(),
            parameters,
            codec_id,
        )
    };
    discard_unselected_streams(&mut input, stream_index);

    let mut decoder =
        super::decoder_open::open_decoder(parameters, stream_time_base, allow_hardware)?;
    let actual_tier = selected_tier(decoder.hardware);
    let decoder_label = decoder
        .hardware
        .map(|choice| choice.api.label())
        .unwrap_or("software");
    let preferred_tier = decoder
        .hardware
        .map(|choice| choice.api.preferred_tier().label())
        .unwrap_or(actual_tier.label());
    info!(
        "[NATIVE-PATH] {} session={} codec={:?} decoder={} selected={} preferred={} control=libavformat/libavcodec",
        config.source_id,
        config.session_id,
        codec_id,
        decoder_label,
        actual_tier.label(),
        preferred_tier
    );

    let default_duration_ns = rate_duration_ns(average_rate);
    let mut state = DecodeState {
        converter: NativeFrameConverter::new(),
        surface_exporter: super::dmabuf::NativeSurfaceExporter::new(),
        transfer_frame: Video::empty(),
        first_frame_decoded: false,
        surface_export_warning_emitted: false,
        timeline: PlaybackTimeline::default(),
        last_publish_ns: AtomicU64::new(super::super::NEVER_PUBLISHED_NS),
    };
    // Keep one packet wrapper for the lifetime of the worker. libavformat
    // replaces its referenced payload after av_packet_unref, avoiding an
    // AVPacket allocation on every demand/stream packet.
    let mut packet = ffmpeg::Packet::empty();
    let mut eof_sent = false;

    loop {
        if config.control.is_stopped() {
            return Ok(());
        }
        if !config.control.wait_for_frame_demand() {
            return Ok(());
        }
        if let Some(seek_ns) = config.control.take_seek() {
            seek_input(&mut input, &mut decoder.decoder, seek_ns)?;
            state.timeline = PlaybackTimeline::default();
            eof_sent = false;
        }

        // Drain a decoder-buffered frame before reading another packet. This
        // keeps demand at one useful decoded frame while preserving B-frame
        // reorder state inside libavcodec.
        if receive_frames(
            config,
            &mut decoder,
            &mut state,
            stream_time_base,
            default_duration_ns,
        )? {
            continue;
        }

        // Once EOF has been submitted, receive_frame above is the only legal
        // decoder operation. If it has no more delayed B-frames, loop and
        // resume this still-unconsumed demand at the start of the source.
        if eof_sent {
            debug!(
                "[NATIVE-VIDEO] {} session={} looping at EOS without rebuilding decoder context",
                config.source_id, config.session_id
            );
            input.seek(0, ..)?;
            decoder.decoder.flush();
            state.timeline = PlaybackTimeline::default();
            config.control.set_position_ns(0);
            eof_sent = false;
            continue;
        }

        // SAFETY: `packet` is a live worker-owned AVPacket. av_read_frame
        // requires the previous reference to be released before reuse.
        unsafe { ffi::av_packet_unref(packet.as_mut_ptr()) };
        match packet.read(&mut input) {
            Ok(()) if packet.stream() != stream_index => continue,
            Ok(()) => {
                decoder.decoder.send_packet(&packet)?;
                let _ = receive_frames(
                    config,
                    &mut decoder,
                    &mut state,
                    stream_time_base,
                    default_duration_ns,
                )?;
            }
            Err(ffmpeg::Error::Eof) => {
                decoder.decoder.send_eof()?;
                eof_sent = true;
                let _ = receive_frames(
                    config,
                    &mut decoder,
                    &mut state,
                    stream_time_base,
                    default_duration_ns,
                )?;
                if config.control.is_stopped() {
                    return Ok(());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn discard_unselected_streams(input: &mut ffmpeg::format::context::Input, selected: usize) {
    // SAFETY: libavformat owns a contiguous `nb_streams` array for the live
    // input context. We only update AVStream's documented discard policy and
    // retain the selected video stream unchanged.
    unsafe {
        let context = input.as_mut_ptr();
        if context.is_null() || (*context).streams.is_null() {
            return;
        }
        for index in 0..(*context).nb_streams as usize {
            if index == selected {
                continue;
            }
            let stream = *(*context).streams.add(index);
            if !stream.is_null() {
                (*stream).discard = ffi::AVDiscard::AVDISCARD_ALL;
            }
        }
    }
}

fn receive_frames(
    config: &NativeDecodeConfig,
    decoder: &mut DecoderInstance,
    state: &mut DecodeState,
    time_base: ffmpeg::Rational,
    default_duration_ns: Option<u64>,
) -> anyhow::Result<bool> {
    let mut decoded = state.surface_exporter.acquire_decode_frame();
    loop {
        if let Err(error) = decoder.decoder.receive_frame(&mut decoded) {
            state.surface_exporter.recycle_decode_frame(decoded);
            return match error {
                ffmpeg::Error::Eof => Ok(false),
                ffmpeg::Error::Other { errno } if errno == ffmpeg::error::EAGAIN => Ok(false),
                error => Err(error.into()),
            };
        }
        let pts_ns = decoded
            .timestamp()
            .or_else(|| decoded.pts())
            .and_then(|timestamp| timestamp_to_ns(timestamp, time_base));
        let duration_ns =
            duration_to_ns(decoded.packet().duration, time_base).or(default_duration_ns);

        if state.first_frame_decoded && !pace_frame(config, &mut state.timeline, pts_ns) {
            state.surface_exporter.recycle_decode_frame(decoded);
            return Ok(true);
        }
        if config.control.is_stopped() {
            state.surface_exporter.recycle_decode_frame(decoded);
            return Ok(true);
        }

        let hardware_frame = decoder.hardware.is_some_and(|choice| {
            let decoded_format: ffi::AVPixelFormat = decoded.format().into();
            decoded_format == choice.pixel_format
        });
        let surface_api = decoder.hardware.map(|choice| choice.api).filter(|api| {
            hardware_frame
                && super::native_storage_format(&decoded) == ffmpeg::format::Pixel::NV12
                && matches!(api, NativeDecoderApi::Vaapi)
                && surface_export_enabled()
        });
        let (frame, decoded_to_recycle) = if let Some(surface_api) = surface_api {
            match state.surface_exporter.export_hardware_nv12(
                decoded,
                surface_api,
                config.session_id,
                pts_ns,
                duration_ns,
            ) {
                Ok(frame) => {
                    config
                        .metrics
                        .record_video_backend_metric(VideoBackendMetricKind::NativeSurfaceExported);
                    (frame, None)
                }
                Err((error, decoded)) => {
                    if !state.surface_export_warning_emitted {
                        warn!(
                            "[NATIVE-PATH] {} session={} {} surface export rejected ({error:#}); degrading to hardware-transfer",
                            config.source_id,
                            config.session_id,
                            surface_api.label()
                        );
                        state.surface_export_warning_emitted = true;
                    }
                    transfer_hardware_frame(&decoded, &mut state.transfer_frame, decoder.hardware)?;
                    let frame = state.converter.convert(
                        &state.transfer_frame,
                        config.session_id,
                        pts_ns,
                        duration_ns,
                    )?;
                    (frame, Some(decoded))
                }
            }
        } else {
            let frame = if hardware_frame {
                transfer_hardware_frame(&decoded, &mut state.transfer_frame, decoder.hardware)?;
                state.converter.convert(
                    &state.transfer_frame,
                    config.session_id,
                    pts_ns,
                    duration_ns,
                )?
            } else {
                state
                    .converter
                    .convert(&decoded, config.session_id, pts_ns, duration_ns)?
            };
            (frame, Some(decoded))
        };
        if let Some(decoded) = decoded_to_recycle {
            state.surface_exporter.recycle_decode_frame(decoded);
        }
        if let Some(pts_ns) = pts_ns {
            config.control.set_position_ns(pts_ns);
        }
        if publish_frame(config, state, frame) {
            return Ok(true);
        }
        decoded = state.surface_exporter.acquire_decode_frame();
    }
}

fn surface_export_enabled() -> bool {
    super::native_surface_import_available()
        && std::env::var("KLD_NATIVE_SURFACE_IMPORT")
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no" | "off"
                )
            })
            .unwrap_or(true)
}

pub(super) fn selected_tier(hardware: Option<HardwareDecoderChoice>) -> NativePathTier {
    match hardware {
        Some(choice)
            if matches!(choice.api, NativeDecoderApi::Vaapi) && surface_export_enabled() =>
        {
            NativePathTier::SingleGpuCopy
        }
        Some(_) => NativePathTier::HardwareTransfer,
        None => NativePathTier::SoftwareDecode,
    }
}

fn transfer_hardware_frame(
    decoded: &Video,
    software: &mut Video,
    hardware: Option<HardwareDecoderChoice>,
) -> anyhow::Result<()> {
    let choice = hardware.ok_or_else(|| anyhow::anyhow!("hardware transfer without decoder"))?;
    let decoded_format: ffi::AVPixelFormat = decoded.format().into();
    if decoded_format != choice.pixel_format {
        anyhow::bail!(
            "{} decoder returned unexpected software frame to hardware transfer path",
            choice.api.label()
        );
    }
    // SAFETY: the scratch wrapper is worker-owned. Unref releases the previous
    // hardware-transfer buffers while retaining the reusable AVFrame object.
    unsafe { ffi::av_frame_unref(software.as_mut_ptr()) };
    // SAFETY: both AVFrames are valid; FFmpeg allocates the destination
    // buffers and performs the device-to-host transfer synchronously.
    let result =
        unsafe { ffi::av_hwframe_transfer_data(software.as_mut_ptr(), decoded.as_ptr(), 0) };
    if result < 0 {
        anyhow::bail!(
            "{} hardware frame transfer failed with FFmpeg error {}",
            choice.api.label(),
            result
        );
    }
    // Transfer the negotiated colorimetry, timestamps, and frame metadata once
    // so downstream YUV shaders do not silently fall back to defaults.
    let props_result = unsafe { ffi::av_frame_copy_props(software.as_mut_ptr(), decoded.as_ptr()) };
    if props_result < 0 {
        anyhow::bail!("copying hardware frame properties failed with FFmpeg error {props_result}");
    }
    Ok(())
}

fn publish_frame(config: &NativeDecodeConfig, state: &mut DecodeState, frame: VideoFrame) -> bool {
    if !state.first_frame_decoded {
        info!(
            "[VIDEO] {}: Actual native decode path={} frame={}x{} session={} color={:?} geometry={:?}",
            config.source_id,
            super::super::appsink::frame_decode_path_label(&frame),
            frame.width,
            frame.height,
            frame.session_id,
            frame.color,
            frame.geometry,
        );
    }
    state.first_frame_decoded = true;
    let now_ns = config
        .creation_start
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    if !super::super::should_publish_now(
        &state.last_publish_ns,
        super::super::publish_interval_ns(config.max_publish_fps),
        now_ns,
    ) {
        return false;
    }
    config.control.consume_frame_demand();
    config.fanout.publish_frame(frame);
    true
}

fn pace_frame(
    config: &NativeDecodeConfig,
    timeline: &mut PlaybackTimeline,
    pts_ns: Option<u64>,
) -> bool {
    if !config.control.wait_until_playing() {
        return false;
    }
    let pts_ns = pts_ns.unwrap_or_else(|| config.control.position_ns());
    let epoch = config.control.play_epoch();
    if timeline.wall_anchor.is_none() || timeline.epoch != epoch {
        timeline.epoch = epoch;
        timeline.media_anchor_ns = pts_ns;
        timeline.wall_anchor = Some(Instant::now());
        return true;
    }
    let due = timeline.wall_anchor.expect("checked above")
        + Duration::from_nanos(pts_ns.saturating_sub(timeline.media_anchor_ns));
    while Instant::now() < due {
        if !config.control.wait_until(due) {
            return false;
        }
        if config.control.has_pending_seek() {
            return false;
        }
        if !config.control.is_playing() {
            if !config.control.wait_until_playing() {
                return false;
            }
            timeline.epoch = config.control.play_epoch();
            timeline.media_anchor_ns = pts_ns;
            timeline.wall_anchor = Some(Instant::now());
            return true;
        }
    }
    true
}

fn seek_input(
    input: &mut ffmpeg::format::context::Input,
    decoder: &mut ffmpeg::decoder::Video,
    seek_ns: u64,
) -> anyhow::Result<()> {
    let timestamp_us = (seek_ns / 1_000).min(i64::MAX as u64) as i64;
    input.seek(timestamp_us, ..)?;
    decoder.flush();
    Ok(())
}

fn timestamp_to_ns(timestamp: i64, time_base: ffmpeg::Rational) -> Option<u64> {
    if timestamp < 0 || time_base.denominator() <= 0 || time_base.numerator() <= 0 {
        return None;
    }
    let value = i128::from(timestamp)
        .checked_mul(i128::from(time_base.numerator()))?
        .checked_mul(1_000_000_000)?
        / i128::from(time_base.denominator());
    u64::try_from(value).ok()
}

fn duration_to_ns(duration: i64, time_base: ffmpeg::Rational) -> Option<u64> {
    (duration > 0)
        .then(|| timestamp_to_ns(duration, time_base))
        .flatten()
}

fn rate_duration_ns(rate: ffmpeg::Rational) -> Option<u64> {
    if rate.numerator() <= 0 || rate.denominator() <= 0 {
        return None;
    }
    let value =
        i128::from(rate.denominator()).checked_mul(1_000_000_000)? / i128::from(rate.numerator());
    u64::try_from(value).ok()
}

fn report_fatal(config: &NativeDecodeConfig, reason: String) {
    config.fanout.report_fatal(reason);
}

#[cfg(test)]
mod tests {
    use super::{rate_duration_ns, timestamp_to_ns};

    #[test]
    fn timestamps_use_stream_time_base() {
        assert_eq!(
            timestamp_to_ns(90_000, ffmpeg_next::Rational(1, 90_000)),
            Some(1_000_000_000)
        );
        assert_eq!(
            rate_duration_ns(ffmpeg_next::Rational(30_000, 1_001)),
            Some(33_366_666)
        );
    }
}
