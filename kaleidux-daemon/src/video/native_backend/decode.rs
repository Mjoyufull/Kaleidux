use super::capabilities::{HardwareDecoderChoice, NativeDecoderApi, NativePathTier};
use super::control::NativePlaybackControl;
use super::frame_copy::NativeFrameConverter;
use super::gpu_delivery::receive_frames;
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

pub(super) struct DecodeState {
    pub(super) converter: NativeFrameConverter,
    pub(super) surface_exporter: super::dmabuf::NativeSurfaceExporter,
    pub(super) transfer_frame: Video,
    pub(super) first_frame_decoded: bool,
    pub(super) surface_export_warning_emitted: bool,
    pub(super) surface_export_disabled: bool,
    pub(super) timeline: PlaybackTimeline,
    pub(super) last_publish_ns: AtomicU64,
}

#[derive(Default)]
pub(super) struct PlaybackTimeline {
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
    let _audio = super::audio::AudioWorker::spawn(&config.uri, config.control.clone());

    let mut rejected = Vec::new();
    loop {
        let mut attempted = None;
        match run_session(&config, &rejected, &mut attempted) {
            Ok(()) => return,
            Err(_) if config.control.is_stopped() => return,
            Err(error) => {
                config
                    .metrics
                    .record_video_backend_metric(VideoBackendMetricKind::NativeDecodeError);
                if let Some(api) = attempted {
                    warn!(
                        "[NATIVE-VIDEO] {} session={} {} failed: {error:#}; trying the next supported decoder",
                        config.source_id,
                        config.session_id,
                        api.label()
                    );
                    rejected.push(api);
                } else {
                    report_fatal(&config, format!("native FFmpeg decode failed: {error:#}"));
                    return;
                }
            }
        }
    }
}

fn run_session(
    config: &NativeDecodeConfig,
    rejected: &[NativeDecoderApi],
    attempted: &mut Option<NativeDecoderApi>,
) -> anyhow::Result<()> {
    let stop_control = config.control.clone();
    let mut input =
        ffmpeg::format::input_with_interrupt(&config.uri, move || stop_control.is_stopped())?;
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

    let mut decoder = super::decoder_open::open_decoder(parameters, stream_time_base, rejected)?;
    *attempted = decoder.hardware.map(|choice| choice.api);
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
        surface_export_disabled: false,
        timeline: PlaybackTimeline::default(),
        last_publish_ns: AtomicU64::new(super::super::NEVER_PUBLISHED_NS),
    };
    // Keep one packet wrapper for the lifetime of the worker. libavformat
    // replaces its referenced payload after av_packet_unref, avoiding an
    // AVPacket allocation on every demand/stream packet.
    let mut packet = ffmpeg::Packet::empty();
    let mut eof_sent = false;

    if !rejected.is_empty() {
        let position = config.control.position_ns();
        if position > 0
            && let Err(error) = seek_input(&mut input, &mut decoder.decoder, position)
        {
            warn!(
                "[NATIVE-VIDEO] {}: decoder fallback cannot restore playback position: {error:#}; continuing from reopened input",
                config.source_id
            );
        }
    }

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
            config.control.looped();
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

pub(super) fn discard_unselected_streams(
    input: &mut ffmpeg::format::context::Input,
    selected: usize,
) {
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

pub(super) fn surface_export_enabled(api: NativeDecoderApi) -> bool {
    (if api == NativeDecoderApi::Nvdec {
        super::native_cuda_import_available()
    } else {
        super::native_surface_import_available()
    }) && std::env::var("KLD_NATIVE_SURFACE_IMPORT")
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
            if matches!(
                choice.api,
                NativeDecoderApi::Vaapi | NativeDecoderApi::Nvdec | NativeDecoderApi::VulkanVideo
            ) && surface_export_enabled(choice.api) =>
        {
            NativePathTier::SingleGpuCopy
        }
        Some(_) => NativePathTier::HardwareTransfer,
        None => NativePathTier::SoftwareDecode,
    }
}

pub(super) fn publish_frame(
    config: &NativeDecodeConfig,
    state: &mut DecodeState,
    frame: VideoFrame,
) -> bool {
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

pub(super) fn pace_frame(
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
    let due = if let Some(audio_position) = config.control.audio_position_ns() {
        // Audio output is the master while its clock is healthy. Extrapolated
        // observations avoid a GStreamer query on every video frame.
        Instant::now() + Duration::from_nanos(pts_ns.saturating_sub(audio_position))
    } else {
        timeline.wall_anchor.expect("checked above")
            + Duration::from_nanos(pts_ns.saturating_sub(timeline.media_anchor_ns))
    };
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

pub(super) fn timestamp_to_ns(timestamp: i64, time_base: ffmpeg::Rational) -> Option<u64> {
    if timestamp < 0 || time_base.denominator() <= 0 || time_base.numerator() <= 0 {
        return None;
    }
    let value = i128::from(timestamp)
        .checked_mul(i128::from(time_base.numerator()))?
        .checked_mul(1_000_000_000)?
        / i128::from(time_base.denominator());
    u64::try_from(value).ok()
}

pub(super) fn duration_to_ns(duration: i64, time_base: ffmpeg::Rational) -> Option<u64> {
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
