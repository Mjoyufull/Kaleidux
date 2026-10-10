use super::capabilities::{HardwareDecoderChoice, NativeDecoderApi};
use super::decode::{
    DecodeState, NativeDecodeConfig, duration_to_ns, pace_frame, publish_frame,
    surface_export_enabled, timestamp_to_ns,
};
use super::decoder_open::DecoderInstance;
use crate::observability::video_backend::VideoBackendMetricKind;
use ffmpeg_next::{self as ffmpeg, ffi, util::frame::video::Video};
use tracing::warn;

pub(super) fn receive_frames(
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

        let (frame, decoded_to_recycle) =
            deliver_frame(decoded, state, decoder, config, pts_ns, duration_ns)?;
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

fn deliver_frame(
    decoded: Video,
    state: &mut DecodeState,
    decoder: &DecoderInstance,
    config: &NativeDecodeConfig,
    pts_ns: Option<u64>,
    duration_ns: Option<u64>,
) -> anyhow::Result<(crate::video::VideoFrame, Option<Video>)> {
    let hardware_frame = decoder.hardware.is_some_and(|choice| {
        let decoded_format: ffi::AVPixelFormat = decoded.format().into();
        decoded_format == choice.pixel_format
    });
    let surface_api = decoder.hardware.map(|choice| choice.api).filter(|api| {
        hardware_frame
            && super::native_storage_format(&decoded) == ffmpeg::format::Pixel::NV12
            && matches!(
                api,
                NativeDecoderApi::Vaapi | NativeDecoderApi::VulkanVideo | NativeDecoderApi::Nvdec
            )
            && !state.surface_export_disabled
            && surface_export_enabled(*api)
    });
    let result = if let Some(surface_api) = surface_api {
        let exported = if surface_api == NativeDecoderApi::Nvdec {
            state
                .surface_exporter
                .export_cuda_nv12(decoded, config.session_id, pts_ns, duration_ns)
        } else {
            state.surface_exporter.export_hardware_nv12(
                decoded,
                surface_api,
                config.session_id,
                pts_ns,
                duration_ns,
            )
        };
        match exported {
            Ok(frame) => {
                config
                    .metrics
                    .record_video_backend_metric(VideoBackendMetricKind::NativeSurfaceExported);
                (frame, None)
            }
            Err((error, decoded)) => {
                state.surface_export_disabled = true;
                if !state.surface_export_warning_emitted {
                    warn!(
                        "[NATIVE-PATH] {} session={} {} surface export rejected ({error:#}); degrading to hardware-transfer",
                        config.source_id,
                        config.session_id,
                        surface_api.label()
                    );
                    state.surface_export_warning_emitted = true;
                }
                let frame = convert_cpu_frame(
                    &decoded,
                    state,
                    decoder,
                    config.session_id,
                    pts_ns,
                    duration_ns,
                    true,
                )?;
                (frame, Some(decoded))
            }
        }
    } else {
        let frame = convert_cpu_frame(
            &decoded,
            state,
            decoder,
            config.session_id,
            pts_ns,
            duration_ns,
            hardware_frame,
        )?;
        (frame, Some(decoded))
    };
    Ok(result)
}

fn convert_cpu_frame(
    decoded: &Video,
    state: &mut DecodeState,
    decoder: &DecoderInstance,
    session_id: u64,
    pts_ns: Option<u64>,
    duration_ns: Option<u64>,
    hardware: bool,
) -> anyhow::Result<crate::video::VideoFrame> {
    if hardware {
        transfer_hardware_frame(decoded, &mut state.transfer_frame, decoder.hardware)?;
        state
            .converter
            .convert(&state.transfer_frame, session_id, pts_ns, duration_ns)
    } else {
        state
            .converter
            .convert(decoded, session_id, pts_ns, duration_ns)
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
