use super::capabilities::{
    HardwareDecoderChoice, codec_hardware_choices, create_device, pixel_format_name,
};
use ffmpeg_next as ffmpeg;
use ffmpeg_next::ffi;
use std::ptr;
use tracing::{debug, info};

pub(super) struct DecoderInstance {
    pub(super) decoder: ffmpeg::decoder::Video,
    pub(super) hardware: Option<HardwareDecoderChoice>,
    _format_selection: Option<Box<HardwareFormatSelection>>,
}

struct HardwareFormatSelection {
    pixel_format: ffi::AVPixelFormat,
}

// SAFETY: the hardware format value is immutable after decoder creation. The
// decoder invokes get_format synchronously on its owner thread and is dropped
// before the boxed selection allocation.
unsafe impl Send for HardwareFormatSelection {}

impl HardwareFormatSelection {
    fn new(choice: HardwareDecoderChoice) -> Box<Self> {
        Box::new(Self {
            pixel_format: choice.pixel_format,
        })
    }
}

pub(super) fn open_decoder(
    parameters: ffmpeg::codec::Parameters,
    stream_time_base: ffmpeg::Rational,
    allow_hardware: bool,
) -> anyhow::Result<DecoderInstance> {
    let codec = ffmpeg::decoder::find(parameters.id()).ok_or(ffmpeg::Error::DecoderNotFound)?;
    if allow_hardware {
        for choice in codec_hardware_choices(unsafe { codec.as_ptr() }) {
            match open_hardware_decoder(&parameters, codec, stream_time_base, choice) {
                Ok(decoder) => return Ok(decoder),
                Err(error) => debug!(
                    "[NATIVE-CAPS] candidate={} preferred={} rejected={error:#}",
                    choice.api.label(),
                    choice.api.preferred_tier().label()
                ),
            }
        }
    }

    let mut context = ffmpeg::codec::context::Context::from_parameters(parameters)?;
    context.set_threading(ffmpeg::codec::threading::Config {
        kind: ffmpeg::codec::threading::Type::Frame,
        count: software_decode_threads(),
    });
    let mut decoder = context.decoder();
    decoder.set_packet_time_base(stream_time_base);
    Ok(DecoderInstance {
        decoder: decoder.open_as(codec)?.video()?,
        hardware: None,
        _format_selection: None,
    })
}

fn open_hardware_decoder(
    parameters: &ffmpeg::codec::Parameters,
    codec: ffmpeg::Codec,
    stream_time_base: ffmpeg::Rational,
    choice: HardwareDecoderChoice,
) -> anyhow::Result<DecoderInstance> {
    let mut device = create_device(choice).map_err(anyhow::Error::msg)?;
    let mut selection = HardwareFormatSelection::new(choice);
    let mut context = match ffmpeg::codec::context::Context::from_parameters(parameters.clone()) {
        Ok(context) => context,
        Err(error) => {
            super::capabilities::unref_device(&mut device);
            return Err(error.into());
        }
    };
    context.set_threading(ffmpeg::codec::threading::Config {
        // Hardware decode is already asynchronous in the device driver. A
        // one-worker FFmpeg frame-thread wrapper only adds packet/frame queue
        // bookkeeping around that driver submission on low-power GPUs.
        kind: ffmpeg::codec::threading::Type::None,
        count: 1,
    });
    // SAFETY: the context owns the device reference after assignment. The
    // boxed format selection has a stable address and is stored beside the
    // opened decoder until the context is destroyed.
    unsafe {
        let context_ptr = context.as_mut_ptr();
        (*context_ptr).hw_device_ctx = device;
        device = ptr::null_mut();
        (*context_ptr).opaque =
            selection.as_mut() as *mut HardwareFormatSelection as *mut std::ffi::c_void;
        (*context_ptr).get_format = Some(select_hardware_format);
    }
    debug_assert!(device.is_null());
    let mut decoder = context.decoder();
    decoder.set_packet_time_base(stream_time_base);
    let decoder = decoder.open_as(codec)?.video()?;
    info!(
        "[NATIVE-CAPS] selected={} hw_format={} preferred={} current_bridge={}",
        choice.api.label(),
        pixel_format_name(choice.pixel_format),
        choice.api.preferred_tier().label(),
        super::decode::selected_tier(Some(choice)).label()
    );
    Ok(DecoderInstance {
        decoder,
        hardware: Some(choice),
        _format_selection: Some(selection),
    })
}

unsafe extern "C" fn select_hardware_format(
    context: *mut ffi::AVCodecContext,
    formats: *const ffi::AVPixelFormat,
) -> ffi::AVPixelFormat {
    if context.is_null() || formats.is_null() {
        return ffi::AVPixelFormat::AV_PIX_FMT_NONE;
    }
    // SAFETY: the opaque pointer is installed before avcodec_open2 and the
    // terminated format list is owned by FFmpeg for this callback.
    let selection = unsafe { &mut *((*context).opaque as *mut HardwareFormatSelection) };
    let mut candidate = formats;
    loop {
        // SAFETY: FFmpeg terminates the array with AV_PIX_FMT_NONE.
        let format = unsafe { *candidate };
        if format == ffi::AVPixelFormat::AV_PIX_FMT_NONE {
            return format;
        }
        if format == selection.pixel_format {
            return format;
        }
        // SAFETY: the current entry was not the terminator.
        candidate = unsafe { candidate.add(1) };
    }
}

fn software_decode_threads() -> usize {
    std::env::var("KLD_NATIVE_DECODE_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .unwrap_or(1)
}
