use gstreamer as gst;
use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, Ordering};

use super::super::{VideoFrame, VideoFrameFormat};

const DEFAULT_SOFTWARE_RENDER_FORMAT: &str = "rgba";
const RENDER_WITHOUT_TARGET_BLOCK: i32 = 0;
const MPV_RENDER_UPDATE_FRAME_FLAG: u64 =
    sys::mpv_render_update_flag_MPV_RENDER_UPDATE_FRAME as u64;

pub(super) fn capture_video_frame_with_context(
    mpv: &Mpv,
    session_id: u64,
    render_context: &SoftwareRenderContext,
    target_size: Option<(u32, u32)>,
    force_redraw: bool,
) -> anyhow::Result<Option<VideoFrame>> {
    let Some(source_size) = video_dimensions(mpv) else {
        return Ok(None);
    };
    let (width, height) = target_render_size(source_size, target_size);
    render_video_frame(session_id, render_context, width, height, force_redraw)
}

fn render_video_frame(
    session_id: u64,
    render_context: &SoftwareRenderContext,
    width: u32,
    height: u32,
    force_redraw: bool,
) -> anyhow::Result<Option<VideoFrame>> {
    let Some(SoftwareRenderedFrame { data, stride }) =
        render_context.render(width, height, force_redraw)?
    else {
        return Ok(None);
    };
    let buffer = gst::Buffer::from_mut_slice(data);
    Ok(Some(VideoFrame {
        buffer,
        width,
        height,
        stride,
        format: VideoFrameFormat::Rgba,
        session_id,
        pts_ns: None,
        duration_ns: None,
    }))
}

fn video_dimensions(mpv: &Mpv) -> Option<(u32, u32)> {
    let width = mpv
        .get_property::<i64>("dwidth")
        .or_else(|_| mpv.get_property::<i64>("width"))
        .ok()?;
    let height = mpv
        .get_property::<i64>("dheight")
        .or_else(|_| mpv.get_property::<i64>("height"))
        .ok()?;
    let width = u32::try_from(width).ok()?;
    let height = u32::try_from(height).ok()?;
    (width > 0 && height > 0).then_some((width, height))
}

fn target_render_size(source_size: (u32, u32), output_bounds: Option<(u32, u32)>) -> (u32, u32) {
    let Some(bounds) = output_bounds else {
        return source_size;
    };
    match render_size_mode() {
        RenderSizeMode::Source => source_size,
        RenderSizeMode::Output => bounds,
        RenderSizeMode::Adaptive => clamp_to_bounds_preserving_aspect(source_size, bounds),
    }
}

fn render_size_mode() -> RenderSizeMode {
    if let Ok(value) = std::env::var("KLD_MPV_RENDER_TO_OUTPUT") {
        return if env_bool_value(&value, true) {
            RenderSizeMode::Output
        } else {
            RenderSizeMode::Source
        };
    }
    std::env::var("KLD_MPV_RENDER_SIZE_MODE")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .and_then(|value| match value.as_str() {
            "source" | "native" => Some(RenderSizeMode::Source),
            "output" | "surface" => Some(RenderSizeMode::Output),
            "adaptive" | "bounded" => Some(RenderSizeMode::Adaptive),
            _ => None,
        })
        .unwrap_or(RenderSizeMode::Adaptive)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RenderSizeMode {
    Source,
    Output,
    Adaptive,
}

fn clamp_to_bounds_preserving_aspect(source_size: (u32, u32), bounds: (u32, u32)) -> (u32, u32) {
    let (source_width, source_height) = source_size;
    let (bound_width, bound_height) = bounds;
    if source_width <= bound_width && source_height <= bound_height {
        return source_size;
    }
    let width_scale = bound_width as f64 / source_width as f64;
    let height_scale = bound_height as f64 / source_height as f64;
    let scale = width_scale.min(height_scale).min(1.0);
    let width = (source_width as f64 * scale).round().max(1.0) as u32;
    let height = (source_height as f64 * scale).round().max(1.0) as u32;
    (width, height)
}

fn env_bool_value(value: &str, default: bool) -> bool {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn software_render_format() -> String {
    std::env::var("KLD_MPV_SW_FORMAT")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| matches!(value.as_str(), "rgba" | "rgb0" | "bgra" | "bgr0"))
        .unwrap_or_else(|| DEFAULT_SOFTWARE_RENDER_FORMAT.to_string())
}

struct SoftwareRenderedFrame {
    stride: u32,
    data: Vec<u8>,
}

pub(super) struct SoftwareRenderContext {
    context: *mut sys::mpv_render_context,
    update_pending: Box<AtomicBool>,
    pub(super) format: CString,
    fill_alpha: bool,
}

// SAFETY: This context is owned by one MpvPlayer and all render calls are serialized.
unsafe impl Send for SoftwareRenderContext {}

impl SoftwareRenderContext {
    pub(super) fn new(mpv: &Mpv) -> anyhow::Result<Self> {
        let api_type = CString::new("sw")?;
        let render_format = software_render_format();
        let format = CString::new(render_format.as_str())?;
        let mut params = [
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_API_TYPE,
                data: api_type.as_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];
        let mut context = std::ptr::null_mut();
        // SAFETY: libmpv reads the live parameter array only for this call.
        let result = unsafe {
            sys::mpv_render_context_create(&mut context, mpv.ctx.as_ptr(), params.as_mut_ptr())
        };
        if result < 0 {
            anyhow::bail!("mpv software render context creation failed: {}", result);
        }
        let update_pending = Box::new(AtomicBool::new(true));
        let callback_ctx = update_pending.as_ref() as *const AtomicBool as *mut _;
        // SAFETY: callback_ctx points to boxed storage owned by this context until Drop clears it.
        unsafe {
            sys::mpv_render_context_set_update_callback(
                context,
                Some(render_update_callback),
                callback_ctx,
            );
        }
        Ok(Self {
            context,
            update_pending,
            format,
            fill_alpha: render_format == "rgb0" || render_format == "bgr0",
        })
    }

    fn render(
        &self,
        width: u32,
        height: u32,
        force_redraw: bool,
    ) -> anyhow::Result<Option<SoftwareRenderedFrame>> {
        let stride = width.saturating_mul(4);
        let mut data = vec![0u8; stride as usize * height as usize];
        let size = [width as i32, height as i32];
        let mut stride_param = stride as usize;
        let mut block_for_target_time = RENDER_WITHOUT_TARGET_BLOCK;
        let mut params = [
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_SIZE,
                data: size.as_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_FORMAT,
                data: self.format.as_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_STRIDE,
                data: &mut stride_param as *mut _ as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_SW_POINTER,
                data: data.as_mut_ptr() as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_BLOCK_FOR_TARGET_TIME,
                data: &mut block_for_target_time as *mut _ as *mut _,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];

        if !force_redraw && !self.update_pending.swap(false, Ordering::SeqCst) {
            return Ok(None);
        }
        // SAFETY: self.context is the live render context owned by this wrapper.
        let update_flags = unsafe { sys::mpv_render_context_update(self.context) };
        if !force_redraw && !has_render_update_frame(update_flags) {
            return Ok(None);
        }
        // SAFETY: every parameter points to storage that remains live through the call.
        let result = unsafe { sys::mpv_render_context_render(self.context, params.as_mut_ptr()) };
        if result < 0 {
            anyhow::bail!("mpv software render failed: {}", result);
        }
        // SAFETY: self.context remains live and this call only reports the completed render.
        unsafe { sys::mpv_render_context_report_swap(self.context) };
        if self.fill_alpha {
            fill_alpha_for_rgb0(&mut data, width, height, stride);
        }
        Ok(Some(SoftwareRenderedFrame { stride, data }))
    }
}

impl Drop for SoftwareRenderContext {
    fn drop(&mut self) {
        // SAFETY: clearing the callback prevents later access to the boxed callback context.
        unsafe {
            sys::mpv_render_context_set_update_callback(self.context, None, std::ptr::null_mut());
            sys::mpv_render_context_free(self.context);
        }
    }
}

unsafe extern "C" fn render_update_callback(callback_ctx: *mut std::ffi::c_void) {
    // SAFETY: libmpv passes back the boxed AtomicBool pointer registered by new().
    let Some(update_pending) = (unsafe { (callback_ctx as *const AtomicBool).as_ref() }) else {
        return;
    };
    update_pending.store(true, Ordering::SeqCst);
}

fn has_render_update_frame(update_flags: u64) -> bool {
    update_flags & MPV_RENDER_UPDATE_FRAME_FLAG != 0
}

fn fill_alpha_for_rgb0(data: &mut [u8], width: u32, height: u32, stride: u32) {
    let row_pixels = width as usize;
    let stride = stride as usize;
    for row in 0..height as usize {
        let row_start = row * stride;
        for pixel in 0..row_pixels {
            let alpha_index = row_start + pixel * 4 + 3;
            if let Some(alpha) = data.get_mut(alpha_index) {
                *alpha = 255;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_alpha_sets_pixels_without_touching_padding() {
        let mut data = vec![7u8; 24];
        fill_alpha_for_rgb0(&mut data, 2, 2, 12);
        assert_eq!([data[3], data[7], data[15], data[19]], [255; 4]);
        assert_eq!([data[8], data[11], data[20], data[23]], [7; 4]);
    }

    #[test]
    fn render_update_check_uses_mpv_frame_bit() {
        assert!(has_render_update_frame(MPV_RENDER_UPDATE_FRAME_FLAG));
        assert!(!has_render_update_frame(0));
    }

    #[test]
    fn adaptive_bounds_preserve_aspect() {
        assert_eq!(
            clamp_to_bounds_preserving_aspect((3840, 2160), (1920, 1080)),
            (1920, 1080)
        );
        assert_eq!(
            clamp_to_bounds_preserving_aspect((2160, 3840), (1920, 1080)),
            (608, 1080)
        );
    }
}
