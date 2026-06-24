use super::super::mpv_native::{MpvComposedVideoTarget, MpvNativeVideoTarget, MpvRenderApiRequest};
use libmpv2::MpvInitializer;
use tracing::warn;

const DEFAULT_MPV_CAPTURE_FPS: u32 = 48;
const MAX_MPV_CAPTURE_FPS: u32 = 120;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MpvRenderApi {
    ComposedGlExperimental,
    NativeGlOverlayExperimental,
    Software,
}

impl MpvRenderApi {
    pub(super) fn is_native_gl(self) -> bool {
        self == Self::NativeGlOverlayExperimental
    }

    pub(super) fn is_composed_gl(self) -> bool {
        self == Self::ComposedGlExperimental
    }
}

pub(super) fn render_api(
    native_target: Option<&MpvNativeVideoTarget>,
    composed_target: Option<&MpvComposedVideoTarget>,
) -> MpvRenderApi {
    render_api_for_request(
        MpvRenderApiRequest::from_env(),
        native_target.is_some(),
        composed_target.is_some(),
    )
}

fn render_api_for_request(
    request: MpvRenderApiRequest,
    native_target_available: bool,
    composed_target_available: bool,
) -> MpvRenderApi {
    if request.enables_composed_gl() && composed_target_available {
        MpvRenderApi::ComposedGlExperimental
    } else if request.enables_native_overlay() && native_target_available {
        MpvRenderApi::NativeGlOverlayExperimental
    } else {
        MpvRenderApi::Software
    }
}

pub(super) fn capture_fps(max_publish_fps: Option<u32>) -> u32 {
    std::env::var("KLD_MPV_CAPTURE_FPS")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|fps| *fps > 0)
        .or(max_publish_fps)
        .unwrap_or(DEFAULT_MPV_CAPTURE_FPS)
        .clamp(1, MAX_MPV_CAPTURE_FPS)
}

pub(super) fn normalized_render_bounds(render_size: Option<(u32, u32)>) -> Option<(u32, u32)> {
    render_size
        .filter(|(width, height)| *width > 0 && *height > 0)
        .map(|(width, height)| (width.max(1), height.max(1)))
}

pub(super) fn hwdec_mode() -> String {
    std::env::var("KLD_MPV_HWDEC")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "auto-safe".to_string())
}

pub(super) fn apply_fast_gpu_options(init: &MpvInitializer) {
    if mpv_quality_mode().eq_ignore_ascii_case("default") {
        return;
    }

    set_optional(init, "vd-lavc-dr", "yes");
    set_optional(init, "video-timing-offset", 0.0f64);
    set_optional(init, "interpolation", false);
    set_optional(init, "scale", "bilinear");
    set_optional(init, "cscale", "bilinear");
    set_optional(init, "dscale", "bilinear");
    set_optional(init, "correct-downscaling", false);
    set_optional(init, "linear-downscaling", false);
    set_optional(init, "linear-upscaling", false);
    set_optional(init, "sigmoid-upscaling", false);
    set_optional(init, "deband", false);
    set_optional(init, "dither", "no");
    set_optional(init, "dither-depth", "no");
    set_optional(init, "temporal-dither", false);
    set_optional(init, "gpu-dumb-mode", "yes");
}

fn mpv_quality_mode() -> String {
    std::env::var("KLD_MPV_QUALITY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "fast".to_string())
}

fn set_optional<T>(init: &MpvInitializer, name: &str, value: T)
where
    T: libmpv2::SetData,
{
    if let Err(error) = init.set_option(name, value) {
        warn!("[VIDEO] libmpv ignored {name} option: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_gl_overlay_requires_explicit_request_and_target() {
        assert_eq!(
            render_api_for_request(
                MpvRenderApiRequest::NativeGlOverlayExperimental,
                true,
                false
            ),
            MpvRenderApi::NativeGlOverlayExperimental
        );
        assert_eq!(
            render_api_for_request(MpvRenderApiRequest::ComposedSoftware, true, true),
            MpvRenderApi::Software
        );
        assert_eq!(
            render_api_for_request(
                MpvRenderApiRequest::NativeGlOverlayExperimental,
                false,
                false
            ),
            MpvRenderApi::Software
        );
    }

    #[test]
    fn composed_gl_requires_explicit_request_and_target() {
        assert_eq!(
            render_api_for_request(MpvRenderApiRequest::ComposedGlExperimental, false, true),
            MpvRenderApi::ComposedGlExperimental
        );
        assert_eq!(
            render_api_for_request(MpvRenderApiRequest::ComposedGlExperimental, false, false),
            MpvRenderApi::Software
        );
    }
}
