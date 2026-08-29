use std::ffi::c_void;
use std::sync::Arc;
use wayland_client::backend::ObjectId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MpvRenderApiRequest {
    ComposedSoftware,
    ComposedGl,
    NativeGlOverlayDiagnostic,
    DeprecatedNativeGlAlias,
    Unknown,
}

impl MpvRenderApiRequest {
    pub(crate) fn from_env() -> Self {
        match std::env::var("KLD_MPV_RENDER_API")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None => Self::ComposedGl,
            Some("" | "sw" | "software" | "cpu" | "composed") => Self::ComposedSoftware,
            Some("gl-composed" | "opengl-composed" | "gpu-composed") => Self::ComposedGl,
            Some(
                "overlay" | "gl-overlay" | "opengl-overlay" | "native-overlay" | "wayland-overlay",
            ) => Self::NativeGlOverlayDiagnostic,
            Some("gl" | "opengl" | "native" | "wayland") => Self::DeprecatedNativeGlAlias,
            Some(_) => Self::Unknown,
        }
    }

    pub(crate) fn enables_native_overlay(self) -> bool {
        self == Self::NativeGlOverlayDiagnostic
    }

    pub(crate) fn enables_composed_gl(self) -> bool {
        self == Self::ComposedGl
    }
}

#[derive(Clone, Debug)]
pub struct MpvNativeVideoTarget;

impl MpvNativeVideoTarget {
    pub(crate) fn new(
        _display_ptr: *mut c_void,
        _surface_id: ObjectId,
        _width: u32,
        _height: u32,
    ) -> Option<Self> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct MpvComposedVideoTarget;

impl MpvComposedVideoTarget {
    pub(crate) fn new(
        _display_ptr: *mut c_void,
        _wgpu_ctx: Arc<crate::renderer::WgpuContext>,
        _width: u32,
        _height: u32,
    ) -> Option<Self> {
        None
    }

    #[cfg(feature = "display-x11")]
    pub(crate) fn new_xcb(
        _connection_ptr: *mut c_void,
        _wgpu_ctx: Arc<crate::renderer::WgpuContext>,
        _width: u32,
        _height: u32,
    ) -> Option<Self> {
        None
    }
}
