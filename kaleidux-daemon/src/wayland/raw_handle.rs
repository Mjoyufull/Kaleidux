use raw_window_handle::{
    DisplayHandle, HandleError, HasDisplayHandle, HasWindowHandle, RawDisplayHandle,
    RawWindowHandle, WaylandDisplayHandle, WaylandWindowHandle, WindowHandle,
};
use smithay_client_toolkit::shell::{WaylandSurface, wlr_layer::LayerSurface};
use std::ptr::NonNull;
use wayland_client::Proxy;

/// Wrapper around LayerSurface that implements raw_window_handle traits
///
/// This struct wraps a Wayland LayerSurface and provides the raw_window_handle
/// interface required by wgpu for creating surfaces. The display_ptr must remain
/// valid for the lifetime of the Wayland connection.
pub struct RawHandleSurface {
    pub layer_surface: LayerSurface,
    /// Raw pointer to the Wayland display connection.
    ///
    /// # Safety
    ///
    /// This pointer must outlive the RawHandleSurface instance. It is obtained
    /// from `Connection::backend().display_ptr()` and remains valid as long as
    /// the Connection is alive. The Connection is stored in the main function
    /// and outlives all RawHandleSurface instances.
    pub display_ptr: *mut std::ffi::c_void,
}

// SAFETY: RawHandleSurface is safe to Send because:
// 1. LayerSurface from smithay-client-toolkit is Send (Wayland objects are thread-safe)
// 2. display_ptr is a raw pointer to the Wayland display connection, which is thread-safe
//    according to Wayland protocol. The pointer itself is just a handle and doesn't
//    need to be dropped or freed - the Connection manages the actual display lifecycle.
// 3. The display_ptr remains valid as long as the Connection exists, and the Connection
//    outlives all RawHandleSurface instances in the main function.
unsafe impl Send for RawHandleSurface {}

// SAFETY: RawHandleSurface is safe to Sync because:
// 1. LayerSurface from smithay-client-toolkit is Sync (Wayland protocol operations
//    are internally synchronized by the Wayland library)
// 2. display_ptr is only read (never written) and points to a thread-safe Wayland
//    display connection. Multiple threads can safely read this pointer value.
// 3. All operations through the pointer are done via Wayland protocol which handles
//    synchronization internally.
unsafe impl Sync for RawHandleSurface {}

impl HasWindowHandle for RawHandleSurface {
    fn window_handle(&self) -> Result<WindowHandle<'_>, HandleError> {
        let wl_surface = self.layer_surface.wl_surface();
        let object_id = wl_surface.id();
        let surface_ptr = object_id.as_ptr() as *mut std::ffi::c_void;

        // SAFETY: object_id.as_ptr() returns a valid pointer to the Wayland object.
        // The pointer is valid as long as the wl_surface exists, which is guaranteed
        // by the LayerSurface's lifetime. If the pointer is null, it indicates a
        // serious Wayland protocol error that should not occur in normal operation.
        let handle = WaylandWindowHandle::new(
            NonNull::new(surface_ptr).expect("wl_surface pointer should never be null"),
        );

        // SAFETY: The handle is created from a valid Wayland surface pointer.
        // WindowHandle::borrow_raw expects a valid RawWindowHandle, which we provide.
        // The borrow is valid for the lifetime of the WindowHandle return value.
        Ok(unsafe { WindowHandle::borrow_raw(RawWindowHandle::Wayland(handle)) })
    }
}

impl HasDisplayHandle for RawHandleSurface {
    fn display_handle(&self) -> Result<DisplayHandle<'_>, HandleError> {
        // SAFETY: display_ptr is set from Connection::backend().display_ptr() in main.rs
        // and is guaranteed to be valid as long as the Connection exists. The Connection
        // outlives all RawHandleSurface instances. If the pointer is null, it indicates
        // a serious initialization error that should not occur in normal operation.
        let handle = WaylandDisplayHandle::new(
            NonNull::new(self.display_ptr).expect("display pointer should never be null"),
        );

        // SAFETY: The handle is created from a valid Wayland display pointer.
        // DisplayHandle::borrow_raw expects a valid RawDisplayHandle, which we provide.
        // The borrow is valid for the lifetime of the DisplayHandle return value.
        Ok(unsafe { DisplayHandle::borrow_raw(RawDisplayHandle::Wayland(handle)) })
    }
}
