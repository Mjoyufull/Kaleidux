use raw_window_handle::{
    DisplayHandle, HandleError, HasDisplayHandle, HasWindowHandle, RawDisplayHandle,
    RawWindowHandle, WaylandDisplayHandle, WaylandWindowHandle, WindowHandle,
};
use smithay_client_toolkit::{
    compositor::{CompositorHandler, CompositorState},
    delegate_compositor, delegate_layer, delegate_output, delegate_registry, delegate_shm,
    output::{OutputHandler, OutputState},
    registry::{ProvidesRegistryState, RegistryState},
    registry_handlers,
    shell::{
        wlr_layer::{
            Anchor, Layer, LayerShell, LayerShellHandler, LayerSurface, LayerSurfaceConfigure,
        },
        WaylandSurface,
    },
    shm::{Shm, ShmHandler},
};
use std::ptr::NonNull;
use tracing::info;
use wayland_client::{
    globals::GlobalList,
    protocol::{wl_output, wl_surface},
    Connection, Proxy, QueueHandle,
};

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

pub struct WaylandBackend {
    pub registry_state: RegistryState,
    pub compositor: CompositorState,
    pub output_state: OutputState,
    pub layer_shell: LayerShell,
    pub shm: Shm,
    pub surfaces: Vec<(String, LayerSurface)>,
    // (name, width, height, serial)
    pub pending_resizes: Vec<(String, u32, u32, u32)>,
    // Frame callback notifications: surface name -> should render
    pub frame_callback_ready: std::collections::HashSet<String>,
}

impl WaylandBackend {
    pub fn new(globals: &GlobalList, qh: &QueueHandle<Self>) -> anyhow::Result<Self> {
        let registry_state = RegistryState::new(globals);
        let compositor = CompositorState::bind(globals, qh)?;
        let layer_shell = LayerShell::bind(globals, qh)?;
        let shm = Shm::bind(globals, qh)?;
        let output_state = OutputState::new(globals, qh);

        Ok(Self {
            registry_state,
            compositor,
            output_state,
            layer_shell,
            shm,
            surfaces: Vec::new(),
            pending_resizes: Vec::new(),
            frame_callback_ready: std::collections::HashSet::new(),
        })
    }

    pub fn create_wallpaper_surface(
        &mut self,
        output: &wl_output::WlOutput,
        qh: &QueueHandle<Self>,
        name: String,
        layer: Layer,
    ) -> anyhow::Result<LayerSurface> {
        let wl_surface = self.compositor.create_surface(qh);

        let layer_surface = self.layer_shell.create_layer_surface(
            qh,
            wl_surface,
            layer,
            Some("kaleidux-wallpaper"),
            Some(output),
        );

        // Match gSlapper initialization
        layer_surface.set_size(0, 0);
        layer_surface.set_anchor(Anchor::all());
        layer_surface.set_exclusive_zone(-1);
        layer_surface.commit();

        // Keep track of them
        self.surfaces.push((name, layer_surface.clone()));

        Ok(layer_surface)
    }
}

// Boilerplate delegates for SCTK
delegate_registry!(WaylandBackend);
delegate_compositor!(WaylandBackend);
delegate_output!(WaylandBackend);
delegate_shm!(WaylandBackend);
delegate_layer!(WaylandBackend);

impl ProvidesRegistryState for WaylandBackend {
    fn registry(&mut self) -> &mut RegistryState {
        &mut self.registry_state
    }
    registry_handlers![OutputState];
}

impl CompositorHandler for WaylandBackend {
    fn scale_factor_changed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &wl_surface::WlSurface,
        _new_factor: i32,
    ) {
    }

    /// Frame callback handler - called when compositor is ready for a new frame
    /// This is the proper Wayland way: wait for compositor to signal readiness before rendering
    fn frame(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        surface: &wl_surface::WlSurface,
        time: u32,
    ) {
        tracing::trace!(
            "[WAYLAND] [TRACE] Frame event for surface #{} (time={})",
            surface.id().protocol_id(),
            time
        );
        // Find which output this surface belongs to
        let name = self
            .surfaces
            .iter()
            .find(|(_, s)| s.wl_surface() == surface)
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| "unknown".to_string());

        if name != "unknown" {
            tracing::debug!("[FRAME] Frame callback received for output: {}", name);
            // Signal that this renderer should render now
            self.frame_callback_ready.insert(name);
        }
    }

    fn transform_changed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &wl_surface::WlSurface,
        _new_transform: wl_output::Transform,
    ) {
    }
    fn surface_enter(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &wl_surface::WlSurface,
        _output: &wl_output::WlOutput,
    ) {
    }
    fn surface_leave(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _surface: &wl_surface::WlSurface,
        _output: &wl_output::WlOutput,
    ) {
    }
}

impl OutputHandler for WaylandBackend {
    fn output_state(&mut self) -> &mut OutputState {
        &mut self.output_state
    }
    fn new_output(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
    }
    fn update_output(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
    }
    fn output_destroyed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
    }
}

impl LayerShellHandler for WaylandBackend {
    fn closed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        layer_surface: &LayerSurface,
    ) {
        // Find the name of the surface being closed
        let name = self
            .surfaces
            .iter()
            .find(|(_, s)| s.wl_surface() == layer_surface.wl_surface())
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| "unknown".to_string());

        tracing::warn!("Layer surface CLOSED by compositor for output: {}. Surface will be re-created if output still exists.", name);
        self.surfaces.retain(|(_, s)| s != layer_surface);
    }
    fn configure(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        layer_surface: &LayerSurface,
        config: LayerSurfaceConfigure,
        serial: u32,
    ) {
        let (width, height) = config.new_size;

        // Find which output name this belongs to
        let name = self
            .surfaces
            .iter()
            .find(|(_, s)| s.wl_surface() == layer_surface.wl_surface())
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| "unknown".to_string());

        let protocol_id = layer_surface.wl_surface().id().protocol_id();
        info!(
            "Configure event received for output {} (id: #{}): size {}x{}, serial {}",
            name, protocol_id, width, height, serial
        );
        tracing::trace!("[WAYLAND] [TRACE] Configure details: name={}, id=#{}, w={}, h={}, serial={}, suggest_resize={:?}, suggest_rescale={:?}", 
            name, protocol_id, width, height, serial, config.new_size, config.new_size);

        // NOTE: SCTK 0.19.2 handles ack_configure(serial) AUTOMATICALLY before calling this handler.
        // Calling it here again causes a FATAL "Serial invalid" protocol error.

        // We also DO NOT call layer_surface.commit() here.
        // We let WGPU's present() handle it, or we rely on the initial commit during creation.

        // Store resize for main loop
        if name != "unknown" {
            self.pending_resizes.push((name, width, height, serial));
        }
    }
}

impl ShmHandler for WaylandBackend {
    fn shm_state(&mut self) -> &mut Shm {
        &mut self.shm
    }
}
