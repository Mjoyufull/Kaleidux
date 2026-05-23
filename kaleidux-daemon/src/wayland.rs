pub(crate) mod frame_callbacks;
pub(crate) mod startup;

use smithay_client_toolkit::{
    compositor::{CompositorHandler, CompositorState},
    delegate_compositor, delegate_layer, delegate_output, delegate_registry, delegate_shm,
    output::{OutputHandler, OutputState},
    registry::{ProvidesRegistryState, RegistryState},
    registry_handlers,
    shell::{
        WaylandSurface,
        wlr_layer::{
            Anchor, Layer, LayerShell, LayerShellHandler, LayerSurface, LayerSurfaceConfigure,
        },
    },
    shm::{Shm, ShmHandler},
};
use std::sync::OnceLock;
use tracing::{info, warn};
use wayland_client::{
    Connection, Proxy, QueueHandle,
    globals::GlobalList,
    protocol::{wl_output, wl_surface},
};

pub(crate) fn trace_frame_events_enabled() -> bool {
    if crate::observability::trace_all::trace_all_enabled() {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_TRACE_FRAME_EVENTS")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

pub(crate) fn video_immediate_present_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_VIDEO_IMMEDIATE_PRESENT")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

#[path = "wayland/raw_handle.rs"]
mod raw_handle;
pub use raw_handle::RawHandleSurface;

pub struct WaylandBackend {
    pub registry_state: RegistryState,
    pub compositor: CompositorState,
    pub output_state: OutputState,
    pub layer_shell: LayerShell,
    pub shm: Shm,
    pub surfaces: std::collections::HashMap<String, LayerSurface>,
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
            surfaces: std::collections::HashMap::new(),
            pending_resizes: Vec::new(),
            frame_callback_ready: std::collections::HashSet::new(),
        })
    }

    fn create_layer_surface_internal(
        &mut self,
        output: &wl_output::WlOutput,
        qh: &QueueHandle<Self>,
        layer: Layer,
        namespace: &'static str,
    ) -> anyhow::Result<LayerSurface> {
        let wl_surface = self.compositor.create_surface(qh);

        let layer_surface = self.layer_shell.create_layer_surface(
            qh,
            wl_surface,
            layer,
            Some(namespace),
            Some(output),
        );

        // Match gSlapper initialization
        layer_surface.set_size(0, 0);
        layer_surface.set_anchor(Anchor::all());
        layer_surface.set_exclusive_zone(-1);
        layer_surface.commit();

        Ok(layer_surface)
    }

    pub fn create_wallpaper_surface(
        &mut self,
        output: &wl_output::WlOutput,
        qh: &QueueHandle<Self>,
        name: String,
        layer: Layer,
    ) -> anyhow::Result<LayerSurface> {
        let layer_surface =
            self.create_layer_surface_internal(output, qh, layer, "kaleidux-wallpaper")?;

        if let Some(_prev) = self.surfaces.insert(name.clone(), layer_surface.clone()) {
            warn!("[WAYLAND] Replacing existing LayerSurface for {}", name);
        }

        Ok(layer_surface)
    }

    pub fn find_renderer_surface_name(&self, surface: &wl_surface::WlSurface) -> Option<String> {
        self.surfaces
            .iter()
            .find(|(_, s)| s.wl_surface() == surface)
            .map(|(n, _)| n.clone())
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
        if trace_frame_events_enabled() {
            tracing::trace!(
                "[WAYLAND] [TRACE] Frame event for surface #{} (time={})",
                surface.id().protocol_id(),
                time
            );
        }
        // Only renderer-owned surfaces should wake the renderer loop.
        if let Some(name) = self.find_renderer_surface_name(surface) {
            if trace_frame_events_enabled() {
                tracing::trace!(
                    "[FRAME] Renderer frame callback received for output: {}",
                    name
                );
            }
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
            .find_renderer_surface_name(layer_surface.wl_surface())
            .unwrap_or_else(|| "unknown".to_string());

        tracing::warn!(
            "Layer surface CLOSED by compositor for output: {}. Surface will be re-created if output still exists.",
            name
        );
        self.surfaces
            .retain(|_, s| s.wl_surface() != layer_surface.wl_surface());
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

        let renderer_name = self.find_renderer_surface_name(layer_surface.wl_surface());
        let name = renderer_name
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let protocol_id = layer_surface.wl_surface().id().protocol_id();
        info!(
            "Configure event received for output {} (id: #{}): size {}x{}, serial {}",
            name, protocol_id, width, height, serial
        );
        if trace_frame_events_enabled() {
            tracing::trace!(
                "[WAYLAND] [TRACE] Configure details: name={}, id=#{}, w={}, h={}, serial={}, suggest_resize={:?}",
                name,
                protocol_id,
                width,
                height,
                serial,
                config.new_size
            );
        }

        // NOTE: SCTK 0.19.2 handles ack_configure(serial) AUTOMATICALLY before calling this handler.
        // Calling it here again causes a FATAL "Serial invalid" protocol error.

        // We also DO NOT call layer_surface.commit() here.
        // We let WGPU's present() handle it, or we rely on the initial commit during creation.

        if let Some(name) = renderer_name {
            self.pending_resizes.push((name, width, height, serial));
        }
    }
}

impl ShmHandler for WaylandBackend {
    fn shm_state(&mut self) -> &mut Shm {
        &mut self.shm
    }
}
