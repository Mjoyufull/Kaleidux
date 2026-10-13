pub(crate) mod color;
pub(crate) mod frame_callbacks;
pub(crate) mod hotplug;
pub(crate) mod native_dmabuf_surface;
pub(crate) mod presentation;
pub(crate) mod startup;
mod surface_protocols;

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
use smithay_client_toolkit::{
    delegate_dmabuf, delegate_simple, delegate_subcompositor, dmabuf::DmabufState,
    registry::SimpleGlobal, subcompositor::SubcompositorState,
};
use std::sync::OnceLock;
use tracing::{info, warn};
use wayland_client::{
    Connection, Proxy, QueueHandle,
    globals::GlobalList,
    protocol::{wl_output, wl_region, wl_surface},
};
use wayland_protocols::wp::color_management::v1::client::{
    wp_color_management_surface_v1::WpColorManagementSurfaceV1,
    wp_color_manager_v1::WpColorManagerV1,
    wp_image_description_creator_params_v1::WpImageDescriptionCreatorParamsV1,
    wp_image_description_v1::WpImageDescriptionV1,
};
use wayland_protocols::wp::content_type::v1::client::{
    wp_content_type_manager_v1::WpContentTypeManagerV1,
    wp_content_type_v1::{self, WpContentTypeV1},
};
use wayland_protocols::wp::fractional_scale::v1::client::{
    wp_fractional_scale_manager_v1::WpFractionalScaleManagerV1,
    wp_fractional_scale_v1::WpFractionalScaleV1,
};
use wayland_protocols::wp::linux_drm_syncobj::v1::client::{
    wp_linux_drm_syncobj_manager_v1::WpLinuxDrmSyncobjManagerV1,
    wp_linux_drm_syncobj_surface_v1::WpLinuxDrmSyncobjSurfaceV1,
    wp_linux_drm_syncobj_timeline_v1::WpLinuxDrmSyncobjTimelineV1,
};
use wayland_protocols::wp::linux_explicit_synchronization::zv1::client::{
    zwp_linux_explicit_synchronization_v1::ZwpLinuxExplicitSynchronizationV1,
    zwp_linux_surface_synchronization_v1::ZwpLinuxSurfaceSynchronizationV1,
};
use wayland_protocols::wp::presentation_time::client::wp_presentation::WpPresentation;
use wayland_protocols::wp::viewporter::client::wp_viewport::WpViewport;
use wayland_protocols::wp::viewporter::client::wp_viewporter::WpViewporter;

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
    dmabuf_state: DmabufState,
    subcompositor_state: SubcompositorState,
    viewporter: SimpleGlobal<WpViewporter, 1>,
    presentation: Option<SimpleGlobal<WpPresentation, 2>>,
    pub(crate) presentation_telemetry: presentation::PresentationTelemetry,
    fractional_scale_manager: Option<SimpleGlobal<WpFractionalScaleManagerV1, 1>>,
    content_type_manager: Option<SimpleGlobal<WpContentTypeManagerV1, 1>>,
    surface_viewports: std::collections::HashMap<String, WpViewport>,
    fractional_scale_objects: std::collections::HashMap<String, WpFractionalScaleV1>,
    content_type_objects: std::collections::HashMap<String, WpContentTypeV1>,
    surface_content_types: std::collections::HashMap<String, crate::queue::ContentType>,
    pub(crate) preferred_fractional_scales: std::collections::HashMap<String, u32>,
    pub(crate) logical_surface_sizes: std::collections::HashMap<String, (u32, u32)>,
    pub(crate) color_manager: Option<SimpleGlobal<WpColorManagerV1, 2>>,
    pub(crate) color_capabilities: color::Capabilities,
    pub(crate) color_surfaces: std::collections::HashMap<String, WpColorManagementSurfaceV1>,
    pub(crate) pending_color_descriptions: std::collections::HashMap<String, WpImageDescriptionV1>,
    explicit_sync: Option<SimpleGlobal<ZwpLinuxExplicitSynchronizationV1, 2>>,
    drm_syncobj: Option<SimpleGlobal<WpLinuxDrmSyncobjManagerV1, 1>>,
    native_dmabuf: native_dmabuf_surface::NativeDmabufPresentation,
    pub mpv_video_surfaces: std::collections::HashMap<String, LayerSurface>,
    // (name, width, height, serial)
    pub pending_resizes: Vec<(String, u32, u32, u32)>,
    // Frame callback notifications: surface name -> should render
    pub frame_callback_ready: std::collections::HashSet<String>,
    // Output membership changes are rare. Keep hotplug cleanup event-driven
    // instead of rebuilding an output-name set on every video/release wake.
    pub outputs_changed: bool,
    pub(crate) closed_outputs: std::collections::HashSet<String>,
    // Vulkan WSI owns a private Wayland queue for DMA-BUF surface feedback.
    // Drain it once more after topology events have reached that queue.
    pub(crate) hotplug_wsi_drain_deadline: Option<std::time::Instant>,
}

impl WaylandBackend {
    pub fn new(globals: &GlobalList, qh: &QueueHandle<Self>) -> anyhow::Result<Self> {
        let registry_state = RegistryState::new(globals);
        let compositor = CompositorState::bind(globals, qh)?;
        let layer_shell = LayerShell::bind(globals, qh)?;
        let shm = Shm::bind(globals, qh)?;
        let output_state = OutputState::new(globals, qh);
        let dmabuf_state = DmabufState::new(globals, qh);
        let subcompositor_state =
            SubcompositorState::bind(compositor.wl_compositor().clone(), globals, qh)?;
        let viewporter = SimpleGlobal::<WpViewporter, 1>::bind(globals, qh)?;
        let presentation = SimpleGlobal::<WpPresentation, 2>::bind(globals, qh).ok();
        let fractional_scale_manager =
            SimpleGlobal::<WpFractionalScaleManagerV1, 1>::bind(globals, qh).ok();
        let content_type_manager =
            SimpleGlobal::<WpContentTypeManagerV1, 1>::bind(globals, qh).ok();
        let color_manager = SimpleGlobal::<WpColorManagerV1, 2>::bind(globals, qh).ok();
        let explicit_sync =
            SimpleGlobal::<ZwpLinuxExplicitSynchronizationV1, 2>::bind(globals, qh).ok();
        let drm_syncobj = SimpleGlobal::<WpLinuxDrmSyncobjManagerV1, 1>::bind(globals, qh).ok();

        Ok(Self {
            registry_state,
            compositor,
            output_state,
            layer_shell,
            shm,
            surfaces: std::collections::HashMap::new(),
            dmabuf_state,
            subcompositor_state,
            viewporter,
            presentation,
            presentation_telemetry: presentation::PresentationTelemetry::default(),
            fractional_scale_manager,
            content_type_manager,
            surface_viewports: std::collections::HashMap::new(),
            fractional_scale_objects: std::collections::HashMap::new(),
            content_type_objects: std::collections::HashMap::new(),
            surface_content_types: std::collections::HashMap::new(),
            preferred_fractional_scales: std::collections::HashMap::new(),
            logical_surface_sizes: std::collections::HashMap::new(),
            color_manager,
            color_capabilities: color::Capabilities::default(),
            color_surfaces: std::collections::HashMap::new(),
            pending_color_descriptions: std::collections::HashMap::new(),
            explicit_sync,
            drm_syncobj,
            native_dmabuf: native_dmabuf_surface::NativeDmabufPresentation::default(),
            mpv_video_surfaces: std::collections::HashMap::new(),
            pending_resizes: Vec::new(),
            frame_callback_ready: std::collections::HashSet::new(),
            outputs_changed: false,
            closed_outputs: std::collections::HashSet::new(),
            hotplug_wsi_drain_deadline: None,
        })
    }

    pub(crate) fn presentation_proxy(&self) -> Option<&WpPresentation> {
        self.presentation
            .as_ref()
            .and_then(|global| global.get().ok())
    }

    fn configure_wallpaper_surface_protocols(
        &mut self,
        name: &str,
        surface: &wl_surface::WlSurface,
        qh: &QueueHandle<Self>,
    ) {
        surface.set_buffer_scale(1);
        if let Ok(viewporter) = self.viewporter.get() {
            self.surface_viewports
                .insert(name.to_owned(), viewporter.get_viewport(surface, qh, ()));
        }
        if let Some(manager) = self
            .fractional_scale_manager
            .as_ref()
            .and_then(|global| global.get().ok())
        {
            self.fractional_scale_objects.insert(
                name.to_owned(),
                manager.get_fractional_scale(surface, qh, name.to_owned()),
            );
        }
        if let Some(manager) = self
            .content_type_manager
            .as_ref()
            .and_then(|global| global.get().ok())
        {
            let content = manager.get_surface_content_type(surface, qh, ());
            content.set_content_type(wp_content_type_v1::Type::Photo);
            self.content_type_objects.insert(name.to_owned(), content);
            self.surface_content_types
                .insert(name.to_owned(), crate::queue::ContentType::Image);
        }
        if self.color_manager.is_some() {
            tracing::info!(
                "[WAYLAND-COLOR] output={} WGPU owns the main surface color object; composed frames use the explicit single-pass SDR policy, native DMA-BUF subsurfaces receive Kaleidux image descriptions",
                name
            );
        }
    }

    pub(crate) fn set_surface_content_type(
        &mut self,
        name: &str,
        content_type: crate::queue::ContentType,
    ) {
        if self.surface_content_types.get(name) == Some(&content_type) {
            return;
        }
        let Some(protocol) = self.content_type_objects.get(name) else {
            return;
        };
        protocol.set_content_type(match content_type {
            crate::queue::ContentType::Image => wp_content_type_v1::Type::Photo,
            crate::queue::ContentType::Video => wp_content_type_v1::Type::Video,
        });
        self.surface_content_types
            .insert(name.to_owned(), content_type);
        tracing::debug!("[WAYLAND-CONTENT] output={} type={content_type:?}", name);
    }

    pub(crate) fn queue_fractional_resize(
        &mut self,
        name: &str,
        logical_width: u32,
        logical_height: u32,
        serial: u32,
    ) {
        let logical_width = logical_width.max(1);
        let logical_height = logical_height.max(1);
        self.logical_surface_sizes
            .insert(name.to_owned(), (logical_width, logical_height));
        if let Some(viewport) = self.surface_viewports.get(name) {
            viewport.set_destination(logical_width as i32, logical_height as i32);
        }
        let scale = self
            .preferred_fractional_scales
            .get(name)
            .copied()
            .unwrap_or(120);
        let buffer_width = fractional_buffer_extent(logical_width, scale);
        let buffer_height = fractional_buffer_extent(logical_height, scale);
        self.pending_resizes
            .push((name.to_owned(), buffer_width, buffer_height, serial));
    }

    pub(crate) fn set_surface_opaque_region(
        &self,
        surface: &wl_surface::WlSurface,
        width: u32,
        height: u32,
        qh: &QueueHandle<Self>,
    ) {
        let region = self.compositor.wl_compositor().create_region(qh, ());
        region.add(0, 0, width.max(1) as i32, height.max(1) as i32);
        surface.set_opaque_region(Some(&region));
        region.destroy();
    }

    pub(crate) fn retire_surface_protocols(&mut self, name: &str) {
        if let Some(object) = self.fractional_scale_objects.remove(name) {
            object.destroy();
        }
        if let Some(object) = self.content_type_objects.remove(name) {
            object.destroy();
        }
        if let Some(viewport) = self.surface_viewports.remove(name) {
            viewport.destroy();
        }
        self.surface_content_types.remove(name);
        self.preferred_fractional_scales.remove(name);
        self.logical_surface_sizes.remove(name);
        self.retire_color_surface(name);
    }

    pub(crate) fn logical_surface_size(&self, name: &str, fallback: (u32, u32)) -> (u32, u32) {
        self.logical_surface_sizes
            .get(name)
            .copied()
            .unwrap_or(fallback)
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

    pub fn create_mpv_video_surface(
        &mut self,
        output: &wl_output::WlOutput,
        qh: &QueueHandle<Self>,
        name: String,
        layer: Layer,
    ) -> anyhow::Result<LayerSurface> {
        let layer_surface =
            self.create_layer_surface_internal(output, qh, layer, "kaleidux-mpv-video")?;

        if let Some(_prev) = self
            .mpv_video_surfaces
            .insert(name.clone(), layer_surface.clone())
        {
            warn!("[WAYLAND] Replacing existing mpv LayerSurface for {}", name);
        }

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

        self.configure_wallpaper_surface_protocols(&name, layer_surface.wl_surface(), qh);

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

    pub fn find_mpv_surface_name(&self, surface: &wl_surface::WlSurface) -> Option<String> {
        self.mpv_video_surfaces
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
delegate_dmabuf!(WaylandBackend);
delegate_subcompositor!(WaylandBackend);
delegate_simple!(WaylandBackend, WpViewporter, 1);
delegate_simple!(WaylandBackend, WpViewport, 1);
delegate_simple!(WaylandBackend, WpFractionalScaleManagerV1, 1);
delegate_simple!(WaylandBackend, WpContentTypeManagerV1, 1);
delegate_simple!(WaylandBackend, WpContentTypeV1, 1);
delegate_simple!(WaylandBackend, WpColorManagementSurfaceV1, 2);
wayland_client::delegate_noop!(WaylandBackend: ignore WpImageDescriptionCreatorParamsV1);
delegate_simple!(WaylandBackend, ZwpLinuxExplicitSynchronizationV1, 2);
delegate_simple!(WaylandBackend, ZwpLinuxSurfaceSynchronizationV1, 2);
delegate_simple!(WaylandBackend, WpLinuxDrmSyncobjManagerV1, 1);
delegate_simple!(WaylandBackend, WpLinuxDrmSyncobjSurfaceV1, 1);
delegate_simple!(WaylandBackend, WpLinuxDrmSyncobjTimelineV1, 1);
wayland_client::delegate_noop!(WaylandBackend: ignore wl_region::WlRegion);

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
        self.outputs_changed = true;
    }
    fn update_output(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
        self.outputs_changed = true;
    }
    fn output_destroyed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _output: wl_output::WlOutput,
    ) {
        self.outputs_changed = true;
    }
}

impl LayerShellHandler for WaylandBackend {
    fn closed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        layer_surface: &LayerSurface,
    ) {
        let renderer_name = self.find_renderer_surface_name(layer_surface.wl_surface());
        let name = renderer_name
            .clone()
            .or_else(|| self.find_mpv_surface_name(layer_surface.wl_surface()))
            .unwrap_or_else(|| "unknown".to_string());

        tracing::warn!(
            "Layer surface CLOSED by compositor for output: {}. Surface will be re-created if output still exists.",
            name
        );
        self.surfaces
            .retain(|_, s| s.wl_surface() != layer_surface.wl_surface());
        self.mpv_video_surfaces
            .retain(|_, s| s.wl_surface() != layer_surface.wl_surface());
        if name != "unknown" {
            if renderer_name.is_some() {
                self.retire_native_dmabuf_output(&name);
                self.retire_surface_protocols(&name);
            }
            self.closed_outputs.insert(name);
            self.outputs_changed = true;
        }
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
        let mpv_name = self.find_mpv_surface_name(layer_surface.wl_surface());
        let name = renderer_name
            .as_ref()
            .cloned()
            .or_else(|| mpv_name.as_ref().cloned())
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
            self.set_surface_opaque_region(
                layer_surface.wl_surface(),
                width.max(1),
                height.max(1),
                _qh,
            );
            self.queue_fractional_resize(&name, width, height, serial);
        }
    }
}

pub(crate) fn fractional_buffer_extent(logical: u32, scale_120: u32) -> u32 {
    let numerator = u64::from(logical.max(1)) * u64::from(scale_120.max(1));
    numerator.div_ceil(120).clamp(1, u64::from(u32::MAX)) as u32
}

impl ShmHandler for WaylandBackend {
    fn shm_state(&mut self) -> &mut Shm {
        &mut self.shm
    }
}
