use crate::shaders::Transition;
use bytemuck::{Pod, Zeroable};
use raw_window_handle::{HasDisplayHandle, HasWindowHandle};
use smithay_client_toolkit::shell::wlr_layer::LayerSurface;
use std::sync::Arc;
use tracing::info;
use wayland_client::QueueHandle;
use wgpu::{Surface, SurfaceConfiguration};

#[path = "renderer/context.rs"]
mod context;
mod context_pipelines;
pub use context::WgpuContext;

#[path = "renderer/texture.rs"]
mod texture;
pub use texture::{RetainedTextureFootprint, compute_cover_target_dimensions};

#[path = "renderer/surface.rs"]
mod surface;
use surface::select_present_mode;

#[path = "renderer/video_layout.rs"]
mod video_layout;

#[path = "renderer/pipeline_cache.rs"]
mod pipeline_cache;

#[path = "renderer/transitions.rs"]
mod transitions;

#[path = "renderer/frame_callback.rs"]
mod frame_callback;

#[path = "renderer/resources.rs"]
mod resources;

#[path = "renderer/image_upload.rs"]
mod image_upload;

#[path = "renderer/state.rs"]
mod state;

#[path = "renderer/video_interop.rs"]
mod video_interop;

#[path = "renderer/video_cpu_upload.rs"]
mod video_cpu_upload;
mod video_rgba_upload;

#[path = "renderer/video_upload.rs"]
mod video_upload;
mod video_zero_copy_upload;

mod render_blit;
#[path = "renderer/render_present.rs"]
mod render_present;
mod render_transition;

#[path = "renderer/lifecycle.rs"]
mod lifecycle;

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
struct TransitionUniforms {
    progress: f32,
    screen_aspect: f32, // width / height
    prev_aspect: f32,
    next_aspect: f32,
    params: [[f32; 4]; 7], // Total 128 bytes (aligned)
}

#[derive(Debug, Clone)]
pub struct TransitionStats {
    pub start_time: std::time::Instant,
    pub frame_count: u64,
    pub target_duration: f32,
    pub batch_id: Option<u64>,
}

pub enum BackendContext<'a> {
    Wayland {
        #[allow(dead_code)]
        surface: &'a LayerSurface,
        #[allow(dead_code)]
        qh: &'a QueueHandle<crate::wayland::WaylandBackend>,
    },
    X11,
}

const MAX_TEXTURE_POOL_SIZE: usize = 16; // Global limit on total textures in pool
const MAX_TEXTURE_POOL_BYTES: u64 = 32 * 1024 * 1024; // Keep pooled RGBA textures under 32 MiB
const MAX_POOLED_TEXTURE_BYTES: u64 = 16 * 1024 * 1024; // Skip pooling huge 4K-class RGBA textures

struct CudaTextureCache {
    #[allow(dead_code)]
    y_texture: wgpu::Texture,
    y_view: wgpu::TextureView,
    y_cuda_alloc: crate::cuda_interop::ExportableCudaAllocation,
    y_pitch: usize,
    y_offset: usize,
    #[allow(dead_code)]
    uv_texture: wgpu::Texture,
    uv_view: wgpu::TextureView,
    uv_cuda_alloc: crate::cuda_interop::ExportableCudaAllocation,
    uv_pitch: usize,
    uv_offset: usize,
    width: u32,
    height: u32,
}

pub struct Renderer {
    pub name: String,
    pub ctx: Arc<WgpuContext>,
    surface: Surface<'static>,
    pub config: SurfaceConfiguration,
    pub configured: bool,
    pub needs_redraw: bool,
    pub last_present_time: std::time::Instant,
    pub frame_callback_pending: bool, // Track if we've requested a frame callback
    pub last_frame_request: Option<std::time::Instant>, // Failsafe for lost callbacks

    // Shared Resources
    uniform_buffer: wgpu::Buffer,
    sampler_linear: wgpu::Sampler,
    composition_texture: Option<wgpu::Texture>,

    current_texture: Option<wgpu::Texture>,
    current_aspect: f32,
    prev_texture: Option<wgpu::Texture>,
    prev_aspect: f32,
    pub transition_progress: f32,
    pub transition_start_time: Option<std::time::Instant>,
    pub transition_active: bool, // Explicit flag tracking if transition is active (following wpaperd pattern)
    pub transition_just_completed: bool, // Flag set when transition completes, cleared by main loop
    content_swap_pending: bool,  // Keep current content visible until replacement upload is ready

    // Transition Settings
    pub active_transition: Transition,
    pub transition_duration: f32,
    pub transition_stats: Option<TransitionStats>,

    // Texture Reuse
    current_texture_size: Option<(u32, u32)>,
    current_texture_view: Option<wgpu::TextureView>,
    prev_texture_view: Option<wgpu::TextureView>,
    composition_texture_view: Option<wgpu::TextureView>,

    // Cached Bind Groups to avoid per-frame creation overhead
    transition_bind_group: Option<wgpu::BindGroup>,
    blit_bind_group: Option<wgpu::BindGroup>,
    blit_source_is_composition: bool, // Helps track which blit BG is currently cached
    blit_source_is_prev: bool,        // Helps track if it was prev or current
    transition_rendered_this_frame: bool, // Track if transition shader ran successfully this frame

    // Content Type state to prevent race conditions (stale video frames overwriting images)
    pub valid_content_type: crate::queue::ContentType,
    pub active_image_session_id: u64,
    pub active_video_session_id: u64,
    presented_video_session_id: u64,
    pub active_batch_id: Option<u64>,
    pub batch_start_time: Option<std::time::Instant>, // Anchor for shared batch transitions
    display_timer_pending: bool,
    display_timer_ready: bool,

    // Metrics tracking
    metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    video_first_frame_time: Option<std::time::Instant>, // Track when video session starts
    last_video_source_size: Option<(u32, u32)>,
    last_video_presentation_size: Option<(u32, u32)>,

    // Background task handle for shader precompilation (aborted on drop)
    shader_precompile_handle: Option<tokio::task::AbortHandle>,

    // Reusable buffer for stride conversion to avoid per-frame allocations
    stride_temp_buffer: Vec<u8>,

    // Track prev_texture size for returning to pool
    prev_texture_size: Option<(u32, u32)>,

    // NV12 conversion staging textures (reused across frames if size matches)
    nv12_y_texture: Option<wgpu::Texture>,
    nv12_uv_texture: Option<wgpu::Texture>,
    nv12_y_view: Option<wgpu::TextureView>,
    nv12_uv_view: Option<wgpu::TextureView>,
    nv12_staging_size: Option<(u32, u32)>,

    i420_y_texture: Option<wgpu::Texture>,
    i420_u_texture: Option<wgpu::Texture>,
    i420_v_texture: Option<wgpu::Texture>,
    i420_y_view: Option<wgpu::TextureView>,
    i420_u_view: Option<wgpu::TextureView>,
    i420_v_view: Option<wgpu::TextureView>,
    i420_staging_size: Option<(u32, u32)>,

    // Per-renderer CUDA texture cache (shared CudaInterop lives in WgpuContext)
    cuda_textures: Option<CudaTextureCache>,
    cuda_nv12_bind_group: Option<wgpu::BindGroup>,
}

impl Renderer {
    pub fn new<W>(
        name: String,
        ctx: Arc<WgpuContext>,
        window: Arc<W>,
        first_surface: Option<Surface<'static>>,
        metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    ) -> anyhow::Result<Self>
    where
        W: HasWindowHandle + HasDisplayHandle + Sync + Send + 'static,
    {
        let desired_maximum_frame_latency = match window.window_handle().map(|h| h.as_raw()) {
            // Wayland/Vulkan can hit swapchain semaphore reuse validation with 3-image
            // surfaces under animated multi-output workloads. Prefer a 2-image class here.
            Ok(raw_window_handle::RawWindowHandle::Wayland(_)) => 1,
            _ => 2,
        };

        let wayland_window = matches!(
            window.window_handle().map(|handle| handle.as_raw()),
            Ok(raw_window_handle::RawWindowHandle::Wayland(_))
        );

        // Reuse the first surface if provided to avoid protocol errors (multiple roles on wl_surface)
        let surface = if let Some(s) = first_surface {
            s
        } else {
            ctx.instance.create_surface(window)?
        };

        let caps = surface.get_capabilities(&ctx.adapter);
        let format = caps
            .formats
            .first()
            .cloned()
            .unwrap_or(wgpu::TextureFormat::Rgba8UnormSrgb);
        let alpha_mode = if wayland_window {
            caps.alpha_modes
                .iter()
                .copied()
                .find(|mode| *mode != wgpu::CompositeAlphaMode::Opaque)
                .or_else(|| caps.alpha_modes.first().copied())
                .unwrap_or(wgpu::CompositeAlphaMode::Auto)
        } else {
            caps.alpha_modes
                .first()
                .copied()
                .unwrap_or(wgpu::CompositeAlphaMode::Auto)
        };
        let present_mode = select_present_mode(&caps.present_modes);

        if caps.formats.is_empty() {
            info!(
                "Surface {} created. Capabilities not yet available (transient).",
                name
            );
        }

        let config = SurfaceConfiguration {
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            format,
            width: 1, // Will be resized
            height: 1,
            present_mode,
            alpha_mode,
            view_formats: vec![],
            desired_maximum_frame_latency,
        };

        let r = Self {
            name,
            ctx: ctx.clone(),
            surface,
            config,
            configured: false,
            needs_redraw: true,
            last_present_time: std::time::Instant::now(),
            frame_callback_pending: false,
            last_frame_request: None,

            uniform_buffer: ctx.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("Transition Uniform Buffer"),
                size: std::mem::size_of::<TransitionUniforms>() as u64,
                usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            }),
            sampler_linear: ctx.device.create_sampler(&wgpu::SamplerDescriptor {
                label: Some("Linear Sampler"),
                address_mode_u: wgpu::AddressMode::ClampToEdge,
                address_mode_v: wgpu::AddressMode::ClampToEdge,
                address_mode_w: wgpu::AddressMode::ClampToEdge,
                mag_filter: wgpu::FilterMode::Linear,
                min_filter: wgpu::FilterMode::Linear,
                mipmap_filter: wgpu::FilterMode::Linear,
                ..Default::default()
            }),
            composition_texture: None,
            current_texture: None,
            current_aspect: 1.0,
            prev_texture: None,
            prev_aspect: 1.0,
            transition_progress: 1.0,
            transition_start_time: None,
            transition_active: false,
            transition_just_completed: false,
            content_swap_pending: false,
            active_transition: Transition::Fade,
            transition_duration: 1.0,
            transition_stats: None,
            current_texture_size: None,
            current_texture_view: None,
            prev_texture_view: None,
            composition_texture_view: None,
            transition_bind_group: None,
            blit_bind_group: None,
            blit_source_is_composition: false,
            blit_source_is_prev: false,
            transition_rendered_this_frame: false,
            valid_content_type: crate::queue::ContentType::Image,
            active_image_session_id: 0,
            active_video_session_id: 0,
            presented_video_session_id: 0,
            active_batch_id: None,
            batch_start_time: None,
            display_timer_pending: false,
            display_timer_ready: false,
            metrics,
            video_first_frame_time: None,
            last_video_source_size: None,
            last_video_presentation_size: None,
            shader_precompile_handle: None,
            stride_temp_buffer: Vec::new(),
            prev_texture_size: None,
            nv12_y_texture: None,
            nv12_uv_texture: None,
            nv12_y_view: None,
            nv12_uv_view: None,
            nv12_staging_size: None,
            i420_y_texture: None,
            i420_u_texture: None,
            i420_v_texture: None,
            i420_y_view: None,
            i420_u_view: None,
            i420_v_view: None,
            i420_staging_size: None,
            cuda_textures: None,
            cuda_nv12_bind_group: None,
        };
        // Shader precompilation is deferred to apply_config() which knows
        // the actual configured transition. No need to precompile 10 hardcoded
        // transitions when the user's config specifies exactly what they want.
        Ok(r)
    }
}
