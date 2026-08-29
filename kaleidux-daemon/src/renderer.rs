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
mod context_queue;
#[path = "renderer/context_vulkan.rs"]
mod context_vulkan;
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

#[path = "renderer/mpv_gl_interop.rs"]
#[cfg_attr(not(feature = "backend-mpv"), allow(dead_code))]
mod mpv_gl_interop;
#[path = "renderer/native_dmabuf_interop.rs"]
mod native_dmabuf_interop;
#[path = "renderer/native_gl_surface.rs"]
mod native_gl_surface;
#[path = "renderer/video_interop.rs"]
mod video_interop;
pub(crate) use mpv_gl_interop::GlInteropSync;
#[cfg(feature = "backend-mpv")]
pub(crate) use mpv_gl_interop::{create_exportable_rgba_texture, prime_shared_texture_for_gl};

#[path = "renderer/video_cpu_upload.rs"]
mod video_cpu_upload;
mod video_rgba_upload;

#[path = "renderer/native_video_upload.rs"]
mod native_video_upload;
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
#[derive(Copy, Clone, Debug, PartialEq, Pod, Zeroable)]
struct TransitionUniforms {
    progress: f32,
    screen_aspect: f32, // width / height
    prev_aspect: f32,
    next_aspect: f32,
    params: [[f32; 4]; 7], // Total 128 bytes (aligned)
}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Pod, Zeroable)]
struct YuvUniforms {
    geometry_and_range: [f32; 4], // screen aspect, content aspect, Y offset, Y scale
    chroma_and_transfer: [f32; 4], // UV offset, UV scale, transfer ID, reserved
    matrix_r: [f32; 4],
    matrix_g: [f32; 4],
    matrix_b: [f32; 4],
    crop_rect: [f32; 4], // x, y, width, height in coded normalized coordinates
    sampling_geometry: [f32; 4], // rotation quarter turns, chroma x/y offset, reserved
}

fn yuv_uniforms_for(
    color: crate::video::VideoColorMetadata,
    geometry: crate::video::VideoGeometry,
    output_width: u32,
    output_height: u32,
) -> YuvUniforms {
    use crate::video::{
        VideoChromaSiting, VideoColorMatrix, VideoColorPrimaries, VideoColorRange, VideoRotation,
        VideoTransfer,
    };
    let depth = color.bit_depth.clamp(8, 16);
    let code_max = ((1u32 << depth) - 1) as f32;
    // P010 code words occupy the most-significant ten bits of each u16.
    // R16Unorm therefore normalizes against 65535 rather than 1023.
    let storage_max = if depth > 8 { 65_535.0 } else { 255.0 };
    let storage_shift = if depth > 8 {
        (1u32 << (16 - depth)) as f32
    } else {
        1.0
    };
    let normalized_code_max = code_max * storage_shift / storage_max;
    let (y_offset, y_scale, uv_offset, uv_scale) = match color.range {
        VideoColorRange::Full => (
            0.0,
            1.0 / normalized_code_max,
            (1u32 << (depth - 1)) as f32 * storage_shift / storage_max,
            1.0 / normalized_code_max,
        ),
        VideoColorRange::Limited => {
            let scale = (1u32 << (depth - 8)) as f32;
            let y_low = 16.0 * scale * storage_shift / storage_max;
            let y_span = 219.0 * scale * storage_shift / storage_max;
            let c_mid = 128.0 * scale * storage_shift / storage_max;
            let c_span = 224.0 * scale * storage_shift / storage_max;
            (y_low, 1.0 / y_span, c_mid, 1.0 / c_span)
        }
    };
    let (r, g, b) = match color.matrix {
        VideoColorMatrix::Bt601 => (
            [1.0, 0.0, 1.4020, 0.0],
            [1.0, -0.344_136, -0.714_136, 0.0],
            [1.0, 1.7720, 0.0, 0.0],
        ),
        VideoColorMatrix::Bt709 => (
            [1.0, 0.0, 1.5748, 0.0],
            [1.0, -0.1873, -0.4681, 0.0],
            [1.0, 1.8556, 0.0, 0.0],
        ),
        VideoColorMatrix::Bt2020 => (
            [1.0, 0.0, 1.4746, 0.0],
            [1.0, -0.164_553, -0.571_353, 0.0],
            [1.0, 1.8814, 0.0, 0.0],
        ),
    };
    let transfer = match color.transfer {
        VideoTransfer::Srgb => 0.0,
        VideoTransfer::Bt709 => 1.0,
        VideoTransfer::Bt1886 => 2.0,
        VideoTransfer::Pq => 3.0,
        VideoTransfer::Hlg => 4.0,
    };
    let primaries = match color.primaries {
        VideoColorPrimaries::Bt601Ntsc | VideoColorPrimaries::Bt601Pal => 0.0,
        VideoColorPrimaries::Bt709 => 1.0,
        VideoColorPrimaries::Bt2020 => 2.0,
        VideoColorPrimaries::DciP3 => 3.0,
        VideoColorPrimaries::DisplayP3 => 4.0,
    };
    let mastering_peak = color
        .mastering
        .map(|metadata| metadata.max_luminance_nits)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or_else(|| match color.transfer {
            VideoTransfer::Pq => {
                color.content_light.max_content_light_level.unwrap_or(1_000) as f32
            }
            VideoTransfer::Hlg => 1_000.0,
            _ => 100.0,
        });
    let coded_width = geometry.coded_width.max(1) as f32;
    let coded_height = geometry.coded_height.max(1) as f32;
    let rotation = match geometry.rotation {
        VideoRotation::Rotate0 => 0.0,
        VideoRotation::Rotate90 => 1.0,
        VideoRotation::Rotate180 => 2.0,
        VideoRotation::Rotate270 => 3.0,
    };
    let (chroma_x, chroma_y) = match color.chroma_siting {
        VideoChromaSiting::Center => (0.0, 0.0),
        VideoChromaSiting::Left => (-0.5 / coded_width, 0.0),
        VideoChromaSiting::TopLeft => (-0.5 / coded_width, -0.5 / coded_height),
    };
    YuvUniforms {
        geometry_and_range: [
            output_width.max(1) as f32 / output_height.max(1) as f32,
            geometry.display_aspect(),
            y_offset,
            y_scale,
        ],
        chroma_and_transfer: [uv_offset, uv_scale, transfer, primaries],
        matrix_r: [r[0], r[1], r[2], mastering_peak],
        matrix_g: [g[0], g[1], g[2], 203.0],
        matrix_b: b,
        crop_rect: [
            geometry.crop.x as f32 / coded_width,
            geometry.crop.y as f32 / coded_height,
            geometry.crop.width.max(1) as f32 / coded_width,
            geometry.crop.height.max(1) as f32 / coded_height,
        ],
        sampling_geometry: [rotation, chroma_x, chroma_y, 0.0],
    }
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
        presentation: Option<
            &'a wayland_protocols::wp::presentation_time::client::wp_presentation::WpPresentation,
        >,
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
    timeline: Option<video_interop::CudaVulkanTimeline>,
    in_flight_frames: std::collections::VecDeque<(u64, crate::video::VideoFrameStorage)>,
    width: u32,
    height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum YuvFormat {
    Nv12,
    P010,
    I420,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum YuvOrigin {
    Cpu,
    DmaBuf,
    NativeDmaBuf,
    Cuda,
}

/// Backend-neutral description of the plane set sampled by the final pass.
/// Plane ownership stays in the backend-specific caches; this state is only
/// made active after those planes and their persistent bind group are ready.
#[derive(Debug, Clone, Copy, PartialEq)]
struct YuvSource {
    format: YuvFormat,
    origin: YuvOrigin,
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
    last_presentation_feedback_request: Option<std::time::Instant>,
    pub frame_callback_pending: bool, // Track if we've requested a frame callback
    pub last_frame_request: Option<std::time::Instant>, // Failsafe for lost callbacks
    pub(crate) pause_on_fullscreen: bool,

    // Shared Resources
    uniform_buffer: wgpu::Buffer,
    last_uniforms: Option<TransitionUniforms>,
    yuv_uniform_buffer: wgpu::Buffer,
    last_yuv_uniforms: Option<YuvUniforms>,
    sampler_linear: wgpu::Sampler,
    composition_texture: Option<wgpu::Texture>,

    current_texture: Option<wgpu::Texture>,
    current_external_view: Option<Arc<wgpu::TextureView>>,
    current_external_frame: Option<crate::video::GlExternalFrame>,
    current_aspect: f32,
    prev_texture: Option<wgpu::Texture>,
    prev_external_view: Option<Arc<wgpu::TextureView>>,
    prev_external_frame: Option<crate::video::GlExternalFrame>,
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
    blit_bind_group: Option<Arc<wgpu::BindGroup>>,
    external_blit_bind_groups: std::collections::HashMap<usize, Arc<wgpu::BindGroup>>,
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

    p010_y_texture: Option<wgpu::Texture>,
    p010_uv_texture: Option<wgpu::Texture>,
    p010_y_view: Option<wgpu::TextureView>,
    p010_uv_view: Option<wgpu::TextureView>,
    p010_staging_size: Option<(u32, u32)>,

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
    native_dmabuf_interop: Option<native_dmabuf_interop::NativeDmaBufInterop>,
    native_gl_surface: Option<native_gl_surface::NativeGlSurfaceRenderer>,
    native_gl_surface_failed: bool,
    native_nv12_bind_group: Option<wgpu::BindGroup>,
    final_nv12_bind_group: Option<Arc<wgpu::BindGroup>>,
    final_p010_bind_group: Option<Arc<wgpu::BindGroup>>,
    final_i420_bind_group: Option<Arc<wgpu::BindGroup>>,
    native_wayland_snapshot_frame: Option<crate::video::VideoFrame>,
    active_yuv_source: Option<YuvSource>,
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
            last_presentation_feedback_request: None,
            frame_callback_pending: false,
            last_frame_request: None,
            pause_on_fullscreen: false,

            uniform_buffer: ctx.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("Transition Uniform Buffer"),
                size: std::mem::size_of::<TransitionUniforms>() as u64,
                usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            }),
            last_uniforms: None,
            yuv_uniform_buffer: ctx.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("Final YUV Uniform Buffer"),
                size: std::mem::size_of::<YuvUniforms>() as u64,
                usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            }),
            last_yuv_uniforms: None,
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
            current_external_view: None,
            current_external_frame: None,
            current_aspect: 1.0,
            prev_texture: None,
            prev_external_view: None,
            prev_external_frame: None,
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
            external_blit_bind_groups: std::collections::HashMap::new(),
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
            p010_y_texture: None,
            p010_uv_texture: None,
            p010_y_view: None,
            p010_uv_view: None,
            p010_staging_size: None,
            i420_y_texture: None,
            i420_u_texture: None,
            i420_v_texture: None,
            i420_y_view: None,
            i420_u_view: None,
            i420_v_view: None,
            i420_staging_size: None,
            cuda_textures: None,
            cuda_nv12_bind_group: None,
            native_dmabuf_interop: None,
            native_gl_surface: None,
            native_gl_surface_failed: false,
            native_nv12_bind_group: None,
            final_nv12_bind_group: None,
            final_p010_bind_group: None,
            final_i420_bind_group: None,
            native_wayland_snapshot_frame: None,
            active_yuv_source: None,
        };
        // Shader precompilation is deferred to apply_config() which knows
        // the actual configured transition. No need to precompile 10 hardcoded
        // transitions when the user's config specifies exactly what they want.
        Ok(r)
    }

    fn write_uniforms_if_changed(&mut self, uniforms: TransitionUniforms) {
        if self.last_uniforms == Some(uniforms) {
            return;
        }
        self.ctx
            .write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));
        self.last_uniforms = Some(uniforms);
    }

    fn write_yuv_uniforms(
        &mut self,
        color: crate::video::VideoColorMetadata,
        geometry: crate::video::VideoGeometry,
    ) {
        let uniforms = yuv_uniforms_for(color, geometry, self.config.width, self.config.height);
        if self.last_yuv_uniforms == Some(uniforms) {
            return;
        }
        self.ctx
            .write_buffer(&self.yuv_uniform_buffer, 0, bytemuck::bytes_of(&uniforms));
        self.last_yuv_uniforms = Some(uniforms);
    }
}

#[cfg(test)]
mod color_uniform_tests {
    use super::yuv_uniforms_for;
    use crate::video::{
        VideoChromaSiting, VideoColorMatrix, VideoColorMetadata, VideoColorPrimaries,
        VideoColorRange, VideoContentLightMetadata, VideoCropRect, VideoGeometry, VideoRotation,
        VideoTransfer,
    };

    fn close(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() <= 1.0e-5,
            "actual={actual} expected={expected}"
        );
    }

    #[test]
    fn limited_8_bit_and_p010_codes_map_black_to_zero_and_white_to_one() {
        let geometry = VideoGeometry::for_dimensions(1920, 1080);
        let eight = yuv_uniforms_for(VideoColorMetadata::default(), geometry, 1920, 1080);
        close(eight.geometry_and_range[2], 16.0 / 255.0);
        close(eight.geometry_and_range[3], 255.0 / 219.0);
        close(eight.chroma_and_transfer[0], 128.0 / 255.0);
        close(eight.chroma_and_transfer[1], 255.0 / 224.0);
        close(
            (16.0 / 255.0 - eight.geometry_and_range[2]) * eight.geometry_and_range[3],
            0.0,
        );
        close(
            (235.0 / 255.0 - eight.geometry_and_range[2]) * eight.geometry_and_range[3],
            1.0,
        );

        let ten_bit = VideoColorMetadata {
            bit_depth: 10,
            ..VideoColorMetadata::default()
        };
        let p010 = yuv_uniforms_for(ten_bit, geometry, 1920, 1080);
        let black_word = (64u32 << 6) as f32 / 65_535.0;
        let white_word = (940u32 << 6) as f32 / 65_535.0;
        let neutral_word = (512u32 << 6) as f32 / 65_535.0;
        close(
            (black_word - p010.geometry_and_range[2]) * p010.geometry_and_range[3],
            0.0,
        );
        close(
            (white_word - p010.geometry_and_range[2]) * p010.geometry_and_range[3],
            1.0,
        );
        close(
            (neutral_word - p010.chroma_and_transfer[0]) * p010.chroma_and_transfer[1],
            0.0,
        );
    }

    #[test]
    fn full_range_p010_preserves_the_entire_ten_bit_code_domain() {
        let color = VideoColorMetadata {
            bit_depth: 10,
            range: VideoColorRange::Full,
            ..VideoColorMetadata::default()
        };
        let uniforms =
            yuv_uniforms_for(color, VideoGeometry::for_dimensions(1024, 576), 1920, 1080);
        let maximum_word = (1023u32 << 6) as f32 / 65_535.0;
        close(maximum_word * uniforms.geometry_and_range[3], 1.0);
        close(
            uniforms.chroma_and_transfer[0],
            (512u32 << 6) as f32 / 65_535.0,
        );
    }

    #[test]
    fn hdr_policy_and_geometry_are_encoded_once_for_the_final_pass() {
        let color = VideoColorMetadata {
            matrix: VideoColorMatrix::Bt2020,
            primaries: VideoColorPrimaries::Bt2020,
            transfer: VideoTransfer::Pq,
            bit_depth: 10,
            chroma_siting: VideoChromaSiting::TopLeft,
            content_light: VideoContentLightMetadata {
                max_content_light_level: Some(4_000),
                max_frame_average_light_level: Some(400),
            },
            ..VideoColorMetadata::default()
        };
        let geometry = VideoGeometry {
            coded_width: 1920,
            coded_height: 1080,
            display_width: 1880,
            display_height: 1040,
            sample_aspect_num: 1,
            sample_aspect_den: 1,
            crop: VideoCropRect {
                x: 20,
                y: 20,
                width: 1880,
                height: 1040,
            },
            rotation: VideoRotation::Rotate90,
        };
        let uniforms = yuv_uniforms_for(color, geometry, 1920, 1080);
        close(uniforms.chroma_and_transfer[2], 3.0);
        close(uniforms.chroma_and_transfer[3], 2.0);
        close(uniforms.matrix_r[3], 4_000.0);
        close(uniforms.matrix_g[3], 203.0);
        close(uniforms.crop_rect[0], 20.0 / 1920.0);
        close(uniforms.crop_rect[1], 20.0 / 1080.0);
        close(uniforms.sampling_geometry[0], 1.0);
        close(uniforms.sampling_geometry[1], -0.5 / 1920.0);
        close(uniforms.sampling_geometry[2], -0.5 / 1080.0);
        close(uniforms.geometry_and_range[1], 1040.0 / 1880.0);
    }
}
