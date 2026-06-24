use crate::shaders::Transition;
use bytemuck::{Pod, Zeroable};
use raw_window_handle::{HasDisplayHandle, HasWindowHandle};
use smithay_client_toolkit::shell::{WaylandSurface, wlr_layer::LayerSurface};
use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};
use wayland_client::QueueHandle;
use wgpu::{Adapter, Device, Instance, Queue, Surface, SurfaceConfiguration};

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

// Texture pool entry for LRU cache
pub struct TexturePoolEntry {
    texture: wgpu::Texture,
    last_used: std::time::Instant,
}

// LRU cache for transition pipelines. Lookups and access updates are O(1), while
// eviction scans `access_order` to find the least-recently-used entry.
pub struct PipelineLRU {
    pipelines: HashMap<String, Arc<wgpu::RenderPipeline>>,
    access_order: HashMap<String, u64>,
    order_counter: u64,
    max_size: usize,
}

impl PipelineLRU {
    fn new(max_size: usize) -> Self {
        Self {
            pipelines: HashMap::new(),
            access_order: HashMap::new(),
            order_counter: 0,
            max_size,
        }
    }

    fn get(&mut self, key: &str) -> Option<Arc<wgpu::RenderPipeline>> {
        match self.pipelines.get(key).cloned() {
            Some(pipeline) => {
                // O(1) update: set access counter to latest value
                self.order_counter += 1;
                self.access_order
                    .insert(key.to_string(), self.order_counter);
                Some(pipeline)
            }
            _ => None,
        }
    }

    fn insert(&mut self, key: String, pipeline: Arc<wgpu::RenderPipeline>) {
        // Remove if already exists
        if self.pipelines.contains_key(&key) {
            // Already tracked, just update
        } else {
            // Evict least recently used if at capacity
            while self.pipelines.len() >= self.max_size {
                // Find key with smallest counter value (LRU)
                if let Some(lru_key) = self
                    .access_order
                    .iter()
                    .min_by_key(|(_, v)| *v)
                    .map(|(k, _)| k.clone())
                {
                    self.pipelines.remove(&lru_key);
                    self.access_order.remove(&lru_key);
                } else {
                    break;
                }
            }
        }
        self.order_counter += 1;
        self.access_order.insert(key.clone(), self.order_counter);
        self.pipelines.insert(key, pipeline);
    }

    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    #[allow(dead_code)]
    pub fn contains_key(&self, key: &str) -> bool {
        self.pipelines.contains_key(key)
    }
}

pub struct WgpuContext {
    pub instance: Instance,
    pub adapter: Adapter,
    pub device: Device,
    pub queue: Queue,
    pub transition_pipelines: parking_lot::Mutex<PipelineLRU>,
    pub blit_pipelines: parking_lot::Mutex<HashMap<wgpu::TextureFormat, Arc<wgpu::RenderPipeline>>>,
    pub mipmap_pipelines:
        parking_lot::Mutex<HashMap<wgpu::TextureFormat, Arc<wgpu::RenderPipeline>>>,
    pub blit_bind_group_layout: wgpu::BindGroupLayout,
    pub transition_bind_group_layout: wgpu::BindGroupLayout,
    #[allow(dead_code)]
    pub mipmap_bind_group_layout: wgpu::BindGroupLayout,
    pub nv12_bind_group_layout: wgpu::BindGroupLayout,
    pub nv12_pipeline: wgpu::RenderPipeline,
    pub i420_bind_group_layout: wgpu::BindGroupLayout,
    pub i420_pipeline: wgpu::RenderPipeline,
    pub pipeline_cache: Option<wgpu::PipelineCache>,
    pipeline_cache_path: Option<PathBuf>,
    // Texture pool: (width, height, mip_level_count) -> Vec of available textures
    pub texture_pool: parking_lot::Mutex<HashMap<(u32, u32, u32), Vec<TexturePoolEntry>>>,
    // Shared CUDA interop context (one per GPU, shared across all renderers)
    cuda_interop: parking_lot::Mutex<Option<crate::cuda_interop::CudaInterop>>,
    cuda_interop_failed: std::sync::atomic::AtomicBool,
}

const MAX_PIPELINE_CACHE_SIZE: usize = 50;
const MAX_TEXTURE_POOL_SIZE: usize = 16; // Global limit on total textures in pool
const MAX_TEXTURE_POOL_BYTES: u64 = 32 * 1024 * 1024; // Keep pooled RGBA textures under 32 MiB
const MAX_POOLED_TEXTURE_BYTES: u64 = 16 * 1024 * 1024; // Skip pooling huge 4K-class RGBA textures

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetainedTextureFootprint {
    pub current_bytes: u64,
    pub prev_bytes: u64,
    pub composition_bytes: u64,
    pub video_aux_bytes: u64,
}

impl RetainedTextureFootprint {
    pub fn total_bytes(self) -> u64 {
        self.current_bytes
            .saturating_add(self.prev_bytes)
            .saturating_add(self.composition_bytes)
            .saturating_add(self.video_aux_bytes)
    }
}

fn texture_byte_size(width: u32, height: u32, mip_level_count: u32) -> u64 {
    let mut total = 0u64;
    let mut mip_width = width.max(1);
    let mut mip_height = height.max(1);
    let levels = mip_level_count.max(1);

    for _ in 0..levels {
        total += mip_width as u64 * mip_height as u64 * 4;
        if mip_width == 1 && mip_height == 1 {
            break;
        }
        mip_width = (mip_width / 2).max(1);
        mip_height = (mip_height / 2).max(1);
    }

    total
}

fn chroma_plane_extent(width: u32, height: u32) -> (u32, u32) {
    (width.div_ceil(2), height.div_ceil(2))
}

fn yuv420_aux_byte_size(width: u32, height: u32) -> u64 {
    let (chroma_width, chroma_height) = chroma_plane_extent(width, height);
    width as u64 * height as u64 + 2 * chroma_width as u64 * chroma_height as u64
}

pub(crate) fn compute_cover_target_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    if source_width == 0 || source_height == 0 || target_width == 0 || target_height == 0 {
        return (source_width.max(1), source_height.max(1));
    }

    let width_scale = target_width as f32 / source_width as f32;
    let height_scale = target_height as f32 / source_height as f32;
    let scale = width_scale.max(height_scale).min(1.0);
    let prepared_width =
        ((source_width as f32 * scale).round() as u32).clamp(1, source_width.max(1));
    let prepared_height =
        ((source_height as f32 * scale).round() as u32).clamp(1, source_height.max(1));
    (prepared_width, prepared_height)
}

fn should_pool_texture(width: u32, height: u32, mip_level_count: u32) -> bool {
    texture_byte_size(width, height, mip_level_count) <= MAX_POOLED_TEXTURE_BYTES
}

fn select_present_mode(present_modes: &[wgpu::PresentMode]) -> wgpu::PresentMode {
    if present_modes.contains(&wgpu::PresentMode::Fifo) {
        wgpu::PresentMode::Fifo
    } else {
        present_modes
            .first()
            .copied()
            .unwrap_or(wgpu::PresentMode::Fifo)
    }
}

fn sanitize_cache_component(component: &str) -> String {
    let mut sanitized = String::with_capacity(component.len());
    for ch in component.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else if !sanitized.ends_with('-') {
            sanitized.push('-');
        }
    }
    sanitized.trim_matches('-').to_string()
}

fn pipeline_cache_path_for_adapter(adapter: &Adapter) -> Option<PathBuf> {
    let info = adapter.get_info();
    let cache_dir = dirs::cache_dir()?.join("kaleidux").join("wgpu");
    let adapter_name = sanitize_cache_component(info.name.as_str());
    Some(cache_dir.join(format!(
        "pipeline-cache-v2-{:?}-{:04x}-{:04x}-{}.bin",
        info.backend, info.vendor, info.device, adapter_name
    )))
}

fn load_pipeline_cache_seed(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(data) if !data.is_empty() => Some(data),
        Ok(_) => None,
        Err(_) => None,
    }
}

fn random_transition_prewarm_set() -> Vec<Transition> {
    vec![
        Transition::Fade,
        Transition::CrossZoom { strength: 0.3 },
        Transition::Radial { smoothness: 0.5 },
        Transition::Circle,
        Transition::Directional {
            direction: [1.0, 0.0],
        },
        Transition::SimpleZoom {
            zoom_quickness: 0.5,
        },
        Transition::Ripple {
            amplitude: 0.1,
            speed: 1.0,
        },
        Transition::Swirl,
        Transition::Pixelize {
            squares_min: [10, 10],
            steps: 10,
        },
        Transition::Mosaic { endx: 20, endy: 20 },
        Transition::Burn,
        Transition::CrossWarp,
        Transition::Dreamy,
        Transition::Morph { strength: 0.1 },
        Transition::Wind { size: 0.2 },
        Transition::Dissolve {
            line_width: 0.1,
            spread_clr: [0.0, 0.0, 0.0],
            hot_clr: [0.9, 0.6, 0.1],
            pow: 5.0,
            intensity: 1.0,
        },
        Transition::FadeColor {
            color: [0.0, 0.0, 0.0],
            color_phase: 0.4,
        },
        Transition::Overexposure,
        Transition::FilmBurn { seed: 2.31 },
        Transition::Pinwheel { speed: 2.0 },
        Transition::Heart,
    ]
}

fn transition_prewarm_candidates(transition: &Transition) -> Vec<Transition> {
    if matches!(transition, Transition::Random) {
        random_transition_prewarm_set()
    } else if matches!(transition, Transition::Fade) {
        vec![Transition::Fade]
    } else {
        vec![Transition::Fade, transition.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texture_size_matches_rgba_pixels_for_single_mip() {
        assert_eq!(texture_byte_size(1920, 1080, 1), 1920 * 1080 * 4);
    }

    #[test]
    fn texture_size_includes_additional_mips() {
        assert!(texture_byte_size(1920, 1080, 4) > texture_byte_size(1920, 1080, 1));
    }

    #[test]
    fn pool_policy_rejects_4k_rgba_textures() {
        assert!(!should_pool_texture(3840, 2160, 1));
        assert!(should_pool_texture(1920, 1080, 1));
    }

    #[test]
    fn retained_footprint_total_sums_sections() {
        let fp = RetainedTextureFootprint {
            current_bytes: 10,
            prev_bytes: 20,
            composition_bytes: 30,
            video_aux_bytes: 40,
        };
        assert_eq!(fp.total_bytes(), 100);
    }

    #[test]
    fn cover_target_preserves_minimum_cover_without_upscaling() {
        assert_eq!(
            compute_cover_target_dimensions(3840, 2160, 1366, 768),
            (1366, 768)
        );
        assert_eq!(
            compute_cover_target_dimensions(3840, 2160, 1280, 1024),
            (1820, 1024)
        );
        assert_eq!(
            compute_cover_target_dimensions(800, 600, 1920, 1080),
            (800, 600)
        );
    }

    #[test]
    fn present_mode_prefers_fifo_when_available() {
        let selected = select_present_mode(&[wgpu::PresentMode::Mailbox, wgpu::PresentMode::Fifo]);
        assert_eq!(selected, wgpu::PresentMode::Fifo);
    }

    #[test]
    fn present_mode_falls_back_to_first_supported_mode() {
        let selected = select_present_mode(&[wgpu::PresentMode::Mailbox]);
        assert_eq!(selected, wgpu::PresentMode::Mailbox);
    }

    #[test]
    fn transition_prewarm_candidates_include_fade_fallback() {
        let transitions = transition_prewarm_candidates(&Transition::Circle);
        assert!(transitions.iter().any(|t| matches!(t, Transition::Fade)));
        assert!(transitions.iter().any(|t| matches!(t, Transition::Circle)));
    }

    #[test]
    fn random_transition_prewarm_contains_multiple_variants() {
        let transitions = transition_prewarm_candidates(&Transition::Random);
        assert!(transitions.len() > 5);
        assert!(transitions.iter().any(|t| matches!(t, Transition::Fade)));
    }

    #[test]
    fn chroma_plane_extent_rounds_up_for_odd_sizes() {
        assert_eq!(chroma_plane_extent(1920, 1080), (960, 540));
        assert_eq!(chroma_plane_extent(1919, 1079), (960, 540));
    }

    #[test]
    fn yuv420_aux_bytes_match_planar_layout() {
        assert_eq!(
            yuv420_aux_byte_size(1920, 1080),
            1920 * 1080 + 2 * 960 * 540
        );
        assert_eq!(
            yuv420_aux_byte_size(1919, 1079),
            1919 * 1079 + 2 * 960 * 540
        );
    }
}

impl WgpuContext {
    pub async fn with_surface(
        window: Arc<impl HasWindowHandle + HasDisplayHandle + Sync + Send + 'static>,
    ) -> anyhow::Result<(Arc<Self>, Surface<'static>)> {
        let instance = Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });
        let compatible_surface = instance.create_surface(window)?;

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: Some(&compatible_surface), // Restore this - required for presentation support
                force_fallback_adapter: false,
            })
            .await
            .ok_or_else(|| anyhow::anyhow!("Failed to find a suitable GPU adapter"))?;

        info!(
            "WGPU picked adapter: {:?} with backend: {:?}",
            adapter.get_info().name,
            adapter.get_info().backend
        );

        let mut required_features = wgpu::Features::empty();
        if adapter.features().contains(wgpu::Features::PIPELINE_CACHE) {
            required_features |= wgpu::Features::PIPELINE_CACHE;
        }

        let (device, queue) = adapter
            .request_device(
                &wgpu::DeviceDescriptor {
                    label: Some("Kaleidux Shared Device"),
                    required_features,
                    required_limits: adapter.limits(),
                    // Favor smaller allocator blocks over peak throughput. The retained
                    // texture logs show renderer-visible textures are not the dominant
                    // RSS anymore, so reducing allocator slack is the next useful lever.
                    memory_hints: wgpu::MemoryHints::MemoryUsage,
                },
                None,
            )
            .await?;

        let pipeline_cache_path = pipeline_cache_path_for_adapter(&adapter);
        let pipeline_cache_seed = pipeline_cache_path
            .as_ref()
            .and_then(|path| load_pipeline_cache_seed(path));
        let pipeline_cache = if required_features.contains(wgpu::Features::PIPELINE_CACHE) {
            Some(unsafe {
                device.create_pipeline_cache(&wgpu::PipelineCacheDescriptor {
                    label: Some("Kaleidux Pipeline Cache"),
                    data: pipeline_cache_seed.as_deref(),
                    fallback: true,
                })
            })
        } else {
            None
        };
        if let Some(path) = &pipeline_cache_path {
            debug!(
                "[RENDER] Pipeline cache initialized at {} (enabled={}, seed_bytes={})",
                path.display(),
                pipeline_cache.is_some(),
                pipeline_cache_seed.as_ref().map_or(0, |data| data.len())
            );
        }

        // --- Shared Bind Group Layouts ---
        let transition_bind_group_layout =
            device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("Transition Bind Group Layout"),
                entries: &[
                    wgpu::BindGroupLayoutEntry {
                        binding: 0,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Buffer {
                            ty: wgpu::BufferBindingType::Uniform,
                            has_dynamic_offset: false,
                            min_binding_size: None,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 1,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 2,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 3,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                        count: None,
                    },
                ],
            });

        let blit_bind_group_layout =
            device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("Blit Bind Group Layout"),
                entries: &[
                    wgpu::BindGroupLayoutEntry {
                        binding: 0,
                        visibility: wgpu::ShaderStages::FRAGMENT | wgpu::ShaderStages::VERTEX,
                        ty: wgpu::BindingType::Buffer {
                            ty: wgpu::BufferBindingType::Uniform,
                            has_dynamic_offset: false,
                            min_binding_size: None,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 1,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 2,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                        count: None,
                    },
                ],
            });

        let mipmap_bind_group_layout =
            device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("Mipmap Bind Group Layout"),
                entries: &[
                    wgpu::BindGroupLayoutEntry {
                        binding: 0,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 1,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                        count: None,
                    },
                ],
            });

        // NV12→RGBA conversion bind group layout (Y texture, UV texture, sampler)
        let nv12_bind_group_layout =
            device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("NV12 Convert Bind Group Layout"),
                entries: &[
                    wgpu::BindGroupLayoutEntry {
                        binding: 0,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 1,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 2,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                        count: None,
                    },
                ],
            });

        let nv12_shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("NV12 Convert Shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/nv12_convert.wgsl").into()),
        });

        let nv12_pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("NV12 Convert Pipeline Layout"),
            bind_group_layouts: &[&nv12_bind_group_layout],
            push_constant_ranges: &[],
        });

        let nv12_pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("NV12 Convert Pipeline"),
            layout: Some(&nv12_pipeline_layout),
            vertex: wgpu::VertexState {
                module: &nv12_shader,
                entry_point: Some("vs_main"),
                buffers: &[],
                compilation_options: Default::default(),
            },
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::TriangleList,
                ..Default::default()
            },
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            fragment: Some(wgpu::FragmentState {
                module: &nv12_shader,
                entry_point: Some("fs_main"),
                targets: &[Some(wgpu::ColorTargetState {
                    format: wgpu::TextureFormat::Rgba8UnormSrgb,
                    blend: None,
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            multiview: None,
            cache: pipeline_cache.as_ref(),
        });

        let i420_bind_group_layout =
            device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("I420 Convert Bind Group Layout"),
                entries: &[
                    wgpu::BindGroupLayoutEntry {
                        binding: 0,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 1,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 2,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Texture {
                            sample_type: wgpu::TextureSampleType::Float { filterable: true },
                            view_dimension: wgpu::TextureViewDimension::D2,
                            multisampled: false,
                        },
                        count: None,
                    },
                    wgpu::BindGroupLayoutEntry {
                        binding: 3,
                        visibility: wgpu::ShaderStages::FRAGMENT,
                        ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                        count: None,
                    },
                ],
            });

        let i420_shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("I420 Convert Shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/i420_convert.wgsl").into()),
        });

        let i420_pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("I420 Convert Pipeline Layout"),
            bind_group_layouts: &[&i420_bind_group_layout],
            push_constant_ranges: &[],
        });

        let i420_pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("I420 Convert Pipeline"),
            layout: Some(&i420_pipeline_layout),
            vertex: wgpu::VertexState {
                module: &i420_shader,
                entry_point: Some("vs_main"),
                buffers: &[],
                compilation_options: Default::default(),
            },
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::TriangleList,
                ..Default::default()
            },
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            fragment: Some(wgpu::FragmentState {
                module: &i420_shader,
                entry_point: Some("fs_main"),
                targets: &[Some(wgpu::ColorTargetState {
                    format: wgpu::TextureFormat::Rgba8UnormSrgb,
                    blend: None,
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            multiview: None,
            cache: pipeline_cache.as_ref(),
        });

        Ok((
            Arc::new(Self {
                instance,
                adapter,
                device,
                queue,
                transition_pipelines: parking_lot::Mutex::new(PipelineLRU::new(
                    MAX_PIPELINE_CACHE_SIZE,
                )),
                blit_pipelines: parking_lot::Mutex::new(HashMap::new()),
                mipmap_pipelines: parking_lot::Mutex::new(HashMap::new()),
                blit_bind_group_layout,
                transition_bind_group_layout,
                mipmap_bind_group_layout,
                nv12_bind_group_layout,
                nv12_pipeline,
                i420_bind_group_layout,
                i420_pipeline,
                pipeline_cache,
                pipeline_cache_path,
                texture_pool: parking_lot::Mutex::new(HashMap::new()),
                cuda_interop: parking_lot::Mutex::new(None),
                cuda_interop_failed: std::sync::atomic::AtomicBool::new(false),
            }),
            compatible_surface,
        ))
    }

    pub fn warmup_cuda_interop(&self) {
        if self
            .cuda_interop_failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        let warmup_start = std::time::Instant::now();
        let mut created_context = false;
        {
            let mut ci_lock = self.cuda_interop.lock();
            if ci_lock.is_none() {
                match crate::cuda_interop::CudaInterop::new() {
                    Ok(interop) => {
                        *ci_lock = Some(interop);
                        created_context = true;
                    }
                    Err(e) => {
                        warn!("[CUDA] Warmup failed to create interop context: {}", e);
                        self.cuda_interop_failed
                            .store(true, std::sync::atomic::Ordering::Release);
                        return;
                    }
                }
            }
        }

        let alloc_start = std::time::Instant::now();
        let alloc_duration = {
            let ci_guard = self.cuda_interop.lock();
            ci_guard
                .as_ref()
                .and_then(|interop| match interop.allocate_exportable(4096) {
                    Ok((alloc, fd)) => {
                        unsafe {
                            libc::close(fd);
                        }
                        interop.free_exportable(alloc);
                        Some(alloc_start.elapsed())
                    }
                    Err(e) => {
                        debug!("[CUDA] Warmup exportable allocation skipped: {}", e);
                        None
                    }
                })
        };

        info!(
            "[CUDA] Warmup complete in {:.1}ms (created_context={}, exportable_alloc_ms={})",
            warmup_start.elapsed().as_secs_f64() * 1000.0,
            created_context,
            alloc_duration
                .map(|d| format!("{:.1}", d.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "n/a".to_string())
        );
    }

    pub fn persist_pipeline_cache(&self) {
        let Some(cache) = &self.pipeline_cache else {
            return;
        };
        let Some(path) = &self.pipeline_cache_path else {
            return;
        };
        let Some(data) = cache.get_data() else {
            return;
        };
        if data.is_empty() {
            return;
        }
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!(
                    "[RENDER] Failed to create pipeline-cache directory {}: {}",
                    parent.display(),
                    e
                );
                return;
            }
        }
        if let Err(e) = std::fs::write(path, &data) {
            warn!(
                "[RENDER] Failed to persist pipeline cache {}: {}",
                path.display(),
                e
            );
            return;
        }
        debug!(
            "[RENDER] Persisted pipeline cache to {} ({} bytes)",
            path.display(),
            data.len()
        );
    }

    pub fn get_blit_pipeline(&self, format: wgpu::TextureFormat) -> Arc<wgpu::RenderPipeline> {
        if let Some(pipe) = self.blit_pipelines.lock().get(&format) {
            return pipe.clone();
        }

        debug!("[RENDER] Compiling blit pipeline for format: {:?}", format);

        let blit_shader = self
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("Quad Shader"),
                source: wgpu::ShaderSource::Wgsl(include_str!("shaders/quad.wgsl").into()),
            });

        let blit_pipeline_layout =
            self.device
                .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                    label: Some("Blit Pipeline Layout"),
                    bind_group_layouts: &[&self.blit_bind_group_layout],
                    push_constant_ranges: &[],
                });

        let pipeline = self
            .device
            .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
                label: Some("Blit Pipeline"),
                layout: Some(&blit_pipeline_layout),
                vertex: wgpu::VertexState {
                    module: &blit_shader,
                    entry_point: Some("vs_main"),
                    compilation_options: Default::default(),
                    buffers: &[],
                },
                fragment: Some(wgpu::FragmentState {
                    module: &blit_shader,
                    entry_point: Some("fs_blit"),
                    compilation_options: Default::default(),
                    targets: &[Some(wgpu::ColorTargetState {
                        format,
                        blend: Some(wgpu::BlendState::REPLACE),
                        write_mask: wgpu::ColorWrites::ALL,
                    })],
                }),
                primitive: wgpu::PrimitiveState::default(),
                depth_stencil: None,
                multisample: wgpu::MultisampleState::default(),
                multiview: None,
                cache: self.pipeline_cache.as_ref(),
            });

        let pipeline_arc = Arc::new(pipeline);

        self.blit_pipelines
            .lock()
            .insert(format, pipeline_arc.clone());
        pipeline_arc
    }

    #[allow(dead_code)]
    pub fn get_mipmap_pipeline(&self, format: wgpu::TextureFormat) -> Arc<wgpu::RenderPipeline> {
        if let Some(pipe) = self.mipmap_pipelines.lock().get(&format) {
            return pipe.clone();
        }

        debug!(
            "[RENDER] Compiling mipmap pipeline for format: {:?}",
            format
        );

        // Load mipmap.wgsl
        let shader = self
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("Mipmap Shader"),
                source: wgpu::ShaderSource::Wgsl(include_str!("shaders/mipmap.wgsl").into()),
            });

        let layout = self
            .device
            .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                label: Some("Mipmap Pipeline Layout"),
                bind_group_layouts: &[&self.mipmap_bind_group_layout],
                push_constant_ranges: &[],
            });

        let pipeline = self
            .device
            .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
                label: Some("Mipmap Pipeline"),
                layout: Some(&layout),
                vertex: wgpu::VertexState {
                    module: &shader,
                    entry_point: Some("vs_main"),
                    compilation_options: Default::default(),
                    buffers: &[],
                },
                fragment: Some(wgpu::FragmentState {
                    module: &shader,
                    entry_point: Some("fs_main"),
                    compilation_options: Default::default(),
                    targets: &[Some(wgpu::ColorTargetState {
                        format,
                        blend: Some(wgpu::BlendState::REPLACE),
                        write_mask: wgpu::ColorWrites::ALL,
                    })],
                }),
                primitive: wgpu::PrimitiveState::default(),
                depth_stencil: None,
                multisample: wgpu::MultisampleState::default(),
                multiview: None,
                cache: self.pipeline_cache.as_ref(),
            });

        let pipeline_arc = Arc::new(pipeline);
        self.mipmap_pipelines
            .lock()
            .insert(format, pipeline_arc.clone());
        pipeline_arc
    }

    /// Get a texture from the pool or create a new one
    pub fn get_texture_from_pool(
        &self,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
        metrics: Option<&crate::metrics::PerformanceMetrics>,
    ) -> wgpu::Texture {
        self.get_texture_from_pool_with_mips(width, height, 1, usage, metrics)
    }

    /// Get a texture from the pool or create a new one, with specified mip level count
    pub fn get_texture_from_pool_with_mips(
        &self,
        width: u32,
        height: u32,
        mip_level_count: u32,
        usage: wgpu::TextureUsages,
        metrics: Option<&crate::metrics::PerformanceMetrics>,
    ) -> wgpu::Texture {
        let mut pool = self.texture_pool.lock();
        let key = (width, height, mip_level_count);

        // Try to find a texture in the pool
        if let Some(entries) = pool.get_mut(&key) {
            // Remove stale entries (older than 5 seconds) and find a fresh one
            let now = std::time::Instant::now();
            entries.retain(|e| now.duration_since(e.last_used).as_secs() < 5);

            if let Some(entry) = entries.pop() {
                if let Some(m) = metrics {
                    m.record_texture_pool_hit();
                }
                return entry.texture;
            }
        }

        // No texture in pool, create new one
        if let Some(m) = metrics {
            m.record_texture_pool_miss();
        }
        self.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("Pooled Texture"),
            size: wgpu::Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
            mip_level_count,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8UnormSrgb,
            usage,
            view_formats: &[],
        })
    }

    /// Return a texture to the pool for reuse
    pub fn return_texture_to_pool(&self, texture: wgpu::Texture, width: u32, height: u32) {
        let mut pool = self.texture_pool.lock();
        let mip_level_count = texture.mip_level_count();
        let key = (width, height, mip_level_count);
        let texture_bytes = texture_byte_size(width, height, mip_level_count);

        if !should_pool_texture(width, height, mip_level_count) {
            return;
        }

        // Calculate total textures in pool
        let total_textures: usize = pool.values().map(|v| v.len()).sum();
        let total_bytes: u64 = pool
            .iter()
            .map(|(&(w, h, mips), entries)| texture_byte_size(w, h, mips) * entries.len() as u64)
            .sum();

        // Check global limit first
        if total_textures >= MAX_TEXTURE_POOL_SIZE
            || total_bytes.saturating_add(texture_bytes) > MAX_TEXTURE_POOL_BYTES
        {
            // Pool is at global limit, drop this texture
            return;
        }

        // Limit pool size per resolution to prevent unbounded growth
        let entries = pool.entry(key).or_default();
        if entries.is_empty() {
            entries.push(TexturePoolEntry {
                texture,
                last_used: std::time::Instant::now(),
            });
        }
        // If pool is full for this resolution, texture is dropped (freed by WGPU)
    }

    /// Clean up old textures from pool
    pub fn cleanup_texture_pool(&self, metrics: Option<&crate::metrics::PerformanceMetrics>) {
        let mut pool = self.texture_pool.lock();
        let now = std::time::Instant::now();

        // Remove expired entries in-place (P-11: avoids full HashMap reconstruction)
        for entries in pool.values_mut() {
            entries.retain(|e| now.duration_since(e.last_used).as_secs() < 10);
        }
        pool.retain(|_, v| !v.is_empty());

        // If over global limits, trim oldest across all resolutions.
        let mut total_textures: usize = pool.values().map(|v| v.len()).sum();
        let mut total_bytes: u64 = pool
            .iter()
            .map(|(&(w, h, mips), entries)| texture_byte_size(w, h, mips) * entries.len() as u64)
            .sum();
        if total_textures > MAX_TEXTURE_POOL_SIZE || total_bytes > MAX_TEXTURE_POOL_BYTES {
            // Collect (key, index, last_used, bytes) for sorting
            let mut oldest: Vec<((u32, u32, u32), usize, std::time::Instant, u64)> =
                pool.iter()
                    .flat_map(|(&k, entries)| {
                        entries.iter().enumerate().map(move |(i, e)| {
                            (k, i, e.last_used, texture_byte_size(k.0, k.1, k.2))
                        })
                    })
                    .collect();
            oldest.sort_by(|a, b| b.3.cmp(&a.3).then(a.2.cmp(&b.2)));

            // Remove oldest entries (iterate in age order, adjust indices)
            let mut removed_per_key: HashMap<(u32, u32, u32), Vec<usize>> = HashMap::new();
            for &(key, idx, _, bytes) in &oldest {
                if total_textures <= MAX_TEXTURE_POOL_SIZE && total_bytes <= MAX_TEXTURE_POOL_BYTES
                {
                    break;
                }
                removed_per_key.entry(key).or_default().push(idx);
                total_textures = total_textures.saturating_sub(1);
                total_bytes = total_bytes.saturating_sub(bytes);
            }
            for (key, mut indices) in removed_per_key {
                indices.sort_unstable_by(|a, b| b.cmp(a)); // Remove from end first
                if let Some(entries) = pool.get_mut(&key) {
                    for idx in indices {
                        if idx < entries.len() {
                            entries.remove(idx);
                        }
                    }
                }
            }
            pool.retain(|_, v| !v.is_empty());
        }

        // Record pool size for leak detection
        let pool_size: usize = pool.values().map(|v| v.len()).sum();
        if let Some(m) = metrics {
            m.record_texture_pool_size(pool_size);
        }
    }

    pub fn texture_pool_stats(&self) -> (usize, u64) {
        let pool = self.texture_pool.lock();
        let count = pool.values().map(|v| v.len()).sum();
        let bytes = pool
            .iter()
            .map(|(&(w, h, mips), entries)| texture_byte_size(w, h, mips) * entries.len() as u64)
            .sum();
        (count, bytes)
    }
}

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
        let alpha_mode = caps
            .alpha_modes
            .first()
            .cloned()
            .unwrap_or(wgpu::CompositeAlphaMode::Auto);
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
        };
        // Shader precompilation is deferred to apply_config() which knows
        // the actual configured transition. No need to precompile 10 hardcoded
        // transitions when the user's config specifies exactly what they want.
        Ok(r)
    }

    pub fn resize_checked(&mut self, width: u32, height: u32) -> anyhow::Result<()> {
        if width > 0 && height > 0 {
            // Check capabilities FIRST to avoid hard panic in wgpu on Nvidia/Wayland
            let caps = self.target_caps();
            if caps.formats.is_empty() {
                warn!(
                    "Surface {} is not ready for configuration (no formats). Skipping reconfiguration.",
                    self.name
                );
                self.configured = false;
                return Ok(());
            }

            // Ensure format is supported
            if !caps.formats.contains(&self.config.format) {
                info!(
                    "Updating surface format for {} to {:?}",
                    self.name, caps.formats[0]
                );
                self.config.format = caps.formats[0];
            }

            self.config.width = width;
            self.config.height = height;

            info!("Configuring surface {} ({}x{})", self.name, width, height);
            self.surface.configure(&self.ctx.device, &self.config);
            if self.composition_texture.is_some() || self.composition_texture_view.is_some() {
                self.release_composition_texture("surface resize");
            }
            self.configured = true;
            // Force redraw after resize
            self.needs_redraw = true;

            // Redundant poll removed (Audit Point 33)
        }
        Ok(())
    }

    fn target_caps(&self) -> wgpu::SurfaceCapabilities {
        self.surface.get_capabilities(&self.ctx.adapter)
    }

    /// Ensures composition texture exists and matches current surface dimensions.
    /// Returns true when a new texture had to be created.
    fn ensure_composition_texture_internal(
        &mut self,
        warn_if_active: bool,
    ) -> anyhow::Result<bool> {
        // Check if we need to create or recreate the composition texture
        let needs_creation =
            self.composition_texture.is_none() || self.composition_texture_view.is_none();

        // Also check if dimensions match (if texture exists but size changed, recreate it)
        let size_mismatch = if self.composition_texture.is_some() {
            // Texture exists, check if we can determine its size
            // We can't easily check texture size, so we'll recreate if dimensions are invalid
            self.config.width == 0 || self.config.height == 0
        } else {
            false
        };

        let created = needs_creation || size_mismatch;
        if created {
            if self.config.width == 0 || self.config.height == 0 {
                // Can't create texture without valid dimensions
                return Err(anyhow::anyhow!(
                    "Cannot create composition texture: invalid dimensions ({}x{})",
                    self.config.width,
                    self.config.height
                ));
            }

            if warn_if_active && self.transition_active {
                warn!(
                    "[TRANSITION] {}: Composition texture missing during active transition, creating now ({}x{})",
                    self.name, self.config.width, self.config.height
                );
            } else {
                debug!(
                    "[RENDER] {}: Creating composition texture ({}x{})",
                    self.name, self.config.width, self.config.height
                );
            }

            // CRITICAL: Explicitly drop old composition texture before creating new one
            // This prevents memory leaks when surface is resized or texture is recreated
            drop(self.composition_texture.take());
            drop(self.composition_texture_view.take());

            let texture = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("Composition Texture"),
                size: wgpu::Extent3d {
                    width: self.config.width,
                    height: self.config.height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rgba8UnormSrgb,
                usage: wgpu::TextureUsages::TEXTURE_BINDING
                    | wgpu::TextureUsages::RENDER_ATTACHMENT
                    | wgpu::TextureUsages::COPY_SRC,
                view_formats: &[],
            });
            self.composition_texture_view =
                Some(texture.create_view(&wgpu::TextureViewDescriptor::default()));
            self.composition_texture = Some(texture);

            // Invalidate bind groups since texture changed
            self.transition_bind_group = None;
            self.blit_bind_group = None;
        }

        Ok(created)
    }

    /// Ensures composition texture exists and matches current surface dimensions
    /// Creates it if missing or if dimensions don't match
    fn ensure_composition_texture(&mut self) -> anyhow::Result<()> {
        self.ensure_composition_texture_internal(true).map(|_| ())
    }

    fn prewarm_transition_resources(&mut self) {
        if !self.configured
            || self.config.width == 0
            || self.config.height == 0
            || self.prev_texture.is_none()
            || self.current_texture.is_none()
        {
            return;
        }

        let warm_start = std::time::Instant::now();
        match self.ensure_composition_texture_internal(false) {
            Ok(true) => {
                debug!(
                    "[TRANSITION] {}: Prewarmed composition texture in {:.1}ms",
                    self.name,
                    warm_start.elapsed().as_secs_f64() * 1000.0
                );
            }
            Ok(false) => {}
            Err(e) => {
                warn!(
                    "[TRANSITION] {}: Failed to prewarm composition texture: {}",
                    self.name, e
                );
            }
        }
    }

    pub fn apply_config(&mut self, config: &crate::orchestration::OutputConfig) {
        self.active_transition = config.transition.clone();
        self.transition_duration = (config.transition_time as f32 / 1000.0).max(0.001);
        self.needs_redraw = true;

        let pipeline_prewarm_start = std::time::Instant::now();
        let prewarm_targets = transition_prewarm_candidates(&self.active_transition);
        for transition in &prewarm_targets {
            let _ = self.get_transition_pipeline(transition);
        }
        debug!(
            "[RENDER] {}: Transition pipeline prewarm completed in {:.1}ms ({} target(s))",
            self.name,
            pipeline_prewarm_start.elapsed().as_secs_f64() * 1000.0,
            prewarm_targets.len()
        );

        // Pre-compile only the configured transition in background (+ Fade as fallback).
        // This replaces the old approach of blindly precompiling 10 hardcoded transitions.
        if let Some(handle) = self.shader_precompile_handle.take() {
            handle.abort();
        }
        let transition = config.transition.clone();
        let name_for_bg = self.name.clone();
        let shader_precompile_handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            // Always precompile Fade (used as fallback on errors)
            let _ = crate::shaders::ShaderManager::get_builtin_shader(&Transition::Fade);
            // Precompile the user's configured transition (skip if it IS Fade or Random)
            if matches!(transition, Transition::Random) {
                let common = random_transition_prewarm_set();
                for t in &common {
                    let _ = crate::shaders::ShaderManager::get_builtin_shader(t);
                }
                let duration = start.elapsed();
                tracing::info!(
                    "[RENDER] {}: Background shader precompilation completed in {:.1}ms ({} common shaders for Random mode)",
                    name_for_bg,
                    duration.as_secs_f64() * 1000.0,
                    common.len(),
                );
            } else if !matches!(transition, Transition::Fade) {
                let _ = crate::shaders::ShaderManager::get_builtin_shader(&transition);
                let duration = start.elapsed();
                tracing::debug!(
                    "[RENDER] {}: Background shader precompilation completed in {:.2}ms ({})",
                    name_for_bg,
                    duration.as_secs_f64() * 1000.0,
                    transition.name()
                );
            }
        })
        .abort_handle();
        self.shader_precompile_handle = Some(shader_precompile_handle);
    }

    /// Pre-compiles common shaders to avoid stalls during the first transition.
    /// Compiles top 10 most commonly used transitions in background.
    #[allow(dead_code)]
    pub fn precompile_common_shaders(&self) {
        debug!("[RENDER] {}: Pre-compiling common shaders", self.name);
        // Pre-compile top 10 most common transitions
        let common_transitions = [
            Transition::Fade,
            Transition::CrossZoom { strength: 0.3 },
            Transition::Radial { smoothness: 0.5 },
            Transition::Circle,
            Transition::Directional {
                direction: [1.0, 0.0],
            },
            Transition::SimpleZoom {
                zoom_quickness: 0.5,
            },
            Transition::Ripple {
                amplitude: 0.1,
                speed: 1.0,
            },
            Transition::Swirl,
            Transition::Pixelize {
                squares_min: [10, 10],
                steps: 10,
            },
            Transition::Mosaic { endx: 20, endy: 20 },
        ];

        for transition in &common_transitions {
            let _ = self.get_transition_pipeline(transition);
        }
    }

    fn get_transition_pipeline(
        &self,
        transition: &Transition,
    ) -> Option<Arc<wgpu::RenderPipeline>> {
        let name = transition.name();

        // Check cache first (using Mutex in ctx)
        if let Some(pipe) = self.ctx.transition_pipelines.lock().get(&name) {
            return Some(pipe.clone());
        }

        // Not in cache, compile it
        // Note: For now we'll do synchronous compilation if missing,
        // but it will be cached for all subsequent calls across all monitors.
        debug!(
            "[RENDER] {}: Compiling shared transition pipeline: {}",
            self.name, name
        );

        // We'll move the actual compilation logic to a helper that populates the cache
        self.compile_transition_pipeline(transition)
    }

    fn compile_transition_pipeline(
        &self,
        transition: &Transition,
    ) -> Option<Arc<wgpu::RenderPipeline>> {
        let compile_start = std::time::Instant::now();
        let name = transition.name();

        // Get compiled WGSL shader code using ShaderManager (fragment shader only)
        let fragment_shader_code = if crate::shaders::ShaderManager::is_transition_broken(&name) {
            error!(
                "Skipping known-broken shader for {}. Falling back to fade.",
                name
            );
            match crate::shaders::ShaderManager::get_builtin_shader(&Transition::Fade) {
                Ok(code) => code,
                Err(fe) => {
                    error!("FATAL: Failed to compile fallback fade shader: {}", fe);
                    if let Some(m) = &self.metrics {
                        m.record_error("shader_compile_fatal");
                    }
                    return None;
                }
            }
        } else {
            match crate::shaders::ShaderManager::get_builtin_shader(transition) {
                Ok(code) => code,
                Err(e) => {
                    crate::shaders::ShaderManager::mark_transition_broken(&name);
                    error!(
                        "Failed to compile shader for {}: {}. Falling back to fade.",
                        name, e
                    );
                    if let Some(m) = &self.metrics {
                        m.record_error("shader_compile");
                    }
                    // Fallback to fade
                    match crate::shaders::ShaderManager::get_builtin_shader(&Transition::Fade) {
                        Ok(code) => code,
                        Err(fe) => {
                            error!("FATAL: Failed to compile fallback fade shader: {}", fe);
                            if let Some(m) = &self.metrics {
                                m.record_error("shader_compile_fatal");
                            }
                            return None;
                        }
                    }
                }
            }
        };

        // Create vertex shader module from the built-in quad.wgsl
        let vertex_shader = self
            .ctx
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("Quad Vertex Shader"),
                source: wgpu::ShaderSource::Wgsl(include_str!("shaders/quad.wgsl").into()),
            });

        // Create fragment shader module from the compiled GLSL transition
        let fragment_shader = self
            .ctx
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some(&format!("Transition Fragment Shader: {}", name)),
                source: wgpu::ShaderSource::Wgsl(fragment_shader_code.into()),
            });

        let pipeline_layout =
            self.ctx
                .device
                .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                    label: Some(&format!("Transition Pipeline Layout: {}", name)),
                    bind_group_layouts: &[&self.ctx.transition_bind_group_layout],
                    push_constant_ranges: &[],
                });

        // Use standard format for composition (always same across all renderers)
        let composition_format = wgpu::TextureFormat::Rgba8UnormSrgb;

        let pipeline = self
            .ctx
            .device
            .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
                label: Some(&format!("Transition Pipeline: {}", name)),
                layout: Some(&pipeline_layout),
                vertex: wgpu::VertexState {
                    module: &vertex_shader,
                    entry_point: Some("vs_main"),
                    compilation_options: Default::default(),
                    buffers: &[],
                },
                fragment: Some(wgpu::FragmentState {
                    module: &fragment_shader,
                    entry_point: Some("main"), // GLSL main() compiles to "main" in WGSL
                    compilation_options: Default::default(),
                    targets: &[Some(wgpu::ColorTargetState {
                        format: composition_format,
                        blend: Some(wgpu::BlendState::REPLACE),
                        write_mask: wgpu::ColorWrites::ALL,
                    })],
                }),
                primitive: wgpu::PrimitiveState::default(),
                depth_stencil: None,
                multisample: wgpu::MultisampleState::default(),
                multiview: None,
                cache: self.ctx.pipeline_cache.as_ref(),
            });

        let pipeline_arc = Arc::new(pipeline);

        // Update cache
        self.ctx
            .transition_pipelines
            .lock()
            .insert(name, pipeline_arc.clone());

        // Record shader compile CPU time
        if let Some(m) = &self.metrics {
            let compile_duration = compile_start.elapsed();
            m.record_shader_compile_cpu_time(compile_duration);
        }

        Some(pipeline_arc)
    }

    pub fn render(
        &mut self,
        context: BackendContext,
        _frame_time: std::time::Instant,
    ) -> anyhow::Result<()> {
        let render_start = std::time::Instant::now();
        let frame_time = render_start;

        if !self.transition_active && !self.content_swap_pending && self.prev_texture.is_some() {
            self.release_prev_texture("render idle cleanup");
        }
        if !self.transition_active
            && !self.content_swap_pending
            && self.composition_texture.is_some()
        {
            self.release_composition_texture("render idle cleanup");
        }

        // CRITICAL: Reset per-frame state at the start of each render cycle
        // This flag tracks whether a transition was rendered in THIS frame
        self.transition_rendered_this_frame = false;

        // CRITICAL: Always render if transition is in progress, even if needs_redraw is false
        // This ensures transitions continue smoothly
        if !self.configured {
            // Try to configure one last time if we have dimensions
            if self.config.width > 0 && self.config.height > 0 {
                let _ = self.resize_checked(self.config.width, self.config.height);
            }
            if !self.configured {
                return Ok(()); // Skip render until configured
            }
        }

        // Always render if transition is active, regardless of needs_redraw
        if !self.transition_active && !self.needs_redraw {
            return Ok(()); // Skip render if no transition and no redraw needed
        }

        // If we are here, we are going to render.
        // CRITICAL: Always keep needs_redraw=true during transitions to ensure continuous rendering
        // This ensures transitions complete smoothly without getting stuck
        // Note: Don't reset needs_redraw here - do it AFTER we've actually rendered and presented

        let surface_acquire_start = std::time::Instant::now();
        let output = match self.surface.get_current_texture() {
            Ok(t) => t,
            Err(wgpu::SurfaceError::Lost) => {
                warn!(
                    "Surface Lost for {}. Marking not-configured to trigger re-creation.",
                    self.name
                );
                self.configured = false;
                self.needs_redraw = true; // Retry ASAP
                self.frame_callback_pending = false; // Callback won't fire for lost surface
                return Ok(());
            }
            Err(wgpu::SurfaceError::Outdated) => {
                warn!("Surface Outdated for {}. Reconfiguring.", self.name);
                self.configured = false;
                self.needs_redraw = true; // Retry ASAP
                self.frame_callback_pending = false; // Callback won't fire for outdated surface
                return Ok(());
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("timeout") {
                    debug!(
                        "Surface acquisition timeout for {}, skipping frame.",
                        self.name
                    );
                    self.needs_redraw = true; // Try again next loop
                    return Ok(());
                }
                error!(
                    "Failed to get current surface texture for {}: {}",
                    self.name, err_str
                );
                return Ok(());
            }
        };
        let surface_acquire_duration = surface_acquire_start.elapsed();
        if surface_acquire_duration > std::time::Duration::from_millis(8) {
            debug!(
                "[FRAME] {}: Surface acquisition took {:.1}ms",
                self.name,
                surface_acquire_duration.as_secs_f64() * 1000.0
            );
        }
        let view = output
            .texture
            .create_view(&wgpu::TextureViewDescriptor::default());

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Main Render Encoder"),
            });

        // Update transition progress BEFORE checking if we should render transition
        // This ensures progress is accurate for the current frame
        // For image transitions: always advance progress once started (textures are always available)
        // For video transitions: only freeze if we're waiting for the first video frame
        if self.transition_active {
            // Advance transition normally using frame_time for synchronization
            if let Some(start) = self.transition_start_time {
                let elapsed = frame_time.saturating_duration_since(start).as_secs_f32();
                let new_progress = (elapsed / self.transition_duration).min(1.0);
                if new_progress != self.transition_progress {
                    trace!(
                        "[TRANSITION] {}: Progress updated {:.3} -> {:.3} (elapsed={:.3}s, duration={:.3}s)",
                        self.name,
                        self.transition_progress,
                        new_progress,
                        elapsed,
                        self.transition_duration
                    );
                }
                self.transition_progress = new_progress;

                // Update stats
                if let Some(stats) = &mut self.transition_stats {
                    stats.frame_count += 1;
                }

                // Check if transition completed
                if self.transition_progress >= 1.0 {
                    // Only set flag if transition was just completed (not already completed)
                    if self.transition_active {
                        self.transition_active = false;
                        self.transition_just_completed = true;
                        self.arm_display_timer_on_present();

                        // Log Audit Report and record metrics
                        if let Some(stats) = self.transition_stats.take() {
                            let duration = stats.start_time.elapsed();
                            let duration_secs = duration.as_secs_f64();

                            // Record transition duration in metrics
                            if let Some(m) = &self.metrics {
                                m.record_transition(duration);
                            }
                            let fps = if duration_secs > 0.001 {
                                stats.frame_count as f64 / duration_secs
                            } else {
                                0.0
                            };
                            let drift = duration.as_secs_f32() - stats.target_duration;
                            let batch_info = stats
                                .batch_id
                                .map(|b| format!(" (Batch: {:x})", b))
                                .unwrap_or_default();

                            info!(
                                "[AUDIT] Transition Completed {}{}:\n  - Duration: {:.3}s (Target: {:.3}s)\n  - Frames: {} (Avg {:.1} FPS)\n  - Drift: {:.3}s",
                                self.name,
                                batch_info,
                                duration.as_secs_f32(),
                                stats.target_duration,
                                stats.frame_count,
                                fps,
                                drift
                            );
                        } else {
                            info!(
                                "[TRANSITION] {}: Transition completed (progress={:.3}) - No stats available",
                                self.name, self.transition_progress
                            );
                        }
                    }
                }
            } else {
                // Start timing from the first render of ready content. This prevents decode or
                // queue delay from consuming the transition before it is ever presented.
                let start = frame_time;
                self.transition_start_time = Some(start);

                self.transition_progress = 0.0;

                // Initialize stats
                self.transition_stats = Some(TransitionStats {
                    start_time: start,
                    frame_count: 0,
                    target_duration: self.transition_duration,
                    batch_id: self.active_batch_id,
                });

                info!(
                    "[TRANSITION] {}: Starting transition (duration={:.3}s, initial_progress={:.3})",
                    self.name, self.transition_duration, self.transition_progress
                );
            }
        }

        // Render transition if we have all required textures and transition is active
        // Ensure component texture exists if transition is active
        // This must be done BEFORE checking should_render_transition to ensure we don't skip
        // the transition just because the texture was lazily dropped or missing.
        if self.transition_active {
            if let Err(e) = self.ensure_composition_texture() {
                error!(
                    "[TRANSITION] {}: Failed to ensure composition texture: {}",
                    self.name, e
                );
            }
        }

        // Render transition if we have all required textures and transition is active
        let should_render_transition = self.transition_active
            && self.prev_texture.is_some()
            && self.current_texture.is_some()
            && self.composition_texture.is_some()
            && self.composition_texture_view.is_some();

        // CRITICAL: Removed TRACE logs from hot path for performance
        // These were causing 5-10% CPU overhead when called every frame

        if should_render_transition {
            // 1. Get/Create pipeline (this will cache it if needed)
            let pipeline = match self.get_transition_pipeline(&self.active_transition) {
                Some(p) => p,
                None => {
                    warn!(
                        "[TRANSITION] {}: Failed to get/create transition pipeline for {}",
                        self.name,
                        self.active_transition.name()
                    );
                    return Ok(());
                }
            };

            // 2. Now we can do immutable borrows
            let raw_params = self.active_transition.to_params();
            let uniforms = TransitionUniforms {
                progress: self.transition_progress,
                screen_aspect: self.config.width as f32 / self.config.height as f32,
                prev_aspect: self.prev_aspect,
                next_aspect: self.current_aspect,
                params: bytemuck::cast(raw_params),
            };
            self.ctx
                .queue
                .write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));

            // Recreate bind group only if invalidated (set to None when textures change)
            self.update_transition_bind_group();

            if let Some(bind_group) = &self.transition_bind_group {
                let composition_view = match self.composition_texture_view.as_ref() {
                    Some(v) => v,
                    None => {
                        error!("Composition texture view missing during transition render");
                        return Ok(());
                    }
                };
                let mut render_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                    label: Some("Transition Render Pass"),
                    color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                        view: composition_view,
                        resolve_target: None,
                        ops: wgpu::Operations {
                            load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                            store: wgpu::StoreOp::Store,
                        },
                    })],
                    depth_stencil_attachment: None,
                    timestamp_writes: None,
                    occlusion_query_set: None,
                });
                render_pass.set_pipeline(&pipeline);
                render_pass.set_bind_group(0, bind_group, &[]);
                render_pass.draw(0..3, 0..1);

                // Mark that transition was successfully rendered this frame
                self.transition_rendered_this_frame = true;

                // debug!("[TRANSITION] {}: Rendered transition frame ...", self.name);
            } else {
                // Transition should render but bind group is missing
                warn!(
                    "[TRANSITION] {}: Transition render FAILED - bind_group missing, transition={}",
                    self.name,
                    self.active_transition.name()
                );
                // Don't set transition_rendered_this_frame - transition didn't actually render
            }

            // CLEANUP: Return prev_texture to pool when transition is TRULY finished
            if self.transition_progress >= 1.0
                && self.current_texture.is_some()
                && self.prev_texture.is_some()
            {
                debug!(
                    "[TRANSITION] {}: Transition completed, returning prev_texture to pool",
                    self.name
                );
                // Return prev_texture to pool for reuse (instead of just dropping)
                if let Some(prev_tex) = self.prev_texture.take() {
                    if let Some((w, h)) = self.prev_texture_size.take() {
                        self.ctx.return_texture_to_pool(prev_tex, w, h);
                    }
                    // If size unknown, texture is still dropped here (freed by WGPU)
                }
                self.prev_texture_view = None;
                self.transition_bind_group = None;
                self.blit_bind_group = None;
                self.transition_start_time = None;
                self.transition_active = false;
                self.release_composition_texture("transition completed");
            }
        }

        // Removed the "else if self.transition_active { ... }" block because we moved ensure_composition_texture
        // to the top and we log missing resources above.

        let height = self.config.height as f32;
        if !self.transition_active {
            let uniforms = TransitionUniforms {
                progress: 1.0,
                screen_aspect: self.config.width as f32 / height,
                prev_aspect: 1.0,
                next_aspect: self.current_aspect,
                params: [[0.0; 4]; 7],
            };
            self.ctx
                .queue
                .write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));
        }

        // Logic: Show Current if ready. Else Show Prev. Else (Black).
        // Blit Selection Logic
        #[derive(Copy, Clone, PartialEq, Debug)]
        enum BlitSource {
            Current,
            Prev,
            Composition,
        }

        // Blit source selection logic:
        let blit_source = if self.current_texture.is_some() {
            if !self.transition_active || self.prev_texture.is_none() {
                // Transition complete or no previous texture -> show current
                Some(BlitSource::Current)
            } else if self.transition_active
                && self.prev_texture.is_some()
                && self.current_texture.is_some()
                && self.composition_texture.is_some()
                && self.composition_texture_view.is_some()
            {
                if !self.transition_rendered_this_frame {
                    // This can happen if setup passed but pipeline/bind_group failed,
                    // OR if this is the very first frame where we created composition but didn't render yet?
                    // Actually we ensured rendering above.
                    // If we missed rendering, showing composition (which is empty) might flash black.
                    // But usually safe to show composition as it was cleared to black.
                    debug!(
                        "[RENDER] {}: Using composition (rendered_this_frame={})",
                        self.name, self.transition_rendered_this_frame
                    );
                }
                Some(BlitSource::Composition)
            } else if self.transition_active && self.prev_texture.is_some() {
                // Transition active but missing required resources -> fall back to prev
                warn!(
                    "[RENDER] {}: Transition FALLBACK to PREV - missing resources (comp_view={})",
                    self.name,
                    self.composition_texture_view.is_some()
                );
                Some(BlitSource::Prev)
            } else {
                // Shouldn't happen, but fallback to current
                Some(BlitSource::Current)
            }
        } else if self.prev_texture.is_some() {
            // No current texture but have previous -> show previous (during transition)
            // This should only happen briefly during transitions
            debug!(
                "[RENDER] {}: No current_texture, falling back to prev_texture (transition_active={})",
                self.name, self.transition_active
            );
            Some(BlitSource::Prev)
        } else {
            // No textures at all -> black screen
            None
        };

        // Removed TRACE log from hot path for performance

        // If no blit source, we can't render anything - return early
        // But keep needs_redraw=true so we try again next frame
        let blit_source = match blit_source {
            Some(s) => s,
            None => {
                debug!(
                    "[RENDER] {}: No blit source available (current={}, prev={})",
                    self.name,
                    self.current_texture.is_some(),
                    self.prev_texture.is_some()
                );

                if matches!(context, BackendContext::X11) {
                    // On X11, leaving the acquired surface unpresented here can wedge that
                    // output into repeated acquisition timeouts. Present an explicit black
                    // frame so startup stays black instead of white and the swapchain keeps
                    // advancing until real content arrives.
                    {
                        let _clear_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                            label: Some("Black Frame Render Pass"),
                            color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                                view: &view,
                                resolve_target: None,
                                ops: wgpu::Operations {
                                    load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                                    store: wgpu::StoreOp::Store,
                                },
                            })],
                            depth_stencil_attachment: None,
                            timestamp_writes: None,
                            occlusion_query_set: None,
                        });
                    }

                    self.ctx.queue.submit(std::iter::once(encoder.finish()));
                    output.present();
                    self.last_present_time = std::time::Instant::now();
                    self.needs_redraw = false;

                    if let Some(m) = &self.metrics {
                        m.record_renderer_cpu_time(render_start.elapsed());
                    }
                }

                return Ok(());
            }
        };

        let is_comp = blit_source == BlitSource::Composition;
        let is_prev = blit_source == BlitSource::Prev;

        // Always recreate bind group if source changed or doesn't exist
        // This ensures we're using the correct texture after content switches
        let needs_recreate = self.blit_bind_group.is_none()
            || self.blit_source_is_composition != is_comp
            || self.blit_source_is_prev != is_prev;

        if needs_recreate {
            let tex_view = match blit_source {
                BlitSource::Current => self.current_texture_view.as_ref(),
                BlitSource::Prev => self.prev_texture_view.as_ref(),
                BlitSource::Composition => self.composition_texture_view.as_ref(),
            };
            match tex_view {
                Some(v) => {
                    // Create bind group with the texture
                    self.blit_bind_group = Some(self.ctx.device.create_bind_group(
                        &wgpu::BindGroupDescriptor {
                            label: Some("Blit Bind Group"),
                            layout: &self.ctx.blit_bind_group_layout,
                            entries: &[
                                wgpu::BindGroupEntry {
                                    binding: 0,
                                    resource: self.uniform_buffer.as_entire_binding(),
                                },
                                wgpu::BindGroupEntry {
                                    binding: 1,
                                    resource: wgpu::BindingResource::TextureView(v),
                                },
                                wgpu::BindGroupEntry {
                                    binding: 2,
                                    resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                                },
                            ],
                        },
                    ));
                    self.blit_source_is_composition = is_comp;
                    self.blit_source_is_prev = is_prev;
                }
                None => {
                    // Fallback logic: if Composition fails, try Prev, then Current
                    let fallback_view = match blit_source {
                        BlitSource::Composition => {
                            warn!(
                                "[RENDER] {}: Composition texture view missing, falling back to prev",
                                self.name
                            );
                            self.prev_texture_view.as_ref().or_else(|| {
                                warn!("[RENDER] {}: Prev texture view also missing, falling back to current", self.name);
                                self.current_texture_view.as_ref()
                            })
                        }
                        BlitSource::Prev => {
                            warn!(
                                "[RENDER] {}: Prev texture view missing, falling back to current",
                                self.name
                            );
                            self.current_texture_view.as_ref()
                        }
                        BlitSource::Current => {
                            error!(
                                "[RENDER] {}: Current texture view missing, cannot render",
                                self.name
                            );
                            None
                        }
                    };

                    match fallback_view {
                        Some(v) => {
                            // Update source to match fallback
                            let is_comp = false;
                            let is_prev = matches!(blit_source, BlitSource::Prev)
                                || matches!(blit_source, BlitSource::Composition);
                            // Recreate bind group with fallback texture
                            self.blit_bind_group = Some(self.ctx.device.create_bind_group(
                                &wgpu::BindGroupDescriptor {
                                    label: Some("Blit Bind Group (Fallback)"),
                                    layout: &self.ctx.blit_bind_group_layout,
                                    entries: &[
                                        wgpu::BindGroupEntry {
                                            binding: 0,
                                            resource: self.uniform_buffer.as_entire_binding(),
                                        },
                                        wgpu::BindGroupEntry {
                                            binding: 1,
                                            resource: wgpu::BindingResource::TextureView(v),
                                        },
                                        wgpu::BindGroupEntry {
                                            binding: 2,
                                            resource: wgpu::BindingResource::Sampler(
                                                &self.sampler_linear,
                                            ),
                                        },
                                    ],
                                },
                            ));
                            self.blit_source_is_composition = is_comp;
                            self.blit_source_is_prev = is_prev;
                        }
                        None => {
                            error!(
                                "Texture view missing for blit source {:?} and all fallbacks failed (current={}, prev={}, composition={})",
                                blit_source,
                                self.current_texture_view.is_some(),
                                self.prev_texture_view.is_some(),
                                self.composition_texture_view.is_some()
                            );
                            return Ok(()); // Can't render anything
                        }
                    }
                }
            };
        }

        // Get format-specific blit pipeline from shared context
        let blit_pipeline = self.ctx.get_blit_pipeline(self.config.format);

        {
            let mut render_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("Blit Render Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });

            render_pass.set_pipeline(&blit_pipeline);
            if let Some(bg) = &self.blit_bind_group {
                render_pass.set_bind_group(0, bg, &[]);
                render_pass.draw(0..3, 0..1);
            } else {
                error!(
                    "[RENDER] {}: blit_bind_group is None, cannot render!",
                    self.name
                );
                return Ok(()); // Can't render without bind group
            }
        } // render_pass dropped here

        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        output.present();
        if self.display_timer_pending {
            self.display_timer_pending = false;
            self.display_timer_ready = true;
        }

        // Note: frame_callback_pending is reset by the main loop when callback is received
        // Don't reset it here to avoid race conditions

        if !self.transition_active {
            self.transition_start_time = None;
        }

        self.last_present_time = std::time::Instant::now();

        // CRITICAL: Reset needs_redraw AFTER we've actually rendered and presented
        // This ensures we render at least once for static images
        // We reset to false even for video; video.rs will set it to true
        // when a new frame is uploaded, preventing redundant renders between frames.
        self.needs_redraw = false;

        // Record renderer CPU time
        if let Some(m) = &self.metrics {
            let render_duration = render_start.elapsed();
            m.record_renderer_cpu_time(render_duration);
        }

        Ok(())
    }

    /// Request a frame callback from Wayland compositor
    /// This should be called when we need to render, and we'll wait for the callback
    pub fn request_frame_callback(
        &mut self,
        layer_surface: &LayerSurface,
        qh: &QueueHandle<crate::wayland::WaylandBackend>,
    ) -> bool {
        if self.frame_callback_pending {
            // Check failsafe: if pending for > 500ms, assume lost and allow re-request
            if let Some(r) = self.last_frame_request {
                if r.elapsed().as_millis() > 500 {
                    warn!(
                        "[FRAME] {}: Frame callback stuck for 500ms, re-requesting!",
                        self.name
                    );
                    self.frame_callback_pending = false; // Reset to allow re-request
                } else {
                    return false; // Truly pending
                }
            }
        }

        let wl_surface = layer_surface.wl_surface();
        wl_surface.frame(qh, wl_surface.clone());

        // CRITICAL: Commit the surface to ensure the frame callback is registered and processed
        // This prevents the compositor from "hanging" the surface if it's waiting for a commit
        wl_surface.commit();

        self.frame_callback_pending = true;
        self.last_frame_request = Some(std::time::Instant::now());
        tracing::trace!(
            "[FRAME] {}: Requested frame callback (configured={}, needs_redraw={}, transition_progress={:.3})",
            self.name,
            self.configured,
            self.needs_redraw,
            self.transition_progress
        );
        true
    }

    pub fn set_content_type(&mut self, content_type: crate::queue::ContentType) {
        if self.valid_content_type == crate::queue::ContentType::Video
            && content_type != crate::queue::ContentType::Video
        {
            self.release_video_backend_resources("leaving video content");
            self.last_video_source_size = None;
            self.last_video_presentation_size = None;
        }
        self.valid_content_type = content_type;
    }

    pub fn retained_texture_footprint(&self) -> RetainedTextureFootprint {
        let current_bytes = match (self.current_texture.as_ref(), self.current_texture_size) {
            (Some(texture), Some((w, h))) => texture_byte_size(w, h, texture.mip_level_count()),
            _ => 0,
        };
        let prev_bytes = match (self.prev_texture.as_ref(), self.prev_texture_size) {
            (Some(texture), Some((w, h))) => texture_byte_size(w, h, texture.mip_level_count()),
            _ => 0,
        };
        let composition_bytes = if self.composition_texture.is_some() {
            texture_byte_size(self.config.width.max(1), self.config.height.max(1), 1)
        } else {
            0
        };

        let mut video_aux_bytes = 0u64;
        if let Some((w, h)) = self.nv12_staging_size {
            video_aux_bytes = video_aux_bytes.saturating_add(yuv420_aux_byte_size(w, h));
        }
        if let Some((w, h)) = self.i420_staging_size {
            video_aux_bytes = video_aux_bytes.saturating_add(yuv420_aux_byte_size(w, h));
        }
        if let Some(cache) = &self.cuda_textures {
            video_aux_bytes =
                video_aux_bytes.saturating_add(yuv420_aux_byte_size(cache.width, cache.height));
        }

        RetainedTextureFootprint {
            current_bytes,
            prev_bytes,
            composition_bytes,
            video_aux_bytes,
        }
    }

    /// Check if current_texture exists (used for throttling logic)
    pub fn has_current_texture(&self) -> bool {
        self.current_texture.is_some()
    }

    /// Check if any renderable content exists (current or previous texture)
    pub fn has_any_content(&self) -> bool {
        self.current_texture.is_some() || self.prev_texture.is_some()
    }

    pub fn needs_wayland_immediate_work(&self) -> bool {
        self.transition_active
            || (self.needs_redraw
                && (!self.frame_callback_pending || self.frame_callback_pending_too_long(500)))
    }

    pub fn next_wayland_retry_deadline(
        &self,
        retry_after: std::time::Duration,
    ) -> Option<std::time::Instant> {
        if self.needs_redraw && self.frame_callback_pending {
            self.last_frame_request.map(|t| t + retry_after)
        } else {
            None
        }
    }

    pub fn trim_idle_retained_resources(&mut self) {
        if !self.transition_active && !self.content_swap_pending {
            if self.prev_texture.is_some() {
                self.release_prev_texture("idle trim");
            }
            if self.composition_texture.is_some() {
                self.release_composition_texture("idle trim");
            }
        }

        if !self.has_any_content() || self.valid_content_type != crate::queue::ContentType::Video {
            self.release_video_backend_resources("idle trim");
        }
    }

    /// Get the duration that the frame callback has been pending, if any
    /// Returns None if no callback is pending or if timing info is unavailable
    pub fn frame_callback_pending_duration(&self) -> Option<std::time::Duration> {
        if self.frame_callback_pending {
            self.last_frame_request.map(|t| t.elapsed())
        } else {
            None
        }
    }

    /// Check if frame callback has been pending for too long (indicating we're stuck)
    /// This is used to prevent memory leaks by throttling frame uploads when stuck
    pub fn frame_callback_pending_too_long(&self, threshold_ms: u64) -> bool {
        self.frame_callback_pending_duration()
            .is_some_and(|d| d.as_millis() > threshold_ms as u128)
    }

    #[allow(dead_code)]
    pub fn upload_image_file(&mut self, path: &std::path::Path) -> anyhow::Result<()> {
        let _load_start = std::time::Instant::now();
        let img = image::open(path)?;
        let rgba = img.to_rgba8();
        let (width, height) = rgba.dimensions();
        let data = rgba.into_raw();

        self.upload_image_data(data, width, height)
    }

    pub fn upload_image_data(
        &mut self,
        data: Vec<u8>,
        width: u32,
        height: u32,
    ) -> anyhow::Result<()> {
        let upload_start = std::time::Instant::now();

        self.begin_content_swap();

        // If an image upload arrives without a pending swap, replace the current
        // texture directly to avoid keeping unused image resources alive.
        if self.current_texture.is_some() {
            self.current_texture_view = None;
            if let Some(curr) = self.current_texture.take() {
                if let Some((w, h)) = self.current_texture_size.take() {
                    self.ctx.return_texture_to_pool(curr, w, h);
                }
            }
        }

        // Static images are pre-sized close to the output in the CPU prep path, so
        // full mip chains mostly add upload cost and idle GPU memory without helping
        // transition correctness. Keep them single-mip so they can reuse the texture pool.
        let texture = self.ctx.get_texture_from_pool(
            width,
            height,
            wgpu::TextureUsages::TEXTURE_BINDING
                | wgpu::TextureUsages::COPY_DST
                | wgpu::TextureUsages::RENDER_ATTACHMENT,
            self.metrics.as_deref(),
        );

        self.ctx.queue.write_texture(
            wgpu::ImageCopyTexture {
                texture: &texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            &data,
            wgpu::ImageDataLayout {
                offset: 0,
                bytes_per_row: Some(4 * width),
                rows_per_image: Some(height),
            },
            wgpu::Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
        );

        self.current_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor {
            label: Some("Image Texture View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            dimension: Some(wgpu::TextureViewDimension::D2),
            aspect: wgpu::TextureAspect::All,
            base_mip_level: 0,
            mip_level_count: Some(1),
            base_array_layer: 0,
            array_layer_count: None,
        }));

        self.current_texture = Some(texture);
        self.current_aspect = width as f32 / height as f32;
        self.current_texture_size = Some((width, height));
        self.last_video_source_size = None;
        self.last_video_presentation_size = None;
        self.needs_redraw = true;
        self.valid_content_type = crate::queue::ContentType::Image;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;

        if self.prev_texture.is_some() {
            self.transition_start_time = None;
            self.transition_progress = 0.0;
            self.transition_active = true;
            // P-32: Reset batch_start_time so transition timer starts fresh from
            // actual upload time, preventing decode latency from eating into duration
            self.batch_start_time = None;
            self.prewarm_transition_resources();
            info!(
                "[TRANSITION] {}: Image data uploaded - transition will start on next render frame",
                self.name
            );
        } else {
            self.transition_active = false;
            self.transition_progress = 1.0;
            self.transition_just_completed = true; // Signal completion for instant switch
            self.arm_display_timer_on_present();
            info!(
                "[TRANSITION] {}: Image data uploaded (Instant) - transition signaled as complete",
                self.name
            );
        }

        let _duration = upload_start.elapsed();
        // Removed TRACE log - use debug! if needed for troubleshooting

        Ok(())
    }

    pub fn upload_frame(&mut self, frame: &crate::video::VideoFrame) {
        if !self.transition_active && !self.content_swap_pending && self.prev_texture.is_some() {
            self.release_prev_texture("video upload cleanup");
        }

        if self.valid_content_type != crate::queue::ContentType::Video
            || frame.session_id != self.active_video_session_id
        {
            debug!(
                "[VIDEO] {}: Discarding stale video frame - valid_type={:?}, frame_session={}, active_session={}",
                self.name, self.valid_content_type, frame.session_id, self.active_video_session_id
            );
            return;
        }

        let is_first_frame_after_switch =
            self.content_swap_pending || self.current_texture.is_none();

        if is_first_frame_after_switch && self.video_first_frame_time.is_none() {
            self.video_first_frame_time = Some(std::time::Instant::now());
        }

        if is_first_frame_after_switch {
            self.begin_content_swap();
        }

        let source_width = frame.width;
        let source_height = frame.height;
        let (presentation_width, presentation_height) = match frame.format {
            crate::video::VideoFrameFormat::Rgba => (source_width, source_height),
            _ => compute_cover_target_dimensions(
                source_width,
                source_height,
                self.config.width.max(1),
                self.config.height.max(1),
            ),
        };

        // Get or reuse the RGBA output texture (same size AND single mip level = reuse)
        let needs_new_texture = match self.current_texture.as_ref() {
            Some(curr) => {
                self.current_texture_size != Some((presentation_width, presentation_height))
                    || curr.mip_level_count() > 1
            }
            None => true,
        };
        let texture = match self.current_texture.take() {
            Some(curr) => {
                if !needs_new_texture {
                    curr
                } else {
                    self.current_texture_view = None;
                    if let Some((w, h)) = self.current_texture_size {
                        self.ctx.return_texture_to_pool(curr, w, h);
                    }
                    self.ctx.get_texture_from_pool(
                        presentation_width,
                        presentation_height,
                        wgpu::TextureUsages::TEXTURE_BINDING
                            | wgpu::TextureUsages::COPY_DST
                            | wgpu::TextureUsages::RENDER_ATTACHMENT,
                        self.metrics.as_deref(),
                    )
                }
            }
            _ => self.ctx.get_texture_from_pool(
                presentation_width,
                presentation_height,
                wgpu::TextureUsages::TEXTURE_BINDING
                    | wgpu::TextureUsages::COPY_DST
                    | wgpu::TextureUsages::RENDER_ATTACHMENT,
                self.metrics.as_deref(),
            ),
        };

        if is_first_frame_after_switch {
            let path_name = match &frame.format {
                crate::video::VideoFrameFormat::CudaNv12 { .. } => "CUDA zero-copy NV12",
                crate::video::VideoFrameFormat::DmaBufNv12 { .. } => "DMA-BUF zero-copy NV12",
                crate::video::VideoFrameFormat::Nv12 { .. } => "NV12 CPU upload",
                crate::video::VideoFrameFormat::I420 { .. } => "I420 CPU upload",
                crate::video::VideoFrameFormat::Rgba => "RGBA CPU upload (legacy)",
            };
            info!(
                "[VIDEO] {}: Frame decode path: {} source={}x{} presentation={}x{}",
                self.name,
                path_name,
                source_width,
                source_height,
                presentation_width,
                presentation_height
            );
        }

        self.last_video_source_size = Some((source_width, source_height));
        self.last_video_presentation_size = Some((presentation_width, presentation_height));

        match &frame.format {
            crate::video::VideoFrameFormat::CudaNv12 { .. } => {
                self.release_nv12_staging("cuda frame path");
                self.release_i420_staging("cuda frame path");
            }
            crate::video::VideoFrameFormat::DmaBufNv12 { .. } => {
                self.release_video_backend_resources("dmabuf frame path");
            }
            crate::video::VideoFrameFormat::Nv12 { .. } => {
                self.release_i420_staging("nv12 frame path");
                self.release_cuda_cache();
            }
            crate::video::VideoFrameFormat::I420 { .. } => {
                self.release_nv12_staging("i420 frame path");
                self.release_cuda_cache();
            }
            crate::video::VideoFrameFormat::Rgba => {
                self.release_video_backend_resources("rgba frame path");
            }
        }

        match &frame.format {
            crate::video::VideoFrameFormat::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                self.upload_frame_nv12(
                    frame,
                    &texture,
                    source_width,
                    source_height,
                    *y_stride,
                    *uv_offset,
                    *uv_stride,
                );
            }
            crate::video::VideoFrameFormat::I420 {
                y_stride,
                u_offset,
                u_stride,
                v_offset,
                v_stride,
            } => {
                self.upload_frame_i420(
                    frame,
                    &texture,
                    source_width,
                    source_height,
                    *y_stride,
                    *u_offset,
                    *u_stride,
                    *v_offset,
                    *v_stride,
                );
            }
            crate::video::VideoFrameFormat::Rgba => {
                self.upload_frame_rgba(frame, &texture, source_width, source_height);
            }
            crate::video::VideoFrameFormat::DmaBufNv12 {
                y_fd,
                y_stride,
                y_offset,
                uv_fd,
                uv_stride,
                uv_offset,
            } => {
                if !self.upload_frame_dmabuf_nv12(
                    &texture,
                    source_width,
                    source_height,
                    y_fd.as_raw_fd(),
                    *y_stride,
                    *y_offset,
                    uv_fd.as_raw_fd(),
                    *uv_stride,
                    *uv_offset,
                ) {
                    warn!("[VIDEO] DMA-BUF import failed, falling back to NV12 CPU path");
                    self.upload_frame_nv12(
                        frame,
                        &texture,
                        source_width,
                        source_height,
                        *y_stride,
                        *uv_offset,
                        *uv_stride,
                    );
                }
            }
            crate::video::VideoFrameFormat::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                if !self.upload_frame_cuda_nv12(
                    frame,
                    &texture,
                    source_width,
                    source_height,
                    *y_stride,
                    *uv_offset,
                    *uv_stride,
                ) {
                    error!(
                        "[VIDEO] {}: CUDA zero-copy failed, falling back to NV12 CPU upload",
                        self.name
                    );
                    self.upload_frame_nv12(
                        frame,
                        &texture,
                        source_width,
                        source_height,
                        *y_stride,
                        *uv_offset,
                        *uv_stride,
                    );
                }
            }
        }

        if needs_new_texture || self.current_texture_view.is_none() {
            drop(self.current_texture_view.take());

            self.current_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor {
                label: Some("Video Texture View"),
                format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                dimension: Some(wgpu::TextureViewDimension::D2),
                aspect: wgpu::TextureAspect::All,
                base_mip_level: 0,
                mip_level_count: None,
                base_array_layer: 0,
                array_layer_count: None,
            }));
            self.transition_bind_group = None;
            self.blit_bind_group = None;
        }

        self.current_texture = Some(texture);
        self.current_texture_size = Some((presentation_width, presentation_height));
        self.current_aspect = source_width as f32 / source_height as f32;
        self.needs_redraw = true;

        if is_first_frame_after_switch {
            if let Some(m) = &self.metrics {
                if let Some(start_time) = self.video_first_frame_time {
                    let first_frame_duration = start_time.elapsed();
                    m.record_video_first_frame(first_frame_duration);
                    self.video_first_frame_time = None;
                }
            }

            if self.prev_texture.is_some() {
                info!(
                    "[TRANSITION] {}: First video frame after switch - transition will start on first render frame",
                    self.name
                );
                self.transition_start_time = None;
                self.transition_progress = 0.0;
                self.transition_active = true;
                self.prewarm_transition_resources();
            } else {
                info!(
                    "[TRANSITION] {}: First video frame after switch (Instant) - transition signaled as complete",
                    self.name
                );
                self.transition_active = false;
                self.transition_progress = 1.0;
                self.transition_just_completed = true;
                self.arm_display_timer_on_present();
            }
        }

        tracing::trace!(
            "[TRANSITION] {}: Video frame uploaded - current_texture={}, prev_texture={}, transition_progress={:.3}, transition_start_time={:?}",
            self.name,
            self.current_texture.is_some(),
            self.prev_texture.is_some(),
            self.transition_progress,
            self.transition_start_time.is_some()
        );

        // device.poll deferred to end-of-loop to avoid redundant driver calls (P-14)
    }

    fn upload_plane_texture(
        queue: &wgpu::Queue,
        stride_temp_buffer: &mut Vec<u8>,
        renderer_name: &str,
        texture: &wgpu::Texture,
        src: &[u8],
        src_offset: usize,
        src_stride: u32,
        copy_width: u32,
        copy_height: u32,
        bytes_per_pixel: u32,
        plane_name: &str,
    ) -> bool {
        if copy_width == 0 || copy_height == 0 {
            return true;
        }

        let bytes_per_row = copy_width.saturating_mul(bytes_per_pixel);
        let aligned_bytes_per_row = (bytes_per_row + 255) & !255;

        if src_stride.is_multiple_of(256) && src_stride >= bytes_per_row {
            let end = src_offset.saturating_add((src_stride * copy_height) as usize);
            if end > src.len() {
                error!(
                    "[RENDERER] {} {} plane truncated upload (expected {} bytes, have {})",
                    renderer_name,
                    plane_name,
                    end,
                    src.len()
                );
                return false;
            }

            queue.write_texture(
                wgpu::ImageCopyTexture {
                    texture,
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                &src[src_offset..end],
                wgpu::ImageDataLayout {
                    offset: 0,
                    bytes_per_row: Some(src_stride),
                    rows_per_image: Some(copy_height),
                },
                wgpu::Extent3d {
                    width: copy_width,
                    height: copy_height,
                    depth_or_array_layers: 1,
                },
            );
            return true;
        }

        stride_temp_buffer.clear();
        let required = (aligned_bytes_per_row * copy_height) as usize;
        if stride_temp_buffer.capacity() < required {
            stride_temp_buffer.reserve(required - stride_temp_buffer.capacity());
        }

        for row in 0..copy_height {
            let start = src_offset + (row * src_stride) as usize;
            let end = start + bytes_per_row as usize;
            if end > src.len() {
                error!(
                    "[RENDERER] {} {} plane truncated row (expected {} bytes, have {})",
                    renderer_name,
                    plane_name,
                    end,
                    src.len()
                );
                return false;
            }
            stride_temp_buffer.extend_from_slice(&src[start..end]);
            let pad = aligned_bytes_per_row - bytes_per_row;
            if pad > 0 {
                stride_temp_buffer.extend(std::iter::repeat_n(0u8, pad as usize));
            }
        }

        queue.write_texture(
            wgpu::ImageCopyTexture {
                texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            stride_temp_buffer,
            wgpu::ImageDataLayout {
                offset: 0,
                bytes_per_row: Some(aligned_bytes_per_row),
                rows_per_image: Some(copy_height),
            },
            wgpu::Extent3d {
                width: copy_width,
                height: copy_height,
                depth_or_array_layers: 1,
            },
        );

        true
    }

    /// Upload an NV12 frame: write Y and UV planes to staging textures, then
    /// run the NV12→RGBA conversion render pass into the output texture.
    fn upload_frame_nv12(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    ) {
        let (uv_width, uv_height) = chroma_plane_extent(width, height);

        if self.nv12_staging_size != Some((width, height)) {
            self.nv12_y_view = None;
            self.nv12_uv_view = None;

            let y_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("NV12 Y Plane"),
                size: wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            let uv_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("NV12 UV Plane"),
                size: wgpu::Extent3d {
                    width: uv_width,
                    height: uv_height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rg8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });

            self.nv12_y_view = Some(y_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.nv12_uv_view = Some(uv_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.nv12_y_texture = Some(y_tex);
            self.nv12_uv_texture = Some(uv_tex);
            self.nv12_staging_size = Some((width, height));
        }

        let y_tex = self.nv12_y_texture.as_ref().unwrap();
        let uv_tex = self.nv12_uv_texture.as_ref().unwrap();

        {
            let map = match frame.buffer.map_readable() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to map NV12 video buffer: {}", e);
                    return;
                }
            };
            let src = map.as_slice();

            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                y_tex,
                src,
                0,
                y_stride,
                width,
                height,
                1,
                "NV12 Y",
            ) {
                return;
            }
            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                uv_tex,
                src,
                uv_offset as usize,
                uv_stride,
                uv_width,
                uv_height,
                2,
                "NV12 UV",
            ) {
                return;
            }
        }

        let y_view = self.nv12_y_view.as_ref().unwrap();
        let uv_view = self.nv12_uv_view.as_ref().unwrap();

        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("NV12 Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        let bind_group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("NV12 Convert Bind Group"),
                layout: &self.ctx.nv12_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(uv_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            });

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("NV12 Convert Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("NV12 Convert Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &output_view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });
            pass.set_pipeline(&self.ctx.nv12_pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));
    }

    /// Upload an I420 frame: write Y/U/V planes to staging textures, then
    /// run the I420→RGBA conversion render pass into the output texture.
    #[allow(clippy::too_many_arguments)]
    fn upload_frame_i420(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_stride: u32,
        u_offset: u32,
        u_stride: u32,
        v_offset: u32,
        v_stride: u32,
    ) {
        let (chroma_width, chroma_height) = chroma_plane_extent(width, height);

        if self.i420_staging_size != Some((width, height)) {
            self.i420_y_view = None;
            self.i420_u_view = None;
            self.i420_v_view = None;

            let y_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("I420 Y Plane"),
                size: wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            let u_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("I420 U Plane"),
                size: wgpu::Extent3d {
                    width: chroma_width,
                    height: chroma_height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            let v_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("I420 V Plane"),
                size: wgpu::Extent3d {
                    width: chroma_width,
                    height: chroma_height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });

            self.i420_y_view = Some(y_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.i420_u_view = Some(u_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.i420_v_view = Some(v_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.i420_y_texture = Some(y_tex);
            self.i420_u_texture = Some(u_tex);
            self.i420_v_texture = Some(v_tex);
            self.i420_staging_size = Some((width, height));
        }

        let y_tex = self.i420_y_texture.as_ref().unwrap();
        let u_tex = self.i420_u_texture.as_ref().unwrap();
        let v_tex = self.i420_v_texture.as_ref().unwrap();

        {
            let map = match frame.buffer.map_readable() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to map I420 video buffer: {}", e);
                    return;
                }
            };
            let src = map.as_slice();

            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                y_tex,
                src,
                0,
                y_stride,
                width,
                height,
                1,
                "I420 Y",
            ) {
                return;
            }
            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                u_tex,
                src,
                u_offset as usize,
                u_stride,
                chroma_width,
                chroma_height,
                1,
                "I420 U",
            ) {
                return;
            }
            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                v_tex,
                src,
                v_offset as usize,
                v_stride,
                chroma_width,
                chroma_height,
                1,
                "I420 V",
            ) {
                return;
            }
        }

        let y_view = self.i420_y_view.as_ref().unwrap();
        let u_view = self.i420_u_view.as_ref().unwrap();
        let v_view = self.i420_v_view.as_ref().unwrap();

        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("I420 Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        let bind_group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("I420 Convert Bind Group"),
                layout: &self.ctx.i420_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(u_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::TextureView(v_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            });

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("I420 Convert Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("I420 Convert Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &output_view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });
            pass.set_pipeline(&self.ctx.i420_pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));
    }

    /// Upload an RGBA frame directly to the output texture (legacy fallback path).
    fn upload_frame_rgba(
        &mut self,
        frame: &crate::video::VideoFrame,
        texture: &wgpu::Texture,
        width: u32,
        height: u32,
    ) {
        let src_stride = frame.stride;
        let expected_stride = width * 4;

        if src_stride.is_multiple_of(256) && src_stride >= expected_stride {
            let map = match frame.buffer.map_readable() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to map RGBA video buffer: {}", e);
                    return;
                }
            };
            let src_data = map.as_slice();
            self.ctx.queue.write_texture(
                wgpu::ImageCopyTexture {
                    texture,
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                src_data,
                wgpu::ImageDataLayout {
                    offset: 0,
                    bytes_per_row: Some(src_stride),
                    rows_per_image: Some(height),
                },
                wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
            );
        } else {
            let align_mask = 255u32;
            let aligned_stride = (expected_stride + align_mask) & !align_mask;
            let required_size = (aligned_stride * height) as usize;

            self.stride_temp_buffer.clear();
            if self.stride_temp_buffer.capacity() < required_size {
                self.stride_temp_buffer
                    .reserve(required_size - self.stride_temp_buffer.capacity());
            }

            {
                let map = match frame.buffer.map_readable() {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Failed to map RGBA video buffer: {}", e);
                        return;
                    }
                };
                let src_data = map.as_slice();
                for row in 0..height {
                    let src_start = (row * src_stride) as usize;
                    let src_end = src_start + expected_stride as usize;
                    if src_end <= src_data.len() {
                        self.stride_temp_buffer
                            .extend_from_slice(&src_data[src_start..src_end]);
                    } else {
                        break;
                    }
                    let padding = aligned_stride - expected_stride;
                    if padding > 0 {
                        self.stride_temp_buffer
                            .extend(std::iter::repeat_n(0u8, padding as usize));
                    }
                }
            }

            self.ctx.queue.write_texture(
                wgpu::ImageCopyTexture {
                    texture,
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                &self.stride_temp_buffer,
                wgpu::ImageDataLayout {
                    offset: 0,
                    bytes_per_row: Some(aligned_stride),
                    rows_per_image: Some(height),
                },
                wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
            );
        }
    }

    /// Import DMA-BUF file descriptors as Vulkan textures and run NV12→RGBA
    /// conversion. Returns false if import fails (caller should fallback).
    #[allow(clippy::too_many_arguments)]
    fn upload_frame_dmabuf_nv12(
        &mut self,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_fd: std::os::unix::io::RawFd,
        y_stride: u32,
        y_offset: u32,
        uv_fd: std::os::unix::io::RawFd,
        uv_stride: u32,
        uv_offset: u32,
    ) -> bool {
        let (uv_width, uv_height) = chroma_plane_extent(width, height);

        // Import Y plane
        let y_tex = match import_dmabuf_as_texture(
            &self.ctx.device,
            y_fd,
            width,
            height,
            y_stride,
            y_offset,
            wgpu::TextureFormat::R8Unorm,
            "DMA-BUF Y Plane",
        ) {
            Some(t) => t,
            None => return false,
        };

        // Import UV plane
        let uv_tex = match import_dmabuf_as_texture(
            &self.ctx.device,
            uv_fd,
            uv_width,
            uv_height,
            uv_stride,
            uv_offset,
            wgpu::TextureFormat::Rg8Unorm,
            "DMA-BUF UV Plane",
        ) {
            Some(t) => t,
            None => return false,
        };

        let y_view = y_tex.create_view(&wgpu::TextureViewDescriptor::default());
        let uv_view = uv_tex.create_view(&wgpu::TextureViewDescriptor::default());

        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("NV12 DMA-BUF Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        let bind_group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("NV12 DMA-BUF Convert Bind Group"),
                layout: &self.ctx.nv12_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(&y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(&uv_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            });

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("NV12 DMA-BUF Convert Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("NV12 DMA-BUF Convert Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &output_view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });
            pass.set_pipeline(&self.ctx.nv12_pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));

        // y_tex and uv_tex are dropped here; the drop callbacks free the Vulkan resources
        true
    }

    /// CUDA zero-copy NV12: map the GStreamer CUDAMemory buffer, GPU-copy to
    /// Vulkan-exported textures via CUDA-Vulkan interop, then run the NV12→RGBA
    /// CUDA zero-copy NV12 upload: allocate CUDA-exportable memory, import into
    /// Vulkan, GPU-copy decoded frame, then run NV12→RGBA conversion shader.
    /// Returns false if any step fails (caller falls back to CPU upload).
    #[allow(clippy::too_many_arguments)]
    fn upload_frame_cuda_nv12(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    ) -> bool {
        // Check shared CUDA interop (lives in WgpuContext, shared across renderers)
        if self
            .ctx
            .cuda_interop_failed
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return false;
        }
        {
            let mut ci_lock = self.ctx.cuda_interop.lock();
            if ci_lock.is_none() {
                match crate::cuda_interop::CudaInterop::new() {
                    Ok(ci) => *ci_lock = Some(ci),
                    Err(e) => {
                        error!("[VIDEO] {}: {e}", self.name);
                        self.ctx
                            .cuda_interop_failed
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        return false;
                    }
                }
            }
        }

        let (uv_width, uv_height) = chroma_plane_extent(width, height);

        // (Re)create shared CUDA↔Vulkan textures when dimensions change
        let need_new = self
            .cuda_textures
            .as_ref()
            .is_none_or(|c| c.width != width || c.height != height);
        if need_new {
            let ci_guard = self.ctx.cuda_interop.lock();
            let ci = ci_guard.as_ref().unwrap();

            // Destroy old cache
            if let Some(old) = self.cuda_textures.take() {
                ci.free_exportable(old.y_cuda_alloc);
                ci.free_exportable(old.uv_cuda_alloc);
            }

            // Allocate Y plane: CUDA exports, Vulkan imports
            let (y_tex, y_cuda_alloc, y_layout) = match create_cuda_backed_texture(
                ci,
                &self.ctx.device,
                width,
                height,
                wgpu::TextureFormat::R8Unorm,
                "CUDA Y Plane",
            ) {
                Some(v) => v,
                None => {
                    error!(
                        "[VIDEO] {}: Failed to create CUDA-backed Y texture",
                        self.name
                    );
                    self.ctx
                        .cuda_interop_failed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return false;
                }
            };

            // Allocate UV plane: CUDA exports, Vulkan imports
            let (uv_tex, uv_cuda_alloc, uv_layout) = match create_cuda_backed_texture(
                ci,
                &self.ctx.device,
                uv_width,
                uv_height,
                wgpu::TextureFormat::Rg8Unorm,
                "CUDA UV Plane",
            ) {
                Some(v) => v,
                None => {
                    error!(
                        "[VIDEO] {}: Failed to create CUDA-backed UV texture",
                        self.name
                    );
                    ci.free_exportable(y_cuda_alloc);
                    self.ctx
                        .cuda_interop_failed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return false;
                }
            };

            let y_view = y_tex.create_view(&wgpu::TextureViewDescriptor::default());
            let uv_view = uv_tex.create_view(&wgpu::TextureViewDescriptor::default());

            info!(
                "[VIDEO] {}: CUDA zero-copy textures: {}x{}, Y(pitch={} offset={}) UV(pitch={} offset={})",
                self.name,
                width,
                height,
                y_layout.row_pitch,
                y_layout.offset,
                uv_layout.row_pitch,
                uv_layout.offset
            );

            self.cuda_textures = Some(CudaTextureCache {
                y_texture: y_tex,
                y_view,
                y_cuda_alloc,
                y_pitch: y_layout.row_pitch,
                y_offset: y_layout.offset,
                uv_texture: uv_tex,
                uv_view,
                uv_cuda_alloc,
                uv_pitch: uv_layout.row_pitch,
                uv_offset: uv_layout.offset,
                width,
                height,
            });
        }

        let cache = self.cuda_textures.as_ref().unwrap();

        // Map the GStreamer CUDA buffer to get the source device pointer
        let guard = match crate::cuda_interop::map_buffer_cuda(&frame.buffer) {
            Some(g) => g,
            None => return false,
        };
        let base_ptr = guard.device_ptr();

        {
            let ci_guard = self.ctx.cuda_interop.lock();
            let ci = ci_guard.as_ref().unwrap();

            // GPU-side copy: Y plane (decoded frame → CUDA exportable buffer)
            // Add Vulkan's layout.offset so data lands where the VkImage expects it
            if let Err(e) = ci.copy_2d(
                base_ptr,
                y_stride as usize,
                cache.y_cuda_alloc.dev_ptr + cache.y_offset as u64,
                cache.y_pitch,
                width as usize,
                height as usize,
            ) {
                error!("[VIDEO] {}: CUDA Y copy failed: {e}", self.name);
                return false;
            }

            // GPU-side copy: UV plane
            let uv_row_bytes = (uv_width * 2) as usize;
            if let Err(e) = ci.copy_2d(
                base_ptr + uv_offset as u64,
                uv_stride as usize,
                cache.uv_cuda_alloc.dev_ptr + cache.uv_offset as u64,
                cache.uv_pitch,
                uv_row_bytes,
                uv_height as usize,
            ) {
                error!("[VIDEO] {}: CUDA UV copy failed: {e}", self.name);
                return false;
            }

            // Synchronize CUDA to ensure writes are visible to Vulkan
            if let Err(e) = ci.synchronize() {
                error!("[VIDEO] {}: CUDA sync failed: {e}", self.name);
                return false;
            }
        }

        drop(guard);

        // Run NV12→RGBA conversion shader (same as DMA-BUF and CPU NV12 paths)
        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("NV12 CUDA Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        let bind_group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("NV12 CUDA Convert Bind Group"),
                layout: &self.ctx.nv12_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(&cache.y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(&cache.uv_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            });

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("NV12 CUDA Convert Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("NV12 CUDA Convert Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &output_view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });
            pass.set_pipeline(&self.ctx.nv12_pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        true
    }

    pub fn switch_content(&mut self) {
        let had_current = self.current_texture.is_some();
        let flattened_active_transition = self.freeze_active_transition_to_current();

        // If a previous transition left a prev texture behind, release it before preparing
        // the next swap. The current texture stays visible until replacement upload is ready.
        if !flattened_active_transition {
            if let Some(old_prev) = self.prev_texture.take() {
                if let Some((w, h)) = self.prev_texture_size.take() {
                    debug!(
                        "[TRANSITION] {}: Releasing carry-over prev_texture before new switch",
                        self.name
                    );
                    self.ctx.return_texture_to_pool(old_prev, w, h);
                }
                drop(self.prev_texture_view.take());
            }
        }

        self.content_swap_pending = true;
        self.transition_progress = 1.0;
        self.transition_start_time = None; // Will be set when content is uploaded
        self.transition_active = false; // Will be set to true when replacement content is ready
        self.transition_just_completed = false; // Reset completion flag
        self.display_timer_pending = false;
        self.display_timer_ready = false;
        self.transition_bind_group = None; // Invalidate
        self.blit_bind_group = None; // Invalidate
        self.batch_start_time = None; // Reset
        self.video_first_frame_time = if self.valid_content_type == crate::queue::ContentType::Video
        {
            Some(std::time::Instant::now())
        } else {
            None
        };
        self.needs_redraw = true;

        debug!(
            "[TRANSITION] {}: switch_content() - had_current={}, prev_texture={}, pending_swap={}, flattened_active_transition={}",
            self.name,
            had_current,
            self.prev_texture.is_some(),
            self.content_swap_pending,
            flattened_active_transition
        );
    }

    pub fn abort_transition(&mut self) {
        if self.transition_active || self.content_swap_pending || self.current_texture.is_none() {
            if self.transition_active {
                info!(
                    "[TRANSITION] {}: Aborting transition due to load failure",
                    self.name
                );
            }
            self.content_swap_pending = false;
            self.transition_active = false;
            self.transition_just_completed = false; // Reset flag
            self.display_timer_pending = false;
            self.display_timer_ready = false;
            self.transition_progress = 1.0;
            self.transition_start_time = None;
            self.needs_redraw = true;
        }
    }

    /// Clears the renderer to black (removes current and previous textures)
    ///
    /// This explicitly drops all texture resources and forces WGPU to reclaim
    /// GPU memory immediately. Useful for cleanup and preventing memory leaks.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        // Explicitly drop textures to free GPU memory
        self.current_texture = None;
        self.current_texture_view = None;
        self.prev_texture = None;
        self.prev_texture_view = None;
        self.current_texture_size = None;
        self.prev_texture_size = None;
        self.last_video_source_size = None;
        self.last_video_presentation_size = None;
        self.transition_progress = 1.0;
        self.transition_active = false;
        self.transition_just_completed = false; // Reset flag
        self.content_swap_pending = false;
        self.display_timer_pending = false;
        self.display_timer_ready = false;
        self.transition_bind_group = None; // Invalidate
        self.blit_bind_group = None; // Invalidate
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;
        self.needs_redraw = true;
        self.release_composition_texture("clear");
        self.release_video_backend_resources("clear");
        // Reclaim memory immediately - this ensures GPU resources are freed
        // rather than waiting for WGPU's automatic cleanup
        self.active_video_session_id = 0; // Invalidate current video session
        self.configured = false; // Force re-config next time
        self.ctx.device.poll(wgpu::Maintain::Poll);
    }

    #[allow(dead_code)]
    pub fn recreate_surface(&mut self, surface: wgpu::Surface<'static>) {
        self.surface = surface;
        self.configured = false;
        self.needs_redraw = true;
    }

    fn begin_content_swap(&mut self) {
        if !self.content_swap_pending {
            return;
        }

        if let Some(curr) = self.current_texture.take() {
            self.prev_texture_view = self.current_texture_view.take();
            self.prev_texture = Some(curr);
            self.prev_aspect = self.current_aspect;
            self.prev_texture_size = self.current_texture_size.take();
        } else {
            self.current_texture_view = None;
            self.current_texture_size = None;
        }

        self.content_swap_pending = false;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;
    }

    fn freeze_active_transition_to_current(&mut self) -> bool {
        if !self.transition_active {
            return false;
        }
        let Some(composition_texture) = self.composition_texture.take() else {
            return false;
        };
        let Some(composition_view) = self.composition_texture_view.take() else {
            self.composition_texture = Some(composition_texture);
            return false;
        };

        debug!(
            "[TRANSITION] {}: Flattening active transition into a stable snapshot before new switch",
            self.name
        );

        if let Some(curr) = self.current_texture.take() {
            if let Some((w, h)) = self.current_texture_size.take() {
                self.ctx.return_texture_to_pool(curr, w, h);
            }
        }

        self.release_prev_texture("flatten active transition");
        self.current_texture = Some(composition_texture);
        self.current_texture_view = Some(composition_view);
        self.current_texture_size = Some((self.config.width.max(1), self.config.height.max(1)));
        self.current_aspect = self.config.width.max(1) as f32 / self.config.height.max(1) as f32;
        self.transition_active = false;
        self.transition_just_completed = false;
        self.transition_progress = 1.0;
        self.transition_start_time = None;
        self.transition_stats = None;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;
        true
    }

    fn release_cuda_cache(&mut self) {
        if let Some(cuda_cache) = self.cuda_textures.take() {
            if let Some(interop) = self.ctx.cuda_interop.lock().as_ref() {
                interop.free_exportable(cuda_cache.y_cuda_alloc);
                interop.free_exportable(cuda_cache.uv_cuda_alloc);
            } else {
                warn!(
                    "[VIDEO] {}: Dropping CUDA texture cache without CUDA interop; exportable allocations will leak until process exit",
                    self.name
                );
            }
        }
    }

    fn arm_display_timer_on_present(&mut self) {
        self.display_timer_pending = true;
        self.display_timer_ready = false;
    }

    pub fn take_display_timer_ready(&mut self) -> bool {
        std::mem::take(&mut self.display_timer_ready)
    }

    fn release_nv12_staging(&mut self, reason: &str) {
        if self.nv12_staging_size.is_some() {
            debug!(
                "[VIDEO] {}: Releasing NV12 staging textures ({})",
                self.name, reason
            );
        }
        self.nv12_y_texture = None;
        self.nv12_uv_texture = None;
        self.nv12_y_view = None;
        self.nv12_uv_view = None;
        self.nv12_staging_size = None;
    }

    fn release_i420_staging(&mut self, reason: &str) {
        if self.i420_staging_size.is_some() {
            debug!(
                "[VIDEO] {}: Releasing I420 staging textures ({})",
                self.name, reason
            );
        }
        self.i420_y_texture = None;
        self.i420_u_texture = None;
        self.i420_v_texture = None;
        self.i420_y_view = None;
        self.i420_u_view = None;
        self.i420_v_view = None;
        self.i420_staging_size = None;
    }

    fn release_video_backend_resources(&mut self, reason: &str) {
        self.release_nv12_staging(reason);
        self.release_i420_staging(reason);
        self.release_cuda_cache();
    }

    fn release_prev_texture(&mut self, reason: &str) {
        if let Some(prev_tex) = self.prev_texture.take() {
            debug!(
                "[TRANSITION] {}: Releasing stale prev_texture ({})",
                self.name, reason
            );
            if let Some((w, h)) = self.prev_texture_size.take() {
                self.ctx.return_texture_to_pool(prev_tex, w, h);
            }
        }
        self.prev_texture_view = None;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
    }

    fn release_composition_texture(&mut self, reason: &str) {
        if self.composition_texture.is_some() || self.composition_texture_view.is_some() {
            debug!(
                "[TRANSITION] {}: Releasing composition texture ({})",
                self.name, reason
            );
        }
        self.composition_texture = None;
        self.composition_texture_view = None;
        self.transition_bind_group = None;
        if self.blit_source_is_composition {
            self.blit_bind_group = None;
            self.blit_source_is_composition = false;
        }
    }

    fn update_transition_bind_group(&mut self) {
        // Skip recreation if bind group already exists — it's invalidated to None
        // whenever textures change (upload_image_data, upload_frame, switch_content)
        if self.transition_bind_group.is_some() {
            return;
        }

        // Only recreate if both texture views are present
        let prev_view = match self.prev_texture_view.as_ref() {
            Some(v) => v,
            None => {
                return;
            }
        };
        let current_view = match self.current_texture_view.as_ref() {
            Some(v) => v,
            None => {
                return;
            }
        };

        self.transition_bind_group = Some(self.ctx.device.create_bind_group(
            &wgpu::BindGroupDescriptor {
                label: Some("Transition Bind Group (Cached)"),
                layout: &self.ctx.transition_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: self.uniform_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(prev_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::TextureView(current_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            },
        ));
    }
}

impl Drop for Renderer {
    fn drop(&mut self) {
        // Abort background shader precompilation task to prevent resource leaks
        if let Some(handle) = self.shader_precompile_handle.take() {
            handle.abort();
        }

        // Clean up CUDA exportable allocations (Audit Point 1 in renderer.rs)
        if let Some(cuda_cache) = self.cuda_textures.take() {
            if let Some(interop) = self.ctx.cuda_interop.lock().as_ref() {
                interop.free_exportable(cuda_cache.y_cuda_alloc);
                interop.free_exportable(cuda_cache.uv_cuda_alloc);
            } else {
                warn!(
                    "[VIDEO] {}: Renderer dropped without CUDA interop; exportable allocations will leak until process exit",
                    self.name
                );
            }
        }
        self.nv12_y_texture = None;
        self.nv12_uv_texture = None;
        self.nv12_y_view = None;
        self.nv12_uv_view = None;
        self.nv12_staging_size = None;
        self.i420_y_texture = None;
        self.i420_u_texture = None;
        self.i420_v_texture = None;
        self.i420_y_view = None;
        self.i420_u_view = None;
        self.i420_v_view = None;
        self.i420_staging_size = None;
    }
}

/// Import a DMA-BUF file descriptor as a wgpu::Texture via the Vulkan backend.
///
/// Returns `None` if the current backend is not Vulkan or if any Vulkan call fails.
/// The caller should fall back to a CPU upload path in that case.
fn import_dmabuf_as_texture(
    device: &wgpu::Device,
    fd: std::os::unix::io::RawFd,
    width: u32,
    height: u32,
    stride: u32,
    offset: u32,
    format: wgpu::TextureFormat,
    label: &str,
) -> Option<wgpu::Texture> {
    use ash::vk;

    // Duplicate the fd so GStreamer can still manage its own copy
    let owned_fd = unsafe { libc::dup(fd) };
    if owned_fd < 0 {
        error!("[DMABUF] Failed to dup fd {}", fd);
        return None;
    }

    let vk_format = match wgpu_format_to_vk(format) {
        Some(f) => f,
        None => {
            unsafe {
                libc::close(owned_fd);
            }
            return None;
        }
    };

    // Validation (P-21): Check if offset + (height * stride) fits in the buffer
    let buf_size = unsafe { libc::lseek(owned_fd, 0, libc::SEEK_END) };
    if buf_size < 0 {
        error!("[DMABUF] Failed to lseek fd {owned_fd}");
        unsafe {
            libc::close(owned_fd);
        }
        return None;
    }
    unsafe {
        libc::lseek(owned_fd, 0, libc::SEEK_SET);
    }

    let req_size = offset as i64 + (height as i64 * stride as i64);
    if req_size > buf_size {
        error!(
            "[DMABUF] {label} validation failed: req_size {} > buf_size {} (offset={}, h={}, stride={})",
            req_size, buf_size, offset, height, stride
        );
        unsafe {
            libc::close(owned_fd);
        }
        return None;
    }

    // Access the underlying Vulkan device through wgpu-hal's callback API
    // and perform all Vulkan operations inside.
    let hal_texture: Option<wgpu_hal::vulkan::Texture> = unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device_opt| {
            let Some(hal_device) = hal_device_opt else {
                libc::close(owned_fd);
                return None;
            };
            let raw_device = hal_device.raw_device();

            // 1. Create VkImage with external memory support
            let mut external_memory_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);

            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D {
                    width,
                    height,
                    depth: 1,
                })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::LINEAR)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_memory_info);

            let vk_image = match raw_device.create_image(&image_info, None) {
                Ok(img) => img,
                Err(e) => {
                    error!("[DMABUF] Failed to create VkImage: {:?}", e);
                    libc::close(owned_fd);
                    return None;
                }
            };

            // 2. Query memory requirements
            let mem_reqs = raw_device.get_image_memory_requirements(vk_image);

            // 3. Query DMA-BUF fd memory properties via VK_KHR_external_memory_fd
            let ash_instance = hal_device.shared_instance().raw_instance();
            let ext_mem_fd = ash::khr::external_memory_fd::Device::new(ash_instance, raw_device);

            let mut fd_mem_props = vk::MemoryFdPropertiesKHR::default();
            if let Err(e) = ext_mem_fd.get_memory_fd_properties(
                vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT,
                owned_fd,
                &mut fd_mem_props,
            ) {
                error!("[DMABUF] Failed to query fd memory properties: {:?}", e);
                raw_device.destroy_image(vk_image, None);
                libc::close(owned_fd);
                return None;
            }

            // 4. Find a suitable memory type
            let type_bits = mem_reqs.memory_type_bits & fd_mem_props.memory_type_bits;
            let memory_type_index = match find_memory_type(type_bits) {
                Some(idx) => idx,
                None => {
                    error!(
                        "[DMABUF] No suitable memory type (reqs={:#x}, fd_props={:#x})",
                        mem_reqs.memory_type_bits, fd_mem_props.memory_type_bits
                    );
                    raw_device.destroy_image(vk_image, None);
                    libc::close(owned_fd);
                    return None;
                }
            };

            // 5. Import the DMA-BUF fd as Vulkan device memory
            let mut import_info = vk::ImportMemoryFdInfoKHR::default()
                .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT)
                .fd(owned_fd);

            let mut dedicated_info = vk::MemoryDedicatedAllocateInfo::default().image(vk_image);

            let alloc_info = vk::MemoryAllocateInfo::default()
                .allocation_size(mem_reqs.size)
                .memory_type_index(memory_type_index)
                .push_next(&mut import_info)
                .push_next(&mut dedicated_info);

            let vk_memory = match raw_device.allocate_memory(&alloc_info, None) {
                Ok(mem) => mem,
                Err(e) => {
                    error!("[DMABUF] vkAllocateMemory failed: {:?}", e);
                    raw_device.destroy_image(vk_image, None);
                    libc::close(owned_fd);
                    return None;
                }
            };
            // Success: Vulkan now owns the FD.
            // DO NOT manually close it here (it causes intermittent double-close crashes).
            // The driver/Vulkan implementation is responsible for closing the imported FD.

            // 6. Bind imported memory to the image
            if let Err(e) =
                raw_device.bind_image_memory(vk_image, vk_memory, offset as vk::DeviceSize)
            {
                error!("[DMABUF] vkBindImageMemory failed: {:?}", e);
                raw_device.free_memory(vk_memory, None);
                raw_device.destroy_image(vk_image, None);
                return None;
            }

            // 7. Wrap as wgpu-hal Texture with a drop callback for cleanup
            let drop_device = raw_device.clone();
            let drop_image = vk_image;
            let drop_memory = vk_memory;

            let hal_desc = wgpu_hal::TextureDescriptor {
                label: Some(label),
                size: wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format,
                usage: wgpu_hal::TextureUses::RESOURCE,
                memory_flags: wgpu_hal::MemoryFlags::empty(),
                view_formats: vec![],
            };

            let drop_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
                drop_device.destroy_image(drop_image, None);
                drop_device.free_memory(drop_memory, None);
            });

            Some(wgpu_hal::vulkan::Device::texture_from_raw(
                vk_image,
                &hal_desc,
                Some(drop_callback),
            ))
        })?
    };

    let hal_texture = hal_texture?;

    // 8. Wrap the HAL texture as a wgpu::Texture
    let wgpu_desc = wgpu::TextureDescriptor {
        label: Some(label),
        size: wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format,
        usage: wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[],
    };

    Some(unsafe {
        device.create_texture_from_hal::<wgpu_hal::vulkan::Api>(hal_texture, &wgpu_desc)
    })
}

/// Convert wgpu TextureFormat to Vulkan format.
fn wgpu_format_to_vk(format: wgpu::TextureFormat) -> Option<ash::vk::Format> {
    match format {
        wgpu::TextureFormat::R8Unorm => Some(ash::vk::Format::R8_UNORM),
        wgpu::TextureFormat::Rg8Unorm => Some(ash::vk::Format::R8G8_UNORM),
        wgpu::TextureFormat::Rgba8UnormSrgb => Some(ash::vk::Format::R8G8B8A8_SRGB),
        wgpu::TextureFormat::Rgba8Unorm => Some(ash::vk::Format::R8G8B8A8_UNORM),
        _ => {
            error!(
                "[DMABUF] Unsupported texture format for DMA-BUF import: {:?}",
                format
            );
            None
        }
    }
}

/// Find the lowest-index set bit in a memory type bitmask.
fn find_memory_type(type_bits: u32) -> Option<u32> {
    (0..32).find(|&i| (type_bits & (1 << i)) != 0)
}

/// Allocate CUDA-exportable memory, import the fd into Vulkan as a LINEAR
/// tiled image, and return the wgpu Texture + CUDA allocation + row pitch.
/// This reverses the usual Vulkan-export→CUDA-import flow: CUDA owns the
/// memory and Vulkan imports it, avoiding the need for vkGetMemoryFdKHR.
/// Returned layout info from Vulkan's LINEAR tiling.
struct CudaTexLayout {
    row_pitch: usize,
    offset: usize,
}

fn create_cuda_backed_texture(
    ci: &crate::cuda_interop::CudaInterop,
    device: &wgpu::Device,
    width: u32,
    height: u32,
    format: wgpu::TextureFormat,
    label: &str,
) -> Option<(
    wgpu::Texture,
    crate::cuda_interop::ExportableCudaAllocation,
    CudaTexLayout,
)> {
    use ash::vk;

    let vk_format = wgpu_format_to_vk(format)?;

    // Step 1: probe Vulkan for memory requirements (create temp image to query)
    let (mem_size, mem_type_bits, tex_layout) = unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device_opt| {
            let hal_device = hal_device_opt?;
            let raw_device = hal_device.raw_device();

            let mut external_memory_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);

            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D { width, height, depth: 1 })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::LINEAR)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_memory_info);

            let probe_image = raw_device.create_image(&image_info, None).ok()?;
            let mem_reqs = raw_device.get_image_memory_requirements(probe_image);
            let subresource = vk::ImageSubresource {
                aspect_mask: vk::ImageAspectFlags::COLOR,
                mip_level: 0,
                array_layer: 0,
            };
            let layout = raw_device.get_image_subresource_layout(probe_image, subresource);
            raw_device.destroy_image(probe_image, None);

            info!(
                "[CUDA-VK] {label} probe: mem_reqs.size={}, row_pitch={}, offset={}, layout.size={}",
                mem_reqs.size, layout.row_pitch, layout.offset, layout.size
            );

            Some((
                mem_reqs.size as usize,
                mem_reqs.memory_type_bits,
                CudaTexLayout {
                    row_pitch: layout.row_pitch as usize,
                    offset: layout.offset as usize,
                },
            ))
        })?
    }?;

    // Step 2: CUDA allocates exportable memory (rounded up to granularity)
    let (cuda_alloc, fd) = match ci.allocate_exportable(mem_size) {
        Ok(v) => v,
        Err(e) => {
            error!("[CUDA-VK] CUDA allocate_exportable failed: {e}");
            return None;
        }
    };

    // The Vulkan spec requires allocation_size to match the export size for OPAQUE_FD
    let cuda_export_size = cuda_alloc.alloc_size() as u64;

    // Step 3: import the CUDA fd into Vulkan and bind to a new VkImage
    let hal_texture: Option<wgpu_hal::vulkan::Texture> = unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device_opt| {
            let Some(hal_device) = hal_device_opt else {
                libc::close(fd);
                return None;
            };
            let raw_device = hal_device.raw_device();

            let mut external_memory_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);

            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D { width, height, depth: 1 })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::LINEAR)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_memory_info);

            let vk_image = match raw_device.create_image(&image_info, None) {
                Ok(img) => img,
                Err(e) => {
                    error!("[CUDA-VK] Failed to create VkImage: {:?}", e);
                    libc::close(fd);
                    return None;
                }
            };

            let mem_reqs = raw_device.get_image_memory_requirements(vk_image);
            let memory_type_index = match find_memory_type(mem_reqs.memory_type_bits & mem_type_bits) {
                Some(idx) => idx,
                None => {
                    error!("[CUDA-VK] No compatible memory type (image={:#x})",
                           mem_reqs.memory_type_bits);
                    raw_device.destroy_image(vk_image, None);
                    libc::close(fd);
                    return None;
                }
            };

            let mut import_info = vk::ImportMemoryFdInfoKHR::default()
                .handle_type(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD)
                .fd(fd);

            let mut dedicated_info = vk::MemoryDedicatedAllocateInfo::default()
                .image(vk_image);

            // Use the CUDA export size (rounded to granularity), not mem_reqs.size
            let alloc_info = vk::MemoryAllocateInfo::default()
                .allocation_size(cuda_export_size)
                .memory_type_index(memory_type_index)
                .push_next(&mut import_info)
                .push_next(&mut dedicated_info);

            let vk_memory = match raw_device.allocate_memory(&alloc_info, None) {
                Ok(mem) => mem,
                Err(e) => {
                    error!("[CUDA-VK] vkAllocateMemory (import fd={fd}, size={cuda_export_size}) failed: {:?}", e);
                    raw_device.destroy_image(vk_image, None);
                    libc::close(fd);
                    return None;
                }
            };

            // Success: Vulkan now owns the FD.
            // DO NOT manually close it here (it causes intermittent double-close crashes).
            // The driver/Vulkan implementation is responsible for closing the imported FD.

            if let Err(e) = raw_device.bind_image_memory(vk_image, vk_memory, 0) {
                error!("[CUDA-VK] vkBindImageMemory failed: {:?}", e);
                raw_device.free_memory(vk_memory, None);
                raw_device.destroy_image(vk_image, None);
                return None;
            }

            let drop_device = raw_device.clone();
            let drop_image = vk_image;
            let drop_memory = vk_memory;

            let hal_desc = wgpu_hal::TextureDescriptor {
                label: Some(label),
                size: wgpu::Extent3d { width, height, depth_or_array_layers: 1 },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format,
                usage: wgpu_hal::TextureUses::RESOURCE,
                memory_flags: wgpu_hal::MemoryFlags::empty(),
                view_formats: vec![],
            };

            let drop_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
                drop_device.destroy_image(drop_image, None);
                drop_device.free_memory(drop_memory, None);
            });

            Some(wgpu_hal::vulkan::Device::texture_from_raw(
                vk_image,
                &hal_desc,
                Some(drop_callback),
            ))
        })?
    };

    let hal_texture = match hal_texture {
        Some(t) => t,
        None => {
            ci.free_exportable(cuda_alloc);
            return None;
        }
    };

    let wgpu_desc = wgpu::TextureDescriptor {
        label: Some(label),
        size: wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format,
        usage: wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[],
    };

    let texture =
        unsafe { device.create_texture_from_hal::<wgpu_hal::vulkan::Api>(hal_texture, &wgpu_desc) };

    Some((texture, cuda_alloc, tex_layout))
}
