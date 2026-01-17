use wgpu::{Instance, Surface, Adapter, Device, Queue, SurfaceConfiguration};
use tracing::{info, debug, error, warn};
use raw_window_handle::{HasWindowHandle, HasDisplayHandle};
use std::sync::Arc;
use bytemuck::{Pod, Zeroable};
use std::collections::HashMap;
use crate::shaders::Transition;
use wayland_client::QueueHandle;
use smithay_client_toolkit::shell::{wlr_layer::LayerSurface, WaylandSurface};

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
        surface: &'a LayerSurface,
        qh: &'a QueueHandle<crate::wayland::WaylandBackend>,
    },
    X11,
}

// Texture pool entry for LRU cache
pub struct TexturePoolEntry {
    texture: wgpu::Texture,
    last_used: std::time::Instant,
}

// LRU cache for transition pipelines
pub struct PipelineLRU {
    pipelines: HashMap<String, Arc<wgpu::RenderPipeline>>,
    access_order: std::collections::VecDeque<String>, // Most recently used at back
    max_size: usize,
}

impl PipelineLRU {
    fn new(max_size: usize) -> Self {
        Self {
            pipelines: HashMap::new(),
            access_order: std::collections::VecDeque::new(),
            max_size,
        }
    }
    
    fn get(&mut self, key: &str) -> Option<Arc<wgpu::RenderPipeline>> {
        if let Some(pipeline) = self.pipelines.get(key).cloned() {
            // Move to back (most recently used)
            self.access_order.retain(|k| k != key);
            self.access_order.push_back(key.to_string());
            Some(pipeline)
        } else {
            None
        }
    }
    
    fn insert(&mut self, key: String, pipeline: Arc<wgpu::RenderPipeline>) {
        // Remove if already exists
        if self.pipelines.contains_key(&key) {
            self.access_order.retain(|k| k != &key);
        } else {
            // Evict least recently used if at capacity
            while self.pipelines.len() >= self.max_size {
                if let Some(lru_key) = self.access_order.pop_front() {
                    self.pipelines.remove(&lru_key);
                } else {
                    break;
                }
            }
        }
        self.pipelines.insert(key.clone(), pipeline);
        self.access_order.push_back(key);
    }
    
    pub fn len(&self) -> usize {
        self.pipelines.len()
    }
    
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
    pub mipmap_pipelines: parking_lot::Mutex<HashMap<wgpu::TextureFormat, Arc<wgpu::RenderPipeline>>>,
    pub blit_bind_group_layout: wgpu::BindGroupLayout,
    pub transition_bind_group_layout: wgpu::BindGroupLayout,
    pub mipmap_bind_group_layout: wgpu::BindGroupLayout,
    // Texture pool: (width, height) -> Vec of available textures
    pub texture_pool: parking_lot::Mutex<HashMap<(u32, u32), Vec<TexturePoolEntry>>>,
}

const MAX_PIPELINE_CACHE_SIZE: usize = 50;

impl WgpuContext {
    pub async fn with_surface(window: Arc<impl HasWindowHandle + HasDisplayHandle + Sync + Send + 'static>) -> anyhow::Result<(Arc<Self>, Surface<'static>)> {
        let instance = Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });
        let compatible_surface = instance.create_surface(window)?;
        
        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None, // Removing this often fixes "Queue Family" issues on Nvidia Wayland
                force_fallback_adapter: false,
            })
            .await
            .ok_or_else(|| anyhow::anyhow!("Failed to find a suitable GPU adapter"))?;

        info!("WGPU picked adapter: {:?} with backend: {:?}", adapter.get_info().name, adapter.get_info().backend);

        let (device, queue) = adapter
            .request_device(
                &wgpu::DeviceDescriptor {
                    label: Some("Kaleidux Shared Device"),
                    required_features: wgpu::Features::empty(),
                    required_limits: adapter.limits(),
                    memory_hints: wgpu::MemoryHints::default(),
                },
                None,
            )
            .await?;

        // --- Shared Bind Group Layouts ---
        let transition_bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
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

        let blit_bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
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

        let mipmap_bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
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

        Ok((
            Arc::new(Self {
                instance,
                adapter,
                device,
                queue,
                transition_pipelines: parking_lot::Mutex::new(PipelineLRU::new(MAX_PIPELINE_CACHE_SIZE)),
                blit_pipelines: parking_lot::Mutex::new(HashMap::new()),
                mipmap_pipelines: parking_lot::Mutex::new(HashMap::new()),
                blit_bind_group_layout,
                transition_bind_group_layout,
                mipmap_bind_group_layout,
                texture_pool: parking_lot::Mutex::new(HashMap::new()),
            }),
            compatible_surface
        ))
    }

    pub fn get_blit_pipeline(&self, format: wgpu::TextureFormat) -> Arc<wgpu::RenderPipeline> {
        if let Some(pipe) = self.blit_pipelines.lock().get(&format) {
            return pipe.clone();
        }

        debug!("[RENDER] Compiling blit pipeline for format: {:?}", format);
        
        let blit_shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Quad Shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/quad.wgsl").into()),
        });

        let blit_pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Blit Pipeline Layout"),
            bind_group_layouts: &[&self.blit_bind_group_layout],
            push_constant_ranges: &[],
        });

        let pipeline = self.device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
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
            cache: None,
        });

        let pipeline_arc = Arc::new(pipeline);

        self.blit_pipelines.lock().insert(format, pipeline_arc.clone());
        pipeline_arc
    }

    pub fn get_mipmap_pipeline(&self, format: wgpu::TextureFormat) -> Arc<wgpu::RenderPipeline> {
        if let Some(pipe) = self.mipmap_pipelines.lock().get(&format) {
            return pipe.clone();
        }

        debug!("[RENDER] Compiling mipmap pipeline for format: {:?}", format);
        
        // Load mipmap.wgsl
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Mipmap Shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/mipmap.wgsl").into()),
        });

        let layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Mipmap Pipeline Layout"),
            bind_group_layouts: &[&self.mipmap_bind_group_layout],
            push_constant_ranges: &[],
        });

        let pipeline = self.device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
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
            cache: None,
        });

        let pipeline_arc = Arc::new(pipeline);
        self.mipmap_pipelines.lock().insert(format, pipeline_arc.clone());
        pipeline_arc
    }
    
    /// Get a texture from the pool or create a new one
    pub fn get_texture_from_pool(&self, width: u32, height: u32, usage: wgpu::TextureUsages, metrics: Option<&crate::metrics::PerformanceMetrics>) -> wgpu::Texture {
        let mut pool = self.texture_pool.lock();
        let key = (width, height);
        
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
            mip_level_count: 1,
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
        let key = (width, height);
        
        // Limit pool size per resolution to prevent unbounded growth
        let entries = pool.entry(key).or_insert_with(Vec::new);
        if entries.len() < 3 {
            entries.push(TexturePoolEntry {
                texture,
                last_used: std::time::Instant::now(),
            });
        }
        // If pool is full, texture is dropped (freed by WGPU)
    }
    
    /// Clean up old textures from pool
    pub fn cleanup_texture_pool(&self) {
        let mut pool = self.texture_pool.lock();
        let now = std::time::Instant::now();
        
        for entries in pool.values_mut() {
            entries.retain(|e| now.duration_since(e.last_used).as_secs() < 10);
        }
        
        // Remove empty entries
        pool.retain(|_, entries| !entries.is_empty());
    }
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
    pub active_video_session_id: u64,
    pub active_batch_id: Option<u64>,
    pub batch_start_time: Option<std::time::Instant>, // Anchor for shared batch transitions
    
    // Metrics tracking
    metrics: Option<Arc<crate::metrics::PerformanceMetrics>>,
    video_first_frame_time: Option<std::time::Instant>, // Track when video session starts
}

impl Renderer {
    pub fn new<W>(name: String, ctx: Arc<WgpuContext>, window: Arc<W>, first_surface: Option<Surface<'static>>, metrics: Option<Arc<crate::metrics::PerformanceMetrics>>) -> anyhow::Result<Self> 
    where 
        W: HasWindowHandle + HasDisplayHandle + Sync + Send + 'static
    {
        // Reuse the first surface if provided to avoid protocol errors (multiple roles on wl_surface)
        let surface = if let Some(s) = first_surface {
            s
        } else {
            ctx.instance.create_surface(window)?
        };
        
        let caps = surface.get_capabilities(&ctx.adapter);
        let format = caps.formats.get(0).cloned().unwrap_or(wgpu::TextureFormat::Rgba8UnormSrgb);
        let alpha_mode = caps.alpha_modes.get(0).cloned().unwrap_or(wgpu::CompositeAlphaMode::Auto);
        // Prefer Mailbox for lower latency, fallback to Immediate, then Fifo
        let present_mode = caps.present_modes.iter()
            .find(|&&m| m == wgpu::PresentMode::Mailbox)
            .copied()
            .unwrap_or_else(|| {
                caps.present_modes.iter()
                    .find(|&&m| m == wgpu::PresentMode::Immediate)
                    .copied()
                    .unwrap_or(wgpu::PresentMode::Fifo)
            });

        if caps.formats.is_empty() {
             info!("Surface {} created. Capabilities not yet available (transient).", name);
        }

        let config = SurfaceConfiguration {
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            format,
            width: 1, // Will be resized
            height: 1,
            present_mode,
            alpha_mode,
            view_formats: vec![],
            desired_maximum_frame_latency: 2,
        };

        // Clone name for background task before moving it into Self
        let name_for_bg = name.clone();
        
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
            active_video_session_id: 0,
            active_batch_id: None,
            batch_start_time: None,
            metrics,
            video_first_frame_time: None,
        };
        // Precompile shaders in background to avoid blocking startup
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            // Pre-compile common transition shader code (the expensive GLSL->WGSL conversion)
            // Pipelines will be created on first use, but shader compilation is cached
            let common_transitions = vec![
                Transition::Fade,
                Transition::CrossZoom { strength: 0.4 },
                Transition::Directional { direction: [0.0, 1.0] },
                Transition::SimpleZoom { zoom_quickness: 0.5 },
                Transition::RotateScaleFade { center: [0.5, 0.5], rotations: 1.0, scale: 0.8, back_color: [0.15, 0.15, 0.15, 1.0] },
                Transition::Circle,
                Transition::LeftRight,
                Transition::Radial { smoothness: 0.5 },
                Transition::Bounce { shadow_colour: [0.0, 0.0, 0.0, 0.6], shadow_height: 0.075, bounces: 3.0 },
                Transition::Swirl,
            ];
            
            for transition in common_transitions {
                // Just compile the shader code - this warms the shader cache
                // Pipeline creation happens on first use and is fast
                let _ = crate::shaders::ShaderManager::get_builtin_shader(&transition);
            }
            let duration = start.elapsed();
            tracing::debug!("[RENDER] {}: Background shader precompilation completed in {:.2}ms", name_for_bg, duration.as_secs_f64() * 1000.0);
        });
        Ok(r)
    }

    pub fn resize_checked(&mut self, width: u32, height: u32) -> anyhow::Result<()> {
        if width > 0 && height > 0 {
            // Check capabilities FIRST to avoid hard panic in wgpu on Nvidia/Wayland
            let caps = self.target_caps();
            if caps.formats.is_empty() {
                warn!("Surface {} is not ready for configuration (no formats). Skipping reconfiguration.", self.name);
                self.configured = false;
                return Ok(());
            }

            // Ensure format is supported
            if !caps.formats.contains(&self.config.format) {
                info!("Updating surface format for {} to {:?}", self.name, caps.formats[0]);
                self.config.format = caps.formats[0];
            }

            self.config.width = width;
            self.config.height = height;

            info!("Configuring surface {} ({}x{})", self.name, width, height);
            self.surface.configure(&self.ctx.device, &self.config);
            
            // Re-create composition texture
            // If transition is active, log that it will continue with new size
            if self.transition_active {
                info!("[TRANSITION] {}: Surface resized during transition, recreating composition texture ({}x{})", 
                    self.name, width, height);
            }
            let texture = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("Composition Texture"),
                size: wgpu::Extent3d { width, height, depth_or_array_layers: 1 },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rgba8UnormSrgb,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::RENDER_ATTACHMENT,
                view_formats: &[],
            });
            self.composition_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor::default()));
            self.composition_texture = Some(texture);
            // Invalidate bind groups since texture changed
            self.transition_bind_group = None;
            self.blit_bind_group = None;
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

    /// Ensures composition texture exists and matches current surface dimensions
    /// Creates it if missing or if dimensions don't match
    fn ensure_composition_texture(&mut self) -> anyhow::Result<()> {
        // Check if we need to create or recreate the composition texture
        let needs_creation = self.composition_texture.is_none() 
            || self.composition_texture_view.is_none();
        
        // Also check if dimensions match (if texture exists but size changed, recreate it)
        let size_mismatch = if let Some(_) = &self.composition_texture {
            // Texture exists, check if we can determine its size
            // We can't easily check texture size, so we'll recreate if dimensions are invalid
            self.config.width == 0 || self.config.height == 0
        } else {
            false
        };
        
        if needs_creation || size_mismatch {
            if self.config.width == 0 || self.config.height == 0 {
                // Can't create texture without valid dimensions
                return Err(anyhow::anyhow!("Cannot create composition texture: invalid dimensions ({}x{})", 
                    self.config.width, self.config.height));
            }
            
            if self.transition_active {
                warn!("[TRANSITION] {}: Composition texture missing during active transition, creating now ({}x{})", 
                    self.name, self.config.width, self.config.height);
            } else {
                debug!("[RENDER] {}: Creating composition texture ({}x{})", 
                    self.name, self.config.width, self.config.height);
            }
            
            let texture = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("Composition Texture"),
                size: wgpu::Extent3d { 
                    width: self.config.width, 
                    height: self.config.height, 
                    depth_or_array_layers: 1 
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rgba8UnormSrgb,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::RENDER_ATTACHMENT,
                view_formats: &[],
            });
            self.composition_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor::default()));
            self.composition_texture = Some(texture);
            
            // Invalidate bind groups since texture changed
            self.transition_bind_group = None;
            self.blit_bind_group = None;
        }
        
        Ok(())
    }

    pub fn apply_config(&mut self, config: &crate::orchestration::OutputConfig) {
        self.active_transition = config.transition.clone();
        self.transition_duration = (config.transition_time as f32 / 1000.0).max(0.001);
        self.needs_redraw = true;
        // Pre-compile the assigned transition early - DISABLED to avoid startup hang
        // self.get_transition_pipeline(&self.active_transition);
    }

    /// Pre-compiles common shaders to avoid stalls during the first transition.
    /// Compiles top 10 most commonly used transitions in background.
    pub fn precompile_common_shaders(&self) {
        debug!("[RENDER] {}: Pre-compiling common shaders", self.name);
        // Pre-compile top 10 most common transitions
        let common_transitions = [
            Transition::Fade,
            Transition::CrossZoom { strength: 0.3 },
            Transition::Radial { smoothness: 0.5 },
            Transition::Circle,
            Transition::Directional { direction: [1.0, 0.0] },
            Transition::SimpleZoom { zoom_quickness: 0.5 },
            Transition::Ripple { amplitude: 0.1, speed: 1.0 },
            Transition::Swirl,
            Transition::Pixelize { squares_min: [10, 10], steps: 10 },
            Transition::Mosaic { endx: 20, endy: 20 },
        ];
        
        for transition in &common_transitions {
            let _ = self.get_transition_pipeline(transition);
        }
    }

    fn get_transition_pipeline(&self, transition: &Transition) -> Option<Arc<wgpu::RenderPipeline>> {
        let name = transition.name();
        
        // Check cache first (using Mutex in ctx)
        if let Some(pipe) = self.ctx.transition_pipelines.lock().get(&name) {
            return Some(pipe.clone());
        }
        
        // Not in cache, compile it
        // Note: For now we'll do synchronous compilation if missing, 
        // but it will be cached for all subsequent calls across all monitors.
        debug!("[RENDER] {}: Compiling shared transition pipeline: {}", self.name, name);
        
        // We'll move the actual compilation logic to a helper that populates the cache
        self.compile_transition_pipeline(transition)
    }

    fn compile_transition_pipeline(&self, transition: &Transition) -> Option<Arc<wgpu::RenderPipeline>> {
        let compile_start = std::time::Instant::now();
        let name = transition.name();
        
        // Get compiled WGSL shader code using ShaderManager (fragment shader only)
        let fragment_shader_code = match crate::shaders::ShaderManager::get_builtin_shader(transition) {
            Ok(code) => code,
            Err(e) => {
                error!("Failed to compile shader for {}: {}. Falling back to fade.", name, e);
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
        };

        // Create vertex shader module from the built-in quad.wgsl
        let vertex_shader = self.ctx.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Quad Vertex Shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/quad.wgsl").into()),
        });

        // Create fragment shader module from the compiled GLSL transition
        let fragment_shader = self.ctx.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some(&format!("Transition Fragment Shader: {}", name)),
            source: wgpu::ShaderSource::Wgsl(fragment_shader_code.into()),
        });

        let pipeline_layout = self.ctx.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some(&format!("Transition Pipeline Layout: {}", name)),
            bind_group_layouts: &[&self.ctx.transition_bind_group_layout],
            push_constant_ranges: &[],
        });

        // Use standard format for composition (always same across all renderers)
        let composition_format = wgpu::TextureFormat::Rgba8UnormSrgb;

        let pipeline = self.ctx.device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
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
                entry_point: Some("main"),  // GLSL main() compiles to "main" in WGSL
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
            cache: None,
        });

        let pipeline_arc = Arc::new(pipeline);
        
        // Update cache
        self.ctx.transition_pipelines.lock().insert(name, pipeline_arc.clone());
        
        // Record shader compile CPU time
        if let Some(m) = &self.metrics {
            let compile_duration = compile_start.elapsed();
            m.record_shader_compile_cpu_time(compile_duration);
        }
        
        Some(pipeline_arc)
    }

    pub fn render(&mut self, context: BackendContext, frame_time: std::time::Instant) -> anyhow::Result<()> {
        let render_start = std::time::Instant::now();
        
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

        let output = match self.surface.get_current_texture() {
            Ok(t) => t,
            Err(wgpu::SurfaceError::Lost) => {
                warn!("Surface Lost for {}. Marking not-configured to trigger re-creation.", self.name);
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
                    debug!("Surface acquisition timeout for {}, skipping frame.", self.name);
                    self.needs_redraw = true; // Try again next loop
                    return Ok(());
                }
                error!("Failed to get current surface texture for {}: {}", self.name, err_str);
                return Ok(());
            }
        };
        let view = output.texture.create_view(&wgpu::TextureViewDescriptor::default());
        
        let mut encoder = self.ctx.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
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
                    debug!("[TRANSITION] {}: Progress updated {:.3} -> {:.3} (elapsed={:.3}s, duration={:.3}s)", 
                        self.name, self.transition_progress, new_progress, elapsed, self.transition_duration);
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
                        
                        // Log Audit Report and record metrics
                        if let Some(stats) = self.transition_stats.take() {
                            let duration = stats.start_time.elapsed();
                            let duration_secs = duration.as_secs_f64();
                            
                            // Record transition duration in metrics
                            if let Some(m) = &self.metrics {
                                m.record_transition(duration);
                            }
                            let fps = if duration_secs > 0.001 { stats.frame_count as f64 / duration_secs } else { 0.0 };
                            let drift = duration.as_secs_f32() - stats.target_duration;
                            let batch_info = stats.batch_id.map(|b| format!(" (Batch: {:x})", b)).unwrap_or_default();
                            
                            info!(
                                "[AUDIT] Transition Completed {}{}:\n  - Duration: {:.3}s (Target: {:.3}s)\n  - Frames: {} (Avg {:.1} FPS)\n  - Drift: {:.3}s",
                                self.name, batch_info, duration.as_secs_f32(), stats.target_duration, stats.frame_count, fps, drift
                            );
                        } else {
                            info!("[TRANSITION] {}: Transition completed (progress={:.3}) - No stats available", self.name, self.transition_progress);
                        }
                    }
                }
            } else {
                // Transition is active but start time not set yet - this is the FIRST RENDER FRAME
                // Initialize the timer using shared batch start time if available for synchronization
                // BUT: If the batch start time is too old (e.g. because asset loading was slow), 
                // we cap it so we don't skip the transition entirely.
                let now = frame_time;
                let start = match self.batch_start_time {
                    Some(batch_start) => {
                        let age = now.saturating_duration_since(batch_start).as_secs_f32();
                        if age > self.transition_duration * 0.8 {
                            // Too old, start nearly from zero (0.1s in) to ensure transition is visible
                            debug!("[TRANSITION] {}: Batch start time too old ({:.3}s), capping drift to preserve transition", self.name, age);
                            now.checked_sub(std::time::Duration::from_millis(100)).unwrap_or(now)
                        } else {
                            batch_start
                        }
                    },
                    None => now,
                };
                self.transition_start_time = Some(start);
                
                // Calculate initial progress based on frame_time vs start
                let elapsed = frame_time.saturating_duration_since(start).as_secs_f32();
                self.transition_progress = (elapsed / self.transition_duration).min(1.0);
                
                // Initialize stats
                self.transition_stats = Some(TransitionStats {
                    start_time: start,
                    frame_count: 0,
                    target_duration: self.transition_duration,
                    batch_id: self.active_batch_id,
                });

                info!("[TRANSITION] {}: Starting transition (duration={:.3}s, initial_progress={:.3})", 
                    self.name, self.transition_duration, self.transition_progress);
            }
        }

        // Render transition if we have all required textures and transition is active
        // Ensure component texture exists if transition is active
        // This must be done BEFORE checking should_render_transition to ensure we don't skip
        // the transition just because the texture was lazily dropped or missing.
        if self.transition_active {
            if let Err(e) = self.ensure_composition_texture() {
                error!("[TRANSITION] {}: Failed to ensure composition texture: {}", self.name, e);
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
                    warn!("[TRANSITION] {}: Failed to get/create transition pipeline for {}", self.name, self.active_transition.name());
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
            self.ctx.queue.write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));

            // USE CACHED BIND GROUP
            if self.transition_bind_group.is_none() {
                self.update_transition_bind_group();
            }

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
                warn!("[TRANSITION] {}: Transition render FAILED - bind_group missing, transition={}", 
                    self.name, self.active_transition.name());
                // Don't set transition_rendered_this_frame - transition didn't actually render
            }
            
            // CLEANUP: Drop prev_texture only when transition is TRULY finished
            if self.transition_progress >= 1.0 && self.current_texture.is_some() {
                if self.prev_texture.is_some() {
                    debug!("[TRANSITION] {}: Transition completed, cleaning up prev_texture", self.name);
                    self.prev_texture = None;
                    self.prev_texture_view = None;
                    self.transition_bind_group = None;
                    self.blit_bind_group = None;
                    self.transition_start_time = None;
                    self.transition_active = false;
                    // Removed redundant poll (Audit Point 33)
                }
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
            self.ctx.queue.write_buffer(&self.uniform_buffer, 0, bytemuck::bytes_of(&uniforms));
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
                && self.composition_texture_view.is_some() {
                
                if !self.transition_rendered_this_frame {
                    // This can happen if setup passed but pipeline/bind_group failed, 
                    // OR if this is the very first frame where we created composition but didn't render yet?
                    // Actually we ensured rendering above.
                    // If we missed rendering, showing composition (which is empty) might flash black.
                    // But usually safe to show composition as it was cleared to black.
                    debug!("[RENDER] {}: Using composition (rendered_this_frame={})", self.name, self.transition_rendered_this_frame);
                }
                Some(BlitSource::Composition)
            } else if self.transition_active && self.prev_texture.is_some() {
                // Transition active but missing required resources -> fall back to prev
                warn!("[RENDER] {}: Transition FALLBACK to PREV - missing resources (comp_view={})", 
                    self.name, self.composition_texture_view.is_some());
                Some(BlitSource::Prev)
            } else {
                // Shouldn't happen, but fallback to current
                Some(BlitSource::Current)
            }
        } else if self.prev_texture.is_some() {
            // No current texture but have previous -> show previous (during transition)
            // This should only happen briefly during transitions
            debug!("[RENDER] {}: No current_texture, falling back to prev_texture (transition_active={})", 
                self.name, self.transition_active);
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
                // No textures available - can't render, but keep needs_redraw for next attempt
                debug!("[RENDER] {}: No blit source available (current={}, prev={})", 
                    self.name, 
                    self.current_texture.is_some(),
                    self.prev_texture.is_some());
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
                let tex_view = match tex_view {
                    Some(v) => {
                        // Create bind group with the texture
                        self.blit_bind_group = Some(self.ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
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
                        }));
                        self.blit_source_is_composition = is_comp;
                        self.blit_source_is_prev = is_prev;
                        v
                    },
                    None => {
                        // Fallback logic: if Composition fails, try Prev, then Current
                        let fallback_view = match blit_source {
                            BlitSource::Composition => {
                                warn!("[RENDER] {}: Composition texture view missing, falling back to prev", self.name);
                                self.prev_texture_view.as_ref().or_else(|| {
                                    warn!("[RENDER] {}: Prev texture view also missing, falling back to current", self.name);
                                    self.current_texture_view.as_ref()
                                })
                            }
                            BlitSource::Prev => {
                                warn!("[RENDER] {}: Prev texture view missing, falling back to current", self.name);
                                self.current_texture_view.as_ref()
                            }
                            BlitSource::Current => {
                                error!("[RENDER] {}: Current texture view missing, cannot render", self.name);
                                None
                            }
                        };
                        
                        match fallback_view {
                            Some(v) => {
                                // Update source to match fallback
                                let is_comp = false;
                                let is_prev = matches!(blit_source, BlitSource::Prev) || matches!(blit_source, BlitSource::Composition);
                                // Recreate bind group with fallback texture
                                self.blit_bind_group = Some(self.ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
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
                                            resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                                        },
                                    ],
                                }));
                                self.blit_source_is_composition = is_comp;
                                self.blit_source_is_prev = is_prev;
                                v
                            }
                            None => {
                                error!("Texture view missing for blit source {:?} and all fallbacks failed (current={}, prev={}, composition={})", 
                                    blit_source, 
                                    self.current_texture_view.is_some(),
                                    self.prev_texture_view.is_some(),
                                    self.composition_texture_view.is_some());
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
                error!("[RENDER] {}: blit_bind_group is None, cannot render!", self.name);
                return Ok(()); // Can't render without bind group
            }
        } // render_pass dropped here

        // Request frame callback BEFORE presenting/committing to ensure correct ordering
        // Deadlock fix: always request on first frame even if one is pending from switch
        if !self.frame_callback_pending || self.transition_progress == 0.0 {
            match context {
                BackendContext::Wayland { surface, qh } => {
                    self.request_frame_callback(surface, qh);
                }
                BackendContext::X11 => {
                    // X11 doesn't use Wayland frame callbacks
                }
            }
        }

        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        output.present();
        
        // Note: frame_callback_pending is reset by the main loop when callback is received
        // Don't reset it here to avoid race conditions
        
        if !self.transition_active {
            self.transition_start_time = None;
        }

        self.last_present_time = std::time::Instant::now();
        
        // CRITICAL: Reset needs_redraw AFTER we've actually rendered and presented
        // This ensures we render at least once for static images
        if self.transition_active {
            // Transition in progress - MUST keep rendering until complete
            self.needs_redraw = true;
        } else if !self.transition_active && self.valid_content_type != crate::queue::ContentType::Video {
            // Transition complete and not video - can reset needs_redraw now that we've presented
            self.needs_redraw = false;
        }
        // For video, keep needs_redraw=true so we continue requesting frame callbacks
        
        // Record renderer CPU time
        if let Some(m) = &self.metrics {
            let render_duration = render_start.elapsed();
            m.record_renderer_cpu_time(render_duration);
        }
        
        Ok(())
    }
    
    /// Request a frame callback from Wayland compositor
    /// This should be called when we need to render, and we'll wait for the callback
    pub fn request_frame_callback(&mut self, layer_surface: &LayerSurface, qh: &QueueHandle<crate::wayland::WaylandBackend>) {
        if self.frame_callback_pending {
            // Check failsafe: if pending for > 500ms, assume lost and allow re-request
            if let Some(r) = self.last_frame_request {
                 if r.elapsed().as_millis() > 500 {
                      warn!("[FRAME] {}: Frame callback stuck for 500ms, re-requesting!", self.name);
                      self.frame_callback_pending = false; // Reset to allow re-request
                 } else {
                      return; // Truly pending
                 }
            }
        }
    
        let wl_surface = layer_surface.wl_surface();
        wl_surface.frame(qh, wl_surface.clone());
        self.frame_callback_pending = true;
        self.last_frame_request = Some(std::time::Instant::now());
        tracing::debug!("[FRAME] {}: Requested frame callback (configured={}, needs_redraw={}, transition_progress={:.3})", 
            self.name, self.configured, self.needs_redraw, self.transition_progress);
    }

    pub fn set_content_type(&mut self, content_type: crate::queue::ContentType) {
        self.valid_content_type = content_type;
    }

    pub fn upload_image_file(&mut self, path: &std::path::Path) -> anyhow::Result<()> {
        let _load_start = std::time::Instant::now();
        let img = image::open(path)?;
        let rgba = img.to_rgba8();
        let (width, height) = rgba.dimensions();
        let data = rgba.into_raw();
        
        self.upload_image_data(data, width, height)
    }

    pub fn upload_image_data(&mut self, data: Vec<u8>, width: u32, height: u32) -> anyhow::Result<()> {
        let upload_start = std::time::Instant::now();
        
        // Calculate mip levels
        let mip_level_count = ((width.max(height) as f32).log2().floor() as u32) + 1;

        // Use Rgba8UnormSrgb for proper color space
        // Use texture pool for image textures (but note: images need mipmaps, so we can't fully pool them)
        // For now, create new texture for images since they need mipmaps
        // Video textures can use the pool since they don't need mipmaps
        let texture = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("Image Texture"),
            size: wgpu::Extent3d { width, height, depth_or_array_layers: 1 },
            mip_level_count,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8UnormSrgb,
            usage: wgpu::TextureUsages::TEXTURE_BINDING 
                | wgpu::TextureUsages::COPY_DST 
                | wgpu::TextureUsages::RENDER_ATTACHMENT,
            view_formats: &[wgpu::TextureFormat::Rgba8UnormSrgb],
        });

        // 1. Upload base level (0)
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
            wgpu::Extent3d { width, height, depth_or_array_layers: 1 },
        );

        // 2. Generate Mipmaps
        if mip_level_count > 1 {
            let mut encoder = self.ctx.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Mipmap Generation Encoder"),
            });

            let pipeline = self.ctx.get_mipmap_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb);

            for i in 1..mip_level_count {
                let src_view = texture.create_view(&wgpu::TextureViewDescriptor {
                    label: Some(&format!("Mip Src Level {}", i-1)),
                    format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                    dimension: Some(wgpu::TextureViewDimension::D2),
                    aspect: wgpu::TextureAspect::All,
                    base_mip_level: i - 1,
                    mip_level_count: Some(1),
                    base_array_layer: 0,
                    array_layer_count: None,
                });

                let dst_view = texture.create_view(&wgpu::TextureViewDescriptor {
                    label: Some(&format!("Mip Dst Level {}", i)),
                    format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                    dimension: Some(wgpu::TextureViewDimension::D2),
                    aspect: wgpu::TextureAspect::All,
                    base_mip_level: i,
                    mip_level_count: Some(1),
                    base_array_layer: 0,
                    array_layer_count: None,
                });

                let bind_group = self.ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
                    label: Some(&format!("Mipmap Bind Group Level {}", i)),
                    layout: &self.ctx.mipmap_bind_group_layout,
                    entries: &[
                        wgpu::BindGroupEntry {
                            binding: 0,
                            resource: wgpu::BindingResource::TextureView(&src_view),
                        },
                        wgpu::BindGroupEntry {
                            binding: 1,
                            resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                        },
                    ],
                });

                let mut rpass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                    label: Some("Mipmap Render Pass"),
                    color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                        view: &dst_view,
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

                rpass.set_pipeline(&pipeline);
                rpass.set_bind_group(0, &bind_group, &[]);
                rpass.draw(0..3, 0..1);
            }
            self.ctx.queue.submit(Some(encoder.finish()));
        }

        self.current_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor {
            label: Some("Image Texture View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            dimension: Some(wgpu::TextureViewDimension::D2),
            aspect: wgpu::TextureAspect::All,
            base_mip_level: 0,
            mip_level_count: Some(mip_level_count),
            base_array_layer: 0,
            array_layer_count: None,
        }));
        
        self.current_texture = Some(texture);
        self.current_aspect = width as f32 / height as f32;
        self.current_texture_size = Some((width, height));
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
            info!("[TRANSITION] {}: Image data uploaded - transition will start on next render frame", self.name);
        } else {
            self.transition_active = false;
            self.transition_progress = 1.0;
            self.transition_just_completed = true; // Signal completion for instant switch
            info!("[TRANSITION] {}: Image data uploaded (Instant) - transition signaled as complete", self.name);
        }
        
        let duration = upload_start.elapsed();
        // Removed TRACE log - use debug! if needed for troubleshooting
        
        Ok(())
    }

    pub fn upload_frame(&mut self, frame: &crate::video::VideoFrame) {
        if self.valid_content_type != crate::queue::ContentType::Video || frame.session_id != self.active_video_session_id {
            debug!("[VIDEO] {}: Discarding stale video frame - valid_type={:?}, frame_session={}, active_session={}", 
                self.name, self.valid_content_type, frame.session_id, self.active_video_session_id);
            return; // Discard stale video frames
        }
        
        // Removed TRACE logs from hot path (called every video frame)
        
        // CRITICAL: If this is the first frame after a switch (prev_texture exists but current_texture is None),
        // reset the transition start time so the transition starts fresh now that we have both textures
        // First frame after a switch is any frame that arrives when current_texture is None
        let is_first_frame_after_switch = self.current_texture.is_none();
        
        // Track first frame timing for metrics (only on first frame of new video session)
        if is_first_frame_after_switch && self.video_first_frame_time.is_none() {
            self.video_first_frame_time = Some(std::time::Instant::now());
        }

        // REUSE texture if size matches (check before creating new texture)
        let needs_new_texture = self.current_texture_size != Some((frame.width, frame.height));
        let texture = if let Some(curr) = self.current_texture.take() {
            if !needs_new_texture {
                // Size matches, reuse texture - this prevents memory leaks
                debug!("[VIDEO] {}: Reusing existing texture {}x{}", self.name, frame.width, frame.height);
                curr
            } else {
                // Size mismatch: return old texture to pool and get new one from pool
                self.current_texture_view = None;
                let old_size = self.current_texture_size;
                if let Some((w, h)) = old_size {
                    self.ctx.return_texture_to_pool(curr, w, h);
                }
                // Get texture from pool or create new one
                self.ctx.get_texture_from_pool(
                    frame.width,
                    frame.height,
                    wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                    self.metrics.as_deref()
                )
            }
        } else {
            // Get texture from pool or create new one
            self.ctx.get_texture_from_pool(
                frame.width,
                frame.height,
                wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                self.metrics.as_deref()
            )
        };

        self.ctx.queue.write_texture(
            wgpu::ImageCopyTexture {
                texture: &texture, 
                mip_level: 0, 
                origin: wgpu::Origin3d::ZERO, 
                aspect: wgpu::TextureAspect::All,
            },
            &frame.data,
            wgpu::ImageDataLayout {
                offset: 0,
                bytes_per_row: Some(4 * frame.width), 
                rows_per_image: Some(frame.height),
            },
            wgpu::Extent3d {
                width: frame.width,
                height: frame.height,
                depth_or_array_layers: 1,
            },
        );

        // Only recreate texture view and invalidate bind groups if size changed (optimization)
        if needs_new_texture || self.current_texture_view.is_none() {
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
            self.transition_bind_group = None; // Invalidate
            self.blit_bind_group = None;      // Invalidate
        }
        
        self.current_texture = Some(texture);
        self.current_texture_size = Some((frame.width, frame.height));
        self.current_aspect = frame.width as f32 / frame.height as f32;
        self.needs_redraw = true;
        
        // CRITICAL: If this is the first frame after a switch, mark transition as active
        // but DON'T set transition_start_time - let render() do that on first actual render frame
        // This ensures consistent timing behavior with image transitions
        if is_first_frame_after_switch {
            // Record video first frame timing
            if let Some(m) = &self.metrics {
                if let Some(start_time) = self.video_first_frame_time {
                    let first_frame_duration = start_time.elapsed();
                    m.record_video_first_frame(first_frame_duration);
                    self.video_first_frame_time = None; // Reset for next video session
                }
            }
            
            if self.prev_texture.is_some() {
                info!("[TRANSITION] {}: First video frame after switch - transition will start on first render frame", self.name);
                self.transition_start_time = None; // Will be set on first render
                self.transition_progress = 0.0;
                self.transition_active = true;
            } else {
                info!("[TRANSITION] {}: First video frame after switch (Instant) - transition signaled as complete", self.name);
                self.transition_active = false;
                self.transition_progress = 1.0;
                self.transition_just_completed = true;
            }
        }
        
        debug!("[TRANSITION] {}: Video frame uploaded - current_texture={}, prev_texture={}, transition_progress={:.3}, transition_start_time={:?}", 
            self.name, 
            self.current_texture.is_some(),
            self.prev_texture.is_some(),
            self.transition_progress,
            self.transition_start_time.is_some());
        
        // Explicitly poll device to ensure old textures are freed promptly
        // This helps prevent GPU memory accumulation during rapid video frame updates
        self.ctx.device.poll(wgpu::Maintain::Poll);
    }

    pub fn switch_content(&mut self) {
        // Always initialize transition state, even if current_texture is None
        // This ensures transitions work even when switching from empty state
        let had_current = self.current_texture.is_some();
        
        if let Some(curr) = self.current_texture.take() {
            self.prev_texture_view = self.current_texture_view.take();
            self.prev_texture = Some(curr);
            self.prev_aspect = self.current_aspect;
        }
        
        // Always reset transition state when switching content
        // Note: transition_start_time will be reset when new content is actually uploaded
        // transition_active will be set to true when new texture is ready (in upload_image/upload_frame)
        self.transition_progress = 0.0;
        self.transition_start_time = None; // Will be set when content is uploaded
        self.transition_active = false; // Will be set to true when new texture is ready
        self.transition_just_completed = false; // Reset completion flag
        self.transition_bind_group = None; // Invalidate
        self.blit_bind_group = None;      // Invalidate
        self.batch_start_time = None;      // Reset
        self.video_first_frame_time = None; // Reset video timing for new session
        self.needs_redraw = true;
        
        // OPTIMIZATION: Don't reset current_texture_size immediately.
        // We want to keep it around to see if next content (video frame) matches.
        // upload_frame will check needs_new_texture against this size.
        
        debug!("[TRANSITION] {}: switch_content() - had_current={}, prev_texture={}, transition_started, current_texture cleared", 
            self.name, had_current, self.prev_texture.is_some());
    }

    pub fn abort_transition(&mut self) {
        if self.transition_active || self.current_texture.is_none() {
            if self.transition_active {
                info!("[TRANSITION] {}: Aborting transition due to load failure", self.name);
            }
            self.transition_active = false;
            self.transition_just_completed = false; // Reset flag
            self.transition_progress = 1.0;
            self.transition_start_time = None;
            self.needs_redraw = true;
        }
    }

    /// Clears the renderer to black (removes current and previous textures)
    /// 
    /// This explicitly drops all texture resources and forces WGPU to reclaim
    /// GPU memory immediately. Useful for cleanup and preventing memory leaks.
    pub fn clear(&mut self) {
        // Explicitly drop textures to free GPU memory
        self.current_texture = None;
        self.current_texture_view = None;
        self.prev_texture = None;
        self.prev_texture_view = None;
        self.composition_texture = None;
        self.composition_texture_view = None;
        self.current_texture_size = None;
        self.transition_progress = 1.0;
        self.transition_active = false;
        self.transition_just_completed = false; // Reset flag
        self.transition_bind_group = None; // Invalidate
        self.blit_bind_group = None;      // Invalidate
        self.needs_redraw = true;
        // Reclaim memory immediately - this ensures GPU resources are freed
        // rather than waiting for WGPU's automatic cleanup
        self.active_video_session_id = 0; // Invalidate current video session
        self.configured = false; // Force re-config next time
        self.ctx.device.poll(wgpu::Maintain::Poll);
    }

    pub fn recreate_surface(&mut self, surface: wgpu::Surface<'static>) {
        self.surface = surface;
        self.configured = false;
        self.needs_redraw = true;
    }
    fn update_transition_bind_group(&mut self) {
        // Only recreate if bind group doesn't exist or texture views changed
        // Check if we already have a valid bind group with the same texture views
        let prev_view = match self.prev_texture_view.as_ref() {
            Some(v) => v,
            None => {
                self.transition_bind_group = None;
                return;
            }
        };
        let current_view = match self.current_texture_view.as_ref() {
            Some(v) => v,
            None => {
                self.transition_bind_group = None;
                return;
            }
        };
        
        // If bind group already exists, assume it's valid (texture views are checked by caller)
        // This avoids unnecessary recreation when render() is called multiple times with same textures
        if self.transition_bind_group.is_some() {
            return;
        }

        self.transition_bind_group = Some(self.ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
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
            }));
    }
}
