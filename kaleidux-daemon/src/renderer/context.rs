use raw_window_handle::{HasDisplayHandle, HasWindowHandle};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};
use wgpu::{Adapter, Device, Instance, Queue, Surface};

use super::pipeline_cache;
use super::pipeline_cache::PipelineLRU;
use super::texture::TexturePoolEntry;

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
    pub(super) pipeline_cache_path: Option<PathBuf>,
    // Texture pool: (width, height, mip_level_count) -> Vec of available textures
    pub texture_pool: parking_lot::Mutex<HashMap<(u32, u32, u32), Vec<TexturePoolEntry>>>,
    // Shared CUDA interop context (one per GPU, shared across all renderers)
    pub(super) cuda_interop: parking_lot::Mutex<Option<crate::cuda_interop::CudaInterop>>,
    pub(super) cuda_interop_failed: std::sync::atomic::AtomicBool,
}

const MAX_PIPELINE_CACHE_SIZE: usize = 50;
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

        let device_descriptor = wgpu::DeviceDescriptor {
            label: Some("Kaleidux Shared Device"),
            required_features,
            required_limits: adapter.limits(),
            // Favor smaller allocator blocks over peak throughput. The retained
            // texture logs show renderer-visible textures are not the dominant
            // RSS anymore, so reducing allocator slack is the next useful lever.
            memory_hints: wgpu::MemoryHints::MemoryUsage,
        };
        #[cfg(feature = "mpv-backend")]
        let (device, queue) = if super::context_vulkan::mpv_gl_interop_requested() {
            super::context_vulkan::create_mpv_gl_interop_device(&adapter, &device_descriptor)?
        } else {
            adapter.request_device(&device_descriptor, None).await?
        };
        #[cfg(not(feature = "mpv-backend"))]
        let (device, queue) = adapter.request_device(&device_descriptor, None).await?;

        let pipeline_cache_path = pipeline_cache::path_for_adapter(&adapter);
        let pipeline_cache_seed = pipeline_cache_path
            .as_ref()
            .and_then(|path| pipeline_cache::load_seed(path));
        let pipeline_cache = if required_features.contains(wgpu::Features::PIPELINE_CACHE) {
            // SAFETY: the cache descriptor is created for this live `Device`; seed bytes come
            // from the adapter-specific cache path and `fallback=true` allows invalid seeds.
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
            source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/nv12_convert.wgsl").into()),
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
            source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/i420_convert.wgsl").into()),
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
                        // SAFETY: `fd` is a fresh exportable allocation descriptor returned by CUDA;
                        // this warmup path does not import it elsewhere, so Kaleidux closes it here.
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
}
