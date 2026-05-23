use super::context::WgpuContext;
use std::sync::Arc;
use tracing::{debug, warn};

impl WgpuContext {
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
                source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/quad.wgsl").into()),
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
                source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/mipmap.wgsl").into()),
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
}
