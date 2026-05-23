use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::transitions::{random_transition_prewarm_set, transition_prewarm_candidates};
use crate::shaders::Transition;

impl super::Renderer {
    pub fn resize_checked(&mut self, width: u32, height: u32) -> anyhow::Result<()> {
        if width > 0 && height > 0 {
            if self.configured && self.config.width == width && self.config.height == height {
                return Ok(());
            }

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
    pub(super) fn ensure_composition_texture_internal(
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
    pub(super) fn ensure_composition_texture(&mut self) -> anyhow::Result<()> {
        self.ensure_composition_texture_internal(true).map(|_| ())
    }

    pub(super) fn prewarm_transition_resources(&mut self) {
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
        if let Some(frame_latency) = config.frame_latency {
            let clamped = frame_latency.clamp(1, 3);
            if self.config.desired_maximum_frame_latency != clamped {
                debug!(
                    "[RENDER] {}: Updating desired frame latency {} -> {}",
                    self.name, self.config.desired_maximum_frame_latency, clamped
                );
                self.config.desired_maximum_frame_latency = clamped;
                if self.configured {
                    self.surface.configure(&self.ctx.device, &self.config);
                }
            }
        }
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

    pub(super) fn get_transition_pipeline(
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
                source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/quad.wgsl").into()),
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
}
