use super::CudaTextureCache;
use super::video_interop::{create_cuda_backed_texture, import_dmabuf_as_texture};
use super::video_layout::chroma_plane_extent;
use std::time::Instant;
use tracing::{error, info, warn};

fn cuda_frame_sync_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_CUDA_SKIP_FRAME_SYNC")
            .ok()
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(true)
    })
}

impl super::Renderer {
    pub(super) fn upload_frame_dmabuf_nv12(
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
    pub(super) fn upload_frame_cuda_nv12(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    ) -> bool {
        let total_start = Instant::now();

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
        let frame_size = frame.buffer.size();
        let min_y_bytes = y_stride as usize * height as usize;
        let min_uv_bytes = uv_stride as usize * uv_height as usize;
        let expected_uv_offset_floor = min_y_bytes;
        if y_stride < width
            || uv_stride < uv_width.saturating_mul(2)
            || min_y_bytes > frame_size
            || (uv_offset as usize) > frame_size
            || (uv_offset as usize).saturating_add(min_uv_bytes) > frame_size
            || (uv_offset as usize) < expected_uv_offset_floor
            || (y_stride & 1) != 0
            || (uv_stride & 1) != 0
        {
            warn!(
                "[VIDEO] {}: Rejecting CUDA NV12 layout and falling back to CPU upload: frame={}x{} size={} y_stride={} uv_offset={} uv_stride={} min_uv_offset={}",
                self.name,
                width,
                height,
                frame_size,
                y_stride,
                uv_offset,
                uv_stride,
                expected_uv_offset_floor
            );
            return false;
        }

        // (Re)create shared CUDA↔Vulkan textures when dimensions change
        let need_new = self
            .cuda_textures
            .as_ref()
            .is_none_or(|c| c.width != width || c.height != height);
        if need_new {
            self.cuda_nv12_bind_group = None;
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
        let cuda_map_start = Instant::now();
        let guard = match crate::cuda_interop::map_buffer_cuda(&frame.buffer) {
            Some(g) => g,
            None => return false,
        };
        let cuda_map_duration = cuda_map_start.elapsed();
        let base_ptr = guard.device_ptr();

        let (cuda_copy_duration, cuda_sync_duration) = {
            let ci_guard = self.ctx.cuda_interop.lock();
            let ci = ci_guard.as_ref().unwrap();

            let cuda_copy_start = Instant::now();

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

            let cuda_copy_duration = cuda_copy_start.elapsed();

            // Synchronize CUDA to ensure writes are visible to Vulkan
            let cuda_sync_start = Instant::now();
            if cuda_frame_sync_enabled() {
                if let Err(e) = ci.synchronize() {
                    error!("[VIDEO] {}: CUDA sync failed: {e}", self.name);
                    return false;
                }
            }
            (cuda_copy_duration, cuda_sync_start.elapsed())
        };

        drop(guard);

        let convert_submit_start = Instant::now();

        // Run NV12→RGBA conversion shader (same as DMA-BUF and CPU NV12 paths)
        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("NV12 CUDA Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        if self.cuda_nv12_bind_group.is_none() {
            self.cuda_nv12_bind_group = Some(self.ctx.device.create_bind_group(
                &wgpu::BindGroupDescriptor {
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
                },
            ));
        }
        let bind_group = self.cuda_nv12_bind_group.as_ref().unwrap();

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
            pass.set_bind_group(0, bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        if let Some(metrics) = &self.metrics {
            metrics.record_video_cuda_upload_stages(
                cuda_map_duration,
                cuda_copy_duration,
                cuda_sync_duration,
                convert_submit_start.elapsed(),
                total_start.elapsed(),
            );
        }
        true
    }
}
