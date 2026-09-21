use super::CudaTextureCache;
use super::video_interop::create_cuda_backed_texture;
use super::video_layout::chroma_plane_extent;
use std::time::Instant;
use tracing::{error, info, warn};

const MAX_CUDA_IN_FLIGHT_FRAMES: usize = 6;

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
    /// CUDA zero-copy NV12: map the GStreamer CUDAMemory buffer, GPU-copy to
    /// Vulkan-exported textures via CUDA-Vulkan interop, then run the NV12→RGBA
    /// CUDA zero-copy NV12 upload: allocate CUDA-exportable memory, import into
    /// Vulkan, GPU-copy decoded frame, then run NV12→RGBA conversion shader.
    /// Returns false if any step fails (caller falls back to CPU upload).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn upload_frame_cuda_nv12(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: Option<&wgpu::Texture>,
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
                match crate::cuda_interop::CudaInterop::new(&self.ctx.device) {
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
        let frame_size = frame.storage.byte_len();
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
            self.final_nv12_bind_group = None;
            let ci_guard = self.ctx.cuda_interop.lock();
            let ci = ci_guard.as_ref().unwrap();

            // Destroy old cache
            if let Some(mut old) = self.cuda_textures.take() {
                if let Some(timeline) = old.timeline.take() {
                    timeline.destroy(ci);
                }
                old.in_flight_frames.clear();
                drop(old.y_view);
                drop(old.uv_view);
                drop(old.y_texture);
                drop(old.uv_texture);
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
                    drop(y_tex);
                    ci.free_exportable(y_cuda_alloc);
                    self.ctx
                        .cuda_interop_failed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return false;
                }
            };

            let y_view = y_tex.create_view(&wgpu::TextureViewDescriptor::default());
            let uv_view = uv_tex.create_view(&wgpu::TextureViewDescriptor::default());
            let timeline = match super::video_interop::CudaVulkanTimeline::new(ci, &self.ctx.device)
            {
                Ok(timeline) => Some(timeline),
                Err(error) => {
                    warn!(
                        "[CUDA-VK] {}: explicit timeline unavailable ({error:#}); using blocking CUDA synchronization fallback",
                        self.name
                    );
                    None
                }
            };

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
                timeline,
                in_flight_frames: std::collections::VecDeque::with_capacity(6),
                width,
                height,
            });
        }

        if let Some(cache) = self.cuda_textures.as_mut()
            && let Some(timeline) = cache.timeline.as_ref()
        {
            match timeline.completed_value() {
                Ok(completed) => {
                    while cache
                        .in_flight_frames
                        .front()
                        .is_some_and(|(ready, _)| *ready <= completed)
                    {
                        cache.in_flight_frames.pop_front();
                    }
                }
                Err(error) => warn!(
                    "[CUDA-VK] {}: timeline completion query failed: {error:#}",
                    self.name
                ),
            }
        }

        if self
            .cuda_textures
            .as_ref()
            .is_some_and(|cache| cache.in_flight_frames.len() >= MAX_CUDA_IN_FLIGHT_FRAMES)
        {
            let ci_guard = self.ctx.cuda_interop.lock();
            let ci = ci_guard.as_ref().expect("CUDA interop was initialized");
            if let Err(error) = ci.synchronize() {
                error!(
                    "[VIDEO] {}: CUDA in-flight retirement sync failed: {error}",
                    self.name
                );
                self.ctx
                    .cuda_interop_failed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return false;
            }
            self.cuda_textures
                .as_mut()
                .expect("CUDA textures are initialized")
                .in_flight_frames
                .clear();
        }

        // Map the GStreamer CUDA buffer to get the source device pointer
        let cuda_map_start = Instant::now();
        let Some(buffer) = frame.storage.gstreamer_buffer() else {
            error!(
                "[VIDEO] {}: CUDA frame did not retain its GStreamer buffer owner",
                self.name
            );
            return false;
        };
        let mut guard = match crate::cuda_interop::map_buffer_cuda(buffer) {
            Some(g) => Some(g),
            None => return false,
        };
        let cuda_map_duration = cuda_map_start.elapsed();
        let base_ptr = guard.as_ref().expect("CUDA map guard is live").device_ptr();

        let (cuda_copy_duration, cuda_sync_duration, used_timeline) = {
            let ci_guard = self.ctx.cuda_interop.lock();
            let ci = ci_guard.as_ref().unwrap();
            let cache = self.cuda_textures.as_mut().unwrap();

            let uv_row_bytes = (uv_width * 2) as usize;
            let y_destination = cache.y_cuda_alloc.dev_ptr + cache.y_offset as u64;
            let uv_destination = cache.uv_cuda_alloc.dev_ptr + cache.uv_offset as u64;
            let y_pitch = cache.y_pitch;
            let uv_pitch = cache.uv_pitch;
            let mut cuda_copy_duration = std::time::Duration::ZERO;
            let cuda_sync_duration;
            let used_timeline = if let Some(timeline) = cache.timeline.as_mut() {
                let (release, ready) = timeline.next_frame_values();
                let handshake_start = Instant::now();
                let result = self.ctx.with_raw_queue_lock(|| -> Result<(), String> {
                    timeline
                        .signal_vulkan_release(release)
                        .map_err(|error| error.to_string())?;
                    ci.wait_timeline_async(timeline.cuda(), release)?;
                    let copy_start = Instant::now();
                    ci.copy_2d_async(
                        base_ptr,
                        y_stride as usize,
                        y_destination,
                        y_pitch,
                        width as usize,
                        height as usize,
                    )?;
                    ci.copy_2d_async(
                        base_ptr + uv_offset as u64,
                        uv_stride as usize,
                        uv_destination,
                        uv_pitch,
                        uv_row_bytes,
                        uv_height as usize,
                    )?;
                    cuda_copy_duration = copy_start.elapsed();
                    ci.signal_timeline_async(timeline.cuda(), ready)?;
                    timeline
                        .wait_vulkan_ready(ready)
                        .map_err(|error| error.to_string())
                });
                if let Err(error) = result {
                    error!(
                        "[VIDEO] {}: CUDA/Vulkan timeline handoff failed: {error}",
                        self.name
                    );
                    if let Err(sync_error) = ci.synchronize() {
                        error!(
                            "[VIDEO] {}: CUDA recovery sync after handoff failure failed: {sync_error}",
                            self.name
                        );
                        // CUDA may still reference the mapped source. Retain
                        // the guard in the bounded cache and disable this path
                        // instead of unmapping it on the error return.
                        cache
                            .in_flight_frames
                            .push_back((u64::MAX, guard.take().expect("CUDA map guard is live")));
                        self.ctx
                            .cuda_interop_failed
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    return false;
                }
                cuda_sync_duration = handshake_start.elapsed().saturating_sub(cuda_copy_duration);
                cache
                    .in_flight_frames
                    .push_back((ready, guard.take().expect("CUDA map guard is live")));
                true
            } else {
                // Compatibility path for pre-timeline drivers. Both copies and
                // cuMemcpy2D calls are synchronous; the optional context fence
                // below is additional cross-API visibility hardening.
                let copy_start = Instant::now();
                if let Err(error) = ci.copy_2d(
                    base_ptr,
                    y_stride as usize,
                    cache.y_cuda_alloc.dev_ptr + cache.y_offset as u64,
                    cache.y_pitch,
                    width as usize,
                    height as usize,
                ) {
                    error!("[VIDEO] {}: CUDA Y copy failed: {error}", self.name);
                    return false;
                }
                if let Err(error) = ci.copy_2d(
                    base_ptr + uv_offset as u64,
                    uv_stride as usize,
                    cache.uv_cuda_alloc.dev_ptr + cache.uv_offset as u64,
                    cache.uv_pitch,
                    uv_row_bytes,
                    uv_height as usize,
                ) {
                    error!("[VIDEO] {}: CUDA UV copy failed: {error}", self.name);
                    return false;
                }
                cuda_copy_duration = copy_start.elapsed();
                let sync_start = Instant::now();
                if cuda_frame_sync_enabled()
                    && let Err(error) = ci.synchronize()
                {
                    error!("[VIDEO] {}: CUDA sync failed: {error}", self.name);
                    return false;
                }
                cuda_sync_duration = sync_start.elapsed();
                false
            };
            (cuda_copy_duration, cuda_sync_duration, used_timeline)
        };

        drop(guard);

        if let Some(metrics) = &self.metrics {
            use crate::observability::video_backend::VideoBackendMetricKind;
            metrics.record_video_backend_metric(if used_timeline {
                VideoBackendMetricKind::CudaTimelineHandoff
            } else {
                VideoBackendMetricKind::CudaBlockingSync
            });
        }

        let convert_submit_start = Instant::now();

        let cache = self.cuda_textures.as_ref().unwrap();

        // Run NV12→RGBA conversion shader (same as DMA-BUF and CPU NV12 paths)
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
            self.final_nv12_bind_group =
                Some(std::sync::Arc::new(self.ctx.device.create_bind_group(
                    &wgpu::BindGroupDescriptor {
                        label: Some("CUDA NV12 Final Blit Bind Group"),
                        layout: &self.ctx.native_nv12_blit_bind_group_layout,
                        entries: &[
                            wgpu::BindGroupEntry {
                                binding: 0,
                                resource: self.yuv_uniform_buffer.as_entire_binding(),
                            },
                            wgpu::BindGroupEntry {
                                binding: 1,
                                resource: wgpu::BindingResource::TextureView(&cache.y_view),
                            },
                            wgpu::BindGroupEntry {
                                binding: 2,
                                resource: wgpu::BindingResource::TextureView(&cache.uv_view),
                            },
                            wgpu::BindGroupEntry {
                                binding: 3,
                                resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                            },
                        ],
                    },
                )));
        }
        if output.is_none() {
            self.active_yuv_source = Some(super::YuvSource {
                format: super::YuvFormat::Nv12,
                origin: super::YuvOrigin::Cuda,
                width,
                height,
            });
            if let Some(metrics) = &self.metrics {
                metrics.record_video_cuda_upload_stages(
                    cuda_map_duration,
                    cuda_copy_duration,
                    cuda_sync_duration,
                    std::time::Duration::ZERO,
                    total_start.elapsed(),
                );
            }
            return true;
        }
        self.active_yuv_source = None;
        let output_view =
            output
                .expect("checked above")
                .create_view(&wgpu::TextureViewDescriptor {
                    label: Some("NV12 CUDA Convert Output View"),
                    format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                    ..Default::default()
                });
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
        self.ctx.submit(std::iter::once(encoder.finish()));
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
