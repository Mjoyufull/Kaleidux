use std::sync::OnceLock;

use super::compute_cover_target_dimensions;
use tracing::{debug, error, info, warn};

fn trace_video_upload_enabled() -> bool {
    if crate::observability::trace_all::trace_all_enabled() {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("KLD_TRACE_VIDEO_UPLOAD")
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

impl super::Renderer {
    pub fn upload_frame(&mut self, frame: &crate::video::VideoFrame) {
        if !self.transition_active && !self.content_swap_pending && self.has_previous_texture() {
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

        let is_first_frame_after_switch = self.content_swap_pending || !self.has_current_texture();

        if is_first_frame_after_switch && self.video_first_frame_time.is_none() {
            self.video_first_frame_time = Some(std::time::Instant::now());
        }

        if is_first_frame_after_switch {
            self.snapshot_yuv_for_transition();
            self.begin_content_swap();
        }

        let source_width = frame.width;
        let source_height = frame.height;
        if !matches!(
            frame.format,
            crate::video::VideoFrameFormat::Rgba
                | crate::video::VideoFrameFormat::GlExternalRgba { .. }
        ) {
            self.write_yuv_uniforms(frame.color, frame.geometry);
        }
        let (presentation_width, presentation_height) = match frame.format {
            crate::video::VideoFrameFormat::Rgba => (source_width, source_height),
            crate::video::VideoFrameFormat::GlExternalRgba { .. } => (source_width, source_height),
            _ => compute_cover_target_dimensions(
                source_width,
                source_height,
                self.config.width.max(1),
                self.config.height.max(1),
            ),
        };

        if let crate::video::VideoFrameFormat::GlExternalRgba {
            frame: external_frame,
        } = &frame.format
        {
            if is_first_frame_after_switch {
                info!(
                    "[VIDEO] {}: Frame decode path: libmpv OpenGL-Vulkan shared RGBA source={}x{} presentation={}x{}",
                    self.name, source_width, source_height, presentation_width, presentation_height
                );
            }
            self.last_video_source_size = Some((source_width, source_height));
            self.last_video_presentation_size = Some((presentation_width, presentation_height));
            self.release_video_backend_resources("libmpv GL shared frame path");
            if is_first_frame_after_switch {
                self.external_blit_bind_groups.clear();
            }
            self.set_current_gl_external_rgba(
                external_frame,
                presentation_width,
                presentation_height,
            );
            self.current_aspect = frame.geometry.display_aspect();
            self.finish_video_frame_upload(is_first_frame_after_switch);
            return;
        }

        let direct_yuv = matches!(
            frame.format,
            crate::video::VideoFrameFormat::Nv12 { .. }
                | crate::video::VideoFrameFormat::P010 { .. }
                | crate::video::VideoFrameFormat::I420 { .. }
                | crate::video::VideoFrameFormat::CudaNv12 { .. }
        ) && !self.transition_active
            && !self.has_previous_texture();

        // Transitions and RGB inputs need an RGBA texture. Steady CPU YUV is
        // retained as planes and sampled by the final surface pass.
        let needs_new_texture = !direct_yuv
            && match self.current_texture.as_ref() {
                Some(curr) => {
                    self.current_texture_size != Some((presentation_width, presentation_height))
                        || curr.mip_level_count() > 1
                }
                None => true,
            };
        let mut replaced_texture = None;
        let mut texture = if direct_yuv {
            if let Some(curr) = self.current_texture.take() {
                if let Some((w, h)) = self.current_texture_size.take() {
                    self.ctx.return_texture_to_pool(curr, w, h);
                }
            }
            self.current_texture_view = None;
            None
        } else {
            Some(match self.current_texture.take() {
                Some(curr) => {
                    if !needs_new_texture {
                        curr
                    } else {
                        if let Some((w, h)) = self.current_texture_size {
                            replaced_texture = Some((curr, w, h));
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
            })
        };
        {
            self.current_external_view = None;
            let frame = self.current_external_frame.take();
            self.drop_external_frame(frame);
        }

        if is_first_frame_after_switch {
            let path_name = match &frame.format {
                crate::video::VideoFrameFormat::CudaNv12 { .. } => "CUDA zero-copy NV12",
                crate::video::VideoFrameFormat::DmaBufNv12 { .. } => "DMA-BUF zero-copy NV12",
                crate::video::VideoFrameFormat::NativeDmaBufNv12 { .. } => {
                    "native cached DMA-BUF NV12 single-GPU-copy"
                }
                crate::video::VideoFrameFormat::Nv12 { .. } => "NV12 CPU upload",
                crate::video::VideoFrameFormat::P010 { .. } => "P010 10-bit CPU upload",
                crate::video::VideoFrameFormat::I420 { .. } => "I420 CPU upload",
                crate::video::VideoFrameFormat::Rgba => "RGBA CPU upload (legacy)",
                crate::video::VideoFrameFormat::GlExternalRgba { .. } => {
                    "libmpv OpenGL-Vulkan shared RGBA"
                }
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
                if self.nv12_staging_size.is_some() {
                    self.release_nv12_staging("cuda frame path");
                }
                self.release_i420_staging("cuda frame path");
                self.release_p010_staging("cuda frame path");
                self.release_dmabuf_cache();
            }
            crate::video::VideoFrameFormat::DmaBufNv12 { .. } => {
                self.release_i420_staging("GStreamer dmabuf frame path");
                self.release_p010_staging("GStreamer dmabuf frame path");
                self.release_cuda_cache();
                self.release_dmabuf_cache();
            }
            crate::video::VideoFrameFormat::NativeDmaBufNv12 { .. } => {
                self.release_i420_staging("native dmabuf frame path");
                self.release_p010_staging("native dmabuf frame path");
                self.release_cuda_cache();
                self.release_dmabuf_cache();
            }
            crate::video::VideoFrameFormat::Nv12 { .. } => {
                self.release_i420_staging("nv12 frame path");
                self.release_p010_staging("nv12 frame path");
                self.release_cuda_cache();
                self.release_dmabuf_cache();
            }
            crate::video::VideoFrameFormat::P010 { .. } => {
                self.release_nv12_staging("p010 frame path");
                self.release_i420_staging("p010 frame path");
                self.release_cuda_cache();
                self.release_dmabuf_cache();
            }
            crate::video::VideoFrameFormat::I420 { .. } => {
                self.release_nv12_staging("i420 frame path");
                self.release_p010_staging("i420 frame path");
                self.release_cuda_cache();
                self.release_dmabuf_cache();
            }
            crate::video::VideoFrameFormat::Rgba => {
                self.release_video_backend_resources("rgba frame path");
            }
            crate::video::VideoFrameFormat::GlExternalRgba { .. } => {
                self.release_video_backend_resources("libmpv GL shared frame path");
            }
        }

        let upload_succeeded = match &frame.format {
            crate::video::VideoFrameFormat::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                self.upload_frame_nv12(
                    frame,
                    texture.as_ref(),
                    source_width,
                    source_height,
                    *y_stride,
                    *uv_offset,
                    *uv_stride,
                );
                true
            }
            crate::video::VideoFrameFormat::P010 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                self.upload_frame_p010(
                    frame,
                    texture.as_ref(),
                    source_width,
                    source_height,
                    *y_stride,
                    *uv_offset,
                    *uv_stride,
                );
                true
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
                    texture.as_ref(),
                    source_width,
                    source_height,
                    *y_stride,
                    *u_offset,
                    *u_stride,
                    *v_offset,
                    *v_stride,
                );
                true
            }
            crate::video::VideoFrameFormat::Rgba => {
                self.upload_frame_rgba(
                    frame,
                    texture.as_ref().expect("RGBA path allocates a texture"),
                    source_width,
                    source_height,
                );
                true
            }
            crate::video::VideoFrameFormat::GlExternalRgba { .. } => {
                unreachable!("external GL frames return before allocating an upload texture");
            }
            crate::video::VideoFrameFormat::DmaBufNv12 { frame: dmabuf } => {
                let uploaded = self.upload_frame_native_dmabuf_nv12(
                    frame,
                    texture
                        .as_ref()
                        .expect("modifier-aware DMA-BUF path primes an RGBA target"),
                    source_width,
                    source_height,
                    dmabuf,
                    super::YuvOrigin::DmaBuf,
                );
                if !uploaded {
                    warn!("[VIDEO] modifier-aware GStreamer DMA-BUF import failed");
                }
                uploaded
            }
            crate::video::VideoFrameFormat::NativeDmaBufNv12 {
                frame: native_dmabuf,
            } => {
                let uploaded = self.upload_frame_native_dmabuf_nv12(
                    frame,
                    texture
                        .as_ref()
                        .expect("native path currently primes an RGBA target"),
                    source_width,
                    source_height,
                    native_dmabuf,
                    super::YuvOrigin::NativeDmaBuf,
                );
                if !uploaded {
                    warn!("[VIDEO] native DMA-BUF import failed");
                }
                uploaded
            }
            crate::video::VideoFrameFormat::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                if !self.upload_frame_cuda_nv12(
                    frame,
                    texture.as_ref(),
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
                        texture.as_ref(),
                        source_width,
                        source_height,
                        *y_stride,
                        *uv_offset,
                        *uv_stride,
                    );
                }
                true
            }
        };

        if !upload_succeeded {
            if self.active_yuv_source.is_some_and(|source| {
                matches!(
                    source.origin,
                    super::YuvOrigin::DmaBuf | super::YuvOrigin::NativeDmaBuf
                )
            }) {
                self.active_yuv_source = None;
                self.final_nv12_bind_group = None;
            }
            if let Some(failed_texture) = texture.take() {
                if needs_new_texture {
                    self.ctx.return_texture_to_pool(
                        failed_texture,
                        presentation_width,
                        presentation_height,
                    );
                } else {
                    self.current_texture = Some(failed_texture);
                }
            }
            if let Some((previous, width, height)) = replaced_texture.take() {
                self.current_texture = Some(previous);
                self.current_texture_size = Some((width, height));
            }
            // Keep presenting the last valid content while the backend
            // degrades this source to its CPU upload path.
            self.needs_redraw = true;
            return;
        }

        if let Some((previous, width, height)) = replaced_texture.take() {
            self.ctx.return_texture_to_pool(previous, width, height);
        }

        if self.active_yuv_source.is_some() && !self.transition_active {
            if let Some(texture) = texture.take() {
                self.ctx
                    .return_texture_to_pool(texture, presentation_width, presentation_height);
            }
            self.current_texture_view = None;
            self.current_texture_size = None;
        }

        if let Some(texture) = texture {
            if needs_new_texture || self.current_texture_view.is_none() {
                drop(self.current_texture_view.take());

                self.current_texture_view =
                    Some(texture.create_view(&wgpu::TextureViewDescriptor {
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
        } else {
            self.current_texture_size = None;
            self.transition_bind_group = None;
            self.blit_bind_group = None;
        }
        self.current_aspect = frame.geometry.display_aspect();
        self.finish_video_frame_upload(is_first_frame_after_switch);

        // device.poll deferred to end-of-loop to avoid redundant driver calls (P-14)
    }

    fn finish_video_frame_upload(&mut self, is_first_frame_after_switch: bool) {
        self.needs_redraw = true;

        if is_first_frame_after_switch {
            if let Some(m) = &self.metrics {
                if let Some(start_time) = self.video_first_frame_time {
                    let first_frame_duration = start_time.elapsed();
                    m.record_video_first_frame(first_frame_duration);
                    self.video_first_frame_time = None;
                }
            }

            if self.has_previous_texture() {
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

        if trace_video_upload_enabled() {
            tracing::trace!(
                "[TRANSITION] {}: Video frame uploaded - current_texture={}, prev_texture={}, transition_progress={:.3}, transition_start_time={:?}",
                self.name,
                self.has_current_texture(),
                self.has_previous_texture(),
                self.transition_progress,
                self.transition_start_time.is_some()
            );
        }

        // device.poll deferred to end-of-loop to avoid redundant driver calls (P-14)
    }

    fn has_previous_texture(&self) -> bool {
        self.prev_texture.is_some() || self.has_prev_external_texture_for_video()
    }

    fn has_prev_external_texture_for_video(&self) -> bool {
        self.prev_external_view.is_some()
    }
}
