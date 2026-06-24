use std::os::fd::AsRawFd;
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
            self.begin_content_swap();
        }

        let source_width = frame.width;
        let source_height = frame.height;
        let (presentation_width, presentation_height) = match frame.format {
            crate::video::VideoFrameFormat::Rgba => (source_width, source_height),
            #[cfg(feature = "mpv-backend")]
            crate::video::VideoFrameFormat::GlExternalRgba { .. } => (source_width, source_height),
            _ => compute_cover_target_dimensions(
                source_width,
                source_height,
                self.config.width.max(1),
                self.config.height.max(1),
            ),
        };

        #[cfg(feature = "mpv-backend")]
        if let crate::video::VideoFrameFormat::GlExternalRgba { frame } = &frame.format {
            if is_first_frame_after_switch {
                info!(
                    "[VIDEO] {}: Frame decode path: libmpv OpenGL-Vulkan shared RGBA source={}x{} presentation={}x{}",
                    self.name, source_width, source_height, presentation_width, presentation_height
                );
            }
            self.last_video_source_size = Some((source_width, source_height));
            self.last_video_presentation_size = Some((presentation_width, presentation_height));
            self.release_video_backend_resources("libmpv GL shared frame path");
            self.set_current_gl_external_rgba(frame, presentation_width, presentation_height);
            self.current_aspect = source_width as f32 / source_height as f32;
            self.finish_video_frame_upload(is_first_frame_after_switch);
            return;
        }

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
        #[cfg(feature = "mpv-backend")]
        {
            self.current_external_view = None;
            let frame = self.current_external_frame.take();
            self.drop_external_frame(frame);
        }

        if is_first_frame_after_switch {
            let path_name = match &frame.format {
                crate::video::VideoFrameFormat::CudaNv12 { .. } => "CUDA zero-copy NV12",
                crate::video::VideoFrameFormat::DmaBufNv12 { .. } => "DMA-BUF zero-copy NV12",
                crate::video::VideoFrameFormat::Nv12 { .. } => "NV12 CPU upload",
                crate::video::VideoFrameFormat::I420 { .. } => "I420 CPU upload",
                crate::video::VideoFrameFormat::Rgba => "RGBA CPU upload (legacy)",
                #[cfg(feature = "mpv-backend")]
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
            #[cfg(feature = "mpv-backend")]
            crate::video::VideoFrameFormat::GlExternalRgba { .. } => {
                self.release_video_backend_resources("libmpv GL shared frame path");
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
            #[cfg(feature = "mpv-backend")]
            crate::video::VideoFrameFormat::GlExternalRgba { frame } => {
                self.upload_frame_gl_external_rgba(frame, &texture);
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

    #[cfg(feature = "mpv-backend")]
    fn has_prev_external_texture_for_video(&self) -> bool {
        self.prev_external_view.is_some()
    }

    #[cfg(not(feature = "mpv-backend"))]
    fn has_prev_external_texture_for_video(&self) -> bool {
        false
    }
}
