use super::video_layout::chroma_plane_extent;
use std::ffi::c_void;
use std::sync::Arc;
use tracing::{info, warn};
use wayland_client::backend::ObjectId;

impl super::Renderer {
    pub(crate) fn try_present_native_gl_surface(
        &mut self,
        frame: &crate::video::VideoFrame,
        wayland_display: *mut c_void,
        surface_id: ObjectId,
        output_size: (u32, u32),
    ) -> bool {
        if self.native_gl_surface_failed || !native_gl_surface_enabled() {
            return false;
        }
        if self.native_gl_surface.is_none() {
            match super::native_gl_surface::NativeGlSurfaceRenderer::new(
                wayland_display,
                surface_id,
                output_size,
            ) {
                Ok(renderer) => {
                    info!(
                        "[NATIVE-GL] {}: persistent DMA-BUF NV12 -> EGLImage -> OpenGL Wayland surface initialized",
                        self.name
                    );
                    self.native_gl_surface = Some(renderer);
                }
                Err(error) => {
                    self.native_gl_surface_failed = true;
                    warn!(
                        "[NATIVE-GL] {}: EGL steady-state path unavailable ({error:#}); retaining Vulkan single-copy composition",
                        self.name
                    );
                    return false;
                }
            }
        }
        let result = self
            .native_gl_surface
            .as_mut()
            .expect("native GL renderer initialized above")
            .present(frame, output_size);
        if let Err(error) = result {
            self.native_gl_surface = None;
            self.native_gl_surface_failed = true;
            self.record_native_import_failure(format!(
                "native EGLImage presentation failed: {error:#}"
            ));
            warn!(
                "[NATIVE-GL] {}: DMA-BUF presentation failed ({error:#}); retaining Vulkan single-copy composition",
                self.name
            );
            return false;
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_video_backend_metric(
                crate::observability::video_backend::VideoBackendMetricKind::NativeGlSurfacePresent,
            );
        }
        true
    }

    pub(crate) fn native_gl_surface_requested() -> bool {
        native_gl_surface_enabled()
    }

    pub(crate) fn can_present_native_wayland_surface(&self) -> bool {
        !self.pause_on_fullscreen
            && self.valid_content_type == crate::queue::ContentType::Video
            && !self.transition_active
            && !self.content_swap_pending
    }

    pub(crate) fn retain_native_wayland_snapshot(&mut self, frame: &crate::video::VideoFrame) {
        self.native_wayland_snapshot_frame = frame.try_clone();
    }

    /// Attribute a successful native present to the exact path that produced
    /// it. Without this the presentation ladder is unobservable: every path
    /// increments the same aggregate counter.
    pub(crate) fn record_native_present_path(
        &self,
        kind: crate::observability::video_backend::VideoBackendMetricKind,
    ) {
        if let Some(metrics) = &self.metrics {
            metrics.record_video_backend_metric(kind);
        }
    }

    pub(crate) fn record_native_wayland_surface_presented(&mut self) {
        // The native subsurface now drives steady video pacing through buffer
        // release/demand. Any callback armed on the WGPU parent for the final
        // transition frame can be occluded indefinitely by that subsurface;
        // do not retain it as renderer work or retry it every 500 ms.
        self.needs_redraw = false;
        self.frame_callback_pending = false;
        self.last_frame_request = None;
        if let Some(metrics) = &self.metrics {
            metrics.record_video_frame_presented();
            metrics.record_video_frame_present_source();
            metrics.record_video_backend_metric(
                crate::observability::video_backend::VideoBackendMetricKind::NativeWaylandSurfacePresented,
            );
        }
    }

    pub(crate) fn prepare_linear_native_wayland_frame(
        &mut self,
        frame: &crate::video::VideoFrame,
        drm_syncobj: bool,
        explicit_sync: bool,
    ) -> Option<crate::video::VideoFrame> {
        let crate::video::VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } = &frame.format
        else {
            return None;
        };
        let mut interop = match self.native_dmabuf_interop.take() {
            Some(interop) => interop,
            None => super::native_dmabuf_interop::NativeDmaBufInterop::new(&self.ctx).ok()?,
        };
        let sync_start = std::time::Instant::now();
        let copied = interop.copy_nv12_to_linear_wayland(
            &self.ctx,
            descriptor,
            frame.session_id,
            frame.storage.clone(),
            frame.width,
            frame.height,
            drm_syncobj,
            explicit_sync,
        );
        if !drm_syncobj
            && !explicit_sync
            && let Some(metrics) = &self.metrics
        {
            metrics.record_native_sync_blocked(sync_start.elapsed());
        }
        self.native_dmabuf_interop = Some(interop);
        let (descriptor, cache_hit) = match copied {
            Ok(Some(copied)) => copied,
            Ok(None) => return None,
            Err(error) => {
                self.record_native_import_failure(format!(
                    "linear Wayland bridge failed: {error:#}"
                ));
                return None;
            }
        };
        self.record_native_import(descriptor.surface_id, cache_hit);
        Some(crate::video::VideoFrame {
            storage: frame.storage.clone(),
            width: frame.width,
            height: frame.height,
            stride: u32::try_from(descriptor.planes[0].pitch).ok()?,
            format: crate::video::VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor },
            session_id: frame.session_id,
            pts_ns: frame.pts_ns,
            duration_ns: frame.duration_ns,
            color: frame.color,
            geometry: frame.geometry,
        })
    }

    pub(super) fn upload_frame_native_dmabuf_nv12(
        &mut self,
        video_frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        descriptor: &crate::video::NativeDmaBufNv12,
        origin: super::YuvOrigin,
    ) -> bool {
        let staging_created = self.prepare_native_nv12_staging(width, height);
        if staging_created {
            // Prime WGPU's texture tracker once. Raw Vulkan then preserves a
            // stable SHADER_READ_ONLY ↔ TRANSFER_DST layout contract.
            self.render_nv12_to_rgba(output, "Native DMA-BUF Staging Prime");
        }

        let mut interop = match self.native_dmabuf_interop.take() {
            Some(interop) => interop,
            None => match super::native_dmabuf_interop::NativeDmaBufInterop::new(&self.ctx) {
                Ok(interop) => interop,
                Err(error) => {
                    self.record_native_import_failure(format!(
                        "cannot initialize DMA-BUF import: {error:#}"
                    ));
                    return false;
                }
            },
        };
        let copy_result = interop.copy_nv12_to_staging(
            &self.ctx,
            descriptor,
            video_frame.session_id,
            video_frame.storage.clone(),
            self.nv12_y_texture
                .as_ref()
                .expect("native luma texture created above"),
            self.nv12_uv_texture
                .as_ref()
                .expect("native UV texture created above"),
            width,
            height,
        );
        self.native_dmabuf_interop = Some(interop);
        match copy_result {
            Ok(cache_hit) => self.record_dmabuf_import(descriptor.surface_id, cache_hit, origin),
            Err(error) => {
                self.record_native_import_failure(format!(
                    "decoder-surface GPU copy failed: {error:#}"
                ));
                return false;
            }
        }

        let direct = !self.transition_active
            && !self.content_swap_pending
            && self.current_texture_size.is_some();
        if direct {
            self.active_yuv_source = Some(super::YuvSource {
                format: super::YuvFormat::Nv12,
                origin,
                width,
                height,
            });
        } else {
            self.render_nv12_to_rgba(output, "Native DMA-BUF NV12 Convert");
        }
        true
    }

    fn prepare_native_nv12_staging(&mut self, width: u32, height: u32) -> bool {
        if self.nv12_staging_size == Some((width, height)) {
            return false;
        }
        self.release_nv12_staging("native DMA-BUF dimensions changed");
        let (uv_width, uv_height) = chroma_plane_extent(width, height);
        let y_texture = self.create_native_plane_texture(
            "Native DMA-BUF Persistent Y Plane",
            width,
            height,
            wgpu::TextureFormat::R8Unorm,
        );
        let uv_texture = self.create_native_plane_texture(
            "Native DMA-BUF Persistent UV Plane",
            uv_width,
            uv_height,
            wgpu::TextureFormat::Rg8Unorm,
        );
        self.nv12_y_view = Some(y_texture.create_view(&wgpu::TextureViewDescriptor::default()));
        self.nv12_uv_view = Some(uv_texture.create_view(&wgpu::TextureViewDescriptor::default()));
        self.nv12_y_texture = Some(y_texture);
        self.nv12_uv_texture = Some(uv_texture);
        self.nv12_staging_size = Some((width, height));
        self.create_nv12_bind_groups();
        true
    }

    fn create_native_plane_texture(
        &self,
        label: &'static str,
        width: u32,
        height: u32,
        format: wgpu::TextureFormat,
    ) -> wgpu::Texture {
        self.ctx.device.create_texture(&wgpu::TextureDescriptor {
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
            usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
            view_formats: &[],
        })
    }

    pub(super) fn create_nv12_bind_groups(&mut self) {
        self.native_nv12_bind_group = Some(self.ctx.device.create_bind_group(
            &wgpu::BindGroupDescriptor {
                label: Some("Native DMA-BUF NV12 Convert Bind Group"),
                layout: &self.ctx.nv12_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(
                            self.nv12_y_view.as_ref().expect("native luma view exists"),
                        ),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(
                            self.nv12_uv_view.as_ref().expect("native UV view exists"),
                        ),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            },
        ));
        self.final_nv12_bind_group = Some(Arc::new(self.ctx.device.create_bind_group(
            &wgpu::BindGroupDescriptor {
                label: Some("Native DMA-BUF NV12 Final Blit Bind Group"),
                layout: &self.ctx.native_nv12_blit_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: self.yuv_uniform_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(
                            self.nv12_y_view.as_ref().expect("native luma view exists"),
                        ),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::TextureView(
                            self.nv12_uv_view.as_ref().expect("native UV view exists"),
                        ),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            },
        )));
    }

    pub(super) fn snapshot_yuv_for_transition(&mut self) {
        // Steady native presentation bypasses WGPU, so its staging planes may
        // contain an old transition frame and its RGBA texture may be absent.
        // Import the retained, last-presented frame before tearing down video.
        if let Some(frame) = self.native_wayland_snapshot_frame.take()
            && let crate::video::VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } =
                &frame.format
        {
            let (width, height) = super::compute_cover_target_dimensions(
                frame.width,
                frame.height,
                self.config.width.max(1),
                self.config.height.max(1),
            );
            let output = self.ctx.get_texture_from_pool(
                width,
                height,
                wgpu::TextureUsages::TEXTURE_BINDING
                    | wgpu::TextureUsages::COPY_DST
                    | wgpu::TextureUsages::RENDER_ATTACHMENT,
                self.metrics.as_deref(),
            );
            self.write_yuv_uniforms(frame.color, frame.geometry);
            if self.upload_frame_native_dmabuf_nv12(
                &frame,
                &output,
                frame.width,
                frame.height,
                descriptor,
                super::YuvOrigin::NativeDmaBuf,
            ) {
                self.render_nv12_to_rgba(&output, "Native Last-Presented Snapshot");
                if let Some(old) = self.current_texture.take() {
                    let (old_width, old_height) = (old.width(), old.height());
                    self.ctx.return_texture_to_pool(old, old_width, old_height);
                }
                self.current_texture_view =
                    Some(output.create_view(&wgpu::TextureViewDescriptor {
                        label: Some("Native Outgoing Snapshot View"),
                        format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                        ..Default::default()
                    }));
                self.current_texture = Some(output);
                self.current_texture_size = Some((width, height));
                self.current_aspect = width as f32 / height as f32;
                self.active_yuv_source = None;
                return;
            }
            // Import failure must never promote an uninitialized pool texture.
            self.ctx.return_texture_to_pool(output, width, height);
        }
        let Some(source) = self.active_yuv_source else {
            return;
        };
        let width = self.config.width.max(1);
        let height = self.config.height.max(1);
        let texture = self.ctx.get_texture_from_pool(
            width,
            height,
            wgpu::TextureUsages::TEXTURE_BINDING
                | wgpu::TextureUsages::COPY_DST
                | wgpu::TextureUsages::RENDER_ATTACHMENT,
            self.metrics.as_deref(),
        );
        match source.format {
            super::YuvFormat::Nv12 => self.render_nv12_to_rgba(&texture, "NV12 Outgoing Snapshot"),
            super::YuvFormat::P010 => self.render_p010_to_rgba(&texture),
            super::YuvFormat::I420 => {
                self.render_i420_final_to_rgba(&texture, "I420 Outgoing Snapshot")
            }
        }
        self.current_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor {
            label: Some("YUV Outgoing Snapshot View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        }));
        if let Some(old) = self.current_texture.replace(texture) {
            let (old_width, old_height) = (old.width(), old.height());
            self.ctx.return_texture_to_pool(old, old_width, old_height);
        }
        self.current_texture_size = Some((width, height));
        self.current_aspect = width as f32 / height as f32;
        self.active_yuv_source = None;
    }

    fn render_i420_final_to_rgba(&self, output: &wgpu::Texture, label: &'static str) {
        let Some(bind_group) = self.final_i420_bind_group.as_ref() else {
            return;
        };
        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some(label),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });
        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor { label: Some(label) });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some(label),
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
            let pipeline = self
                .ctx
                .get_final_i420_blit_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb);
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, bind_group.as_ref(), &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.submit(std::iter::once(encoder.finish()));
    }

    fn record_native_import(&self, surface_id: u64, cache_hit: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.record_video_backend_metric(if cache_hit {
                crate::observability::video_backend::VideoBackendMetricKind::NativeImportCacheHit
            } else {
                crate::observability::video_backend::VideoBackendMetricKind::NativeImportCacheMiss
            });
            metrics.record_video_backend_metric(
                crate::observability::video_backend::VideoBackendMetricKind::NativeGpuCopy,
            );
        }
        if !cache_hit {
            info!(
                "[NATIVE-IMPORT] {}: cached decoder surface={} modifier-aware Vulkan import",
                self.name, surface_id
            );
        }
    }

    fn record_dmabuf_import(&self, surface_id: u64, cache_hit: bool, origin: super::YuvOrigin) {
        if origin == super::YuvOrigin::NativeDmaBuf {
            self.record_native_import(surface_id, cache_hit);
            return;
        }
        if let Some(metrics) = &self.metrics {
            use crate::observability::video_backend::VideoBackendMetricKind;
            metrics.record_video_backend_metric(if cache_hit {
                VideoBackendMetricKind::AppsinkDmaBufImportCacheHit
            } else {
                VideoBackendMetricKind::AppsinkDmaBufImportCacheMiss
            });
            metrics.record_video_backend_metric(VideoBackendMetricKind::AppsinkDmaBufGpuCopy);
        }
        if !cache_hit {
            info!(
                "[GST-DMABUF] {}: cached allocation surface={} modifier-aware Vulkan import",
                self.name, surface_id
            );
        }
    }

    fn record_native_import_failure(&self, detail: String) {
        if let Some(metrics) = &self.metrics {
            metrics.record_video_backend_metric(
                crate::observability::video_backend::VideoBackendMetricKind::NativeImportError,
            );
        }
        if crate::video::report_native_surface_import_failure() {
            warn!(
                "[NATIVE-PATH] {}: {detail}; degrading once to hardware-transfer for subsequent frames",
                self.name
            );
        } else {
            tracing::debug!(
                "[NATIVE-PATH] {}: surface import already disabled after an earlier failure",
                self.name
            );
        }
    }

    pub(super) fn render_nv12_to_rgba(&self, output: &wgpu::Texture, label: &'static str) {
        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some(label),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });
        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor { label: Some(label) });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some(label),
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
            pass.set_bind_group(
                0,
                self.native_nv12_bind_group
                    .as_ref()
                    .expect("native NV12 bind group exists"),
                &[],
            );
            pass.draw(0..3, 0..1);
        }
        self.ctx.submit(std::iter::once(encoder.finish()));
    }
}

fn native_gl_surface_enabled() -> bool {
    parse_native_gl_surface_enabled(std::env::var("KLD_NATIVE_GL_SURFACE").ok().as_deref())
}

fn parse_native_gl_surface_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

#[cfg(test)]
mod native_gl_surface_tests {
    use super::parse_native_gl_surface_enabled;

    #[test]
    fn native_gl_surface_is_opt_in() {
        assert!(!parse_native_gl_surface_enabled(None));
        assert!(!parse_native_gl_surface_enabled(Some("false")));
        assert!(parse_native_gl_surface_enabled(Some("true")));
    }
}
