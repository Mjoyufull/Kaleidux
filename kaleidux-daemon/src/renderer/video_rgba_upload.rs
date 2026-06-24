use tracing::error;

impl super::Renderer {
    #[cfg(feature = "mpv-backend")]
    pub(super) fn set_current_gl_external_rgba(
        &mut self,
        frame: &crate::video::GlExternalFrame,
        width: u32,
        height: u32,
    ) {
        if let Some(curr) = self.current_texture.take()
            && let Some((w, h)) = self.current_texture_size.take()
        {
            self.ctx.return_texture_to_pool(curr, w, h);
        }
        self.current_texture_view = None;
        self.current_external_view =
            Some(frame.texture().create_view(&wgpu::TextureViewDescriptor {
                label: Some("libmpv GL Shared Current View"),
                format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                dimension: Some(wgpu::TextureViewDimension::D2),
                aspect: wgpu::TextureAspect::All,
                base_mip_level: 0,
                mip_level_count: None,
                base_array_layer: 0,
                array_layer_count: None,
            }));
        let old_frame = self.current_external_frame.replace(frame.clone());
        self.drop_external_frame(old_frame);
        self.current_texture_size = Some((width, height));
        self.transition_bind_group = None;
        self.blit_bind_group = None;
    }

    #[cfg(feature = "mpv-backend")]
    pub(super) fn upload_frame_gl_external_rgba(
        &mut self,
        frame: &crate::video::GlExternalFrame,
        output: &wgpu::Texture,
    ) {
        let source_view = frame
            .texture()
            .create_view(&wgpu::TextureViewDescriptor::default());
        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("libmpv GL Shared RGBA Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });
        let bind_group = self.build_blit_bind_group(&source_view, "libmpv GL Shared RGBA Blit");
        let pipeline = self
            .ctx
            .get_blit_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb);
        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("libmpv GL Shared RGBA Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("libmpv GL Shared RGBA Pass"),
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
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));
        frame.release_after_submit(&self.ctx.queue);
    }

    /// Upload an RGBA frame directly to the output texture (legacy fallback path).
    pub(super) fn upload_frame_rgba(
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
}
