use super::video_layout::chroma_plane_extent;
use tracing::error;

impl super::Renderer {
    pub(super) fn upload_plane_texture(
        queue: &wgpu::Queue,
        stride_temp_buffer: &mut Vec<u8>,
        renderer_name: &str,
        texture: &wgpu::Texture,
        src: &[u8],
        src_offset: usize,
        src_stride: u32,
        copy_width: u32,
        copy_height: u32,
        bytes_per_pixel: u32,
        plane_name: &str,
    ) -> bool {
        if copy_width == 0 || copy_height == 0 {
            return true;
        }

        let bytes_per_row = copy_width.saturating_mul(bytes_per_pixel);
        let aligned_bytes_per_row = (bytes_per_row + 255) & !255;

        if src_stride.is_multiple_of(256) && src_stride >= bytes_per_row {
            let end = src_offset.saturating_add((src_stride * copy_height) as usize);
            if end > src.len() {
                error!(
                    "[RENDERER] {} {} plane truncated upload (expected {} bytes, have {})",
                    renderer_name,
                    plane_name,
                    end,
                    src.len()
                );
                return false;
            }

            queue.write_texture(
                wgpu::ImageCopyTexture {
                    texture,
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                &src[src_offset..end],
                wgpu::ImageDataLayout {
                    offset: 0,
                    bytes_per_row: Some(src_stride),
                    rows_per_image: Some(copy_height),
                },
                wgpu::Extent3d {
                    width: copy_width,
                    height: copy_height,
                    depth_or_array_layers: 1,
                },
            );
            return true;
        }

        stride_temp_buffer.clear();
        let required = (aligned_bytes_per_row * copy_height) as usize;
        if stride_temp_buffer.capacity() < required {
            stride_temp_buffer.reserve(required - stride_temp_buffer.capacity());
        }

        for row in 0..copy_height {
            let start = src_offset + (row * src_stride) as usize;
            let end = start + bytes_per_row as usize;
            if end > src.len() {
                error!(
                    "[RENDERER] {} {} plane truncated row (expected {} bytes, have {})",
                    renderer_name,
                    plane_name,
                    end,
                    src.len()
                );
                return false;
            }
            stride_temp_buffer.extend_from_slice(&src[start..end]);
            let pad = aligned_bytes_per_row - bytes_per_row;
            if pad > 0 {
                stride_temp_buffer.extend(std::iter::repeat_n(0u8, pad as usize));
            }
        }

        queue.write_texture(
            wgpu::ImageCopyTexture {
                texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            stride_temp_buffer,
            wgpu::ImageDataLayout {
                offset: 0,
                bytes_per_row: Some(aligned_bytes_per_row),
                rows_per_image: Some(copy_height),
            },
            wgpu::Extent3d {
                width: copy_width,
                height: copy_height,
                depth_or_array_layers: 1,
            },
        );

        true
    }

    /// Upload an NV12 frame: write Y and UV planes to staging textures, then
    /// run the NV12→RGBA conversion render pass into the output texture.
    pub(super) fn upload_frame_nv12(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    ) {
        let (uv_width, uv_height) = chroma_plane_extent(width, height);

        if self.nv12_staging_size != Some((width, height)) {
            self.nv12_y_view = None;
            self.nv12_uv_view = None;

            let y_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("NV12 Y Plane"),
                size: wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            let uv_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("NV12 UV Plane"),
                size: wgpu::Extent3d {
                    width: uv_width,
                    height: uv_height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rg8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });

            self.nv12_y_view = Some(y_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.nv12_uv_view = Some(uv_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.nv12_y_texture = Some(y_tex);
            self.nv12_uv_texture = Some(uv_tex);
            self.nv12_staging_size = Some((width, height));
        }

        let y_tex = self.nv12_y_texture.as_ref().unwrap();
        let uv_tex = self.nv12_uv_texture.as_ref().unwrap();

        {
            let map = match frame.buffer.map_readable() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to map NV12 video buffer: {}", e);
                    return;
                }
            };
            let src = map.as_slice();

            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                y_tex,
                src,
                0,
                y_stride,
                width,
                height,
                1,
                "NV12 Y",
            ) {
                return;
            }
            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                uv_tex,
                src,
                uv_offset as usize,
                uv_stride,
                uv_width,
                uv_height,
                2,
                "NV12 UV",
            ) {
                return;
            }
        }

        let y_view = self.nv12_y_view.as_ref().unwrap();
        let uv_view = self.nv12_uv_view.as_ref().unwrap();

        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("NV12 Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        let bind_group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("NV12 Convert Bind Group"),
                layout: &self.ctx.nv12_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(uv_view),
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
                label: Some("NV12 Convert Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("NV12 Convert Pass"),
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
    }

    /// Upload an I420 frame: write Y/U/V planes to staging textures, then
    /// run the I420→RGBA conversion render pass into the output texture.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn upload_frame_i420(
        &mut self,
        frame: &crate::video::VideoFrame,
        output: &wgpu::Texture,
        width: u32,
        height: u32,
        y_stride: u32,
        u_offset: u32,
        u_stride: u32,
        v_offset: u32,
        v_stride: u32,
    ) {
        let (chroma_width, chroma_height) = chroma_plane_extent(width, height);

        if self.i420_staging_size != Some((width, height)) {
            self.i420_y_view = None;
            self.i420_u_view = None;
            self.i420_v_view = None;

            let y_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("I420 Y Plane"),
                size: wgpu::Extent3d {
                    width,
                    height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            let u_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("I420 U Plane"),
                size: wgpu::Extent3d {
                    width: chroma_width,
                    height: chroma_height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });
            let v_tex = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
                label: Some("I420 V Plane"),
                size: wgpu::Extent3d {
                    width: chroma_width,
                    height: chroma_height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::R8Unorm,
                usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
                view_formats: &[],
            });

            self.i420_y_view = Some(y_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.i420_u_view = Some(u_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.i420_v_view = Some(v_tex.create_view(&wgpu::TextureViewDescriptor::default()));
            self.i420_y_texture = Some(y_tex);
            self.i420_u_texture = Some(u_tex);
            self.i420_v_texture = Some(v_tex);
            self.i420_staging_size = Some((width, height));
        }

        let y_tex = self.i420_y_texture.as_ref().unwrap();
        let u_tex = self.i420_u_texture.as_ref().unwrap();
        let v_tex = self.i420_v_texture.as_ref().unwrap();

        {
            let map = match frame.buffer.map_readable() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to map I420 video buffer: {}", e);
                    return;
                }
            };
            let src = map.as_slice();

            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                y_tex,
                src,
                0,
                y_stride,
                width,
                height,
                1,
                "I420 Y",
            ) {
                return;
            }
            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                u_tex,
                src,
                u_offset as usize,
                u_stride,
                chroma_width,
                chroma_height,
                1,
                "I420 U",
            ) {
                return;
            }
            if !Self::upload_plane_texture(
                &self.ctx.queue,
                &mut self.stride_temp_buffer,
                self.name.as_str(),
                v_tex,
                src,
                v_offset as usize,
                v_stride,
                chroma_width,
                chroma_height,
                1,
                "I420 V",
            ) {
                return;
            }
        }

        let y_view = self.i420_y_view.as_ref().unwrap();
        let u_view = self.i420_u_view.as_ref().unwrap();
        let v_view = self.i420_v_view.as_ref().unwrap();

        let output_view = output.create_view(&wgpu::TextureViewDescriptor {
            label: Some("I420 Convert Output View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            ..Default::default()
        });

        let bind_group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("I420 Convert Bind Group"),
                layout: &self.ctx.i420_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(u_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::TextureView(v_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            });

        let mut encoder = self
            .ctx
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("I420 Convert Encoder"),
            });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("I420 Convert Pass"),
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
            pass.set_pipeline(&self.ctx.i420_pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.queue.submit(std::iter::once(encoder.finish()));
    }
}
