use tracing::error;

impl super::Renderer {
    pub(super) fn set_current_gl_external_rgba(
        &mut self,
        frame: &crate::video::GlExternalFrame,
        width: u32,
        height: u32,
    ) {
        let old_frame = self.current_external_frame.take();
        if let Err(error) = frame.prepare_for_wgpu_releasing(old_frame.as_ref()) {
            error!(
                "[MPV-GL] {}: failed to acquire shared frame for WGPU: {error:#}",
                self.name
            );
            self.current_external_frame = old_frame;
            return;
        }
        if let Some(curr) = self.current_texture.take()
            && let Some((w, h)) = self.current_texture_size.take()
        {
            self.ctx.return_texture_to_pool(curr, w, h);
        }
        self.current_texture_view = None;
        self.current_external_view = Some(frame.view());
        self.current_external_frame = Some(frame.clone());
        drop(old_frame);
        self.current_texture_size = Some((width, height));
        self.transition_bind_group = None;
        self.blit_bind_group = None;
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
            if let Err(error) = frame.storage.with_readable_bytes(|src_data| {
                self.ctx.write_texture(
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
            }) {
                error!("Failed to read RGBA video buffer: {}", error);
            }
        } else {
            let align_mask = 255u32;
            let aligned_stride = (expected_stride + align_mask) & !align_mask;
            let required_size = (aligned_stride * height) as usize;

            self.stride_temp_buffer.clear();
            if self.stride_temp_buffer.capacity() < required_size {
                self.stride_temp_buffer
                    .reserve(required_size - self.stride_temp_buffer.capacity());
            }

            let repacked = frame.storage.with_readable_bytes(|src_data| {
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
            });
            if let Err(error) = repacked {
                error!("Failed to read RGBA video buffer: {}", error);
                return;
            }

            self.ctx.write_texture(
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
