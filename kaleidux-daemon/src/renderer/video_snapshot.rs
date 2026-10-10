impl super::Renderer {
    pub(super) fn snapshot_yuv_for_transition(&mut self) {
        // Steady native presentation bypasses WGPU, so its staging planes may
        // contain an old transition frame and its RGBA texture may be absent.
        // Import the retained, last-presented frame before tearing down video.
        if let Some(frame) = self.native_wayland_snapshot_frame.take()
            && let crate::video::VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } =
                &frame.format
        {
            let (width, height) = (self.config.width.max(1), self.config.height.max(1));
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
                Some(&output),
                frame.width,
                frame.height,
                descriptor,
                super::YuvOrigin::NativeDmaBuf,
            ) {
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
            super::YuvFormat::Nv12 => {
                self.render_final_yuv_to_rgba(&texture, source.format, "NV12 Outgoing Snapshot")
            }
            super::YuvFormat::P010 => self.render_p010_to_rgba(&texture),
            super::YuvFormat::I420 => {
                self.render_final_yuv_to_rgba(&texture, source.format, "I420 Outgoing Snapshot")
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

    fn render_final_yuv_to_rgba(
        &self,
        output: &wgpu::Texture,
        format: super::YuvFormat,
        label: &'static str,
    ) {
        let Some(bind_group) = (match format {
            super::YuvFormat::Nv12 => self.final_nv12_bind_group.as_ref(),
            super::YuvFormat::I420 => self.final_i420_bind_group.as_ref(),
            super::YuvFormat::P010 => self.final_p010_bind_group.as_ref(),
        }) else {
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
            let pipeline = if matches!(format, super::YuvFormat::I420) {
                self.ctx
                    .get_final_i420_blit_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb)
            } else {
                self.ctx
                    .get_native_nv12_blit_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb)
            };
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, bind_group.as_ref(), &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.submit(std::iter::once(encoder.finish()));
    }
}
