//! Retains the displayed GPU resources while a candidate uses the same surface.
//! Ownership prevents texture-pool reuse until preview apply or cancel completes.

use std::sync::Arc;

pub(crate) struct FrozenWallpaper {
    texture: Option<wgpu::Texture>,
    external_view: Option<Arc<wgpu::TextureView>>,
    external_frame: Option<crate::video::GlExternalFrame>,
    aspect: f32,
    kind: crate::queue::ContentType,
    image_session: u64,
    video_session: u64,
    tiled_image: bool,
}

impl super::Renderer {
    pub(crate) fn content_is_loading(&self) -> bool {
        self.content_swap_pending
    }
    pub(crate) fn freeze_wallpaper(&mut self) -> FrozenWallpaper {
        self.snapshot_yuv_for_transition();
        self.abort_transition();
        self.snapshot_external_for_preview();
        let texture = self.current_texture.take();
        let external_view = self.current_external_view.take();
        let external_frame = self.current_external_frame.take();
        self.current_texture_view = None;
        self.current_texture_size = None;
        self.current_external_view = texture
            .as_ref()
            .map(|texture| {
                Arc::new(texture.create_view(&wgpu::TextureViewDescriptor {
                    format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                    ..Default::default()
                }))
            })
            .or_else(|| external_view.clone());
        self.blit_bind_group = None;
        FrozenWallpaper {
            texture,
            external_view,
            external_frame,
            aspect: self.current_aspect,
            kind: self.valid_content_type,
            image_session: self.active_image_session_id,
            video_session: self.active_video_session_id,
            tiled_image: self.tiled_image,
        }
    }

    pub(crate) fn restore_wallpaper(&mut self, frozen: FrozenWallpaper) {
        self.clear();
        self.current_texture = frozen.texture;
        self.current_texture_size = self
            .current_texture
            .as_ref()
            .map(|texture| (texture.width(), texture.height()));
        self.current_texture_view = self.current_texture.as_ref().map(|texture| {
            texture.create_view(&wgpu::TextureViewDescriptor {
                format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
                ..Default::default()
            })
        });
        self.current_external_view = frozen.external_view;
        self.current_external_frame = frozen.external_frame;
        self.current_aspect = frozen.aspect;
        self.valid_content_type = frozen.kind;
        self.active_image_session_id = frozen.image_session;
        self.active_video_session_id = frozen.video_session;
        self.tiled_image = frozen.tiled_image;
        self.prev_tiled_image = frozen.tiled_image;
        self.needs_redraw = true;
    }

    fn snapshot_external_for_preview(&mut self) {
        let Some(view) = self.current_external_view.clone() else {
            return;
        };
        let (width, height) = self.content_size();
        let texture = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("Preview original frame"),
            size: wgpu::Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8UnormSrgb,
            usage: wgpu::TextureUsages::TEXTURE_BINDING
                | wgpu::TextureUsages::RENDER_ATTACHMENT
                | wgpu::TextureUsages::COPY_DST
                | wgpu::TextureUsages::COPY_SRC,
            view_formats: &[],
        });
        self.write_uniforms_if_changed(super::TransitionUniforms {
            progress: 1.0,
            screen_aspect: width as f32 / height as f32,
            prev_aspect: self.current_aspect,
            next_aspect: self.current_aspect,
            params: [[0.0; 4]; 7],
            layout: self.layout_uniform(),
        });
        let group = self.build_blit_bind_group(view.as_ref(), "Preview snapshot source");
        let target = texture.create_view(&Default::default());
        let mut encoder = self.ctx.device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("Preview snapshot"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &target,
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
                .get_blit_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb);
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, group.as_ref(), &[]);
            pass.draw(0..3, 0..1);
        }
        self.ctx.submit(std::iter::once(encoder.finish()));
        self.current_external_view = None;
        let frame = self.current_external_frame.take();
        self.drop_external_frame(frame);
        self.current_texture = Some(texture);
        self.current_aspect = width as f32 / height as f32;
        self.tiled_image = false;
    }
}
