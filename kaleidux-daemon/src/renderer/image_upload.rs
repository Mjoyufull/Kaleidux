use tracing::info;

impl super::Renderer {
    #[allow(dead_code)]
    pub fn upload_image_file(&mut self, path: &std::path::Path) -> anyhow::Result<()> {
        let _load_start = std::time::Instant::now();
        let img = image::open(path)?;
        let rgba = img.to_rgba8();
        let (width, height) = rgba.dimensions();
        let data = rgba.into_raw();

        self.upload_image_data(&data, width, height)
    }

    pub fn upload_image_data(
        &mut self,
        data: &[u8],
        width: u32,
        height: u32,
    ) -> anyhow::Result<()> {
        let upload_start = std::time::Instant::now();

        self.begin_content_swap();

        // If an image upload arrives without a pending swap, replace the current
        // texture directly to avoid keeping unused image resources alive.
        if self.current_texture.is_some() {
            self.current_texture_view = None;
            if let Some(curr) = self.current_texture.take() {
                if let Some((w, h)) = self.current_texture_size.take() {
                    self.ctx.return_texture_to_pool(curr, w, h);
                }
            }
        }

        // Static images are pre-sized close to the output in the CPU prep path, so
        // full mip chains mostly add upload cost and idle GPU memory without helping
        // transition correctness. Keep them single-mip so they can reuse the texture pool.
        let texture = self.ctx.get_texture_from_pool(
            width,
            height,
            wgpu::TextureUsages::TEXTURE_BINDING
                | wgpu::TextureUsages::COPY_DST
                | wgpu::TextureUsages::RENDER_ATTACHMENT,
            self.metrics.as_deref(),
        );

        self.ctx.queue.write_texture(
            wgpu::ImageCopyTexture {
                texture: &texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            data,
            wgpu::ImageDataLayout {
                offset: 0,
                bytes_per_row: Some(4 * width),
                rows_per_image: Some(height),
            },
            wgpu::Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
        );

        self.current_texture_view = Some(texture.create_view(&wgpu::TextureViewDescriptor {
            label: Some("Image Texture View"),
            format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
            dimension: Some(wgpu::TextureViewDimension::D2),
            aspect: wgpu::TextureAspect::All,
            base_mip_level: 0,
            mip_level_count: Some(1),
            base_array_layer: 0,
            array_layer_count: None,
        }));

        self.current_texture = Some(texture);
        self.current_aspect = width as f32 / height as f32;
        self.current_texture_size = Some((width, height));
        self.last_video_source_size = None;
        self.last_video_presentation_size = None;
        self.needs_redraw = true;
        self.valid_content_type = crate::queue::ContentType::Image;
        self.transition_bind_group = None;
        self.blit_bind_group = None;
        self.blit_source_is_composition = false;
        self.blit_source_is_prev = false;

        if self.prev_texture.is_some() {
            self.transition_start_time = None;
            self.transition_progress = 0.0;
            self.transition_active = true;
            // P-32: Reset batch_start_time so transition timer starts fresh from
            // actual upload time, preventing decode latency from eating into duration
            self.batch_start_time = None;
            self.prewarm_transition_resources();
            info!(
                "[TRANSITION] {}: Image data uploaded - transition will start on next render frame",
                self.name
            );
        } else {
            self.transition_active = false;
            self.transition_progress = 1.0;
            self.transition_just_completed = true; // Signal completion for instant switch
            self.arm_display_timer_on_present();
            info!(
                "[TRANSITION] {}: Image data uploaded (Instant) - transition signaled as complete",
                self.name
            );
        }

        let _duration = upload_start.elapsed();
        // Removed TRACE log - use debug! if needed for troubleshooting

        Ok(())
    }
}
