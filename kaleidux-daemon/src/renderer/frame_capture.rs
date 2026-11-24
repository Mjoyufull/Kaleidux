//! Copies a single base wallpaper frame to a bounded readback buffer on request.

use super::{Renderer, TransitionUniforms, WgpuContext, YuvFormat};
use anyhow::{Context, Result};
use image::ImageEncoder;
use std::{sync::Arc, time::Duration};
use wgpu::util::DeviceExt;

const FORMAT: wgpu::TextureFormat = wgpu::TextureFormat::Rgba8UnormSrgb;
const LABEL: &str = "Wallpaper frame capture";

pub(crate) struct FrameReadback {
    context: Arc<WgpuContext>,
    buffer: wgpu::Buffer,
    size: wgpu::Extent3d,
    row_pitch: u32,
    ready: tokio::sync::oneshot::Receiver<Result<(), wgpu::BufferAsyncError>>,
}

impl Renderer {
    pub(crate) fn capture_wallpaper_frame(&mut self, max_size: u32) -> Result<FrameReadback> {
        anyhow::ensure!(
            !self.content_is_loading() && self.configured,
            "Wallpaper is still loading"
        );
        let (width, height) = self.content_size();
        let (capture_width, capture_height) = scaled_dimensions(width, height, max_size);
        let size = wgpu::Extent3d {
            width: capture_width,
            height: capture_height,
            depth_or_array_layers: 1,
        };
        let texture = self.ctx.device.create_texture(&wgpu::TextureDescriptor {
            label: Some(LABEL),
            size,
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: FORMAT,
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT | wgpu::TextureUsages::COPY_SRC,
            view_formats: &[],
        });
        let row_pitch = (capture_width * 4).next_multiple_of(wgpu::COPY_BYTES_PER_ROW_ALIGNMENT);
        let buffer = self.ctx.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some(LABEL),
            size: u64::from(row_pitch) * u64::from(capture_height),
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });
        let mut encoder = self.ctx.device.create_command_encoder(&Default::default());
        if let Some(frame) = self.native_wayland_snapshot_frame.take() {
            let result = self.capture_retained_video(&frame, &texture);
            self.native_wayland_snapshot_frame = Some(frame);
            result?;
        } else {
            let (pipeline, group) = self.capture_source(width, height)?;
            draw_capture(&mut encoder, &texture, &pipeline, &group);
        }
        encoder.copy_texture_to_buffer(
            texture.as_image_copy(),
            wgpu::ImageCopyBuffer {
                buffer: &buffer,
                layout: wgpu::ImageDataLayout {
                    offset: 0,
                    bytes_per_row: Some(row_pitch),
                    rows_per_image: Some(capture_height),
                },
            },
            size,
        );
        self.ctx.submit([encoder.finish()]);
        let (notify, ready) = tokio::sync::oneshot::channel();
        buffer
            .slice(..)
            .map_async(wgpu::MapMode::Read, move |result| {
                let _ = notify.send(result);
            });
        Ok(FrameReadback {
            context: self.ctx.clone(),
            buffer,
            size,
            row_pitch,
            ready,
        })
    }

    fn capture_retained_video(
        &mut self,
        frame: &crate::video::VideoFrame,
        texture: &wgpu::Texture,
    ) -> Result<()> {
        let crate::video::VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } = &frame.format
        else {
            anyhow::bail!("Current native frame is not an NV12 DMA-BUF");
        };
        self.write_yuv_uniforms(frame.color, frame.geometry);
        self.stage_native_dmabuf_nv12(frame, Some(texture), frame.width, frame.height, descriptor)
            .context("Could not copy the current native video frame")?;
        self.render_final_yuv_to_rgba(texture, YuvFormat::Nv12, LABEL);
        Ok(())
    }

    fn capture_source(
        &self,
        width: u32,
        height: u32,
    ) -> Result<(Arc<wgpu::RenderPipeline>, Arc<wgpu::BindGroup>)> {
        if let Some(source) = self.active_yuv_source {
            let (pipeline, group) = match source.format {
                YuvFormat::Nv12 => (
                    self.ctx.get_native_nv12_blit_pipeline(FORMAT),
                    self.final_nv12_bind_group.as_ref(),
                ),
                YuvFormat::P010 => (
                    self.ctx.get_native_nv12_blit_pipeline(FORMAT),
                    self.final_p010_bind_group.as_ref(),
                ),
                YuvFormat::I420 => (
                    self.ctx.get_final_i420_blit_pipeline(FORMAT),
                    self.final_i420_bind_group.as_ref(),
                ),
            };
            return Ok((pipeline, group.context("No current YUV frame")?.clone()));
        }
        let view = self
            .current_texture_view
            .as_ref()
            .or(self.current_external_view.as_deref())
            .context("No current wallpaper frame")?;
        let uniforms = TransitionUniforms {
            progress: 1.0,
            screen_aspect: width as f32 / height as f32,
            prev_aspect: 1.0,
            next_aspect: self.current_aspect,
            params: [[0.0; 4]; 7],
            layout: self.layout_uniform(),
        };
        let uniform = self
            .ctx
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some(LABEL),
                contents: bytemuck::bytes_of(&uniforms),
                usage: wgpu::BufferUsages::UNIFORM,
            });
        let group = self
            .ctx
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some(LABEL),
                layout: &self.ctx.blit_bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: uniform.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: wgpu::BindingResource::Sampler(&self.sampler_linear),
                    },
                ],
            });
        Ok((self.ctx.get_blit_pipeline(FORMAT), Arc::new(group)))
    }
}

impl FrameReadback {
    pub(crate) async fn png(mut self) -> Result<Vec<u8>> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                self.context.device.poll(wgpu::Maintain::Poll);
                tokio::select! {
                    result = &mut self.ready => return result,
                    () = tokio::time::sleep(Duration::from_millis(4)) => {}
                }
            }
        })
        .await
        .context("GPU frame capture timed out")???;
        tokio::task::spawn_blocking(move || self.encode_png()).await?
    }

    fn encode_png(self) -> Result<Vec<u8>> {
        let mapped = self.buffer.slice(..).get_mapped_range();
        let row_bytes = (self.size.width * 4) as usize;
        let rgba = if self.row_pitch as usize == row_bytes {
            std::borrow::Cow::Borrowed(&*mapped)
        } else {
            let mut packed = Vec::with_capacity(row_bytes * self.size.height as usize);
            for row in mapped.chunks_exact(self.row_pitch as usize) {
                packed.extend_from_slice(&row[..row_bytes]);
            }
            std::borrow::Cow::Owned(packed)
        };
        let result = encode_rgba(&rgba, self.size.width, self.size.height);
        drop(rgba);
        drop(mapped);
        self.buffer.unmap();
        result
    }
}

fn encode_rgba(rgba: &[u8], width: u32, height: u32) -> Result<Vec<u8>> {
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new_with_quality(
        &mut png,
        image::codecs::png::CompressionType::Fast,
        image::codecs::png::FilterType::Adaptive,
    )
    .write_image(rgba, width, height, image::ExtendedColorType::Rgba8)?;
    Ok(png)
}

fn draw_capture(
    encoder: &mut wgpu::CommandEncoder,
    texture: &wgpu::Texture,
    pipeline: &wgpu::RenderPipeline,
    group: &wgpu::BindGroup,
) {
    let view = texture.create_view(&Default::default());
    let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
        label: Some(LABEL),
        color_attachments: &[Some(wgpu::RenderPassColorAttachment {
            view: &view,
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
    pass.set_pipeline(pipeline);
    pass.set_bind_group(0, group, &[]);
    pass.draw(0..3, 0..1);
}

fn scaled_dimensions(width: u32, height: u32, max_size: u32) -> (u32, u32) {
    let edge = width.max(height);
    if edge <= max_size {
        return (width, height);
    }
    let scale = |dimension| ((u64::from(dimension) * u64::from(max_size)) / u64::from(edge)) as u32;
    (scale(width).max(1), scale(height).max(1))
}
