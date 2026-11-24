//! Loads bounded PNG reveal fields and binds them to a dedicated render pipeline.

use super::{Renderer, WgpuContext};
use anyhow::{Context, Result, ensure};
use std::fs;
use std::io::BufReader;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, Weak};

const MAX_MAP_EDGE: u32 = 1024;
const MAX_MAP_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct RevealMap {
    width: u32,
    height: u32,
    channels: Vec<u8>,
    cached_binding: parking_lot::Mutex<Option<(Weak<WgpuContext>, Arc<wgpu::BindGroup>)>>,
}

impl RevealMap {
    pub(crate) fn load_png(path: &Path) -> Result<Self> {
        // Nonblocking open lets us reject a FIFO before it can tie up a worker.
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .context("Opening reveal map")?;
        let meta = file.metadata()?;
        ensure!(meta.is_file(), "Reveal map must be a regular PNG file");
        ensure!(meta.len() <= MAX_MAP_BYTES, "Reveal map exceeds 8 MiB");
        // Take an immutable, size-bounded snapshot before reading the PNG.
        let mut data = Vec::with_capacity(meta.len() as usize);
        use std::io::Read;
        file.take(MAX_MAP_BYTES + 1).read_to_end(&mut data)?;
        ensure!(
            data.len() as u64 <= MAX_MAP_BYTES,
            "Reveal map exceeds 8 MiB"
        );

        let mut reader = image::ImageReader::with_format(
            BufReader::new(std::io::Cursor::new(data)),
            image::ImageFormat::Png,
        );
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(MAX_MAP_EDGE);
        limits.max_image_height = Some(MAX_MAP_EDGE);
        limits.max_alloc = Some(MAX_MAP_BYTES);
        reader.limits(limits);
        let decoded = reader.decode().context("Decoding reveal map PNG")?;
        let gray = matches!(
            decoded.color(),
            image::ColorType::L8
                | image::ColorType::La8
                | image::ColorType::L16
                | image::ColorType::La16
        );
        let pixels = decoded.into_rgba8();
        let mut channels = Vec::with_capacity(pixels.as_raw().len() / 2);
        for pixel in pixels.pixels() {
            channels.extend_from_slice(&[pixel.0[0], if gray { 0 } else { pixel.0[1] }]);
        }
        Ok(Self {
            width: pixels.width(),
            height: pixels.height(),
            channels,
            cached_binding: parking_lot::Mutex::new(None),
        })
    }
}

pub(super) struct RevealPipeline {
    layout: wgpu::BindGroupLayout,
    render: wgpu::RenderPipeline,
}

impl RevealPipeline {
    fn new(ctx: &WgpuContext) -> Self {
        let layout = ctx
            .device
            .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("Reveal Map Layout"),
                entries: &[wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Texture {
                        sample_type: wgpu::TextureSampleType::Float { filterable: true },
                        view_dimension: wgpu::TextureViewDimension::D2,
                        multisampled: false,
                    },
                    count: None,
                }],
            });
        let pipeline_layout = ctx
            .device
            .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                label: Some("Reveal Transition Layout"),
                bind_group_layouts: &[&ctx.transition_bind_group_layout, &layout],
                push_constant_ranges: &[],
            });
        let vertex = ctx
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("Reveal Transition Vertex"),
                source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/quad.wgsl").into()),
            });
        let fragment = ctx
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("Reveal Transition Fragment"),
                source: wgpu::ShaderSource::Wgsl(include_str!("../shaders/reveal_map.wgsl").into()),
            });
        let render = ctx
            .device
            .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
                label: Some("Reveal Transition"),
                layout: Some(&pipeline_layout),
                vertex: wgpu::VertexState {
                    module: &vertex,
                    entry_point: Some("vs_main"),
                    compilation_options: Default::default(),
                    buffers: &[],
                },
                fragment: Some(wgpu::FragmentState {
                    module: &fragment,
                    entry_point: Some("fs_main"),
                    compilation_options: Default::default(),
                    targets: &[Some(wgpu::ColorTargetState {
                        format: wgpu::TextureFormat::Rgba8UnormSrgb,
                        blend: Some(wgpu::BlendState::REPLACE),
                        write_mask: wgpu::ColorWrites::ALL,
                    })],
                }),
                primitive: Default::default(),
                depth_stencil: None,
                multisample: Default::default(),
                multiview: None,
                cache: ctx.pipeline_cache.as_ref(),
            });
        Self { layout, render }
    }

    pub(super) fn render(&self) -> &wgpu::RenderPipeline {
        &self.render
    }
}

impl Renderer {
    pub(super) fn set_reveal_map(&mut self, map: &RevealMap) {
        self.reveal_bind_group = Some(self.upload_reveal_map(map));
    }

    fn upload_reveal_map(&self, map: &RevealMap) -> Arc<wgpu::BindGroup> {
        let ctx = &self.ctx;
        let context = Arc::downgrade(ctx);
        let mut cache = map.cached_binding.lock();
        if let Some((owner, binding)) = cache.as_ref()
            && Weak::ptr_eq(owner, &context)
        {
            return binding.clone();
        }
        let pipeline = ctx.reveal_pipeline.get_or_init(|| RevealPipeline::new(ctx));
        let size = wgpu::Extent3d {
            width: map.width,
            height: map.height,
            depth_or_array_layers: 1,
        };
        let texture = ctx.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("Reveal Map"),
            size,
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rg8Unorm,
            usage: wgpu::TextureUsages::COPY_DST | wgpu::TextureUsages::TEXTURE_BINDING,
            view_formats: &[],
        });
        ctx.write_texture(
            texture.as_image_copy(),
            &map.channels,
            wgpu::ImageDataLayout {
                offset: 0,
                bytes_per_row: Some(map.width * 2),
                rows_per_image: Some(map.height),
            },
            size,
        );
        let view = texture.create_view(&Default::default());
        let binding = Arc::new(ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Reveal Map"),
            layout: &pipeline.layout,
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: wgpu::BindingResource::TextureView(&view),
            }],
        }));
        *cache = Some((context, binding.clone()));
        binding
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grayscale_has_no_highlight_and_rgb_preserves_both_channels() {
        let path = std::env::temp_dir().join(format!("kld-reveal-{}.png", rand::random::<u64>()));
        image::GrayImage::from_pixel(1, 1, image::Luma([128]))
            .save(&path)
            .unwrap();
        let gray = RevealMap::load_png(&path).unwrap();
        image::RgbImage::from_pixel(1, 1, image::Rgb([15, 49, 255]))
            .save(&path)
            .unwrap();
        let colour = RevealMap::load_png(&path).unwrap();
        fs::remove_file(path).unwrap();
        assert_eq!(gray.channels, [128, 0]);
        assert_eq!(colour.channels, [15, 49]);
    }

    #[test]
    fn oversized_png_is_rejected_before_pixel_allocation() {
        let path = std::env::temp_dir().join(format!("kld-reveal-{}.png", rand::random::<u64>()));
        image::GrayImage::new(MAX_MAP_EDGE + 1, 1)
            .save(&path)
            .unwrap();
        let result = RevealMap::load_png(&path);
        fs::remove_file(path).unwrap();
        assert!(result.is_err());
    }
}
