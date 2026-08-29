use ash::vk;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use tracing::{error, info};
use wgpu::util::DeviceExt;

pub(crate) struct ExportedWgpuTexture {
    pub(crate) texture: Arc<wgpu::Texture>,
    pub(crate) view: Arc<wgpu::TextureView>,
    pub(crate) memory_fd: OwnedFd,
    pub(crate) memory_size: u64,
    pub(crate) gl_to_vulkan_fd: OwnedFd,
    pub(crate) vulkan_to_gl_fd: OwnedFd,
    pub(crate) sync: Arc<GlInteropSync>,
}

pub(crate) fn create_exportable_rgba_texture(
    ctx: &super::WgpuContext,
    width: u32,
    height: u32,
    label: &'static str,
) -> Option<ExportedWgpuTexture> {
    let exported = create_exportable_hal_texture(
        &ctx.device,
        width,
        height,
        wgpu::TextureFormat::Rgba8Unorm,
        label,
    )?;
    let descriptor = wgpu::TextureDescriptor {
        label: Some(label),
        size: wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu::TextureFormat::Rgba8Unorm,
        usage: wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[wgpu::TextureFormat::Rgba8UnormSrgb],
    };
    // SAFETY: the HAL texture was created from this live WGPU Vulkan device
    // and its descriptor matches the raw image exactly.
    let texture = unsafe {
        ctx.device
            .create_texture_from_hal::<wgpu_hal::vulkan::Api>(exported.texture, &descriptor)
    };
    let texture = Arc::new(texture);
    let view = Arc::new(texture.create_view(&wgpu::TextureViewDescriptor {
        label: Some("libmpv GL Shared RGBA View"),
        format: Some(wgpu::TextureFormat::Rgba8UnormSrgb),
        ..Default::default()
    }));
    Some(ExportedWgpuTexture {
        texture,
        view,
        memory_fd: exported.memory_fd,
        memory_size: exported.memory_size,
        gl_to_vulkan_fd: exported.gl_to_vulkan_fd,
        vulkan_to_gl_fd: exported.vulkan_to_gl_fd,
        sync: exported.sync,
    })
}

/// Put WGPU's resource tracker and the Vulkan image into shader-read state
/// before GL first acquires the imported memory.
pub(crate) fn prime_shared_texture_for_gl(
    ctx: &super::WgpuContext,
    view: &wgpu::TextureView,
    sync: &GlInteropSync,
) -> anyhow::Result<()> {
    let uniform = ctx
        .device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("libmpv GL Interop Prime Uniform"),
            contents: &[0; 128],
            usage: wgpu::BufferUsages::UNIFORM,
        });
    let sampler = ctx
        .device
        .create_sampler(&wgpu::SamplerDescriptor::default());
    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("libmpv GL Interop Prime Bind Group"),
        layout: &ctx.blit_bind_group_layout,
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
                resource: wgpu::BindingResource::Sampler(&sampler),
            },
        ],
    });
    let target = ctx.device.create_texture(&wgpu::TextureDescriptor {
        label: Some("libmpv GL Interop Prime Target"),
        size: wgpu::Extent3d {
            width: 1,
            height: 1,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu::TextureFormat::Rgba8UnormSrgb,
        usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
        view_formats: &[],
    });
    let target_view = target.create_view(&wgpu::TextureViewDescriptor::default());
    let pipeline = ctx.get_blit_pipeline(wgpu::TextureFormat::Rgba8UnormSrgb);
    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("libmpv GL Interop Prime Encoder"),
        });
    {
        let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
            label: Some("libmpv GL Interop Prime Pass"),
            color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                view: &target_view,
                resolve_target: None,
                ops: wgpu::Operations {
                    load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                    store: wgpu::StoreOp::Discard,
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
    ctx.submit(std::iter::once(encoder.finish()));
    sync.signal_gl_reuse(ctx)
}

struct ExportedHalTexture {
    texture: wgpu_hal::vulkan::Texture,
    memory_fd: OwnedFd,
    memory_size: u64,
    gl_to_vulkan_fd: OwnedFd,
    vulkan_to_gl_fd: OwnedFd,
    sync: Arc<GlInteropSync>,
}

fn create_exportable_hal_texture(
    device: &wgpu::Device,
    width: u32,
    height: u32,
    format: wgpu::TextureFormat,
    label: &'static str,
) -> Option<ExportedHalTexture> {
    // SAFETY: all Vulkan objects are created from the live WGPU Vulkan device.
    unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device| {
            let hal_device = hal_device?;
            if !hal_device
                .enabled_device_extensions()
                .contains(&ash::khr::external_memory_fd::NAME)
            {
                error!("[MPV-GL] WGPU Vulkan device lacks VK_KHR_external_memory_fd");
                return None;
            }
            if !hal_device
                .enabled_device_extensions()
                .contains(&ash::khr::external_semaphore_fd::NAME)
            {
                error!("[MPV-GL] WGPU Vulkan device lacks VK_KHR_external_semaphore_fd");
                return None;
            }
            let raw_device = hal_device.raw_device();
            let mut external = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);
            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk::Format::R8G8B8A8_UNORM)
                .extent(vk::Extent3D {
                    width,
                    height,
                    depth: 1,
                })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::OPTIMAL)
                .usage(vk::ImageUsageFlags::SAMPLED | vk::ImageUsageFlags::COLOR_ATTACHMENT)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .flags(vk::ImageCreateFlags::MUTABLE_FORMAT)
                .push_next(&mut external);
            let image = raw_device.create_image(&image_info, None).ok()?;
            let requirements = raw_device.get_image_memory_requirements(image);
            let instance = hal_device.shared_instance().raw_instance();
            let memory_properties =
                instance.get_physical_device_memory_properties(hal_device.raw_physical_device());
            let Some(memory_type_index) =
                find_device_local_memory_type(&memory_properties, requirements.memory_type_bits)
            else {
                error!("[MPV-GL] No device-local Vulkan memory type for shared RGBA texture");
                raw_device.destroy_image(image, None);
                return None;
            };
            let mut export = vk::ExportMemoryAllocateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);
            let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
            let allocation_info = vk::MemoryAllocateInfo::default()
                .allocation_size(requirements.size)
                .memory_type_index(memory_type_index)
                .push_next(&mut export)
                .push_next(&mut dedicated);
            let memory = match raw_device.allocate_memory(&allocation_info, None) {
                Ok(memory) => memory,
                Err(error) => {
                    error!("[MPV-GL] Vulkan shared-memory allocation failed: {error:?}");
                    raw_device.destroy_image(image, None);
                    return None;
                }
            };
            if let Err(error) = raw_device.bind_image_memory(image, memory, 0) {
                error!("[MPV-GL] Vulkan shared image bind failed: {error:?}");
                raw_device.free_memory(memory, None);
                raw_device.destroy_image(image, None);
                return None;
            }
            let external_memory = ash::khr::external_memory_fd::Device::new(instance, raw_device);
            let fd_info = vk::MemoryGetFdInfoKHR::default()
                .memory(memory)
                .handle_type(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);
            let memory_fd = match external_memory.get_memory_fd(&fd_info) {
                Ok(fd) => OwnedFd::from_raw_fd(fd),
                Err(error) => {
                    error!("[MPV-GL] Vulkan memory FD export failed: {error:?}");
                    raw_device.free_memory(memory, None);
                    raw_device.destroy_image(image, None);
                    return None;
                }
            };
            let (sync, gl_to_vulkan_fd, vulkan_to_gl_fd) =
                match create_external_semaphores(instance, raw_device) {
                    Ok(sync) => sync,
                    Err(error) => {
                        error!("[MPV-GL] Vulkan semaphore export failed: {error:#}");
                        raw_device.free_memory(memory, None);
                        raw_device.destroy_image(image, None);
                        return None;
                    }
                };
            let drop_device = raw_device.clone();
            let descriptor = wgpu_hal::TextureDescriptor {
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
                usage: wgpu_hal::TextureUses::RESOURCE,
                memory_flags: wgpu_hal::MemoryFlags::empty(),
                view_formats: vec![wgpu::TextureFormat::Rgba8UnormSrgb],
            };
            let cleanup: Box<dyn Fn() + Send + Sync> = Box::new(move || {
                drop_device.destroy_image(image, None);
                drop_device.free_memory(memory, None);
            });
            let texture =
                wgpu_hal::vulkan::Device::texture_from_raw(image, &descriptor, Some(cleanup));
            Some(ExportedHalTexture {
                texture,
                memory_fd,
                memory_size: requirements.size,
                gl_to_vulkan_fd,
                vulkan_to_gl_fd,
                sync,
            })
        })?
    }
}

/// Pair of reusable binary semaphores for one shared texture slot.
///
/// GL signals `gl_to_vulkan` after rendering. Vulkan waits before WGPU
/// samples, then signals `vulkan_to_gl` after sampling completes so GL can
/// safely reuse the slot.
pub(crate) struct GlInteropSync {
    device: ash::Device,
    gl_to_vulkan: vk::Semaphore,
    vulkan_to_gl: vk::Semaphore,
}

impl GlInteropSync {
    pub(crate) fn wait_for_gl_render(&self, ctx: &super::WgpuContext) -> anyhow::Result<()> {
        self.submit(ctx, Some(self.gl_to_vulkan), None)
    }

    pub(crate) fn wait_for_gl_render_and_release(
        &self,
        ctx: &super::WgpuContext,
        previous: &Self,
    ) -> anyhow::Result<()> {
        self.submit(ctx, Some(self.gl_to_vulkan), Some(previous.vulkan_to_gl))
    }

    pub(crate) fn signal_gl_reuse(&self, ctx: &super::WgpuContext) -> anyhow::Result<()> {
        self.submit(ctx, None, Some(self.vulkan_to_gl))
    }

    fn submit(
        &self,
        ctx: &super::WgpuContext,
        wait: Option<vk::Semaphore>,
        signal: Option<vk::Semaphore>,
    ) -> anyhow::Result<()> {
        ctx.with_raw_queue_lock(|| {
            // SAFETY: the raw queue belongs to ctx.device and access is
            // serialized with every WGPU queue operation by queue_lock.
            let submitted = unsafe {
                ctx.device
                    .as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device| {
                        let hal_device = hal_device
                            .ok_or_else(|| anyhow::anyhow!("WGPU device is not Vulkan"))?;
                        let wait_semaphores = wait.into_iter().collect::<Vec<_>>();
                        let wait_stages = wait_semaphores
                            .iter()
                            .map(|_| vk::PipelineStageFlags::FRAGMENT_SHADER)
                            .collect::<Vec<_>>();
                        let signal_semaphores = signal.into_iter().collect::<Vec<_>>();
                        let submit = vk::SubmitInfo::default()
                            .wait_semaphores(&wait_semaphores)
                            .wait_dst_stage_mask(&wait_stages)
                            .signal_semaphores(&signal_semaphores);
                        self.device
                            .queue_submit(
                                hal_device.raw_queue(),
                                std::slice::from_ref(&submit),
                                vk::Fence::null(),
                            )
                            .map_err(|error| {
                                anyhow::anyhow!("Vulkan interop submit failed: {error:?}")
                            })
                    })
            };
            submitted.ok_or_else(|| anyhow::anyhow!("WGPU core Vulkan device is unavailable"))?
        })
    }
}

impl Drop for GlInteropSync {
    fn drop(&mut self) {
        // A frame can be the last Arc owner immediately after its signal-only
        // release submit. Vulkan forbids destroying a semaphore while a queue
        // operation still references it, so make teardown synchronous. This
        // only runs when a three-slot player pool is retired, never per frame.
        unsafe {
            if let Err(error) = self.device.device_wait_idle() {
                error!(
                    "[MPV-GL] Vulkan device-idle wait during semaphore teardown failed: {error:?}"
                );
            }
            self.device.destroy_semaphore(self.gl_to_vulkan, None);
            self.device.destroy_semaphore(self.vulkan_to_gl, None);
        }
    }
}

fn create_external_semaphores(
    instance: &ash::Instance,
    device: &ash::Device,
) -> anyhow::Result<(Arc<GlInteropSync>, OwnedFd, OwnedFd)> {
    // SAFETY: both semaphores are created and exported from this live device.
    unsafe {
        let mut export = vk::ExportSemaphoreCreateInfo::default()
            .handle_types(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD);
        let create_info = vk::SemaphoreCreateInfo::default().push_next(&mut export);
        let gl_to_vulkan = device
            .create_semaphore(&create_info, None)
            .map_err(|error| anyhow::anyhow!("creating GL-to-Vulkan semaphore: {error:?}"))?;
        let vulkan_to_gl = match device.create_semaphore(&create_info, None) {
            Ok(semaphore) => semaphore,
            Err(error) => {
                device.destroy_semaphore(gl_to_vulkan, None);
                return Err(anyhow::anyhow!(
                    "creating Vulkan-to-GL semaphore: {error:?}"
                ));
            }
        };
        let external = ash::khr::external_semaphore_fd::Device::new(instance, device);
        let export_fd = |semaphore| {
            external
                .get_semaphore_fd(
                    &vk::SemaphoreGetFdInfoKHR::default()
                        .semaphore(semaphore)
                        .handle_type(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD),
                )
                .map(|fd| OwnedFd::from_raw_fd(fd))
        };
        let gl_to_vulkan_fd = match export_fd(gl_to_vulkan) {
            Ok(fd) => fd,
            Err(error) => {
                device.destroy_semaphore(gl_to_vulkan, None);
                device.destroy_semaphore(vulkan_to_gl, None);
                return Err(anyhow::anyhow!(
                    "exporting GL-to-Vulkan semaphore: {error:?}"
                ));
            }
        };
        let vulkan_to_gl_fd = match export_fd(vulkan_to_gl) {
            Ok(fd) => fd,
            Err(error) => {
                device.destroy_semaphore(gl_to_vulkan, None);
                device.destroy_semaphore(vulkan_to_gl, None);
                return Err(anyhow::anyhow!(
                    "exporting Vulkan-to-GL semaphore: {error:?}"
                ));
            }
        };
        info!("[MPV-GL] Created bidirectional external semaphore pair");
        Ok((
            Arc::new(GlInteropSync {
                device: device.clone(),
                gl_to_vulkan,
                vulkan_to_gl,
            }),
            gl_to_vulkan_fd,
            vulkan_to_gl_fd,
        ))
    }
}

fn find_device_local_memory_type(
    properties: &vk::PhysicalDeviceMemoryProperties,
    type_bits: u32,
) -> Option<u32> {
    (0..properties.memory_type_count).find(|index| {
        let supported = type_bits & (1 << index) != 0;
        let flags = properties.memory_types[*index as usize].property_flags;
        supported && flags.contains(vk::MemoryPropertyFlags::DEVICE_LOCAL)
    })
}
