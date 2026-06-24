use ash::vk;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use tracing::error;

pub(crate) struct ExportedWgpuTexture {
    pub(crate) texture: Arc<wgpu::Texture>,
    pub(crate) memory_fd: OwnedFd,
    pub(crate) memory_size: u64,
}

pub(crate) fn create_exportable_rgba_texture(
    device: &wgpu::Device,
    width: u32,
    height: u32,
    label: &'static str,
) -> Option<ExportedWgpuTexture> {
    let (hal_texture, memory_fd, memory_size) = create_exportable_hal_texture(
        device,
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
        device.create_texture_from_hal::<wgpu_hal::vulkan::Api>(hal_texture, &descriptor)
    };
    Some(ExportedWgpuTexture {
        texture: Arc::new(texture),
        memory_fd,
        memory_size,
    })
}

fn create_exportable_hal_texture(
    device: &wgpu::Device,
    width: u32,
    height: u32,
    format: wgpu::TextureFormat,
    label: &'static str,
) -> Option<(wgpu_hal::vulkan::Texture, OwnedFd, u64)> {
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
            Some((texture, memory_fd, requirements.size))
        })?
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
