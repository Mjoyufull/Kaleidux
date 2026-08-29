use crate::video::{NativeDmaBufNv12, NativeDmaBufObject, NativeDmaBufPlane};
use ash::vk;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;

const fn drm_fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

const DRM_FORMAT_NV12: u32 = drm_fourcc(b'N', b'V', b'1', b'2');
const DRM_FORMAT_MOD_LINEAR: u64 = 0;

struct LinearImage {
    device: ash::Device,
    image: vk::Image,
    memory: vk::DeviceMemory,
}

impl Drop for LinearImage {
    fn drop(&mut self) {
        // SAFETY: the interop owner waits for device idle before dropping its
        // cached bridges, and both handles belong to this device.
        unsafe {
            self.device.destroy_image(self.image, None);
            self.device.free_memory(self.memory, None);
        }
    }
}

pub(super) struct LinearBridge {
    _allocation: LinearImage,
    pub(super) image: vk::Image,
    descriptor: NativeDmaBufNv12,
    pub(super) initialized: bool,
}

impl LinearBridge {
    pub(super) fn create(
        device: &ash::Device,
        external_memory_fd: &ash::khr::external_memory_fd::Device,
        surface_id: u64,
        width: u32,
        height: u32,
    ) -> anyhow::Result<Self> {
        let created = create_nv12_image(device, external_memory_fd, width, height)?;
        let image = created.allocation.image;
        let objects: Arc<[NativeDmaBufObject]> = vec![created.object].into();
        let descriptor = NativeDmaBufNv12 {
            surface_id,
            objects,
            planes: [
                NativeDmaBufPlane {
                    layer_index: 0,
                    object_index: 0,
                    offset: created.offsets[0],
                    pitch: created.pitches[0],
                    drm_fourcc: DRM_FORMAT_NV12,
                },
                NativeDmaBufPlane {
                    layer_index: 0,
                    object_index: 0,
                    offset: created.offsets[1],
                    pitch: created.pitches[1],
                    drm_fourcc: DRM_FORMAT_NV12,
                },
            ],
            acquire_fence: None,
            drm_syncobj: None,
        };
        Ok(Self {
            _allocation: created.allocation,
            image,
            descriptor,
            initialized: false,
        })
    }

    pub(super) fn descriptor(&self) -> NativeDmaBufNv12 {
        self.descriptor.clone()
    }
}

struct CreatedImage {
    allocation: LinearImage,
    object: NativeDmaBufObject,
    offsets: [u64; 2],
    pitches: [u64; 2],
}

fn create_nv12_image(
    device: &ash::Device,
    external_memory_fd: &ash::khr::external_memory_fd::Device,
    width: u32,
    height: u32,
) -> anyhow::Result<CreatedImage> {
    let modifiers = [DRM_FORMAT_MOD_LINEAR];
    let mut modifier_list =
        vk::ImageDrmFormatModifierListCreateInfoEXT::default().drm_format_modifiers(&modifiers);
    let mut external = vk::ExternalMemoryImageCreateInfo::default()
        .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
    let image_info = vk::ImageCreateInfo::default()
        .image_type(vk::ImageType::TYPE_2D)
        .format(vk::Format::G8_B8R8_2PLANE_420_UNORM)
        .extent(vk::Extent3D {
            width,
            height,
            depth: 1,
        })
        .mip_levels(1)
        .array_layers(1)
        .samples(vk::SampleCountFlags::TYPE_1)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(vk::ImageUsageFlags::TRANSFER_DST)
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .initial_layout(vk::ImageLayout::UNDEFINED)
        .push_next(&mut modifier_list)
        .push_next(&mut external);
    // SAFETY: the pNext structures live through the call.
    let image = unsafe { device.create_image(&image_info, None) }
        .map_err(|error| anyhow::anyhow!("creating linear NV12 DMA-BUF image: {error:?}"))?;
    let requirements = unsafe { device.get_image_memory_requirements(image) };
    let memory_type_index = requirements.memory_type_bits.trailing_zeros();
    if memory_type_index >= 32 {
        unsafe { device.destroy_image(image, None) };
        anyhow::bail!("linear NV12 DMA-BUF image has no compatible memory type");
    }
    let mut export = vk::ExportMemoryAllocateInfo::default()
        .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
    let allocation_info = vk::MemoryAllocateInfo::default()
        .allocation_size(requirements.size)
        .memory_type_index(memory_type_index)
        .push_next(&mut export)
        .push_next(&mut dedicated);
    let memory = match unsafe { device.allocate_memory(&allocation_info, None) } {
        Ok(memory) => memory,
        Err(error) => {
            unsafe { device.destroy_image(image, None) };
            return Err(anyhow::anyhow!(
                "allocating linear NV12 DMA-BUF memory: {error:?}"
            ));
        }
    };
    if let Err(error) = unsafe { device.bind_image_memory(image, memory, 0) } {
        unsafe {
            device.free_memory(memory, None);
            device.destroy_image(image, None);
        }
        return Err(anyhow::anyhow!(
            "binding linear NV12 DMA-BUF image: {error:?}"
        ));
    }

    let layouts = [
        vk::ImageAspectFlags::MEMORY_PLANE_0_EXT,
        vk::ImageAspectFlags::MEMORY_PLANE_1_EXT,
    ]
    .map(|aspect| unsafe {
        device.get_image_subresource_layout(
            image,
            vk::ImageSubresource::default()
                .aspect_mask(aspect)
                .mip_level(0)
                .array_layer(0),
        )
    });
    if layouts.iter().any(|layout| layout.row_pitch == 0) {
        unsafe {
            device.free_memory(memory, None);
            device.destroy_image(image, None);
        }
        anyhow::bail!("linear NV12 DMA-BUF image returned an invalid zero plane pitch");
    }

    let fd_info = vk::MemoryGetFdInfoKHR::default()
        .memory(memory)
        .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
    let exported_fd = match unsafe { external_memory_fd.get_memory_fd(&fd_info) } {
        Ok(fd) => fd,
        Err(error) => {
            unsafe {
                device.free_memory(memory, None);
                device.destroy_image(image, None);
            }
            return Err(anyhow::anyhow!("exporting linear NV12 DMA-BUF: {error:?}"));
        }
    };
    Ok(CreatedImage {
        allocation: LinearImage {
            device: device.clone(),
            image,
            memory,
        },
        object: NativeDmaBufObject {
            // SAFETY: Vulkan returned a new owned file descriptor.
            fd: unsafe { OwnedFd::from_raw_fd(exported_fd) },
            size: requirements.size,
            modifier: DRM_FORMAT_MOD_LINEAR,
        },
        offsets: layouts.map(|layout| layout.offset),
        pitches: layouts.map(|layout| layout.row_pitch),
    })
}
