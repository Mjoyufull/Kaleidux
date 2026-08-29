use crate::video::NativeDmaBufNv12;
use ash::vk;
use std::hash::{Hash, Hasher};
use std::os::fd::AsRawFd;

const fn drm_fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

const DRM_FORMAT_R8: u32 = drm_fourcc(b'R', b'8', b' ', b' ');
const DRM_FORMAT_RG88: u32 = drm_fourcc(b'R', b'G', b'8', b'8');
const DRM_FORMAT_GR88: u32 = drm_fourcc(b'G', b'R', b'8', b'8');
const DRM_FORMAT_NV12: u32 = drm_fourcc(b'N', b'V', b'1', b'2');

pub(super) struct ImportedLayer {
    pub(super) device: ash::Device,
    pub(super) image: vk::Image,
    pub(super) memories: Vec<vk::DeviceMemory>,
    pub(super) format: vk::Format,
    pub(super) plane_count: usize,
    pub(super) initialized: bool,
}

impl Drop for ImportedLayer {
    fn drop(&mut self) {
        // SAFETY: NativeDmaBufInterop waits for the device before its cache is
        // dropped, and these handles were allocated from this exact device.
        unsafe {
            self.device.destroy_image(self.image, None);
            for memory in self.memories.drain(..) {
                self.device.free_memory(memory, None);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct CopySource {
    pub(super) layer_index: usize,
    pub(super) aspect: vk::ImageAspectFlags,
}

pub(super) struct ImportedSource {
    pub(super) layers: Vec<ImportedLayer>,
    pub(super) y: CopySource,
    pub(super) uv: CopySource,
}

pub(super) fn import_source(
    device: &ash::Device,
    external_memory_fd: &ash::khr::external_memory_fd::Device,
    descriptor: &NativeDmaBufNv12,
    width: u32,
    height: u32,
) -> anyhow::Result<ImportedSource> {
    let max_layer = descriptor
        .planes
        .iter()
        .map(|plane| plane.layer_index)
        .max()
        .ok_or_else(|| anyhow::anyhow!("DMA-BUF descriptor has no layers"))?;
    let mut layers = Vec::with_capacity(max_layer + 1);
    let mut y = None;
    let mut uv = None;
    for layer_index in 0..=max_layer {
        let planes = descriptor
            .planes
            .iter()
            .filter(|plane| plane.layer_index == layer_index)
            .copied()
            .collect::<Vec<_>>();
        anyhow::ensure!(!planes.is_empty(), "DMA-BUF layer {layer_index} is empty");
        let fourcc = planes[0].drm_fourcc;
        anyhow::ensure!(
            planes.iter().all(|plane| plane.drm_fourcc == fourcc),
            "DMA-BUF layer {layer_index} mixes DRM formats"
        );
        let (format, layer_width, layer_height) = match fourcc {
            DRM_FORMAT_NV12 if planes.len() == 2 => {
                (vk::Format::G8_B8R8_2PLANE_420_UNORM, width, height)
            }
            DRM_FORMAT_R8 if planes.len() == 1 => (vk::Format::R8_UNORM, width, height),
            DRM_FORMAT_RG88 | DRM_FORMAT_GR88 if planes.len() == 1 => {
                let (uv_width, uv_height) =
                    super::super::video_layout::chroma_plane_extent(width, height);
                (vk::Format::R8G8_UNORM, uv_width, uv_height)
            }
            _ => anyhow::bail!(
                "unsupported DMA-BUF layer format {fourcc:#010x} with {} memory planes",
                planes.len()
            ),
        };
        let imported = import_layer(
            device,
            external_memory_fd,
            descriptor,
            &planes,
            format,
            layer_width,
            layer_height,
        )?;
        let imported_index = layers.len();
        match fourcc {
            DRM_FORMAT_NV12 => {
                y = Some(CopySource {
                    layer_index: imported_index,
                    aspect: vk::ImageAspectFlags::PLANE_0,
                });
                uv = Some(CopySource {
                    layer_index: imported_index,
                    aspect: vk::ImageAspectFlags::PLANE_1,
                });
            }
            DRM_FORMAT_R8 => {
                y = Some(CopySource {
                    layer_index: imported_index,
                    aspect: vk::ImageAspectFlags::COLOR,
                });
            }
            DRM_FORMAT_RG88 | DRM_FORMAT_GR88 => {
                uv = Some(CopySource {
                    layer_index: imported_index,
                    aspect: vk::ImageAspectFlags::COLOR,
                });
            }
            _ => unreachable!("validated above"),
        }
        layers.push(imported);
    }
    Ok(ImportedSource {
        layers,
        y: y.ok_or_else(|| anyhow::anyhow!("DMA-BUF descriptor has no luma plane"))?,
        uv: uv.ok_or_else(|| anyhow::anyhow!("DMA-BUF descriptor has no UV plane"))?,
    })
}

fn import_layer(
    device: &ash::Device,
    external_memory_fd: &ash::khr::external_memory_fd::Device,
    descriptor: &NativeDmaBufNv12,
    planes: &[crate::video::NativeDmaBufPlane],
    format: vk::Format,
    width: u32,
    height: u32,
) -> anyhow::Result<ImportedLayer> {
    let mut object_indices = planes
        .iter()
        .map(|plane| plane.object_index)
        .collect::<Vec<_>>();
    object_indices.sort_unstable();
    object_indices.dedup();
    anyhow::ensure!(!object_indices.is_empty(), "DMA-BUF layer has no objects");
    let modifier_value = descriptor
        .objects
        .get(object_indices[0])
        .ok_or_else(|| anyhow::anyhow!("DMA-BUF layer references an invalid object"))?
        .modifier;
    anyhow::ensure!(
        object_indices.iter().all(|index| descriptor
            .objects
            .get(*index)
            .is_some_and(|object| object.modifier == modifier_value)),
        "DMA-BUF layer objects disagree on DRM modifier"
    );
    let disjoint = object_indices.len() > 1;
    anyhow::ensure!(
        !disjoint || object_indices.len() == planes.len(),
        "mixed shared/disjoint DMA-BUF plane allocation is unsupported"
    );
    let layouts = planes
        .iter()
        .map(|plane| {
            vk::SubresourceLayout::default()
                .offset(plane.offset)
                .size(0)
                .row_pitch(plane.pitch)
                .array_pitch(0)
                .depth_pitch(0)
        })
        .collect::<Vec<_>>();
    let mut modifier = vk::ImageDrmFormatModifierExplicitCreateInfoEXT::default()
        .drm_format_modifier(modifier_value)
        .plane_layouts(&layouts);
    let mut external = vk::ExternalMemoryImageCreateInfo::default()
        .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
    let image_info = vk::ImageCreateInfo::default()
        .flags(if disjoint {
            vk::ImageCreateFlags::DISJOINT
        } else {
            vk::ImageCreateFlags::empty()
        })
        .image_type(vk::ImageType::TYPE_2D)
        .format(format)
        .extent(vk::Extent3D {
            width,
            height,
            depth: 1,
        })
        .mip_levels(1)
        .array_layers(1)
        .samples(vk::SampleCountFlags::TYPE_1)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(vk::ImageUsageFlags::TRANSFER_SRC)
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .initial_layout(vk::ImageLayout::UNDEFINED)
        .push_next(&mut modifier)
        .push_next(&mut external);
    // SAFETY: all pNext data remains live for the call.
    let image = unsafe { device.create_image(&image_info, None) }
        .map_err(|error| anyhow::anyhow!("creating DRM modifier image: {error:?}"))?;

    let mut memories = Vec::with_capacity(if disjoint { planes.len() } else { 1 });
    let allocation_result = if disjoint {
        let mut allocation_error = None;
        for (plane_index, plane) in planes.iter().enumerate() {
            let aspect = memory_plane_aspect(plane_index);
            match allocate_imported_memory(
                device,
                external_memory_fd,
                descriptor,
                image,
                plane.object_index,
                Some(aspect),
            ) {
                Ok(memory) => memories.push(memory),
                Err(error) => {
                    allocation_error = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = allocation_error {
            Err(error)
        } else if memories.len() == planes.len() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("incomplete disjoint DMA-BUF allocation"))
        }
    } else {
        allocate_imported_memory(
            device,
            external_memory_fd,
            descriptor,
            image,
            object_indices[0],
            None,
        )
        .map(|memory| memories.push(memory))
    };
    if let Err(error) = allocation_result {
        unsafe {
            for memory in memories.drain(..) {
                device.free_memory(memory, None);
            }
            device.destroy_image(image, None);
        }
        return Err(error);
    }

    let bind_result = if disjoint {
        bind_disjoint_plane_memory(device, image, &memories)
    } else {
        // SAFETY: the non-disjoint image and imported allocation belong to this device.
        unsafe { device.bind_image_memory(image, memories[0], 0) }
            .map_err(|error| anyhow::anyhow!("binding DMA-BUF image memory: {error:?}"))
    };
    if let Err(error) = bind_result {
        unsafe {
            for memory in memories.drain(..) {
                device.free_memory(memory, None);
            }
            device.destroy_image(image, None);
        }
        return Err(error);
    }
    Ok(ImportedLayer {
        device: device.clone(),
        image,
        memories,
        format,
        plane_count: planes.len(),
        initialized: false,
    })
}

fn allocate_imported_memory(
    device: &ash::Device,
    external_memory_fd: &ash::khr::external_memory_fd::Device,
    descriptor: &NativeDmaBufNv12,
    image: vk::Image,
    object_index: usize,
    plane_aspect: Option<vk::ImageAspectFlags>,
) -> anyhow::Result<vk::DeviceMemory> {
    let object = descriptor
        .objects
        .get(object_index)
        .ok_or_else(|| anyhow::anyhow!("DMA-BUF layer references object {object_index}"))?;
    let mut plane_requirements = plane_aspect
        .map(|aspect| vk::ImagePlaneMemoryRequirementsInfo::default().plane_aspect(aspect));
    let mut requirements_info = vk::ImageMemoryRequirementsInfo2::default().image(image);
    if let Some(plane) = plane_requirements.as_mut() {
        requirements_info = requirements_info.push_next(plane);
    }
    let mut dedicated_requirements = vk::MemoryDedicatedRequirements::default();
    let mut requirements =
        vk::MemoryRequirements2::default().push_next(&mut dedicated_requirements);
    unsafe { device.get_image_memory_requirements2(&requirements_info, &mut requirements) };

    let imported_fd = unsafe { libc::dup(object.fd.as_raw_fd()) };
    if imported_fd < 0 {
        anyhow::bail!("duplicating DMA-BUF object fd failed");
    }
    let mut fd_properties = vk::MemoryFdPropertiesKHR::default();
    if let Err(error) = unsafe {
        external_memory_fd.get_memory_fd_properties(
            vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT,
            imported_fd,
            &mut fd_properties,
        )
    } {
        unsafe { libc::close(imported_fd) };
        return Err(anyhow::anyhow!(
            "querying DMA-BUF memory properties: {error:?}"
        ));
    }
    let type_bits =
        requirements.memory_requirements.memory_type_bits & fd_properties.memory_type_bits;
    let Some(memory_type_index) = (0..32).find(|index| type_bits & (1 << index) != 0) else {
        unsafe { libc::close(imported_fd) };
        anyhow::bail!("DMA-BUF has no compatible Vulkan memory type");
    };
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT)
        .fd(imported_fd);
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
    let allocate_info = vk::MemoryAllocateInfo::default()
        .allocation_size(requirements.memory_requirements.size)
        .memory_type_index(memory_type_index)
        .push_next(&mut import);
    let allocate_info = if dedicated_requirements.requires_dedicated_allocation == vk::TRUE
        || dedicated_requirements.prefers_dedicated_allocation == vk::TRUE
    {
        allocate_info.push_next(&mut dedicated)
    } else {
        allocate_info
    };
    match unsafe { device.allocate_memory(&allocate_info, None) } {
        Ok(memory) => Ok(memory),
        Err(error) => {
            unsafe { libc::close(imported_fd) };
            Err(anyhow::anyhow!("importing DMA-BUF memory: {error:?}"))
        }
    }
}

fn memory_plane_aspect(index: usize) -> vk::ImageAspectFlags {
    match index {
        0 => vk::ImageAspectFlags::MEMORY_PLANE_0_EXT,
        1 => vk::ImageAspectFlags::MEMORY_PLANE_1_EXT,
        2 => vk::ImageAspectFlags::MEMORY_PLANE_2_EXT,
        _ => vk::ImageAspectFlags::MEMORY_PLANE_3_EXT,
    }
}

fn bind_disjoint_plane_memory(
    device: &ash::Device,
    image: vk::Image,
    memories: &[vk::DeviceMemory],
) -> anyhow::Result<()> {
    let mut plane_infos = (0..memories.len())
        .map(|index| {
            vk::BindImagePlaneMemoryInfo::default().plane_aspect(memory_plane_aspect(index))
        })
        .collect::<Vec<_>>();
    let binds = plane_infos
        .iter_mut()
        .zip(memories)
        .map(|(plane_info, memory)| {
            vk::BindImageMemoryInfo::default()
                .image(image)
                .memory(*memory)
                .memory_offset(0)
                .push_next(plane_info)
        })
        .collect::<Vec<_>>();
    // SAFETY: image/memory and every pNext plane_info remain live.
    unsafe { device.bind_image_memory2(&binds) }
        .map_err(|error| anyhow::anyhow!("binding DMA-BUF image memory: {error:?}"))
}

pub(super) fn descriptor_key(descriptor: &NativeDmaBufNv12, width: u32, height: u32) -> u64 {
    // A NativeDmaBufInterop is scoped to one renderer/video-resource session.
    // VA surface IDs (and AVVkFrame addresses) are stable and unique inside
    // that scope. Modifier/plane validation happens on the cold import, so the
    // hot lookup needs no repeated fstat calls on duplicated descriptors.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    descriptor.surface_id.hash(&mut hasher);
    width.hash(&mut hasher);
    height.hash(&mut hasher);
    hasher.finish()
}
