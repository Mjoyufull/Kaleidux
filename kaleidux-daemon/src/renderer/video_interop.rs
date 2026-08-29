use tracing::{error, info};

/// Import a DMA-BUF file descriptor as a wgpu::Texture via the Vulkan backend.
///
/// Returns `None` if the current backend is not Vulkan or if any Vulkan call fails.
/// The caller should fall back to a CPU upload path in that case.
#[allow(dead_code)]
pub(super) fn import_dmabuf_as_texture(
    device: &wgpu::Device,
    fd: std::os::unix::io::RawFd,
    width: u32,
    height: u32,
    stride: u32,
    offset: u32,
    format: wgpu::TextureFormat,
    label: &str,
) -> Option<wgpu::Texture> {
    use ash::vk;

    // Duplicate the fd so GStreamer can still manage its own copy.
    // SAFETY: `fd` is borrowed from GStreamer/DMA-BUF memory; `dup` creates a new descriptor
    // that this function either closes on failure or transfers to Vulkan on successful import.
    let owned_fd = unsafe { libc::dup(fd) };
    if owned_fd < 0 {
        error!("[DMABUF] Failed to dup fd {}", fd);
        return None;
    }

    let vk_format = match wgpu_format_to_vk(format) {
        Some(f) => f,
        None => {
            // SAFETY: `owned_fd` is this function's duplicate and has not been imported.
            unsafe {
                libc::close(owned_fd);
            }
            return None;
        }
    };

    // Validation (P-21): Check if offset + (height * stride) fits in the buffer.
    // SAFETY: `owned_fd` is a valid duplicate descriptor owned by this function.
    let buf_size = unsafe { libc::lseek(owned_fd, 0, libc::SEEK_END) };
    if buf_size < 0 {
        error!("[DMABUF] Failed to lseek fd {owned_fd}");
        // SAFETY: `owned_fd` is this function's duplicate and has not been imported.
        unsafe {
            libc::close(owned_fd);
        }
        return None;
    }
    // SAFETY: resetting the offset affects only this duplicate descriptor used for validation.
    unsafe {
        libc::lseek(owned_fd, 0, libc::SEEK_SET);
    }

    let req_size = offset as i64 + (height as i64 * stride as i64);
    if req_size > buf_size {
        error!(
            "[DMABUF] {label} validation failed: req_size {} > buf_size {} (offset={}, h={}, stride={})",
            req_size, buf_size, offset, height, stride
        );
        // SAFETY: `owned_fd` is this function's duplicate and has not been imported.
        unsafe {
            libc::close(owned_fd);
        }
        return None;
    }

    // Access the underlying Vulkan device through wgpu-hal's callback API
    // and perform all Vulkan operations inside.
    // SAFETY: Vulkan objects are created from the live WGPU Vulkan device; `owned_fd` is
    // closed on every pre-import failure path and transferred to Vulkan on successful import.
    let hal_texture: Option<wgpu_hal::vulkan::Texture> = unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device_opt| {
            let Some(hal_device) = hal_device_opt else {
                libc::close(owned_fd);
                return None;
            };
            let raw_device = hal_device.raw_device();

            // 1. Create VkImage with external memory support
            let mut external_memory_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);

            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D {
                    width,
                    height,
                    depth: 1,
                })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::LINEAR)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_memory_info);

            let vk_image = match raw_device.create_image(&image_info, None) {
                Ok(img) => img,
                Err(e) => {
                    error!("[DMABUF] Failed to create VkImage: {:?}", e);
                    libc::close(owned_fd);
                    return None;
                }
            };

            // 2. Query memory requirements
            let mem_reqs = raw_device.get_image_memory_requirements(vk_image);

            // 3. Query DMA-BUF fd memory properties via VK_KHR_external_memory_fd
            let ash_instance = hal_device.shared_instance().raw_instance();
            let ext_mem_fd = ash::khr::external_memory_fd::Device::new(ash_instance, raw_device);

            let mut fd_mem_props = vk::MemoryFdPropertiesKHR::default();
            if let Err(e) = ext_mem_fd.get_memory_fd_properties(
                vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT,
                owned_fd,
                &mut fd_mem_props,
            ) {
                error!("[DMABUF] Failed to query fd memory properties: {:?}", e);
                raw_device.destroy_image(vk_image, None);
                libc::close(owned_fd);
                return None;
            }

            // 4. Find a suitable memory type
            let type_bits = mem_reqs.memory_type_bits & fd_mem_props.memory_type_bits;
            let memory_type_index = match find_memory_type(type_bits) {
                Some(idx) => idx,
                None => {
                    error!(
                        "[DMABUF] No suitable memory type (reqs={:#x}, fd_props={:#x})",
                        mem_reqs.memory_type_bits, fd_mem_props.memory_type_bits
                    );
                    raw_device.destroy_image(vk_image, None);
                    libc::close(owned_fd);
                    return None;
                }
            };

            // 5. Import the DMA-BUF fd as Vulkan device memory
            let mut import_info = vk::ImportMemoryFdInfoKHR::default()
                .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT)
                .fd(owned_fd);

            let mut dedicated_info = vk::MemoryDedicatedAllocateInfo::default().image(vk_image);

            let alloc_info = vk::MemoryAllocateInfo::default()
                .allocation_size(mem_reqs.size)
                .memory_type_index(memory_type_index)
                .push_next(&mut import_info)
                .push_next(&mut dedicated_info);

            let vk_memory = match raw_device.allocate_memory(&alloc_info, None) {
                Ok(mem) => mem,
                Err(e) => {
                    error!("[DMABUF] vkAllocateMemory failed: {:?}", e);
                    raw_device.destroy_image(vk_image, None);
                    libc::close(owned_fd);
                    return None;
                }
            };
            // Success: Vulkan now owns the FD.
            // DO NOT manually close it here (it causes intermittent double-close crashes).
            // The driver/Vulkan implementation is responsible for closing the imported FD.

            // 6. Bind imported memory to the image
            if let Err(e) =
                raw_device.bind_image_memory(vk_image, vk_memory, offset as vk::DeviceSize)
            {
                error!("[DMABUF] vkBindImageMemory failed: {:?}", e);
                raw_device.free_memory(vk_memory, None);
                raw_device.destroy_image(vk_image, None);
                return None;
            }

            // 7. Wrap as wgpu-hal Texture with a drop callback for cleanup
            let drop_device = raw_device.clone();
            let drop_image = vk_image;
            let drop_memory = vk_memory;

            let hal_desc = wgpu_hal::TextureDescriptor {
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
                view_formats: vec![],
            };

            let drop_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
                drop_device.destroy_image(drop_image, None);
                drop_device.free_memory(drop_memory, None);
            });

            Some(wgpu_hal::vulkan::Device::texture_from_raw(
                vk_image,
                &hal_desc,
                Some(drop_callback),
            ))
        })?
    };

    let hal_texture = hal_texture?;

    // 8. Wrap the HAL texture as a wgpu::Texture
    let wgpu_desc = wgpu::TextureDescriptor {
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
        usage: wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[],
    };

    // SAFETY: `hal_texture` was created from the same live Vulkan device and matches
    // `wgpu_desc`; WGPU takes ownership and runs the HAL cleanup callback on drop.
    Some(unsafe {
        device.create_texture_from_hal::<wgpu_hal::vulkan::Api>(hal_texture, &wgpu_desc)
    })
}

/// Convert wgpu TextureFormat to Vulkan format.
fn wgpu_format_to_vk(format: wgpu::TextureFormat) -> Option<ash::vk::Format> {
    match format {
        wgpu::TextureFormat::R8Unorm => Some(ash::vk::Format::R8_UNORM),
        wgpu::TextureFormat::Rg8Unorm => Some(ash::vk::Format::R8G8_UNORM),
        wgpu::TextureFormat::Rgba8UnormSrgb => Some(ash::vk::Format::R8G8B8A8_SRGB),
        wgpu::TextureFormat::Rgba8Unorm => Some(ash::vk::Format::R8G8B8A8_UNORM),
        _ => {
            error!(
                "[DMABUF] Unsupported texture format for DMA-BUF import: {:?}",
                format
            );
            None
        }
    }
}

/// Find the lowest-index set bit in a memory type bitmask.
fn find_memory_type(type_bits: u32) -> Option<u32> {
    (0..32).find(|&i| (type_bits & (1 << i)) != 0)
}

/// Allocate CUDA-exportable memory, import the fd into Vulkan as a LINEAR
/// tiled image, and return the wgpu Texture + CUDA allocation + row pitch.
/// This reverses the usual Vulkan-export→CUDA-import flow: CUDA owns the
/// memory and Vulkan imports it, avoiding the need for vkGetMemoryFdKHR.
/// Returned layout info from Vulkan's LINEAR tiling.
pub(super) struct CudaTexLayout {
    pub(super) row_pitch: usize,
    pub(super) offset: usize,
}

pub(super) struct CudaVulkanTimeline {
    device: ash::Device,
    queue: ash::vk::Queue,
    semaphore: ash::vk::Semaphore,
    cuda: Option<crate::cuda_interop::CudaTimelineSemaphore>,
    next_value: u64,
}

impl CudaVulkanTimeline {
    pub(super) fn new(
        ci: &crate::cuda_interop::CudaInterop,
        device: &wgpu::Device,
    ) -> anyhow::Result<Self> {
        use ash::vk;
        anyhow::ensure!(
            ci.supports_external_timeline(),
            "CUDA driver does not expose external timeline semaphore APIs"
        );
        // SAFETY: handles are cloned from the live WGPU Vulkan device. The
        // semaphore is retained by this object and destroyed before that
        // device is released.
        let (raw_device, queue, semaphore, fd) = unsafe {
            device
                .as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device| {
                    let hal_device =
                        hal_device.ok_or_else(|| anyhow::anyhow!("WGPU is not using Vulkan"))?;
                    let instance = hal_device.shared_instance().raw_instance().clone();
                    let raw_device = hal_device.raw_device().clone();
                    let queue = hal_device.raw_queue();
                    let mut export = vk::ExportSemaphoreCreateInfo::default()
                        .handle_types(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD);
                    let mut timeline = vk::SemaphoreTypeCreateInfo::default()
                        .semaphore_type(vk::SemaphoreType::TIMELINE)
                        .initial_value(0);
                    let create_info = vk::SemaphoreCreateInfo::default()
                        .push_next(&mut export)
                        .push_next(&mut timeline);
                    let semaphore =
                        raw_device
                            .create_semaphore(&create_info, None)
                            .map_err(|error| {
                                anyhow::anyhow!(
                                    "creating CUDA/Vulkan timeline semaphore: {error:?}"
                                )
                            })?;
                    let external =
                        ash::khr::external_semaphore_fd::Device::new(&instance, &raw_device);
                    let fd_info = vk::SemaphoreGetFdInfoKHR::default()
                        .semaphore(semaphore)
                        .handle_type(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD);
                    let fd = match external.get_semaphore_fd(&fd_info) {
                        Ok(fd) => fd,
                        Err(error) => {
                            raw_device.destroy_semaphore(semaphore, None);
                            return Err(anyhow::anyhow!(
                                "exporting CUDA/Vulkan timeline semaphore: {error:?}"
                            ));
                        }
                    };
                    Ok::<_, anyhow::Error>((raw_device, queue, semaphore, fd))
                })
                .ok_or_else(|| anyhow::anyhow!("WGPU Vulkan HAL device is unavailable"))??
        };
        let cuda = match ci.import_timeline_semaphore(fd) {
            Ok(cuda) => cuda,
            Err(error) => {
                // SAFETY: CUDA did not import the handle, and the Vulkan
                // semaphore has no pending submissions yet.
                unsafe { raw_device.destroy_semaphore(semaphore, None) };
                anyhow::bail!(error);
            }
        };
        info!("[CUDA-VK] explicit timeline semaphore handoff enabled");
        Ok(Self {
            device: raw_device,
            queue,
            semaphore,
            cuda: Some(cuda),
            next_value: 1,
        })
    }

    pub(super) fn next_frame_values(&mut self) -> (u64, u64) {
        let release = self.next_value;
        let ready = release + 1;
        self.next_value = ready.checked_add(1).unwrap_or(1);
        (release, ready)
    }

    pub(super) fn cuda(&self) -> &crate::cuda_interop::CudaTimelineSemaphore {
        self.cuda.as_ref().expect("timeline CUDA handle is live")
    }

    pub(super) fn signal_vulkan_release(&self, value: u64) -> anyhow::Result<()> {
        use ash::vk;
        let values = [value];
        let semaphores = [self.semaphore];
        let mut timeline =
            vk::TimelineSemaphoreSubmitInfo::default().signal_semaphore_values(&values);
        let submit = vk::SubmitInfo::default()
            .signal_semaphores(&semaphores)
            .push_next(&mut timeline);
        // SAFETY: queue and timeline semaphore belong to this live device;
        // queue access is serialized by WgpuContext::queue_lock at the caller.
        unsafe {
            self.device
                .queue_submit(self.queue, &[submit], vk::Fence::null())
        }
        .map_err(|error| anyhow::anyhow!("signaling Vulkan release timeline: {error:?}"))
    }

    pub(super) fn wait_vulkan_ready(&self, value: u64) -> anyhow::Result<()> {
        use ash::vk;
        let values = [value];
        let semaphores = [self.semaphore];
        let stages = [vk::PipelineStageFlags::ALL_COMMANDS];
        let mut timeline =
            vk::TimelineSemaphoreSubmitInfo::default().wait_semaphore_values(&values);
        let submit = vk::SubmitInfo::default()
            .wait_semaphores(&semaphores)
            .wait_dst_stage_mask(&stages)
            .push_next(&mut timeline);
        // SAFETY: same queue-serialization and ownership contract as above.
        unsafe {
            self.device
                .queue_submit(self.queue, &[submit], vk::Fence::null())
        }
        .map_err(|error| anyhow::anyhow!("waiting on CUDA-ready timeline: {error:?}"))
    }

    pub(super) fn completed_value(&self) -> anyhow::Result<u64> {
        // SAFETY: semaphore is a live timeline object on this device.
        unsafe { self.device.get_semaphore_counter_value(self.semaphore) }
            .map_err(|error| anyhow::anyhow!("querying CUDA/Vulkan timeline: {error:?}"))
    }

    pub(super) fn destroy(mut self, ci: &crate::cuda_interop::CudaInterop) {
        // Cache retirement is rare (resize/backend teardown). Ensure no queue
        // operation still references the semaphore or shared allocations.
        // SAFETY: this is the owning logical device.
        if let Err(error) = unsafe { self.device.device_wait_idle() } {
            error!("[CUDA-VK] device-idle wait during timeline teardown failed: {error:?}");
        }
        if let Some(cuda) = self.cuda.take() {
            ci.destroy_timeline_semaphore(cuda);
        }
        // SAFETY: the queue is idle and this object owns the semaphore.
        unsafe { self.device.destroy_semaphore(self.semaphore, None) };
        self.semaphore = ash::vk::Semaphore::null();
    }
}

impl Drop for CudaVulkanTimeline {
    fn drop(&mut self) {
        if self.semaphore != ash::vk::Semaphore::null() {
            // This is a last-resort cleanup path; normal renderer teardown
            // calls destroy() so the CUDA handle is released first.
            unsafe {
                let _ = self.device.device_wait_idle();
                self.device.destroy_semaphore(self.semaphore, None);
            }
        }
    }
}

pub(super) fn create_cuda_backed_texture(
    ci: &crate::cuda_interop::CudaInterop,
    device: &wgpu::Device,
    width: u32,
    height: u32,
    format: wgpu::TextureFormat,
    label: &str,
) -> Option<(
    wgpu::Texture,
    crate::cuda_interop::ExportableCudaAllocation,
    CudaTexLayout,
)> {
    use ash::vk;

    let vk_format = wgpu_format_to_vk(format)?;

    // Step 1: probe Vulkan for memory requirements (create temp image to query).
    // SAFETY: the temporary image is created and destroyed on the live WGPU Vulkan device
    // inside the HAL callback before any value escapes.
    let (mem_size, mem_type_bits, tex_layout) = unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device_opt| {
            let hal_device = hal_device_opt?;
            let raw_device = hal_device.raw_device();

            let mut external_memory_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);

            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D { width, height, depth: 1 })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::LINEAR)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_memory_info);

            let probe_image = raw_device.create_image(&image_info, None).ok()?;
            let mem_reqs = raw_device.get_image_memory_requirements(probe_image);
            let subresource = vk::ImageSubresource {
                aspect_mask: vk::ImageAspectFlags::COLOR,
                mip_level: 0,
                array_layer: 0,
            };
            let layout = raw_device.get_image_subresource_layout(probe_image, subresource);
            raw_device.destroy_image(probe_image, None);

            info!(
                "[CUDA-VK] {label} probe: mem_reqs.size={}, row_pitch={}, offset={}, layout.size={}",
                mem_reqs.size, layout.row_pitch, layout.offset, layout.size
            );

            Some((
                mem_reqs.size as usize,
                mem_reqs.memory_type_bits,
                CudaTexLayout {
                    row_pitch: layout.row_pitch as usize,
                    offset: layout.offset as usize,
                },
            ))
        })?
    }?;

    // Step 2: CUDA allocates exportable memory (rounded up to granularity)
    let (cuda_alloc, fd) = match ci.allocate_exportable(mem_size) {
        Ok(v) => v,
        Err(e) => {
            error!("[CUDA-VK] CUDA allocate_exportable failed: {e}");
            return None;
        }
    };

    // The Vulkan spec requires allocation_size to match the export size for OPAQUE_FD
    let cuda_export_size = cuda_alloc.alloc_size() as u64;

    // Step 3: import the CUDA fd into Vulkan and bind to a new VkImage.
    // SAFETY: all Vulkan objects are created from the live WGPU Vulkan device; the CUDA fd is
    // either closed on every failure path before import or transferred to Vulkan on success.
    let hal_texture: Option<wgpu_hal::vulkan::Texture> = unsafe {
        device.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device_opt| {
            let Some(hal_device) = hal_device_opt else {
                libc::close(fd);
                return None;
            };
            let raw_device = hal_device.raw_device();

            let mut external_memory_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD);

            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D { width, height, depth: 1 })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::LINEAR)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_memory_info);

            let vk_image = match raw_device.create_image(&image_info, None) {
                Ok(img) => img,
                Err(e) => {
                    error!("[CUDA-VK] Failed to create VkImage: {:?}", e);
                    libc::close(fd);
                    return None;
                }
            };

            let mem_reqs = raw_device.get_image_memory_requirements(vk_image);
            let memory_type_index = match find_memory_type(mem_reqs.memory_type_bits & mem_type_bits) {
                Some(idx) => idx,
                None => {
                    error!("[CUDA-VK] No compatible memory type (image={:#x})",
                           mem_reqs.memory_type_bits);
                    raw_device.destroy_image(vk_image, None);
                    libc::close(fd);
                    return None;
                }
            };

            let mut import_info = vk::ImportMemoryFdInfoKHR::default()
                .handle_type(vk::ExternalMemoryHandleTypeFlags::OPAQUE_FD)
                .fd(fd);

            let mut dedicated_info = vk::MemoryDedicatedAllocateInfo::default()
                .image(vk_image);

            // Use the CUDA export size (rounded to granularity), not mem_reqs.size
            let alloc_info = vk::MemoryAllocateInfo::default()
                .allocation_size(cuda_export_size)
                .memory_type_index(memory_type_index)
                .push_next(&mut import_info)
                .push_next(&mut dedicated_info);

            let vk_memory = match raw_device.allocate_memory(&alloc_info, None) {
                Ok(mem) => mem,
                Err(e) => {
                    error!("[CUDA-VK] vkAllocateMemory (import fd={fd}, size={cuda_export_size}) failed: {:?}", e);
                    raw_device.destroy_image(vk_image, None);
                    libc::close(fd);
                    return None;
                }
            };

            // Success: Vulkan now owns the FD.
            // DO NOT manually close it here (it causes intermittent double-close crashes).
            // The driver/Vulkan implementation is responsible for closing the imported FD.

            if let Err(e) = raw_device.bind_image_memory(vk_image, vk_memory, 0) {
                error!("[CUDA-VK] vkBindImageMemory failed: {:?}", e);
                raw_device.free_memory(vk_memory, None);
                raw_device.destroy_image(vk_image, None);
                return None;
            }

            let drop_device = raw_device.clone();
            let drop_image = vk_image;
            let drop_memory = vk_memory;

            let hal_desc = wgpu_hal::TextureDescriptor {
                label: Some(label),
                size: wgpu::Extent3d { width, height, depth_or_array_layers: 1 },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format,
                usage: wgpu_hal::TextureUses::RESOURCE,
                memory_flags: wgpu_hal::MemoryFlags::empty(),
                view_formats: vec![],
            };

            let drop_callback: Box<dyn Fn() + Send + Sync> = Box::new(move || {
                drop_device.destroy_image(drop_image, None);
                drop_device.free_memory(drop_memory, None);
            });

            Some(wgpu_hal::vulkan::Device::texture_from_raw(
                vk_image,
                &hal_desc,
                Some(drop_callback),
            ))
        })?
    };

    let hal_texture = match hal_texture {
        Some(t) => t,
        None => {
            ci.free_exportable(cuda_alloc);
            return None;
        }
    };

    let wgpu_desc = wgpu::TextureDescriptor {
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
        usage: wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[],
    };

    // SAFETY: `hal_texture` was created from the same live Vulkan device and matches `wgpu_desc`;
    // WGPU takes ownership and will invoke the HAL drop callback for Vulkan image/memory cleanup.
    let texture =
        unsafe { device.create_texture_from_hal::<wgpu_hal::vulkan::Api>(hal_texture, &wgpu_desc) };

    Some((texture, cuda_alloc, tex_layout))
}
