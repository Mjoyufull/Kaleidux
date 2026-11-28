//! Records reusable ownership barriers matching GL_EXT_semaphore layouts.

use ash::vk;

pub(super) struct OwnershipTransfers {
    pub(super) pool: vk::CommandPool,
    pub(super) acquire: vk::CommandBuffer,
    pub(super) release: vk::CommandBuffer,
}

impl OwnershipTransfers {
    pub(super) fn new(device: &ash::Device, image: vk::Image, family: u32) -> anyhow::Result<Self> {
        // SAFETY: caller owns the live Vulkan device and shared image. The
        // semaphore owner retires this pool after all queue uses complete.
        unsafe {
            let pool = device.create_command_pool(
                &vk::CommandPoolCreateInfo::default().queue_family_index(family),
                None,
            )?;
            let buffers = match device.allocate_command_buffers(
                &vk::CommandBufferAllocateInfo::default()
                    .command_pool(pool)
                    .level(vk::CommandBufferLevel::PRIMARY)
                    .command_buffer_count(2),
            ) {
                Ok(buffers) => buffers,
                Err(error) => {
                    device.destroy_command_pool(pool, None);
                    return Err(error.into());
                }
            };
            let record = || -> anyhow::Result<()> {
                record_barrier(
                    device,
                    buffers[0],
                    image,
                    vk::QUEUE_FAMILY_EXTERNAL,
                    family,
                    true,
                )?;
                record_barrier(
                    device,
                    buffers[1],
                    image,
                    family,
                    vk::QUEUE_FAMILY_EXTERNAL,
                    false,
                )
            };
            if let Err(error) = record() {
                device.destroy_command_pool(pool, None);
                return Err(error);
            }
            Ok(Self {
                pool,
                acquire: buffers[0],
                release: buffers[1],
            })
        }
    }
}

fn record_barrier(
    device: &ash::Device,
    command: vk::CommandBuffer,
    image: vk::Image,
    source_family: u32,
    destination_family: u32,
    acquire: bool,
) -> anyhow::Result<()> {
    let barrier = vk::ImageMemoryBarrier::default()
        .src_access_mask(if acquire {
            vk::AccessFlags::empty()
        } else {
            vk::AccessFlags::SHADER_READ
        })
        .dst_access_mask(if acquire {
            vk::AccessFlags::SHADER_READ
        } else {
            vk::AccessFlags::empty()
        })
        // Both GL semaphore calls use LAYOUT_SHADER_READ_ONLY_EXT. GL owns
        // the intervening attachment layout transitions for its render pass.
        .old_layout(vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL)
        .new_layout(vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL)
        .src_queue_family_index(source_family)
        .dst_queue_family_index(destination_family)
        .image(image)
        .subresource_range(
            vk::ImageSubresourceRange::default()
                .aspect_mask(vk::ImageAspectFlags::COLOR)
                .level_count(1)
                .layer_count(1),
        );
    // SAFETY: caller just allocated this command buffer, and the image stays
    // live through all submissions. Reuse can overlap until GL's GPU wait.
    unsafe {
        device.begin_command_buffer(
            command,
            &vk::CommandBufferBeginInfo::default()
                .flags(vk::CommandBufferUsageFlags::SIMULTANEOUS_USE),
        )?;
        device.cmd_pipeline_barrier(
            command,
            if acquire {
                vk::PipelineStageFlags::TOP_OF_PIPE
            } else {
                vk::PipelineStageFlags::FRAGMENT_SHADER
            },
            if acquire {
                vk::PipelineStageFlags::FRAGMENT_SHADER
            } else {
                vk::PipelineStageFlags::BOTTOM_OF_PIPE
            },
            vk::DependencyFlags::empty(),
            &[],
            &[],
            &[barrier],
        );
        device.end_command_buffer(command)?;
    }
    Ok(())
}
