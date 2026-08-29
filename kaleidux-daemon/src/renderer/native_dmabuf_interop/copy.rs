use super::import::{CopySource, ImportedLayer, ImportedSource};
use ash::vk;

pub(super) fn record_copy(
    device: &ash::Device,
    queue_family_index: u32,
    source: &mut ImportedSource,
    command_buffer: vk::CommandBuffer,
    y_destination: vk::Image,
    uv_destination: vk::Image,
    width: u32,
    height: u32,
) -> anyhow::Result<()> {
    let begin =
        vk::CommandBufferBeginInfo::default().flags(vk::CommandBufferUsageFlags::ONE_TIME_SUBMIT);
    // SAFETY: command_buffer is reset, primary, and not in flight.
    unsafe {
        device
            .begin_command_buffer(command_buffer, &begin)
            .map_err(|error| anyhow::anyhow!("beginning DMA-BUF copy: {error:?}"))?;
    }

    let source_acquire = source
        .layers
        .iter()
        .map(|layer| {
            vk::ImageMemoryBarrier::default()
                .src_access_mask(if layer.initialized {
                    vk::AccessFlags::MEMORY_WRITE
                } else {
                    vk::AccessFlags::empty()
                })
                .dst_access_mask(vk::AccessFlags::TRANSFER_READ)
                .old_layout(if layer.initialized {
                    vk::ImageLayout::GENERAL
                } else {
                    vk::ImageLayout::UNDEFINED
                })
                .new_layout(vk::ImageLayout::TRANSFER_SRC_OPTIMAL)
                .src_queue_family_index(vk::QUEUE_FAMILY_FOREIGN_EXT)
                .dst_queue_family_index(queue_family_index)
                .image(layer.image)
                .subresource_range(full_layer_range(layer))
        })
        .collect::<Vec<_>>();
    let destination_acquire = [y_destination, uv_destination].map(|image| {
        vk::ImageMemoryBarrier::default()
            .src_access_mask(vk::AccessFlags::SHADER_READ)
            .dst_access_mask(vk::AccessFlags::TRANSFER_WRITE)
            .old_layout(vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL)
            .new_layout(vk::ImageLayout::TRANSFER_DST_OPTIMAL)
            .src_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
            .dst_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
            .image(image)
            .subresource_range(color_range())
    });
    let mut acquire = source_acquire;
    acquire.extend(destination_acquire);
    // SAFETY: all barriers reference live images and the command buffer is recording.
    unsafe {
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::PipelineStageFlags::TRANSFER,
            vk::DependencyFlags::BY_REGION,
            &[],
            &[],
            &acquire,
        );
    }

    let (uv_width, uv_height) = super::super::video_layout::chroma_plane_extent(width, height);
    let copies = [
        (
            source.y,
            y_destination,
            vk::Extent3D {
                width,
                height,
                depth: 1,
            },
        ),
        (
            source.uv,
            uv_destination,
            vk::Extent3D {
                width: uv_width,
                height: uv_height,
                depth: 1,
            },
        ),
    ];
    for (copy_source, destination, extent) in copies {
        copy_plane(
            device,
            command_buffer,
            &source.layers,
            copy_source,
            destination,
            vk::ImageAspectFlags::COLOR,
            extent,
        );
    }

    let source_release = source
        .layers
        .iter()
        .map(|layer| {
            vk::ImageMemoryBarrier::default()
                .src_access_mask(vk::AccessFlags::TRANSFER_READ)
                .dst_access_mask(vk::AccessFlags::empty())
                .old_layout(vk::ImageLayout::TRANSFER_SRC_OPTIMAL)
                .new_layout(vk::ImageLayout::GENERAL)
                .src_queue_family_index(queue_family_index)
                .dst_queue_family_index(vk::QUEUE_FAMILY_FOREIGN_EXT)
                .image(layer.image)
                .subresource_range(full_layer_range(layer))
        })
        .collect::<Vec<_>>();
    let destination_release = [y_destination, uv_destination].map(|image| {
        vk::ImageMemoryBarrier::default()
            .src_access_mask(vk::AccessFlags::TRANSFER_WRITE)
            .dst_access_mask(vk::AccessFlags::SHADER_READ)
            .old_layout(vk::ImageLayout::TRANSFER_DST_OPTIMAL)
            .new_layout(vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL)
            .src_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
            .dst_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
            .image(image)
            .subresource_range(color_range())
    });
    let mut release = source_release;
    release.extend(destination_release);
    // SAFETY: barriers reference the same live images copied above.
    unsafe {
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::TRANSFER,
            vk::PipelineStageFlags::FRAGMENT_SHADER | vk::PipelineStageFlags::ALL_COMMANDS,
            vk::DependencyFlags::BY_REGION,
            &[],
            &[],
            &release,
        );
        device
            .end_command_buffer(command_buffer)
            .map_err(|error| anyhow::anyhow!("ending DMA-BUF copy: {error:?}"))?;
    }
    for layer in &mut source.layers {
        layer.initialized = true;
    }
    Ok(())
}

pub(super) fn record_copy_to_linear(
    device: &ash::Device,
    queue_family_index: u32,
    source: &mut ImportedSource,
    command_buffer: vk::CommandBuffer,
    output: &mut super::linear::LinearBridge,
    width: u32,
    height: u32,
) -> anyhow::Result<()> {
    let begin =
        vk::CommandBufferBeginInfo::default().flags(vk::CommandBufferUsageFlags::ONE_TIME_SUBMIT);
    unsafe {
        device
            .begin_command_buffer(command_buffer, &begin)
            .map_err(|error| anyhow::anyhow!("beginning linear bridge copy: {error:?}"))?;
    }

    let mut acquire = source_acquire_barriers(source, queue_family_index);
    acquire.push(
        vk::ImageMemoryBarrier::default()
            .src_access_mask(if output.initialized {
                vk::AccessFlags::MEMORY_READ
            } else {
                vk::AccessFlags::empty()
            })
            .dst_access_mask(vk::AccessFlags::TRANSFER_WRITE)
            .old_layout(if output.initialized {
                vk::ImageLayout::GENERAL
            } else {
                vk::ImageLayout::UNDEFINED
            })
            .new_layout(vk::ImageLayout::TRANSFER_DST_OPTIMAL)
            .src_queue_family_index(if output.initialized {
                vk::QUEUE_FAMILY_FOREIGN_EXT
            } else {
                vk::QUEUE_FAMILY_IGNORED
            })
            .dst_queue_family_index(queue_family_index)
            .image(output.image)
            .subresource_range(nv12_plane_range()),
    );
    unsafe {
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::PipelineStageFlags::TRANSFER,
            vk::DependencyFlags::BY_REGION,
            &[],
            &[],
            &acquire,
        );
    }

    let (uv_width, uv_height) = super::super::video_layout::chroma_plane_extent(width, height);
    for (copy_source, destination_aspect, extent) in [
        (
            source.y,
            vk::ImageAspectFlags::PLANE_0,
            vk::Extent3D {
                width,
                height,
                depth: 1,
            },
        ),
        (
            source.uv,
            vk::ImageAspectFlags::PLANE_1,
            vk::Extent3D {
                width: uv_width,
                height: uv_height,
                depth: 1,
            },
        ),
    ] {
        copy_plane(
            device,
            command_buffer,
            &source.layers,
            copy_source,
            output.image,
            destination_aspect,
            extent,
        );
    }

    let mut release = source_release_barriers(source, queue_family_index);
    release.push(
        vk::ImageMemoryBarrier::default()
            .src_access_mask(vk::AccessFlags::TRANSFER_WRITE)
            .dst_access_mask(vk::AccessFlags::MEMORY_READ)
            .old_layout(vk::ImageLayout::TRANSFER_DST_OPTIMAL)
            .new_layout(vk::ImageLayout::GENERAL)
            .src_queue_family_index(queue_family_index)
            .dst_queue_family_index(vk::QUEUE_FAMILY_FOREIGN_EXT)
            .image(output.image)
            .subresource_range(nv12_plane_range()),
    );
    unsafe {
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::TRANSFER,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::DependencyFlags::BY_REGION,
            &[],
            &[],
            &release,
        );
        device
            .end_command_buffer(command_buffer)
            .map_err(|error| anyhow::anyhow!("ending linear bridge copy: {error:?}"))?;
    }
    for layer in &mut source.layers {
        layer.initialized = true;
    }
    output.initialized = true;
    Ok(())
}

fn source_acquire_barriers(
    source: &ImportedSource,
    queue_family_index: u32,
) -> Vec<vk::ImageMemoryBarrier<'static>> {
    source
        .layers
        .iter()
        .map(|layer| {
            vk::ImageMemoryBarrier::default()
                .src_access_mask(if layer.initialized {
                    vk::AccessFlags::MEMORY_WRITE
                } else {
                    vk::AccessFlags::empty()
                })
                .dst_access_mask(vk::AccessFlags::TRANSFER_READ)
                .old_layout(if layer.initialized {
                    vk::ImageLayout::GENERAL
                } else {
                    vk::ImageLayout::UNDEFINED
                })
                .new_layout(vk::ImageLayout::TRANSFER_SRC_OPTIMAL)
                .src_queue_family_index(vk::QUEUE_FAMILY_FOREIGN_EXT)
                .dst_queue_family_index(queue_family_index)
                .image(layer.image)
                .subresource_range(full_layer_range(layer))
        })
        .collect()
}

fn source_release_barriers(
    source: &ImportedSource,
    queue_family_index: u32,
) -> Vec<vk::ImageMemoryBarrier<'static>> {
    source
        .layers
        .iter()
        .map(|layer| {
            vk::ImageMemoryBarrier::default()
                .src_access_mask(vk::AccessFlags::TRANSFER_READ)
                .dst_access_mask(vk::AccessFlags::empty())
                .old_layout(vk::ImageLayout::TRANSFER_SRC_OPTIMAL)
                .new_layout(vk::ImageLayout::GENERAL)
                .src_queue_family_index(queue_family_index)
                .dst_queue_family_index(vk::QUEUE_FAMILY_FOREIGN_EXT)
                .image(layer.image)
                .subresource_range(full_layer_range(layer))
        })
        .collect()
}

fn copy_plane(
    device: &ash::Device,
    command_buffer: vk::CommandBuffer,
    layers: &[ImportedLayer],
    source: CopySource,
    destination: vk::Image,
    destination_aspect: vk::ImageAspectFlags,
    extent: vk::Extent3D,
) {
    let layer = &layers[source.layer_index];
    let region = vk::ImageCopy::default()
        .src_subresource(
            vk::ImageSubresourceLayers::default()
                .aspect_mask(source.aspect)
                .mip_level(0)
                .base_array_layer(0)
                .layer_count(1),
        )
        .dst_subresource(
            vk::ImageSubresourceLayers::default()
                .aspect_mask(destination_aspect)
                .mip_level(0)
                .base_array_layer(0)
                .layer_count(1),
        )
        .extent(extent);
    // SAFETY: the source/destination formats and plane extents were validated
    // during import, and both images are in transfer layouts.
    unsafe {
        device.cmd_copy_image(
            command_buffer,
            layer.image,
            vk::ImageLayout::TRANSFER_SRC_OPTIMAL,
            destination,
            vk::ImageLayout::TRANSFER_DST_OPTIMAL,
            std::slice::from_ref(&region),
        );
    }
}

fn nv12_plane_range() -> vk::ImageSubresourceRange {
    vk::ImageSubresourceRange::default()
        .aspect_mask(vk::ImageAspectFlags::PLANE_0 | vk::ImageAspectFlags::PLANE_1)
        .base_mip_level(0)
        .level_count(1)
        .base_array_layer(0)
        .layer_count(1)
}

fn color_range() -> vk::ImageSubresourceRange {
    vk::ImageSubresourceRange::default()
        .aspect_mask(vk::ImageAspectFlags::COLOR)
        .base_mip_level(0)
        .level_count(1)
        .base_array_layer(0)
        .layer_count(1)
}

fn full_layer_range(layer: &ImportedLayer) -> vk::ImageSubresourceRange {
    let aspect = if layer.format == vk::Format::G8_B8R8_2PLANE_420_UNORM && layer.plane_count == 2 {
        vk::ImageAspectFlags::PLANE_0 | vk::ImageAspectFlags::PLANE_1
    } else {
        vk::ImageAspectFlags::COLOR
    };
    vk::ImageSubresourceRange::default()
        .aspect_mask(aspect)
        .base_mip_level(0)
        .level_count(1)
        .base_array_layer(0)
        .layer_count(1)
}
