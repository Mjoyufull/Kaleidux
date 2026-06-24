use anyhow::Context;
use ash::vk;
use std::ffi::CStr;
use tracing::info;

pub(super) fn mpv_gl_interop_requested() -> bool {
    matches!(
        crate::video::get_video_backend_request(),
        crate::video::VideoBackendRequest::ForceMpvExperimental
    ) && crate::video::MpvRenderApiRequest::from_env().enables_composed_gl()
}

pub(super) fn create_mpv_gl_interop_device(
    adapter: &wgpu::Adapter,
    descriptor: &wgpu::DeviceDescriptor<'_>,
) -> anyhow::Result<(wgpu::Device, wgpu::Queue)> {
    anyhow::ensure!(
        adapter.get_info().backend == wgpu::Backend::Vulkan,
        "KLD_MPV_RENDER_API=gl-composed currently requires WGPU's Vulkan backend"
    );
    // SAFETY: the HAL adapter is borrowed only while creating an owned HAL device
    // from that same adapter. The returned device is wrapped by the same WGPU adapter.
    let hal_device = unsafe {
        adapter.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_adapter| {
            let hal_adapter = hal_adapter.context("WGPU adapter is not Vulkan-backed")?;
            create_hal_device(hal_adapter, descriptor)
        })
    }?;
    // SAFETY: hal_device was created from this exact adapter, with the features
    // and memory hints declared by descriptor.
    let device = unsafe {
        adapter.create_device_from_hal::<wgpu_hal::vulkan::Api>(hal_device, descriptor, None)
    }
    .context("wrapping Vulkan mpv GL interop device with WGPU")?;
    info!("[MPV-GL] WGPU Vulkan device enables external memory/semaphore FD interop");
    Ok(device)
}

fn create_hal_device(
    adapter: &wgpu_hal::vulkan::Adapter,
    descriptor: &wgpu::DeviceDescriptor<'_>,
) -> anyhow::Result<wgpu_hal::OpenDevice<wgpu_hal::vulkan::Api>> {
    let mut extensions = adapter.required_device_extensions(descriptor.required_features);
    add_required_extension(adapter, &mut extensions, ash::khr::external_memory_fd::NAME)?;
    add_required_extension(
        adapter,
        &mut extensions,
        ash::khr::external_semaphore_fd::NAME,
    )?;
    let mut enabled_features =
        adapter.physical_device_features(&extensions, descriptor.required_features);
    let queue_info = vk::DeviceQueueCreateInfo::default()
        .queue_family_index(0)
        .queue_priorities(&[1.0]);
    let extension_ptrs = extensions
        .iter()
        .map(|extension| extension.as_ptr())
        .collect::<Vec<_>>();
    let create_info = vk::DeviceCreateInfo::default()
        .queue_create_infos(std::slice::from_ref(&queue_info))
        .enabled_extension_names(&extension_ptrs);
    let create_info = enabled_features.add_to_device_create(create_info);
    let instance = adapter.shared_instance().raw_instance();
    // SAFETY: create_info points to live extension and feature arrays, and the
    // physical device belongs to instance.
    let raw_device =
        unsafe { instance.create_device(adapter.raw_physical_device(), &create_info, None) }
            .context("creating Vulkan device with mpv GL interop extensions")?;
    // SAFETY: raw_device was created from adapter with the declared extension,
    // feature, queue-family, and queue-index values. HAL takes ownership.
    unsafe {
        adapter.device_from_raw(
            raw_device,
            None,
            &extensions,
            descriptor.required_features,
            &descriptor.memory_hints,
            0,
            0,
        )
    }
    .context("creating HAL device with mpv GL interop extensions")
}

fn add_required_extension(
    adapter: &wgpu_hal::vulkan::Adapter,
    extensions: &mut Vec<&'static CStr>,
    required: &'static CStr,
) -> anyhow::Result<()> {
    if extensions.contains(&required) {
        return Ok(());
    }
    let instance = adapter.shared_instance().raw_instance();
    // SAFETY: the physical device belongs to the live Vulkan instance.
    let supported =
        unsafe { instance.enumerate_device_extension_properties(adapter.raw_physical_device()) }
            .context("enumerating Vulkan device extensions")?;
    let is_supported = supported.iter().any(|property| {
        // SAFETY: Vulkan extension names are fixed-size null-terminated arrays.
        unsafe { CStr::from_ptr(property.extension_name.as_ptr()) == required }
    });
    anyhow::ensure!(
        is_supported,
        "Vulkan device does not support required mpv GL interop extension {}",
        required.to_string_lossy()
    );
    extensions.push(required);
    Ok(())
}
