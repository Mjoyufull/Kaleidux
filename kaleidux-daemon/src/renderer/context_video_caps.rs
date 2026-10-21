//! Queries the selected Vulkan adapter for compatible DMA-BUF allocation and formats.

use ash::vk;
use std::ffi::CStr;

pub(super) fn configure(wgpu_adapter: &wgpu::Adapter) {
    crate::video::set_renderer_vendor(wgpu_adapter.get_info().vendor);
    // SAFETY: the HAL adapter and instance stay borrowed throughout all queries.
    let result = unsafe {
        wgpu_adapter.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_adapter_option| {
            let hal_adapter = hal_adapter_option?;
            let instance = hal_adapter.shared_instance().raw_instance();
            let physical = hal_adapter.raw_physical_device();
            let extensions = instance
                .enumerate_device_extension_properties(physical)
                .ok()?;
            let supports = |name: &CStr| {
                extensions
                    .iter()
                    .any(|property| CStr::from_ptr(property.extension_name.as_ptr()) == name)
            };
            let primary = if supports(ash::ext::physical_device_drm::NAME) {
                let mut drm = vk::PhysicalDeviceDrmPropertiesEXT::default();
                let mut properties = vk::PhysicalDeviceProperties2::default().push_next(&mut drm);
                instance.get_physical_device_properties2(physical, &mut properties);
                (drm.has_primary != 0
                    && drm.primary_major == 226
                    && (0..64).contains(&drm.primary_minor))
                .then(|| std::path::PathBuf::from(format!("/dev/dri/card{}", drm.primary_minor)))
            } else {
                None
            };
            let formats = if supports(ash::ext::image_drm_format_modifier::NAME) {
                nv12_formats(instance, physical)
            } else {
                Vec::new()
            };
            Some((primary, formats))
        })
    };
    let (primary, formats) = result.unwrap_or_default();
    crate::video::import_caps::set(primary, formats);
}

unsafe fn nv12_formats(instance: &ash::Instance, physical: vk::PhysicalDevice) -> Vec<String> {
    let mut list = vk::DrmFormatModifierPropertiesListEXT::default();
    let mut initial_properties = vk::FormatProperties2::default().push_next(&mut list);
    // SAFETY: caller retains instance/physical, and list lives across the query.
    unsafe {
        instance.get_physical_device_format_properties2(
            physical,
            vk::Format::G8_B8R8_2PLANE_420_UNORM,
            &mut initial_properties,
        );
    }
    let mut modifiers = vec![
        vk::DrmFormatModifierPropertiesEXT::default();
        list.drm_format_modifier_count as usize
    ];
    list.p_drm_format_modifier_properties = modifiers.as_mut_ptr();
    list.drm_format_modifier_count = modifiers.len() as u32;
    let mut populated_properties = vk::FormatProperties2::default().push_next(&mut list);
    // SAFETY: Vulkan writes at most the declared live array length.
    unsafe {
        instance.get_physical_device_format_properties2(
            physical,
            vk::Format::G8_B8R8_2PLANE_420_UNORM,
            &mut populated_properties,
        );
    }
    modifiers.truncate(list.drm_format_modifier_count as usize);
    modifiers
        .into_iter()
        .filter(|modifier| {
            modifier
                .drm_format_modifier_tiling_features
                .contains(vk::FormatFeatureFlags::TRANSFER_SRC)
        })
        .map(|modifier| {
            if modifier.drm_format_modifier == 0 {
                "NV12".into()
            } else {
                format!("NV12:0x{:016x}", modifier.drm_format_modifier)
            }
        })
        .collect()
}
