const PCI_VENDOR_AMD: u32 = 0x1002;
const PCI_VENDOR_NVIDIA: u32 = 0x10de;
const PCI_VENDOR_INTEL: u32 = 0x8086;

fn expected_drivers(adapter_vendor: u32) -> Option<&'static [&'static str]> {
    match adapter_vendor {
        PCI_VENDOR_INTEL => Some(&["iHD", "i965"]),
        PCI_VENDOR_AMD => Some(&["radeonsi", "r600"]),
        // `nvidia` can be a deliberate nvidia-vaapi-driver choice. Keep it and
        // every other override intact on NVIDIA hardware.
        PCI_VENDOR_NVIDIA => None,
        _ => None,
    }
}

fn drm_node_vendor(minor: u32) -> Option<u32> {
    let vendor_path = format!("/sys/class/drm/renderD{minor}/device/vendor");
    let raw = std::fs::read_to_string(vendor_path).ok()?;
    u32::from_str_radix(raw.trim().trim_start_matches("0x"), 16).ok()
}

fn single_drm_vendor() -> Option<u32> {
    let mut selected = None;
    for minor in 128..=143 {
        let Some(vendor) = drm_node_vendor(minor) else {
            continue;
        };
        match selected {
            Some(existing) if existing != vendor => return None,
            None => selected = Some(vendor),
            _ => {}
        }
    }
    selected
}

/// Defend libva users against a driver override for another GPU vendor.
///
/// This changes only the daemon process. NVIDIA and unknown adapters are left
/// alone because their override may be intentional.
pub(crate) fn sanitize_libva_driver_env(adapter_vendor: u32, component: &str) {
    let Some(expected) = expected_drivers(adapter_vendor) else {
        return;
    };
    let Ok(current) = std::env::var("LIBVA_DRIVER_NAME") else {
        return;
    };
    if expected.contains(&current.trim()) {
        return;
    }
    tracing::warn!(
        "{component} LIBVA_DRIVER_NAME={current} mismatches adapter vendor \
         0x{adapter_vendor:04x}; unsetting it for this process so libva \
         auto-detects the right driver"
    );
    // SAFETY: callers run this during serialized backend initialization before
    // Kaleidux starts any libva consumer. No Kaleidux thread mutates the
    // process environment concurrently.
    unsafe { std::env::remove_var("LIBVA_DRIVER_NAME") };
}

/// Sanitize before GStreamer scans dynamic VA-API elements. A multi-GPU host
/// is deliberately left unchanged until WGPU identifies the selected adapter.
pub fn sanitize_libva_driver_env_for_gstreamer() {
    if let Some(adapter_vendor) = single_drm_vendor() {
        sanitize_libva_driver_env(adapter_vendor, "[GSTREAMER]");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vaapi_vendors_have_explicit_driver_sets() {
        assert_eq!(
            expected_drivers(PCI_VENDOR_INTEL),
            Some(&["iHD", "i965"][..])
        );
        assert_eq!(
            expected_drivers(PCI_VENDOR_AMD),
            Some(&["radeonsi", "r600"][..])
        );
    }

    #[test]
    fn nvidia_and_unknown_vendors_keep_user_overrides() {
        assert_eq!(expected_drivers(PCI_VENDOR_NVIDIA), None);
        assert_eq!(expected_drivers(0xffff), None);
    }
}
