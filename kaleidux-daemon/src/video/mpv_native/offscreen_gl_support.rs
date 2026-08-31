use anyhow::Context;
use khronos_egl as egl;
use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::ffi::{CStr, CString, c_char, c_void};
use std::os::fd::OwnedFd;
use std::path::PathBuf;

const PCI_VENDOR_NVIDIA: u32 = 0x10DE;

pub(super) type EglApi = egl::DynamicInstance<egl::EGL1_5>;

pub(super) struct EglProcLoader {
    egl: *const EglApi,
}

impl EglProcLoader {
    pub(super) fn new(egl: &EglApi) -> Self {
        Self {
            egl: egl as *const EglApi,
        }
    }
}

/// Select the display resource mpv uses for hardware decode probing.
///
/// On non-NVIDIA adapters a DRM render node avoids mpv's failed Wayland VA-API
/// probe on wlroots compositors without `wl_drm`. NVIDIA keeps the existing
/// wl_display behavior for nvdec/CUDA interop.
pub(super) fn select_hwdec_display_resource(adapter_vendor: Option<u32>) -> Option<OwnedFd> {
    if adapter_vendor == Some(PCI_VENDOR_NVIDIA) {
        tracing::debug!("[MPV-GL] NVIDIA adapter: keeping wl_display hwdec resource (nvdec path)");
        return None;
    }
    let (fd, path) = open_matching_drm_render_node(adapter_vendor)?;
    if adapter_vendor.is_none()
        && let Some(minor) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("renderD"))
            .and_then(|minor| minor.parse::<u32>().ok())
        && let Some(detected_vendor) = drm_node_vendor(minor)
    {
        crate::video::sanitize_libva_driver_env(detected_vendor, "[MPV-GL]");
    }
    if let Some(adapter_vendor) = adapter_vendor {
        tracing::info!(
            "[MPV-GL] hwdec display resource: DRM render node {} \
             (vendor=0x{adapter_vendor:04x})",
            path.display()
        );
    } else {
        tracing::info!(
            "[MPV-GL] hwdec display resource: DRM render node {}",
            path.display()
        );
    }
    Some(fd)
}

fn open_matching_drm_render_node(vendor_id: Option<u32>) -> Option<(OwnedFd, PathBuf)> {
    let mut first_available: Option<(OwnedFd, PathBuf)> = None;
    for minor in 128..=143u32 {
        let path = PathBuf::from(format!("/dev/dri/renderD{minor}"));
        if !path.exists() {
            continue;
        }
        let vendor_matches = vendor_matches(drm_node_vendor(minor), vendor_id);
        let Ok(file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
        else {
            continue;
        };
        let fd = OwnedFd::from(file);
        match vendor_matches {
            Some(true) => return Some((fd, path)),
            None if first_available.is_none() => first_available = Some((fd, path)),
            _ => {}
        }
    }
    first_available
}

fn vendor_matches(node_vendor: Option<u32>, wanted: Option<u32>) -> Option<bool> {
    node_vendor
        .zip(wanted)
        .map(|(node_vendor, wanted)| node_vendor == wanted)
}

fn drm_node_vendor(minor: u32) -> Option<u32> {
    let vendor_path = format!("/sys/class/drm/renderD{minor}/device/vendor");
    let raw = std::fs::read_to_string(vendor_path).ok()?;
    u32::from_str_radix(raw.trim().trim_start_matches("0x"), 16).ok()
}

pub(super) fn create_mpv_gl_context(
    mpv: &Mpv,
    native_display: Option<(u32, *mut c_void)>,
    drm_render_fd: Option<&OwnedFd>,
    adapter_vendor: Option<u32>,
    loader: &EglProcLoader,
) -> anyhow::Result<*mut sys::mpv_render_context> {
    let api_type = CString::new("opengl")?;
    let mut init = sys::mpv_opengl_init_params {
        get_proc_address: Some(mpv_get_proc_address),
        get_proc_address_ctx: loader as *const EglProcLoader as *mut c_void,
    };
    let mut drm_params = sys::mpv_opengl_drm_params_v2 {
        fd: -1,
        crtc_id: 0,
        connector_id: 0,
        atomic_request_ptr: std::ptr::null_mut(),
        render_fd: drm_render_fd
            .map(std::os::fd::AsRawFd::as_raw_fd)
            .unwrap_or(-1),
    };
    let use_drm_display = drm_params.render_fd >= 0;
    let mut advanced_control = 1i32;
    if !use_drm_display && adapter_vendor != Some(PCI_VENDOR_NVIDIA) {
        tracing::warn!(
            "[MPV-GL] no DRM render node available; falling back to the native display when available for hwdec probing"
        );
    }
    let mut params = Vec::with_capacity(5);
    if use_drm_display {
        params.push(sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_DRM_DISPLAY_V2,
            data: &mut drm_params as *mut sys::mpv_opengl_drm_params_v2 as *mut c_void,
        });
    } else if let Some((type_, data)) = native_display {
        params.push(sys::mpv_render_param { type_, data });
    }
    params.extend([
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_API_TYPE,
            data: api_type.as_ptr() as *mut c_void,
        },
        render_param(
            sys::mpv_render_param_type_MPV_RENDER_PARAM_OPENGL_INIT_PARAMS,
            &mut init,
        ),
        render_param(
            sys::mpv_render_param_type_MPV_RENDER_PARAM_ADVANCED_CONTROL,
            &mut advanced_control,
        ),
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
            data: std::ptr::null_mut(),
        },
    ]);
    let mut context = std::ptr::null_mut();
    // SAFETY: libmpv reads the live parameter array only during creation.
    let result = unsafe {
        sys::mpv_render_context_create(&mut context, mpv.ctx.as_ptr(), params.as_mut_ptr())
    };
    if result < 0 {
        anyhow::bail!("mpv composed OpenGL context creation failed: {result}");
    }
    Ok(context)
}

pub(super) fn render_param<T>(
    type_: sys::mpv_render_param_type,
    value: &mut T,
) -> sys::mpv_render_param {
    sys::mpv_render_param {
        type_,
        data: value as *mut T as *mut c_void,
    }
}

unsafe extern "C" fn mpv_get_proc_address(ctx: *mut c_void, name: *const c_char) -> *mut c_void {
    let Some(loader) = (unsafe { (ctx as *const EglProcLoader).as_ref() }) else {
        return std::ptr::null_mut();
    };
    let Some(egl) = (unsafe { loader.egl.as_ref() }) else {
        return std::ptr::null_mut();
    };
    let Ok(name) = (unsafe { CStr::from_ptr(name) }).to_str() else {
        return std::ptr::null_mut();
    };
    egl.get_proc_address(name)
        .map(|proc| proc as *const () as *mut c_void)
        .unwrap_or(std::ptr::null_mut())
}

pub(super) struct GlApi {
    pub(super) create_memory_objects: unsafe extern "system" fn(i32, *mut u32),
    pub(super) delete_memory_objects: unsafe extern "system" fn(i32, *const u32),
    pub(super) import_memory_fd: unsafe extern "system" fn(u32, u64, u32, i32),
    pub(super) gen_semaphores: unsafe extern "system" fn(i32, *mut u32),
    pub(super) delete_semaphores: unsafe extern "system" fn(i32, *const u32),
    pub(super) import_semaphore_fd: unsafe extern "system" fn(u32, u32, i32),
    pub(super) wait_semaphore:
        unsafe extern "system" fn(u32, u32, *const u32, u32, *const u32, *const u32),
    pub(super) signal_semaphore:
        unsafe extern "system" fn(u32, u32, *const u32, u32, *const u32, *const u32),
    pub(super) create_textures: unsafe extern "system" fn(u32, i32, *mut u32),
    pub(super) delete_textures: unsafe extern "system" fn(i32, *const u32),
    pub(super) texture_storage_mem_2d: unsafe extern "system" fn(u32, i32, u32, i32, i32, u32, u64),
    pub(super) gen_framebuffers: unsafe extern "system" fn(i32, *mut u32),
    pub(super) delete_framebuffers: unsafe extern "system" fn(i32, *const u32),
    pub(super) bind_framebuffer: unsafe extern "system" fn(u32, u32),
    pub(super) framebuffer_texture_2d: unsafe extern "system" fn(u32, u32, u32, u32, i32),
    pub(super) check_framebuffer_status: unsafe extern "system" fn(u32) -> u32,
    pub(super) viewport: unsafe extern "system" fn(i32, i32, i32, i32),
    finish: unsafe extern "system" fn(),
}

impl GlApi {
    pub(super) fn load(egl: &EglApi) -> anyhow::Result<Self> {
        Ok(Self {
            create_memory_objects: load_gl(egl, "glCreateMemoryObjectsEXT")?,
            delete_memory_objects: load_gl(egl, "glDeleteMemoryObjectsEXT")?,
            import_memory_fd: load_gl(egl, "glImportMemoryFdEXT")?,
            gen_semaphores: load_gl(egl, "glGenSemaphoresEXT")?,
            delete_semaphores: load_gl(egl, "glDeleteSemaphoresEXT")?,
            import_semaphore_fd: load_gl(egl, "glImportSemaphoreFdEXT")?,
            wait_semaphore: load_gl(egl, "glWaitSemaphoreEXT")?,
            signal_semaphore: load_gl(egl, "glSignalSemaphoreEXT")?,
            create_textures: load_gl(egl, "glCreateTextures")?,
            delete_textures: load_gl(egl, "glDeleteTextures")?,
            texture_storage_mem_2d: load_gl(egl, "glTextureStorageMem2DEXT")?,
            gen_framebuffers: load_gl(egl, "glGenFramebuffers")?,
            delete_framebuffers: load_gl(egl, "glDeleteFramebuffers")?,
            bind_framebuffer: load_gl(egl, "glBindFramebuffer")?,
            framebuffer_texture_2d: load_gl(egl, "glFramebufferTexture2D")?,
            check_framebuffer_status: load_gl(egl, "glCheckFramebufferStatus")?,
            viewport: load_gl(egl, "glViewport")?,
            finish: load_gl(egl, "glFinish")?,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum GlSyncPolicy {
    Semaphore,
    Finish,
}

impl GlSyncPolicy {
    pub(super) fn from_env() -> Self {
        match std::env::var("KLD_MPV_GL_SYNC")
            .unwrap_or_else(|_| "semaphore".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "finish" | "strict" => Self::Finish,
            _ => Self::Semaphore,
        }
    }

    pub(super) fn apply(self, gl: &GlApi) {
        if matches!(self, Self::Finish) {
            // SAFETY: loaded from the current GL context. This diagnostic mode
            // adds a CPU-visible wait after the production semaphore handoff.
            unsafe { (gl.finish)() }
        }
    }
}

fn load_gl<T: Copy>(egl: &EglApi, name: &str) -> anyhow::Result<T> {
    let proc = egl
        .get_proc_address(name)
        .with_context(|| format!("loading OpenGL symbol {name}"))?;
    debug_assert_eq!(std::mem::size_of::<T>(), std::mem::size_of_val(&proc));
    // SAFETY: every requested T is the ABI-correct function pointer for name.
    Ok(unsafe { std::mem::transmute_copy(&proc) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendor_matching_requires_both_sides_known() {
        assert_eq!(vendor_matches(Some(0x8086), Some(0x8086)), Some(true));
        assert_eq!(vendor_matches(Some(0x1002), Some(0x8086)), Some(false));
        assert_eq!(vendor_matches(None, Some(0x8086)), None);
        assert_eq!(vendor_matches(Some(0x8086), None), None);
        assert_eq!(vendor_matches(None, None), None);
    }

    #[test]
    fn nvidia_vendor_id_is_the_exclusion_value() {
        assert_eq!(PCI_VENDOR_NVIDIA, 0x10DE);
    }
}
