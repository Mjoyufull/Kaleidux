use anyhow::Context;
use khronos_egl as egl;
use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::ffi::{CStr, CString, c_char, c_void};
use std::os::fd::RawFd;
use std::time::Duration;
use wayland_egl::WlEglSurface;

use super::MpvNativeVideoTarget;

const EGL_PLATFORM_WAYLAND_KHR: egl::Enum = 0x31D8;
const RENDER_WITHOUT_TARGET_BLOCK: i32 = 0;
const MPV_RENDER_UPDATE_FRAME_FLAG: u64 =
    sys::mpv_render_update_flag_MPV_RENDER_UPDATE_FRAME as u64;

type EglApi = egl::DynamicInstance<egl::EGL1_5>;

pub(super) struct NativeGlRenderContext {
    egl: Box<EglApi>,
    display: egl::Display,
    context: egl::Context,
    surface: egl::Surface,
    egl_window: WlEglSurface,
    mpv_context: *mut sys::mpv_render_context,
    proc_loader: Box<EglProcLoader>,
    update_wake: NativeRenderWake,
    update_callback_fd: Box<RawFd>,
    width: i32,
    height: i32,
}

impl NativeGlRenderContext {
    pub(super) fn new(mpv: &Mpv, target: &MpvNativeVideoTarget) -> anyhow::Result<Self> {
        let (width, height) = target.size();
        // SAFETY: the dynamic EGL loader validates the required symbols before returning.
        let egl =
            Box::new(unsafe { EglApi::load_required() }.context("loading libEGL 1.5 dynamic API")?);
        // SAFETY: target.display_ptr() is the live Wayland display owned by the backend.
        let display = unsafe {
            egl.get_platform_display(
                EGL_PLATFORM_WAYLAND_KHR,
                target.display_ptr(),
                &[egl::ATTRIB_NONE],
            )
        }
        .context("creating EGL Wayland platform display")?;
        egl.initialize(display)
            .context("initializing EGL display")?;
        egl.bind_api(egl::OPENGL_API)
            .context("binding EGL OpenGL API")?;

        let config = egl
            .choose_first_config(
                display,
                &[
                    egl::SURFACE_TYPE,
                    egl::WINDOW_BIT,
                    egl::RENDERABLE_TYPE,
                    egl::OPENGL_BIT,
                    egl::RED_SIZE,
                    8,
                    egl::GREEN_SIZE,
                    8,
                    egl::BLUE_SIZE,
                    8,
                    egl::ALPHA_SIZE,
                    8,
                    egl::NONE,
                ],
            )
            .context("choosing EGL config")?
            .context("no EGL config supports Wayland OpenGL window rendering")?;

        let context = egl
            .create_context(
                display,
                config,
                None,
                &[
                    egl::CONTEXT_MAJOR_VERSION,
                    3,
                    egl::CONTEXT_MINOR_VERSION,
                    2,
                    egl::CONTEXT_OPENGL_PROFILE_MASK,
                    egl::CONTEXT_OPENGL_CORE_PROFILE_BIT,
                    egl::NONE,
                ],
            )
            .or_else(|_| egl.create_context(display, config, None, &[egl::NONE]))
            .context("creating EGL OpenGL context")?;

        let egl_window = WlEglSurface::new(target.surface_id.clone(), width, height)
            .context("creating wl_egl_window for mpv native surface")?;
        // SAFETY: egl_window is live and belongs to the selected Wayland EGL display.
        let surface = unsafe {
            egl.create_platform_window_surface(
                display,
                config,
                egl_window.ptr() as *mut c_void,
                &[egl::ATTRIB_NONE],
            )
        }
        .context("creating EGL window surface")?;
        egl.make_current(display, Some(surface), Some(surface), Some(context))
            .context("making EGL context current")?;
        let _ = egl.swap_interval(display, 0);

        let proc_loader = Box::new(EglProcLoader {
            egl: egl.as_ref() as *const EglApi,
        });
        let mpv_context = create_mpv_gl_context(mpv, target.display_ptr(), proc_loader.as_ref())?;
        let update_wake = NativeRenderWake::new().context("creating native mpv render wake fd")?;
        let update_callback_fd = Box::new(update_wake.fd);
        // SAFETY: update_callback_fd is boxed and outlives the registered callback.
        unsafe {
            sys::mpv_render_context_set_update_callback(
                mpv_context,
                Some(native_render_update_callback),
                update_callback_fd.as_ref() as *const RawFd as *mut c_void,
            );
        }

        Ok(Self {
            egl,
            display,
            context,
            surface,
            egl_window,
            mpv_context,
            proc_loader,
            update_wake,
            update_callback_fd,
            width,
            height,
        })
    }

    pub(super) fn drain_pending_updates(&self) {
        self.update_wake.wait(Duration::ZERO);
    }

    pub(super) fn render(&mut self, force_redraw: bool) -> anyhow::Result<bool> {
        self.egl
            .make_current(
                self.display,
                Some(self.surface),
                Some(self.surface),
                Some(self.context),
            )
            .context("making native mpv EGL context current for render")?;

        if !force_redraw {
            // SAFETY: mpv_context is live and used only by this render thread.
            let update_flags = unsafe { sys::mpv_render_context_update(self.mpv_context) };
            if update_flags & MPV_RENDER_UPDATE_FRAME_FLAG == 0 {
                return Ok(false);
            }
        }

        self.viewport();
        let mut fbo = sys::mpv_opengl_fbo {
            fbo: 0,
            w: self.width,
            h: self.height,
            internal_format: 0,
        };
        let mut flip_y = 1i32;
        let mut block_for_target_time = RENDER_WITHOUT_TARGET_BLOCK;
        let mut params = [
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_OPENGL_FBO,
                data: &mut fbo as *mut _ as *mut c_void,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_FLIP_Y,
                data: &mut flip_y as *mut _ as *mut c_void,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_BLOCK_FOR_TARGET_TIME,
                data: &mut block_for_target_time as *mut _ as *mut c_void,
            },
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];
        // SAFETY: all FBO render parameters remain live through the render call.
        let result =
            unsafe { sys::mpv_render_context_render(self.mpv_context, params.as_mut_ptr()) };
        if result < 0 {
            anyhow::bail!("mpv native OpenGL render failed: {}", result);
        }
        self.egl
            .swap_buffers(self.display, self.surface)
            .context("swapping native mpv EGL buffers")?;
        // SAFETY: mpv_context remains live and the EGL swap completed successfully.
        unsafe { sys::mpv_render_context_report_swap(self.mpv_context) };
        Ok(true)
    }

    fn viewport(&self) {
        let Some(proc) = self.egl.get_proc_address("glViewport") else {
            return;
        };
        // SAFETY: EGL returned the glViewport symbol with the OpenGL ABI.
        let viewport: unsafe extern "system" fn(i32, i32, i32, i32) = unsafe {
            std::mem::transmute::<extern "system" fn(), unsafe extern "system" fn(i32, i32, i32, i32)>(
                proc,
            )
        };
        // SAFETY: the OpenGL context is current and dimensions are validated positive values.
        unsafe { viewport(0, 0, self.width, self.height) };
    }
}

impl Drop for NativeGlRenderContext {
    fn drop(&mut self) {
        let _ = self.proc_loader.egl;
        let _ = *self.update_callback_fd;
        // SAFETY: clearing the callback precedes freeing the live mpv render context.
        unsafe {
            sys::mpv_render_context_set_update_callback(
                self.mpv_context,
                None,
                std::ptr::null_mut(),
            );
            sys::mpv_render_context_free(self.mpv_context);
        }
        let _ = self.egl.make_current(self.display, None, None, None);
        let _ = self.egl.destroy_surface(self.display, self.surface);
        let _ = self.egl.destroy_context(self.display, self.context);
        let _ = self.egl_window.ptr();
    }
}

struct EglProcLoader {
    egl: *const EglApi,
}

fn create_mpv_gl_context(
    mpv: &Mpv,
    wayland_display: *mut c_void,
    proc_loader: &EglProcLoader,
) -> anyhow::Result<*mut sys::mpv_render_context> {
    let api_type = CString::new("opengl")?;
    let mut init_params = sys::mpv_opengl_init_params {
        get_proc_address: Some(mpv_get_proc_address),
        get_proc_address_ctx: proc_loader as *const EglProcLoader as *mut c_void,
    };
    let mut params = [
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_WL_DISPLAY,
            data: wayland_display,
        },
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_API_TYPE,
            data: api_type.as_ptr() as *mut c_void,
        },
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_OPENGL_INIT_PARAMS,
            data: &mut init_params as *mut _ as *mut c_void,
        },
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
            data: std::ptr::null_mut(),
        },
    ];
    let mut context = std::ptr::null_mut();
    // SAFETY: libmpv reads the live parameter array only for this context creation call.
    let result = unsafe {
        sys::mpv_render_context_create(&mut context, mpv.ctx.as_ptr(), params.as_mut_ptr())
    };
    if result < 0 {
        anyhow::bail!(
            "mpv native OpenGL render context creation failed: {}",
            result
        );
    }
    Ok(context)
}

unsafe extern "C" fn mpv_get_proc_address(ctx: *mut c_void, name: *const c_char) -> *mut c_void {
    // SAFETY: libmpv passes back the EglProcLoader pointer registered at context creation.
    let Some(loader) = (unsafe { (ctx as *const EglProcLoader).as_ref() }) else {
        return std::ptr::null_mut();
    };
    // SAFETY: proc_loader is retained by NativeGlRenderContext for the callback lifetime.
    let Some(egl) = (unsafe { loader.egl.as_ref() }) else {
        return std::ptr::null_mut();
    };
    // SAFETY: libmpv supplies a NUL-terminated OpenGL symbol name for this callback.
    let Ok(name) = (unsafe { CStr::from_ptr(name) }).to_str() else {
        return std::ptr::null_mut();
    };
    egl.get_proc_address(name)
        .map(|proc| proc as *const () as *mut c_void)
        .unwrap_or(std::ptr::null_mut())
}

struct NativeRenderWake {
    fd: RawFd,
}

impl NativeRenderWake {
    fn new() -> anyhow::Result<Self> {
        // SAFETY: eventfd has no borrowed pointer arguments and returns an owned descriptor.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            anyhow::bail!("eventfd failed: {}", std::io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    fn wait(&self, timeout: Duration) {
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut poll_fd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll_fd points to one initialized pollfd for the duration of the call.
        let ret = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
        if ret <= 0 || poll_fd.revents & libc::POLLIN == 0 {
            return;
        }
        let mut value = 0u64;
        loop {
            // SAFETY: value is writable for one u64 and self.fd is this wake object's fd.
            let read = unsafe {
                libc::read(
                    self.fd,
                    &mut value as *mut u64 as *mut c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if read as usize != std::mem::size_of::<u64>() {
                break;
            }
        }
    }
}

impl Drop for NativeRenderWake {
    fn drop(&mut self) {
        // SAFETY: self.fd is owned by this wake object and closed exactly once here.
        unsafe { libc::close(self.fd) };
    }
}

unsafe extern "C" fn native_render_update_callback(callback_ctx: *mut c_void) {
    // SAFETY: callback_ctx is the boxed RawFd retained by NativeGlRenderContext.
    let Some(fd) = (unsafe { (callback_ctx as *const RawFd).as_ref() }) else {
        return;
    };
    let value = 1u64;
    // SAFETY: fd is a live nonblocking eventfd and value is readable for one u64.
    unsafe {
        libc::write(
            *fd,
            &value as *const u64 as *const c_void,
            std::mem::size_of::<u64>(),
        );
    }
}
