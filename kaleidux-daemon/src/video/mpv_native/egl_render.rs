use anyhow::Context;
use khronos_egl as egl;
use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::ffi::c_void;
use std::os::fd::OwnedFd;
use wayland_egl::WlEglSurface;

use super::MpvNativeVideoTarget;
use super::offscreen_gl_support::{
    EglApi, EglProcLoader, create_mpv_gl_context, select_hwdec_display_resource,
};
use super::render_wake::{RenderWake, render_update_callback};

const EGL_PLATFORM_WAYLAND_KHR: egl::Enum = 0x31D8;
const MPV_RENDER_UPDATE_FRAME_FLAG: u64 =
    sys::mpv_render_update_flag_MPV_RENDER_UPDATE_FRAME as u64;

pub(super) struct NativeGlRenderContext {
    egl: Box<EglApi>,
    display: egl::Display,
    context: egl::Context,
    surface: egl::Surface,
    egl_window: WlEglSurface,
    mpv_context: *mut sys::mpv_render_context,
    _proc_loader: Box<EglProcLoader>,
    update_wake: RenderWake,
    width: i32,
    height: i32,
    #[allow(dead_code)]
    drm_render_fd: Option<OwnedFd>,
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

        let proc_loader = Box::new(EglProcLoader::new(egl.as_ref()));
        let drm_render_fd = select_hwdec_display_resource(None);
        let mpv_context = create_mpv_gl_context(
            mpv,
            Some((
                sys::mpv_render_param_type_MPV_RENDER_PARAM_WL_DISPLAY,
                target.display_ptr(),
            )),
            drm_render_fd.as_ref(),
            None,
            proc_loader.as_ref(),
        )?;
        let update_wake = RenderWake::new().context("creating native mpv render wake fd")?;
        // SAFETY: update_wake owns a boxed fd that outlives the callback.
        unsafe {
            sys::mpv_render_context_set_update_callback(
                mpv_context,
                Some(render_update_callback),
                update_wake.callback_context(),
            );
        }

        Ok(Self {
            egl,
            display,
            context,
            surface,
            egl_window,
            mpv_context,
            _proc_loader: proc_loader,
            update_wake,
            width,
            height,
            drm_render_fd,
        })
    }

    pub(super) fn wait_for_update_or_stop(&self, stop: &RenderWake) -> bool {
        self.update_wake.wait_for_update_or_stop(stop)
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
