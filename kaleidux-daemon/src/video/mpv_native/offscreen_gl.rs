use anyhow::Context;
use khronos_egl as egl;
use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::ffi::{CStr, CString, c_char, c_void};
use std::os::fd::IntoRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::MpvComposedVideoTarget;
use crate::renderer::create_exportable_rgba_texture;
use crate::video::{GlExternalFrame, VideoFrame, VideoFrameFormat};

const EGL_PLATFORM_WAYLAND_KHR: egl::Enum = 0x31D8;
const GL_TEXTURE_2D: u32 = 0x0DE1;
const GL_RGBA8: u32 = 0x8058;
const GL_FRAMEBUFFER: u32 = 0x8D40;
const GL_COLOR_ATTACHMENT0: u32 = 0x8CE0;
const GL_FRAMEBUFFER_COMPLETE: u32 = 0x8CD5;
const GL_HANDLE_TYPE_OPAQUE_FD_EXT: u32 = 0x9586;
type EglApi = egl::DynamicInstance<egl::EGL1_5>;

pub(super) struct ComposedGlRenderContext {
    egl: Box<EglApi>,
    display: egl::Display,
    context: egl::Context,
    surface: egl::Surface,
    mpv_context: *mut sys::mpv_render_context,
    proc_loader: Box<EglProcLoader>,
    gl: GlApi,
    sync_policy: GlSyncPolicy,
    slots: Vec<SharedGlSlot>,
    next_slot: usize,
    width: i32,
    height: i32,
}

impl ComposedGlRenderContext {
    pub(super) fn new(mpv: &Mpv, target: &MpvComposedVideoTarget) -> anyhow::Result<Self> {
        let (width, height) = target.size();
        // SAFETY: the dynamic EGL loader validates required symbols before returning.
        let egl = Box::new(unsafe { EglApi::load_required() }.context("loading EGL API")?);
        // SAFETY: display_ptr is the live Wayland display owned by the backend.
        let display = unsafe {
            egl.get_platform_display(
                EGL_PLATFORM_WAYLAND_KHR,
                target.display_ptr(),
                &[egl::ATTRIB_NONE],
            )
        }
        .context("creating composed mpv EGL display")?;
        egl.initialize(display)
            .context("initializing composed mpv EGL display")?;
        egl.bind_api(egl::OPENGL_API)
            .context("binding composed mpv OpenGL API")?;
        let config = egl
            .choose_first_config(
                display,
                &[
                    egl::SURFACE_TYPE,
                    egl::PBUFFER_BIT,
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
            .context("choosing composed mpv EGL config")?
            .context("no EGL config supports composed mpv OpenGL")?;
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
            .context("creating composed mpv EGL context")?;
        let surface = egl
            .create_pbuffer_surface(display, config, &[egl::WIDTH, 1, egl::HEIGHT, 1, egl::NONE])
            .context("creating composed mpv EGL pbuffer")?;
        egl.make_current(display, Some(surface), Some(surface), Some(context))
            .context("making composed mpv EGL context current")?;
        let gl = GlApi::load(&egl)?;
        let proc_loader = Box::new(EglProcLoader {
            egl: egl.as_ref() as *const EglApi,
        });
        let mpv_context = create_mpv_gl_context(mpv, target.display_ptr(), proc_loader.as_ref())?;
        let mut slots = Vec::with_capacity(3);
        for _ in 0..3 {
            slots.push(SharedGlSlot::new(&gl, &target.wgpu_ctx, width, height)?);
        }
        Ok(Self {
            egl,
            display,
            context,
            surface,
            mpv_context,
            proc_loader,
            gl,
            sync_policy: GlSyncPolicy::from_env(),
            slots,
            next_slot: 0,
            width,
            height,
        })
    }

    pub(super) fn render_frame(&mut self, session_id: u64) -> anyhow::Result<Option<VideoFrame>> {
        // SAFETY: mpv_context is live and used only from this thread.
        unsafe { sys::mpv_render_context_update(self.mpv_context) };
        let Some(slot_index) = self.find_available_slot() else {
            return Ok(None);
        };
        self.egl
            .make_current(
                self.display,
                Some(self.surface),
                Some(self.surface),
                Some(self.context),
            )
            .context("making composed mpv EGL context current for render")?;
        let slot = &self.slots[slot_index];
        unsafe {
            (self.gl.bind_framebuffer)(GL_FRAMEBUFFER, slot.framebuffer);
            (self.gl.viewport)(0, 0, self.width, self.height);
        }
        let mut fbo = sys::mpv_opengl_fbo {
            fbo: slot.framebuffer as i32,
            w: self.width,
            h: self.height,
            internal_format: GL_RGBA8 as i32,
        };
        let mut flip_y = 0i32;
        let mut block_for_target_time = 0i32;
        let mut params = [
            render_param(
                sys::mpv_render_param_type_MPV_RENDER_PARAM_OPENGL_FBO,
                &mut fbo,
            ),
            render_param(
                sys::mpv_render_param_type_MPV_RENDER_PARAM_FLIP_Y,
                &mut flip_y,
            ),
            render_param(
                sys::mpv_render_param_type_MPV_RENDER_PARAM_BLOCK_FOR_TARGET_TIME,
                &mut block_for_target_time,
            ),
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];
        // SAFETY: the framebuffer and render parameters remain live through this call.
        let result =
            unsafe { sys::mpv_render_context_render(self.mpv_context, params.as_mut_ptr()) };
        if result < 0 {
            anyhow::bail!("mpv composed OpenGL render failed: {result}");
        }
        self.sync_policy.apply(&self.gl);
        // SAFETY: the mpv render context remains live after the completed GL render.
        unsafe { sys::mpv_render_context_report_swap(self.mpv_context) };
        slot.busy.store(true, Ordering::Release);
        self.next_slot = (slot_index + 1) % self.slots.len();
        Ok(Some(VideoFrame {
            buffer: gstreamer::Buffer::new(),
            width: self.width as u32,
            height: self.height as u32,
            stride: 0,
            format: VideoFrameFormat::GlExternalRgba {
                frame: GlExternalFrame::new(slot.texture.clone(), slot.busy.clone()),
            },
            session_id,
            pts_ns: None,
            duration_ns: None,
        }))
    }

    fn find_available_slot(&self) -> Option<usize> {
        (0..self.slots.len())
            .map(|offset| (self.next_slot + offset) % self.slots.len())
            .find(|index| !self.slots[*index].busy.load(Ordering::Acquire))
    }
}

impl Drop for ComposedGlRenderContext {
    fn drop(&mut self) {
        let _ = self.proc_loader.egl;
        // SAFETY: the mpv context and GL objects are owned by this current context.
        unsafe {
            sys::mpv_render_context_free(self.mpv_context);
            for slot in &self.slots {
                (self.gl.delete_framebuffers)(1, &slot.framebuffer);
                (self.gl.delete_textures)(1, &slot.gl_texture);
                (self.gl.delete_memory_objects)(1, &slot.memory_object);
            }
        }
        let _ = self.egl.make_current(self.display, None, None, None);
        let _ = self.egl.destroy_surface(self.display, self.surface);
        let _ = self.egl.destroy_context(self.display, self.context);
    }
}

struct SharedGlSlot {
    texture: Arc<wgpu::Texture>,
    busy: Arc<AtomicBool>,
    memory_object: u32,
    gl_texture: u32,
    framebuffer: u32,
}

impl SharedGlSlot {
    fn new(
        gl: &GlApi,
        wgpu_ctx: &crate::renderer::WgpuContext,
        width: i32,
        height: i32,
    ) -> anyhow::Result<Self> {
        let exported = create_exportable_rgba_texture(
            &wgpu_ctx.device,
            width as u32,
            height as u32,
            "libmpv GL Shared RGBA Texture",
        )
        .context("allocating Vulkan-exported libmpv GL texture")?;
        let mut memory_object = 0;
        let mut gl_texture = 0;
        let mut framebuffer = 0;
        // SAFETY: every OpenGL function pointer was loaded from the current EGL context.
        unsafe {
            (gl.create_memory_objects)(1, &mut memory_object);
            (gl.import_memory_fd)(
                memory_object,
                exported.memory_size,
                GL_HANDLE_TYPE_OPAQUE_FD_EXT,
                exported.memory_fd.into_raw_fd(),
            );
            (gl.create_textures)(GL_TEXTURE_2D, 1, &mut gl_texture);
            (gl.texture_storage_mem_2d)(gl_texture, 1, GL_RGBA8, width, height, memory_object, 0);
            (gl.gen_framebuffers)(1, &mut framebuffer);
            (gl.bind_framebuffer)(GL_FRAMEBUFFER, framebuffer);
            (gl.framebuffer_texture_2d)(
                GL_FRAMEBUFFER,
                GL_COLOR_ATTACHMENT0,
                GL_TEXTURE_2D,
                gl_texture,
                0,
            );
            let status = (gl.check_framebuffer_status)(GL_FRAMEBUFFER);
            (gl.bind_framebuffer)(GL_FRAMEBUFFER, 0);
            if status != GL_FRAMEBUFFER_COMPLETE {
                anyhow::bail!("shared OpenGL framebuffer is incomplete: 0x{status:x}");
            }
        }
        Ok(Self {
            texture: exported.texture,
            busy: Arc::new(AtomicBool::new(false)),
            memory_object,
            gl_texture,
            framebuffer,
        })
    }
}

fn render_param<T>(type_: sys::mpv_render_param_type, value: &mut T) -> sys::mpv_render_param {
    sys::mpv_render_param {
        type_,
        data: value as *mut T as *mut c_void,
    }
}

struct EglProcLoader {
    egl: *const EglApi,
}

fn create_mpv_gl_context(
    mpv: &Mpv,
    wayland_display: *mut c_void,
    loader: &EglProcLoader,
) -> anyhow::Result<*mut sys::mpv_render_context> {
    let api_type = CString::new("opengl")?;
    let mut init = sys::mpv_opengl_init_params {
        get_proc_address: Some(mpv_get_proc_address),
        get_proc_address_ctx: loader as *const EglProcLoader as *mut c_void,
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
        render_param(
            sys::mpv_render_param_type_MPV_RENDER_PARAM_OPENGL_INIT_PARAMS,
            &mut init,
        ),
        sys::mpv_render_param {
            type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
            data: std::ptr::null_mut(),
        },
    ];
    let mut context = std::ptr::null_mut();
    // SAFETY: libmpv reads the live parameter array only during context creation.
    let result = unsafe {
        sys::mpv_render_context_create(&mut context, mpv.ctx.as_ptr(), params.as_mut_ptr())
    };
    if result < 0 {
        anyhow::bail!("mpv composed OpenGL context creation failed: {result}");
    }
    Ok(context)
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

struct GlApi {
    create_memory_objects: unsafe extern "system" fn(i32, *mut u32),
    delete_memory_objects: unsafe extern "system" fn(i32, *const u32),
    import_memory_fd: unsafe extern "system" fn(u32, u64, u32, i32),
    create_textures: unsafe extern "system" fn(u32, i32, *mut u32),
    delete_textures: unsafe extern "system" fn(i32, *const u32),
    texture_storage_mem_2d: unsafe extern "system" fn(u32, i32, u32, i32, i32, u32, u64),
    gen_framebuffers: unsafe extern "system" fn(i32, *mut u32),
    delete_framebuffers: unsafe extern "system" fn(i32, *const u32),
    bind_framebuffer: unsafe extern "system" fn(u32, u32),
    framebuffer_texture_2d: unsafe extern "system" fn(u32, u32, u32, u32, i32),
    check_framebuffer_status: unsafe extern "system" fn(u32) -> u32,
    viewport: unsafe extern "system" fn(i32, i32, i32, i32),
    flush: unsafe extern "system" fn(),
    finish: unsafe extern "system" fn(),
}

#[derive(Clone, Copy)]
enum GlSyncPolicy {
    Flush,
    Finish,
}

impl GlSyncPolicy {
    fn from_env() -> Self {
        match std::env::var("KLD_MPV_GL_SYNC")
            .unwrap_or_else(|_| "flush".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "finish" | "strict" => Self::Finish,
            _ => Self::Flush,
        }
    }

    fn apply(self, gl: &GlApi) {
        // SAFETY: both functions are loaded from the current GL context and
        // only order the already-submitted libmpv draw for interop visibility.
        unsafe {
            match self {
                Self::Flush => (gl.flush)(),
                Self::Finish => (gl.finish)(),
            }
        }
    }
}

impl GlApi {
    fn load(egl: &EglApi) -> anyhow::Result<Self> {
        Ok(Self {
            create_memory_objects: load_gl(egl, "glCreateMemoryObjectsEXT")?,
            delete_memory_objects: load_gl(egl, "glDeleteMemoryObjectsEXT")?,
            import_memory_fd: load_gl(egl, "glImportMemoryFdEXT")?,
            create_textures: load_gl(egl, "glCreateTextures")?,
            delete_textures: load_gl(egl, "glDeleteTextures")?,
            texture_storage_mem_2d: load_gl(egl, "glTextureStorageMem2DEXT")?,
            gen_framebuffers: load_gl(egl, "glGenFramebuffers")?,
            delete_framebuffers: load_gl(egl, "glDeleteFramebuffers")?,
            bind_framebuffer: load_gl(egl, "glBindFramebuffer")?,
            framebuffer_texture_2d: load_gl(egl, "glFramebufferTexture2D")?,
            check_framebuffer_status: load_gl(egl, "glCheckFramebufferStatus")?,
            viewport: load_gl(egl, "glViewport")?,
            flush: load_gl(egl, "glFlush")?,
            finish: load_gl(egl, "glFinish")?,
        })
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
