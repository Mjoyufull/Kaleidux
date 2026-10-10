use anyhow::Context;
use khronos_egl as egl;
use libmpv2::Mpv;
use libmpv2_sys as sys;
use std::os::fd::{IntoRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::MpvComposedVideoTarget;
use super::offscreen_gl_support::{
    EglApi, EglProcLoader, GlApi, GlSyncPolicy, create_mpv_gl_context, render_param,
    select_hwdec_display_resource,
};
use super::render_wake::{RenderWake, render_update_callback};
use crate::renderer::{create_exportable_rgba_texture, prime_shared_texture_for_gl};
use crate::video::{GlExternalFrame, VideoFrame, VideoFrameFormat};

const GL_TEXTURE_2D: u32 = 0x0DE1;
const GL_RGBA8: u32 = 0x8058;
const GL_FRAMEBUFFER: u32 = 0x8D40;
const GL_COLOR_ATTACHMENT0: u32 = 0x8CE0;
const GL_FRAMEBUFFER_COMPLETE: u32 = 0x8CD5;
const GL_HANDLE_TYPE_OPAQUE_FD_EXT: u32 = 0x9586;
const GL_LAYOUT_SHADER_READ_ONLY_EXT: u32 = 0x9591;

// Declared before GL allocations so error unwinding deletes them while their
// context is still current, then releases the EGL context and pbuffer.
struct EglConstructionGuard<'a> {
    egl: &'a EglApi,
    display: egl::Display,
    context: egl::Context,
    surface: Option<egl::Surface>,
    armed: bool,
}

impl Drop for EglConstructionGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.egl.make_current(self.display, None, None, None);
            if let Some(surface) = self.surface {
                let _ = self.egl.destroy_surface(self.display, surface);
            }
            let _ = self.egl.destroy_context(self.display, self.context);
        }
    }
}

pub(super) struct ComposedGlRenderContext {
    egl: Box<EglApi>,
    display: egl::Display,
    context: egl::Context,
    surface: egl::Surface,
    mpv_context: *mut sys::mpv_render_context,
    _proc_loader: Box<EglProcLoader>,
    update_wake: RenderWake,
    gl: GlApi,
    sync_policy: GlSyncPolicy,
    slots: Vec<SharedGlSlot>,
    next_slot: usize,
    width: i32,
    height: i32,
    // Kept alive for the whole lifetime of the mpv render context: mpv only
    // copies the params struct, not the fd, and libva does not take ownership.
    #[allow(dead_code)]
    drm_render_fd: Option<OwnedFd>,
}

impl ComposedGlRenderContext {
    pub(super) fn new(mpv: &Mpv, target: &MpvComposedVideoTarget) -> anyhow::Result<Self> {
        let (width, height) = target.size();
        // SAFETY: the dynamic EGL loader validates required symbols before returning.
        let egl = Box::new(unsafe { EglApi::load_required() }.context("loading EGL API")?);
        // SAFETY: display_ptr is the live native display connection selected
        // by the composed target (wl_display or xcb_connection_t).
        let display = unsafe {
            egl.get_platform_display(
                target.egl_platform(),
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
        let mut construction = EglConstructionGuard {
            egl: &egl,
            display,
            context,
            surface: None,
            armed: true,
        };
        let surface = egl
            .create_pbuffer_surface(display, config, &[egl::WIDTH, 1, egl::HEIGHT, 1, egl::NONE])
            .context("creating composed mpv EGL pbuffer")?;
        construction.surface = Some(surface);
        egl.make_current(display, Some(surface), Some(surface), Some(context))
            .context("making composed mpv EGL context current")?;
        let gl = GlApi::load(&egl)?;
        let proc_loader = Box::new(EglProcLoader::new(egl.as_ref()));
        let adapter_vendor = target.wgpu_ctx.adapter.get_info().vendor;
        let drm_render_fd = select_hwdec_display_resource(Some(adapter_vendor));
        let update_wake = RenderWake::new().context("creating composed mpv render wake fd")?;
        let mut slots = Vec::with_capacity(3);
        for _ in 0..3 {
            slots.push(SharedGlSlot::new(&gl, &target.wgpu_ctx, width, height)?);
        }
        let mpv_context = create_mpv_gl_context(
            mpv,
            target.mpv_native_display_param(),
            drm_render_fd.as_ref(),
            Some(adapter_vendor),
            proc_loader.as_ref(),
        )?;
        // SAFETY: update_wake owns a boxed fd that outlives the callback.
        unsafe {
            sys::mpv_render_context_set_update_callback(
                mpv_context,
                Some(render_update_callback),
                update_wake.callback_context(),
            );
        }
        construction.armed = false;
        drop(construction);
        Ok(Self {
            egl,
            display,
            context,
            surface,
            mpv_context,
            _proc_loader: proc_loader,
            update_wake,
            gl,
            sync_policy: GlSyncPolicy::from_env(),
            slots,
            next_slot: 0,
            width,
            height,
            drm_render_fd,
        })
    }

    pub(super) fn wait_for_update_or_stop(&self, stop: &RenderWake) -> bool {
        self.update_wake.wait_for_update_or_stop(stop)
    }

    pub(super) fn render_frame(
        &mut self,
        session_id: u64,
        publish: bool,
    ) -> anyhow::Result<Option<VideoFrame>> {
        self.egl
            .make_current(
                self.display,
                Some(self.surface),
                Some(self.surface),
                Some(self.context),
            )
            .context("making composed mpv EGL context current for render")?;
        // SAFETY: mpv_context is live, its GL context is current, and it is
        // used only from this render thread.
        let update_flags = unsafe { sys::mpv_render_context_update(self.mpv_context) };
        if update_flags & (sys::mpv_render_update_flag_MPV_RENDER_UPDATE_FRAME as u64) == 0 {
            return Ok(None);
        }
        // Updates may race a core reconfiguration and have no queued image.
        // Publishing a cleared offscreen FBO would flash through the wallpaper.
        let mut frame_info = sys::mpv_render_frame_info {
            flags: 0,
            target_time: 0,
        };
        let parameter = render_param(
            sys::mpv_render_param_type_MPV_RENDER_PARAM_NEXT_FRAME_INFO,
            &mut frame_info,
        );
        // SAFETY: the render context and frame-info output live through the call.
        let info_result = unsafe { sys::mpv_render_context_get_info(self.mpv_context, parameter) };
        if info_result >= 0 && frame_info.flags & 1 == 0 {
            return Ok(None);
        }
        if !publish {
            self.skip_frame()?;
            return Ok(None);
        }
        let Some(slot_index) = self.find_available_slot() else {
            self.skip_frame()?;
            return Ok(None);
        };
        let slot = &self.slots[slot_index];
        slot.acquire_for_gl(&self.gl);
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
        let mut params = [
            render_param(
                sys::mpv_render_param_type_MPV_RENDER_PARAM_OPENGL_FBO,
                &mut fbo,
            ),
            render_param(
                sys::mpv_render_param_type_MPV_RENDER_PARAM_FLIP_Y,
                &mut flip_y,
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
        slot.release_to_vulkan(&self.gl);
        self.sync_policy.apply(&self.gl);
        slot.busy.store(true, Ordering::Release);
        self.next_slot = (slot_index + 1) % self.slots.len();
        Ok(Some(VideoFrame {
            storage: crate::video::VideoFrameStorage::External,
            width: self.width as u32,
            height: self.height as u32,
            stride: 0,
            format: VideoFrameFormat::GlExternalRgba {
                frame: GlExternalFrame::new(
                    slot.view.clone(),
                    slot.sync.clone(),
                    slot.wgpu_ctx.clone(),
                    slot.busy.clone(),
                ),
            },
            session_id,
            pts_ns: None,
            duration_ns: None,
            color: Default::default(),
            geometry: crate::video::VideoGeometry::for_dimensions(
                self.width as u32,
                self.height as u32,
            ),
        }))
    }

    fn find_available_slot(&self) -> Option<usize> {
        (0..self.slots.len())
            .map(|offset| (self.next_slot + offset) % self.slots.len())
            .find(|index| !self.slots[*index].busy.load(Ordering::Acquire))
    }

    fn skip_frame(&self) -> anyhow::Result<()> {
        let mut skip = 1i32;
        let mut params = [
            render_param(
                sys::mpv_render_param_type_MPV_RENDER_PARAM_SKIP_RENDERING,
                &mut skip,
            ),
            sys::mpv_render_param {
                type_: sys::mpv_render_param_type_MPV_RENDER_PARAM_INVALID,
                data: std::ptr::null_mut(),
            },
        ];
        // SAFETY: mpv_context is live and the parameter remains valid for the call.
        let result =
            unsafe { sys::mpv_render_context_render(self.mpv_context, params.as_mut_ptr()) };
        if result < 0 {
            anyhow::bail!("mpv composed OpenGL skip-render failed: {result}");
        }
        Ok(())
    }
}

impl Drop for ComposedGlRenderContext {
    fn drop(&mut self) {
        // SAFETY: the mpv context and GL objects are owned by this current context.
        unsafe {
            sys::mpv_render_context_set_update_callback(
                self.mpv_context,
                None,
                std::ptr::null_mut(),
            );
            sys::mpv_render_context_free(self.mpv_context);
            self.slots.clear();
        }
        let _ = self.egl.make_current(self.display, None, None, None);
        let _ = self.egl.destroy_surface(self.display, self.surface);
        let _ = self.egl.destroy_context(self.display, self.context);
    }
}

struct SharedGlSlot {
    gl: GlApi,
    _texture: Arc<wgpu::Texture>,
    view: Arc<wgpu::TextureView>,
    sync: Arc<crate::renderer::GlInteropSync>,
    wgpu_ctx: Arc<crate::renderer::WgpuContext>,
    busy: Arc<AtomicBool>,
    memory_object: u32,
    gl_texture: u32,
    framebuffer: u32,
    gl_to_vulkan_semaphore: u32,
    vulkan_to_gl_semaphore: u32,
}

impl SharedGlSlot {
    fn new(
        gl: &GlApi,
        wgpu_ctx: &Arc<crate::renderer::WgpuContext>,
        width: i32,
        height: i32,
    ) -> anyhow::Result<Self> {
        let exported = create_exportable_rgba_texture(
            wgpu_ctx,
            width as u32,
            height as u32,
            "libmpv GL Shared RGBA Texture",
        )
        .context("allocating Vulkan-exported libmpv GL texture")?;
        let mut memory_object = 0;
        let mut gl_texture = 0;
        let mut framebuffer = 0;
        let mut gl_to_vulkan_semaphore = 0;
        let mut vulkan_to_gl_semaphore = 0;
        // SAFETY: every OpenGL function pointer was loaded from the current EGL context.
        unsafe {
            (gl.create_memory_objects)(1, &mut memory_object);
            // Vulkan exports a dedicated image allocation. EXT_memory_object
            // requires this property to be set before importing that handle.
            (gl.memory_object_parameter_iv)(memory_object, 0x9581, &1);
            (gl.import_memory_fd)(
                memory_object,
                exported.memory_size,
                GL_HANDLE_TYPE_OPAQUE_FD_EXT,
                exported.memory_fd.into_raw_fd(),
            );
            (gl.gen_semaphores)(1, &mut gl_to_vulkan_semaphore);
            (gl.import_semaphore_fd)(
                gl_to_vulkan_semaphore,
                GL_HANDLE_TYPE_OPAQUE_FD_EXT,
                exported.gl_to_vulkan_fd.into_raw_fd(),
            );
            (gl.gen_semaphores)(1, &mut vulkan_to_gl_semaphore);
            (gl.import_semaphore_fd)(
                vulkan_to_gl_semaphore,
                GL_HANDLE_TYPE_OPAQUE_FD_EXT,
                exported.vulkan_to_gl_fd.into_raw_fd(),
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
                (gl.delete_framebuffers)(1, &framebuffer);
                (gl.delete_textures)(1, &gl_texture);
                (gl.delete_memory_objects)(1, &memory_object);
                (gl.delete_semaphores)(1, &gl_to_vulkan_semaphore);
                (gl.delete_semaphores)(1, &vulkan_to_gl_semaphore);
                anyhow::bail!("shared OpenGL framebuffer is incomplete: 0x{status:x}");
            }
        }
        let slot = Self {
            gl: *gl,
            _texture: exported.texture,
            view: exported.view,
            sync: exported.sync,
            wgpu_ctx: wgpu_ctx.clone(),
            busy: Arc::new(AtomicBool::new(false)),
            memory_object,
            gl_texture,
            framebuffer,
            gl_to_vulkan_semaphore,
            vulkan_to_gl_semaphore,
        };
        prime_shared_texture_for_gl(wgpu_ctx, &slot.view, &slot.sync)
            .context("priming WGPU shared texture state for OpenGL")?;
        Ok(slot)
    }

    fn acquire_for_gl(&self, gl: &GlApi) {
        // SAFETY: the imported Vulkan-to-GL semaphore and texture are live.
        unsafe {
            (gl.wait_semaphore)(
                self.vulkan_to_gl_semaphore,
                0,
                std::ptr::null(),
                1,
                &self.gl_texture,
                &GL_LAYOUT_SHADER_READ_ONLY_EXT,
            );
        }
    }

    fn release_to_vulkan(&self, gl: &GlApi) {
        // SAFETY: the imported GL-to-Vulkan semaphore and texture are live.
        // SignalSemaphoreEXT also flushes the GL command stream.
        unsafe {
            (gl.signal_semaphore)(
                self.gl_to_vulkan_semaphore,
                0,
                std::ptr::null(),
                1,
                &self.gl_texture,
                &GL_LAYOUT_SHADER_READ_ONLY_EXT,
            );
        }
    }
}

impl Drop for SharedGlSlot {
    fn drop(&mut self) {
        // SAFETY: slots are destroyed on their owner render thread before EGL
        // teardown, including partial construction and libmpv init failures.
        unsafe {
            (self.gl.delete_framebuffers)(1, &self.framebuffer);
            (self.gl.delete_textures)(1, &self.gl_texture);
            (self.gl.delete_memory_objects)(1, &self.memory_object);
            (self.gl.delete_semaphores)(1, &self.gl_to_vulkan_semaphore);
            (self.gl.delete_semaphores)(1, &self.vulkan_to_gl_semaphore);
        }
    }
}
