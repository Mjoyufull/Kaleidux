use anyhow::Context as _;
use glow::HasContext as _;
use khronos_egl as egl;
use std::collections::HashMap;
use std::ffi::c_void;
use std::os::fd::AsRawFd;
use wayland_client::backend::ObjectId;
use wayland_egl::WlEglSurface;

use crate::video::{NativeDmaBufNv12, VideoFrame, VideoFrameFormat};

type EglApi = egl::DynamicInstance<egl::EGL1_5>;

const EGL_PLATFORM_WAYLAND_KHR: egl::Enum = 0x31D8;
const EGL_LINUX_DMA_BUF_EXT: egl::Enum = 0x3270;
const EGL_LINUX_DRM_FOURCC_EXT: egl::Attrib = 0x3271;
const EGL_DMA_BUF_PLANE0_FD_EXT: egl::Attrib = 0x3272;
const EGL_DMA_BUF_PLANE0_OFFSET_EXT: egl::Attrib = 0x3273;
const EGL_DMA_BUF_PLANE0_PITCH_EXT: egl::Attrib = 0x3274;
const EGL_DMA_BUF_PLANE0_MODIFIER_LO_EXT: egl::Attrib = 0x3443;
const EGL_DMA_BUF_PLANE0_MODIFIER_HI_EXT: egl::Attrib = 0x3444;
const DRM_FORMAT_MOD_INVALID: u64 = 0x00ff_ffff_ffff_ffff;

const fn fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

const DRM_FORMAT_R8: u32 = fourcc(b'R', b'8', b' ', b' ');
const DRM_FORMAT_GR88: u32 = fourcc(b'G', b'R', b'8', b'8');

const VERTEX_SHADER: &str = r#"#version 150 core
out vec2 v_uv;

void main() {
    vec2 uv;
    if (gl_VertexID == 0) uv = vec2(0.0, 2.0);
    if (gl_VertexID == 1) uv = vec2(0.0, 0.0);
    if (gl_VertexID == 2) uv = vec2(2.0, 0.0);
    gl_Position = vec4(uv.x * 2.0 - 1.0, 1.0 - uv.y * 2.0, 0.0, 1.0);
    v_uv = uv;
}
"#;

const FRAGMENT_SHADER: &str = r#"#version 150 core
in vec2 v_uv;
out vec4 out_color;
uniform sampler2D y_tex;
uniform sampler2D uv_tex;
uniform float screen_aspect;
uniform float content_aspect;

vec2 cover(vec2 uv) {
    float scale = screen_aspect / content_aspect;
    if (scale > 1.0) return vec2(uv.x, (uv.y - 0.5) / scale + 0.5);
    return vec2((uv.x - 0.5) * scale + 0.5, uv.y);
}

void main() {
    vec2 sample_uv = cover(v_uv);
    float y = (texture(y_tex, sample_uv).r * 255.0 - 16.0) / 219.0;
    vec2 chroma = (texture(uv_tex, sample_uv).rg * 255.0 - vec2(128.0)) / 224.0;
    float r = y + 1.5748 * chroma.y;
    float g = y - 0.1873 * chroma.x - 0.4681 * chroma.y;
    float b = y + 1.8556 * chroma.x;
    out_color = vec4(clamp(vec3(r, g, b), 0.0, 1.0), 1.0);
}
"#;

type EglImageTargetTexture2d = unsafe extern "system" fn(u32, *mut c_void);

struct ImportedFrame {
    images: [egl::Image; 2],
    textures: [glow::NativeTexture; 2],
}

/// Persistent native steady-state presenter.
///
/// Decoder DMA-BUF planes remain GPU-resident. EGL imports them as R8/GR88
/// textures and a single GL draw performs the only required YUV conversion
/// directly into the compositor-owned EGL window buffer.
pub(super) struct NativeGlSurfaceRenderer {
    egl: Box<EglApi>,
    display: egl::Display,
    context: egl::Context,
    surface: egl::Surface,
    egl_window: WlEglSurface,
    gl: glow::Context,
    image_target_texture_2d: EglImageTargetTexture2d,
    program: glow::NativeProgram,
    vertex_array: glow::NativeVertexArray,
    frames: HashMap<u64, ImportedFrame>,
    active_session: Option<u64>,
    width: i32,
    height: i32,
}

// SAFETY: this renderer is created lazily only after `Renderer` has returned
// from background initialization and entered the Wayland main-loop thread. All
// methods require `&mut self`; EGL/GL calls and destruction therefore remain on
// that one thread. The Send bound is needed only because an empty `Option` of
// this type is present while the outer Renderer is transferred out of its
// initialization worker.
unsafe impl Send for NativeGlSurfaceRenderer {}

impl NativeGlSurfaceRenderer {
    pub(super) fn new(
        wayland_display: *mut c_void,
        surface_id: ObjectId,
        output_size: (u32, u32),
    ) -> anyhow::Result<Self> {
        let (width, height) = checked_dimensions(output_size)?;
        // SAFETY: loading validates the EGL 1.5 symbols before returning.
        let egl = Box::new(
            unsafe { EglApi::load_required() }.context("loading libEGL for native video")?,
        );
        // SAFETY: the pointer belongs to the live Wayland connection.
        let display = unsafe {
            egl.get_platform_display(
                EGL_PLATFORM_WAYLAND_KHR,
                wayland_display,
                &[egl::ATTRIB_NONE],
            )
        }
        .context("creating the native-video EGL Wayland display")?;
        egl.initialize(display)
            .context("initializing the native-video EGL display")?;
        egl.bind_api(egl::OPENGL_API)
            .context("binding the EGL OpenGL API for native video")?;

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
            .context("choosing the native-video EGL config")?
            .context("no EGL config supports native-video OpenGL rendering")?;
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
            .context("creating the native-video OpenGL context")?;
        let egl_window = WlEglSurface::new(surface_id, width, height)
            .context("creating the native-video wl_egl_window")?;
        // SAFETY: egl_window is live and was created from this Wayland display.
        let surface = unsafe {
            egl.create_platform_window_surface(
                display,
                config,
                egl_window.ptr() as *mut c_void,
                &[egl::ATTRIB_NONE],
            )
        }
        .context("creating the native-video EGL window surface")?;
        egl.make_current(display, Some(surface), Some(surface), Some(context))
            .context("making the native-video EGL context current")?;
        let _ = egl.swap_interval(display, 0);

        // SAFETY: EGL owns each returned symbol for at least the EGL instance lifetime.
        let gl = unsafe {
            glow::Context::from_loader_function(|name| {
                egl.get_proc_address(name)
                    .map(|proc| proc as *const () as *const c_void)
                    .unwrap_or(std::ptr::null())
            })
        };
        let image_target_texture_2d = load_image_target(&egl)?;
        // SAFETY: the EGL OpenGL context is current on this thread.
        let (program, vertex_array) = unsafe { create_pipeline(&gl)? };

        Ok(Self {
            egl,
            display,
            context,
            surface,
            egl_window,
            gl,
            image_target_texture_2d,
            program,
            vertex_array,
            frames: HashMap::new(),
            active_session: None,
            width,
            height,
        })
    }

    pub(super) fn present(
        &mut self,
        frame: &VideoFrame,
        output_size: (u32, u32),
    ) -> anyhow::Result<()> {
        let VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } = &frame.format else {
            anyhow::bail!("native GL presenter requires a DMA-BUF NV12 frame");
        };
        self.make_current()?;
        self.resize(output_size)?;
        if self.active_session != Some(frame.session_id) {
            self.clear_imports();
            self.active_session = Some(frame.session_id);
        }
        let key = descriptor_key(descriptor, frame.width, frame.height);
        if !self.frames.contains_key(&key) {
            let imported = self.import_frame(descriptor, frame.width, frame.height)?;
            self.frames.insert(key, imported);
        }
        let imported = self
            .frames
            .get(&key)
            .context("native GL frame cache insertion failed")?;

        // SAFETY: all GL objects belong to the current context and imported
        // EGLImages remain live through the swap.
        unsafe {
            self.gl.viewport(0, 0, self.width, self.height);
            self.gl.disable(glow::BLEND);
            self.gl.use_program(Some(self.program));
            self.gl.bind_vertex_array(Some(self.vertex_array));
            self.gl.active_texture(glow::TEXTURE0);
            self.gl
                .bind_texture(glow::TEXTURE_2D, Some(imported.textures[0]));
            self.gl.active_texture(glow::TEXTURE1);
            self.gl
                .bind_texture(glow::TEXTURE_2D, Some(imported.textures[1]));
            self.gl.uniform_1_i32(
                self.gl.get_uniform_location(self.program, "y_tex").as_ref(),
                0,
            );
            self.gl.uniform_1_i32(
                self.gl
                    .get_uniform_location(self.program, "uv_tex")
                    .as_ref(),
                1,
            );
            self.gl.uniform_1_f32(
                self.gl
                    .get_uniform_location(self.program, "screen_aspect")
                    .as_ref(),
                self.width as f32 / self.height as f32,
            );
            self.gl.uniform_1_f32(
                self.gl
                    .get_uniform_location(self.program, "content_aspect")
                    .as_ref(),
                frame.width as f32 / frame.height.max(1) as f32,
            );
            self.gl.draw_arrays(glow::TRIANGLES, 0, 3);
        }
        self.egl
            .swap_buffers(self.display, self.surface)
            .context("swapping the native-video EGL surface")
    }

    fn make_current(&self) -> anyhow::Result<()> {
        self.egl
            .make_current(
                self.display,
                Some(self.surface),
                Some(self.surface),
                Some(self.context),
            )
            .context("making the native-video EGL context current")
    }

    fn resize(&mut self, output_size: (u32, u32)) -> anyhow::Result<()> {
        let (width, height) = checked_dimensions(output_size)?;
        if (width, height) != (self.width, self.height) {
            self.egl_window.resize(width, height, 0, 0);
            self.width = width;
            self.height = height;
        }
        Ok(())
    }

    fn import_frame(
        &self,
        descriptor: &NativeDmaBufNv12,
        width: u32,
        height: u32,
    ) -> anyhow::Result<ImportedFrame> {
        let mut images = Vec::with_capacity(2);
        let mut textures = Vec::with_capacity(2);
        for (plane_index, (format, plane_width, plane_height)) in [
            (DRM_FORMAT_R8, width, height),
            (DRM_FORMAT_GR88, width.div_ceil(2), height.div_ceil(2)),
        ]
        .into_iter()
        .enumerate()
        {
            let plane = descriptor.planes[plane_index];
            let object = descriptor
                .objects
                .get(plane.object_index)
                .with_context(|| format!("NV12 plane {plane_index} references a missing object"))?;
            let mut attributes = vec![
                EGL_LINUX_DRM_FOURCC_EXT,
                format as egl::Attrib,
                egl::WIDTH as egl::Attrib,
                plane_width as egl::Attrib,
                egl::HEIGHT as egl::Attrib,
                plane_height as egl::Attrib,
                EGL_DMA_BUF_PLANE0_FD_EXT,
                object.fd.as_raw_fd() as egl::Attrib,
                EGL_DMA_BUF_PLANE0_OFFSET_EXT,
                egl::Attrib::try_from(plane.offset)
                    .context("NV12 plane offset exceeds EGLAttrib")?,
                EGL_DMA_BUF_PLANE0_PITCH_EXT,
                egl::Attrib::try_from(plane.pitch).context("NV12 plane pitch exceeds EGLAttrib")?,
            ];
            if object.modifier != DRM_FORMAT_MOD_INVALID {
                attributes.extend_from_slice(&[
                    EGL_DMA_BUF_PLANE0_MODIFIER_LO_EXT,
                    (object.modifier & 0xffff_ffff) as egl::Attrib,
                    EGL_DMA_BUF_PLANE0_MODIFIER_HI_EXT,
                    (object.modifier >> 32) as egl::Attrib,
                ]);
            }
            attributes.push(egl::ATTRIB_NONE);
            // SAFETY: EGL_NO_CONTEXT and a null client buffer are required by
            // EGL_EXT_image_dma_buf_import; the attribute fds stay live in the descriptor.
            let image = self
                .egl
                .create_image(
                    self.display,
                    unsafe { egl::Context::from_ptr(egl::NO_CONTEXT) },
                    EGL_LINUX_DMA_BUF_EXT,
                    unsafe { egl::ClientBuffer::from_ptr(std::ptr::null_mut()) },
                    &attributes,
                )
                .with_context(|| format!("importing NV12 plane {plane_index} as an EGLImage"))?;
            // SAFETY: the current context owns the texture and the EGLImage is live.
            let texture = unsafe {
                let texture = self.gl.create_texture().map_err(anyhow::Error::msg)?;
                self.gl.active_texture(glow::TEXTURE0 + plane_index as u32);
                self.gl.bind_texture(glow::TEXTURE_2D, Some(texture));
                self.gl.tex_parameter_i32(
                    glow::TEXTURE_2D,
                    glow::TEXTURE_MIN_FILTER,
                    glow::LINEAR as i32,
                );
                self.gl.tex_parameter_i32(
                    glow::TEXTURE_2D,
                    glow::TEXTURE_MAG_FILTER,
                    glow::LINEAR as i32,
                );
                self.gl.tex_parameter_i32(
                    glow::TEXTURE_2D,
                    glow::TEXTURE_WRAP_S,
                    glow::CLAMP_TO_EDGE as i32,
                );
                self.gl.tex_parameter_i32(
                    glow::TEXTURE_2D,
                    glow::TEXTURE_WRAP_T,
                    glow::CLAMP_TO_EDGE as i32,
                );
                (self.image_target_texture_2d)(glow::TEXTURE_2D, image.as_ptr());
                let error = self.gl.get_error();
                if error != glow::NO_ERROR {
                    self.gl.delete_texture(texture);
                    let _ = self.egl.destroy_image(self.display, image);
                    anyhow::bail!(
                        "binding NV12 plane {plane_index} EGLImage failed with GL error {error:#x}"
                    );
                }
                texture
            };
            images.push(image);
            textures.push(texture);
        }
        Ok(ImportedFrame {
            images: images
                .try_into()
                .map_err(|_| anyhow::anyhow!("expected two EGLImages"))?,
            textures: textures
                .try_into()
                .map_err(|_| anyhow::anyhow!("expected two GL textures"))?,
        })
    }

    fn clear_imports(&mut self) {
        for (_, imported) in self.frames.drain() {
            // SAFETY: this context is current whenever this method is called.
            unsafe {
                self.gl.delete_texture(imported.textures[0]);
                self.gl.delete_texture(imported.textures[1]);
            }
            let _ = self.egl.destroy_image(self.display, imported.images[0]);
            let _ = self.egl.destroy_image(self.display, imported.images[1]);
        }
    }
}

impl Drop for NativeGlSurfaceRenderer {
    fn drop(&mut self) {
        if self.make_current().is_ok() {
            self.clear_imports();
            // SAFETY: these objects belong to the current GL context.
            unsafe {
                self.gl.delete_vertex_array(self.vertex_array);
                self.gl.delete_program(self.program);
            }
        }
        let _ = self.egl.make_current(self.display, None, None, None);
        let _ = self.egl.destroy_surface(self.display, self.surface);
        let _ = self.egl.destroy_context(self.display, self.context);
        let _ = self.egl_window.ptr();
    }
}

fn checked_dimensions(output_size: (u32, u32)) -> anyhow::Result<(i32, i32)> {
    Ok((
        i32::try_from(output_size.0.max(1)).context("native-video output width exceeds i32")?,
        i32::try_from(output_size.1.max(1)).context("native-video output height exceeds i32")?,
    ))
}

fn descriptor_key(descriptor: &NativeDmaBufNv12, width: u32, height: u32) -> u64 {
    descriptor.surface_id ^ (u64::from(width) << 32) ^ u64::from(height)
}

fn load_image_target(egl: &EglApi) -> anyhow::Result<EglImageTargetTexture2d> {
    let proc = egl
        .get_proc_address("glEGLImageTargetTexture2DOES")
        .context("EGL does not expose glEGLImageTargetTexture2DOES")?;
    // SAFETY: EGL returned this exact extension entry point.
    Ok(unsafe { std::mem::transmute::<extern "system" fn(), EglImageTargetTexture2d>(proc) })
}

unsafe fn create_pipeline(
    gl: &glow::Context,
) -> anyhow::Result<(glow::NativeProgram, glow::NativeVertexArray)> {
    // SAFETY: caller guarantees a current OpenGL context.
    unsafe {
        let program = gl.create_program().map_err(anyhow::Error::msg)?;
        let vertex = compile_shader(gl, glow::VERTEX_SHADER, VERTEX_SHADER)?;
        let fragment = compile_shader(gl, glow::FRAGMENT_SHADER, FRAGMENT_SHADER)?;
        gl.attach_shader(program, vertex);
        gl.attach_shader(program, fragment);
        gl.link_program(program);
        gl.detach_shader(program, vertex);
        gl.detach_shader(program, fragment);
        gl.delete_shader(vertex);
        gl.delete_shader(fragment);
        if !gl.get_program_link_status(program) {
            let log = gl.get_program_info_log(program);
            gl.delete_program(program);
            anyhow::bail!("native-video GL program link failed: {log}");
        }
        let vertex_array = gl.create_vertex_array().map_err(anyhow::Error::msg)?;
        Ok((program, vertex_array))
    }
}

unsafe fn compile_shader(
    gl: &glow::Context,
    kind: u32,
    source: &str,
) -> anyhow::Result<glow::NativeShader> {
    // SAFETY: caller guarantees a current OpenGL context.
    unsafe {
        let shader = gl.create_shader(kind).map_err(anyhow::Error::msg)?;
        gl.shader_source(shader, source);
        gl.compile_shader(shader);
        if !gl.get_shader_compile_status(shader) {
            let log = gl.get_shader_info_log(shader);
            gl.delete_shader(shader);
            anyhow::bail!("native-video GL shader compilation failed: {log}");
        }
        Ok(shader)
    }
}
