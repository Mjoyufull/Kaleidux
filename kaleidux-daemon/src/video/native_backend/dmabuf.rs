use super::capabilities::NativeDecoderApi;
use crate::video::{
    NativeDmaBufNv12, NativeDmaBufObject, NativeDmaBufPlane, VideoFrame, VideoFrameFormat,
    VideoFrameStorage,
};
use ffmpeg_next::ffi;
use ffmpeg_next::util::frame::video::Video;
use std::collections::HashMap;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use tracing::warn;

const fn fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

const DRM_FORMAT_NV12: u32 = fourcc(b'N', b'V', b'1', b'2');

#[derive(Clone)]
struct CachedSurfaceLayout {
    objects: Arc<[NativeDmaBufObject]>,
    planes: [NativeDmaBufPlane; 2],
    stride: u32,
}

pub struct NativeSurfaceExporter {
    layouts: HashMap<u64, CachedSurfaceLayout>,
    frame_pool: Arc<parking_lot::Mutex<Vec<Video>>>,
}

const MAX_REUSABLE_FRAMES: usize = 16;

struct RecycledNativeFrame {
    frame: Option<Video>,
    pool: Arc<parking_lot::Mutex<Vec<Video>>>,
}

impl Drop for RecycledNativeFrame {
    fn drop(&mut self) {
        let Some(mut frame) = self.frame.take() else {
            return;
        };
        // SAFETY: this lease is the last owner of the AVFrame wrapper. Drop
        // all buffer references before returning the wrapper to the worker's
        // bounded reuse pool.
        unsafe { ffi::av_frame_unref(frame.as_mut_ptr()) };
        let mut pool = self.pool.lock();
        if pool.len() < MAX_REUSABLE_FRAMES {
            pool.push(frame);
        }
    }
}

impl NativeSurfaceExporter {
    pub fn new() -> Self {
        Self {
            layouts: HashMap::new(),
            frame_pool: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    pub fn acquire_decode_frame(&self) -> Video {
        self.frame_pool.lock().pop().unwrap_or_else(Video::empty)
    }

    pub fn recycle_decode_frame(&self, mut frame: Video) {
        // SAFETY: the decode worker owns this frame exclusively and no storage
        // lease references it.
        unsafe { ffi::av_frame_unref(frame.as_mut_ptr()) };
        let mut pool = self.frame_pool.lock();
        if pool.len() < MAX_REUSABLE_FRAMES {
            pool.push(frame);
        }
    }

    pub fn export_hardware_nv12(
        &mut self,
        decoded: Video,
        api: NativeDecoderApi,
        session_id: u64,
        pts_ns: Option<u64>,
        duration_ns: Option<u64>,
    ) -> Result<VideoFrame, (anyhow::Error, Video)> {
        let width = decoded.width();
        let height = decoded.height();
        let color = super::native_color_metadata(&decoded);
        let geometry = super::native_geometry(&decoded);
        if api == NativeDecoderApi::Vaapi
            && let Err(error) = sync_vaapi_surface(&decoded)
        {
            return Err((error, decoded));
        }
        // VAAPI stores VASurfaceID in data[3]. Vulkan frames store an AVVkFrame
        // owner in data[0]; object inode/modifier data remains part of the renderer
        // cache key, so this API-local identifier cannot alias decoder generations.
        // SAFETY: `decoded` is a live hardware AVFrame selected by libavcodec.
        let surface_id = unsafe {
            match api {
                NativeDecoderApi::Vaapi => (*decoded.as_ptr()).data[3] as usize as u64,
                _ => (*decoded.as_ptr()).data[0] as usize as u64,
            }
        };
        let layout = if let Some(layout) = self.layouts.get(&surface_id) {
            layout.clone()
        } else {
            let layout = match map_surface_layout(&decoded, api) {
                Ok(layout) => layout,
                Err(error) => return Err((error, decoded)),
            };
            self.layouts.insert(surface_id, layout.clone());
            layout
        };

        Ok(VideoFrame {
            // Move the received AVFrame into storage instead of cloning it per
            // frame. The owner still retains the decoder surface until the
            // compositor release/copy fence, with no av_frame_clone allocation.
            storage: VideoFrameStorage::Native(Arc::new(RecycledNativeFrame {
                frame: Some(decoded),
                pool: self.frame_pool.clone(),
            })),
            width,
            height,
            stride: layout.stride,
            format: VideoFrameFormat::NativeDmaBufNv12 {
                frame: NativeDmaBufNv12 {
                    surface_id,
                    objects: layout.objects,
                    planes: layout.planes,
                    // Decoder-native VA surfaces are synchronized before
                    // export. Vulkan bridge frames carry an explicit fence.
                    acquire_fence: None,
                    drm_syncobj: None,
                },
            },
            session_id,
            pts_ns,
            duration_ns,
            color,
            geometry,
        })
    }
}

fn map_surface_layout(
    decoded: &Video,
    api: NativeDecoderApi,
) -> anyhow::Result<CachedSurfaceLayout> {
    if api == NativeDecoderApi::Vaapi {
        match export_vaapi_composed_layout(decoded) {
            Ok(layout) => return Ok(layout),
            Err(error) => warn!(
                "[NATIVE-PATH] VAAPI composed NV12 export unavailable ({error:#}); retaining separate-layer Vulkan copy fallback"
            ),
        }
    }
    map_drm_prime_layout(decoded, api)
}

fn map_drm_prime_layout(
    decoded: &Video,
    api: NativeDecoderApi,
) -> anyhow::Result<CachedSurfaceLayout> {
    let mut mapped = Video::empty();
    // SAFETY: setting the requested destination format before av_hwframe_map is
    // FFmpeg's documented way to request an AVDRMFrameDescriptor.
    unsafe {
        (*mapped.as_mut_ptr()).format = ffi::AVPixelFormat::AV_PIX_FMT_DRM_PRIME as i32;
    }
    // SAFETY: both frames remain live for the call. The returned mapped frame
    // owns a reference to the decoder surface until its AVFrame is dropped.
    let result = unsafe {
        ffi::av_hwframe_map(
            mapped.as_mut_ptr(),
            decoded.as_ptr(),
            ffi::AV_HWFRAME_MAP_READ as i32,
        )
    };
    if result < 0 {
        anyhow::bail!(
            "{} -> DRM PRIME map failed with FFmpeg error {result}",
            api.label()
        );
    }

    // SAFETY: DRM_PRIME frames carry AVDRMFrameDescriptor in data[0].
    let descriptor_ptr = unsafe { (*mapped.as_ptr()).data[0] as *const ffi::AVDRMFrameDescriptor };
    if descriptor_ptr.is_null() {
        anyhow::bail!("FFmpeg returned a DRM PRIME frame without a descriptor");
    }
    // SAFETY: the descriptor is owned by `mapped`, which is retained below.
    let descriptor = unsafe { &*descriptor_ptr };
    let object_count = usize::try_from(descriptor.nb_objects)
        .ok()
        .filter(|count| (1..=4).contains(count))
        .ok_or_else(|| anyhow::anyhow!("invalid DRM object count {}", descriptor.nb_objects))?;
    let layer_count = usize::try_from(descriptor.nb_layers)
        .ok()
        .filter(|count| (1..=4).contains(count))
        .ok_or_else(|| anyhow::anyhow!("invalid DRM layer count {}", descriptor.nb_layers))?;

    let mut objects = Vec::with_capacity(object_count);
    for object in descriptor.objects.iter().take(object_count) {
        // SAFETY: dup creates ownership independent of the mapped AVFrame.
        let duplicated = unsafe { libc::dup(object.fd) };
        if duplicated < 0 {
            anyhow::bail!("failed to duplicate DRM PRIME object fd {}", object.fd);
        }
        // SAFETY: `duplicated` is a fresh descriptor owned by this function.
        let fd = unsafe { OwnedFd::from_raw_fd(duplicated) };
        objects.push(NativeDmaBufObject {
            fd,
            size: object.size as u64,
            modifier: object.format_modifier,
        });
    }

    let mut planes = Vec::with_capacity(2);
    for (layer_index, layer) in descriptor.layers.iter().take(layer_count).enumerate() {
        let plane_count = usize::try_from(layer.nb_planes)
            .ok()
            .filter(|count| (1..=4).contains(count))
            .ok_or_else(|| anyhow::anyhow!("invalid DRM plane count {}", layer.nb_planes))?;
        for plane in layer.planes.iter().take(plane_count) {
            let object_index = usize::try_from(plane.object_index)
                .ok()
                .filter(|index| *index < object_count)
                .ok_or_else(|| {
                    anyhow::anyhow!("DRM plane references invalid object {}", plane.object_index)
                })?;
            planes.push(NativeDmaBufPlane {
                layer_index,
                object_index,
                offset: u64::try_from(plane.offset)
                    .map_err(|_| anyhow::anyhow!("negative DRM plane offset"))?,
                pitch: u64::try_from(plane.pitch)
                    .map_err(|_| anyhow::anyhow!("negative DRM plane pitch"))?,
                drm_fourcc: layer.format,
            });
        }
    }
    let planes: [NativeDmaBufPlane; 2] = planes.try_into().map_err(|planes: Vec<_>| {
        anyhow::anyhow!(
            "NV12 DRM PRIME export returned {} planes instead of 2",
            planes.len()
        )
    })?;
    let stride = u32::try_from(planes[0].pitch)
        .map_err(|_| anyhow::anyhow!("DRM luma pitch exceeds u32"))?;
    Ok(CachedSurfaceLayout {
        objects: objects.into(),
        planes,
        stride,
    })
}

#[repr(C)]
struct VaapiDeviceContext {
    display: libva_sys::VADisplay,
    _driver_quirks: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct VaDrmPrimeObject {
    fd: i32,
    size: u32,
    modifier: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct VaDrmPrimeLayer {
    drm_format: u32,
    num_planes: u32,
    object_index: [u32; 4],
    offset: [u32; 4],
    pitch: [u32; 4],
}

#[repr(C)]
struct VaDrmPrimeDescriptor {
    fourcc: u32,
    width: u32,
    height: u32,
    num_objects: u32,
    objects: [VaDrmPrimeObject; 4],
    num_layers: u32,
    layers: [VaDrmPrimeLayer; 4],
}

impl Default for VaDrmPrimeDescriptor {
    fn default() -> Self {
        Self {
            fourcc: 0,
            width: 0,
            height: 0,
            num_objects: 0,
            objects: [VaDrmPrimeObject {
                fd: -1,
                size: 0,
                modifier: 0,
            }; 4],
            num_layers: 0,
            layers: [VaDrmPrimeLayer::default(); 4],
        }
    }
}

fn export_vaapi_composed_layout(decoded: &Video) -> anyhow::Result<CachedSurfaceLayout> {
    let (display, surface_id) = vaapi_display_and_surface(decoded)?;

    let mut descriptor = VaDrmPrimeDescriptor::default();
    // SAFETY: descriptor matches VADRMPRIMESurfaceDescriptor from va_drmcommon.h;
    // display/surface_id belong to the live FFmpeg VAAPI device and frame.
    let status = unsafe {
        libva_sys::vaExportSurfaceHandle(
            display,
            surface_id,
            0x4000_0000,
            libva_sys::VA_EXPORT_SURFACE_READ_ONLY | libva_sys::VA_EXPORT_SURFACE_COMPOSED_LAYERS,
            std::ptr::addr_of_mut!(descriptor).cast(),
        )
    };
    anyhow::ensure!(
        status == libva_sys::VA_STATUS_SUCCESS as i32,
        "vaExportSurfaceHandle returned status {status}"
    );

    // Adopt every descriptor the driver could have written before validating
    // the remaining metadata. Any later error then closes the exported fds
    // through OwnedFd instead of leaking decoder-surface handles.
    let exported_slot_count = usize::try_from(descriptor.num_objects)
        .unwrap_or(descriptor.objects.len())
        .min(descriptor.objects.len());
    let mut exported_fds = descriptor
        .objects
        .iter()
        .take(exported_slot_count)
        .map(|object| {
            (object.fd >= 0).then(|| {
                // SAFETY: vaExportSurfaceHandle transfers each reported fd to
                // the caller exactly once.
                unsafe { OwnedFd::from_raw_fd(object.fd) }
            })
        })
        .collect::<Vec<_>>();
    let object_count = usize::try_from(descriptor.num_objects)
        .ok()
        .filter(|count| (1..=4).contains(count))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "invalid composed VA object count {}",
                descriptor.num_objects
            )
        })?;
    let mut objects = Vec::with_capacity(object_count);
    for (object_index, object) in descriptor.objects.iter().take(object_count).enumerate() {
        let fd = exported_fds[object_index]
            .take()
            .ok_or_else(|| anyhow::anyhow!("composed VA export returned an invalid fd"))?;
        objects.push(NativeDmaBufObject {
            fd,
            size: u64::from(object.size),
            modifier: object.modifier,
        });
    }
    anyhow::ensure!(
        descriptor.num_layers == 1,
        "composed VA export returned {} layers",
        descriptor.num_layers
    );
    let layer = descriptor.layers[0];
    anyhow::ensure!(
        descriptor.fourcc == DRM_FORMAT_NV12 && layer.drm_format == DRM_FORMAT_NV12,
        "composed VA export is not NV12 (surface={:#010x}, layer={:#010x})",
        descriptor.fourcc,
        layer.drm_format
    );
    anyhow::ensure!(
        layer.num_planes == 2,
        "composed VA NV12 layer returned {} memory planes",
        layer.num_planes
    );
    let planes = std::array::from_fn(|plane_index| NativeDmaBufPlane {
        layer_index: 0,
        object_index: layer.object_index[plane_index] as usize,
        offset: u64::from(layer.offset[plane_index]),
        pitch: u64::from(layer.pitch[plane_index]),
        drm_fourcc: layer.drm_format,
    });
    anyhow::ensure!(
        planes
            .iter()
            .all(|plane| plane.object_index < objects.len()),
        "composed VA layer references an invalid object"
    );
    Ok(CachedSurfaceLayout {
        objects: objects.into(),
        planes,
        stride: layer.pitch[0],
    })
}

fn sync_vaapi_surface(decoded: &Video) -> anyhow::Result<()> {
    let (display, surface_id) = vaapi_display_and_surface(decoded)?;
    // SAFETY: the display and surface belong to the live FFmpeg AVFrame.
    // libva requires this synchronization before an exported surface is read
    // by an external API or Wayland compositor.
    let status = unsafe { libva_sys::vaSyncSurface(display, surface_id) };
    anyhow::ensure!(
        status == libva_sys::VA_STATUS_SUCCESS as i32,
        "vaSyncSurface returned status {status}"
    );
    Ok(())
}

fn vaapi_display_and_surface(decoded: &Video) -> anyhow::Result<(libva_sys::VADisplay, u32)> {
    // SAFETY: libavcodec returned a live VAAPI AVFrame. Its hw_frames_ctx owns
    // the AVHWFramesContext/device context for at least the duration of this call.
    let (display, surface_id) = unsafe {
        let frame = &*decoded.as_ptr();
        let frames_ref = frame.hw_frames_ctx;
        anyhow::ensure!(!frames_ref.is_null(), "VAAPI frame has no hw_frames_ctx");
        let frames = (*frames_ref).data.cast::<ffi::AVHWFramesContext>();
        anyhow::ensure!(!frames.is_null(), "VAAPI hw_frames_ctx has no data");
        let device = (*frames).device_ctx;
        anyhow::ensure!(!device.is_null(), "VAAPI frames have no device context");
        anyhow::ensure!(
            (*device).type_ == ffi::AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI,
            "hardware device is not VAAPI"
        );
        let vaapi = (*device).hwctx.cast::<VaapiDeviceContext>();
        anyhow::ensure!(!vaapi.is_null(), "VAAPI device has no private context");
        ((*vaapi).display, frame.data[3] as usize as u32)
    };
    anyhow::ensure!(!display.is_null(), "VAAPI device has no display");
    Ok((display, surface_id))
}
