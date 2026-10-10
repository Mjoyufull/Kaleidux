use super::capabilities::NativeDecoderApi;
use super::drm_mapping::map_drm_prime_layout;
use crate::video::{
    NativeDmaBufNv12, NativeDmaBufObject, NativeDmaBufPlane, VideoFrame, VideoFrameFormat,
    VideoFrameStorage,
};
use ffmpeg_next::ffi;
use ffmpeg_next::util::frame::video::Video;
use std::collections::{HashMap, VecDeque};
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use tracing::warn;

const MAX_CACHED_SURFACE_LAYOUTS: usize = 64;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct SurfaceLayoutKey {
    api: NativeDecoderApi,
    frames_context: usize,
    surface_id: u64,
    width: u32,
    height: u32,
}

const fn fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

const DRM_FORMAT_NV12: u32 = fourcc(b'N', b'V', b'1', b'2');

#[derive(Clone)]
pub(super) struct CachedSurfaceLayout {
    pub(super) objects: Arc<[NativeDmaBufObject]>,
    pub(super) planes: [NativeDmaBufPlane; 2],
    pub(super) stride: u32,
}

pub struct NativeSurfaceExporter {
    layouts: HashMap<SurfaceLayoutKey, CachedSurfaceLayout>,
    layout_order: VecDeque<SurfaceLayoutKey>,
    frame_pool: Arc<parking_lot::Mutex<Vec<Video>>>,
}

const MAX_REUSABLE_FRAMES: usize = 16;

struct MappedNativeFrame {
    _mapped: Video,
    _decoded: Arc<dyn std::any::Any + Send + Sync>,
}

pub(super) struct RecycledNativeFrame {
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
    pub(super) fn retain_frame(&self, frame: Video) -> Arc<dyn std::any::Any + Send + Sync> {
        Arc::new(RecycledNativeFrame {
            frame: Some(frame),
            pool: self.frame_pool.clone(),
        })
    }
    pub fn new() -> Self {
        Self {
            layouts: HashMap::new(),
            layout_order: VecDeque::new(),
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
        let layout_key = SurfaceLayoutKey {
            api,
            frames_context: hardware_frames_context_identity(&decoded),
            surface_id,
            width,
            height,
        };
        // Vulkan mapping performs timeline waits and transfers ownership for
        // each decode. Reusing only its fd layout would skip that handoff.
        let mut mapped_lease = None;
        let layout = if api == NativeDecoderApi::VulkanVideo {
            match map_drm_prime_layout(&decoded, api) {
                Ok((layout, mapped)) => {
                    mapped_lease = Some(mapped);
                    layout
                }
                Err(error) => return Err((error, decoded)),
            }
        } else if let Some(layout) = self.layouts.get(&layout_key) {
            if let Some(position) = self.layout_order.iter().position(|key| key == &layout_key) {
                self.layout_order.remove(position);
            }
            self.layout_order.push_back(layout_key);
            layout.clone()
        } else {
            let layout = match map_surface_layout(&decoded, api) {
                Ok(layout) => layout,
                Err(error) => return Err((error, decoded)),
            };
            if self.layouts.len() >= MAX_CACHED_SURFACE_LAYOUTS {
                if let Some(oldest_key) = self.layout_order.pop_front() {
                    self.layouts.remove(&oldest_key);
                }
            }
            self.layouts.insert(layout_key, layout.clone());
            self.layout_order.push_back(layout_key);
            layout
        };

        let acquire_fence = if api == NativeDecoderApi::VulkanVideo {
            match crate::video::dmabuf::export_implicit_read_fence(&layout.objects) {
                Ok(fence) => Some(Arc::new(fence)),
                Err(error) => return Err((error, decoded)),
            }
        } else {
            None
        };
        let retained = self.retain_frame(decoded);
        let owner = if let Some(mapped) = mapped_lease {
            Arc::new(MappedNativeFrame {
                _mapped: mapped,
                _decoded: retained,
            }) as Arc<dyn std::any::Any + Send + Sync>
        } else {
            retained
        };
        Ok(VideoFrame {
            // Drop the map only after the renderer's copy fence releases storage.
            storage: VideoFrameStorage::Native(owner),
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
                    acquire_fence,
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

fn hardware_frames_context_identity(decoded: &Video) -> usize {
    // SAFETY: decoded is a live hardware AVFrame. AVBufferRef::data points to
    // the stable AVHWFramesContext allocation for this decoder generation.
    unsafe {
        let frames_ref = (*decoded.as_ptr()).hw_frames_ctx;
        if frames_ref.is_null() {
            return 0;
        }
        (*frames_ref).data as usize
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
    map_drm_prime_layout(decoded, api).map(|(layout, _mapped)| layout)
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
