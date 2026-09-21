use gstreamer as gst;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::os::unix::io::OwnedFd;
use std::sync::Arc;

use super::VideoBackendKind;
use super::drm_syncobj::DrmSyncobjTimeline;
use super::frame_gl::GlExternalFrame;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VideoColorMatrix {
    Bt601,
    #[default]
    Bt709,
    Bt2020,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VideoColorRange {
    Full,
    #[default]
    Limited,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VideoTransfer {
    Srgb,
    #[default]
    Bt709,
    Bt1886,
    Pq,
    Hlg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VideoColorPrimaries {
    Bt601Ntsc,
    Bt601Pal,
    #[default]
    Bt709,
    Bt2020,
    DciP3,
    DisplayP3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VideoChromaSiting {
    #[default]
    Center,
    Left,
    TopLeft,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VideoMasteringMetadata {
    /// CIE 1931 xy coordinates in red, green, blue, white order.
    pub primaries_xy: [[f32; 2]; 4],
    pub min_luminance_nits: f32,
    pub max_luminance_nits: f32,
}

impl Eq for VideoMasteringMetadata {}

impl Hash for VideoMasteringMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for coordinate in self.primaries_xy.into_iter().flatten() {
            coordinate.to_bits().hash(state);
        }
        self.min_luminance_nits.to_bits().hash(state);
        self.max_luminance_nits.to_bits().hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct VideoContentLightMetadata {
    pub max_content_light_level: Option<u32>,
    pub max_frame_average_light_level: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct VideoCropRect {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VideoRotation {
    #[default]
    Rotate0,
    Rotate90,
    Rotate180,
    Rotate270,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VideoGeometry {
    pub coded_width: u32,
    pub coded_height: u32,
    pub display_width: u32,
    pub display_height: u32,
    pub sample_aspect_num: u32,
    pub sample_aspect_den: u32,
    pub crop: VideoCropRect,
    pub rotation: VideoRotation,
}

impl VideoGeometry {
    pub fn for_dimensions(width: u32, height: u32) -> Self {
        Self {
            coded_width: width,
            coded_height: height,
            display_width: width,
            display_height: height,
            sample_aspect_num: 1,
            sample_aspect_den: 1,
            crop: VideoCropRect {
                x: 0,
                y: 0,
                width,
                height,
            },
            rotation: VideoRotation::Rotate0,
        }
    }

    pub fn display_aspect(self) -> f32 {
        let (width, height) = match self.rotation {
            VideoRotation::Rotate90 | VideoRotation::Rotate270 => {
                (self.display_height, self.display_width)
            }
            VideoRotation::Rotate0 | VideoRotation::Rotate180 => {
                (self.display_width, self.display_height)
            }
        };
        width.max(1) as f32 * self.sample_aspect_num.max(1) as f32
            / (height.max(1) as f32 * self.sample_aspect_den.max(1) as f32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VideoColorMetadata {
    pub matrix: VideoColorMatrix,
    pub range: VideoColorRange,
    pub transfer: VideoTransfer,
    pub primaries: VideoColorPrimaries,
    pub chroma_siting: VideoChromaSiting,
    pub bit_depth: u8,
    pub mastering: Option<VideoMasteringMetadata>,
    pub content_light: VideoContentLightMetadata,
}

impl Default for VideoColorMetadata {
    fn default() -> Self {
        Self::sdr(
            VideoColorMatrix::Bt709,
            VideoColorRange::Limited,
            VideoTransfer::Bt709,
        )
    }
}

impl VideoColorMetadata {
    pub fn sdr(matrix: VideoColorMatrix, range: VideoColorRange, transfer: VideoTransfer) -> Self {
        Self {
            matrix,
            range,
            transfer,
            primaries: VideoColorPrimaries::Bt709,
            chroma_siting: VideoChromaSiting::Center,
            bit_depth: 8,
            mastering: None,
            content_light: VideoContentLightMetadata::default(),
        }
    }

    pub fn is_hdr(self) -> bool {
        matches!(self.transfer, VideoTransfer::Pq | VideoTransfer::Hlg)
    }
}

#[derive(Clone)]
pub enum VideoFrameStorage {
    Gstreamer(gst::Buffer),
    Cpu(Arc<[u8]>),
    Native(Arc<dyn Any + Send + Sync>),
    External,
}

impl VideoFrameStorage {
    pub fn byte_len(&self) -> usize {
        match self {
            Self::Gstreamer(buffer) => buffer.size(),
            Self::Cpu(bytes) => bytes.len(),
            Self::Native(_) => 0,
            Self::External => 0,
        }
    }

    pub fn memory_count(&self) -> usize {
        match self {
            Self::Gstreamer(buffer) => buffer.n_memory(),
            Self::Cpu(_) => 1,
            Self::Native(_) => 1,
            Self::External => 0,
        }
    }

    pub fn gstreamer_buffer(&self) -> Option<&gst::Buffer> {
        match self {
            Self::Gstreamer(buffer) => Some(buffer),
            Self::Cpu(_) | Self::External => None,
            Self::Native(_) => None,
        }
    }

    pub fn with_readable_bytes<T>(&self, consume: impl FnOnce(&[u8]) -> T) -> anyhow::Result<T> {
        match self {
            Self::Gstreamer(buffer) => {
                let map = buffer
                    .map_readable()
                    .map_err(|error| anyhow::anyhow!("failed to map GStreamer buffer: {error}"))?;
                Ok(consume(map.as_slice()))
            }
            Self::Cpu(bytes) => Ok(consume(bytes)),
            Self::Native(_) => anyhow::bail!("native GPU frame has no CPU-readable storage"),
            Self::External => anyhow::bail!("external GPU frame has no CPU-readable storage"),
        }
    }

    fn trace_hash(&self, hasher: &mut impl Hasher) {
        match self {
            Self::Gstreamer(buffer) => {
                "gstreamer".hash(hasher);
                buffer.size().hash(hasher);
                buffer.n_memory().hash(hasher);
                buffer.offset().hash(hasher);
                buffer.offset_end().hash(hasher);
                format!("{:?}", buffer.flags()).hash(hasher);
            }
            Self::Cpu(bytes) => {
                "cpu".hash(hasher);
                bytes.len().hash(hasher);
            }
            Self::Native(owner) => {
                "native".hash(hasher);
                Arc::as_ptr(owner).hash(hasher);
            }
            Self::External => "external".hash(hasher),
        }
    }
}

impl From<gst::Buffer> for VideoFrameStorage {
    fn from(buffer: gst::Buffer) -> Self {
        Self::Gstreamer(buffer)
    }
}

impl From<Vec<u8>> for VideoFrameStorage {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Cpu(bytes.into())
    }
}

#[derive(Debug)]
pub struct NativeDmaBufObject {
    pub fd: OwnedFd,
    pub size: u64,
    pub modifier: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NativeDmaBufPlane {
    pub layer_index: usize,
    pub object_index: usize,
    pub offset: u64,
    pub pitch: u64,
    pub drm_fourcc: u32,
}

#[derive(Debug, Clone)]
pub struct DrmSyncobjFrame {
    pub acquire_timeline: DrmSyncobjTimeline,
    pub acquire_point: u64,
    pub release_timeline: DrmSyncobjTimeline,
    pub release_point: u64,
}

impl DrmSyncobjFrame {
    pub fn release_signaled(&self) -> bool {
        self.release_timeline.is_signaled(self.release_point)
    }
}

#[derive(Debug, Clone)]
pub struct NativeDmaBufNv12 {
    pub surface_id: u64,
    pub objects: Arc<[NativeDmaBufObject]>,
    pub planes: [NativeDmaBufPlane; 2],
    /// Producer completion fence for this specific frame. This is populated
    /// by the Vulkan linear bridge and consumed by Wayland explicit sync.
    pub acquire_fence: Option<Arc<OwnedFd>>,
    /// Preferred modern explicit-sync state. Each stable bridge buffer owns a
    /// distinct release timeline so compositor completion cannot signal reuse
    /// of an unrelated slot.
    pub drm_syncobj: Option<DrmSyncobjFrame>,
}

impl NativeDmaBufNv12 {
    fn try_clone(&self) -> Option<Self> {
        Some(self.clone())
    }
}

#[derive(Debug)]
pub enum VideoFrameFormat {
    Rgba,
    /// OpenGL renders into memory shared with Vulkan; WGPU blits the GPU texture.
    GlExternalRgba {
        frame: GlExternalFrame,
    },
    Nv12 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
    /// Little-endian P010: 10-bit samples stored in the most significant bits
    /// of 16-bit Y and interleaved UV words. Kept as 16-bit planes through the
    /// final shader, with no 8-bit or output-sized RGBA intermediate.
    P010 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
    /// GStreamer DMA-BUF frame with the negotiated DRM format/modifier and
    /// stable allocation identity preserved. The descriptor is cached by the
    /// appsink callback, so cloning a frame does not duplicate file
    /// descriptors or reparse caps.
    DmaBufNv12 {
        frame: NativeDmaBufNv12,
    },
    NativeDmaBufNv12 {
        frame: NativeDmaBufNv12,
    },
    /// CUDA zero-copy: buffer stays in GPU memory, renderer uses CUDA-Vulkan interop.
    CudaNv12 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
    I420 {
        y_stride: u32,
        u_offset: u32,
        u_stride: u32,
        v_offset: u32,
        v_stride: u32,
    },
}

impl VideoFrameFormat {
    pub fn try_clone(&self) -> Option<Self> {
        match self {
            Self::Rgba => Some(Self::Rgba),
            Self::GlExternalRgba { frame } => Some(Self::GlExternalRgba {
                frame: frame.clone(),
            }),
            Self::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Some(Self::Nv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            }),
            Self::P010 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Some(Self::P010 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            }),
            Self::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Some(Self::CudaNv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            }),
            Self::I420 {
                y_stride,
                u_offset,
                u_stride,
                v_offset,
                v_stride,
            } => Some(Self::I420 {
                y_stride: *y_stride,
                u_offset: *u_offset,
                u_stride: *u_stride,
                v_offset: *v_offset,
                v_stride: *v_stride,
            }),
            Self::DmaBufNv12 { frame } => Some(Self::DmaBufNv12 {
                frame: frame.try_clone()?,
            }),
            Self::NativeDmaBufNv12 { frame } => Some(Self::NativeDmaBufNv12 {
                frame: frame.try_clone()?,
            }),
        }
    }
}

/// Backend-neutral video frame carrying CPU bytes or an externally-owned GPU image.
pub struct VideoFrame {
    pub storage: VideoFrameStorage,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub format: VideoFrameFormat,
    pub session_id: u64,
    pub pts_ns: Option<u64>,
    pub duration_ns: Option<u64>,
    pub color: VideoColorMetadata,
    pub geometry: VideoGeometry,
}

impl VideoFrame {
    pub fn trace_fingerprint(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.width.hash(&mut hasher);
        self.height.hash(&mut hasher);
        self.stride.hash(&mut hasher);
        self.session_id.hash(&mut hasher);
        self.pts_ns.hash(&mut hasher);
        self.duration_ns.hash(&mut hasher);
        self.color.hash(&mut hasher);
        self.geometry.hash(&mut hasher);
        self.storage.trace_hash(&mut hasher);
        match &self.format {
            VideoFrameFormat::Rgba => "rgba".hash(&mut hasher),
            VideoFrameFormat::GlExternalRgba { .. } => "gl-external-rgba".hash(&mut hasher),
            VideoFrameFormat::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            }
            | VideoFrameFormat::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                y_stride.hash(&mut hasher);
                uv_offset.hash(&mut hasher);
                uv_stride.hash(&mut hasher);
            }
            VideoFrameFormat::P010 {
                y_stride,
                uv_offset,
                uv_stride,
            } => {
                "p010".hash(&mut hasher);
                y_stride.hash(&mut hasher);
                uv_offset.hash(&mut hasher);
                uv_stride.hash(&mut hasher);
            }
            VideoFrameFormat::DmaBufNv12 { frame }
            | VideoFrameFormat::NativeDmaBufNv12 { frame } => {
                frame.surface_id.hash(&mut hasher);
                frame.planes.hash(&mut hasher);
                for object in frame.objects.iter() {
                    object.size.hash(&mut hasher);
                    object.modifier.hash(&mut hasher);
                }
            }
            VideoFrameFormat::I420 {
                y_stride,
                u_offset,
                u_stride,
                v_offset,
                v_stride,
            } => {
                y_stride.hash(&mut hasher);
                u_offset.hash(&mut hasher);
                u_stride.hash(&mut hasher);
                v_offset.hash(&mut hasher);
                v_stride.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    #[allow(dead_code)]
    pub fn try_clone(&self) -> Option<Self> {
        Some(Self {
            storage: self.storage.clone(),
            width: self.width,
            height: self.height,
            stride: self.stride,
            format: self.format.try_clone()?,
            session_id: self.session_id,
            pts_ns: self.pts_ns,
            duration_ns: self.duration_ns,
            color: self.color,
            geometry: self.geometry,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerEventKind {
    FirstPresent,
    Error,
    Eos,
    FatalLifecycle,
}

#[derive(Debug, Clone)]
pub struct PlayerEvent {
    pub source_id: String,
    pub session_id: u64,
    pub backend_kind: VideoBackendKind,
    pub kind: PlayerEventKind,
    pub reason: String,
}
