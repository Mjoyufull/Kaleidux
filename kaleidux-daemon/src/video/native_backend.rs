#[path = "native_backend/capabilities.rs"]
mod capabilities;
#[path = "native_backend/control.rs"]
mod control;
#[path = "native_backend/decode.rs"]
mod decode;
#[path = "native_backend/decoder_open.rs"]
mod decoder_open;
#[path = "native_backend/dmabuf.rs"]
mod dmabuf;
#[path = "native_backend/frame_copy.rs"]
mod frame_copy;
#[path = "native_backend/shared_decode.rs"]
mod shared_decode;

pub use capabilities::{NativeDecoderApi, NativePathTier};

use crate::metrics::PerformanceMetrics;
use crate::video::{LatestFrameMailbox, PlayerEvent, VideoFrame};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

fn native_color_metadata(
    frame: &ffmpeg_next::util::frame::video::Video,
) -> crate::video::VideoColorMetadata {
    use ffmpeg_next::util::frame::side_data::Type as SideDataType;
    let matrix = match frame.color_space().name() {
        Some("bt470bg" | "smpte170m" | "fcc") => crate::video::VideoColorMatrix::Bt601,
        Some(name) if name.contains("bt2020") => crate::video::VideoColorMatrix::Bt2020,
        _ => crate::video::VideoColorMatrix::Bt709,
    };
    let range = match frame.color_range().name() {
        Some("pc" | "jpeg") => crate::video::VideoColorRange::Full,
        _ => crate::video::VideoColorRange::Limited,
    };
    let transfer = match frame.color_transfer_characteristic().name() {
        Some("iec61966-2-1") => crate::video::VideoTransfer::Srgb,
        Some("bt709") => crate::video::VideoTransfer::Bt709,
        Some("smpte2084") => crate::video::VideoTransfer::Pq,
        Some("arib-std-b67") => crate::video::VideoTransfer::Hlg,
        _ => crate::video::VideoTransfer::Bt1886,
    };
    let primaries = match frame.color_primaries().name() {
        Some("bt2020") => crate::video::VideoColorPrimaries::Bt2020,
        Some("bt470bg") => crate::video::VideoColorPrimaries::Bt601Pal,
        Some("smpte170m") => crate::video::VideoColorPrimaries::Bt601Ntsc,
        Some("smpte431") => crate::video::VideoColorPrimaries::DciP3,
        Some("smpte432") => crate::video::VideoColorPrimaries::DisplayP3,
        _ => crate::video::VideoColorPrimaries::Bt709,
    };
    let mastering = frame
        .side_data(SideDataType::MasteringDisplayMetadata)
        .and_then(|side_data| parse_mastering_display(side_data.data()));
    let content_light = frame
        .side_data(SideDataType::ContentLightLevel)
        .and_then(|side_data| parse_content_light(side_data.data()))
        .unwrap_or_default();
    crate::video::VideoColorMetadata {
        matrix,
        range,
        transfer,
        primaries,
        chroma_siting: crate::video::VideoChromaSiting::Center,
        bit_depth: match native_storage_format(frame) {
            ffmpeg_next::format::Pixel::P010LE | ffmpeg_next::format::Pixel::P010BE => 10,
            _ => 8,
        },
        mastering,
        content_light,
    }
}

/// Return the software-visible layout behind an opaque hardware frame.
///
/// `AVFrame::format` is VAAPI/Vulkan for decoder surfaces, while `sw_format`
/// is the actual NV12/P010 layout exported or transferred downstream. Picking
/// an import path from the opaque wrapper would otherwise mislabel P010 as
/// NV12 and fail only after the renderer tries to import it.
fn native_storage_format(
    frame: &ffmpeg_next::util::frame::video::Video,
) -> ffmpeg_next::format::Pixel {
    let direct = frame.format();
    // SAFETY: `frame` retains `hw_frames_ctx`. AVBufferRef::data points to the
    // documented AVHWFramesContext for the duration of this immutable borrow.
    unsafe {
        let context_ref = (*frame.as_ptr()).hw_frames_ctx;
        if context_ref.is_null() || (*context_ref).data.is_null() {
            return direct;
        }
        let frames_context = (*context_ref)
            .data
            .cast::<ffmpeg_next::ffi::AVHWFramesContext>();
        ffmpeg_next::format::Pixel::from((*frames_context).sw_format)
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AvRationalLayout {
    num: i32,
    den: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AvMasteringDisplayLayout {
    display_primaries: [[AvRationalLayout; 2]; 3],
    white_point: [AvRationalLayout; 2],
    min_luminance: AvRationalLayout,
    max_luminance: AvRationalLayout,
    has_primaries: i32,
    has_luminance: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AvContentLightLayout {
    max_cll: u32,
    max_fall: u32,
}

fn rational(value: AvRationalLayout) -> Option<f32> {
    (value.den != 0).then(|| value.num as f32 / value.den as f32)
}

fn parse_mastering_display(bytes: &[u8]) -> Option<crate::video::VideoMasteringMetadata> {
    if bytes.len() < std::mem::size_of::<AvMasteringDisplayLayout>() {
        return None;
    }
    // SAFETY: length is checked and read_unaligned copies the C payload into a
    // repr(C) layout matching AVMasteringDisplayMetadata.
    let metadata =
        unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<AvMasteringDisplayLayout>()) };
    if metadata.has_primaries == 0 || metadata.has_luminance == 0 {
        return None;
    }
    Some(crate::video::VideoMasteringMetadata {
        primaries_xy: [
            [
                rational(metadata.display_primaries[0][0])?,
                rational(metadata.display_primaries[0][1])?,
            ],
            [
                rational(metadata.display_primaries[1][0])?,
                rational(metadata.display_primaries[1][1])?,
            ],
            [
                rational(metadata.display_primaries[2][0])?,
                rational(metadata.display_primaries[2][1])?,
            ],
            [
                rational(metadata.white_point[0])?,
                rational(metadata.white_point[1])?,
            ],
        ],
        min_luminance_nits: rational(metadata.min_luminance)?,
        max_luminance_nits: rational(metadata.max_luminance)?,
    })
}

fn parse_content_light(bytes: &[u8]) -> Option<crate::video::VideoContentLightMetadata> {
    if bytes.len() < std::mem::size_of::<AvContentLightLayout>() {
        return None;
    }
    // SAFETY: see parse_mastering_display; this payload consists of two u32s.
    let metadata =
        unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<AvContentLightLayout>()) };
    Some(crate::video::VideoContentLightMetadata {
        max_content_light_level: (metadata.max_cll != 0).then_some(metadata.max_cll),
        max_frame_average_light_level: (metadata.max_fall != 0).then_some(metadata.max_fall),
    })
}

fn native_geometry(frame: &ffmpeg_next::util::frame::video::Video) -> crate::video::VideoGeometry {
    let width = frame.width();
    let height = frame.height();
    // SAFETY: `raw` is the live AVFrame wrapped by `frame`; crop fields and the
    // sample aspect ratio are immutable decoder metadata while it is borrowed.
    let (crop_left, crop_top, crop_right, crop_bottom) = unsafe {
        let raw = frame.as_ptr();
        (
            (*raw).crop_left as u32,
            (*raw).crop_top as u32,
            (*raw).crop_right as u32,
            (*raw).crop_bottom as u32,
        )
    };
    let mut geometry = crate::video::VideoGeometry::for_dimensions(width, height);
    let sar = frame.aspect_ratio();
    geometry.sample_aspect_num = sar.numerator().max(1) as u32;
    geometry.sample_aspect_den = sar.denominator().max(1) as u32;
    let crop_width = width
        .saturating_sub(crop_left.saturating_add(crop_right))
        .max(1);
    let crop_height = height
        .saturating_sub(crop_top.saturating_add(crop_bottom))
        .max(1);
    geometry.crop = crate::video::VideoCropRect {
        x: crop_left,
        y: crop_top,
        width: crop_width,
        height: crop_height,
    };
    geometry.display_width = crop_width;
    geometry.display_height = crop_height;
    use ffmpeg_next::util::frame::side_data::Type as SideDataType;
    if let Some(matrix) = frame.side_data(SideDataType::DisplayMatrix)
        && matrix.data().len() >= 9 * std::mem::size_of::<i32>()
    {
        // SAFETY: FFmpeg defines display-matrix side data as nine i32 values;
        // length is checked before passing its immutable pointer.
        let degrees = unsafe {
            ffmpeg_next::ffi::av_display_rotation_get(matrix.data().as_ptr().cast::<i32>())
        };
        if degrees.is_finite() {
            let quarter_turns = ((degrees / 90.0).round() as i32).rem_euclid(4);
            geometry.rotation = match quarter_turns {
                1 => crate::video::VideoRotation::Rotate90,
                2 => crate::video::VideoRotation::Rotate180,
                3 => crate::video::VideoRotation::Rotate270,
                _ => crate::video::VideoRotation::Rotate0,
            };
        }
    }
    geometry
}

static NATIVE_SURFACE_IMPORT_FAILED: AtomicBool = AtomicBool::new(false);

pub(crate) fn native_surface_import_available() -> bool {
    !NATIVE_SURFACE_IMPORT_FAILED.load(Ordering::Acquire)
}

/// Permanently lower this process to the next rung after the renderer proves
/// that the selected decoder-surface layout cannot be imported safely.
/// Returns true only for the first reporter.
pub(crate) fn report_native_surface_import_failure() -> bool {
    NATIVE_SURFACE_IMPORT_FAILED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

pub struct NativePlayer {
    source_id: Arc<String>,
    session_id: u64,
    mailbox: LatestFrameMailbox,
    shared: Arc<shared_decode::SharedDecodeSession>,
    first_frame_rx: Receiver<VideoFrame>,
    subscribed: bool,
    volume: f64,
}

impl NativePlayer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        mailbox: LatestFrameMailbox,
        event_tx: tokio::sync::mpsc::Sender<PlayerEvent>,
        metrics: Arc<PerformanceMetrics>,
        decode_group_id: Option<u64>,
        max_publish_fps: Option<u32>,
        creation_start: Instant,
    ) -> anyhow::Result<Self> {
        let worker_name = native_thread_name(source_id.as_ref(), session_id);
        let (shared, first_frame_rx) =
            shared_decode::SharedDecodeSession::acquire(shared_decode::AcquireRequest {
                uri: uri.to_string(),
                source_id: source_id.clone(),
                session_id,
                mailbox: mailbox.clone(),
                event_tx,
                metrics,
                group_id: decode_group_id.unwrap_or(session_id),
                max_publish_fps,
                creation_start,
                worker_name,
            })?;
        if volume > f64::EPSILON {
            warn!(
                "[NATIVE-VIDEO] {} session={}: audio is not wired in the first experimental slice; video remains muted",
                source_id, session_id
            );
        }
        Ok(Self {
            source_id,
            session_id,
            mailbox,
            shared,
            first_frame_rx,
            subscribed: true,
            volume,
        })
    }

    pub fn prebuffer<F>(&mut self, should_abort: F) -> anyhow::Result<Option<VideoFrame>>
    where
        F: Fn() -> bool,
    {
        let deadline = Instant::now() + Duration::from_millis(2_000);
        loop {
            if should_abort() {
                anyhow::bail!("native prebuffer aborted");
            }
            if Instant::now() >= deadline {
                debug!(
                    "[NATIVE-VIDEO] {} session={}: prebuffer timed out; playback may still start asynchronously",
                    self.source_id, self.session_id
                );
                return Ok(None);
            }
            match self.first_frame_rx.recv_timeout(Duration::from_millis(50)) {
                Ok(frame) => {
                    self.mailbox.clear_source(self.source_id.as_ref());
                    return Ok(Some(frame));
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    anyhow::bail!("native decoder exited before producing a frame");
                }
            }
        }
    }

    pub fn start(&self) {
        info!(
            "[NATIVE-VIDEO] {} session={}: playback started",
            self.source_id, self.session_id
        );
        self.shared.control.play();
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        self.mailbox.clear_source(self.source_id.as_ref());
        if self.subscribed {
            self.subscribed = false;
            self.shared.release(self.session_id)?;
        }
        Ok(())
    }

    pub fn pause(&self) {
        self.shared.control.pause();
    }

    pub fn resume(&self) {
        self.shared.control.play();
    }

    pub fn seek_to_position_ns(&self, position_ns: u64) {
        self.shared.control.seek(position_ns);
    }

    pub fn current_position_ns(&self) -> u64 {
        self.shared.control.position_ns()
    }

    pub fn request_frame(&self) {
        self.shared.control.request_frame();
    }

    pub fn set_volume(&mut self, volume: f64) {
        if volume > f64::EPSILON && self.volume <= f64::EPSILON {
            warn!(
                "[NATIVE-VIDEO] {} session={}: audio is not implemented in native-experimental",
                self.source_id, self.session_id
            );
        }
        self.volume = volume;
    }
}

impl Drop for NativePlayer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn native_thread_name(source_id: &str, session_id: u64) -> String {
    let short_source: String = source_id.chars().take(10).collect();
    format!("kld-native-{short_source}-{session_id}")
}
