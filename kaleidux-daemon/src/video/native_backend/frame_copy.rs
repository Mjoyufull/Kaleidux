use crate::video::{VideoFrame, VideoFrameFormat, VideoFrameStorage};
use ffmpeg_next as ffmpeg;
use ffmpeg_next::format::Pixel;
use ffmpeg_next::software::scaling::{context::Context as ScaleContext, flag::Flags};
use ffmpeg_next::util::frame::video::Video;

pub struct NativeFrameConverter {
    scaler: Option<ScaleContext>,
    converted: Video,
    p010_fallback_logged: bool,
}

impl NativeFrameConverter {
    pub fn new() -> Self {
        Self {
            scaler: None,
            converted: Video::empty(),
            p010_fallback_logged: false,
        }
    }

    pub fn convert(
        &mut self,
        decoded: &Video,
        session_id: u64,
        pts_ns: Option<u64>,
        duration_ns: Option<u64>,
    ) -> anyhow::Result<VideoFrame> {
        let mut color = super::native_color_metadata(decoded);
        let geometry = super::native_geometry(decoded);
        match decoded.format() {
            Pixel::NV12 => copy_nv12(decoded, session_id, pts_ns, duration_ns, color, geometry),
            Pixel::P010LE if crate::video::p010_sampling_supported() == Some(true) => {
                copy_p010(decoded, session_id, pts_ns, duration_ns, color, geometry)
            }
            Pixel::P010LE => {
                if !self.p010_fallback_logged {
                    self.p010_fallback_logged = true;
                    tracing::warn!(
                        "[VIDEO] P010 source is being converted to 8-bit I420 because this renderer cannot sample R16/RG16 normalized textures"
                    );
                }
                self.ensure_scaler(
                    Pixel::P010LE,
                    decoded.width(),
                    decoded.height(),
                    Pixel::YUV420P,
                )?;
                self.scaler
                    .as_mut()
                    .expect("scaler was initialized")
                    .run(decoded, &mut self.converted)?;
                color.bit_depth = 8;
                copy_i420(
                    &self.converted,
                    session_id,
                    pts_ns,
                    duration_ns,
                    color,
                    geometry,
                )
            }
            Pixel::YUV420P | Pixel::YUVJ420P => {
                copy_i420(decoded, session_id, pts_ns, duration_ns, color, geometry)
            }
            source_format => {
                self.ensure_scaler(
                    source_format,
                    decoded.width(),
                    decoded.height(),
                    Pixel::YUV420P,
                )?;
                self.scaler
                    .as_mut()
                    .expect("scaler was initialized")
                    .run(decoded, &mut self.converted)?;
                color.bit_depth = 8;
                copy_i420(
                    &self.converted,
                    session_id,
                    pts_ns,
                    duration_ns,
                    color,
                    geometry,
                )
            }
        }
    }

    fn ensure_scaler(
        &mut self,
        source_format: Pixel,
        width: u32,
        height: u32,
        target_format: Pixel,
    ) -> Result<(), ffmpeg::Error> {
        let matches = self
            .scaler
            .as_ref()
            .map(|scaler| {
                scaler.input().format == source_format
                    && scaler.input().width == width
                    && scaler.input().height == height
                    && scaler.output().format == target_format
            })
            .unwrap_or(false);
        if matches {
            return Ok(());
        }
        // SAFETY: the converter owns this scratch AVFrame. A format/size
        // renegotiation invalidates its allocated output buffers, while the
        // wrapper itself remains reusable.
        unsafe { ffmpeg_next::ffi::av_frame_unref(self.converted.as_mut_ptr()) };
        self.scaler = Some(ScaleContext::get(
            source_format,
            width,
            height,
            target_format,
            width,
            height,
            Flags::FAST_BILINEAR,
        )?);
        Ok(())
    }
}

fn copy_p010(
    frame: &Video,
    session_id: u64,
    pts_ns: Option<u64>,
    duration_ns: Option<u64>,
    mut color: crate::video::VideoColorMetadata,
    geometry: crate::video::VideoGeometry,
) -> anyhow::Result<VideoFrame> {
    let width = frame.width();
    let height = frame.height();
    let uv_width = width.div_ceil(2);
    let uv_height = height.div_ceil(2);
    let y_stride = width
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("P010 luma stride overflow"))?;
    let uv_stride = uv_width
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("P010 chroma stride overflow"))?;
    let uv_offset = y_stride
        .checked_mul(height)
        .ok_or_else(|| anyhow::anyhow!("P010 frame dimensions overflow"))?;
    let size = uv_offset
        .checked_add(
            uv_stride
                .checked_mul(uv_height)
                .ok_or_else(|| anyhow::anyhow!("P010 chroma dimensions overflow"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("P010 frame size overflow"))?;
    let mut bytes = vec![0u8; size as usize];
    copy_plane(
        frame.data(0),
        frame.stride(0),
        &mut bytes,
        0,
        y_stride as usize,
        y_stride as usize,
        height as usize,
    )?;
    copy_plane(
        frame.data(1),
        frame.stride(1),
        &mut bytes,
        uv_offset as usize,
        uv_stride as usize,
        uv_stride as usize,
        uv_height as usize,
    )?;
    color.bit_depth = 10;
    Ok(VideoFrame {
        storage: VideoFrameStorage::from(bytes),
        width,
        height,
        stride: y_stride,
        format: VideoFrameFormat::P010 {
            y_stride,
            uv_offset,
            uv_stride,
        },
        session_id,
        pts_ns,
        duration_ns,
        color,
        geometry,
    })
}

fn copy_nv12(
    frame: &Video,
    session_id: u64,
    pts_ns: Option<u64>,
    duration_ns: Option<u64>,
    color: crate::video::VideoColorMetadata,
    geometry: crate::video::VideoGeometry,
) -> anyhow::Result<VideoFrame> {
    let width = frame.width();
    let height = frame.height();
    let uv_width = width.div_ceil(2);
    let uv_height = height.div_ceil(2);
    let y_stride = width;
    let uv_stride = uv_width * 2;
    let uv_offset = y_stride
        .checked_mul(height)
        .ok_or_else(|| anyhow::anyhow!("NV12 frame dimensions overflow"))?;
    let size = uv_offset
        .checked_add(
            uv_stride
                .checked_mul(uv_height)
                .ok_or_else(|| anyhow::anyhow!("NV12 chroma dimensions overflow"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("NV12 frame size overflow"))?;
    let mut bytes = vec![0u8; size as usize];
    copy_plane(
        frame.data(0),
        frame.stride(0),
        &mut bytes,
        0,
        y_stride as usize,
        width as usize,
        height as usize,
    )?;
    copy_plane(
        frame.data(1),
        frame.stride(1),
        &mut bytes,
        uv_offset as usize,
        uv_stride as usize,
        uv_stride as usize,
        uv_height as usize,
    )?;
    Ok(VideoFrame {
        storage: VideoFrameStorage::from(bytes),
        width,
        height,
        stride: y_stride,
        format: VideoFrameFormat::Nv12 {
            y_stride,
            uv_offset,
            uv_stride,
        },
        session_id,
        pts_ns,
        duration_ns,
        color,
        geometry,
    })
}

fn copy_i420(
    frame: &Video,
    session_id: u64,
    pts_ns: Option<u64>,
    duration_ns: Option<u64>,
    color: crate::video::VideoColorMetadata,
    geometry: crate::video::VideoGeometry,
) -> anyhow::Result<VideoFrame> {
    let width = frame.width();
    let height = frame.height();
    let chroma_width = width.div_ceil(2);
    let chroma_height = height.div_ceil(2);
    let y_stride = width;
    let u_stride = chroma_width;
    let v_stride = chroma_width;
    let u_offset = y_stride
        .checked_mul(height)
        .ok_or_else(|| anyhow::anyhow!("I420 frame dimensions overflow"))?;
    let chroma_size = chroma_width
        .checked_mul(chroma_height)
        .ok_or_else(|| anyhow::anyhow!("I420 chroma dimensions overflow"))?;
    let v_offset = u_offset
        .checked_add(chroma_size)
        .ok_or_else(|| anyhow::anyhow!("I420 U offset overflow"))?;
    let size = v_offset
        .checked_add(chroma_size)
        .ok_or_else(|| anyhow::anyhow!("I420 frame size overflow"))?;
    let mut bytes = vec![0u8; size as usize];
    copy_plane(
        frame.data(0),
        frame.stride(0),
        &mut bytes,
        0,
        y_stride as usize,
        width as usize,
        height as usize,
    )?;
    copy_plane(
        frame.data(1),
        frame.stride(1),
        &mut bytes,
        u_offset as usize,
        u_stride as usize,
        chroma_width as usize,
        chroma_height as usize,
    )?;
    copy_plane(
        frame.data(2),
        frame.stride(2),
        &mut bytes,
        v_offset as usize,
        v_stride as usize,
        chroma_width as usize,
        chroma_height as usize,
    )?;
    Ok(VideoFrame {
        storage: VideoFrameStorage::from(bytes),
        width,
        height,
        stride: y_stride,
        format: VideoFrameFormat::I420 {
            y_stride,
            u_offset,
            u_stride,
            v_offset,
            v_stride,
        },
        session_id,
        pts_ns,
        duration_ns,
        color,
        geometry,
    })
}

#[allow(clippy::too_many_arguments)]
fn copy_plane(
    source: &[u8],
    source_stride: usize,
    target: &mut [u8],
    target_offset: usize,
    target_stride: usize,
    row_bytes: usize,
    rows: usize,
) -> anyhow::Result<()> {
    if source_stride < row_bytes || target_stride < row_bytes {
        anyhow::bail!(
            "invalid plane stride source={source_stride} target={target_stride} row={row_bytes}"
        );
    }
    for row in 0..rows {
        let source_start = row
            .checked_mul(source_stride)
            .ok_or_else(|| anyhow::anyhow!("source plane offset overflow"))?;
        let source_end = source_start + row_bytes;
        let target_start = target_offset
            .checked_add(
                row.checked_mul(target_stride)
                    .ok_or_else(|| anyhow::anyhow!("target plane offset overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("target plane offset overflow"))?;
        let target_end = target_start + row_bytes;
        let source_row = source
            .get(source_start..source_end)
            .ok_or_else(|| anyhow::anyhow!("source frame plane is truncated"))?;
        let target_row = target
            .get_mut(target_start..target_end)
            .ok_or_else(|| anyhow::anyhow!("target frame plane is truncated"))?;
        target_row.copy_from_slice(source_row);
    }
    Ok(())
}
