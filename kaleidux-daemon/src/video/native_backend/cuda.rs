use crate::video::{NativeCudaFrame, VideoFrame, VideoFrameFormat, VideoFrameStorage};
use ffmpeg_next::ffi;
use ffmpeg_next::util::frame::video::Video;
use std::sync::Arc;

// Public AVCUDADeviceContext prefix, from libavutil/hwcontext_cuda.h. Opaque
// driver tokens have pointer width; the private trailing field is never read.
#[repr(C)]
struct CudaDeviceContext {
    context: usize,
    stream: usize,
}

impl super::dmabuf::NativeSurfaceExporter {
    pub(super) fn export_cuda_nv12(
        &self,
        decoded: Video,
        session_id: u64,
        pts_ns: Option<u64>,
        duration_ns: Option<u64>,
    ) -> Result<VideoFrame, (anyhow::Error, Video)> {
        let layout = match cuda_layout(&decoded) {
            Ok(layout) => layout,
            Err(error) => return Err((error, decoded)),
        };
        let color = super::native_color_metadata(&decoded);
        let geometry = super::native_geometry(&decoded);
        let width = decoded.width();
        let height = decoded.height();
        Ok(VideoFrame {
            storage: VideoFrameStorage::Native(Arc::new(NativeCudaFrame {
                device_ptr: layout.address,
                byte_len: layout.byte_len,
                context: layout.context,
                stream: layout.stream,
                owner: self.retain_frame(decoded),
            })),
            width,
            height,
            stride: layout.y_stride,
            format: VideoFrameFormat::CudaNv12 {
                y_stride: layout.y_stride,
                uv_stride: layout.uv_stride,
                uv_offset: layout.uv_offset,
            },
            session_id,
            pts_ns,
            duration_ns,
            color,
            geometry,
        })
    }
}

struct CudaLayout {
    address: u64,
    byte_len: usize,
    y_stride: u32,
    uv_stride: u32,
    uv_offset: u32,
    context: usize,
    stream: usize,
}

fn cuda_layout(decoded: &Video) -> anyhow::Result<CudaLayout> {
    // SAFETY: the received CUDA AVFrame retains its frames/device references.
    // AVCUDADeviceContext is the documented hardware-context prefix above.
    unsafe {
        let frame = &*decoded.as_ptr();
        anyhow::ensure!(
            !frame.hw_frames_ctx.is_null(),
            "CUDA frame has no frames context"
        );
        let frames = &*(*frame.hw_frames_ctx).data.cast::<ffi::AVHWFramesContext>();
        anyhow::ensure!(
            !frames.device_ctx.is_null(),
            "CUDA frame has no device context"
        );
        let hardware = (*frames.device_ctx).hwctx.cast::<CudaDeviceContext>();
        anyhow::ensure!(!hardware.is_null(), "CUDA device has no CUDA context");
        let address = frame.data[0] as usize as u64;
        let uv_address = frame.data[1] as usize as u64;
        let y_stride = u32::try_from(frame.linesize[0])?;
        let uv_stride = u32::try_from(frame.linesize[1])?;
        let uv_offset = u32::try_from(
            uv_address
                .checked_sub(address)
                .ok_or_else(|| anyhow::anyhow!("CUDA NV12 planes are not contiguous"))?,
        )?;
        anyhow::ensure!(
            address != 0
                && y_stride >= decoded.width()
                && uv_stride >= decoded.width().div_ceil(2) * 2
                && u64::from(uv_offset) >= u64::from(y_stride) * u64::from(decoded.height()),
            "invalid CUDA NV12 plane layout"
        );
        let byte_len = (uv_offset as usize)
            .checked_add(
                (uv_stride as usize)
                    .checked_mul(decoded.height().div_ceil(2) as usize)
                    .ok_or_else(|| anyhow::anyhow!("CUDA chroma size overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("CUDA frame size overflow"))?;
        Ok(CudaLayout {
            address,
            byte_len,
            y_stride,
            uv_stride,
            uv_offset,
            context: (*hardware).context,
            stream: (*hardware).stream,
        })
    }
}
