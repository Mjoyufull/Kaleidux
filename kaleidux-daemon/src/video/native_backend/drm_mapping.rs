use super::capabilities::NativeDecoderApi;
use super::dmabuf::CachedSurfaceLayout;
use crate::video::{NativeDmaBufObject, NativeDmaBufPlane};
use ffmpeg_next::{ffi, util::frame::video::Video};
use std::os::fd::{FromRawFd, OwnedFd};

pub(super) fn map_drm_prime_layout(
    decoded: &Video,
    api: NativeDecoderApi,
) -> anyhow::Result<(CachedSurfaceLayout, Video)> {
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
        let duplicated = unsafe { libc::fcntl(object.fd, libc::F_DUPFD_CLOEXEC, 0) };
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
    Ok((
        CachedSurfaceLayout {
            objects: objects.into(),
            planes,
            stride,
        },
        mapped,
    ))
}
