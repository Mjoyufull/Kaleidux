use crate::renderer;

pub(crate) const MAX_IMAGE_UPLOAD_DIMENSION: u32 = 8192;

fn compute_cover_target_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    renderer::compute_cover_target_dimensions(
        source_width,
        source_height,
        target_width,
        target_height,
    )
}

fn apply_upload_dimension_clamp(source_width: u32, source_height: u32) -> Option<(u32, u32)> {
    if source_width <= MAX_IMAGE_UPLOAD_DIMENSION && source_height <= MAX_IMAGE_UPLOAD_DIMENSION {
        return None;
    }

    let longest_edge = source_width.max(source_height) as f32;
    let scale = MAX_IMAGE_UPLOAD_DIMENSION as f32 / longest_edge;
    let resized_width = ((source_width as f32 * scale).round() as u32).max(1);
    let resized_height = ((source_height as f32 * scale).round() as u32).max(1);
    Some((resized_width, resized_height))
}

pub(crate) fn compute_upload_downscale_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> Option<(u32, u32)> {
    let (cover_width, cover_height) =
        compute_cover_target_dimensions(source_width, source_height, target_width, target_height);
    let (prepared_width, prepared_height) = apply_upload_dimension_clamp(cover_width, cover_height)
        .unwrap_or((cover_width, cover_height));

    if prepared_width == source_width && prepared_height == source_height {
        None
    } else {
        Some((prepared_width, prepared_height))
    }
}

pub(crate) fn prepared_target_dimensions_from_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    let (cover_width, cover_height) =
        compute_cover_target_dimensions(source_width, source_height, target_width, target_height);
    apply_upload_dimension_clamp(cover_width, cover_height).unwrap_or((cover_width, cover_height))
}

fn select_resize_filter(
    source_width: u32,
    source_height: u32,
    resized_width: u32,
    resized_height: u32,
) -> fast_image_resize::FilterType {
    let width_ratio = source_width as f32 / resized_width as f32;
    let height_ratio = source_height as f32 / resized_height as f32;
    if width_ratio >= 2.0 || height_ratio >= 2.0 {
        fast_image_resize::FilterType::Bilinear
    } else {
        fast_image_resize::FilterType::CatmullRom
    }
}

fn resize_filter_label(filter: fast_image_resize::FilterType) -> &'static str {
    match filter {
        fast_image_resize::FilterType::Box => "box",
        fast_image_resize::FilterType::Bilinear => "bilinear",
        fast_image_resize::FilterType::Hamming => "hamming",
        fast_image_resize::FilterType::CatmullRom => "catmull-rom",
        fast_image_resize::FilterType::Mitchell => "mitchell",
        fast_image_resize::FilterType::Gaussian => "gaussian",
        fast_image_resize::FilterType::Lanczos3 => "lanczos3",
        fast_image_resize::FilterType::Custom(_) => "custom",
        _ => "unknown",
    }
}

fn resize_image_buffer(
    source_data: &[u8],
    source_width: u32,
    source_height: u32,
    resized_width: u32,
    resized_height: u32,
    pixel_type: fast_image_resize::PixelType,
    filter: fast_image_resize::FilterType,
) -> anyhow::Result<Vec<u8>> {
    use fast_image_resize as fr;

    let source = fr::images::ImageRef::new(source_width, source_height, source_data, pixel_type)
        .map_err(|e| anyhow::anyhow!("invalid source image buffer: {}", e))?;
    let mut resized = fr::images::Image::new(resized_width, resized_height, pixel_type);
    let mut resizer = fr::Resizer::new();
    resizer
        .resize(
            &source,
            &mut resized,
            Some(&fr::ResizeOptions::new().resize_alg(fr::ResizeAlg::Convolution(filter))),
        )
        .map_err(|e| anyhow::anyhow!("image resize failed: {}", e))?;
    Ok(resized.into_vec())
}

fn expand_rgb_to_rgba(rgb: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((rgb.len() / 3) * 4);
    for chunk in rgb.chunks_exact(3) {
        rgba.extend_from_slice(&[chunk[0], chunk[1], chunk[2], 255]);
    }
    rgba
}

fn expand_luma_to_rgba(luma: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity(luma.len() * 4);
    for value in luma {
        rgba.extend_from_slice(&[*value, *value, *value, 255]);
    }
    rgba
}

fn expand_lumaa_to_rgba(lumaa: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((lumaa.len() / 2) * 4);
    for chunk in lumaa.chunks_exact(2) {
        rgba.extend_from_slice(&[chunk[0], chunk[0], chunk[0], chunk[1]]);
    }
    rgba
}

pub(crate) fn prepare_rgb_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(
    Vec<u8>,
    u32,
    u32,
    std::time::Duration,
    std::time::Duration,
    Option<String>,
)> {
    let pixels = pixels.as_ref();
    let mut resize_duration = std::time::Duration::ZERO;
    let mut resize_filter = None;
    let (rgba_data, width, height, expand_duration) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = std::time::Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x3,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        let expand_start = std::time::Instant::now();
        let rgba = expand_rgb_to_rgba(&resized);
        (rgba, resized_width, resized_height, expand_start.elapsed())
    } else {
        let expand_start = std::time::Instant::now();
        let rgba = expand_rgb_to_rgba(pixels);
        (rgba, source_width, source_height, expand_start.elapsed())
    };

    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

pub(crate) fn prepare_rgba_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(Vec<u8>, u32, u32, std::time::Duration, Option<String>)> {
    let pixels = pixels.as_ref();
    if let Some((resized_width, resized_height)) = compute_upload_downscale_dimensions(
        source_width,
        source_height,
        target_width,
        target_height,
    ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = std::time::Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x4,
            filter,
        )?;
        return Ok((
            resized,
            resized_width,
            resized_height,
            resize_start.elapsed(),
            Some(resize_filter_label(filter).to_string()),
        ));
    }

    Ok((
        pixels.to_vec(),
        source_width,
        source_height,
        std::time::Duration::ZERO,
        None,
    ))
}

pub(crate) fn prepare_luma_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(
    Vec<u8>,
    u32,
    u32,
    std::time::Duration,
    std::time::Duration,
    Option<String>,
)> {
    let pixels = pixels.as_ref();
    let mut resize_duration = std::time::Duration::ZERO;
    let mut resize_filter = None;
    let (rgba_data, width, height, expand_duration) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = std::time::Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        let expand_start = std::time::Instant::now();
        let rgba = expand_luma_to_rgba(&resized);
        (rgba, resized_width, resized_height, expand_start.elapsed())
    } else {
        let expand_start = std::time::Instant::now();
        let rgba = expand_luma_to_rgba(pixels);
        (rgba, source_width, source_height, expand_start.elapsed())
    };

    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

pub(crate) fn prepare_lumaa_image<T: AsRef<[u8]>>(
    pixels: T,
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<(
    Vec<u8>,
    u32,
    u32,
    std::time::Duration,
    std::time::Duration,
    Option<String>,
)> {
    let pixels = pixels.as_ref();
    let mut resize_duration = std::time::Duration::ZERO;
    let mut resize_filter = None;
    let (rgba_data, width, height, expand_duration) = if let Some((resized_width, resized_height)) =
        compute_upload_downscale_dimensions(
            source_width,
            source_height,
            target_width,
            target_height,
        ) {
        let filter =
            select_resize_filter(source_width, source_height, resized_width, resized_height);
        let resize_start = std::time::Instant::now();
        let resized = resize_image_buffer(
            pixels,
            source_width,
            source_height,
            resized_width,
            resized_height,
            fast_image_resize::PixelType::U8x2,
            filter,
        )?;
        resize_duration = resize_start.elapsed();
        resize_filter = Some(resize_filter_label(filter).to_string());
        let expand_start = std::time::Instant::now();
        let rgba = expand_lumaa_to_rgba(&resized);
        (rgba, resized_width, resized_height, expand_start.elapsed())
    } else {
        let expand_start = std::time::Instant::now();
        let rgba = expand_lumaa_to_rgba(pixels);
        (rgba, source_width, source_height, expand_start.elapsed())
    };

    Ok((
        rgba_data,
        width,
        height,
        resize_duration,
        expand_duration,
        resize_filter,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_resize_does_not_touch_normal_images() {
        assert_eq!(
            compute_upload_downscale_dimensions(1280, 720, 1920, 1080),
            None
        );
    }

    #[test]
    fn upload_resize_downscales_only_oversized_sources() {
        assert_eq!(
            compute_upload_downscale_dimensions(
                MAX_IMAGE_UPLOAD_DIMENSION * 2,
                4000,
                MAX_IMAGE_UPLOAD_DIMENSION * 2,
                4000,
            ),
            Some((MAX_IMAGE_UPLOAD_DIMENSION, 2000))
        );
    }

    #[test]
    fn cover_target_downscales_to_minimum_cover_size() {
        assert_eq!(
            compute_upload_downscale_dimensions(6000, 4000, 1920, 1080),
            Some((1920, 1280))
        );
        assert_eq!(
            compute_upload_downscale_dimensions(3000, 4500, 1920, 1080),
            Some((1920, 2880))
        );
    }

    #[test]
    fn rgb_prep_expands_to_rgba_without_resize_when_not_needed() {
        let rgb = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 2, 2, 3840, 2160).expect("rgb prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, std::time::Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 20, 30, 255, 40, 50, 60, 255, 70, 80, 90, 255, 100, 110, 120, 255,
            ]
        );
    }

    #[test]
    fn rgb_prep_keeps_source_dimensions_even_when_target_is_smaller() {
        let rgb = vec![64; 4 * 3];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_rgb_image(rgb, 4, 1, 1, 1).expect("rgb prep should succeed");

        assert_eq!((width, height), (4, 1));
        assert_eq!(resize, std::time::Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(rgba.len(), 4 * 4);
    }

    #[test]
    fn luma_prep_expands_to_rgba_without_resize_when_not_needed() {
        let luma = vec![10, 40, 70, 100];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_luma_image(luma, 2, 2, 3840, 2160).expect("luma prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, std::time::Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 10, 10, 255, 40, 40, 40, 255, 70, 70, 70, 255, 100, 100, 100, 255,
            ]
        );
    }

    #[test]
    fn lumaa_prep_preserves_alpha() {
        let lumaa = vec![10, 11, 40, 41, 70, 71, 100, 101];
        let (rgba, width, height, resize, _expand, filter) =
            prepare_lumaa_image(lumaa, 2, 2, 3840, 2160).expect("lumaa prep should succeed");

        assert_eq!((width, height), (2, 2));
        assert_eq!(resize, std::time::Duration::ZERO);
        assert_eq!(filter, None);
        assert_eq!(
            rgba,
            vec![
                10, 10, 10, 11, 40, 40, 40, 41, 70, 70, 70, 71, 100, 100, 100, 101,
            ]
        );
    }
}
