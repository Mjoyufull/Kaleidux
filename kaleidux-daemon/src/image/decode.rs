use crate::image::types::{
    DecodedImagePayload, DecodedSourceImage, DecodedSourcePixels, ImageLoadProfile,
};
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use zune_core::bytestream::ZCursor;
use zune_core::colorspace::ColorSpace;
use zune_core::options::DecoderOptions;

fn image_format_label(format: Option<image::ImageFormat>, fast_path: bool) -> String {
    let label = match format {
        Some(image::ImageFormat::Avif) => "avif",
        Some(image::ImageFormat::Bmp) => "bmp",
        Some(image::ImageFormat::Gif) => "gif",
        Some(image::ImageFormat::Hdr) => "hdr",
        Some(image::ImageFormat::Ico) => "ico",
        Some(image::ImageFormat::Jpeg) => "jpeg",
        Some(image::ImageFormat::OpenExr) => "openexr",
        Some(image::ImageFormat::Png) => "png",
        Some(image::ImageFormat::Pnm) => "pnm",
        Some(image::ImageFormat::Qoi) => "qoi",
        Some(image::ImageFormat::Tga) => "tga",
        Some(image::ImageFormat::Tiff) => "tiff",
        Some(image::ImageFormat::WebP) => "webp",
        Some(image::ImageFormat::Dds) => "dds",
        Some(image::ImageFormat::Farbfeld) => "farbfeld",
        _ => "unknown",
    };

    if fast_path {
        format!("{}-fast", label)
    } else {
        label.to_string()
    }
}

fn decode_jpeg_source_fast(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    let decode_start = Instant::now();
    let encoded = std::fs::read(path)?;
    let options = DecoderOptions::new_fast()
        .set_strict_mode(false)
        .set_max_width(usize::MAX)
        .set_max_height(usize::MAX)
        .jpeg_set_out_colorspace(ColorSpace::RGB);
    let mut decoder =
        zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(encoded.as_slice()), options);
    decoder
        .decode_headers()
        .map_err(|e| anyhow::anyhow!("jpeg header decode failed: {}", e))?;
    let (source_width, source_height) = decoder
        .dimensions()
        .ok_or_else(|| anyhow::anyhow!("jpeg dimensions missing after header decode"))?;
    let source_width =
        u32::try_from(source_width).map_err(|_| anyhow::anyhow!("jpeg width is too large"))?;
    let source_height =
        u32::try_from(source_height).map_err(|_| anyhow::anyhow!("jpeg height is too large"))?;
    log_decode_time_downscale_status(path, source_width, source_height);
    let decoded = decoder
        .decode()
        .map_err(|e| anyhow::anyhow!("jpeg decode failed: {}", e))?;
    Ok(DecodedSourceImage {
        pixels: DecodedSourcePixels::Rgb(decoded.into()),
        width: source_width,
        height: source_height,
        format: "jpeg-fast".to_string(),
        decode: decode_start.elapsed(),
        convert: Duration::ZERO,
    })
}

fn log_decode_time_downscale_status(path: &Path, source_width: u32, source_height: u32) {
    let max_dimension = crate::image::prepare::MAX_IMAGE_UPLOAD_DIMENSION;
    if source_width <= max_dimension && source_height <= max_dimension {
        return;
    }

    debug!(
        "[IMAGE-CACHE] decode_downscale_unavailable path={} source={}x{} max_upload_dimension={} decoder=zune-jpeg reason=no_scaled_decode_api_in_current_dependency",
        path.display(),
        source_width,
        source_height,
        max_dimension
    );
}

fn decode_png_source_fast(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    let decode_start = Instant::now();
    let encoded = std::fs::read(path)?;
    let options = DecoderOptions::default()
        .set_strict_mode(false)
        .set_max_width(usize::MAX)
        .set_max_height(usize::MAX)
        .png_set_strip_to_8bit(true);
    let mut decoder =
        zune_png::PngDecoder::new_with_options(ZCursor::new(encoded.as_slice()), options);
    decoder
        .decode_headers()
        .map_err(|e| anyhow::anyhow!("png header decode failed: {}", e))?;
    let (source_width, source_height) = decoder
        .dimensions()
        .ok_or_else(|| anyhow::anyhow!("png dimensions missing after header decode"))?;
    let source_width =
        u32::try_from(source_width).map_err(|_| anyhow::anyhow!("png width is too large"))?;
    let source_height =
        u32::try_from(source_height).map_err(|_| anyhow::anyhow!("png height is too large"))?;
    let colorspace = decoder
        .colorspace()
        .ok_or_else(|| anyhow::anyhow!("png colorspace missing after header decode"))?;
    let decoded = decoder
        .decode_raw()
        .map_err(|e| anyhow::anyhow!("png decode failed: {}", e))?;

    let pixels = match colorspace {
        ColorSpace::RGB => DecodedSourcePixels::Rgb(decoded.into()),
        ColorSpace::RGBA => DecodedSourcePixels::Rgba(decoded.into()),
        ColorSpace::Luma => DecodedSourcePixels::Luma(decoded.into()),
        ColorSpace::LumaA => DecodedSourcePixels::LumaA(decoded.into()),
        other => {
            return Err(anyhow::anyhow!(
                "unsupported fast png colorspace {:?} for {}",
                other,
                path.display()
            ));
        }
    };

    Ok(DecodedSourceImage {
        pixels,
        width: source_width,
        height: source_height,
        format: "png-fast".to_string(),
        decode: decode_start.elapsed(),
        convert: Duration::ZERO,
    })
}

fn decode_source_generic(
    path: &Path,
    format: Option<image::ImageFormat>,
) -> anyhow::Result<DecodedSourceImage> {
    let decode_start = Instant::now();
    let image = image::open(path)?;
    let decode_duration = decode_start.elapsed();
    let source_width = image.width();
    let source_height = image.height();

    if image.has_alpha() {
        let convert_start = Instant::now();
        let rgba = image.into_rgba8().into_raw();
        return Ok(DecodedSourceImage {
            pixels: DecodedSourcePixels::Rgba(rgba.into()),
            width: source_width,
            height: source_height,
            format: image_format_label(format, false),
            decode: decode_duration,
            convert: convert_start.elapsed(),
        });
    }

    let convert_start = Instant::now();
    let rgb = image.into_rgb8().into_raw();
    Ok(DecodedSourceImage {
        pixels: DecodedSourcePixels::Rgb(rgb.into()),
        width: source_width,
        height: source_height,
        format: image_format_label(format, false),
        decode: decode_duration,
        convert: convert_start.elapsed(),
    })
}

pub(crate) fn decode_source_image(path: &Path) -> anyhow::Result<DecodedSourceImage> {
    let format = image::ImageFormat::from_path(path).ok();
    match format {
        Some(image::ImageFormat::Jpeg) => match decode_jpeg_source_fast(path) {
            Ok(source) => Ok(source),
            Err(e) => {
                warn!(
                    "[ASSET] {}: Fast JPEG decode failed, falling back to generic image path: {}",
                    path.display(),
                    e
                );
                decode_source_generic(path, format)
            }
        },
        Some(image::ImageFormat::Png) => match decode_png_source_fast(path) {
            Ok(source) => Ok(source),
            Err(e) => {
                warn!(
                    "[ASSET] {}: Fast PNG decode failed, falling back to generic image path: {}",
                    path.display(),
                    e
                );
                decode_source_generic(path, format)
            }
        },
        _ => decode_source_generic(path, format),
    }
}

pub(crate) fn prepare_source_image_for_output(
    source: &DecodedSourceImage,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    let (data, width, height, resize_duration, expand_duration, resize_filter) =
        match &source.pixels {
            DecodedSourcePixels::Rgb(pixels) => crate::image::prepare::prepare_rgb_image(
                pixels,
                source.width,
                source.height,
                target_width,
                target_height,
            )?,
            DecodedSourcePixels::Rgba(pixels) => {
                let (prepared, width, height, resize_duration, resize_filter) =
                    crate::image::prepare::prepare_rgba_image(
                        pixels,
                        source.width,
                        source.height,
                        target_width,
                        target_height,
                    )?;
                (
                    prepared,
                    width,
                    height,
                    resize_duration,
                    Duration::ZERO,
                    resize_filter,
                )
            }
            DecodedSourcePixels::Luma(pixels) => crate::image::prepare::prepare_luma_image(
                pixels,
                source.width,
                source.height,
                target_width,
                target_height,
            )?,
            DecodedSourcePixels::LumaA(pixels) => crate::image::prepare::prepare_lumaa_image(
                pixels,
                source.width,
                source.height,
                target_width,
                target_height,
            )?,
        };

    Ok(DecodedImagePayload {
        data: data.into(),
        width,
        height,
        profile: ImageLoadProfile {
            format: source.format.clone(),
            source_width: source.width,
            source_height: source.height,
            permit_wait: Duration::ZERO,
            decode: source.decode,
            convert: source.convert,
            resize: resize_duration,
            expand: expand_duration,
            resize_filter,
        },
    })
}

pub(crate) fn prepare_image_for_output_uncached(
    path: &Path,
    target_width: u32,
    target_height: u32,
) -> anyhow::Result<DecodedImagePayload> {
    let source = decode_source_image(path)?;
    prepare_source_image_for_output(&source, target_width, target_height)
}
