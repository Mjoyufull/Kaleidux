//! Reads only the highest-resolution TEX mip, reusing image's BC decoder.

use super::package::read_u32;
use anyhow::{Context, Result};
use image::{DynamicImage, ImageReader, RgbaImage, codecs::dds::DdsDecoder};
use std::borrow::Cow;
use std::io::{Cursor, Read, Seek};

const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_EDGE: u32 = 4096;

pub(super) fn decode(bytes: &[u8]) -> Result<RgbaImage> {
    if !bytes.starts_with(b"TEXV") {
        return ordinary_image(bytes);
    }
    // These fields follow the TEXV/TEXI/TEXB layout documented in Anima.
    let mut file = Cursor::new(bytes);
    let version = tag(&mut file, b"TEXV")?;
    anyhow::ensure!(
        version == 5 && tag(&mut file, b"TEXI")? == 1,
        "Unsupported TEX header"
    );
    let format = read_u32(&mut file)?;
    file.seek_relative(12)?; // flags and backing texture dimensions
    let image_width = read_u32(&mut file)?;
    let image_height = read_u32(&mut file)?;
    file.seek_relative(4)?; // reserved
    let body = tag(&mut file, b"TEXB")?;
    anyhow::ensure!((1..=4).contains(&body), "Unsupported TEX body");
    anyhow::ensure!(read_u32(&mut file)? > 0, "TEX contains no images");
    if body >= 3 {
        file.seek_relative(4)?; // embedded image format
    }
    if body >= 4 {
        file.seek_relative(4)?; // video flag
    }
    anyhow::ensure!(read_u32(&mut file)? > 0, "TEX contains no mipmaps");
    let width = read_u32(&mut file)?;
    let height = read_u32(&mut file)?;
    anyhow::ensure!(
        (1..=MAX_EDGE).contains(&width) && (1..=MAX_EDGE).contains(&height),
        "TEX mipmap size exceeds the layer limit"
    );
    let uncompressed = if body >= 2 {
        let compressed = read_u32(&mut file)?;
        let length = read_u32(&mut file)? as usize;
        (compressed != 0).then_some(length)
    } else {
        None
    };
    let byte_count = read_u32(&mut file)? as usize;
    let start = file.position() as usize;
    let payload = bytes
        .get(start..start.saturating_add(byte_count))
        .context("Truncated TEX mipmap")?;
    let payload = match uncompressed {
        Some(length) => Cow::Owned(decompress(payload, length)?),
        None => Cow::Borrowed(payload),
    };
    let image = if image::guess_format(&payload).is_ok() {
        ordinary_image(&payload)?
    } else {
        match format {
            0 => RgbaImage::from_raw(width, height, payload.into_owned())
                .context("Invalid RGBA TEX size")?,
            4 => decode_bc(width, height, b"DXT5", &payload)?,
            6 => decode_bc(width, height, b"DXT3", &payload)?,
            7 => decode_bc(width, height, b"DXT1", &payload)?,
            _ => anyhow::bail!("Unsupported TEX pixel format {format}"),
        }
    };
    if image_width > 0
        && image_height > 0
        && image_width <= image.width()
        && image_height <= image.height()
        && (image_width != image.width() || image_height != image.height())
    {
        return Ok(image::imageops::crop_imm(&image, 0, 0, image_width, image_height).to_image());
    }
    Ok(image)
}

fn ordinary_image(bytes: &[u8]) -> Result<RgbaImage> {
    let mut reader = ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_EDGE);
    limits.max_image_height = Some(MAX_EDGE);
    limits.max_alloc = Some(MAX_BYTES as u64);
    reader.limits(limits);
    Ok(reader.decode()?.into_rgba8())
}

fn tag(reader: &mut impl Read, expected: &[u8; 4]) -> Result<u32> {
    let mut tag = [0; 9];
    reader.read_exact(&mut tag)?;
    anyhow::ensure!(
        &tag[..4] == expected && tag[8] == 0,
        "Invalid TEX block tag"
    );
    Ok(std::str::from_utf8(&tag[4..8])?.parse()?)
}

fn decode_bc(width: u32, height: u32, encoding: &[u8; 4], payload: &[u8]) -> Result<RgbaImage> {
    let padded_width = width.next_multiple_of(4);
    let padded_height = height.next_multiple_of(4);
    let block_size = if encoding == b"DXT1" { 8 } else { 16 };
    let size = (padded_width / 4 * (padded_height / 4) * block_size) as usize;
    anyhow::ensure!(payload.len() == size, "Invalid TEX block compression size");
    let mut header = [0; 128];
    header[..4].copy_from_slice(b"DDS ");
    for (offset, value) in [
        (4, 124_u32),
        (8, 0x81007),
        (12, padded_height),
        (16, padded_width),
        (20, size as u32),
        (76, 32),
        (80, 4),
        (108, 0x1000),
    ] {
        header[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }
    header[84..88].copy_from_slice(encoding);
    let decoder = DdsDecoder::new(header.as_slice().chain(payload))?;
    let image = DynamicImage::from_decoder(decoder)?.into_rgba8();
    Ok(image)
}

fn decompress(input: &[u8], len: usize) -> Result<Vec<u8>> {
    anyhow::ensure!((1..=MAX_BYTES).contains(&len), "TEX mipmap exceeds 64 MiB");
    let src_size = i32::try_from(input.len())?;
    let mut output = vec![0u8; len];
    // SAFETY: This opens the system LZ4 SONAME and keeps it loaded for the call.
    let library = unsafe { libloading::Library::new("liblz4.so.1") }
        .context("Compressed TEX textures require liblz4.so.1")?;
    type Decode = unsafe extern "C" fn(*const u8, *mut u8, i32, i32) -> i32;
    // SAFETY: The symbol takes pointers to buffers and their exact byte lengths.
    let expand: libloading::Symbol<'_, Decode> = unsafe { library.get(b"LZ4_decompress_safe") }?;
    // SAFETY: Input and output are disjoint live slices for the entire call.
    let written = unsafe { expand(input.as_ptr(), output.as_mut_ptr(), src_size, len as i32) };
    anyhow::ensure!(written == len as i32, "Invalid compressed TEX mipmap");
    Ok(output)
}
