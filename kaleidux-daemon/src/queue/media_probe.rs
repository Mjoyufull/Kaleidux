use super::ContentType;
use std::path::Path;

pub(super) fn probe(path: &Path) -> Option<ContentType> {
    use std::io::Read;
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return None,
    };
    let mut buffer = [0u8; 16];
    if file.read_exact(&mut buffer).is_err() {
        return None;
    }

    // JPEG: FF D8 FF
    if buffer[0..3] == [0xFF, 0xD8, 0xFF] {
        return Some(ContentType::Image);
    }
    // PNG: 89 50 4E 47
    if buffer[0..4] == [0x89, 0x50, 0x4E, 0x47] {
        return Some(ContentType::Image);
    }
    // GIF: GIF8
    if buffer[0..4] == *b"GIF8" {
        return Some(ContentType::Image);
    }
    // WebP: RIFF .... WEBP
    if &buffer[0..4] == b"RIFF" && &buffer[8..12] == b"WEBP" {
        return Some(ContentType::Image);
    }
    // EBML (MKV/WebM): 1A 45 DF A3
    if buffer[0..4] == [0x1A, 0x45, 0xDF, 0xA3] {
        return ContentType::Video.supported().then_some(ContentType::Video);
    }
    // ISO BMFF container: ....ftyp (shared by MP4/MOV video and AVIF/HEIF images)
    if &buffer[4..8] == b"ftyp" {
        // AVIF images use brands: avif, avis, mif1
        if &buffer[8..12] == b"avif" || &buffer[8..12] == b"avis" || &buffer[8..12] == b"mif1" {
            return Some(ContentType::Image);
        }
        return ContentType::Video.supported().then_some(ContentType::Video);
    }

    if let Ok(reader) = image::ImageReader::open(path)
        && let Ok(reader) = reader.with_guessed_format()
        && reader.into_dimensions().is_ok()
    {
        return Some(ContentType::Image);
    }
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    if matches!(
        extension.as_str(),
        "mp4"
            | "mkv"
            | "webm"
            | "mov"
            | "m4v"
            | "avi"
            | "ogv"
            | "mpeg"
            | "mpg"
            | "ts"
            | "mts"
            | "flv"
            | "wmv"
    ) {
        return ContentType::Video.supported().then_some(ContentType::Video);
    }
    None
}
