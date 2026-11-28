//! Classifies user-selected media before filesystem admission or decoder creation.
//! Queue locators preserve remote URLs; only local sources enter filesystem probes.

use std::path::{Path, PathBuf};

/// A half-open playback interval in the source timeline, shared by all decoders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlaybackWindow {
    pub start_ns: u64,
    pub end_ns: u64,
}

#[cfg(feature = "network-sources")]
pub mod network;

pub(crate) fn resolve_for_playback(
    input: &str,
    should_abort: impl Fn() -> bool,
) -> anyhow::Result<String> {
    #[cfg(not(feature = "network-sources"))]
    let _ = &should_abort;
    match MediaSource::parse(input)? {
        MediaSource::Network(uri) => {
            #[cfg(feature = "network-sources")]
            return network::resolve(&uri, should_abort);
            #[cfg(not(feature = "network-sources"))]
            anyhow::bail!("Network sources are disabled: {}", uri);
        }
        MediaSource::File(path) => Ok(path.to_string_lossy().into_owned()),
        MediaSource::Colour(_) => anyhow::bail!("A solid colour is not a video source"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaSource {
    File(PathBuf),
    Network(String),
    Colour([u8; 4]),
}

impl MediaSource {
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        if let Some(colour) = parse_colour(input) {
            return Ok(Self::Colour(colour));
        }
        anyhow::ensure!(
            !input.starts_with('#')
                && !input.starts_with("colour:")
                && !input.starts_with("color:"),
            "A colour must use #RRGGBB or #RRGGBBAA"
        );
        let normalized = if [
            "youtube.com/",
            "www.youtube.com/",
            "m.youtube.com/",
            "youtu.be/",
        ]
        .iter()
        .any(|prefix| input.starts_with(prefix))
        {
            format!("https://{input}")
        } else {
            input.to_owned()
        };
        if normalized.contains("://") {
            anyhow::ensure!(
                cfg!(feature = "network-sources"),
                "Network sources are disabled in this build (enable network-sources)"
            );
            let (scheme, address) = normalized.split_once("://").expect("scheme delimiter");
            anyhow::ensure!(
                matches!(scheme, "http" | "https" | "rtsp" | "rtsps"),
                "Unsupported media URL scheme"
            );
            anyhow::ensure!(
                !address.is_empty()
                    && !normalized
                        .chars()
                        .any(|ch| ch.is_control() || ch.is_whitespace()),
                "Invalid media URL"
            );
            return Ok(Self::Network(normalized));
        }
        Ok(Self::File(PathBuf::from(input)))
    }

    pub fn is_virtual(path: &Path) -> bool {
        matches!(
            Self::parse(&path.to_string_lossy()),
            Ok(Self::Network(_) | Self::Colour(_))
        )
    }

    pub fn locator(&self) -> PathBuf {
        match self {
            Self::File(path) => path.clone(),
            Self::Network(uri) => PathBuf::from(uri),
            Self::Colour(rgba) => PathBuf::from(format!(
                "colour:#{:02x}{:02x}{:02x}{:02x}",
                rgba[0], rgba[1], rgba[2], rgba[3]
            )),
        }
    }
}

pub fn parse_colour(input: &str) -> Option<[u8; 4]> {
    let value = input
        .strip_prefix("colour:")
        .or_else(|| input.strip_prefix("color:"))
        .unwrap_or(input);
    let digits = value.strip_prefix('#')?;
    if !matches!(digits.len(), 6 | 8) || !digits.is_ascii() {
        return None;
    }
    let mut result = [255; 4];
    for (index, channel) in result.iter_mut().enumerate().take(digits.len() / 2) {
        *channel = u8::from_str_radix(&digits[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    #[test]
    fn colours_are_validated_and_have_a_canonical_locator() {
        let source = super::MediaSource::parse("#1a1b26").unwrap();
        assert_eq!(source, super::MediaSource::Colour([26, 27, 38, 255]));
        assert_eq!(
            super::MediaSource::parse(source.locator().to_str().unwrap()).unwrap(),
            source
        );
        assert_eq!(super::parse_colour("#zz0022"), None);
    }

    #[cfg(feature = "network-sources")]
    #[test]
    fn network_sources_accept_youtube_and_reject_unsupported_protocols() {
        assert!(matches!(
            super::MediaSource::parse("youtu.be/example").unwrap(),
            super::MediaSource::Network(_)
        ));
        assert!(super::MediaSource::parse("ftp://example/video.mp4").is_err());
        assert!(super::MediaSource::parse("https://example/\nsecret").is_err());
    }
}
