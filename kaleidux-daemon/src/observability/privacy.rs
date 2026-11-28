//! Redacts identifying log fields before either console or file formatting.
//! Raw identifiers are available only through an explicit startup opt-in.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::{DefaultFields, Writer};

static SENSITIVE: AtomicBool = AtomicBool::new(false);
/// Sets the explicit startup policy for logs emitted outside tracing.
pub fn set_sensitive(enabled: bool) {
    SENSITIVE.store(enabled, Ordering::Release);
}
/// Returns whether identifying diagnostics were explicitly enabled.
pub fn sensitive() -> bool {
    SENSITIVE.load(Ordering::Acquire)
}

static OUTPUTS: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PRIVATE_TEXT: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r#"(?i)"(?:\\.|[^"\\])*"|'[^'\n]*'|(?:https?|rtsps?|file)://[^\s<>]+|(?:^|[\s=:(<>\[])/[^\n<>]+|\b(?:eDP|DP|HDMI|DVI|LVDS|VGA|HEADLESS|XWAYLAND)-[A-Za-z0-9_-]+\b"#,
    )
    .expect("valid privacy pattern")
});

pub(crate) fn register_output(name: &str, description: &str) {
    if name.is_empty() {
        return;
    }
    let mut outputs = OUTPUTS.lock().unwrap_or_else(|error| error.into_inner());
    if outputs.len() >= 256 {
        return;
    }
    let alias = format!("output-{}", outputs.len() + 1);
    outputs
        .entry(name.to_owned())
        .or_insert_with(|| alias.clone());
    if !description.is_empty() {
        outputs.entry(description.to_owned()).or_insert(alias);
    }
}

/// Removes identifying text before console or file output.
pub fn redact(text: &str) -> String {
    let mut result = text.to_owned();
    let outputs = OUTPUTS.lock().unwrap_or_else(|error| error.into_inner());
    let mut identifiers: Vec<_> = outputs.iter().collect();
    identifiers.sort_unstable_by_key(|(name, _)| std::cmp::Reverse(name.len()));
    for (name, alias) in identifiers {
        result = result.replace(name, alias);
    }
    PRIVATE_TEXT.replace_all(&result, "[redacted]").into_owned()
}

/// Applies the same privacy policy to event and span fields on every log sink.
pub struct PrivateFields {
    sensitive: bool,
}

impl PrivateFields {
    /// Creates a formatter with an immutable startup privacy policy.
    pub fn new(sensitive: bool) -> Self {
        Self { sensitive }
    }
}

impl<'writer> FormatFields<'writer> for PrivateFields {
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        if self.sensitive {
            return DefaultFields::new().format_fields(writer, fields);
        }
        let mut formatted = String::new();
        DefaultFields::new().format_fields(Writer::new(&mut formatted), fields)?;
        writer.write_str(&redact(&formatted))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn private_messages_hide_paths_urls_outputs_and_quoted_metadata() {
        super::register_output("CUSTOM-PANEL", "Private monitor serial 12345");
        let text = super::redact(
            "CUSTOM-PANEL Private monitor serial 12345 file=\"/home/alice/secret image.png\" uri=https://user:password@host/private?token=secret codec=AV1 frame=3840x2160 DP-2",
        );
        for secret in [
            "CUSTOM-PANEL",
            "12345",
            "alice",
            "secret",
            "password",
            "DP-2",
        ] {
            assert!(!text.contains(secret), "{text}");
        }
        assert!(text.contains("codec=AV1 frame=3840x2160"));
        assert_eq!(
            super::redact("fps=24000/1001 bitrate=128000"),
            "fps=24000/1001 bitrate=128000"
        );
        assert!(!super::redact("media=/home/alice/private file.png").contains("private"));
    }
}
