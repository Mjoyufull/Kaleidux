//! Shares selected-adapter NV12 modifiers and DRM identity with appsink negotiation.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

static APPSINK_IMPORT_FAILED: AtomicBool = AtomicBool::new(false);

pub(crate) fn report_appsink_import_failure() -> bool {
    !APPSINK_IMPORT_FAILED.swap(true, Ordering::AcqRel)
}

pub(super) fn appsink_import_failed() -> bool {
    APPSINK_IMPORT_FAILED.load(Ordering::Acquire)
}

#[derive(Default)]
struct ImportCaps {
    resolved: bool,
    primary: Option<PathBuf>,
    nv12_formats: Vec<String>,
}

static CAPS: std::sync::LazyLock<parking_lot::RwLock<ImportCaps>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new(ImportCaps::default()));

pub(crate) fn set(primary: Option<PathBuf>, nv12_formats: Vec<String>) {
    *CAPS.write() = ImportCaps {
        resolved: true,
        primary,
        nv12_formats,
    };
}

pub(super) fn primary_node() -> Option<PathBuf> {
    CAPS.read().primary.clone()
}

pub(super) fn nv12_formats() -> Vec<String> {
    if appsink_import_failed() {
        return Vec::new();
    }
    let caps = CAPS.read();
    if !caps.resolved {
        vec!["NV12".into()]
    } else {
        caps.nv12_formats.clone()
    }
}

pub(super) fn can_import_dmabuf() -> bool {
    if appsink_import_failed() {
        return false;
    }
    let caps = CAPS.read();
    !caps.resolved || !caps.nv12_formats.is_empty()
}

#[cfg(test)]
#[path = "import_caps_tests.rs"]
mod tests;
