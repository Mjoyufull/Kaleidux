use gst::prelude::*;
use gstreamer as gst;
use tracing::{debug, info};

use super::is_nvcodec_decoder_factory;

pub(super) fn build_publish_rate_filter(
    source_id: &str,
    max_publish_fps: Option<u32>,
) -> Option<gst::Element> {
    let max_rate = max_publish_fps.filter(|fps| *fps > 0)?;
    if !publish_rate_filter_enabled() {
        info!(
            "[VIDEO-BACKEND] {} publish_rate_filter disabled max_rate={}",
            source_id, max_rate
        );
        return None;
    }

    let filter = gst::ElementFactory::make("videorate")
        .name("kld-publish-rate-filter")
        .build()
        .ok()?;
    set_if_present(&filter, "drop-only", true);
    set_if_present(&filter, "silent", true);
    set_if_present(&filter, "skip-to-first", true);
    set_if_present(&filter, "max-rate", max_rate.min(i32::MAX as u32) as i32);
    info!(
        "[VIDEO-BACKEND] {} publish_rate_filter element=videorate max_rate={} drop_only=true",
        source_id, max_rate
    );
    Some(filter)
}

fn publish_rate_filter_enabled() -> bool {
    std::env::var("KLD_VIDEO_RATE_FILTER")
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true)
}

pub(super) fn configure_pipeline_element(
    source_id: &str,
    audio_enabled: bool,
    element: &gst::Element,
) {
    let Some(factory_name) = element.factory().map(|factory| factory.name().to_string()) else {
        return;
    };

    info!(
        "[VIDEO-BACKEND] {} element_setup name={} factory={} klass={}",
        source_id,
        element.name(),
        factory_name,
        element
            .factory()
            .map(|factory| factory.metadata("klass").unwrap_or_default().to_string())
            .unwrap_or_default()
    );

    if !audio_enabled {
        configure_video_only_decodebin(source_id, factory_name.as_str(), element);
    }

    if is_nvcodec_decoder_factory(factory_name.as_str()) {
        debug!(
            "[VIDEO] {}: Keeping decoder {} on default scheduling/presentation settings",
            source_id, factory_name
        );
    }
}

fn configure_video_only_decodebin(source_id: &str, factory_name: &str, element: &gst::Element) {
    if !matches!(factory_name, "decodebin" | "uridecodebin") {
        return;
    }

    set_if_present(element, "expose-all-streams", false);
    if element.find_property("caps").is_some()
        && let Ok(caps) = "video/x-raw(ANY)".parse::<gst::Caps>()
    {
        element.set_property("caps", &caps);
    }

    info!(
        "[VIDEO-BACKEND] {} video_only_decodebin element={} factory={} caps=video/x-raw(ANY)",
        source_id,
        element.name(),
        factory_name
    );
}

fn set_if_present<T>(element: &gst::Element, property_name: &str, value: T)
where
    T: Send + Sync + 'static + gst::glib::value::ToValue,
{
    if element.find_property(property_name).is_some() {
        element.set_property(property_name, &value);
    }
}
