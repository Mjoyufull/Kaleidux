//! Applies source-time loop bounds through each backend's native playback controls.

use super::VideoPlayer;
use crate::media_source::PlaybackWindow;
use gst::prelude::*;
use gstreamer as gst;

pub(super) fn seek_segment(pipeline: &gst::Element, window: PlaybackWindow) -> anyhow::Result<()> {
    seek_segment_with_flush(pipeline, window, true)
}

pub(super) fn seek_segment_with_flush(
    pipeline: &gst::Element,
    window: PlaybackWindow,
    flush: bool,
) -> anyhow::Result<()> {
    let mut flags = gst::SeekFlags::SEGMENT | gst::SeekFlags::ACCURATE;
    if flush {
        flags |= gst::SeekFlags::FLUSH;
    }
    pipeline.seek(
        1.0,
        flags,
        gst::SeekType::Set,
        gst::ClockTime::from_nseconds(window.start_ns),
        gst::SeekType::Set,
        gst::ClockTime::from_nseconds(window.end_ns),
    )?;
    Ok(())
}

impl VideoPlayer {
    pub fn playback_window(&self) -> Option<PlaybackWindow> {
        self.playback_window
    }
    pub fn duration_ns(&self) -> Option<u64> {
        if let Some(native) = self.native.as_ref() {
            return native.duration_ns();
        }
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.duration_ns();
        }
        self.pipeline
            .as_ref()?
            .query_duration::<gst::ClockTime>()
            .map(|duration| duration.nseconds())
    }

    pub fn set_playback_window(&mut self, window: PlaybackWindow) -> anyhow::Result<()> {
        anyhow::ensure!(window.end_ns > window.start_ns, "Invalid playback window");
        if let Some(native) = self.native.as_ref() {
            native.set_playback_window(window)?;
        } else if let Some(mpv) = self.mpv.as_ref() {
            mpv.set_playback_window(window)?;
        } else if let Some(pipeline) = self.pipeline.as_ref() {
            seek_segment(pipeline, window)?;
        }
        self.playback_window = Some(window);
        if self.pipeline.is_some() && self.is_running.load(std::sync::atomic::Ordering::SeqCst) {
            // The bus handler owns the loop interval. Preparation can discover
            // duration after the initial watch was installed; refresh its
            // captured bounds so SegmentDone cannot restart the whole file.
            self.install_bus_watch()?;
        }
        Ok(())
    }
}
