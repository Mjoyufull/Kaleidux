use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

pub(super) struct AudioOutput {
    pipeline: gst::Pipeline,
    source: gst_app::AppSrc,
    volume: gst::Element,
}

impl AudioOutput {
    pub(super) fn new() -> anyhow::Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();
        let source = gst_app::AppSrc::builder()
            .format(gst::Format::Time)
            .is_live(true)
            .block(false)
            .caps(
                &gst::Caps::builder("audio/x-raw")
                    .field("format", "S16LE")
                    .field("layout", "interleaved")
                    .field("rate", 48_000i32)
                    .field("channels", 2i32)
                    .build(),
            )
            .build();
        source.set_property("max-bytes", 19_200u64);
        source.set_property("max-buffers", 8u64);
        source.set_property("max-time", 100_000_000u64);
        source.set_property("leaky-type", gst_app::AppLeakyType::Downstream);
        let convert = gst::ElementFactory::make("audioconvert").build()?;
        let resample = gst::ElementFactory::make("audioresample").build()?;
        let volume = gst::ElementFactory::make("volume").build()?;
        let sink_name =
            std::env::var("KLD_NATIVE_AUDIO_SINK").unwrap_or_else(|_| "autoaudiosink".to_string());
        let sink = gst::ElementFactory::make(&sink_name).build()?;
        if sink.find_property("sync").is_some() {
            sink.set_property("sync", true);
        }
        pipeline.add_many([source.upcast_ref(), &convert, &resample, &volume, &sink])?;
        gst::Element::link_many([source.upcast_ref(), &convert, &resample, &volume, &sink])?;
        Ok(Self {
            pipeline,
            source,
            volume,
        })
    }

    pub(super) fn restart(&self) -> anyhow::Result<()> {
        // READY flushes queued PCM and resets the running-time origin on seek.
        self.pipeline.set_state(gst::State::Ready)?;
        self.pipeline.set_state(gst::State::Playing)?;
        Ok(())
    }

    pub(super) fn pause(&self) -> anyhow::Result<()> {
        self.pipeline.set_state(gst::State::Paused)?;
        Ok(())
    }

    pub(super) fn position_ns(&self) -> Option<u64> {
        self.pipeline
            .query_position::<gst::ClockTime>()
            .map(|time| time.nseconds())
    }

    pub(super) fn push(
        &self,
        bytes: Vec<u8>,
        pts: u64,
        duration: u64,
        volume: f64,
    ) -> anyhow::Result<()> {
        self.volume.set_property("volume", volume);
        let mut buffer = gst::Buffer::from_mut_slice(bytes);
        let writable = buffer.get_mut().expect("new PCM buffer is uniquely owned");
        writable.set_pts(gst::ClockTime::from_nseconds(pts));
        writable.set_duration(gst::ClockTime::from_nseconds(duration));
        self.source.push_buffer(buffer)?;
        if let Some(message) = self
            .pipeline
            .bus()
            .and_then(|bus| bus.pop_filtered(&[gst::MessageType::Error]))
            && let gst::MessageView::Error(error) = message.view()
        {
            anyhow::bail!("audio output failed: {}", error.error());
        }
        Ok(())
    }
}

impl Drop for AudioOutput {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}
