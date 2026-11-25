//! Captures desktop output spectrum only while a visible wallpaper uses it.

use crate::main_loop::{CommandSender, MainLoopContext};
use anyhow::{Context, Result};
use gst::prelude::*;
use gstreamer as gst;
use kaleidux_common::{LayerInputs, Request, Response};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

const BANDS: usize = 64;

#[derive(Default)]
pub(crate) struct DesktopAudio {
    source: Option<String>,
    stop: Option<Arc<AtomicBool>>,
}

impl DesktopAudio {
    fn update(&mut self, source: Option<&str>, commands: &CommandSender) {
        if self.source.as_deref() == source {
            return;
        }
        self.stop();
        let Some(source) = source else { return };
        self.source = Some(source.to_owned());
        let stop = Arc::new(AtomicBool::new(false));
        self.stop = Some(stop.clone());
        let source = source.to_owned();
        let commands = commands.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("kld-spectrum".into())
            .spawn(move || {
                if let Err(error) = capture(&source, &commands, &stop)
                    && !stop.load(Ordering::Relaxed)
                {
                    tracing::warn!("Desktop audio capture stopped: {error:#}");
                }
            })
        {
            tracing::warn!("Could not start desktop audio capture: {error}");
        }
    }

    pub(crate) fn stop(&mut self) {
        self.source = None;
        if let Some(stop) = self.stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
    }
}

impl Drop for DesktopAudio {
    fn drop(&mut self) {
        self.stop();
    }
}

impl MainLoopContext {
    pub(crate) fn sync_desktop_audio(&mut self) {
        let source = self.monitor_manager.desktop_audio_source().filter(|_| {
            !self.display_power_suspended
                && !self.monitor_manager.is_paused()
                && self.renderers.iter().any(|(name, renderer)| {
                    !self.powered_off_outputs.contains(name)
                        && (renderer.uses_desktop_audio()
                            || self
                                .video_players
                                .get(name)
                                .is_some_and(|player| player.is_web_wallpaper()))
                })
        });
        self.desktop_audio.update(source, &self.rule_command_sender);
    }
}

fn capture(source: &str, commands: &CommandSender, stop: &AtomicBool) -> Result<()> {
    gst::init().context("Initialize GStreamer for desktop audio")?;
    let pipeline = gst::parse::launch(
        "pulsesrc name=capture client-name=Kaleidux buffer-time=80000 latency-time=25000 \
         ! audioconvert ! audioresample ! audio/x-raw,rate=22050,channels=1 \
         ! spectrum bands=64 interval=50000000 threshold=-70 ! fakesink sync=false",
    )?
    .downcast::<gst::Pipeline>()
    .map_err(|_| anyhow::anyhow!("Invalid desktop audio pipeline"))?;
    pipeline
        .by_name("capture")
        .context("Missing desktop audio source")?
        .set_property("device", source);
    let bus = pipeline.bus().context("Missing desktop audio bus")?;
    let playback = pipeline.set_state(gst::State::Playing);
    let result = match playback {
        Ok(_) => collect_spectrum(&bus, commands, stop),
        Err(error) => Err(error.into()),
    };
    let _ = pipeline.set_state(gst::State::Null);
    result
}

fn collect_spectrum(bus: &gst::Bus, commands: &CommandSender, stop: &AtomicBool) -> Result<()> {
    let mut last = None;
    let mut reply: Option<oneshot::Receiver<Response>> = None;
    while !stop.load(Ordering::Relaxed) {
        let Some(message) = bus.timed_pop(gst::ClockTime::from_mseconds(200)) else {
            continue;
        };
        if let gst::MessageView::Error(error) = message.view() {
            return Err(error.error().into());
        }
        let Some(structure) = message
            .structure()
            .filter(|value| value.name() == "spectrum")
        else {
            continue;
        };
        if reply.as_mut().is_some_and(|reply| {
            matches!(reply.try_recv(), Err(oneshot::error::TryRecvError::Empty))
        }) {
            continue;
        }
        let list = structure.get::<gst::List>("magnitude")?;
        let mut values = [0.0; BANDS];
        for (band, db) in values.iter_mut().zip(list.as_slice()) {
            *band = ((db.get::<f32>().unwrap_or(-70.0) + 65.0) / 65.0).clamp(0.0, 1.0);
        }
        if last.is_some_and(|last: [f32; BANDS]| {
            last.iter().zip(values).all(|(a, b)| (a - b).abs() < 0.015)
        }) {
            continue;
        }
        let request = Request::LayerInputs(LayerInputs {
            output: Some("all".into()),
            audio: Some(values.to_vec()),
            ..Default::default()
        });
        let (sender, receiver) = oneshot::channel();
        match commands.try_send((request.into(), sender)) {
            Ok(()) => {
                last = Some(values);
                reply = Some(receiver);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
        }
    }
    Ok(())
}
