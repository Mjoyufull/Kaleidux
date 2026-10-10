use super::control::NativePlaybackControl;
use ffmpeg_next as ffmpeg;
use ffmpeg_next::packet::Mut as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[path = "audio/output.rs"]
mod output;

pub(super) struct AudioWorker {
    stopped: Arc<AtomicBool>,
    control: Arc<NativePlaybackControl>,
    worker: Option<JoinHandle<()>>,
}

impl AudioWorker {
    pub(super) fn spawn(uri: &str, control: Arc<NativePlaybackControl>) -> Self {
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_stop = stopped.clone();
        let worker_control = control.clone();
        let source = uri.to_owned();
        let worker = std::thread::Builder::new()
            .name("kld-native-audio".into())
            .spawn(move || {
                if worker_control.wait_for_audio(&worker_stop)
                    && let Err(error) = run(&source, &worker_control, &worker_stop)
                    && !worker_control.is_stopped()
                    && !worker_stop.load(Ordering::Acquire)
                {
                    warn!(
                        "[NATIVE-AUDIO] {source}: audio unavailable ({error:#}); video continues"
                    );
                }
            })
            .map_err(|error| warn!("[NATIVE-AUDIO] cannot spawn output worker: {error}"))
            .ok();
        Self {
            stopped,
            control,
            worker,
        }
    }
}

impl Drop for AudioWorker {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.control.update_audio_clock(None);
        self.control.notify_audio();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct AudioSession {
    input: ffmpeg::format::context::Input,
    decoder: ffmpeg::decoder::Audio,
    resampler: Option<ffmpeg::software::resampling::Context>,
    output: output::AudioOutput,
    stream_index: usize,
    time_base: ffmpeg::Rational,
    epoch: u64,
    seek_ns: u64,
    cursor_ns: u64,
    wall_anchor: Instant,
    eof: bool,
}

fn run(
    uri: &str,
    control: &Arc<NativePlaybackControl>,
    stopped: &Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let interrupt_control = control.clone();
    let interrupt_stop = stopped.clone();
    let mut input = ffmpeg::format::input_with_interrupt(uri, move || {
        interrupt_control.is_stopped() || interrupt_stop.load(Ordering::Acquire)
    })?;
    let Some(stream) = input.streams().best(ffmpeg::media::Type::Audio) else {
        return Ok(());
    };
    let stream_index = stream.index();
    let time_base = stream.time_base();
    let decoder = ffmpeg::codec::context::Context::from_parameters(stream.parameters())?
        .decoder()
        .audio()?;
    super::decode::discard_unselected_streams(&mut input, stream_index);
    let mut session = AudioSession {
        input,
        decoder,
        resampler: None,
        output: output::AudioOutput::new()?,
        stream_index,
        time_base,
        epoch: u64::MAX,
        seek_ns: 0,
        cursor_ns: 0,
        wall_anchor: Instant::now(),
        eof: false,
    };
    info!(
        "[NATIVE-AUDIO] {uri}: FFmpeg PCM decode; bounded 100ms audio output, one stream per group"
    );
    let mut packet = ffmpeg::Packet::empty();
    loop {
        if !control.is_playing() || control.volume() == 0.0 {
            session.output.pause()?;
            control.update_audio_clock(None);
            session.epoch = u64::MAX;
        }
        if !control.wait_for_audio(stopped) {
            return Ok(());
        }
        if session.epoch != control.play_epoch() {
            session.restart(control)?;
        }
        if session.receive(control, stopped)? {
            continue;
        }
        if session.eof {
            // Video owns the loop boundary. Short audio must not loop by itself.
            if !control.wait_until(Instant::now() + Duration::from_millis(50)) {
                return Ok(());
            }
            continue;
        }
        // SAFETY: packet is exclusively owned and read_frame replaces its payload.
        unsafe {
            ffmpeg::ffi::av_packet_unref(packet.as_mut_ptr());
        }
        match packet.read(&mut session.input) {
            Ok(()) if packet.stream() == session.stream_index => {
                session.decoder.send_packet(&packet)?
            }
            Ok(()) => {}
            Err(ffmpeg::Error::Eof) => {
                session.decoder.send_eof()?;
                session.eof = true;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

impl AudioSession {
    fn restart(&mut self, control: &NativePlaybackControl) -> anyhow::Result<()> {
        self.seek_ns = control.position_ns();
        self.input
            .seek((self.seek_ns / 1000).min(i64::MAX as u64) as i64, ..)?;
        self.decoder.flush();
        self.resampler = None;
        self.cursor_ns = 0;
        self.wall_anchor = Instant::now();
        self.eof = false;
        self.epoch = control.play_epoch();
        tracing::debug!(
            "[NATIVE-AUDIO] reset epoch={} position_ns={}",
            self.epoch,
            self.seek_ns
        );
        self.output.restart()
    }

    fn receive(
        &mut self,
        control: &NativePlaybackControl,
        stopped: &AtomicBool,
    ) -> anyhow::Result<bool> {
        let mut decoded = ffmpeg::frame::Audio::empty();
        match self.decoder.receive_frame(&mut decoded) {
            Ok(()) => {}
            Err(ffmpeg::Error::Eof) => {
                let Some(resampler) = self.resampler.as_mut() else {
                    return Ok(false);
                };
                let mut tail = ffmpeg::frame::Audio::new(
                    ffmpeg::format::Sample::I16(ffmpeg::format::sample::Type::Packed),
                    256,
                    ffmpeg::ChannelLayout::STEREO,
                );
                if resampler.flush(&mut tail)?.is_none() {
                    self.resampler = None;
                }
                let bytes = tail.data(0)[..tail.samples() * 4].to_vec();
                if bytes.is_empty() {
                    return Ok(false);
                }
                return self.push_pcm(bytes, self.cursor_ns, control, stopped);
            }
            Err(ffmpeg::Error::Other { errno }) if errno == ffmpeg::error::EAGAIN => {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        }
        let pts_ns = decoded
            .pts()
            .and_then(|pts| super::decode::timestamp_to_ns(pts, self.time_base))
            .unwrap_or(self.seek_ns + self.cursor_ns);
        let frame_end = pts_ns.saturating_add(
            decoded.samples() as u64 * 1_000_000_000 / u64::from(decoded.rate().max(1)),
        );
        if frame_end <= self.seek_ns {
            return Ok(true);
        }
        let mut converted = self.convert(&decoded)?;
        let skip_samples =
            self.seek_ns.saturating_sub(pts_ns).saturating_mul(48_000) / 1_000_000_000;
        let skip_bytes = (skip_samples as usize)
            .saturating_mul(4)
            .min(converted.len());
        converted.drain(..skip_bytes);
        if converted.is_empty() {
            return Ok(true);
        }
        let relative_pts = pts_ns.saturating_sub(self.seek_ns).max(self.cursor_ns);
        self.push_pcm(converted, relative_pts, control, stopped)
    }

    fn push_pcm(
        &mut self,
        converted: Vec<u8>,
        relative_pts: u64,
        control: &NativePlaybackControl,
        stopped: &AtomicBool,
    ) -> anyhow::Result<bool> {
        // Limit decode-ahead to 50ms. Appsrc is also bounded and never blocks a
        // worker on an unresponsive sound server. Control changes interrupt pacing.
        let due = self.wall_anchor + Duration::from_nanos(relative_pts.saturating_sub(50_000_000));
        while Instant::now() < due && !stopped.load(Ordering::Acquire) {
            if self.epoch != control.play_epoch()
                || !control.is_playing()
                || control.volume() == 0.0
            {
                return Ok(true);
            }
            if !control.wait_until(due.min(Instant::now() + Duration::from_millis(20))) {
                return Ok(true);
            }
        }
        if stopped.load(Ordering::Acquire) {
            return Ok(true);
        }
        if self.epoch != control.play_epoch() || !control.is_playing() || control.volume() == 0.0 {
            return Ok(true);
        }
        let duration = converted.len() as u64 / 4 * 1_000_000_000 / 48_000;
        self.output
            .push(converted, relative_pts, duration, control.volume())?;
        if self.cursor_ns == 0 {
            info!(
                "[NATIVE-AUDIO] playing epoch={} position_ns={} volume={:.2}",
                self.epoch,
                self.seek_ns.saturating_add(relative_pts),
                control.volume()
            );
        }
        control.update_audio_clock(
            self.output
                .position_ns()
                .map(|position| self.seek_ns.saturating_add(position)),
        );
        self.cursor_ns = relative_pts.saturating_add(duration);
        Ok(true)
    }

    fn convert(&mut self, frame: &ffmpeg::frame::Audio) -> anyhow::Result<Vec<u8>> {
        let layout = if frame.channel_layout().is_empty() {
            ffmpeg::ChannelLayout::default(i32::from(frame.channels()))
        } else {
            frame.channel_layout()
        };
        let input_changed = self.resampler.as_ref().is_none_or(|resampler| {
            resampler.input().format != frame.format()
                || resampler.input().rate != frame.rate()
                || resampler.input().channel_layout != layout
        });
        if input_changed {
            self.resampler = Some(ffmpeg::software::resampling::Context::get(
                frame.format(),
                layout,
                frame.rate(),
                ffmpeg::format::Sample::I16(ffmpeg::format::sample::Type::Packed),
                ffmpeg::ChannelLayout::STEREO,
                48_000,
            )?);
        }
        let capacity = (frame.samples() as u64 * 48_000).div_ceil(u64::from(frame.rate().max(1)))
            as usize
            + 256;
        let mut output = ffmpeg::frame::Audio::new(
            ffmpeg::format::Sample::I16(ffmpeg::format::sample::Type::Packed),
            capacity,
            ffmpeg::ChannelLayout::STEREO,
        );
        self.resampler
            .as_mut()
            .expect("resampler initialized above")
            .run(frame, &mut output)?;
        Ok(output.data(0)[..output.samples() * 4].to_vec())
    }
}
