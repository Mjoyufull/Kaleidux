use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app as gst_app;
use std::sync::atomic::Ordering;
use tracing::{debug, info};

use super::appsink::{maybe_log_decode_path, sample_to_video_frame};
use super::bus::attach_bus_watch;
use super::{PlayerEvent, PlayerEventKind, VideoFrame, VideoPlayer};

#[derive(Debug, Clone, Copy)]
pub struct VideoPrebufferProfile {
    pub set_state: std::time::Duration,
    pub state_wait: std::time::Duration,
    pub pull_preroll: std::time::Duration,
    pub set_state_result: &'static str,
    pub state_wait_settled: bool,
    pub current_state: gst::State,
    pub pending_state: gst::State,
}

pub struct VideoPrebufferResult {
    pub frame: Option<VideoFrame>,
    pub profile: VideoPrebufferProfile,
}

fn restart_pipeline_after_eos(
    pipeline: &gst::Element,
    _source_id: &str,
    _backend_kind: super::VideoBackendKind,
) -> Result<(), String> {
    if pipeline
        .seek_simple(
            gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT,
            gst::ClockTime::ZERO,
        )
        .is_ok()
    {
        return Ok(());
    }

    Err("failed to seek to start after eos".to_string())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AppsinkQueueLevels {
    pub buffers: u64,
    pub bytes: u64,
    pub time_ns: u64,
}

impl VideoPlayer {
    pub fn prebuffer<F>(&mut self, should_abort: F) -> anyhow::Result<VideoPrebufferResult>
    where
        F: Fn() -> bool,
    {
        debug!("[VIDEO] {}: Pre-buffering video pipeline", self.source_id);
        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_mut() {
            let pull_preroll_start = std::time::Instant::now();
            let frame = mpv.prebuffer(should_abort)?;
            let pull_preroll = pull_preroll_start.elapsed();
            return Ok(VideoPrebufferResult {
                frame,
                profile: VideoPrebufferProfile {
                    set_state: std::time::Duration::ZERO,
                    state_wait: std::time::Duration::ZERO,
                    pull_preroll,
                    set_state_result: "mpv-paused",
                    state_wait_settled: true,
                    current_state: gst::State::Paused,
                    pending_state: gst::State::VoidPending,
                },
            });
        }
        let pipeline = self
            .pipeline
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("GStreamer pipeline missing for prebuffer"))?;
        let set_state_start = std::time::Instant::now();
        let ret = pipeline.set_state(gst::State::Paused)?;
        let set_state_duration = set_state_start.elapsed();
        match ret {
            gst::StateChangeSuccess::Success => debug!(
                "[VIDEO] {}: Pipeline state -> Paused (pre-roll complete)",
                self.source_id
            ),
            gst::StateChangeSuccess::Async => debug!(
                "[VIDEO] {}: Pipeline state -> Paused (Async, pre-buffering)",
                self.source_id
            ),
            _ => {}
        };
        let set_state_result = match ret {
            gst::StateChangeSuccess::Success => "success",
            gst::StateChangeSuccess::Async => "async",
            gst::StateChangeSuccess::NoPreroll => "no-preroll",
        };

        let state_wait_start = std::time::Instant::now();
        let state_wait_budget = std::time::Duration::from_millis(1500);
        let state_wait_slice = std::time::Duration::from_millis(50);
        let mut state_settled = false;
        let mut current = gst::State::Null;
        let mut pending = gst::State::VoidPending;
        while state_wait_start.elapsed() < state_wait_budget {
            if should_abort() {
                anyhow::bail!("prebuffer aborted");
            }

            let remaining = state_wait_budget.saturating_sub(state_wait_start.elapsed());
            let wait_slice = remaining.min(state_wait_slice);
            let (state_result, current_state, pending_state) =
                pipeline.state(gst::ClockTime::from_mseconds(wait_slice.as_millis() as u64));
            current = current_state;
            pending = pending_state;
            if state_result.is_ok() {
                state_settled = true;
                break;
            }
        }
        let state_wait_duration = state_wait_start.elapsed();
        if state_settled {
            debug!(
                "[VIDEO] {}: Pre-buffer state settled at {:?} (pending {:?})",
                self.source_id, current, pending
            );
        } else {
            debug!(
                "[VIDEO] {}: Timed out waiting for pre-buffer state ({:?} -> {:?})",
                self.source_id, current, pending
            );
        }

        let pull_preroll_start = std::time::Instant::now();
        let preroll_budget = std::time::Duration::from_millis(250);
        let preroll_slice = std::time::Duration::from_millis(50);
        let mut preroll = None;
        if let Some(appsink) = self.appsink.as_ref() {
            while pull_preroll_start.elapsed() < preroll_budget {
                if should_abort() {
                    anyhow::bail!("prebuffer aborted");
                }

                let remaining = preroll_budget.saturating_sub(pull_preroll_start.elapsed());
                let wait_slice = remaining.min(preroll_slice);
                let Some(sample) = appsink
                    .try_pull_preroll(gst::ClockTime::from_mseconds(wait_slice.as_millis() as u64))
                else {
                    continue;
                };

                preroll =
                    match sample_to_video_frame(self.source_id.as_ref(), sample, self.session_id) {
                        Ok(frame) => Some(frame),
                        Err(e) => {
                            debug!(
                                "[VIDEO] {}: Failed to decode preroll sample: {:?}",
                                self.source_id, e
                            );
                            None
                        }
                    };
                break;
            }
        }
        let pull_preroll_duration = pull_preroll_start.elapsed();

        let profile = VideoPrebufferProfile {
            set_state: set_state_duration,
            state_wait: state_wait_duration,
            pull_preroll: pull_preroll_duration,
            set_state_result,
            state_wait_settled: state_settled,
            current_state: current,
            pending_state: pending,
        };

        debug!(
            "[VIDEO] {}: Pre-buffer timings set_state {:.1}ms ({}) + wait_state {:.1}ms settled={} current={:?} pending={:?} + pull_preroll {:.1}ms preroll_frame={}",
            self.source_id,
            profile.set_state.as_secs_f64() * 1000.0,
            profile.set_state_result,
            profile.state_wait.as_secs_f64() * 1000.0,
            profile.state_wait_settled,
            profile.current_state,
            profile.pending_state,
            profile.pull_preroll.as_secs_f64() * 1000.0,
            preroll.is_some()
        );

        if preroll.is_some() && !self.first_frame_logged.swap(true, Ordering::SeqCst) {
            let duration = self.start_time.elapsed();
            info!(
                "[ASSET] {}: First video frame produced in {:.3}ms (preroll)",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            );
        }
        if let Some(frame) = preroll.as_ref() {
            maybe_log_decode_path(self.source_id.as_ref(), frame, &self.decode_path_logged);
        }

        Ok(VideoPrebufferResult {
            frame: preroll,
            profile,
        })
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        info!(
            "[VIDEO] {}: Starting playback for {}",
            self.source_id,
            self.pipeline
                .as_ref()
                .map(|pipeline| pipeline.name().to_string())
                .unwrap_or_else(|| self.backend_label().to_string())
        );
        self.log_backend_snapshot("start");

        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_mut() {
            mpv.start()?;
            if let Some(position_ns) = self.pending_start_position_ns.take()
                && let Err(e) = mpv.seek_to_position_ns(position_ns)
            {
                debug!(
                    "[VIDEO] {}: libmpv startup seek to {:.1}ms was skipped after start: {}",
                    self.source_id,
                    position_ns as f64 / 1_000_000.0,
                    e
                );
            }
            self.is_running.store(true, Ordering::SeqCst);
            return Ok(());
        }

        let pipeline = self
            .pipeline
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("GStreamer pipeline missing for start"))?;
        let ret = pipeline.set_state(gst::State::Playing)?;
        let duration = self.start_time.elapsed();
        match ret {
            gst::StateChangeSuccess::Success => info!(
                "[VIDEO] {}: Pipeline state -> Playing in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
            gst::StateChangeSuccess::Async => info!(
                "[VIDEO] {}: Pipeline state -> Playing (Async) in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
            gst::StateChangeSuccess::NoPreroll => info!(
                "[VIDEO] {}: Pipeline state -> Playing (Live) in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
        }

        self.install_bus_watch()?;
        if let Some(position_ns) = self.pending_start_position_ns.take()
            && let Err(e) = self.seek_to_position_ns(position_ns)
        {
            debug!(
                "[VIDEO] {}: startup seek to {:.1}ms was skipped after start: {}",
                self.source_id,
                position_ns as f64 / 1_000_000.0,
                e
            );
        }
        self.is_running.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn install_bus_watch(&mut self) -> anyhow::Result<()> {
        self.remove_bus_watch();
        let pipeline = self
            .pipeline
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("GStreamer pipeline missing for bus watch"))?;
        let bus = pipeline
            .bus()
            .ok_or_else(|| anyhow::anyhow!("Pipeline has no bus"))?;
        let pipeline = pipeline.clone();
        let source_id = self.source_id.clone();
        let player_event_tx = self.player_event_tx.clone();
        let session_id = self.session_id;
        let backend_kind = self.backend_kind;

        let watch = bus.create_watch(
            Some(&format!("kaleidux-bus-{}", source_id)),
            gst::glib::Priority::DEFAULT,
            move |_bus, msg| {
                use gst::MessageView;

                if crate::observability::trace_all::trace_all_enabled() {
                    tracing::trace!(
                        "[TRACE5][GST-BUS] output={} session={} backend={:?} type={:?} src={:?} seqnum={:?}",
                        source_id,
                        session_id,
                        backend_kind,
                        msg.type_(),
                        msg.src().map(|src| src.path_string()),
                        msg.seqnum()
                    );
                }

                match msg.view() {
                    MessageView::StateChanged(s)
                        if s.src()
                            .as_ref()
                            .map(|src| {
                                std::ptr::eq(
                                    src.as_ptr() as *const std::ffi::c_void,
                                    pipeline.as_ptr() as *const std::ffi::c_void,
                                )
                            })
                            .unwrap_or(false) =>
                    {
                        debug!(
                            "[VIDEO] {}: Pipeline state changed from {:?} to {:?}",
                            source_id,
                            s.old(),
                            s.current()
                        );
                    }
                    MessageView::Eos(..) => {
                        let _ = player_event_tx.send(PlayerEvent {
                            source_id: source_id.to_string(),
                            session_id,
                            backend_kind,
                            kind: PlayerEventKind::Eos,
                            reason: "eos".to_string(),
                        });
                        if let Err(reason) =
                            restart_pipeline_after_eos(&pipeline, source_id.as_ref(), backend_kind)
                        {
                            let _ = player_event_tx.send(PlayerEvent {
                                source_id: source_id.to_string(),
                                session_id,
                                backend_kind,
                                kind: PlayerEventKind::FatalLifecycle,
                                reason: reason.clone(),
                            });
                            tracing::error!("[VIDEO] {}: {}", source_id, reason);
                            return gst::glib::ControlFlow::Break;
                        }
                    }
                    MessageView::SegmentDone(..)
                        if pipeline
                            .seek_simple(
                                gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT,
                                gst::ClockTime::ZERO,
                            )
                            .is_err() =>
                    {
                        let reason = "failed to restart segment loop".to_string();
                        let _ = player_event_tx.send(PlayerEvent {
                            source_id: source_id.to_string(),
                            session_id,
                            backend_kind,
                            kind: PlayerEventKind::FatalLifecycle,
                            reason: reason.clone(),
                        });
                        tracing::error!("[VIDEO] {}: {}", source_id, reason);
                        return gst::glib::ControlFlow::Break;
                    }
                    MessageView::SegmentDone(..) => {}
                    MessageView::Error(err) => {
                        let error_msg = format!(
                            "Error from {:?}: {} ({:?})",
                            err.src().map(|s| s.path_string()),
                            err.error(),
                            err.debug()
                        );
                        tracing::error!("[VIDEO] {}: {}", source_id, error_msg);
                        let _ = player_event_tx.send(PlayerEvent {
                            source_id: source_id.to_string(),
                            session_id,
                            backend_kind,
                            kind: PlayerEventKind::Error,
                            reason: error_msg,
                        });
                        return gst::glib::ControlFlow::Break;
                    }
                    _ => {}
                }

                gst::glib::ControlFlow::Continue
            },
        );

        self.bus_watch = Some(attach_bus_watch(watch));
        Ok(())
    }

    fn remove_bus_watch(&mut self) {
        if let Some(watch) = self.bus_watch.take() {
            watch.remove();
        }
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        self.request_stop()
    }

    pub fn appsink_queue_levels(&self) -> Option<AppsinkQueueLevels> {
        let appsink = self.appsink.as_ref()?;
        appsink.find_property("current-level-buffers")?;

        Some(AppsinkQueueLevels {
            buffers: appsink.property::<u64>("current-level-buffers"),
            bytes: appsink
                .find_property("current-level-bytes")
                .map(|_| appsink.property::<u64>("current-level-bytes"))
                .unwrap_or_default(),
            time_ns: appsink
                .find_property("current-level-time")
                .map(|_| appsink.property::<u64>("current-level-time"))
                .unwrap_or_default(),
        })
    }

    pub fn request_stop(&mut self) -> anyhow::Result<()> {
        let was_running = self.is_running.swap(false, Ordering::SeqCst);
        self.remove_bus_watch();
        self.accept_samples.store(false, Ordering::SeqCst);
        self.frame_mailbox.clear_source(self.source_id.as_ref());
        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_mut() {
            mpv.stop()?;
            return Ok(());
        }
        let Some(pipeline) = self.pipeline.as_ref() else {
            return Ok(());
        };
        if let Some(bus) = pipeline.bus() {
            bus.unset_sync_handler();
        }
        if gst::version() >= (1, 16, 3, 0)
            && let Some(appsink) = self.appsink.as_ref()
        {
            appsink.set_callbacks(gst_app::AppSinkCallbacks::builder().build());
        }
        if was_running {
            info!(
                "[VIDEO] {}: Stopping video playback (session={} backend={})",
                self.source_id,
                self.session_id,
                self.backend_label()
            );
            // Fade audio before teardown to prevent clicks on audio-enabled pipelines.
            pipeline.set_property("volume", 0.0);
        }

        // Pause first (transition to Ready state first helps cleanup).
        let _ = pipeline.set_state(gst::State::Paused);

        // Always force Null, even for players that never fully entered Playing.
        // Prebuffered or failed-start pipelines can still hold decoder state.
        pipeline.set_state(gst::State::Null)?;
        Ok(())
    }

    pub fn set_volume(&mut self, volume: f64) {
        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_ref() {
            mpv.set_volume(volume);
            return;
        }
        if let Some(pipeline) = self.pipeline.as_ref()
            && pipeline.find_property("volume").is_some()
        {
            pipeline.set_property("volume", volume);
        }
    }

    pub fn pause(&self) -> anyhow::Result<()> {
        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.pause();
        }
        self.pipeline
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("GStreamer pipeline missing for pause"))?
            .set_state(gst::State::Paused)?;
        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        #[cfg(feature = "mpv-backend")]
        if let Some(mpv) = self.mpv.as_ref() {
            return mpv.resume();
        }
        self.pipeline
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("GStreamer pipeline missing for resume"))?
            .set_state(gst::State::Playing)?;
        Ok(())
    }
}

impl Drop for VideoPlayer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
