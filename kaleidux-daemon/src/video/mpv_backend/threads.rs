use super::*;
use crate::video::VideoBackendKind;

impl MpvPlayer {
    pub(super) fn spawn_event_thread(&mut self) {
        if self.event_thread.is_some() {
            return;
        }
        let event_handle = self.mpv.clone();
        let source_id = self.source_id.clone();
        let session_id = self.session_id;
        let player_event_tx = self.player_event_tx.clone();
        let stop_requested = self.stop_requested.clone();
        let first_hwdec_logged = self.first_hwdec_logged.clone();
        self.event_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-events-{}", source_id))
            .spawn(move || {
                while !stop_requested.load(Ordering::SeqCst) {
                    let Some(event) = event_handle.wait_event(-1.0) else {
                        continue;
                    };
                    match event {
                        Ok(events::Event::EndFile(reason)) => {
                            trace!(
                                "[VIDEO] {}: libmpv EndFile event session={} reason={:?}",
                                source_id, session_id, reason
                            );
                        }
                        Ok(events::Event::PlaybackRestart) => {
                            trace!(
                                "[VIDEO] {}: libmpv PlaybackRestart session={}",
                                source_id, session_id
                            );
                            log_decode_path_once(
                                &event_handle,
                                &source_id,
                                session_id,
                                &first_hwdec_logged,
                            );
                        }
                        Ok(events::Event::VideoReconfig) => {
                            trace!(
                                "[VIDEO] {}: libmpv VideoReconfig session={}",
                                source_id, session_id
                            );
                            log_decode_path_once(
                                &event_handle,
                                &source_id,
                                session_id,
                                &first_hwdec_logged,
                            );
                        }
                        Ok(events::Event::Shutdown) => break,
                        Ok(events::Event::LogMessage {
                            prefix,
                            level,
                            text,
                            ..
                        }) => {
                            let text = text.trim_end();
                            // drmprime-overlay is not used by this path and its
                            // no-KMS-card probe failure is expected.
                            if prefix.contains("drmprime-overlay") {
                                tracing::debug!("[MPV:{}] {}", prefix, text);
                                continue;
                            }
                            match level {
                                "error" => tracing::error!("[MPV:{}] {}", prefix, text),
                                "warn" => tracing::warn!("[MPV:{}] {}", prefix, text),
                                "info" => tracing::info!("[MPV:{}] {}", prefix, text),
                                _ => tracing::debug!("[MPV:{}] {}", prefix, text),
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            if is_ignorable_event_error(&error) {
                                trace!(
                                    "[VIDEO] {}: ignoring libmpv non-fatal event error: {}",
                                    source_id, error
                                );
                                continue;
                            }
                            let reason = format!("libmpv event error: {error}");
                            warn!("[VIDEO] {}: {}", source_id, reason);
                            let _ = player_event_tx.blocking_send(PlayerEvent {
                                source_id: source_id.to_string(),
                                session_id,
                                backend_kind: VideoBackendKind::Mpv,
                                kind: PlayerEventKind::Error,
                                reason,
                            });
                        }
                    }
                }
            })
            .ok();
    }

    pub(super) fn spawn_frame_thread(&mut self) {
        if self.frame_thread.is_some() {
            return;
        }
        let mpv = self.mpv.clone();
        let Some(render_context) = self.render_context.take() else {
            warn!(
                "[VIDEO] {}: libmpv frame thread not started; render context missing",
                self.source_id
            );
            return;
        };
        let source_id = self.source_id.clone();
        let session_id = self.session_id;
        let frame_mailbox = self.frame_mailbox.clone();
        let metrics = self.metrics.clone();
        let stop_requested = self.stop_requested.clone();
        let first_frame_logged = self.first_frame_logged.clone();
        let interval = self.capture_interval;
        let start_time = self.start_time;
        let publish_interval_ns = publish_interval_ns(self.max_publish_fps);
        let render_size = self.render_size;
        let last_publish_ns = Arc::new(std::sync::atomic::AtomicU64::new(
            super::super::NEVER_PUBLISHED_NS,
        ));
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-frames-{}", source_id))
            .spawn(move || {
                let render_context = render_context;
                while !stop_requested.load(Ordering::SeqCst) {
                    let frame_start = Instant::now();
                    let elapsed_ns = start_time.elapsed().as_nanos() as u64;
                    if should_publish_now(&last_publish_ns, publish_interval_ns, elapsed_ns) {
                        metrics
                            .record_video_backend_metric(VideoBackendMetricKind::MpvCaptureAttempt);
                        match capture_video_frame_with_context(
                            &mpv,
                            session_id,
                            &render_context,
                            render_size,
                            false,
                        ) {
                            Ok(Some(frame)) => {
                                if !first_frame_logged.swap(true, Ordering::SeqCst) {
                                    info!(
                                        "[ASSET] {}: First libmpv frame captured in {:.3}ms",
                                        source_id,
                                        start_time.elapsed().as_secs_f64() * 1000.0
                                    );
                                }
                                frame_mailbox.publish_frame(source_id.as_ref(), frame);
                                metrics.record_video_backend_metric(
                                    VideoBackendMetricKind::MpvFramePublished,
                                );
                            }
                            Ok(None) => {}
                            Err(error) => {
                                metrics.record_video_backend_metric(
                                    VideoBackendMetricKind::MpvCaptureError,
                                );
                                trace!("[VIDEO] {}: libmpv capture skipped: {}", source_id, error);
                            }
                        }
                    }
                    std::thread::sleep(interval.saturating_sub(frame_start.elapsed()));
                }
            })
            .ok();
    }

    pub(super) fn spawn_native_render_thread(&mut self) {
        if self.frame_thread.is_some() {
            return;
        }
        let Some(target) = self.native_target.clone() else {
            return;
        };
        let config = MpvNativeRenderThreadConfig {
            mpv: self.mpv.clone(),
            source_id: self.source_id.clone(),
            session_id: self.session_id,
            target,
            stop_requested: self.stop_requested.clone(),
            first_frame_logged: self.first_frame_logged.clone(),
            metrics: self.metrics.clone(),
            player_event_tx: self.player_event_tx.clone(),
            start_time: self.start_time,
            render_ready: self.render_ready.clone(),
            stop_wake: self
                .render_stop_wake
                .clone()
                .expect("native GL player has a stop wake"),
        };
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-gl-{}", self.source_id))
            .spawn(move || run_native_render_thread(config))
            .ok();
    }

    pub(super) fn spawn_composed_render_thread(&mut self) {
        if self.frame_thread.is_some() {
            return;
        }
        let Some(target) = self.composed_target.clone() else {
            return;
        };
        let config = MpvComposedRenderThreadConfig {
            mpv: self.mpv.clone(),
            source_id: self.source_id.clone(),
            session_id: self.session_id,
            target,
            frame_mailbox: self.frame_mailbox.clone(),
            stop_requested: self.stop_requested.clone(),
            first_frame_logged: self.first_frame_logged.clone(),
            metrics: self.metrics.clone(),
            player_event_tx: self.player_event_tx.clone(),
            start_time: self.start_time,
            min_render_interval: self
                .max_publish_fps
                .map(|fps| Duration::from_nanos(1_000_000_000u64 / fps.max(1) as u64)),
            render_ready: self.render_ready.clone(),
            stop_wake: self
                .render_stop_wake
                .clone()
                .expect("composed GL player has a stop wake"),
        };
        self.frame_thread = std::thread::Builder::new()
            .name(format!("kld-mpv-gpu-{}", self.source_id))
            .spawn(move || run_composed_render_thread(config))
            .ok();
    }
}
