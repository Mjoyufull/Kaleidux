//! Starts explicit wallpaper inspection requests without waiting on the display loop.

use super::first_frame::first_png;
use crate::main_loop::MainLoopContext;
use crate::media_source::PlaybackWindow;
use crate::queue::ContentType;
use crate::runtime::prepared_command::PreparedCommand;
use anyhow::{Context, Result};
use gstreamer::prelude::*;
use kaleidux_common::{
    DEFAULT_FRAME_EDGE, FrameRequest, MAX_FRAME_EDGE, Request, Response, WallpaperInfo,
};
use tokio::sync::oneshot;

static GST_TIMELINE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

impl MainLoopContext {
    pub(super) fn dispatch_wallpaper_data(
        &mut self,
        command: PreparedCommand,
        response: oneshot::Sender<Response>,
    ) -> Option<(PreparedCommand, oneshot::Sender<Response>)> {
        match command.request {
            Request::WallpaperInfo { output } => self.send_wallpaper_info(output, response),
            Request::Frame(request) => self.start_frame_request(request, response),
            _ => return Some((command, response)),
        }
        None
    }

    fn inspection_output(&self, output: Option<String>) -> Result<String> {
        self.monitor_manager
            .command_output(
                output,
                self.renderers
                    .iter()
                    .map(|(name, renderer)| (name, renderer.config.width, renderer.config.height)),
            )?
            .context("Select a single connected output")
    }

    fn send_wallpaper_info(&self, output: Option<String>, response: oneshot::Sender<Response>) {
        let details = self.inspection_output(output).and_then(|name| {
            let renderer = self
                .renderers
                .get(&name)
                .context("Output is still connecting")?;
            let output = self
                .monitor_manager
                .outputs
                .get(&name)
                .context("Output disconnected")?;
            let shader = renderer.shader_wallpaper();
            let loading = shader.is_none()
                && (renderer.content_is_loading()
                    || self.pending_video_switches.contains_key(&name));
            let source = shader.map(str::to_owned).or_else(|| {
                output
                    .current_path
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned())
            });
            let kind = source.as_ref().filter(|_| !loading).map(|_| {
                if shader.is_some_and(|source| crate::scene::is_scene(std::path::Path::new(source)))
                {
                    "scene".into()
                } else if shader.is_some() {
                    "shader".into()
                } else if let Some(kind) = self
                    .video_players
                    .get(&name)
                    .and_then(crate::video::VideoPlayer::canvas_kind)
                {
                    kind.into()
                } else {
                    match renderer.valid_content_type {
                        ContentType::Image => "image".into(),
                        ContentType::Video => "video".into(),
                    }
                }
            });
            Ok(WallpaperInfo {
                width: renderer.config.width,
                height: renderer.config.height,
                paused: self.monitor_manager.is_paused()
                    || self.display_power_suspended
                    || self.powered_off_outputs.contains(&name)
                    || renderer.transition_playback_is_held(),
                output: name,
                source,
                kind,
                loading,
                transition_progress: renderer
                    .transition_active
                    .then_some(renderer.transition_progress),
                slideshow_duration_ns: output.config.duration.as_nanos().min(u64::MAX as u128)
                    as u64,
                media_duration_ns: None,
                scene_start_ns: None,
                scene_duration_ns: None,
                position_ns: None,
            })
        });
        let mut info = match details {
            Ok(info) => info,
            Err(error) => return send_error(response, error),
        };
        let Some(player) = self
            .video_players
            .get(&info.output)
            .filter(|_| matches!(info.kind.as_deref(), Some("video" | "gif" | "web")))
        else {
            let _ = response.send(Response::WallpaperInfo(info));
            return;
        };
        let scene = player.playback_window();
        let held_position = self.renderers[&info.output].held_transition_position();
        if let Some(pipeline) = player.pipeline.clone() {
            let Ok(permit) = GST_TIMELINE.try_acquire() else {
                send_error(response, anyhow::anyhow!("Wallpaper timeline is busy"));
                return;
            };
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                if response.is_closed() {
                    return;
                }
                let duration = pipeline.query_duration::<gstreamer::ClockTime>();
                let position = pipeline.query_position::<gstreamer::ClockTime>();
                set_timeline(
                    &mut info,
                    scene,
                    duration.map(gstreamer::ClockTime::nseconds),
                    held_position.or_else(|| position.map(gstreamer::ClockTime::nseconds)),
                );
                let _ = response.send(Response::WallpaperInfo(info));
            });
        } else {
            set_timeline(
                &mut info,
                scene,
                player.duration_ns(),
                held_position.or_else(|| player.current_position_ns()),
            );
            let _ = response.send(Response::WallpaperInfo(info));
        }
    }

    fn start_frame_request(&mut self, request: FrameRequest, response: oneshot::Sender<Response>) {
        if response.is_closed() {
            return;
        }
        let work = match self.prepare_frame(request) {
            Ok(work) => work,
            Err(error) => return send_error(response, error),
        };
        tokio::spawn(async move {
            if response.is_closed() {
                return;
            }
            let result = match work {
                FrameWork::Current(readback) => readback.png().await,
                FrameWork::First {
                    source,
                    at_ns,
                    max_size,
                } => first_png(source, at_ns, max_size).await,
            };
            match result {
                Ok(png) => {
                    let _ = response.send(Response::FramePng(png));
                }
                Err(error) => send_error(response, error),
            }
        });
    }

    fn prepare_frame(&mut self, request: FrameRequest) -> Result<FrameWork> {
        let max_size = request.max_size.unwrap_or(DEFAULT_FRAME_EDGE);
        anyhow::ensure!(
            (1..=MAX_FRAME_EDGE).contains(&max_size),
            "Frame size must be 1..={MAX_FRAME_EDGE}"
        );
        if request.first
            && let Some(source) = request.source
        {
            anyhow::ensure!(request.output.is_none(), "Select a source or an output");
            return Ok(FrameWork::First {
                source,
                at_ns: request.at_ns.unwrap_or_default(),
                max_size,
            });
        }
        anyhow::ensure!(
            request.first || (request.source.is_none() && request.at_ns.is_none()),
            "A source or timestamp requires first-frame export"
        );
        let name = self.inspection_output(request.output)?;
        if request.first {
            let source = self
                .monitor_manager
                .outputs
                .get(&name)
                .and_then(|output| output.current_path.as_ref())
                .context("Output has no wallpaper")?;
            // The queue describes the requested selection while a previous
            // player may still be active. Use the incoming excerpt in that gap.
            let player_window = self
                .video_players
                .get(&name)
                .filter(|_| {
                    !self.pending_video_switches.contains_key(&name)
                        && self.renderers.get(&name).is_some_and(|renderer| {
                            !renderer.content_is_loading()
                                && renderer.valid_content_type == ContentType::Video
                        })
                })
                .and_then(|player| player.playback_window());
            let scene = player_window.or_else(|| self.monitor_manager.excerpt_request(&name).0);
            let offset = request.at_ns.unwrap_or_default();
            if let Some(window) = scene {
                anyhow::ensure!(
                    offset < window.end_ns - window.start_ns,
                    "Frame offset is outside the selected scene"
                );
            }
            let at_ns = scene
                .map_or(0, |window| window.start_ns)
                .checked_add(offset)
                .context("Frame timestamp overflow")?;
            return Ok(FrameWork::First {
                source: source.to_string_lossy().into_owned(),
                at_ns,
                max_size,
            });
        }
        let renderer = self
            .renderers
            .get_mut(&name)
            .context("Output is still connecting")?;
        anyhow::ensure!(
            !self.pending_video_switches.contains_key(&name),
            "Wallpaper is still loading"
        );
        anyhow::ensure!(
            !self
                .video_players
                .get(&name)
                .is_some_and(|player| player.renders_natively()),
            "Current frame is unavailable in the native mpv overlay"
        );
        Ok(FrameWork::Current(
            renderer.capture_wallpaper_frame(max_size)?,
        ))
    }
}

enum FrameWork {
    Current(crate::renderer::frame_capture::FrameReadback),
    First {
        source: String,
        at_ns: u64,
        max_size: u32,
    },
}

fn set_timeline(
    info: &mut WallpaperInfo,
    scene: Option<PlaybackWindow>,
    duration: Option<u64>,
    position: Option<u64>,
) {
    let start = scene.map_or(0, |window| window.start_ns);
    let scene_duration = scene
        .map(|window| window.end_ns - window.start_ns)
        .or(duration);
    info.media_duration_ns = duration;
    info.scene_start_ns = Some(start);
    info.scene_duration_ns = scene_duration;
    info.position_ns = position.map(|position| {
        let relative = position.saturating_sub(start);
        scene_duration.map_or(relative, |length| relative.min(length))
    });
}

fn send_error(response: oneshot::Sender<Response>, error: anyhow::Error) {
    let _ = response.send(Response::Error(error.to_string()));
}
