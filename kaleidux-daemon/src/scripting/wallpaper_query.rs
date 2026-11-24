//! Exposes asynchronous wallpaper timeline queries to Rhai hooks.

use crate::main_loop::CommandSender;
use kaleidux_common::{Request, Response};
use rhai::{Dynamic, Engine, EvalAltResult, Map, Position};
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Clone)]
struct WallpaperQuery(Arc<parking_lot::Mutex<oneshot::Receiver<Response>>>);

pub(super) fn register(engine: &mut Engine, tx: CommandSender) {
    engine.register_type_with_name::<WallpaperQuery>("WallpaperQuery");
    let default_tx = tx.clone();
    engine.register_fn("wallpaper_info", move || start(&default_tx, None));
    engine.register_fn("wallpaper_info", move |output: String| {
        start(&tx, Some(output))
    });
    engine.register_fn("read", WallpaperQuery::read);
}

fn start(tx: &CommandSender, output: Option<String>) -> Result<WallpaperQuery, Box<EvalAltResult>> {
    let (sender, receiver) = oneshot::channel();
    tx.try_send((Request::WallpaperInfo { output }.into(), sender))
        .map_err(|_| runtime_error("Wallpaper command queue is unavailable"))?;
    Ok(WallpaperQuery(Arc::new(parking_lot::Mutex::new(receiver))))
}

impl WallpaperQuery {
    fn read(&mut self) -> Result<Dynamic, Box<EvalAltResult>> {
        let reply = match self.0.lock().try_recv() {
            Ok(response) => response,
            Err(oneshot::error::TryRecvError::Empty) => return Ok(Dynamic::UNIT),
            Err(oneshot::error::TryRecvError::Closed) => {
                return Err(runtime_error("Wallpaper query is closed or already read"));
            }
        };
        let info = match reply {
            Response::WallpaperInfo(info) => info,
            Response::Error(error) => return Err(runtime_error(&error)),
            _ => return Err(runtime_error("Unexpected wallpaper query response")),
        };
        let mut map = Map::new();
        map.insert("output".into(), Dynamic::from(info.output));
        map.insert("width".into(), Dynamic::from_int(i64::from(info.width)));
        map.insert("height".into(), Dynamic::from_int(i64::from(info.height)));
        map.insert(
            "source".into(),
            info.source.map_or(Dynamic::UNIT, Dynamic::from),
        );
        map.insert(
            "kind".into(),
            info.kind.map_or(Dynamic::UNIT, Dynamic::from),
        );
        map.insert("loading".into(), Dynamic::from(info.loading));
        map.insert("paused".into(), Dynamic::from(info.paused));
        map.insert(
            "transition_progress".into(),
            info.transition_progress.map_or(Dynamic::UNIT, |progress| {
                Dynamic::from_float(f64::from(progress))
            }),
        );
        for (key, value) in [
            ("slideshow_duration_ns", Some(info.slideshow_duration_ns)),
            ("media_duration_ns", info.media_duration_ns),
            ("scene_start_ns", info.scene_start_ns),
            ("scene_duration_ns", info.scene_duration_ns),
            ("position_ns", info.position_ns),
        ] {
            map.insert(
                key.into(),
                value
                    .and_then(|value| i64::try_from(value).ok())
                    .map_or(Dynamic::UNIT, Dynamic::from_int),
            );
        }
        Ok(Dynamic::from_map(map))
    }
}

fn runtime_error(message: &str) -> Box<EvalAltResult> {
    EvalAltResult::ErrorRuntime(message.to_owned().into(), Position::NONE).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripting::ScriptManager;

    #[test]
    fn script_queries_without_waiting_for_the_display_loop() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let mut script = ScriptManager::new(sender);
        script
            .eval(r#"let query = wallpaper_info("DP-2"); if query.read() != () { throw "expected pending"; }"#)
            .unwrap();
        let (command, reply) = receiver.try_recv().unwrap();
        assert!(matches!(
            command.request,
            Request::WallpaperInfo { output: Some(output) } if output == "DP-2"
        ));
        reply
            .send(Response::WallpaperInfo(kaleidux_common::WallpaperInfo {
                output: "DP-2".into(),
                width: 3840,
                height: 2160,
                source: Some("test.mp4".into()),
                kind: Some("video".into()),
                loading: false,
                paused: false,
                transition_progress: None,
                slideshow_duration_ns: 10_000_000_000,
                media_duration_ns: Some(60_000_000_000),
                scene_start_ns: Some(20_000_000_000),
                scene_duration_ns: Some(10_000_000_000),
                position_ns: Some(500_000_000),
            }))
            .unwrap();
        script
            .eval(r#"let data = query.read(); if data.output != "DP-2" || data.position_ns != 500000000 { throw "wrong timeline"; }"#)
            .unwrap();
    }
}
