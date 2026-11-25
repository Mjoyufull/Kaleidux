//! Loads bounded Rhai hooks and exposes daemon control to scripts.

mod command_queue;
mod wallpaper_query;

use crate::main_loop::CommandSender;
use command_queue::ScriptCommands;
#[cfg(test)]
use kaleidux_common::Response;
use kaleidux_common::{PlaylistCommand, Request};
use rhai::{AST, CallFnOptions, Engine, EvalAltResult, Position, Scope};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
#[cfg(test)]
use tokio::sync::mpsc;
use tracing::{error, info};

pub const SCRIPT_MAX_OPERATIONS: u64 = 50_000;
pub const SCRIPT_MAX_CALL_LEVELS: usize = 32;
pub const SCRIPT_MAX_EXPR_DEPTH: usize = 32;
pub const SCRIPT_MAX_STRING_SIZE: usize = 65_536;
pub const SCRIPT_MAX_ARRAY_SIZE: usize = 10_000;
pub const SCRIPT_MAX_MAP_SIZE: usize = 10_000;
pub const SCRIPT_MAX_SOURCE_BYTES: usize = 1024 * 1024;

pub struct ScriptManager {
    engine: Engine,
    ast: Option<AST>,
    scope: Scope<'static>,
    has_tick: bool,
    has_morph_action: bool,
}

impl ScriptManager {
    pub fn new(cmd_tx: CommandSender) -> Self {
        let mut engine = Engine::new();
        engine.set_max_operations(SCRIPT_MAX_OPERATIONS);
        engine.set_max_call_levels(SCRIPT_MAX_CALL_LEVELS);
        engine.set_max_expr_depths(SCRIPT_MAX_EXPR_DEPTH, SCRIPT_MAX_EXPR_DEPTH);
        engine.set_max_string_size(SCRIPT_MAX_STRING_SIZE);
        engine.set_max_array_size(SCRIPT_MAX_ARRAY_SIZE);
        engine.set_max_map_size(SCRIPT_MAX_MAP_SIZE);
        // Filesystem module imports can block the display thread outside the
        // operation budget. Scripts use the supplied in-process API only.
        engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver::new());
        let commands = ScriptCommands::new(cmd_tx);
        wallpaper_query::register(&mut engine, commands.clone());

        engine.register_fn("print", |text: String| {
            info!("[Script] {}", text);
        });

        register_layer_commands(&mut engine, &commands);

        type ImageRequest = fn(String, Option<String>) -> Request;
        let image_commands: [(&str, ImageRequest); 3] = [
            ("jump", |path, output| Request::Jump { path, output }),
            ("set", |path, output| Request::Set { path, output }),
            ("img", |path, output| Request::Img { path, output }),
        ];
        for (name, request) in image_commands {
            let tx = commands.clone();
            engine.register_fn(name, move |path: String| tx.submit(request(path, None)));
            let tx = commands.clone();
            engine.register_fn(name, move |path: String, output: String| {
                let output = Some(if output == "*" { "all".into() } else { output });
                tx.submit(request(path, output))
            });
        }

        type PreparedSwitch = fn(String, String, Option<String>) -> Request;
        let prepared_switches: [(&str, PreparedSwitch); 2] = [
            ("set_reveal", |path, map, output| Request::SetReveal {
                path,
                map,
                output,
            }),
            ("set_transition", |path, transition, output| {
                Request::SetTransition {
                    path,
                    transition,
                    output,
                }
            }),
        ];
        for (name, request) in prepared_switches {
            let tx = commands.clone();
            engine.register_fn(name, move |path: String, effect: String| {
                tx.submit(request(path, effect, None))
            });
            let tx = commands.clone();
            engine.register_fn(name, move |path: String, effect: String, output: String| {
                let output = Some(if output == "*" { "all".into() } else { output });
                tx.submit(request(path, effect, output))
            });
        }

        let tx = commands.clone();
        engine.register_fn("next", move |output: String| {
            let out = Some(if output == "*" { "all".into() } else { output });
            tx.submit(Request::Next { output: out })
        });

        let tx = commands.clone();
        engine.register_fn("next", move || tx.submit(Request::Next { output: None }));

        let tx = commands.clone();
        engine.register_fn("prev", move |output: String| {
            let out = Some(if output == "*" { "all".into() } else { output });
            tx.submit(Request::Prev { output: out })
        });

        let tx = commands.clone();
        engine.register_fn("prev", move || tx.submit(Request::Prev { output: None }));

        let tx = commands.clone();
        engine.register_fn("pause", move || tx.submit(Request::Pause));

        let tx = commands.clone();
        engine.register_fn("resume", move || tx.submit(Request::Resume));

        let tx = commands.clone();
        engine.register_fn("inhibit", move |reason: String| {
            tx.submit(Request::Inhibit { reason })
        });

        let tx = commands.clone();
        engine.register_fn("uninhibit", move |reason: String| {
            tx.submit(Request::Uninhibit { reason })
        });

        let tx = commands.clone();
        engine.register_fn("load_playlist", move |name: String| {
            let load_name = if name.is_empty() || name == "*" {
                None
            } else {
                Some(name)
            };
            tx.submit(Request::Playlist(PlaylistCommand::Load { name: load_name }))
        });

        let tx = commands.clone();
        engine.register_fn("load_playlist", move || {
            tx.submit(Request::Playlist(PlaylistCommand::Load { name: None }))
        });

        let tx = commands.clone();
        engine.register_fn("clear", move |output: String| {
            let out = Some(if output == "*" { "all".into() } else { output });
            tx.submit(Request::Clear { output: out })
        });

        let tx = commands.clone();
        engine.register_fn("clear", move || tx.submit(Request::Clear { output: None }));

        Self {
            engine,
            ast: None,
            scope: Scope::new(),
            has_tick: false,
            has_morph_action: false,
        }
    }

    pub async fn load(&mut self, path: &PathBuf) -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        tokio::fs::File::open(path)
            .await?
            .take((SCRIPT_MAX_SOURCE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await?;
        anyhow::ensure!(
            bytes.len() <= SCRIPT_MAX_SOURCE_BYTES,
            "script exceeds 1 MiB source limit"
        );
        let content = String::from_utf8(bytes)?;
        self.load_from_str(&content)?;
        info!("Rhai script loaded from {:?}", path);
        Ok(())
    }

    pub fn load_from_str(&mut self, content: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            content.len() <= SCRIPT_MAX_SOURCE_BYTES,
            "script exceeds 1 MiB source limit"
        );
        let ast = self.engine.compile(content)?;
        let mut scope = Scope::new();
        self.engine
            .run_ast_with_scope(&mut scope, &ast)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        if ast
            .iter_functions()
            .any(|function| function.name == "init" && function.params.is_empty())
        {
            self.engine
                .call_fn_with_options::<()>(
                    CallFnOptions::new().eval_ast(false),
                    &mut scope,
                    &ast,
                    "init",
                    (),
                )
                .map_err(|error| anyhow::anyhow!("{error}"))?;
        }
        self.has_tick = ast
            .iter_functions()
            .any(|function| function.name == "on_tick" && function.params.is_empty());
        self.has_morph_action = ast
            .iter_functions()
            .any(|function| function.name == "on_morph_action" && function.params.len() == 4);
        self.ast = Some(ast);
        self.scope = scope;
        Ok(())
    }

    pub(crate) fn morph_action(&mut self, action: &str, output: &str, [x, y]: [f32; 2]) {
        if self.has_morph_action
            && let Some(ast) = &self.ast
            && let Err(error) = self.engine.call_fn_with_options::<()>(
                CallFnOptions::new().eval_ast(false),
                &mut self.scope,
                ast,
                "on_morph_action",
                (
                    action.to_owned(),
                    output.to_owned(),
                    f64::from(x),
                    f64::from(y),
                ),
            )
        {
            tracing::warn!("Rhai on_morph_action failed: {error}");
        }
    }

    pub fn tick_result(&mut self) -> Result<(), Box<EvalAltResult>> {
        if self.has_tick
            && let Some(ast) = &self.ast
        {
            self.engine.call_fn_with_options::<()>(
                CallFnOptions::new().eval_ast(false),
                &mut self.scope,
                ast,
                "on_tick",
                (),
            )?;
        }
        Ok(())
    }

    pub fn tick(&mut self) {
        if let Err(e) = self.tick_result() {
            error!("Rhai tick error: {}", e);
        }
    }

    pub fn has_tick(&self) -> bool {
        self.has_tick
    }

    #[cfg(test)]
    pub fn eval(&mut self, script: &str) -> Result<(), Box<EvalAltResult>> {
        self.engine.run_with_scope(&mut self.scope, script)
    }
}

fn register_layer_commands(engine: &mut Engine, commands: &ScriptCommands) {
    let tx = commands.clone();
    engine.register_fn("react", move |input: &str| {
        let signals =
            serde_json::from_str::<kaleidux_common::LayerInputs>(input).map_err(|error| {
                Box::new(EvalAltResult::ErrorRuntime(
                    error.to_string().into(),
                    Position::NONE,
                ))
            })?;
        tx.submit(Request::LayerInputs(signals))
    });
    let tx = commands.clone();
    engine.register_fn("morph_group", move |input: &str| {
        let selection =
            serde_json::from_str::<kaleidux_common::LayerSelection>(input).map_err(|error| {
                Box::new(EvalAltResult::ErrorRuntime(
                    error.to_string().into(),
                    Position::NONE,
                ))
            })?;
        if selection.stage != kaleidux_common::LayerStage::Morph || selection.group.is_none() {
            return Err(Box::new(EvalAltResult::ErrorRuntime(
                "Use stage=morph and a group name".into(),
                Position::NONE,
            )));
        }
        tx.submit(Request::LayersSet(selection))
    });
    for (name, stage) in [
        ("filters", kaleidux_common::LayerStage::Filter),
        ("shaders", kaleidux_common::LayerStage::Filter),
        ("shader_wallpaper", kaleidux_common::LayerStage::Wallpaper),
        ("morphs", kaleidux_common::LayerStage::Morph),
    ] {
        let tx = commands.clone();
        engine.register_fn(name, move |names: rhai::Array, fps: i64, output: String| {
            let invalid =
                |reason: &str| Box::new(EvalAltResult::ErrorRuntime(reason.into(), Position::NONE));
            if !(-1..=i64::from(u16::MAX - 1)).contains(&fps) {
                return Err(invalid("Use a frame rate of -1 (unlimited), 0 or higher"));
            }
            let layers = names
                .into_iter()
                .map(|name| {
                    name.into_string()
                        .map_err(|_| invalid("Layer names must be strings"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let output =
                (!output.is_empty()).then(|| if output == "*" { "all".into() } else { output });
            tx.submit(Request::LayersSet(kaleidux_common::LayerSelection {
                output,
                stage,
                layers,
                fps: if fps == -1 {
                    kaleidux_common::LAYER_FPS_UNLIMITED
                } else {
                    fps as u16
                },
                group: None,
                layout: None,
            }))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prepared_calls_wait_for_install_before_later_script_calls() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut script = ScriptManager::new(tx);
        script
            .eval(r#"filters(["crt"], 0, "*"); filters([], 0, "*"); next();"#)
            .unwrap();

        let (install, installed) = rx.recv().await.unwrap();
        assert!(
            matches!(install.request, Request::LayersSet(selection) if selection.layers == ["crt"])
        );
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err());
        installed.send(Response::Ok).unwrap();

        let (clear, cleared) = rx.recv().await.unwrap();
        assert!(
            matches!(clear.request, Request::LayersSet(selection) if selection.layers.is_empty())
        );
        cleared.send(Response::Ok).unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().0.request,
            Request::Next { .. }
        ));
    }

    #[tokio::test]
    async fn oversized_unicode_script_reports_size_before_utf8() {
        let path = std::env::temp_dir().join(format!(
            "kld-script-limit-{}-{}.rhai",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut content = " ".repeat(SCRIPT_MAX_SOURCE_BYTES);
        content.push('界');
        tokio::fs::write(&path, content).await.unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let error = ScriptManager::new(tx).load(&path).await.unwrap_err();
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(error.to_string().contains("1 MiB source limit"));
    }

    #[test]
    fn image_commands_support_all_outputs_and_targeted_selection() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut manager = ScriptManager::new(tx);
        manager
            .eval(r#"jump("/a.png"); set("/b.png", "DP-1"); img("/c.png", "*");"#)
            .unwrap();
        assert!(
            matches!(rx.try_recv().unwrap().0.request, Request::Jump { path, output: None } if path == "/a.png")
        );
        assert!(
            matches!(rx.try_recv().unwrap().0.request, Request::Set { path, output: Some(output) } if path == "/b.png" && output == "DP-1")
        );
        assert!(
            matches!(rx.try_recv().unwrap().0.request, Request::Img { path, output: Some(output) } if path == "/c.png" && output == "all")
        );
    }

    #[test]
    fn globals_persist_and_initialization_runs_once() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut manager = ScriptManager::new(tx);
        manager
            .load_from_str(
                r#"
            let count = 0;
            fn init() { pause(); }
            fn on_tick() { count += 1; if count == 2 { next(); } }
        "#,
            )
            .unwrap();
        assert!(matches!(rx.try_recv().unwrap().0.request, Request::Pause));
        manager.tick_result().unwrap();
        assert!(rx.try_recv().is_err());
        manager.tick_result().unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().0.request,
            Request::Next { output: None }
        ));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn invalid_reason_and_file_import_fail_without_queueing() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut manager = ScriptManager::new(tx);
        assert!(manager.eval(r#"inhibit("");"#).is_err());
        assert!(manager.eval(r#"import "sample" as module;"#).is_err());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn execution_limit_infinite_loop_aborts() {
        let (tx, _rx) = mpsc::channel(16);
        let mut sm = ScriptManager::new(tx);
        let result = sm.eval("while true {}");
        assert!(result.is_err(), "infinite while loop must abort");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Too many operations") || err_msg.contains("operations"),
            "expected operation limit error, got: {err_msg}"
        );
    }

    #[test]
    fn execution_limit_call_depth_aborts() {
        let (tx, _rx) = mpsc::channel(16);
        let mut sm = ScriptManager::new(tx);
        let script = r#"
            fn deep(n) {
                deep(n + 1);
            }
            deep(0);
        "#;
        let result = sm.eval(script);
        assert!(result.is_err(), "excessive call depth must abort");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Stack overflow")
                || err_msg.contains("Call stack")
                || err_msg.contains("call levels")
                || err_msg.contains("depth"),
            "expected call levels exceeded error, got: {err_msg}"
        );
    }

    #[test]
    fn execution_limit_string_size_aborts() {
        let (tx, _rx) = mpsc::channel(16);
        let mut sm = ScriptManager::new(tx);
        let script = r#"
            let s = "abcdefghijklmnopqrstuvwxyz";
            while true {
                s += s;
            }
        "#;
        let result = sm.eval(script);
        assert!(result.is_err(), "excessive string allocation must abort");
    }

    #[test]
    fn script_queue_full_returns_error_rather_than_discard() {
        // Channel with capacity 1
        let (tx, mut rx) = mpsc::channel(1);
        let mut sm = ScriptManager::new(tx);

        // First command fills the queue
        assert!(sm.eval(r#"next("*");"#).is_ok(), "first command enqueued");

        // Second command when queue is full must error rather than discard
        let res = sm.eval(r#"next("*");"#);
        assert!(
            res.is_err(),
            "second command must return error when queue is full"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("command queue full"),
            "expected queue full error message, got: {err_msg}"
        );

        // Same for inhibit when queue is full
        let res_inh = sm.eval(r#"inhibit("game");"#);
        assert!(
            res_inh.is_err(),
            "inhibit must return error when queue is full"
        );

        // Now drain one and verify a command succeeds again
        let (req, _) = rx.try_recv().expect("drain queue");
        assert!(matches!(req.request, Request::Next { output: Some(output) } if output == "all"));

        assert!(
            sm.eval(r#"inhibit("game");"#).is_ok(),
            "command succeeds after queue freed"
        );
    }

    #[test]
    fn script_functions_enqueue_correct_requests() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut sm = ScriptManager::new(tx);

        assert!(sm.eval(r#"inhibit("gaming");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv inhibit");
        match req.request {
            Request::Inhibit { reason } => assert_eq!(reason, "gaming"),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"uninhibit("gaming");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv uninhibit");
        match req.request {
            Request::Uninhibit { reason } => assert_eq!(reason, "gaming"),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"prev("*");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv prev all");
        match req.request {
            Request::Prev { output } => assert_eq!(output, Some("all".into())),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"prev("DP-1");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv prev single");
        match req.request {
            Request::Prev { output } => assert_eq!(output, Some("DP-1".to_string())),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"load_playlist("night");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv load_playlist");
        match req.request {
            Request::Playlist(PlaylistCommand::Load { name }) => {
                assert_eq!(name, Some("night".to_string()))
            }
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"load_playlist("");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv unload playlist");
        match req.request {
            Request::Playlist(PlaylistCommand::Load { name }) => assert_eq!(name, None),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"clear("*");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv clear all");
        match req.request {
            Request::Clear { output } => assert_eq!(output, Some("all".into())),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"clear("HDMI-A-1");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv clear single");
        match req.request {
            Request::Clear { output } => assert_eq!(output, Some("HDMI-A-1".to_string())),
            _ => panic!("unexpected request"),
        }
    }
}
