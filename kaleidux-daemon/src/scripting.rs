use kaleidux_common::{PlaylistCommand, Request, Response};
use rhai::{AST, CallFnOptions, Engine, EvalAltResult, Position, Scope};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

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
}

impl ScriptManager {
    pub fn new(cmd_tx: mpsc::Sender<(Request, oneshot::Sender<Response>)>) -> Self {
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

        engine.register_fn("print", |text: String| {
            info!("[Script] {}", text);
        });

        type ImageRequest = fn(String, Option<String>) -> Request;
        let image_commands: [(&str, ImageRequest); 3] = [
            ("jump", |path, output| Request::Jump { path, output }),
            ("set", |path, output| Request::Set { path, output }),
            ("img", |path, output| Request::Img { path, output }),
        ];
        for (name, request) in image_commands {
            let tx = cmd_tx.clone();
            engine.register_fn(name, move |path: String| {
                let (resp_tx, _) = oneshot::channel();
                enqueue_script_command(&tx, request(path, None), resp_tx)
            });
            let tx = cmd_tx.clone();
            engine.register_fn(name, move |path: String, output: String| {
                let (resp_tx, _) = oneshot::channel();
                let output = if output == "*" { None } else { Some(output) };
                enqueue_script_command(&tx, request(path, output), resp_tx)
            });
        }

        let tx = cmd_tx.clone();
        engine.register_fn("next", move |output: String| {
            let (resp_tx, _) = oneshot::channel();
            let out = if output == "*" { None } else { Some(output) };
            enqueue_script_command(&tx, Request::Next { output: out }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("next", move || {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Next { output: None }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("prev", move |output: String| {
            let (resp_tx, _) = oneshot::channel();
            let out = if output == "*" { None } else { Some(output) };
            enqueue_script_command(&tx, Request::Prev { output: out }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("prev", move || {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Prev { output: None }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("pause", move || {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Pause, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("resume", move || {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Resume, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("inhibit", move |reason: String| {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Inhibit { reason }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("uninhibit", move |reason: String| {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Uninhibit { reason }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("load_playlist", move |name: String| {
            let (resp_tx, _) = oneshot::channel();
            let load_name = if name.is_empty() || name == "*" {
                None
            } else {
                Some(name)
            };
            enqueue_script_command(
                &tx,
                Request::Playlist(PlaylistCommand::Load { name: load_name }),
                resp_tx,
            )
        });

        let tx = cmd_tx.clone();
        engine.register_fn("load_playlist", move || {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(
                &tx,
                Request::Playlist(PlaylistCommand::Load { name: None }),
                resp_tx,
            )
        });

        let tx = cmd_tx.clone();
        engine.register_fn("clear", move |output: String| {
            let (resp_tx, _) = oneshot::channel();
            let out = if output == "*" { None } else { Some(output) };
            enqueue_script_command(&tx, Request::Clear { output: out }, resp_tx)
        });

        let tx = cmd_tx.clone();
        engine.register_fn("clear", move || {
            let (resp_tx, _) = oneshot::channel();
            enqueue_script_command(&tx, Request::Clear { output: None }, resp_tx)
        });

        Self {
            engine,
            ast: None,
            scope: Scope::new(),
            has_tick: false,
        }
    }

    pub async fn load(&mut self, path: &PathBuf) -> anyhow::Result<()> {
        let mut content = String::new();
        tokio::fs::File::open(path)
            .await?
            .take((SCRIPT_MAX_SOURCE_BYTES + 1) as u64)
            .read_to_string(&mut content)
            .await?;
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
        self.ast = Some(ast);
        self.scope = scope;
        Ok(())
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

fn enqueue_script_command(
    tx: &mpsc::Sender<(Request, oneshot::Sender<Response>)>,
    request: Request,
    response: oneshot::Sender<Response>,
) -> Result<(), Box<EvalAltResult>> {
    if let Request::Inhibit { reason } | Request::Uninhibit { reason } = &request {
        kaleidux_common::validate_inhibit_reason(reason)
            .map_err(|error| Box::new(EvalAltResult::ErrorRuntime(error.into(), Position::NONE)))?;
    }
    tx.try_send((request, response)).map_err(|error| {
        warn!("[Script] Command was not enqueued: {error}");
        let msg = format!("command queue full: {error}");
        EvalAltResult::ErrorRuntime(msg.into(), Position::NONE).into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_commands_support_all_outputs_and_targeted_selection() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut manager = ScriptManager::new(tx);
        manager
            .eval(r#"jump("/a.png"); set("/b.png", "DP-1"); img("/c.png", "*");"#)
            .unwrap();
        assert!(
            matches!(rx.try_recv().unwrap().0, Request::Jump { path, output: None } if path == "/a.png")
        );
        assert!(
            matches!(rx.try_recv().unwrap().0, Request::Set { path, output: Some(output) } if path == "/b.png" && output == "DP-1")
        );
        assert!(
            matches!(rx.try_recv().unwrap().0, Request::Img { path, output: None } if path == "/c.png")
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
        assert!(matches!(rx.try_recv().unwrap().0, Request::Pause));
        manager.tick_result().unwrap();
        assert!(rx.try_recv().is_err());
        manager.tick_result().unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().0,
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
        assert!(matches!(req, Request::Next { output: None }));

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
        match req {
            Request::Inhibit { reason } => assert_eq!(reason, "gaming"),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"uninhibit("gaming");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv uninhibit");
        match req {
            Request::Uninhibit { reason } => assert_eq!(reason, "gaming"),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"prev("*");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv prev all");
        match req {
            Request::Prev { output } => assert_eq!(output, None),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"prev("DP-1");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv prev single");
        match req {
            Request::Prev { output } => assert_eq!(output, Some("DP-1".to_string())),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"load_playlist("night");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv load_playlist");
        match req {
            Request::Playlist(PlaylistCommand::Load { name }) => {
                assert_eq!(name, Some("night".to_string()))
            }
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"load_playlist("");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv unload playlist");
        match req {
            Request::Playlist(PlaylistCommand::Load { name }) => assert_eq!(name, None),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"clear("*");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv clear all");
        match req {
            Request::Clear { output } => assert_eq!(output, None),
            _ => panic!("unexpected request"),
        }

        assert!(sm.eval(r#"clear("HDMI-A-1");"#).is_ok());
        let (req, _) = rx.try_recv().expect("recv clear single");
        match req {
            Request::Clear { output } => assert_eq!(output, Some("HDMI-A-1".to_string())),
            _ => panic!("unexpected request"),
        }
    }
}
