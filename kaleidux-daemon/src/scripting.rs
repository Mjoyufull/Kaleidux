use rhai::{Engine, Scope, AST};
use std::path::PathBuf;
use tracing::{info, error};
use tokio::sync::{mpsc, oneshot};
use kaleidux_common::{Request, Response};

pub struct ScriptManager {
    engine: Engine,
    ast: Option<AST>,
    scope: Scope<'static>,
}

impl ScriptManager {
    pub fn new(cmd_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>) -> Self {
        let mut engine = Engine::new();

        engine.register_fn("print", |text: String| {
            info!("[Script] {}", text);
        });

        let tx = cmd_tx.clone();
        engine.register_fn("next", move |output: String| {
            let (resp_tx, _) = oneshot::channel();
            let out = if output == "*" { None } else { Some(output) };
            let _ = tx.send((Request::Next { output: out }, resp_tx));
        });

        let tx = cmd_tx.clone();
        engine.register_fn("pause", move || {
            let (resp_tx, _) = oneshot::channel();
            let _ = tx.send((Request::Pause, resp_tx));
        });

        let tx = cmd_tx.clone();
        engine.register_fn("resume", move || {
            let (resp_tx, _) = oneshot::channel();
            let _ = tx.send((Request::Resume, resp_tx));
        });

        Self {
            engine,
            ast: None,
            scope: Scope::new(),
        }
    }

    pub fn load(&mut self, path: &PathBuf) -> anyhow::Result<()> {
        let content = std::fs::read_to_string(path)?;
        let ast = self.engine.compile(content)?;
        self.ast = Some(ast);
        info!("Rhai script loaded from {:?}", path);
        
        // Run initial setup if it exists
        if let Some(ast) = &self.ast {
            if let Err(e) = self.engine.call_fn::<()>(&mut self.scope, ast, "init", ()) {
                if !e.to_string().contains("not found") {
                    error!("Rhai init error: {}", e);
                }
            }
        }
        
        Ok(())
    }

    pub fn tick(&mut self) {
        if let Some(ast) = &self.ast {
            if let Err(e) = self.engine.call_fn::<()>(&mut self.scope, ast, "on_tick", ()) {
                if !e.to_string().contains("not found") {
                    error!("Rhai tick error: {}", e);
                }
            }
        }
    }
}
