//! Preserves Rhai call order across asset preparation and asynchronous installs.

use crate::main_loop::CommandSender;
use crate::runtime::prepared_command::prepare_command;
use kaleidux_common::{Request, Response};
use parking_lot::Mutex;
use rhai::{EvalAltResult, Position};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

type ScriptRequest = (Request, oneshot::Sender<Response>);
const SCRIPT_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
pub(super) struct ScriptCommands {
    display: CommandSender,
    pending: Arc<Mutex<Option<mpsc::Sender<ScriptRequest>>>>,
}

impl ScriptCommands {
    pub(super) fn new(display: CommandSender) -> Self {
        Self {
            display,
            pending: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn submit(&self, request: Request) -> Result<(), Box<EvalAltResult>> {
        self.enqueue(request, oneshot::channel().0)
    }

    pub(super) fn enqueue(
        &self,
        request: Request,
        response: oneshot::Sender<Response>,
    ) -> Result<(), Box<EvalAltResult>> {
        if let Request::Inhibit { reason } | Request::Uninhibit { reason } = &request {
            kaleidux_common::validate_inhibit_reason(reason).map_err(runtime_error)?;
        }

        let mut pending = self.pending.lock();
        if pending.is_none() && !needs_preparation(&request) {
            return self
                .display
                .try_send((request.into(), response))
                .map_err(|error| runtime_error(format!("command queue full: {error}")));
        }
        if pending.is_none() {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|error| runtime_error(error.to_string()))?;
            let (sender, receiver) = mpsc::channel(SCRIPT_QUEUE_CAPACITY);
            runtime.spawn(drain(receiver, self.display.clone()));
            *pending = Some(sender);
        }
        pending
            .as_ref()
            .expect("script worker was initialized")
            .try_send((request, response))
            .map_err(|error| runtime_error(format!("command queue full: {error}")))
    }
}

fn needs_preparation(request: &Request) -> bool {
    matches!(
        request,
        Request::SetReveal { .. } | Request::SetTransition { .. } | Request::LayersSet(_)
    )
}

async fn drain(mut pending: mpsc::Receiver<ScriptRequest>, display: CommandSender) {
    while let Some((request, response)) = pending.recv().await {
        let prepared = needs_preparation(&request);
        // Layer requests reenter the display queue after GPU compilation.
        // Wait for installation before submitting any later Rhai command.
        let reply = match prepare_command(request).await {
            Ok(command) => {
                let (send, receive) = oneshot::channel();
                if display.send((command, send)).await.is_err() {
                    break;
                }
                receive
                    .await
                    .unwrap_or_else(|_| Response::Error("Display command closed".into()))
            }
            Err(error) => Response::Error(format!("{error:#}")),
        };
        if prepared && let Response::Error(error) = &reply {
            tracing::warn!("[Script] Command failed: {error}");
        }
        let _ = response.send(reply);
    }
}

fn runtime_error(message: impl Into<String>) -> Box<EvalAltResult> {
    EvalAltResult::ErrorRuntime(message.into().into(), Position::NONE).into()
}
