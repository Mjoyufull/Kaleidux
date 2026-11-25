//! Bounds request buffering while the caller owns the connected Unix stream.

use kaleidux_common::{MorphEvent, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tracing::warn;

pub(crate) async fn write_response(
    stream: &mut tokio::net::UnixStream,
    response: Response,
) -> std::io::Result<()> {
    match response {
        Response::FramePng(bytes) => stream.write_all(&bytes).await,
        response => stream.write_all(&serde_json::to_vec(&response)?).await,
    }
}

pub(crate) async fn watch_morphs(
    mut stream: UnixStream,
    output: Option<String>,
    mut events: broadcast::Receiver<MorphEvent>,
    write_timeout: std::time::Duration,
) -> std::io::Result<()> {
    let output = output.filter(|output| output != "all" && output != "*");
    tokio::time::timeout(write_timeout, stream.write_all(b"\"Ok\"\n")).await??;
    let mut disconnected = [0];
    loop {
        let event = tokio::select! {
            _ = stream.read(&mut disconnected) => return Ok(()),
            event = events.recv() => match event {
                Ok(event) => event,
                Err(_) => return Ok(()),
            }
        };
        if output.as_ref().is_some_and(|name| name != &event.output) {
            continue;
        }
        let mut line = serde_json::to_vec(&event)?;
        line.push(b'\n');
        tokio::time::timeout(write_timeout, stream.write_all(&line)).await??;
    }
}

pub(crate) async fn read_ipc_request_line(
    stream: &mut tokio::net::UnixStream,
    max_message_size: usize,
) -> Option<String> {
    let mut message = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        match stream.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                let chunk = &chunk[..n];
                let bytes_to_take = chunk.iter().position(|&b| b == b'\n').unwrap_or(n);
                if message.len() + bytes_to_take > max_message_size {
                    warn!(
                        "[IPC] Dropping oversized request (>{} bytes) from control socket",
                        max_message_size
                    );
                    return None;
                }
                message.extend_from_slice(&chunk[..bytes_to_take]);
                if bytes_to_take != n {
                    break;
                }
            }
            Err(e) => {
                warn!("[IPC] Failed reading request from control socket: {}", e);
                return None;
            }
        }
    }

    if message.is_empty() {
        return None;
    }

    match String::from_utf8(message) {
        Ok(message) => Some(message),
        Err(e) => {
            warn!("[IPC] Received non-UTF8 request on control socket: {}", e);
            None
        }
    }
}
