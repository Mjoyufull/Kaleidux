use tokio::io::AsyncReadExt;
use tracing::warn;

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
