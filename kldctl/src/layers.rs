//! Resolves ordered layer names and paths before IPC serialization.

use kaleidux_common::Request;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub(crate) fn layer_request(
    stage: kaleidux_common::LayerStage,
    names: Vec<String>,
    fps: u16,
    output: Option<String>,
) -> anyhow::Result<Request> {
    let layers = names
        .into_iter()
        .map(|name| {
            let (source, options) = name.split_once('|').unwrap_or((&name, ""));
            let (prefix, source) = match source.split_once('=') {
                Some((prefix @ ("image" | "parallax"), path)) => (Some(prefix), path),
                _ => (None, source),
            };
            let path = std::path::Path::new(source);
            if path.is_absolute()
                || (prefix.is_none()
                    && !source.ends_with(".shadertoy.json")
                    && !matches!(
                        path.extension().and_then(|ext| ext.to_str()),
                        Some("wgsl" | "glsl" | "frag" | "fs" | "fsh" | "glslp" | "slangp")
                    ))
            {
                return Ok(name);
            }
            let path = std::env::current_dir()?.join(path);
            let path = path
                .into_os_string()
                .into_string()
                .map_err(|_| anyhow::anyhow!("Shader path must be valid UTF-8"))?;
            let path = prefix.map_or_else(|| path.clone(), |prefix| format!("{prefix}={path}"));
            Ok(if options.is_empty() {
                path
            } else {
                format!("{path}|{options}")
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Request::LayersSet(kaleidux_common::LayerSelection {
        output,
        stage,
        layers,
        fps,
    }))
}

pub(crate) fn parse_layer_fps(value: &str) -> Result<u16, String> {
    if value.eq_ignore_ascii_case("unlimited") {
        return Ok(kaleidux_common::LAYER_FPS_UNLIMITED);
    }
    value
        .parse()
        .map_err(|_| "Use 0, a positive frame rate, or unlimited".into())
}

pub(crate) async fn watch_morphs(stream: tokio::net::UnixStream) -> anyhow::Result<()> {
    let mut lines = BufReader::new(stream).lines();
    let response = lines.next_line().await?.unwrap_or_default();
    match serde_json::from_str::<kaleidux_common::Response>(&response)? {
        kaleidux_common::Response::Ok => {}
        kaleidux_common::Response::Error(error) => anyhow::bail!("{error}"),
        _ => anyhow::bail!("Unexpected response to morph subscription"),
    }
    let mut output = tokio::io::stdout();
    while let Some(event) = lines.next_line().await? {
        output.write_all(event.as_bytes()).await?;
        output.write_all(b"\n").await?;
        output.flush().await?;
    }
    Ok(())
}
