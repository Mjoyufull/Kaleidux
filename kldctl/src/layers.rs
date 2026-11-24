//! Resolves ordered layer names and paths before IPC serialization.

use kaleidux_common::Request;

pub(crate) fn layer_request(
    stage: kaleidux_common::LayerStage,
    names: Vec<String>,
    fps: u8,
    output: Option<String>,
) -> anyhow::Result<Request> {
    let layers = names
        .into_iter()
        .map(|name| {
            if !name.ends_with(".wgsl") || std::path::Path::new(&name).is_absolute() {
                return Ok(name);
            }
            let path = std::env::current_dir()?.join(name);
            path.into_os_string()
                .into_string()
                .map_err(|_| anyhow::anyhow!("Shader path must be valid UTF-8"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Request::LayersSet(kaleidux_common::LayerSelection {
        output,
        stage,
        layers,
        fps,
    }))
}
