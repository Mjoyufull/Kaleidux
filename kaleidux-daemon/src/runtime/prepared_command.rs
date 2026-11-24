//! Prepares file-backed assets and named shaders before dispatching display commands.

use crate::background::{BackgroundWorkKind, spawn_blocking_tracked};
use crate::renderer::{PreparedShader, RevealMap, TransitionAsset};
use kaleidux_common::{Request, Transition};
use std::{path::PathBuf, sync::Arc};

/// Deserialized control request and any assets prepared for its display-loop call.
pub struct PreparedCommand {
    pub request: Request,
    pub(crate) transition_override: Option<TransitionOverride>,
    pub(crate) prepared_layers: Option<crate::renderer::PreparedLayers>,
    pub(super) layer_rule_ticket: Option<u64>,
    pub(super) compiled_layers: Option<super::layer_commands::CompiledLayers>,
}

impl From<Request> for PreparedCommand {
    fn from(request: Request) -> Self {
        Self {
            request,
            transition_override: None,
            prepared_layers: None,
            layer_rule_ticket: None,
            compiled_layers: None,
        }
    }
}

#[derive(Clone)]
pub(crate) enum TransitionOverride {
    Asset(TransitionAsset),
    Named(Transition),
}

pub(crate) fn needs_preparation(request: &Request) -> bool {
    match request {
        Request::Set { path, .. } => {
            let path = std::path::Path::new(path);
            crate::renderer::is_shader_file(path) || crate::scene::is_scene(path)
        }
        Request::Workshop { .. }
        | Request::SetReveal { .. }
        | Request::SetTransition { .. }
        | Request::LayersSet(_) => true,
        _ => false,
    }
}

pub(crate) async fn prepare_command(request: Request) -> anyhow::Result<PreparedCommand> {
    let request = if let Request::Workshop { item, output } = request {
        let job = spawn_blocking_tracked(BackgroundWorkKind::QueueDiscovery, move || {
            crate::scene::workshop_path(std::path::Path::new(&item))
        })
        .ok_or_else(|| anyhow::anyhow!("Workshop discovery is busy"))?;
        Request::Set {
            path: job.await??.to_string_lossy().into_owned(),
            output,
        }
    } else {
        request
    };
    let request = match request {
        Request::Set { path, output }
            if !path.contains("://")
                && (crate::renderer::is_shader_file(std::path::Path::new(&path))
                    || crate::scene::is_scene(std::path::Path::new(&path))) =>
        {
            Request::LayersSet(kaleidux_common::LayerSelection {
                output,
                stage: kaleidux_common::LayerStage::Wallpaper,
                layers: vec![path],
                fps: kaleidux_common::LAYER_FPS_UNLIMITED,
            })
        }
        request => request,
    };
    let mut command = PreparedCommand::from(request);
    if let Request::SetReveal { map, .. } = &command.request {
        let path = PathBuf::from(map);
        let job = spawn_blocking_tracked(BackgroundWorkKind::ImageDecode, move || {
            RevealMap::load_png(&path)
        })
        .ok_or_else(|| anyhow::anyhow!("Transition map workers are busy; retry shortly"))?;
        command.transition_override = Some(TransitionOverride::Asset(TransitionAsset::Map(
            Arc::new(job.await??),
        )));
    }
    if let Request::SetTransition { transition, .. } = &command.request {
        let name = transition.trim().to_owned();
        let job = spawn_blocking_tracked(BackgroundWorkKind::ImageDecode, move || {
            prepare_named_transition(&name)
        })
        .ok_or_else(|| anyhow::anyhow!("Transition shader workers are busy; retry shortly"))?;
        command.transition_override = Some(job.await??);
    }
    if let Request::LayersSet(selection) = &command.request {
        command.prepared_layers = Some(crate::renderer::PreparedLayers::load(selection).await?);
    }
    Ok(command)
}

fn prepare_named_transition(name: &str) -> anyhow::Result<TransitionOverride> {
    use crate::shaders::ShaderManager;
    if let Some(custom) = name.strip_prefix("custom:") {
        let wgsl = ShaderManager::prepare_custom_transition(custom)?;
        return Ok(TransitionOverride::Asset(TransitionAsset::Shader(
            Arc::new(PreparedShader::new(custom.to_owned(), wgsl)),
        )));
    }
    let requested = Transition::from_name(name);
    anyhow::ensure!(
        !matches!(requested, Transition::Fade) || name.eq_ignore_ascii_case("fade"),
        "Unknown built-in transition: {name}"
    );
    match requested {
        Transition::Random => Ok(TransitionOverride::Named(
            ShaderManager::pick_random_transition(),
        )),
        named => {
            ShaderManager::get_builtin_shader(&named)?;
            Ok(TransitionOverride::Named(named))
        }
    }
}
