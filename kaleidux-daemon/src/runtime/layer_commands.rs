//! Installs prepared layer stacks atomically on selected live renderers.

use super::prepared_command::PreparedCommand;
use crate::background::{BackgroundWorkKind, spawn_blocking_tracked};
use crate::main_loop::{CmdMsg, CommandContext, CommandSender, MainLoopContext};
use crate::monitor_manager::MonitorManager;
use crate::renderer::PreparedLayers;
use crate::renderer::layer_pipeline::LayerPipelines;
use crate::renderer::{Renderer, WgpuContext};
use kaleidux_common::{LayerStage, Request, Response};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub(super) struct CompiledLayers {
    stage: LayerStage,
    targets: Vec<(LayerTarget, Option<LayerPipelines>)>,
}

struct LayerTarget {
    name: String,
    gpu: Arc<WgpuContext>,
    format: wgpu::TextureFormat,
}

impl CompiledLayers {
    pub(super) fn install(self, ctx: &mut CommandContext<'_>) -> anyhow::Result<()> {
        for (target, pipeline) in &self.targets {
            let renderer = ctx.renderers.get(&target.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "Output {:?} disappeared while compiling layers",
                    target.name
                )
            })?;
            anyhow::ensure!(
                Arc::ptr_eq(&target.gpu, &renderer.ctx) && target.format == renderer.config.format,
                "Output {:?} changed while compiling layers",
                target.name
            );
            anyhow::ensure!(
                pipeline.is_none() || !is_overlay_target(ctx, &target.name),
                "Layers require composed playback; disable the libmpv GL overlay"
            );
        }
        for (target, pipeline) in self.targets {
            ctx.renderers
                .get_mut(&target.name)
                .expect("validated output is live")
                .replace_layers(self.stage, pipeline);
        }
        Ok(())
    }
}

pub(crate) struct LayerCompileJob {
    command: PreparedCommand,
    response: oneshot::Sender<Response>,
    prepared: PreparedLayers,
    targets: Vec<LayerTarget>,
}

/// The single compiler keeps per-device validation scopes ordered.
pub(crate) fn start_worker(command_sender: CommandSender) -> mpsc::Sender<LayerCompileJob> {
    let (sender, mut queue) = mpsc::channel::<LayerCompileJob>(4);
    tokio::spawn(async move {
        while let Some(job) = queue.recv().await {
            let LayerCompileJob {
                mut command,
                response,
                prepared,
                targets,
            } = job;
            if response.is_closed() {
                continue;
            }
            let runtime = tokio::runtime::Handle::current();
            let task = spawn_blocking_tracked(BackgroundWorkKind::RendererInit, move || {
                runtime.block_on(compile_layers(prepared, targets))
            });
            let Some(task) = task else {
                respond_error(
                    response,
                    anyhow::anyhow!("Renderer workers busy; try again shortly"),
                );
                continue;
            };
            match task.await {
                Ok(Ok(layers)) => {
                    command.compiled_layers = Some(layers);
                    if command_sender.send((command, response)).await.is_err() {
                        break;
                    }
                }
                Ok(Err(error)) => respond_error(response, error),
                Err(error) => respond_error(response, error.into()),
            }
        }
    });
    sender
}

impl MainLoopContext {
    pub(super) fn dispatch_layers(
        &self,
        mut command: PreparedCommand,
        response: oneshot::Sender<Response>,
    ) -> Option<CmdMsg> {
        let Request::LayersSet(selection) = &command.request else {
            return Some((command, response));
        };
        if command.compiled_layers.is_some() {
            return Some((command, response));
        }
        let Some(prepared) = command.prepared_layers.take() else {
            respond_error(response, anyhow::anyhow!("Layers were not prepared"));
            return None;
        };
        match self.layer_targets(selection.output.clone(), &prepared) {
            Ok(targets) => {
                let job = LayerCompileJob {
                    command,
                    response,
                    prepared,
                    targets,
                };
                if let Err(error) = self.layer_compile_queue.try_send(job) {
                    respond_error(
                        error.into_inner().response,
                        anyhow::anyhow!("Layer compiler queue is busy; try again shortly"),
                    );
                }
            }
            Err(error) => respond_error(response, error),
        }
        None
    }

    fn layer_targets(
        &self,
        requested: Option<String>,
        prepared: &PreparedLayers,
    ) -> anyhow::Result<Vec<LayerTarget>> {
        let names = targets(&self.monitor_manager, &self.renderers, requested)?;
        anyhow::ensure!(
            prepared.is_empty()
                || names
                    .iter()
                    .all(|name| !self.mpv_native_targets.contains_key(name)),
            "Layers require composed playback; disable the libmpv GL overlay"
        );
        let targets = names
            .into_iter()
            .map(|name| {
                let renderer = &self.renderers[&name];
                LayerTarget {
                    name,
                    gpu: renderer.ctx.clone(),
                    format: renderer.config.format,
                }
            })
            .collect();
        Ok(targets)
    }
}

async fn compile_layers(
    prepared: PreparedLayers,
    targets: Vec<LayerTarget>,
) -> anyhow::Result<CompiledLayers> {
    let mut compiled = Vec::with_capacity(targets.len());
    for target in targets {
        let pipeline = LayerPipelines::compile(&target.gpu, target.format, &prepared).await?;
        compiled.push((target, pipeline));
    }
    Ok(CompiledLayers {
        stage: prepared.stage(),
        targets: compiled,
    })
}

fn respond_error(response: oneshot::Sender<Response>, error: anyhow::Error) {
    let _ = response.send(Response::Error(format!("{error:#}")));
}

pub(super) fn query(
    ctx: &CommandContext<'_>,
    requested: Option<String>,
) -> anyhow::Result<Response> {
    let names = targets(ctx.monitor_manager, ctx.renderers, requested)?;
    let outputs = names
        .iter()
        .map(|name| ctx.renderers[name].output_layers())
        .collect();
    Ok(Response::Layers(outputs))
}

pub(super) fn love(ctx: &mut CommandContext<'_>, path: String, multiplier: f32) -> Response {
    let position = ctx.monitor_manager.love_heart_position();
    let selected = if position.is_some() && multiplier > 1.0 {
        ctx.monitor_manager
            .outputs
            .iter()
            .filter(|(name, output)| {
                output.current_path.as_deref() == Some(Path::new(&path))
                    && !is_overlay_target(ctx, name)
                    && !ctx.powered_off_outputs.contains(*name)
            })
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        Vec::new()
    };
    if let Err(err) = ctx.monitor_manager.love_file(path, multiplier) {
        return Response::Error(err.to_string());
    }
    if let Some(position) = position
        && !ctx.display_power_suspended
    {
        for name in selected {
            if let Some(renderer) = ctx.renderers.get_mut(&name) {
                renderer.show_love(position);
            }
        }
    }
    Response::Ok
}

fn targets(
    monitor_manager: &MonitorManager,
    renderers: &HashMap<String, Renderer>,
    requested: Option<String>,
) -> anyhow::Result<Vec<String>> {
    let selected = monitor_manager.command_output(
        requested,
        renderers
            .iter()
            .map(|(name, renderer)| (name, renderer.config.width, renderer.config.height)),
    )?;
    if let Some(name) = selected {
        anyhow::ensure!(
            renderers.contains_key(&name),
            "Output {name:?} is not ready"
        );
        return Ok(vec![name]);
    }
    let mut names: Vec<_> = renderers.keys().cloned().collect();
    names.sort_unstable();
    Ok(names)
}

fn is_overlay_target(ctx: &CommandContext<'_>, name: &str) -> bool {
    ctx.mpv_native_targets
        .is_some_and(|targets| targets.contains_key(name))
}
