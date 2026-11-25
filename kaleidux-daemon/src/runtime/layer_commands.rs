//! Installs prepared layer stacks atomically on selected live renderers.

use super::prepared_command::PreparedCommand;
use crate::background::{BackgroundWorkKind, spawn_blocking_tracked_wait};
use crate::main_loop::{CmdMsg, CommandContext, CommandSender, MainLoopContext};
use crate::monitor_manager::MonitorManager;
use crate::renderer::PreparedLayers;
use crate::renderer::layer_pipeline::LayerPipelines;
use crate::renderer::{Renderer, WgpuContext};
use kaleidux_common::{LayerInputs, LayerStage, Request, Response};
use std::collections::HashMap;
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
    pub(super) fn install(
        self,
        ctx: &mut CommandContext<'_>,
        rule_ticket: Option<u64>,
        group: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(ticket) = rule_ticket
            && self.targets.iter().any(|(target, _)| {
                let source = ctx
                    .monitor_manager
                    .outputs
                    .get(&target.name)
                    .and_then(|output| output.current_path.as_deref());
                let is_current = ctx
                    .renderers
                    .get(&target.name)
                    .is_some_and(|renderer| renderer.layer_rule_matches(ticket, source));
                !is_current
            })
        {
            return Ok(());
        }
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
            if self.stage == LayerStage::Morph
                && let Some(stack) = pipeline
            {
                renderer.check_morph_budget(stack, rule_ticket.is_some(), group)?;
            }
        }
        anyhow::ensure!(
            self.stage != LayerStage::Wallpaper || ctx.preview.is_none() || rule_ticket.is_some(),
            "End the current preview before setting a shader wallpaper"
        );
        for (target, pipeline) in self.targets {
            let starts_wallpaper = self.stage == LayerStage::Wallpaper && pipeline.is_some();
            if self.stage == LayerStage::Wallpaper {
                super::commands::clear_media(ctx, &target.name);
                if let Some(output) = ctx.monitor_manager.outputs.get_mut(&target.name) {
                    output.current_path = pipeline
                        .as_ref()
                        .and_then(|stack| stack.source.as_ref())
                        .map(std::path::PathBuf::from);
                    output.display_start_time = None;
                    output.next_change = None;
                }
            }
            ctx.renderers
                .get_mut(&target.name)
                .expect("validated output is live")
                .replace_layers(self.stage, pipeline, rule_ticket.is_some(), group);
            if starts_wallpaper && ctx.monitor_manager.has_slideshow_queue(&target.name) {
                ctx.monitor_manager.mark_transition_completed(&target.name);
            }
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

impl LayerCompileJob {
    fn is_current_rule(&self, renderers: &HashMap<String, Renderer>) -> bool {
        self.command.layer_rule_ticket.is_some_and(|ticket| {
            self.targets.iter().all(|target| {
                renderers
                    .get(&target.name)
                    .is_some_and(|renderer| renderer.layer_rule_ticket() == ticket)
            })
        })
    }

    fn same_rule_stage(&self, other: &Self) -> bool {
        self.prepared.stage() == other.prepared.stage()
            && self.targets.first().map(|target| &target.name)
                == other.targets.first().map(|target| &target.name)
    }
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
            let task = spawn_blocking_tracked_wait(BackgroundWorkKind::RendererInit, move || {
                runtime.block_on(compile_layers(prepared, targets))
            })
            .await;
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
        &mut self,
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
                if job.command.layer_rule_ticket.is_some() && !job.is_current_rule(&self.renderers)
                {
                    return None;
                }
                match self.layer_compile_queue.try_send(job) {
                    Err(mpsc::error::TrySendError::Full(job))
                        if job.command.layer_rule_ticket.is_some() =>
                    {
                        self.deferred_layer_rules
                            .retain(|pending| !pending.same_rule_stage(&job));
                        self.deferred_layer_rules.push_back(job);
                    }
                    Err(error) => respond_error(
                        error.into_inner().response,
                        anyhow::anyhow!("Layer compiler queue is busy; try again shortly"),
                    ),
                    Ok(()) => {}
                }
            }
            Err(error) => respond_error(response, error),
        }
        None
    }

    pub(super) fn drain_deferred_layer_rules(&mut self) {
        self.deferred_layer_rules
            .retain(|job| job.is_current_rule(&self.renderers));
        while !self.deferred_layer_rules.is_empty()
            && let Ok(permit) = self.layer_compile_queue.try_reserve()
        {
            permit.send(
                self.deferred_layer_rules
                    .pop_front()
                    .expect("checked for pending rules"),
            );
        }
    }

    fn layer_targets(
        &self,
        requested: Option<String>,
        prepared: &PreparedLayers,
    ) -> anyhow::Result<Vec<LayerTarget>> {
        let names = targets(&self.monitor_manager, &self.renderers, requested)?;
        anyhow::ensure!(
            prepared.is_empty()
                || names.iter().all(|name| !self
                    .video_players
                    .get(name)
                    .is_some_and(crate::video::VideoPlayer::renders_natively)),
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

pub(super) fn input(
    ctx: &mut CommandContext<'_>,
    mut inputs: LayerInputs,
) -> anyhow::Result<Response> {
    let normalized = |value: &f32| value.is_finite() && (0.0..=1.0).contains(value);
    anyhow::ensure!(
        inputs.pointer.iter().flatten().all(normalized),
        "Pointer values must be in 0..=1"
    );
    if let Some(bands) = &inputs.audio {
        anyhow::ensure!(
            bands.len() <= 64 && bands.iter().all(normalized),
            "Use at most 64 audio values in 0..=1"
        );
    }
    if let Some(values) = &inputs.controls {
        anyhow::ensure!(
            values.len() <= 32 && values.iter().all(|value| value.is_finite()),
            "Use at most 32 finite shader controls"
        );
    }
    anyhow::ensure!(
        inputs
            .data
            .iter()
            .flatten()
            .chain(inputs.date.iter().flatten())
            .all(|value| value.is_finite()),
        "Shader data values must be finite"
    );
    let outputs = targets(ctx.monitor_manager, ctx.renderers, inputs.output.take())?;
    for name in outputs {
        if let Some(renderer) = ctx.renderers.get_mut(&name) {
            renderer.apply_layer_inputs(&inputs);
        }
        if let Some(player) = ctx.video_players.get(&name) {
            player.apply_web_inputs(&inputs);
        }
    }
    Ok(Response::Ok)
}

pub(super) fn love(ctx: &mut CommandContext<'_>, path: String, multiplier: f32) -> Response {
    super::love_button::set(ctx.monitor_manager, ctx.renderers, path, multiplier)
        .map(|()| Response::Ok)
        .unwrap_or_else(|error| Response::Error(error.to_string()))
}

pub(super) fn love_current(
    ctx: &mut CommandContext<'_>,
    output: Option<String>,
    multiplier: f32,
) -> anyhow::Result<Response> {
    let names = targets(ctx.monitor_manager, ctx.renderers, output)?;
    anyhow::ensure!(names.len() == 1, "Select a single output");
    let source = ctx
        .monitor_manager
        .outputs
        .get(&names[0])
        .and_then(|output| output.current_path.as_deref())
        .ok_or_else(|| anyhow::anyhow!("Output has no wallpaper"))?;
    Ok(love(ctx, source.to_string_lossy().into_owned(), multiplier))
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
    ctx.video_players
        .get(name)
        .is_some_and(crate::video::VideoPlayer::renders_natively)
        || (ctx.pending_video_switches.contains_key(name)
            && ctx
                .mpv_native_targets
                .is_some_and(|targets| targets.contains_key(name))
            && ctx
                .monitor_manager
                .get_output_config(name)
                .is_none_or(|config| config.smooth_loop.is_none()))
}
