//! Selects wallpaper-specific stacks and rejects worker results for old selections.

use super::prepared_command::prepare_command;
use crate::main_loop::MainLoopContext;
use kaleidux_common::{LayerSelection, LayerStage, Request};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct LayerRule {
    pub(crate) path: PathBuf,
    shaders: Option<Vec<String>>,
    morphs: Option<Vec<String>>,
    #[serde(default)]
    fps: u16,
}

pub(crate) fn requires_composition(rules: &[LayerRule], path: &Path) -> bool {
    [LayerStage::Filter, LayerStage::Morph]
        .into_iter()
        .any(|stage| {
            find_rule_stack(rules, path, stage).is_some_and(|(_, layers)| !layers.is_empty())
        })
}

impl MainLoopContext {
    pub(crate) fn sync_layer_rules(&mut self) {
        let rules = self.monitor_manager.layer_rules();
        for (name, renderer) in &mut self.renderers {
            let source = self
                .monitor_manager
                .outputs
                .get(name)
                .and_then(|output| output.current_path.as_deref());
            let procedural = source.is_some_and(|path| {
                crate::renderer::is_shader_file(path) || crate::scene::is_scene(path)
            });
            let key = source.filter(|_| procedural || !rules.is_empty());
            let Some(ticket) = renderer.prepare_layer_rule(key) else {
                continue;
            };
            let Some(path) = key else {
                continue;
            };
            let shader_already_selected = renderer
                .shader_wallpaper()
                .is_some_and(|source| Path::new(source) == path);
            let wall = (procedural && !shader_already_selected).then(|| LayerSelection {
                output: Some(name.clone()),
                stage: LayerStage::Wallpaper,
                layers: vec![path.to_string_lossy().into_owned()],
                fps: kaleidux_common::LAYER_FPS_UNLIMITED,
                group: None,
                layout: None,
            });
            let matched = [LayerStage::Filter, LayerStage::Morph]
                .into_iter()
                .filter_map(|stage| find_rule(rules, path, name, stage));
            for selection in wall.into_iter().chain(matched) {
                let sender = self.rule_command_sender.clone();
                tokio::spawn(async move {
                    match prepare_command(Request::LayersSet(selection)).await {
                        Ok(mut command) => {
                            command.layer_rule_ticket = Some(ticket);
                            let (reply, receive) = oneshot::channel();
                            if sender.send((command, reply)).await.is_ok()
                                && let Ok(kaleidux_common::Response::Error(error)) = receive.await
                            {
                                tracing::warn!(
                                    "[SHADER] Wallpaper recipe was not installed: {error}"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!("[SHADER] Wallpaper recipe ignored: {error:#}")
                        }
                    }
                });
            }
        }
        self.drain_deferred_layer_rules();
    }
}

fn find_rule(
    rules: &[LayerRule],
    source: &Path,
    output: &str,
    stage: LayerStage,
) -> Option<LayerSelection> {
    find_rule_stack(rules, source, stage).map(|(rule, layers)| LayerSelection {
        output: Some(output.to_owned()),
        stage,
        layers: layers.clone(),
        fps: rule.fps,
        group: None,
        layout: None,
    })
}

fn find_rule_stack<'a>(
    rules: &'a [LayerRule],
    source: &Path,
    stage: LayerStage,
) -> Option<(&'a LayerRule, &'a Vec<String>)> {
    rules
        .iter()
        .filter(|rule| source.starts_with(&rule.path))
        .filter_map(|rule| {
            let names = match stage {
                LayerStage::Filter => rule.shaders.as_ref(),
                LayerStage::Morph => rule.morphs.as_ref(),
                LayerStage::Wallpaper => None,
            }?;
            Some((rule, names))
        })
        .max_by_key(|(rule, _)| rule.path.components().count())
}
