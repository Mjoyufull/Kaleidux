//! Retains optional map or compiler output for exactly one wallpaper switch.

use super::{Renderer, RevealMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum TransitionAsset {
    Map(Arc<RevealMap>),
    Shader(Arc<PreparedShader>),
}

#[derive(Debug)]
pub(crate) struct PreparedShader {
    pub(super) name: String,
    pub(super) cache_key: String,
    pub(super) wgsl: String,
}

impl PreparedShader {
    pub(crate) fn new(name: String, wgsl: String) -> Self {
        let mut hash = DefaultHasher::new();
        (&name, &wgsl).hash(&mut hash);
        let cache_key = format!("request:{name}:{:016x}", hash.finish());
        Self {
            name,
            cache_key,
            wgsl,
        }
    }
}

impl Renderer {
    pub(crate) fn set_transition_asset(&mut self, asset: Option<TransitionAsset>) {
        if self.transition_duration <= 0.0
            || (!self.has_current_texture() && self.native_wayland_snapshot_frame.is_none())
        {
            return;
        }
        match asset {
            Some(TransitionAsset::Map(map)) => self.set_reveal_map(&map),
            Some(TransitionAsset::Shader(shader)) => self.prepared_transition_shader = Some(shader),
            None => {}
        }
    }

    pub(super) fn clear_transition_asset(&mut self) {
        self.reveal_bind_group = None;
        self.prepared_transition_shader = None;
    }
}
