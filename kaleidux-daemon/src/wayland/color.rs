use std::collections::HashSet;

use wayland_client::{Connection, Dispatch, QueueHandle, WEnum};
use wayland_protocols::wp::color_management::v1::client::{
    wp_color_manager_v1::{self, WpColorManagerV1},
    wp_image_description_v1::{self, WpImageDescriptionV1},
};

#[derive(Debug, Default)]
pub(crate) struct Capabilities {
    pub done: bool,
    pub intents: HashSet<u32>,
    pub features: HashSet<u32>,
    pub transfer_functions: HashSet<u32>,
    pub primaries: HashSet<u32>,
}

impl Capabilities {
    pub fn supports_native_bt709(&self) -> bool {
        // Perceptual intent=0, parametric feature=1, BT.709/sRGB
        // primaries=1, and BT.1886 transfer=1. The native direct surface
        // carries SDR video rather than the WGPU surface's sRGB pixels.
        self.done
            && self.intents.contains(&0)
            && self.features.contains(&1)
            && self.primaries.contains(&1)
            && self.transfer_functions.contains(&1)
    }

    pub fn supports_hdr_metadata(&self) -> bool {
        self.done
            && self.features.contains(&1)
            && self.primaries.contains(&6)
            && (self.transfer_functions.contains(&11) || self.transfer_functions.contains(&13))
    }

    pub fn summary(&self) -> String {
        format!(
            "done={} intents={:?} features={:?} transfer_functions={:?} primaries={:?} native_bt709={} hdr_metadata={}",
            self.done,
            self.intents,
            self.features,
            self.transfer_functions,
            self.primaries,
            self.supports_native_bt709(),
            self.supports_hdr_metadata(),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DescriptionData {
    pub output: String,
    pub policy: &'static str,
}

impl Dispatch<WpColorManagerV1, ()> for super::WaylandBackend {
    fn event(
        state: &mut Self,
        _proxy: &WpColorManagerV1,
        event: wp_color_manager_v1::Event,
        _data: &(),
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        match event {
            wp_color_manager_v1::Event::SupportedIntent { render_intent } => {
                let value = match render_intent {
                    WEnum::Value(value) => value as u32,
                    WEnum::Unknown(value) => value,
                };
                state.color_capabilities.intents.insert(value);
            }
            wp_color_manager_v1::Event::SupportedFeature { feature } => {
                let value = match feature {
                    WEnum::Value(value) => value as u32,
                    WEnum::Unknown(value) => value,
                };
                state.color_capabilities.features.insert(value);
            }
            wp_color_manager_v1::Event::SupportedTfNamed { tf } => {
                let value = match tf {
                    WEnum::Value(value) => value as u32,
                    WEnum::Unknown(value) => value,
                };
                state.color_capabilities.transfer_functions.insert(value);
            }
            wp_color_manager_v1::Event::SupportedPrimariesNamed { primaries } => {
                let value = match primaries {
                    WEnum::Value(value) => value as u32,
                    WEnum::Unknown(value) => value,
                };
                state.color_capabilities.primaries.insert(value);
            }
            wp_color_manager_v1::Event::Done => {
                state.color_capabilities.done = true;
                tracing::info!(
                    "[WAYLAND-COLOR] capabilities {}",
                    state.color_capabilities.summary()
                );
            }
            _ => {}
        }
    }
}

impl Dispatch<WpImageDescriptionV1, DescriptionData> for super::WaylandBackend {
    fn event(
        state: &mut Self,
        description: &WpImageDescriptionV1,
        event: wp_image_description_v1::Event,
        data: &DescriptionData,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        match event {
            wp_image_description_v1::Event::Ready { identity } => {
                state.apply_ready_color_description(description, data, u64::from(identity));
            }
            wp_image_description_v1::Event::Ready2 {
                identity_hi,
                identity_lo,
            } => {
                let identity = (u64::from(identity_hi) << 32) | u64::from(identity_lo);
                state.apply_ready_color_description(description, data, identity);
            }
            wp_image_description_v1::Event::Failed { cause, msg } => {
                tracing::warn!(
                    "[WAYLAND-COLOR] output={} policy={} description failed cause={cause:?}: {msg}; retaining explicit shader SDR fallback",
                    data.output,
                    data.policy,
                );
                description.destroy();
            }
            _ => {}
        }
    }
}

impl super::WaylandBackend {
    pub(crate) fn configure_color_surface(
        &mut self,
        name: &str,
        surface: &wayland_client::protocol::wl_surface::WlSurface,
        qh: &QueueHandle<Self>,
    ) {
        let Some(manager) = self
            .color_manager
            .as_ref()
            .and_then(|global| global.get().ok())
            .cloned()
        else {
            return;
        };
        let color_surface = manager.get_surface(surface, qh, ());
        self.color_surfaces.insert(name.to_owned(), color_surface);

        if !self.color_capabilities.supports_native_bt709() {
            tracing::info!(
                "[WAYLAND-COLOR] output={} compositor cannot describe native BT.709/BT.1886 SDR; leaving the direct surface description unset",
                name
            );
            return;
        }

        let creator = manager.create_parametric_creator(qh, ());
        creator.set_tf_named(wp_color_manager_v1::TransferFunction::Bt1886);
        creator.set_primaries_named(wp_color_manager_v1::Primaries::Srgb);
        let description = creator.create(
            qh,
            DescriptionData {
                output: name.to_owned(),
                policy: "native-bt709-bt1886-sdr",
            },
        );
        self.pending_color_descriptions
            .insert(name.to_owned(), description);
    }

    pub(crate) fn apply_ready_color_description(
        &mut self,
        description: &WpImageDescriptionV1,
        data: &DescriptionData,
        identity: u64,
    ) {
        let Some(surface) = self.color_surfaces.get(&data.output) else {
            description.destroy();
            return;
        };
        surface.set_image_description(description, wp_color_manager_v1::RenderIntent::Perceptual);
        tracing::info!(
            "[WAYLAND-COLOR] output={} policy={} image_description={} ready; composed HDR remains tone-mapped to SDR in the single final shader",
            data.output,
            data.policy,
            identity,
        );
        description.destroy();
        self.pending_color_descriptions.remove(&data.output);
    }

    pub(crate) fn retire_color_surface(&mut self, name: &str) {
        if let Some(description) = self.pending_color_descriptions.remove(name) {
            description.destroy();
        }
        if let Some(surface) = self.color_surfaces.remove(name) {
            surface.destroy();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Capabilities;

    #[test]
    fn color_capability_policy_requires_every_advertised_component() {
        let mut caps = Capabilities {
            done: true,
            intents: std::collections::HashSet::from([0]),
            features: std::collections::HashSet::from([1]),
            primaries: std::collections::HashSet::from([1]),
            ..Capabilities::default()
        };
        assert!(!caps.supports_native_bt709());
        caps.transfer_functions.insert(1);
        assert!(caps.supports_native_bt709());
    }
}
