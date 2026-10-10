use wayland_client::{Connection, Dispatch, QueueHandle};
use wayland_protocols::wp::fractional_scale::v1::client::wp_fractional_scale_v1::{
    self, WpFractionalScaleV1,
};

impl Dispatch<WpFractionalScaleV1, String> for super::WaylandBackend {
    fn event(
        state: &mut Self,
        _proxy: &WpFractionalScaleV1,
        event: wp_fractional_scale_v1::Event,
        output: &String,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        let wp_fractional_scale_v1::Event::PreferredScale { scale } = event else {
            return;
        };
        let scale = scale.max(1);
        if state
            .preferred_fractional_scales
            .insert(output.clone(), scale)
            == Some(scale)
        {
            return;
        }
        tracing::info!(
            "[WAYLAND-SCALE] output={} preferred={}/120 ({:.3}x), buffer_scale=1",
            output,
            scale,
            scale as f64 / 120.0,
        );
        if let Some(&(logical_width, logical_height)) = state.logical_surface_sizes.get(output) {
            state.queue_fractional_resize(output, logical_width, logical_height, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn fractional_buffer_extent_rounds_up_to_cover_logical_size() {
        assert_eq!(super::super::fractional_buffer_extent(1536, 150), 1920);
        assert_eq!(super::super::fractional_buffer_extent(1, 180), 2);
        assert_eq!(super::super::fractional_buffer_extent(100, 120), 100);
        assert_eq!(super::super::fractional_buffer_extent(1, 121), 2);
    }
}
