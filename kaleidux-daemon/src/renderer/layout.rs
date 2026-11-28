//! Computes a bounded content rectangle shared by Wayland and X11 presentation.
//! Margins use physical pixels in top, left, bottom, right order.

pub(super) fn content_rect(width: u32, height: u32, margin: [u32; 4]) -> (u32, u32, u32, u32) {
    let [top, left, bottom, right] = margin;
    let x = left.min(width.saturating_sub(1));
    let y = top.min(height.saturating_sub(1));
    let content_width = width.saturating_sub(x).saturating_sub(right).max(1);
    let content_height = height.saturating_sub(y).saturating_sub(bottom).max(1);
    (x, y, content_width, content_height)
}

impl super::Renderer {
    pub(crate) fn content_size(&self) -> (u32, u32) {
        let (_, _, width, height) =
            content_rect(self.config.width, self.config.height, self.margin);
        (width, height)
    }

    pub(super) fn layout_uniform(&self) -> [f32; 4] {
        let (width, height) = self.content_size();
        [
            width as f32,
            height as f32,
            f32::from(self.prev_tiled_image),
            f32::from(self.tiled_image),
        ]
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn margin_order_and_oversized_insets_stay_inside_the_surface() {
        assert_eq!(
            super::content_rect(1920, 1080, [40, 20, 20, 20]),
            (20, 40, 1880, 1020)
        );
        assert_eq!(super::content_rect(10, 5, [99; 4]), (9, 4, 1, 1));
    }
}
