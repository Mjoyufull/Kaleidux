pub(crate) fn select_present_mode(
    present_modes: &[wgpu::PresentMode],
    wayland: bool,
) -> wgpu::PresentMode {
    // Wayland can stop delivering FIFO callbacks while a DMA-BUF subsurface
    // covers the swapchain. Kaleidux already paces composition itself.
    if wayland && present_modes.contains(&wgpu::PresentMode::Mailbox) {
        return wgpu::PresentMode::Mailbox;
    }
    if present_modes.contains(&wgpu::PresentMode::Fifo) {
        wgpu::PresentMode::Fifo
    } else {
        present_modes
            .first()
            .copied()
            .unwrap_or(wgpu::PresentMode::Fifo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wayland_prefers_mailbox_when_available() {
        let selected =
            select_present_mode(&[wgpu::PresentMode::Fifo, wgpu::PresentMode::Mailbox], true);
        assert_eq!(selected, wgpu::PresentMode::Mailbox);
        assert_eq!(
            select_present_mode(&[wgpu::PresentMode::Fifo], true),
            wgpu::PresentMode::Fifo
        );
    }

    #[test]
    fn x11_prefers_fifo_when_available() {
        let selected = select_present_mode(
            &[wgpu::PresentMode::Mailbox, wgpu::PresentMode::Fifo],
            false,
        );
        assert_eq!(selected, wgpu::PresentMode::Fifo);
    }

    #[test]
    fn present_mode_falls_back_to_first_supported_mode() {
        let selected = select_present_mode(&[wgpu::PresentMode::Mailbox], false);
        assert_eq!(selected, wgpu::PresentMode::Mailbox);
    }
}
