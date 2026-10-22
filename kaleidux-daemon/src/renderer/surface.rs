pub(crate) fn select_present_mode(present_modes: &[wgpu::PresentMode]) -> wgpu::PresentMode {
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
    fn present_mode_prefers_fifo_when_available() {
        let selected = select_present_mode(&[wgpu::PresentMode::Mailbox, wgpu::PresentMode::Fifo]);
        assert_eq!(selected, wgpu::PresentMode::Fifo);
    }

    #[test]
    fn present_mode_falls_back_to_first_supported_mode() {
        let selected = select_present_mode(&[wgpu::PresentMode::Mailbox]);
        assert_eq!(selected, wgpu::PresentMode::Mailbox);
    }
}
