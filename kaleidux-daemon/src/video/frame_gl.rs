use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
pub struct GlExternalFrame {
    inner: Arc<GlExternalFrameInner>,
}

struct GlExternalFrameInner {
    view: Arc<wgpu::TextureView>,
    sync: Arc<crate::renderer::GlInteropSync>,
    wgpu_ctx: Arc<crate::renderer::WgpuContext>,
    slot_busy: Arc<AtomicBool>,
    acquired_for_wgpu: AtomicBool,
    release_scheduled: AtomicBool,
}

impl GlExternalFrame {
    #[cfg_attr(not(feature = "backend-mpv"), allow(dead_code))]
    pub(crate) fn new(
        view: Arc<wgpu::TextureView>,
        sync: Arc<crate::renderer::GlInteropSync>,
        wgpu_ctx: Arc<crate::renderer::WgpuContext>,
        slot_busy: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner: Arc::new(GlExternalFrameInner {
                view,
                sync,
                wgpu_ctx,
                slot_busy,
                acquired_for_wgpu: AtomicBool::new(false),
                release_scheduled: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn view(&self) -> Arc<wgpu::TextureView> {
        self.inner.view.clone()
    }

    pub(crate) fn prepare_for_wgpu_releasing(&self, previous: Option<&Self>) -> anyhow::Result<()> {
        if self.inner.acquired_for_wgpu.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let Some(previous) = previous else {
            return self.wait_for_gl_render();
        };
        if previous
            .inner
            .release_scheduled
            .swap(true, Ordering::AcqRel)
        {
            return self.wait_for_gl_render();
        }
        match self
            .inner
            .sync
            .wait_for_gl_render_and_release(&self.inner.wgpu_ctx, &previous.inner.sync)
        {
            Ok(()) => {
                previous.inner.slot_busy.store(false, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                previous
                    .inner
                    .release_scheduled
                    .store(false, Ordering::Release);
                self.inner.acquired_for_wgpu.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    fn wait_for_gl_render(&self) -> anyhow::Result<()> {
        let result = self.inner.sync.wait_for_gl_render(&self.inner.wgpu_ctx);
        if result.is_err() {
            self.inner.acquired_for_wgpu.store(false, Ordering::Release);
        }
        result
    }

    pub(crate) fn release_after_submit(&self) {
        if self.inner.release_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        // Every WGPU and raw Vulkan submit is serialized onto the same queue.
        // This signal-only submit therefore executes after the WGPU sampling
        // already submitted for this frame.
        match self.inner.sync.signal_gl_reuse(&self.inner.wgpu_ctx) {
            Ok(()) => self.inner.slot_busy.store(false, Ordering::Release),
            Err(error) => tracing::error!(
                "[MPV-GL] Failed to return shared texture ownership to GL: {error:#}"
            ),
        }
    }
}

impl std::fmt::Debug for GlExternalFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GlExternalFrame")
            .field(
                "acquired_for_wgpu",
                &self.inner.acquired_for_wgpu.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl Drop for GlExternalFrameInner {
    fn drop(&mut self) {
        if self.release_scheduled.load(Ordering::Acquire) {
            return;
        }
        if !self.acquired_for_wgpu.swap(true, Ordering::AcqRel)
            && let Err(error) = self.sync.wait_for_gl_render(&self.wgpu_ctx)
        {
            tracing::error!("[MPV-GL] Failed to consume an unpublished GL frame: {error:#}");
            return;
        }
        match self.sync.signal_gl_reuse(&self.wgpu_ctx) {
            Ok(()) => self.slot_busy.store(false, Ordering::Release),
            Err(error) => {
                tracing::error!("[MPV-GL] Failed to recycle an unpublished GL frame: {error:#}")
            }
        }
    }
}
