use super::{CachedSource, CopyMode};
use ash::vk;
use std::sync::Arc;

pub(super) struct CommandPool {
    pub(super) handle: vk::CommandPool,
    pub(super) context: Arc<crate::renderer::WgpuContext>,
    pub(super) device: ash::Device,
    pub(super) access: parking_lot::Mutex<()>,
}

impl Drop for CommandPool {
    fn drop(&mut self) {
        // SAFETY: each pending retirement retains this pool. Its last reference
        // disappears only after the last submitted command buffer completes.
        unsafe {
            self.device.destroy_command_pool(self.handle, None);
        }
    }
}

pub(super) fn retire(pool: Arc<CommandPool>, sources: Vec<CachedSource>) {
    if sources.is_empty() {
        return;
    }
    let context = pool.context.clone();
    context.with_raw_queue_lock(|| {
        context.queue.submit(std::iter::empty());
        context.queue.on_submitted_work_done(move || {
            for mut source in sources {
                source.owner = None;
                // SAFETY: completion covers all raw copies on this serialized
                // queue. The pool retains the logical device through cleanup.
                unsafe {
                    pool.device.destroy_fence(source.fence, None);
                    pool.device
                        .destroy_semaphore(source.acquire_semaphore, None);
                    pool.device
                        .destroy_semaphore(source.producer_semaphore, None);
                    let _pool_access = pool.access.lock();
                    pool.device
                        .free_command_buffers(pool.handle, &[source.command_buffer]);
                }
                drop(source);
            }
        });
    });
    context.request_device_poll();
}

impl CachedSource {
    pub(super) fn ensure_syncobj_timelines(
        &mut self,
        device: &Arc<crate::video::drm_syncobj::DrmSyncobjDevice>,
    ) -> anyhow::Result<()> {
        if self.syncobj_timelines.is_none() {
            self.syncobj_timelines = Some((device.create_timeline()?, device.create_timeline()?));
        }
        Ok(())
    }

    pub(super) fn select_copy_mode(&mut self, mode: CopyMode) {
        if self.copy_mode != Some(mode) {
            self.copy_mode = Some(mode);
            self.reusable_command = false;
        }
    }

    pub(super) fn try_reset(
        &mut self,
        device: &ash::Device,
        nonblocking: bool,
    ) -> anyhow::Result<()> {
        unsafe {
            if self.has_submitted {
                if nonblocking {
                    anyhow::ensure!(
                        device.get_fence_status(self.fence).unwrap_or(false),
                        "previous linear bridge producer copy is still in flight"
                    );
                } else {
                    device
                        .wait_for_fences(std::slice::from_ref(&self.fence), true, u64::MAX)
                        .map_err(|error| {
                            anyhow::anyhow!("waiting for DMA-BUF surface: {error:?}")
                        })?;
                }
            }
            device
                .reset_fences(std::slice::from_ref(&self.fence))
                .map_err(|error| anyhow::anyhow!("resetting DMA-BUF copy fence: {error:?}"))?;
        }
        self.owner = None;
        Ok(())
    }
}
