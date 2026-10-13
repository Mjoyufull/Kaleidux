//! Bounds imported surfaces and retires evictions after serialized queue completion.

use super::{CachedSource, NativeDmaBufInterop, import, retirement};
use crate::video::NativeDmaBufNv12;
use ash::vk;
use std::sync::atomic::Ordering;

const MAX_SOURCES: usize = 24;

impl NativeDmaBufInterop {
    pub(super) fn prepare_source_slot(&mut self, key: u64) -> anyhow::Result<()> {
        self.source_sequence = self.source_sequence.wrapping_add(1);
        if let Some(source) = self.sources.get_mut(&key) {
            source.last_used = self.source_sequence;
            return Ok(());
        }
        // A wedged GPU must not turn bounded cache eviction into an unbounded
        // list of callbacks retaining images, memory, semaphores and AVFrames.
        anyhow::ensure!(
            self.command_pool.pending_sources.load(Ordering::Acquire) < MAX_SOURCES,
            "DMA-BUF import retirement is still pending"
        );
        if self.sources.len() >= MAX_SOURCES {
            let oldest = self
                .sources
                .iter()
                .min_by_key(|(_, source)| source.last_used)
                .map(|(key, _)| *key)
                .expect("full source cache");
            let source = self.sources.remove(&oldest).expect("cached source");
            retirement::retire(self.command_pool.clone(), vec![source]);
        }
        Ok(())
    }

    pub(super) fn import_cached_source(
        &self,
        descriptor: &NativeDmaBufNv12,
        width: u32,
        height: u32,
    ) -> anyhow::Result<CachedSource> {
        let imported = import::import_source(
            &self.device,
            &self.external_memory_fd,
            descriptor,
            width,
            height,
        )?;
        let (command_buffer, fence, acquire_semaphore, producer_semaphore) =
            self.allocate_copy_handles()?;
        Ok(CachedSource {
            imported,
            last_used: self.source_sequence,
            staging_destinations: None,
            command_buffer,
            fence,
            acquire_semaphore,
            producer_semaphore,
            owner: None,
            has_submitted: false,
            fence_in_flight: false,
            reusable_command: false,
            copy_mode: None,
            linear_bridge: None,
            syncobj_timelines: None,
            next_sync_point: 1,
            last_release_point: None,
        })
    }

    fn allocate_copy_handles(
        &self,
    ) -> anyhow::Result<(vk::CommandBuffer, vk::Fence, vk::Semaphore, vk::Semaphore)> {
        let _pool_access = self.command_pool.access.lock();
        let allocate_info = vk::CommandBufferAllocateInfo::default()
            .command_pool(self.command_pool.handle)
            .level(vk::CommandBufferLevel::PRIMARY)
            .command_buffer_count(1);
        // SAFETY: command_pool is live and retained by this object.
        let command_buffer = unsafe { self.device.allocate_command_buffers(&allocate_info) }
            .map_err(|error| anyhow::anyhow!("allocating surface command buffer: {error:?}"))?[0];
        let fence_info = vk::FenceCreateInfo::default().flags(vk::FenceCreateFlags::SIGNALED);
        // SAFETY: device is live. Free the buffer if fence allocation fails.
        let fence = match unsafe { self.device.create_fence(&fence_info, None) } {
            Ok(fence) => fence,
            Err(error) => {
                unsafe {
                    self.device
                        .free_command_buffers(self.command_pool.handle, &[command_buffer])
                };
                return Err(anyhow::anyhow!("creating surface copy fence: {error:?}"));
            }
        };
        let mut export = vk::ExportSemaphoreCreateInfo::default()
            .handle_types(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
        let semaphore_info = vk::SemaphoreCreateInfo::default().push_next(&mut export);
        let acquire_semaphore = match unsafe { self.device.create_semaphore(&semaphore_info, None) }
        {
            Ok(semaphore) => semaphore,
            Err(error) => {
                unsafe {
                    self.device.destroy_fence(fence, None);
                    self.device
                        .free_command_buffers(self.command_pool.handle, &[command_buffer]);
                }
                return Err(anyhow::anyhow!(
                    "creating linear bridge acquire semaphore: {error:?}"
                ));
            }
        };
        let producer_semaphore = match unsafe {
            self.device
                .create_semaphore(&vk::SemaphoreCreateInfo::default(), None)
        } {
            Ok(semaphore) => semaphore,
            Err(error) => {
                unsafe {
                    self.device.destroy_semaphore(acquire_semaphore, None);
                    self.device.destroy_fence(fence, None);
                    self.device
                        .free_command_buffers(self.command_pool.handle, &[command_buffer]);
                }
                return Err(anyhow::anyhow!(
                    "creating DMA-BUF producer semaphore: {error:?}"
                ));
            }
        };
        Ok((command_buffer, fence, acquire_semaphore, producer_semaphore))
    }

    pub(super) fn reap_completed(&mut self) {
        for source in self.sources.values_mut() {
            if source.owner.is_none() {
                continue;
            }
            // SAFETY: fence is live; success means the raw plane copy no longer
            // references the decoder-owned surface retained in owner.
            if unsafe { self.device.get_fence_status(source.fence) }.unwrap_or(false) {
                source.owner = None;
            }
        }
    }

    pub(super) fn clear_sources(&mut self) -> anyhow::Result<()> {
        retirement::retire(
            self.command_pool.clone(),
            self.sources.drain().map(|(_, source)| source).collect(),
        );
        Ok(())
    }
}
