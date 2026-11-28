//! Serializes GL semaphore handoffs and Vulkan external queue-family ownership.

use ash::vk;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use tracing::info;

/// Pair of reusable binary semaphores for one shared texture slot.
///
/// GL signals `gl_to_vulkan` after rendering. Vulkan waits before WGPU
/// samples, then signals `vulkan_to_gl` after sampling completes so GL can
/// safely reuse the slot.
pub(crate) struct GlInteropSync {
    device: ash::Device,
    context: std::sync::Weak<super::WgpuContext>,
    gl_to_vulkan: vk::Semaphore,
    vulkan_to_gl: vk::Semaphore,
    ownership: super::mpv_gl_ownership::OwnershipTransfers,
}

impl GlInteropSync {
    pub(crate) fn wait_for_gl_render(&self, ctx: &super::WgpuContext) -> anyhow::Result<()> {
        self.submit(
            ctx,
            Some(self.gl_to_vulkan),
            None,
            &[self.ownership.acquire],
        )
    }

    pub(crate) fn wait_for_gl_render_and_release(
        &self,
        ctx: &super::WgpuContext,
        previous: &Self,
    ) -> anyhow::Result<()> {
        self.submit(
            ctx,
            Some(self.gl_to_vulkan),
            Some(previous.vulkan_to_gl),
            &[previous.ownership.release, self.ownership.acquire],
        )
    }

    pub(crate) fn signal_gl_reuse(&self, ctx: &super::WgpuContext) -> anyhow::Result<()> {
        self.submit(
            ctx,
            None,
            Some(self.vulkan_to_gl),
            &[self.ownership.release],
        )
    }

    fn submit(
        &self,
        ctx: &super::WgpuContext,
        wait: Option<vk::Semaphore>,
        signal: Option<vk::Semaphore>,
        commands: &[vk::CommandBuffer],
    ) -> anyhow::Result<()> {
        ctx.with_raw_queue_lock(|| {
            // SAFETY: the raw queue belongs to ctx.device and access is
            // serialized with every WGPU queue operation by queue_lock.
            let submitted = unsafe {
                ctx.device
                    .as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device| {
                        let hal_device = hal_device
                            .ok_or_else(|| anyhow::anyhow!("WGPU device is not Vulkan"))?;
                        let wait_semaphores = wait.into_iter().collect::<Vec<_>>();
                        let wait_stages = wait_semaphores
                            .iter()
                            .map(|_| vk::PipelineStageFlags::ALL_COMMANDS)
                            .collect::<Vec<_>>();
                        let signal_semaphores = signal.into_iter().collect::<Vec<_>>();
                        let submit = vk::SubmitInfo::default()
                            .wait_semaphores(&wait_semaphores)
                            .wait_dst_stage_mask(&wait_stages)
                            .signal_semaphores(&signal_semaphores)
                            .command_buffers(commands);
                        self.device
                            .queue_submit(
                                hal_device.raw_queue(),
                                std::slice::from_ref(&submit),
                                vk::Fence::null(),
                            )
                            .map_err(|error| {
                                anyhow::anyhow!("Vulkan interop submit failed: {error:?}")
                            })
                    })
            };
            submitted.ok_or_else(|| anyhow::anyhow!("WGPU core Vulkan device is unavailable"))?
        })
    }
}

impl Drop for GlInteropSync {
    fn drop(&mut self) {
        let Some(context) = self.context.upgrade() else {
            return;
        };
        let device = self.device.clone();
        let ready = self.gl_to_vulkan;
        let release = self.vulkan_to_gl;
        let pool = self.ownership.pool;
        // Include the final raw signal in a WGPU completion point on the same
        // serialized queue. A global device-idle wait can freeze IPC while an
        // unrelated mpv core is waiting for its render thread to run.
        context.with_raw_queue_lock(|| {
            context.queue.submit(std::iter::empty());
            let device_owner = context.clone();
            context.queue.on_submitted_work_done(move || {
                // SAFETY: completion follows all uses of these semaphores.
                // GL imports own their payload and the WGPU device stays live.
                unsafe {
                    device.destroy_semaphore(ready, None);
                    device.destroy_semaphore(release, None);
                    device.destroy_command_pool(pool, None);
                }
                drop(device_owner);
            });
        });
        context.request_device_poll();
    }
}

pub(super) fn create_external_semaphores(
    instance: &ash::Instance,
    device: &ash::Device,
    context: &Arc<super::WgpuContext>,
    image: vk::Image,
    queue_family: u32,
) -> anyhow::Result<(Arc<GlInteropSync>, OwnedFd, OwnedFd)> {
    // SAFETY: both semaphores are created and exported from this live device.
    unsafe {
        let mut export = vk::ExportSemaphoreCreateInfo::default()
            .handle_types(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD);
        let create_info = vk::SemaphoreCreateInfo::default().push_next(&mut export);
        let gl_to_vulkan = device
            .create_semaphore(&create_info, None)
            .map_err(|error| anyhow::anyhow!("creating GL-to-Vulkan semaphore: {error:?}"))?;
        let vulkan_to_gl = match device.create_semaphore(&create_info, None) {
            Ok(semaphore) => semaphore,
            Err(error) => {
                device.destroy_semaphore(gl_to_vulkan, None);
                return Err(anyhow::anyhow!(
                    "creating Vulkan-to-GL semaphore: {error:?}"
                ));
            }
        };
        let external = ash::khr::external_semaphore_fd::Device::new(instance, device);
        let export_fd = |semaphore| {
            external
                .get_semaphore_fd(
                    &vk::SemaphoreGetFdInfoKHR::default()
                        .semaphore(semaphore)
                        .handle_type(vk::ExternalSemaphoreHandleTypeFlags::OPAQUE_FD),
                )
                .map(|fd| OwnedFd::from_raw_fd(fd))
        };
        let gl_to_vulkan_fd = match export_fd(gl_to_vulkan) {
            Ok(fd) => fd,
            Err(error) => {
                device.destroy_semaphore(gl_to_vulkan, None);
                device.destroy_semaphore(vulkan_to_gl, None);
                return Err(anyhow::anyhow!(
                    "exporting GL-to-Vulkan semaphore: {error:?}"
                ));
            }
        };
        let vulkan_to_gl_fd = match export_fd(vulkan_to_gl) {
            Ok(fd) => fd,
            Err(error) => {
                device.destroy_semaphore(gl_to_vulkan, None);
                device.destroy_semaphore(vulkan_to_gl, None);
                return Err(anyhow::anyhow!(
                    "exporting Vulkan-to-GL semaphore: {error:?}"
                ));
            }
        };
        let ownership =
            match super::mpv_gl_ownership::OwnershipTransfers::new(device, image, queue_family) {
                Ok(ownership) => ownership,
                Err(error) => {
                    device.destroy_semaphore(gl_to_vulkan, None);
                    device.destroy_semaphore(vulkan_to_gl, None);
                    return Err(error);
                }
            };
        info!("[MPV-GL] Created bidirectional external semaphore pair");
        Ok((
            Arc::new(GlInteropSync {
                device: device.clone(),
                context: Arc::downgrade(context),
                gl_to_vulkan,
                vulkan_to_gl,
                ownership,
            }),
            gl_to_vulkan_fd,
            vulkan_to_gl_fd,
        ))
    }
}
