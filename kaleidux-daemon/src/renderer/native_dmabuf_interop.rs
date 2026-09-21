#[path = "native_dmabuf_interop/copy.rs"]
mod copy;
#[path = "native_dmabuf_interop/import.rs"]
mod import;
#[path = "native_dmabuf_interop/linear.rs"]
mod linear;

use crate::video::{NativeDmaBufNv12, VideoFrameStorage};
use ash::vk;
use std::collections::HashMap;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use tracing::{debug, info};

struct CachedSource {
    imported: import::ImportedSource,
    command_buffer: vk::CommandBuffer,
    fence: vk::Fence,
    acquire_semaphore: vk::Semaphore,
    producer_semaphore: vk::Semaphore,
    owner: Option<VideoFrameStorage>,
    has_submitted: bool,
    reusable_command: bool,
    copy_mode: Option<CopyMode>,
    linear_bridge: Option<linear::LinearBridge>,
    syncobj_timelines: Option<(
        crate::video::drm_syncobj::DrmSyncobjTimeline,
        crate::video::drm_syncobj::DrmSyncobjTimeline,
    )>,
    next_sync_point: u64,
    last_release_point: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CopyMode {
    WgpuStaging,
    LinearWayland,
}

pub(super) struct NativeDmaBufInterop {
    device: ash::Device,
    external_memory_fd: ash::khr::external_memory_fd::Device,
    external_semaphore_fd: ash::khr::external_semaphore_fd::Device,
    queue: vk::Queue,
    queue_family_index: u32,
    command_pool: vk::CommandPool,
    drm_syncobj_device: Option<Arc<crate::video::drm_syncobj::DrmSyncobjDevice>>,
    sources: HashMap<u64, CachedSource>,
    active_session_id: Option<u64>,
}

impl NativeDmaBufInterop {
    pub(super) fn new(ctx: &super::WgpuContext) -> anyhow::Result<Self> {
        // SAFETY: all copied handles remain owned by the live WGPU context.
        let raw = unsafe {
            ctx.device
                .as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_device| {
                    let hal_device =
                        hal_device.ok_or_else(|| anyhow::anyhow!("WGPU is not using Vulkan"))?;
                    let instance = hal_device.shared_instance().raw_instance().clone();
                    let device = hal_device.raw_device().clone();
                    Ok::<_, anyhow::Error>((
                        ash::khr::external_memory_fd::Device::new(&instance, &device),
                        ash::khr::external_semaphore_fd::Device::new(&instance, &device),
                        device,
                        hal_device.raw_queue(),
                        hal_device.queue_family_index(),
                    ))
                })
                .ok_or_else(|| anyhow::anyhow!("WGPU core Vulkan device is unavailable"))??
        };
        let (external_memory_fd, external_semaphore_fd, device, queue, queue_family_index) = raw;

        let command_pool_info = vk::CommandPoolCreateInfo::default()
            .flags(vk::CommandPoolCreateFlags::RESET_COMMAND_BUFFER)
            .queue_family_index(queue_family_index);
        // SAFETY: the queue family belongs to this live logical device.
        let command_pool = unsafe { device.create_command_pool(&command_pool_info, None) }
            .map_err(|error| anyhow::anyhow!("creating native DMA-BUF command pool: {error:?}"))?;
        info!(
            "[NATIVE-IMPORT] Vulkan DMA-BUF single-GPU-copy engine ready queue_family={} command_cache=per-surface",
            queue_family_index
        );
        let drm_syncobj_device =
            match crate::video::drm_syncobj::DrmSyncobjDevice::open_render_node() {
                Ok(device) => {
                    info!(
                        "[NATIVE-SYNC] DRM timeline syncobj support ready on {}",
                        device.path()
                    );
                    Some(device)
                }
                Err(error) => {
                    debug!("[NATIVE-SYNC] DRM timeline syncobj unavailable: {error:#}");
                    None
                }
            };
        Ok(Self {
            device,
            external_memory_fd,
            external_semaphore_fd,
            queue,
            queue_family_index,
            command_pool,
            drm_syncobj_device,
            sources: HashMap::new(),
            active_session_id: None,
        })
    }

    pub(super) fn copy_nv12_to_staging(
        &mut self,
        ctx: &super::WgpuContext,
        descriptor: &NativeDmaBufNv12,
        session_id: u64,
        owner: VideoFrameStorage,
        y_destination: &wgpu::Texture,
        uv_destination: &wgpu::Texture,
        width: u32,
        height: u32,
    ) -> anyhow::Result<bool> {
        if self.active_session_id != Some(session_id) {
            self.clear_sources()?;
            self.active_session_id = Some(session_id);
        }
        self.reap_completed();
        let key = import::descriptor_key(descriptor, width, height);
        let cache_hit = self.sources.contains_key(&key);
        if !cache_hit {
            let source = self.import_cached_source(descriptor, width, height)?;
            debug!(
                "[NATIVE-IMPORT] imported surface={} key={key:#x} layers={}",
                descriptor.surface_id,
                source.imported.layers.len()
            );
            self.sources.insert(key, source);
        }

        let y_destination = raw_texture_handle(y_destination)?;
        let uv_destination = raw_texture_handle(uv_destination)?;
        let source = self
            .sources
            .get_mut(&key)
            .expect("native DMA-BUF source was inserted above");

        source.select_copy_mode(CopyMode::WgpuStaging);

        // Decoder pools normally rotate across several surfaces, so this fence
        // is already signaled by the time the same surface returns. Retaining
        // the AVFrame owner until then prevents decoder reuse during the copy.
        unsafe {
            if source.owner.is_some() {
                self.device
                    .wait_for_fences(std::slice::from_ref(&source.fence), true, u64::MAX)
                    .map_err(|error| anyhow::anyhow!("waiting for DMA-BUF surface: {error:?}"))?;
                source.owner = None;
            }
            self.device
                .reset_fences(std::slice::from_ref(&source.fence))
                .map_err(|error| anyhow::anyhow!("resetting DMA-BUF copy fence: {error:?}"))?;
        }

        // The first recording imports an undefined external layout. Record once
        // more after that submission establishes GENERAL, then reuse the same
        // executable command buffer for this decoder surface indefinitely.
        if !source.reusable_command {
            if source.has_submitted {
                // SAFETY: the per-surface fence was awaited above.
                unsafe {
                    self.device
                        .reset_command_buffer(
                            source.command_buffer,
                            vk::CommandBufferResetFlags::empty(),
                        )
                        .map_err(|error| {
                            anyhow::anyhow!("resetting DMA-BUF command buffer: {error:?}")
                        })?;
                }
            }
            copy::record_copy(
                &self.device,
                self.queue_family_index,
                &mut source.imported,
                source.command_buffer,
                y_destination,
                uv_destination,
                width,
                height,
            )?;
            source.reusable_command = source.has_submitted;
            source.has_submitted = true;
        }
        let wait_for_producer = import_producer_fence(
            &self.external_semaphore_fd,
            source.producer_semaphore,
            descriptor.acquire_fence.as_deref(),
        )?;
        let wait_semaphores = [source.producer_semaphore];
        let wait_stages = [vk::PipelineStageFlags::TRANSFER];
        let mut submit =
            vk::SubmitInfo::default().command_buffers(std::slice::from_ref(&source.command_buffer));
        if wait_for_producer {
            submit = submit
                .wait_semaphores(&wait_semaphores)
                .wait_dst_stage_mask(&wait_stages);
        }
        let result = ctx.with_raw_queue_lock(|| {
            // SAFETY: access to WGPU's raw queue is serialized by queue_lock;
            // all resources referenced by the command are retained below.
            unsafe {
                self.device
                    .queue_submit(self.queue, std::slice::from_ref(&submit), source.fence)
            }
        });
        result.map_err(|error| anyhow::anyhow!("submitting DMA-BUF plane copy: {error:?}"))?;
        source.owner = Some(owner);
        Ok(cache_hit)
    }

    pub(super) fn copy_nv12_to_linear_wayland(
        &mut self,
        ctx: &super::WgpuContext,
        descriptor: &NativeDmaBufNv12,
        session_id: u64,
        owner: VideoFrameStorage,
        width: u32,
        height: u32,
        drm_syncobj: bool,
        explicit_sync: bool,
    ) -> anyhow::Result<Option<(NativeDmaBufNv12, bool)>> {
        if self.active_session_id != Some(session_id) {
            self.clear_sources()?;
            self.active_session_id = Some(session_id);
        }
        self.reap_completed();
        let key = import::descriptor_key(descriptor, width, height);
        let cache_hit = self.sources.contains_key(&key);
        if !cache_hit {
            let source = self.import_cached_source(descriptor, width, height)?;
            self.sources.insert(key, source);
        }
        let source = self.sources.get_mut(&key).expect("source inserted above");
        if source.linear_bridge.is_none() {
            source.linear_bridge = Some(linear::LinearBridge::create(
                &self.device,
                &self.external_memory_fd,
                descriptor.surface_id,
                width,
                height,
            )?);
            debug!(
                "[NATIVE-IMPORT] allocated persistent linear Wayland NV12 bridge for decoder surface={} size={}x{}",
                descriptor.surface_id, width, height
            );
        }
        source.select_copy_mode(CopyMode::LinearWayland);
        let asynchronous = drm_syncobj || explicit_sync;
        if drm_syncobj {
            let device = self
                .drm_syncobj_device
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("DRM timeline syncobj device is unavailable"))?;
            source.ensure_syncobj_timelines(device)?;
            if let Some(point) = source.last_release_point {
                let (_, release) = source
                    .syncobj_timelines
                    .as_ref()
                    .expect("timelines initialized above");
                if !release.is_signaled(point) {
                    // This is ordinary compositor back-pressure, not an import
                    // capability failure. Let this frame use the composed path
                    // without permanently disabling decoder-surface export.
                    return Ok(None);
                }
            }
        }
        source.try_reset(&self.device, asynchronous)?;

        if !source.reusable_command {
            if source.has_submitted {
                unsafe {
                    self.device
                        .reset_command_buffer(
                            source.command_buffer,
                            vk::CommandBufferResetFlags::empty(),
                        )
                        .map_err(|error| {
                            anyhow::anyhow!("resetting linear bridge command buffer: {error:?}")
                        })?;
                }
            }
            copy::record_copy_to_linear(
                &self.device,
                self.queue_family_index,
                &mut source.imported,
                source.command_buffer,
                source
                    .linear_bridge
                    .as_mut()
                    .expect("linear bridge created above"),
                width,
                height,
            )?;
            source.reusable_command = source.has_submitted;
            source.has_submitted = true;
        }
        let signal_semaphores = [source.acquire_semaphore];
        let wait_for_producer = import_producer_fence(
            &self.external_semaphore_fd,
            source.producer_semaphore,
            descriptor.acquire_fence.as_deref(),
        )?;
        let wait_semaphores = [source.producer_semaphore];
        let wait_stages = [vk::PipelineStageFlags::TRANSFER];
        let mut submit =
            vk::SubmitInfo::default().command_buffers(std::slice::from_ref(&source.command_buffer));
        if wait_for_producer {
            submit = submit
                .wait_semaphores(&wait_semaphores)
                .wait_dst_stage_mask(&wait_stages);
        }
        if asynchronous {
            submit = submit.signal_semaphores(&signal_semaphores);
        }
        ctx.with_raw_queue_lock(|| unsafe {
            self.device
                .queue_submit(self.queue, std::slice::from_ref(&submit), source.fence)
        })
        .map_err(|error| anyhow::anyhow!("submitting linear bridge copy: {error:?}"))?;
        source.owner = Some(owner);
        let acquire_fence = if asynchronous {
            let fd_info = vk::SemaphoreGetFdInfoKHR::default()
                .semaphore(source.acquire_semaphore)
                .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
            let fd = unsafe { self.external_semaphore_fd.get_semaphore_fd(&fd_info) }.map_err(
                |error| anyhow::anyhow!("exporting linear bridge acquire fence: {error:?}"),
            )?;
            // SAFETY: vkGetSemaphoreFdKHR returned a new descriptor owned by
            // the caller. SYNC_FD export transfers the temporary payload, so
            // the binary semaphore can be signaled again on its next use.
            Some(unsafe { OwnedFd::from_raw_fd(fd) })
        } else {
            // Explicit synchronization is optional. Preserve the known-correct
            // CPU fence fallback when the compositor does not expose it.
            unsafe {
                self.device
                    .wait_for_fences(std::slice::from_ref(&source.fence), true, u64::MAX)
            }
            .map_err(|error| anyhow::anyhow!("waiting for linear bridge copy: {error:?}"))?;
            source.owner = None;
            None
        };
        let mut bridged = source
            .linear_bridge
            .as_ref()
            .expect("linear bridge retained")
            .descriptor();
        if drm_syncobj {
            let point = source.next_sync_point;
            source.next_sync_point = source.next_sync_point.wrapping_add(1).max(1);
            let (acquire, release) = source
                .syncobj_timelines
                .as_ref()
                .expect("timelines initialized above");
            acquire.import_sync_file_at(
                acquire_fence
                    .as_ref()
                    .expect("asynchronous submit exported a fence")
                    .as_fd(),
                point,
            )?;
            source.last_release_point = Some(point);
            bridged.drm_syncobj = Some(crate::video::DrmSyncobjFrame {
                acquire_timeline: acquire.clone(),
                acquire_point: point,
                release_timeline: release.clone(),
                release_point: point,
            });
            bridged.acquire_fence = None;
        } else {
            bridged.acquire_fence = acquire_fence.map(Arc::new);
        }
        Ok(Some((bridged, cache_hit)))
    }

    fn import_cached_source(
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
        let allocate_info = vk::CommandBufferAllocateInfo::default()
            .command_pool(self.command_pool)
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
                        .free_command_buffers(self.command_pool, &[command_buffer])
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
                        .free_command_buffers(self.command_pool, &[command_buffer]);
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
                        .free_command_buffers(self.command_pool, &[command_buffer]);
                }
                return Err(anyhow::anyhow!(
                    "creating DMA-BUF producer semaphore: {error:?}"
                ));
            }
        };
        Ok(CachedSource {
            imported,
            command_buffer,
            fence,
            acquire_semaphore,
            producer_semaphore,
            owner: None,
            has_submitted: false,
            reusable_command: false,
            copy_mode: None,
            linear_bridge: None,
            syncobj_timelines: None,
            next_sync_point: 1,
            last_release_point: None,
        })
    }

    fn reap_completed(&mut self) {
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

    fn clear_sources(&mut self) -> anyhow::Result<()> {
        // Session changes occur only after the outgoing frame was snapshotted.
        // Waiting once here lets us retire the old decoder's imported surfaces
        // while retaining the device, command pool, and capability selection.
        unsafe { self.device.device_wait_idle() }
            .map_err(|error| anyhow::anyhow!("waiting to retire native session: {error:?}"))?;
        for (_, mut source) in self.sources.drain() {
            source.owner = None;
            // SAFETY: the device is idle and the fence belongs to it.
            unsafe { self.device.destroy_fence(source.fence, None) };
            unsafe {
                self.device
                    .destroy_semaphore(source.acquire_semaphore, None)
            };
            unsafe {
                self.device
                    .destroy_semaphore(source.producer_semaphore, None)
            };
            drop(source);
        }
        Ok(())
    }
}

impl CachedSource {
    fn ensure_syncobj_timelines(
        &mut self,
        device: &Arc<crate::video::drm_syncobj::DrmSyncobjDevice>,
    ) -> anyhow::Result<()> {
        if self.syncobj_timelines.is_none() {
            self.syncobj_timelines = Some((device.create_timeline()?, device.create_timeline()?));
        }
        Ok(())
    }

    fn select_copy_mode(&mut self, mode: CopyMode) {
        if self.copy_mode != Some(mode) {
            self.copy_mode = Some(mode);
            self.reusable_command = false;
        }
    }

    fn try_reset(&mut self, device: &ash::Device, nonblocking: bool) -> anyhow::Result<()> {
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

impl Drop for NativeDmaBufInterop {
    fn drop(&mut self) {
        // Cache teardown is session-level, never per frame. Waiting here keeps
        // both imported surfaces and command buffers valid through queued work.
        // SAFETY: every object below belongs to this device.
        unsafe {
            let _ = self.device.device_wait_idle();
            for (_, mut source) in self.sources.drain() {
                source.owner = None;
                self.device.destroy_fence(source.fence, None);
                self.device
                    .destroy_semaphore(source.acquire_semaphore, None);
                self.device
                    .destroy_semaphore(source.producer_semaphore, None);
                drop(source);
            }
            self.device.destroy_command_pool(self.command_pool, None);
        }
    }
}

fn import_producer_fence(
    external_semaphore_fd: &ash::khr::external_semaphore_fd::Device,
    semaphore: vk::Semaphore,
    fence: Option<&OwnedFd>,
) -> anyhow::Result<bool> {
    let Some(fence) = fence else {
        return Ok(false);
    };
    // Vulkan consumes a SYNC_FD import on success, so preserve the frame's
    // immutable descriptor by importing a CLOEXEC duplicate.
    let imported_fd = unsafe { libc::fcntl(fence.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if imported_fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let info = vk::ImportSemaphoreFdInfoKHR::default()
        .semaphore(semaphore)
        .flags(vk::SemaphoreImportFlags::TEMPORARY)
        .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD)
        .fd(imported_fd);
    if let Err(error) = unsafe { external_semaphore_fd.import_semaphore_fd(&info) } {
        // Failed imports do not transfer descriptor ownership.
        unsafe { libc::close(imported_fd) };
        return Err(anyhow::anyhow!(
            "importing DMA-BUF producer sync_file: {error:?}"
        ));
    }
    Ok(true)
}

fn raw_texture_handle(texture: &wgpu::Texture) -> anyhow::Result<vk::Image> {
    // SAFETY: the raw handle is borrowed only for queue work while the owning
    // WGPU texture remains live in Renderer.
    unsafe {
        texture.as_hal::<wgpu_hal::vulkan::Api, _, _>(|hal_texture| {
            hal_texture
                .map(|texture| texture.raw_handle())
                .ok_or_else(|| anyhow::anyhow!("destination WGPU texture is not Vulkan-backed"))
        })
    }
}
