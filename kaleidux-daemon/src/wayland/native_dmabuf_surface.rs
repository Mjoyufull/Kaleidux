use super::WaylandBackend;
use crate::video::{NativeDmaBufNv12, VideoFrame, VideoFrameFormat, VideoFrameStorage};
use smithay_client_toolkit::dmabuf::{DmabufFeedback, DmabufHandler};
use smithay_client_toolkit::shell::WaylandSurface;
use std::collections::{HashMap, HashSet};
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::sync::Arc;
use tracing::{debug, info, warn};
use wayland_client::protocol::{wl_buffer, wl_subsurface, wl_surface};
use wayland_client::{Connection, Dispatch, Proxy, QueueHandle, backend::ObjectId};
use wayland_protocols::wp::linux_dmabuf::zv1::client::zwp_linux_buffer_params_v1;
use wayland_protocols::wp::linux_dmabuf::zv1::client::zwp_linux_dmabuf_feedback_v1::ZwpLinuxDmabufFeedbackV1;
use wayland_protocols::wp::linux_drm_syncobj::v1::client::{
    wp_linux_drm_syncobj_manager_v1::WpLinuxDrmSyncobjManagerV1,
    wp_linux_drm_syncobj_surface_v1::WpLinuxDrmSyncobjSurfaceV1,
    wp_linux_drm_syncobj_timeline_v1::WpLinuxDrmSyncobjTimelineV1,
};
use wayland_protocols::wp::linux_explicit_synchronization::zv1::client::{
    zwp_linux_buffer_release_v1,
    zwp_linux_surface_synchronization_v1::ZwpLinuxSurfaceSynchronizationV1,
};
use wayland_protocols::wp::presentation_time::client::wp_presentation::WpPresentation;
use wayland_protocols::wp::viewporter::client::wp_viewport::WpViewport;

const fn drm_fourcc(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16) | ((d as u32) << 24)
}

const DRM_FORMAT_NV12: u32 = drm_fourcc(b'N', b'V', b'1', b'2');
const DRM_FORMAT_MOD_LINEAR: u64 = 0;
const DRM_FORMAT_MOD_INVALID: u64 = 0x00ff_ffff_ffff_ffff;
const PENDING_BUFFER_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
const RETIRED_BUFFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

struct BufferSlot {
    buffer: wl_buffer::WlBuffer,
    busy: bool,
    wayland_released: bool,
    explicit_release_pending: bool,
    release_fence: Option<OwnedFd>,
    drm_syncobj: Option<WaylandDrmSyncobjSlot>,
    owner: Option<VideoFrameStorage>,
    retired_at: Option<std::time::Instant>,
}

struct WaylandDrmSyncobjSlot {
    acquire_timeline: WpLinuxDrmSyncobjTimelineV1,
    release_timeline: WpLinuxDrmSyncobjTimelineV1,
    frame: crate::video::DrmSyncobjFrame,
}

impl Drop for WaylandDrmSyncobjSlot {
    fn drop(&mut self) {
        self.acquire_timeline.destroy();
        self.release_timeline.destroy();
    }
}

struct NativeSurface {
    subsurface: wl_subsurface::WlSubsurface,
    surface: wl_surface::WlSurface,
    viewport: WpViewport,
    explicit_sync: Option<ZwpLinuxSurfaceSynchronizationV1>,
    drm_syncobj_surface: Option<WpLinuxDrmSyncobjSurfaceV1>,
    buffers: HashMap<(u64, u64), BufferSlot>,
    viewport_geometry: Option<((u32, u32), (u32, u32))>,
    mapped: bool,
    egl_bound: bool,
    last_presentation_feedback_request: Option<std::time::Instant>,
}

struct PendingBuffer {
    params: zwp_linux_buffer_params_v1::ZwpLinuxBufferParamsV1,
    output: String,
    session_id: u64,
    surface_id: u64,
    owner: VideoFrameStorage,
    source_size: (u32, u32),
    output_size: (u32, u32),
    acquire_fence: Option<Arc<OwnedFd>>,
    drm_syncobj: Option<crate::video::DrmSyncobjFrame>,
    created_at: std::time::Instant,
}

#[derive(Default)]
pub(super) struct NativeDmabufPresentation {
    surfaces: HashMap<String, NativeSurface>,
    pending: HashMap<ObjectId, PendingBuffer>,
    retired_buffers: HashMap<ObjectId, BufferSlot>,
    explicit_releases: HashMap<ObjectId, ObjectId>,
    feedbacks: HashMap<String, ZwpLinuxDmabufFeedbackV1>,
    formats: HashSet<(u32, u64)>,
    rejected_modifiers: HashMap<String, HashSet<u64>>,
    legacy_formats_loaded: bool,
    disabled: bool,
    logged_ready: bool,
    logged_unsupported: bool,
    logged_linear_probe: bool,
    logged_present_block: HashSet<String>,
    linear_bridge_disabled: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct NativeDmabufResourceCounts {
    pub surfaces: usize,
    pub buffers: usize,
    pub busy: usize,
    pub owners: usize,
    pub pending: usize,
    pub retired: usize,
    pub explicit_releases: usize,
    pub feedbacks: usize,
}

impl WaylandBackend {
    pub(crate) fn native_dmabuf_resource_counts(&self) -> NativeDmabufResourceCounts {
        let mut counts = NativeDmabufResourceCounts {
            surfaces: self.native_dmabuf.surfaces.len(),
            pending: self.native_dmabuf.pending.len(),
            retired: self.native_dmabuf.retired_buffers.len(),
            explicit_releases: self.native_dmabuf.explicit_releases.len(),
            feedbacks: self.native_dmabuf.feedbacks.len(),
            ..NativeDmabufResourceCounts::default()
        };
        for surface in self.native_dmabuf.surfaces.values() {
            counts.buffers = counts.buffers.saturating_add(surface.buffers.len());
            counts.busy = counts
                .busy
                .saturating_add(surface.buffers.values().filter(|slot| slot.busy).count());
            counts.owners = counts.owners.saturating_add(
                surface
                    .buffers
                    .values()
                    .filter(|slot| slot.owner.is_some())
                    .count(),
            );
        }
        counts.busy = counts.busy.saturating_add(
            self.native_dmabuf
                .retired_buffers
                .values()
                .filter(|slot| slot.busy)
                .count(),
        );
        counts.owners = counts.owners.saturating_add(
            self.native_dmabuf
                .retired_buffers
                .values()
                .filter(|slot| slot.owner.is_some())
                .count(),
        );
        counts
    }

    pub(crate) fn native_drm_syncobj_available(&self) -> bool {
        self.drm_syncobj.is_some()
    }

    pub(crate) fn native_explicit_sync_available(&self) -> bool {
        self.explicit_sync.is_some()
    }

    pub(crate) fn native_dmabuf_surface_active(&self, output: &str) -> bool {
        self.native_dmabuf
            .surfaces
            .get(output)
            .is_some_and(|surface| surface.mapped)
    }

    /// Log the first reason a native present was refused per output. Silent
    /// `false` returns here are why the presentation ladder had to be guessed
    /// at from aggregate counters.
    fn log_present_block(&mut self, output: &str, reason: &str) {
        if self
            .native_dmabuf
            .logged_present_block
            .insert(output.to_string())
        {
            info!("[NATIVE-WAYLAND] {output}: native present unavailable: {reason}");
        }
    }

    pub(crate) fn native_dmabuf_accepts_linear(&self) -> bool {
        !self.native_dmabuf.linear_bridge_disabled
            && self
                .native_dmabuf
                .formats
                .contains(&(DRM_FORMAT_NV12, DRM_FORMAT_MOD_LINEAR))
    }

    pub(crate) fn try_present_native_dmabuf(
        &mut self,
        qh: &QueueHandle<Self>,
        output: &str,
        frame: &VideoFrame,
        output_size: (u32, u32),
    ) -> bool {
        self.reap_native_release_fences();
        self.reap_stale_pending_buffers(output);
        if self.native_dmabuf.disabled || !native_surface_enabled() {
            self.log_present_block(output, "surface disabled");
            return false;
        }
        if self
            .native_dmabuf
            .surfaces
            .get(output)
            .is_some_and(|surface| surface.egl_bound)
        {
            self.log_present_block(output, "EGL is bound to this surface");
            return false;
        }
        let VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } = &frame.format else {
            self.log_present_block(output, "frame is not native DMA-BUF NV12");
            return false;
        };
        let Some(modifier) = common_modifier(descriptor) else {
            warn!(
                "[NATIVE-WAYLAND] frame surface={} does not expose one common NV12 modifier",
                descriptor.surface_id
            );
            return false;
        };
        if self
            .native_dmabuf
            .rejected_modifiers
            .get(output)
            .is_some_and(|modifiers| modifiers.contains(&modifier))
        {
            return false;
        }
        if !self.ensure_native_surface(qh, output) {
            return false;
        }
        self.retire_stale_native_buffers(output, frame.session_id);
        if !self.native_dmabuf.legacy_formats_loaded {
            self.native_dmabuf.formats.extend(
                self.dmabuf_state
                    .modifiers()
                    .iter()
                    .map(|format| (format.format, format.modifier)),
            );
            self.native_dmabuf.legacy_formats_loaded = true;
        }
        let presentation_modifier = if let Some(presentation_modifier) =
            select_presentation_modifier(&self.native_dmabuf.formats, modifier)
        {
            presentation_modifier
        } else {
            if !self.native_dmabuf.formats.is_empty() && !self.native_dmabuf.logged_unsupported {
                let advertised = self
                    .native_dmabuf
                    .formats
                    .iter()
                    .filter_map(|(format, advertised_modifier)| {
                        (*format == DRM_FORMAT_NV12).then_some(*advertised_modifier)
                    })
                    .collect::<Vec<_>>();
                info!(
                    "[NATIVE-WAYLAND] composed NV12 modifier {modifier:#018x} has no exact match in surface feedback NV12={advertised:x?}; implicit-only import is rejected because it produced corrupt chroma on this compositor, retaining single-GPU-copy"
                );
                self.native_dmabuf.logged_unsupported = true;
            }
            if !self.native_dmabuf.formats.is_empty() {
                self.native_dmabuf
                    .rejected_modifiers
                    .entry(output.to_string())
                    .or_default()
                    .insert(modifier);
            }
            return false;
        };

        if modifier == DRM_FORMAT_MOD_LINEAR && !self.native_dmabuf.logged_linear_probe {
            debug!(
                "[NATIVE-WAYLAND] probing linear bridge surface={} objects={} offsets={:?} pitches={:?} presentation_modifier={presentation_modifier:#018x}",
                descriptor.surface_id,
                descriptor.objects.len(),
                descriptor.planes.map(|plane| plane.offset),
                descriptor.planes.map(|plane| plane.pitch),
            );
            self.native_dmabuf.logged_linear_probe = true;
        }

        let key = (frame.session_id, descriptor.surface_id);
        let drm_manager = self
            .drm_syncobj
            .as_ref()
            .and_then(|global| global.get().ok())
            .cloned();
        let presentation = self.presentation_proxy().cloned();
        if let Some(surface) = self.native_dmabuf.surfaces.get_mut(output)
            && let Some(mut slot) = surface.buffers.remove(&key)
        {
            if slot.busy {
                surface.buffers.insert(key, slot);
                return false;
            }
            let release = match attach(
                qh,
                drm_manager.as_ref(),
                presentation.as_ref(),
                output,
                surface,
                &mut slot,
                frame.storage.clone(),
                (frame.width, frame.height),
                output_size,
                descriptor.acquire_fence.as_deref(),
                descriptor.drm_syncobj.as_ref(),
            ) {
                Ok(release) => release,
                Err(error) => {
                    warn!(
                        "[NATIVE-SYNC] {output}: refusing unsynchronized DMA-BUF attach: {error:#}"
                    );
                    surface.buffers.insert(key, slot);
                    return false;
                }
            };
            let buffer_id = slot.buffer.id();
            surface.buffers.insert(key, slot);
            if let Some(release) = release {
                self.native_dmabuf
                    .explicit_releases
                    .insert(release.id(), buffer_id);
            }
            return true;
        }
        if self.native_dmabuf.pending.values().any(|pending| {
            pending.output == output
                && pending.session_id == frame.session_id
                && pending.surface_id == descriptor.surface_id
        }) {
            return true;
        }
        let created = self.create_native_buffer(
            qh,
            output,
            frame,
            descriptor,
            presentation_modifier,
            output_size,
        );
        if modifier == DRM_FORMAT_MOD_LINEAR && !created {
            warn!(
                "[NATIVE-WAYLAND] could not submit linear bridge surface={} for compositor import",
                descriptor.surface_id
            );
            self.native_dmabuf.linear_bridge_disabled = true;
        }
        created
    }

    pub(crate) fn native_dmabuf_slot_available(
        &mut self,
        output: &str,
        session_id: u64,
        surface_id: u64,
    ) -> bool {
        self.reap_native_release_fences();
        let key = (session_id, surface_id);
        !self.native_dmabuf.pending.values().any(|pending| {
            pending.output == output
                && pending.session_id == session_id
                && pending.surface_id == surface_id
        }) && self
            .native_dmabuf
            .surfaces
            .get(output)
            .and_then(|surface| surface.buffers.get(&key))
            .is_none_or(|slot| !slot.busy)
    }

    pub(crate) fn hide_native_dmabuf(&mut self, output: &str) -> bool {
        let Some(surface) = self.native_dmabuf.surfaces.get_mut(output) else {
            return false;
        };
        if !surface.mapped {
            return false;
        }
        surface.surface.attach(None, 0, 0);
        surface.surface.commit();
        surface.mapped = false;
        true
    }

    pub(crate) fn retire_native_dmabuf_output(&mut self, output: &str) {
        let pending_ids: Vec<_> = self
            .native_dmabuf
            .pending
            .iter()
            .filter_map(|(id, pending)| (pending.output == output).then_some(id.clone()))
            .collect();
        for id in pending_ids {
            if let Some(pending) = self.native_dmabuf.pending.remove(&id) {
                pending.params.destroy();
            }
        }
        let Some(mut surface) = self.native_dmabuf.surfaces.remove(output) else {
            if let Some(feedback) = self.native_dmabuf.feedbacks.remove(output) {
                feedback.destroy();
            }
            self.native_dmabuf.rejected_modifiers.remove(output);
            self.native_dmabuf.logged_present_block.remove(output);
            return;
        };

        surface.surface.attach(None, 0, 0);
        surface.surface.commit();
        for (_, mut slot) in surface.buffers.drain() {
            if slot.busy {
                // The compositor may still be reading this DMA-BUF. Keep the
                // backing frame alive until wl_buffer.release arrives.
                slot.retired_at = Some(std::time::Instant::now());
                self.native_dmabuf
                    .retired_buffers
                    .insert(slot.buffer.id(), slot);
            } else {
                slot.owner = None;
                slot.buffer.destroy();
            }
        }
        surface.viewport.destroy();
        if let Some(explicit_sync) = surface.explicit_sync {
            explicit_sync.destroy();
        }
        if let Some(drm_syncobj_surface) = surface.drm_syncobj_surface {
            drm_syncobj_surface.destroy();
        }
        surface.subsurface.destroy();
        surface.surface.destroy();
        if let Some(feedback) = self.native_dmabuf.feedbacks.remove(output) {
            feedback.destroy();
        }
        self.native_dmabuf.rejected_modifiers.remove(output);
        self.native_dmabuf.logged_present_block.remove(output);
    }

    fn retire_stale_native_buffers(&mut self, output: &str, active_session_id: u64) {
        let Some(surface) = self.native_dmabuf.surfaces.get_mut(output) else {
            return;
        };
        let stale_keys: Vec<_> = surface
            .buffers
            .keys()
            .filter(|(session_id, _)| *session_id != active_session_id)
            .copied()
            .collect();
        let mut retired = Vec::new();
        for key in stale_keys {
            if let Some(slot) = surface.buffers.remove(&key) {
                retired.push(slot);
            }
        }
        for mut slot in retired {
            if slot.busy {
                // Session replacement does not revoke compositor ownership.
                // Release the backing storage only from the release callback.
                slot.retired_at = Some(std::time::Instant::now());
                self.native_dmabuf
                    .retired_buffers
                    .insert(slot.buffer.id(), slot);
            } else {
                slot.owner = None;
                slot.buffer.destroy();
            }
        }
    }

    pub(crate) fn native_gl_surface_target(
        &mut self,
        qh: &QueueHandle<Self>,
        conn: &Connection,
        output: &str,
    ) -> Option<(*mut std::ffi::c_void, ObjectId)> {
        if !native_surface_enabled() || !self.ensure_native_surface(qh, output) {
            return None;
        }
        // Binding EGL to this surface is irreversible: `egl_bound` then blocks
        // every wl_buffer attach, because a surface cannot be driven by both
        // eglSwapBuffers and manual attach. So GL must not claim the surface
        // before the cheaper paths have had a fair chance. Until the compositor
        // has delivered DMA-BUF feedback we do not yet know which modifiers it
        // accepts, so the direct and linear-bridge paths cannot be evaluated;
        // hold GL back and let WGPU composition cover those first few frames.
        if self.native_dmabuf.formats.is_empty() {
            return None;
        }
        let surface = self.native_dmabuf.surfaces.get(output)?;
        Some((
            conn.backend().display_ptr() as *mut std::ffi::c_void,
            surface.surface.id(),
        ))
    }

    pub(crate) fn mark_native_gl_surface_presented(&mut self, output: &str) {
        if let Some(surface) = self.native_dmabuf.surfaces.get_mut(output) {
            surface.egl_bound = true;
            surface.mapped = true;
        }
    }

    fn ensure_native_surface(&mut self, qh: &QueueHandle<Self>, output: &str) -> bool {
        if self.native_dmabuf.surfaces.contains_key(output) {
            return true;
        }
        let Some(parent_surface) = self
            .surfaces
            .get(output)
            .map(|parent| parent.wl_surface().clone())
        else {
            return false;
        };
        let (subsurface, surface) = self
            .subcompositor_state
            .create_subsurface(parent_surface.clone(), qh);
        subsurface.set_desync();
        subsurface.set_position(0, 0);
        subsurface.place_above(&parent_surface);
        let Ok(viewporter) = self.viewporter.get() else {
            self.native_dmabuf.disabled = true;
            return false;
        };
        let viewport = viewporter.get_viewport(&surface, qh, ());
        // linux-drm-syncobj and the deprecated sync-file protocol are mutually
        // exclusive per surface. Defer the modern surface object until the
        // first timeline-backed bridge commit; direct decoder buffers need no
        // explicit points and therefore keep implicit synchronization.
        let explicit_sync = if self.drm_syncobj.is_none() {
            self.explicit_sync.as_ref().and_then(|global| {
                global
                    .get()
                    .ok()
                    .map(|manager| manager.get_synchronization(&surface, qh, ()))
            })
        } else {
            None
        };
        self.configure_color_surface(output, &surface, qh);
        self.native_dmabuf.surfaces.insert(
            output.to_string(),
            NativeSurface {
                subsurface,
                surface: surface.clone(),
                viewport,
                explicit_sync,
                drm_syncobj_surface: None,
                buffers: HashMap::new(),
                viewport_geometry: None,
                mapped: false,
                egl_bound: false,
                last_presentation_feedback_request: None,
            },
        );
        if !self.native_dmabuf.feedbacks.contains_key(output)
            && let Ok(feedback) = self.dmabuf_state.get_surface_feedback(&surface, qh)
        {
            self.native_dmabuf
                .feedbacks
                .insert(output.to_string(), feedback);
        }
        true
    }

    fn create_native_buffer(
        &mut self,
        qh: &QueueHandle<Self>,
        output: &str,
        frame: &VideoFrame,
        descriptor: &NativeDmaBufNv12,
        modifier: u64,
        output_size: (u32, u32),
    ) -> bool {
        let Ok(params) = self.dmabuf_state.create_params(qh) else {
            self.native_dmabuf.disabled = true;
            warn!("[NATIVE-WAYLAND] zwp_linux_dmabuf parameters are unavailable");
            return false;
        };
        for (plane_index, plane) in descriptor.planes.iter().enumerate() {
            let Some(object) = descriptor.objects.get(plane.object_index) else {
                warn!(
                    "[NATIVE-WAYLAND] plane {plane_index} references absent object {}",
                    plane.object_index
                );
                return false;
            };
            let (Ok(offset), Ok(stride)) =
                (u32::try_from(plane.offset), u32::try_from(plane.pitch))
            else {
                warn!(
                    "[NATIVE-WAYLAND] plane {plane_index} offset/pitch exceeds protocol u32 range"
                );
                return false;
            };
            params.add(
                object.fd.as_fd(),
                plane_index as u32,
                offset,
                stride,
                modifier,
            );
        }
        let (Ok(width), Ok(height)) = (i32::try_from(frame.width), i32::try_from(frame.height))
        else {
            warn!("[NATIVE-WAYLAND] frame dimensions exceed protocol i32 range");
            return false;
        };
        let params = params.create(
            width,
            height,
            DRM_FORMAT_NV12,
            zwp_linux_buffer_params_v1::Flags::empty(),
        );
        self.native_dmabuf.pending.insert(
            params.id(),
            PendingBuffer {
                params: params.clone(),
                output: output.to_string(),
                session_id: frame.session_id,
                surface_id: descriptor.surface_id,
                owner: frame.storage.clone(),
                source_size: (frame.width, frame.height),
                output_size,
                acquire_fence: descriptor.acquire_fence.clone(),
                drm_syncobj: descriptor.drm_syncobj.clone(),
                created_at: std::time::Instant::now(),
            },
        );
        true
    }

    fn native_buffer_created(
        &mut self,
        params: &zwp_linux_buffer_params_v1::ZwpLinuxBufferParamsV1,
        buffer: wl_buffer::WlBuffer,
        qh: &QueueHandle<Self>,
    ) {
        let Some(pending) = self.native_dmabuf.pending.remove(&params.id()) else {
            buffer.destroy();
            params.destroy();
            return;
        };
        params.destroy();
        let drm_manager = self
            .drm_syncobj
            .as_ref()
            .and_then(|global| global.get().ok())
            .cloned();
        let presentation = self.presentation_proxy().cloned();
        let Some(surface) = self.native_dmabuf.surfaces.get_mut(&pending.output) else {
            buffer.destroy();
            return;
        };
        let key = (pending.session_id, pending.surface_id);
        let mut slot = BufferSlot {
            buffer,
            busy: false,
            wayland_released: true,
            explicit_release_pending: false,
            release_fence: None,
            drm_syncobj: None,
            owner: None,
            retired_at: None,
        };
        let release = match attach(
            qh,
            drm_manager.as_ref(),
            presentation.as_ref(),
            &pending.output,
            surface,
            &mut slot,
            pending.owner,
            pending.source_size,
            pending.output_size,
            pending.acquire_fence.as_deref(),
            pending.drm_syncobj.as_ref(),
        ) {
            Ok(release) => release,
            Err(error) => {
                warn!(
                    "[NATIVE-SYNC] {}: rejecting created DMA-BUF without valid synchronization: {error:#}",
                    pending.output
                );
                slot.buffer.destroy();
                return;
            }
        };
        let buffer_id = slot.buffer.id();
        surface.buffers.insert(key, slot);
        if let Some(release) = release {
            self.native_dmabuf
                .explicit_releases
                .insert(release.id(), buffer_id);
        }
        if !self.native_dmabuf.logged_ready {
            info!(
                "[NATIVE-WAYLAND] surface zero-copy active: decoder DMA-BUF -> desynchronized video subsurface"
            );
            self.native_dmabuf.logged_ready = true;
        }
    }

    fn native_buffer_failed(
        &mut self,
        params: &zwp_linux_buffer_params_v1::ZwpLinuxBufferParamsV1,
    ) {
        self.native_dmabuf.pending.remove(&params.id());
        params.destroy();
        self.native_dmabuf.disabled = true;
        warn!(
            "[NATIVE-WAYLAND] compositor rejected the advertised NV12 modifier; degrading to single-GPU-copy composition"
        );
    }

    fn native_buffer_released(&mut self, buffer: &wl_buffer::WlBuffer) {
        for surface in self.native_dmabuf.surfaces.values_mut() {
            if let Some(slot) = surface
                .buffers
                .values_mut()
                .find(|slot| slot.buffer == *buffer)
            {
                slot.wayland_released = true;
                finish_released_slot(slot);
                return;
            }
        }
        if let Some(slot) = self.native_dmabuf.retired_buffers.get_mut(&buffer.id()) {
            slot.wayland_released = true;
            finish_released_slot(slot);
            if !slot.busy
                && let Some(mut slot) = self.native_dmabuf.retired_buffers.remove(&buffer.id())
            {
                slot.owner = None;
                slot.buffer.destroy();
            }
        }
    }

    pub(crate) fn reap_native_release_fences(&mut self) {
        for surface in self.native_dmabuf.surfaces.values_mut() {
            for slot in surface.buffers.values_mut() {
                finish_released_slot(slot);
            }
        }
        let now = std::time::Instant::now();
        let released = self
            .native_dmabuf
            .retired_buffers
            .iter_mut()
            .filter_map(|(id, slot)| {
                finish_released_slot(slot);
                let timed_out = slot.retired_at.is_some_and(|retired_at| {
                    now.duration_since(retired_at) >= RETIRED_BUFFER_TIMEOUT
                });
                (!slot.busy || timed_out).then_some((id.clone(), timed_out && slot.busy))
            })
            .collect::<Vec<_>>();
        for (id, timed_out) in released {
            if timed_out {
                warn!(
                    "[WAYLAND-RESOURCES] forcing release of a retired native DMA-BUF after {}ms without a compositor release event",
                    RETIRED_BUFFER_TIMEOUT.as_millis()
                );
            }
            if let Some(mut slot) = self.native_dmabuf.retired_buffers.remove(&id) {
                self.native_dmabuf
                    .explicit_releases
                    .retain(|_, buffer_id| *buffer_id != id);
                slot.owner = None;
                slot.buffer.destroy();
            }
        }
    }

    fn reap_stale_pending_buffers(&mut self, output: &str) {
        let now = std::time::Instant::now();
        let stale = self
            .native_dmabuf
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                (pending.output == output
                    && now.duration_since(pending.created_at) >= PENDING_BUFFER_TIMEOUT)
                    .then_some(id.clone())
            })
            .collect::<Vec<_>>();
        for id in stale {
            if let Some(pending) = self.native_dmabuf.pending.remove(&id) {
                warn!(
                    "[NATIVE-WAYLAND] {}: DMA-BUF creation did not complete within {}ms; reverting this frame to composition",
                    pending.output,
                    PENDING_BUFFER_TIMEOUT.as_millis()
                );
                pending.params.destroy();
            }
        }
    }

    fn native_explicit_release(
        &mut self,
        release: &zwp_linux_buffer_release_v1::ZwpLinuxBufferReleaseV1,
        fence: Option<OwnedFd>,
    ) {
        let Some(buffer_id) = self.native_dmabuf.explicit_releases.remove(&release.id()) else {
            return;
        };
        for surface in self.native_dmabuf.surfaces.values_mut() {
            if let Some(slot) = surface
                .buffers
                .values_mut()
                .find(|slot| slot.buffer.id() == buffer_id)
            {
                slot.explicit_release_pending = false;
                slot.release_fence = fence;
                finish_released_slot(slot);
                return;
            }
        }
        if let Some(slot) = self.native_dmabuf.retired_buffers.get_mut(&buffer_id) {
            slot.explicit_release_pending = false;
            slot.release_fence = fence;
            finish_released_slot(slot);
        }
    }
}

impl DmabufHandler for WaylandBackend {
    fn dmabuf_state(&mut self) -> &mut smithay_client_toolkit::dmabuf::DmabufState {
        &mut self.dmabuf_state
    }

    fn dmabuf_feedback(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        _proxy: &wayland_protocols::wp::linux_dmabuf::zv1::client::zwp_linux_dmabuf_feedback_v1::ZwpLinuxDmabufFeedbackV1,
        feedback: DmabufFeedback,
    ) {
        let table = feedback.format_table();
        self.native_dmabuf.formats.extend(
            feedback
                .tranches()
                .iter()
                .flat_map(|tranche| tranche.formats.iter())
                .filter_map(|index| table.get(usize::from(*index)))
                .map(|format| (format.format, format.modifier)),
        );
        // Surface feedback can arrive after an initial legacy/global-format
        // decision. Retry only when the compositor has supplied new evidence.
        self.native_dmabuf.rejected_modifiers.clear();
        let nv12_modifiers = self
            .native_dmabuf
            .formats
            .iter()
            .filter_map(|(format, modifier)| (*format == DRM_FORMAT_NV12).then_some(*modifier))
            .collect::<Vec<_>>();
        debug!(
            "[NATIVE-WAYLAND] compositor DMA-BUF feedback contains {} format/modifier pairs; NV12={nv12_modifiers:x?}",
            self.native_dmabuf.formats.len(),
        );
    }

    fn created(
        &mut self,
        _conn: &Connection,
        qh: &QueueHandle<Self>,
        params: &zwp_linux_buffer_params_v1::ZwpLinuxBufferParamsV1,
        buffer: wl_buffer::WlBuffer,
    ) {
        self.native_buffer_created(params, buffer, qh);
    }

    fn failed(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        params: &zwp_linux_buffer_params_v1::ZwpLinuxBufferParamsV1,
    ) {
        self.native_buffer_failed(params);
    }

    fn released(
        &mut self,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
        buffer: &wl_buffer::WlBuffer,
    ) {
        self.native_buffer_released(buffer);
    }
}

impl Dispatch<zwp_linux_buffer_release_v1::ZwpLinuxBufferReleaseV1, ()> for WaylandBackend {
    fn event(
        state: &mut Self,
        release: &zwp_linux_buffer_release_v1::ZwpLinuxBufferReleaseV1,
        event: zwp_linux_buffer_release_v1::Event,
        _data: &(),
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        match event {
            zwp_linux_buffer_release_v1::Event::FencedRelease { fence } => {
                state.native_explicit_release(release, Some(fence));
            }
            zwp_linux_buffer_release_v1::Event::ImmediateRelease => {
                state.native_explicit_release(release, None);
            }
            _ => {}
        }
    }
}

fn attach(
    qh: &QueueHandle<WaylandBackend>,
    drm_manager: Option<&WpLinuxDrmSyncobjManagerV1>,
    presentation: Option<&WpPresentation>,
    output: &str,
    surface: &mut NativeSurface,
    slot: &mut BufferSlot,
    owner: VideoFrameStorage,
    source_size: (u32, u32),
    output_size: (u32, u32),
    acquire_fence: Option<&OwnedFd>,
    drm_syncobj: Option<&crate::video::DrmSyncobjFrame>,
) -> anyhow::Result<Option<zwp_linux_buffer_release_v1::ZwpLinuxBufferReleaseV1>> {
    if let Some(frame_sync) = drm_syncobj {
        let manager =
            drm_manager.ok_or_else(|| anyhow::anyhow!("DRM syncobj manager disappeared"))?;
        if surface.drm_syncobj_surface.is_none() {
            surface.drm_syncobj_surface = Some(manager.get_surface(&surface.surface, qh, ()));
        }
        if slot.drm_syncobj.is_none() {
            slot.drm_syncobj = Some(WaylandDrmSyncobjSlot {
                acquire_timeline: manager.import_timeline(
                    frame_sync.acquire_timeline.export_fd(),
                    qh,
                    (),
                ),
                release_timeline: manager.import_timeline(
                    frame_sync.release_timeline.export_fd(),
                    qh,
                    (),
                ),
                frame: frame_sync.clone(),
            });
        }
        let slot_sync = slot
            .drm_syncobj
            .as_mut()
            .expect("syncobj slot initialized above");
        slot_sync.frame = frame_sync.clone();
        let sync_surface = surface
            .drm_syncobj_surface
            .as_ref()
            .expect("syncobj surface initialized above");
        sync_surface.set_acquire_point(
            &slot_sync.acquire_timeline,
            (frame_sync.acquire_point >> 32) as u32,
            frame_sync.acquire_point as u32,
        );
        sync_surface.set_release_point(
            &slot_sync.release_timeline,
            (frame_sync.release_point >> 32) as u32,
            frame_sync.release_point as u32,
        );
    } else if surface.drm_syncobj_surface.is_some() {
        anyhow::bail!("timeline points are required after enabling DRM syncobj on this surface");
    }
    if surface.viewport_geometry != Some((source_size, output_size)) {
        let (x, y, width, height) = cover_crop(source_size, output_size);
        surface.viewport.set_source(x, y, width, height);
        surface
            .viewport
            .set_destination(output_size.0.max(1) as i32, output_size.1.max(1) as i32);
        surface.viewport_geometry = Some((source_size, output_size));
    }
    surface.surface.attach(Some(&slot.buffer), 0, 0);
    let release = surface.explicit_sync.as_ref().and_then(|sync| {
        acquire_fence.map(|fence| {
            sync.set_acquire_fence(fence.as_fd());
            sync.get_release(qh, ())
        })
    });
    // damage_buffer is in buffer coordinates, which for this DMA-BUF are the
    // decoder's source dimensions. The viewporter crop/scale to `output_size`
    // happens after damage is applied, so damaging the destination extent
    // leaves the rest of a larger source buffer showing the previous frame.
    surface.surface.damage_buffer(
        0,
        0,
        source_size.0.max(1) as i32,
        source_size.1.max(1) as i32,
    );
    slot.owner = Some(owner);
    slot.busy = true;
    slot.wayland_released = false;
    slot.explicit_release_pending = release.is_some();
    slot.release_fence = None;
    if let Some(presentation) = presentation
        && surface
            .last_presentation_feedback_request
            .is_none_or(|requested| requested.elapsed() >= std::time::Duration::from_secs(1))
    {
        surface.last_presentation_feedback_request = Some(std::time::Instant::now());
        presentation.feedback(
            &surface.surface,
            qh,
            crate::wayland::presentation::FeedbackData {
                output: output.to_owned(),
                requested_at: std::time::Instant::now(),
            },
        );
    }
    surface.surface.commit();
    surface.mapped = true;
    Ok(release)
}

fn finish_released_slot(slot: &mut BufferSlot) {
    if !slot.busy {
        return;
    }
    if let Some(syncobj) = &slot.drm_syncobj {
        if !syncobj.frame.release_signaled() {
            return;
        }
        slot.owner = None;
        slot.busy = false;
        return;
    }
    if !slot.wayland_released || slot.explicit_release_pending {
        return;
    }
    if slot
        .release_fence
        .as_ref()
        .is_some_and(|fence| !fence_signaled(fence))
    {
        return;
    }
    slot.release_fence = None;
    slot.owner = None;
    slot.busy = false;
}

fn fence_signaled(fence: &OwnedFd) -> bool {
    let mut poll_fd = libc::pollfd {
        fd: fence.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: poll_fd points to one valid entry and timeout zero never blocks.
    unsafe { libc::poll(std::ptr::addr_of_mut!(poll_fd), 1, 0) > 0 }
}

fn common_modifier(descriptor: &NativeDmaBufNv12) -> Option<u64> {
    let first = descriptor
        .objects
        .get(descriptor.planes[0].object_index)?
        .modifier;
    descriptor
        .planes
        .iter()
        .all(|plane| {
            descriptor
                .objects
                .get(plane.object_index)
                .is_some_and(|o| o.modifier == first)
        })
        .then_some(first)
}

fn select_presentation_modifier(
    formats: &HashSet<(u32, u64)>,
    actual_modifier: u64,
) -> Option<u64> {
    // An implicit-modifier advertisement is protocol-valid, but it is not
    // sufficient proof that this compositor/driver combination will infer a
    // VA-exported tiled NV12 layout correctly. Hyprland 0.55 on Intel UHD 620
    // accepted such buffers and rendered stable luma with catastrophically red
    // chroma. Require an exact modifier match for tiled layouts so capability
    // negotiation falls back to the visually verified conversion paths.
    if actual_modifier != DRM_FORMAT_MOD_INVALID
        && formats.contains(&(DRM_FORMAT_NV12, actual_modifier))
    {
        return Some(actual_modifier);
    }
    // Do not treat an implicit-only entry as proof of linear support. Live
    // Hyprland/Intel validation showed both unsafe outcomes: passing INVALID
    // accepted the buffer but sampled luma bytes as red chroma, while passing
    // the real LINEAR modifier was rejected with linux-dmabuf protocol error 4.
    // The correct contract is therefore an exact advertised modifier match.
    None
}

fn cover_crop(source: (u32, u32), output: (u32, u32)) -> (f64, f64, f64, f64) {
    let (sw, sh) = (source.0.max(1) as f64, source.1.max(1) as f64);
    let target_aspect = output.0.max(1) as f64 / output.1.max(1) as f64;
    if sw / sh > target_aspect {
        let width = sh * target_aspect;
        ((sw - width) * 0.5, 0.0, width, sh)
    } else {
        let height = sw / target_aspect;
        (0.0, (sh - height) * 0.5, sw, height)
    }
}

fn native_surface_enabled() -> bool {
    std::env::var("KLD_NATIVE_WAYLAND_SURFACE")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::FromRawFd;

    #[test]
    fn direct_surface_requires_exact_nv12_modifier() {
        let actual = 0x0100_0000_0000_0002;
        let formats = HashSet::from([(DRM_FORMAT_NV12, actual)]);

        assert_eq!(select_presentation_modifier(&formats, actual), Some(actual));
    }

    #[test]
    fn implicit_only_nv12_degrades_to_single_copy() {
        let actual = 0x0100_0000_0000_0002;
        let formats = HashSet::from([(DRM_FORMAT_NV12, DRM_FORMAT_MOD_INVALID)]);

        assert_eq!(select_presentation_modifier(&formats, actual), None);
    }

    #[test]
    fn implicit_export_and_feedback_still_degrade_to_single_copy() {
        let formats = HashSet::from([(DRM_FORMAT_NV12, DRM_FORMAT_MOD_INVALID)]);

        assert_eq!(
            select_presentation_modifier(&formats, DRM_FORMAT_MOD_INVALID),
            None
        );
    }

    #[test]
    fn linear_export_rejects_implicit_only_feedback() {
        let formats = HashSet::from([(DRM_FORMAT_NV12, DRM_FORMAT_MOD_INVALID)]);

        assert_eq!(
            select_presentation_modifier(&formats, DRM_FORMAT_MOD_LINEAR),
            None
        );
    }

    #[test]
    fn linear_export_requires_explicit_linear_feedback() {
        let formats = HashSet::from([(DRM_FORMAT_NV12, DRM_FORMAT_MOD_LINEAR)]);

        assert_eq!(
            select_presentation_modifier(&formats, DRM_FORMAT_MOD_LINEAR),
            Some(DRM_FORMAT_MOD_LINEAR)
        );
    }

    #[test]
    fn unrelated_explicit_modifier_is_not_accepted() {
        let actual = 0x0100_0000_0000_0002;
        let formats = HashSet::from([(DRM_FORMAT_NV12, 0)]);

        assert_eq!(select_presentation_modifier(&formats, actual), None);
    }

    #[test]
    fn release_fence_poll_is_nonblocking_and_observes_signal() {
        let mut fds = [-1; 2];
        // SAFETY: fds points to two writable integers; pipe2 initializes both
        // descriptors on success.
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        // SAFETY: successful pipe2 returned two fresh owned descriptors.
        let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        assert!(!fence_signaled(&read));
        let byte = [1_u8];
        // SAFETY: write is live and byte points to one initialized byte.
        assert_eq!(
            unsafe { libc::write(write.as_raw_fd(), byte.as_ptr().cast(), byte.len()) },
            1
        );
        assert!(fence_signaled(&read));
    }
}

/// Present one decoded video frame on the native Wayland surface, trying each
/// path in increasing cost order and reporting which one won.
///
/// Order matters for more than speed. Presenting through GL binds EGL to the
/// surface permanently (`egl_bound`), because a surface cannot be driven by
/// both `eglSwapBuffers` and manual `wl_buffer` attach. So GL is offered only
/// once the cheaper paths are known to be unusable; otherwise a single early
/// GL present locks the session into the most expensive path.
///
/// This lives in one place on purpose: the ladder was previously duplicated in
/// the main loop and the frame-callback path, and the two copies disagreed.
pub(crate) fn present_native_frame(
    backend: &mut WaylandBackend,
    renderer: &mut crate::renderer::Renderer,
    qh: &QueueHandle<WaylandBackend>,
    conn: &Connection,
    output: &str,
    frame: &VideoFrame,
    output_size: (u32, u32),
) -> bool {
    use crate::observability::video_backend::VideoBackendMetricKind;

    if !renderer.can_present_native_wayland_surface() {
        return false;
    }
    // This ladder owns decoder-native DMA-BUF surfaces only. CPU frames and
    // appsink-owned DMA-BUF frames stay in the composed renderer; probing the
    // native surface for them creates an EGL stack that can never present the
    // frame and incorrectly records an import failure.
    if !matches!(frame.format, VideoFrameFormat::NativeDmaBufNv12 { .. }) {
        return false;
    }

    // 1. The decoder surface itself, when the compositor accepts its modifier.
    if backend.try_present_native_dmabuf(qh, output, frame, output_size) {
        renderer.record_native_present_path(VideoBackendMetricKind::NativeDirectSurfacePresent);
        renderer.retain_native_wayland_snapshot(frame);
        renderer.record_native_wayland_surface_presented();
        return true;
    }

    // 2. One Vulkan blit into a linear buffer the compositor can always import.
    let linear_slot_available = match &frame.format {
        VideoFrameFormat::NativeDmaBufNv12 { frame: descriptor } => {
            backend.native_dmabuf_slot_available(output, frame.session_id, descriptor.surface_id)
        }
        _ => false,
    };
    let drm_syncobj = backend.native_drm_syncobj_available();
    let explicit_sync = !drm_syncobj && backend.native_explicit_sync_available();
    if backend.native_dmabuf_accepts_linear()
        && linear_slot_available
        && let Some(bridged) =
            renderer.prepare_linear_native_wayland_frame(frame, drm_syncobj, explicit_sync)
    {
        let sync_tier = match &bridged.format {
            VideoFrameFormat::NativeDmaBufNv12 { frame } if frame.drm_syncobj.is_some() => {
                VideoBackendMetricKind::NativeDrmSyncobjPresent
            }
            VideoFrameFormat::NativeDmaBufNv12 { frame } if frame.acquire_fence.is_some() => {
                VideoBackendMetricKind::NativeSyncFilePresent
            }
            _ => VideoBackendMetricKind::NativeBlockingSyncPresent,
        };
        if backend.try_present_native_dmabuf(qh, output, &bridged, output_size) {
            renderer.record_native_present_path(VideoBackendMetricKind::NativeLinearBridgePresent);
            renderer.record_native_present_path(sync_tier);
            renderer.retain_native_wayland_snapshot(&bridged);
            renderer.record_native_wayland_surface_presented();
            return true;
        }
    }

    // 3. Optional EGLImage -> GL round-trip. Some compositors acknowledge the
    //    swaps without repainting an otherwise idle background subsurface, so
    //    this diagnostic path is opt-in and composition is the safe fallback.
    if !backend.native_dmabuf_accepts_linear()
        && crate::renderer::Renderer::native_gl_surface_requested()
        && let Some((display_ptr, surface_id)) = backend.native_gl_surface_target(qh, conn, output)
        && renderer.try_present_native_gl_surface(frame, display_ptr, surface_id, output_size)
    {
        backend.mark_native_gl_surface_presented(output);
        renderer.retain_native_wayland_snapshot(frame);
        renderer.record_native_wayland_surface_presented();
        return true;
    }

    false
}
