use super::{CUDA_SUCCESS, CUcontext, CUresult, CUstream, CudaInterop, cuda_err};
use crate::video::{NativeCudaFrame, VideoFrame, VideoFrameStorage};
use std::sync::Arc;
use tracing::error;

type Event = *mut std::ffi::c_void;
type Create = unsafe extern "C" fn(*mut Event, u32) -> CUresult;
type Record = unsafe extern "C" fn(Event, CUstream) -> CUresult;
type Wait = unsafe extern "C" fn(CUstream, Event, u32) -> CUresult;
type Destroy = unsafe extern "C" fn(Event) -> CUresult;

pub(super) struct SourceEventFns {
    create: Create,
    record: Record,
    wait: Wait,
    destroy: Destroy,
}

impl SourceEventFns {
    pub(super) unsafe fn load(library: &libloading::Library) -> Result<Self, String> {
        // SAFETY: these are the CUDA driver signatures; the parent retains Library.
        unsafe {
            Ok(Self {
                create: load_fn!(library, b"cuEventCreate\0"),
                record: load_fn!(library, b"cuEventRecord\0"),
                wait: load_fn!(library, b"cuStreamWaitEvent\0"),
                destroy: load_fn!(library, b"cuEventDestroy_v2\0"),
            })
        }
    }
}

impl CudaInterop {
    /// Order our copies after FFmpeg's decoder stream without waiting on the CPU.
    pub fn wait_for_source(&self, source: &CudaMapGuard) -> Result<(), String> {
        let CudaMapGuard::Native(frame) = source else {
            return Ok(());
        };
        let _guard = self.op_lock.lock();
        let mut event = std::ptr::null_mut();
        // SAFETY: the source guard retains the frame and its CUDA device/stream.
        // CUDA permits an event dependency between streams in different contexts.
        unsafe {
            let result = (self.cu_ctx_set_current)(frame.context as CUcontext);
            if result != CUDA_SUCCESS {
                return Err(cuda_err("cuCtxSetCurrent(source)", result));
            }
            let created = (self.source_events.create)(&mut event, 2); // DISABLE_TIMING
            let recorded = if created == CUDA_SUCCESS {
                (self.source_events.record)(event, frame.stream as CUstream)
            } else {
                created
            };
            let restored = self.push_context();
            let waited = if recorded == CUDA_SUCCESS && restored.is_ok() {
                (self.source_events.wait)(self.stream, event, 0)
            } else {
                recorded
            };
            if !event.is_null() {
                // Destruction defers freeing a recorded event until pending waits finish.
                (self.source_events.destroy)(event);
            }
            restored?;
            if waited != CUDA_SUCCESS {
                return Err(cuda_err("CUDA producer event", waited));
            }
        }
        Ok(())
    }
}

pub enum CudaMapGuard {
    Gstreamer {
        buffer: gstreamer::Buffer,
        map: gstreamer::ffi::GstMapInfo,
    },
    Native(Arc<NativeCudaFrame>),
}

// SAFETY: each variant retains its allocation. Device pointers are opaque and
// all copy operations are serialized by CudaInterop, with producer dependencies.
unsafe impl Send for CudaMapGuard {}

impl CudaMapGuard {
    pub fn device_ptr(&self) -> u64 {
        match self {
            Self::Gstreamer { map, .. } => map.data as u64,
            Self::Native(frame) => frame.device_ptr,
        }
    }
}

impl Drop for CudaMapGuard {
    fn drop(&mut self) {
        if let Self::Gstreamer { buffer, map } = self {
            // SAFETY: each successful map is unmapped once while its buffer is live.
            unsafe {
                gstreamer::ffi::gst_buffer_unmap(buffer.as_ptr() as *mut _, map);
            }
        }
    }
}

pub fn map_frame_cuda(frame: &VideoFrame) -> Option<CudaMapGuard> {
    if let VideoFrameStorage::Native(owner) = &frame.storage {
        return owner
            .clone()
            .downcast::<NativeCudaFrame>()
            .ok()
            .map(CudaMapGuard::Native);
    }
    let buffer = frame.storage.gstreamer_buffer()?;
    // SAFETY: GstMapInfo is initialized by gst_buffer_map; the guard owns buffer.
    unsafe {
        let mut map = std::mem::zeroed::<gstreamer::ffi::GstMapInfo>();
        let flags = gstreamer::ffi::GST_MAP_READ | (1 << 17); // GST_MAP_CUDA
        if gstreamer::ffi::gst_buffer_map(buffer.as_ptr() as *mut _, &mut map, flags) == 0 {
            error!("[CUDA] CUDA buffer map failed");
            return None;
        }
        if map.data.is_null() {
            gstreamer::ffi::gst_buffer_unmap(buffer.as_ptr() as *mut _, &mut map);
            return None;
        }
        Some(CudaMapGuard::Gstreamer {
            buffer: buffer.clone(),
            map,
        })
    }
}
