use super::{CudaTextureCache, WgpuContext};
use std::sync::Arc;

/// Retire Vulkan reads on their serialized queue, then finish CUDA on a worker.
/// A stalled driver cannot hold the display or command loop during retirement.
pub(super) fn retire(context: Arc<WgpuContext>, cache: CudaTextureCache) {
    let retirement_context = context.clone();
    context.with_raw_queue_lock(|| {
        context.queue.submit(std::iter::empty());
        context.queue.on_submitted_work_done(move || {
            std::thread::spawn(move || {
                let interop = retirement_context.cuda_interop.lock().clone();
                let Some(interop) = interop else {
                    tracing::error!(
                        "[CUDA-VK] retirement lost its CUDA context; retaining allocations"
                    );
                    std::mem::forget(cache);
                    return;
                };
                if let Err(error) = interop.synchronize() {
                    // A failed stream may still access these allocations. Keep
                    // the bounded failed cache and context until process exit.
                    tracing::error!("[CUDA-VK] retirement synchronization failed: {error}");
                    std::mem::forget((cache, retirement_context));
                    return;
                }
                drop(cache.timeline);
                drop(cache.in_flight_frames);
                drop(cache.y_view);
                drop(cache.uv_view);
                drop(cache.y_texture);
                drop(cache.uv_texture);
                interop.free_exportable(cache.y_cuda_alloc);
                interop.free_exportable(cache.uv_cuda_alloc);
            });
        });
    });
    context.request_device_poll();
}
