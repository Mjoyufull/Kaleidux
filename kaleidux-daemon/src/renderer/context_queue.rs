use super::WgpuContext;
use std::sync::atomic::Ordering;

impl WgpuContext {
    pub(crate) fn submit(
        &self,
        command_buffers: impl IntoIterator<Item = wgpu::CommandBuffer>,
    ) -> wgpu::SubmissionIndex {
        let _queue_guard = self.queue_lock.lock();
        self.queue.submit(command_buffers)
    }

    pub(crate) fn write_buffer(&self, buffer: &wgpu::Buffer, offset: u64, data: &[u8]) {
        let _queue_guard = self.queue_lock.lock();
        self.queue.write_buffer(buffer, offset, data);
    }

    pub(crate) fn write_texture(
        &self,
        texture: wgpu::ImageCopyTexture<'_>,
        data: &[u8],
        data_layout: wgpu::ImageDataLayout,
        size: wgpu::Extent3d,
    ) {
        let _queue_guard = self.queue_lock.lock();
        self.queue.write_texture(texture, data, data_layout, size);
    }

    pub(crate) fn with_raw_queue_lock<T>(&self, operation: impl FnOnce() -> T) -> T {
        let _queue_guard = self.queue_lock.lock();
        operation()
    }

    pub(crate) fn request_device_poll(&self) {
        self.device_poll_requested.store(true, Ordering::Release);
    }

    pub(crate) fn poll_device_if_requested(&self) -> bool {
        if self.device_poll_requested.swap(false, Ordering::AcqRel) {
            self.device.poll(wgpu::Maintain::Poll);
            true
        } else {
            false
        }
    }
}
