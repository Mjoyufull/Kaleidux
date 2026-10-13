use drm::control::{Device as ControlDevice, syncobj};
use drm::{Device, DriverCapability};
use std::fs::{File, OpenOptions};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct DrmSyncobjDevice {
    file: File,
    path: String,
}

impl AsFd for DrmSyncobjDevice {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }
}

impl Device for DrmSyncobjDevice {}
impl ControlDevice for DrmSyncobjDevice {}

impl DrmSyncobjDevice {
    pub(crate) fn open_render_node() -> anyhow::Result<Arc<Self>> {
        let mut errors = Vec::new();
        for minor in 128..=191 {
            let path = format!("/dev/dri/renderD{minor}");
            let file = match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    errors.push(format!("{path}: {error}"));
                    continue;
                }
            };
            let device = Arc::new(Self {
                file,
                path: path.clone(),
            });
            match device.get_driver_capability(DriverCapability::TimelineSyncObj) {
                Ok(value) if value != 0 => return Ok(device),
                Ok(_) => errors.push(format!("{path}: timeline syncobj unsupported")),
                Err(error) => errors.push(format!("{path}: capability query failed: {error}")),
            }
        }
        anyhow::bail!(
            "no DRM render node with timeline syncobj support ({})",
            errors.join("; ")
        )
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn create_timeline(self: &Arc<Self>) -> anyhow::Result<DrmSyncobjTimeline> {
        let handle = self.create_syncobj(false)?;
        let export_fd = match self.syncobj_to_fd(handle, false) {
            Ok(fd) => fd,
            Err(error) => {
                let _ = self.destroy_syncobj(handle);
                return Err(error.into());
            }
        };
        Ok(DrmSyncobjTimeline {
            inner: Arc::new(DrmSyncobjTimelineInner {
                device: self.clone(),
                handle,
                export_fd,
            }),
        })
    }
}

struct DrmSyncobjTimelineInner {
    device: Arc<DrmSyncobjDevice>,
    handle: syncobj::Handle,
    export_fd: OwnedFd,
}

impl std::fmt::Debug for DrmSyncobjTimelineInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DrmSyncobjTimeline")
            .field("device", &self.device.path)
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl Drop for DrmSyncobjTimelineInner {
    fn drop(&mut self) {
        let _ = self.device.destroy_syncobj(self.handle);
    }
}

#[derive(Debug, Clone)]
pub struct DrmSyncobjTimeline {
    inner: Arc<DrmSyncobjTimelineInner>,
}

impl DrmSyncobjTimeline {
    pub(crate) fn export_fd(&self) -> BorrowedFd<'_> {
        self.inner.export_fd.as_fd()
    }

    pub(crate) fn import_sync_file_at(
        &self,
        sync_file: BorrowedFd<'_>,
        point: u64,
    ) -> anyhow::Result<()> {
        #[repr(C)]
        struct DrmSyncobjHandle {
            handle: u32,
            flags: u32,
            fd: i32,
            pad: u32,
            point: u64,
        }

        // DRM_IOWR(0xC2, struct drm_syncobj_handle), expressed locally because
        // drm-rs 0.14's safe fd_to_syncobj helper always passes handle=0 and
        // cannot perform the timeline-point form of this UAPI operation.
        const IOC_WRITE: libc::c_ulong = 1;
        const IOC_READ: libc::c_ulong = 2;
        const IOC_NRSHIFT: u32 = 0;
        const IOC_TYPESHIFT: u32 = 8;
        const IOC_SIZESHIFT: u32 = 16;
        const IOC_DIRSHIFT: u32 = 30;
        const DRM_IOCTL_SYNCOBJ_FD_TO_HANDLE: libc::c_ulong = ((IOC_READ | IOC_WRITE)
            << IOC_DIRSHIFT)
            | ((std::mem::size_of::<DrmSyncobjHandle>() as libc::c_ulong) << IOC_SIZESHIFT)
            | ((b'd' as libc::c_ulong) << IOC_TYPESHIFT)
            | ((0xC2_u32 as libc::c_ulong) << IOC_NRSHIFT);
        let mut request = DrmSyncobjHandle {
            handle: self.inner.handle.into(),
            // DRM_SYNCOBJ_FD_TO_HANDLE_FLAGS_IMPORT_SYNC_FILE | TIMELINE
            flags: 1 | 2,
            fd: sync_file.as_raw_fd(),
            pad: 0,
            point,
        };
        // SAFETY: request matches the Linux UAPI layout and both descriptors
        // remain live through this synchronous ioctl.
        let result = unsafe {
            libc::ioctl(
                self.inner.device.as_fd().as_raw_fd(),
                DRM_IOCTL_SYNCOBJ_FD_TO_HANDLE,
                std::ptr::addr_of_mut!(request),
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    pub fn is_signaled(&self, point: u64) -> bool {
        self.inner
            .device
            .syncobj_timeline_wait(&[self.inner.handle], &[point], 0, true, false, false)
            .is_ok()
    }
}
