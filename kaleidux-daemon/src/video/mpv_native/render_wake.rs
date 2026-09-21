use std::ffi::c_void;
use std::os::fd::RawFd;
#[cfg(test)]
use std::time::Duration;

/// Eventfd bridge from libmpv's update callback to its dedicated render thread.
///
/// The callback is intentionally limited to a nonblocking write. The render
/// thread drains the descriptor and calls the render API, as required by mpv.
pub(crate) struct RenderWake {
    fd: Box<RawFd>,
}

impl RenderWake {
    pub(crate) fn new() -> anyhow::Result<Self> {
        // SAFETY: eventfd has no borrowed pointer arguments and returns an
        // owned descriptor on success.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            anyhow::bail!("eventfd failed: {}", std::io::Error::last_os_error());
        }
        Ok(Self { fd: Box::new(fd) })
    }

    pub(super) fn callback_context(&self) -> *mut c_void {
        self.fd.as_ref() as *const RawFd as *mut c_void
    }

    pub(crate) fn signal(&self) {
        let value = 1u64;
        // SAFETY: fd is a live nonblocking eventfd and value is readable for one u64.
        unsafe {
            libc::write(
                *self.fd,
                &value as *const u64 as *const c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// Block until either libmpv has an update or shutdown explicitly wakes the
    /// render thread. Returns `true` for an update and `false` for shutdown.
    pub(crate) fn wait_for_update_or_stop(&self, stop: &Self) -> bool {
        let mut poll_fds = [
            libc::pollfd {
                fd: *self.fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: *stop.fd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: poll_fds contains two initialized pollfd values for live eventfds.
        let ret = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as _, -1) };
        if ret <= 0 {
            return false;
        }
        if poll_fds[1].revents & libc::POLLIN != 0 {
            stop.drain();
            return false;
        }
        if poll_fds[0].revents & libc::POLLIN == 0 {
            return false;
        }
        self.drain();
        true
    }

    #[cfg(test)]
    fn wait_timeout(&self, timeout: Duration) -> bool {
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut poll_fd = libc::pollfd {
            fd: *self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll_fd points to one initialized pollfd for the call.
        let ret = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
        if ret <= 0 || poll_fd.revents & libc::POLLIN == 0 {
            return false;
        }
        self.drain();
        true
    }

    fn drain(&self) {
        let mut value = 0u64;
        loop {
            // SAFETY: value is writable for one u64 and fd remains live.
            let read = unsafe {
                libc::read(
                    *self.fd,
                    &mut value as *mut u64 as *mut c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if read as usize != std::mem::size_of::<u64>() {
                break;
            }
        }
    }
}

impl Drop for RenderWake {
    fn drop(&mut self) {
        // SAFETY: this object owns the descriptor and closes it exactly once.
        unsafe { libc::close(*self.fd) };
    }
}

pub(super) unsafe extern "C" fn render_update_callback(callback_ctx: *mut c_void) {
    // SAFETY: callback_ctx is the boxed RawFd retained by RenderWake.
    let Some(fd) = (unsafe { (callback_ctx as *const RawFd).as_ref() }) else {
        return;
    };
    let value = 1u64;
    // SAFETY: fd is a live nonblocking eventfd and value is readable for one u64.
    unsafe {
        libc::write(
            *fd,
            &value as *const u64 as *const c_void,
            std::mem::size_of::<u64>(),
        )
    };
}

#[cfg(test)]
mod tests {
    use super::{RenderWake, render_update_callback};
    use std::time::Duration;

    #[test]
    fn callback_wakes_and_coalesces_render_thread_notifications() {
        let wake = RenderWake::new().unwrap();
        // SAFETY: callback_context points to wake's live boxed eventfd.
        unsafe {
            render_update_callback(wake.callback_context());
            render_update_callback(wake.callback_context());
        }
        assert!(wake.wait_timeout(Duration::from_millis(50)));
        assert!(!wake.wait_timeout(Duration::ZERO));
    }

    #[test]
    fn stop_wake_interrupts_an_indefinite_render_wait() {
        let update = std::sync::Arc::new(RenderWake::new().unwrap());
        let stop = std::sync::Arc::new(RenderWake::new().unwrap());
        let waiter_update = update.clone();
        let waiter_stop = stop.clone();
        let waiter =
            std::thread::spawn(move || waiter_update.wait_for_update_or_stop(&waiter_stop));
        stop.signal();
        assert!(!waiter.join().unwrap());
    }
}
