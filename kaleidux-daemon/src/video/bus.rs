use gstreamer as gst;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use tracing::{info, warn};

pub(super) struct BusWatchHandle {
    context: gst::glib::MainContext,
    source_id: gst::glib::SourceId,
}

impl BusWatchHandle {
    pub(super) fn remove(self) {
        if let Some(source) = self.context.find_source_by_id(&self.source_id) {
            source.destroy();
        }
    }
}

struct BusDispatcher {
    context: gst::glib::MainContext,
    main_loop: gst::glib::MainLoop,
    thread: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
    finished_rx: parking_lot::Mutex<Option<mpsc::Receiver<()>>>,
    shutdown_started: AtomicBool,
}

impl BusDispatcher {
    fn new() -> Self {
        let context = gst::glib::MainContext::new();
        let main_loop = gst::glib::MainLoop::new(Some(&context), false);
        let context_for_thread = context.clone();
        let main_loop_for_thread = main_loop.clone();
        let (finished_tx, finished_rx) = mpsc::channel();

        let thread = std::thread::Builder::new()
            .name("kaleidux-gst-bus".to_string())
            .spawn(move || {
                context_for_thread
                    .with_thread_default(|| {
                        info!("[VIDEO] Shared GStreamer bus dispatcher started");
                        main_loop_for_thread.run();
                    })
                    .expect("failed to set GLib thread-default context for bus dispatcher");
                let _ = finished_tx.send(());
            })
            .expect("failed to spawn GStreamer bus dispatcher");

        Self {
            context,
            main_loop,
            thread: parking_lot::Mutex::new(Some(thread)),
            finished_rx: parking_lot::Mutex::new(Some(finished_rx)),
            shutdown_started: AtomicBool::new(false),
        }
    }

    pub(super) fn attach(&self, source: gst::glib::Source) -> BusWatchHandle {
        let source_id = source.attach(Some(&self.context));
        BusWatchHandle {
            context: self.context.clone(),
            source_id,
        }
    }

    fn shutdown(&self, timeout: std::time::Duration) {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return;
        }

        self.main_loop.quit();

        if let Some(receiver) = self.finished_rx.lock().take()
            && receiver.recv_timeout(timeout).is_err()
        {
            warn!(
                "[VIDEO] Shared GStreamer bus dispatcher did not exit within {:.1}ms",
                timeout.as_secs_f64() * 1000.0
            );
            return;
        }

        if let Some(thread) = self.thread.lock().take() {
            let _ = thread.join();
        }
    }
}

static BUS_DISPATCHER: once_cell::sync::Lazy<BusDispatcher> =
    once_cell::sync::Lazy::new(BusDispatcher::new);

pub(super) fn attach_bus_watch(source: gst::glib::Source) -> BusWatchHandle {
    BUS_DISPATCHER.attach(source)
}

pub fn shutdown_bus_dispatcher(timeout: std::time::Duration) {
    BUS_DISPATCHER.shutdown(timeout);
}
