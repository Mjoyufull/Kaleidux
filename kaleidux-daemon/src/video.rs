use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::Semaphore;
use tracing::{debug, info};

/// Video frame containing RGBA pixel data
/// Uses gst::Buffer to avoid copying data
#[derive(Clone)]
pub struct VideoFrame {
    pub buffer: gst::Buffer,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub session_id: u64,
}

pub enum VideoEvent {
    Frame(VideoFrame),
    Error(String),
}

/// Shared thread pool for GStreamer bus watchers
/// Uses a semaphore to limit concurrent bus watcher threads
pub struct BusWatcherPool {
    semaphore: Arc<Semaphore>,
}

impl BusWatcherPool {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    pub fn default() -> Self {
        // Default to 8 concurrent bus watchers (enough for multiple videos)
        Self::new(8)
    }
}

// Global bus watcher pool (lazy initialized)
static BUS_WATCHER_POOL: once_cell::sync::Lazy<Arc<BusWatcherPool>> =
    once_cell::sync::Lazy::new(|| Arc::new(BusWatcherPool::default()));

pub fn get_bus_watcher_pool() -> Arc<BusWatcherPool> {
    BUS_WATCHER_POOL.clone()
}

pub struct VideoPlayer {
    pub pipeline: gst::Element,
    is_running: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>, // Keep for compatibility, but will use thread pool
    frame_tx: tokio::sync::mpsc::Sender<(Arc<String>, VideoEvent)>,
    source_id: Arc<String>,
    start_time: std::time::Instant,
}

impl VideoPlayer {
    /// Create a new video player with a bounded channel for backpressure
    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        frame_tx: tokio::sync::mpsc::Sender<(Arc<String>, VideoEvent)>,
    ) -> anyhow::Result<Self> {
        let _video_start = std::time::Instant::now();
        let creation_start = std::time::Instant::now();
        // Use playbin - the same high-level element that gSlapper uses
        let pipeline = gst::ElementFactory::make("playbin")
            .name("playbin")
            .build()?;

        // Set the URI
        let full_uri = if uri.contains("://") {
            uri.to_string()
        } else {
            // Convert local path to file:// URI
            let path = std::path::Path::new(uri);
            let abs_path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            };
            format!("file://{}", abs_path.display())
        };

        info!("Setting video URI: {}", full_uri);
        pipeline.set_property("uri", &full_uri);

        // Note: Removed buffer-size property setting - it expects gint (i32) not u64
        // and may not be necessary for preventing memory leaks

        // Default flags (video+audio+text+softvolume) are usually fine.
        // Explicitly setting them to 3 (video+audio) requires the GstPlayFlags type.
        // pipeline.set_property("flags", 3u32);

        // Create appsink for video frames - configure like gSlapper does
        let appsink = gst::ElementFactory::make("appsink")
            .name("video-sink")
            .build()?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast to AppSink"))?;

        // Configure appsink to output RGBA frames (same as gSlapper)
        let caps = gst::Caps::builder("video/x-raw")
            .field("format", "RGBA")
            .build();

        appsink.set_caps(Some(&caps));
        appsink.set_sync(true); // Sync to clock
        appsink.set_drop(true); // Drop frames if late - CRITICAL for preventing buffer accumulation
        appsink.set_max_buffers(1); // Match gSlapper: 1 buffer to minimize latency and memory
                                    // CRITICAL: Enable emit-signals to get callbacks, but ensure we handle them quickly
                                    // The new_sample callback will be called for each frame

        // Keep source_id for closure
        let cb_source_id = source_id.clone();

        // Set up new-sample callback
        let frame_tx_clone = frame_tx.clone();
        let first_frame_logged = Arc::new(AtomicBool::new(false));
        let creation_time_ref = creation_start;

        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let source_id = cb_source_id.clone();

                    if !first_frame_logged.load(Ordering::SeqCst) {
                        first_frame_logged.store(true, Ordering::SeqCst);
                        let duration = creation_time_ref.elapsed();
                        info!("[ASSET] {}: First video frame produced in {:.3}ms", source_id, duration.as_secs_f64() * 1000.0);
                    }

                    let session_id = session_id;

                    // CRITICAL: Pull sample and extract buffer in explicit scope
                    // This ensures sample is dropped immediately after buffer extraction
                    let (buffer, width, height, stride) = {
                        let sample = match sink.pull_sample() {
                            Ok(s) => s,
                            Err(_) => return Err(gst::FlowError::Error),
                        };

                        let buffer = match sample.buffer() {
                            Some(b) => b.to_owned(),
                            None => return Err(gst::FlowError::Error),
                        };

                        let caps = match sample.caps() {
                            Some(c) => c,
                            None => return Err(gst::FlowError::Error),
                        };

                        let video_info = match gst_video::VideoInfo::from_caps(caps) {
                            Ok(vi) => vi,
                            Err(_) => return Err(gst::FlowError::Error),
                        };

                        let width = video_info.width();
                        let height = video_info.height();
                        let stride = video_info.stride()[0] as u32;

                        // sample is dropped here, releasing GStreamer sample resources
                        (buffer, width, height, stride)
                    };

                    let frame = VideoFrame {
                        buffer,
                        width,
                        height,
                        stride,
                        session_id,
                    };

                    // Send frame - if channel is full, drop frame immediately to release gst::Buffer
                    match frame_tx_clone.try_send((source_id.clone(), VideoEvent::Frame(frame))) {
                        Ok(()) => {
                            // Frame sent successfully
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            // CRITICAL: Channel full - drop frame immediately to release gst::Buffer
                            // This prevents buffer accumulation in GStreamer's internal pool
                            tracing::warn!("[VIDEO] Frame channel full for {}, dropping frame and releasing buffer", source_id);
                            // frame is dropped here, releasing the gst::Buffer
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            tracing::warn!("[VIDEO] Frame channel closed for {}, stopping", source_id);
                            // frame is dropped here
                            return Err(gst::FlowError::Eos);
                        }
                    }

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        // Configure appsink
        appsink.set_property("drop", true);
        appsink.set_property("max-buffers", 1u32);

        // Set appsink as the video sink
        pipeline.set_property("video-sink", &appsink);

        info!("VideoPlayer created with playbin + appsink (RGBA mode)");

        Ok(Self {
            pipeline,
            is_running: Arc::new(AtomicBool::new(false)),
            thread_handle: None,
            frame_tx,
            source_id,
            start_time: creation_start,
        })
    }

    /// Pre-buffer video by setting pipeline to READY state (buffers but doesn't play)
    pub fn prebuffer(&mut self) -> anyhow::Result<()> {
        debug!("[VIDEO] {}: Pre-buffering video pipeline", self.source_id);
        let ret = self.pipeline.set_state(gst::State::Ready)?;
        match ret {
            gst::StateChangeSuccess::Success => debug!(
                "[VIDEO] {}: Pipeline state -> Ready (pre-buffered)",
                self.source_id
            ),
            gst::StateChangeSuccess::Async => debug!(
                "[VIDEO] {}: Pipeline state -> Ready (Async, pre-buffering)",
                self.source_id
            ),
            _ => {}
        }
        Ok(())
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        info!(
            "[VIDEO] {}: Starting playback for {}",
            self.source_id,
            self.pipeline.name()
        );

        // Start pipeline (or transition from Ready to Playing if pre-buffered)
        let ret = self.pipeline.set_state(gst::State::Playing)?;
        let duration = self.start_time.elapsed();
        match ret {
            gst::StateChangeSuccess::Success => info!(
                "[VIDEO] {}: Pipeline state -> Playing in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
            gst::StateChangeSuccess::Async => info!(
                "[VIDEO] {}: Pipeline state -> Playing (Async) in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
            gst::StateChangeSuccess::NoPreroll => info!(
                "[VIDEO] {}: Pipeline state -> Playing (Live) in {:.3}ms",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            ),
        }

        // Spawn bus watcher using thread pool with semaphore to limit concurrent threads
        let bus = self
            .pipeline
            .bus()
            .ok_or_else(|| anyhow::anyhow!("Pipeline has no bus"))?;
        let pipeline = self.pipeline.clone();

        self.is_running.store(true, Ordering::SeqCst);
        let is_running = self.is_running.clone();
        let frame_tx = self.frame_tx.clone();
        let source_id = self.source_id.clone();
        let pool = get_bus_watcher_pool();
        let semaphore = pool.semaphore.clone();

        // Spawn thread but use semaphore to limit concurrent bus watchers
        // Note: We spawn a std::thread but the semaphore limits how many can run concurrently
        // The semaphore is acquired synchronously before the thread starts its loop
        // Capture runtime handle from caller context (must be called from within a Tokio runtime/task)
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                tracing::error!(
                    "[VIDEO] No tokio runtime available for start() caller of {}",
                    self.source_id
                );
                return Err(anyhow::anyhow!("No tokio runtime available"));
            }
        };

        // Spawn thread but use semaphore to limit concurrent bus watchers
        // Note: We spawn a std::thread but the semaphore limits how many can run concurrently
        // The semaphore is acquired synchronously before the thread starts its loop
        let handle = std::thread::spawn(move || {
            // Acquire permit - block until available to ensure proper resource control
            // Using runtime block_on in a thread ensures threads wait when pool is at capacity
            let _permit = match rt.block_on(semaphore.acquire_owned()) {
                Ok(p) => p,
                Err(_) => {
                    tracing::error!("[VIDEO] Semaphore closed unexpectedly for {}", source_id);
                    return;
                }
            };

            while is_running.load(Ordering::SeqCst) {
                // Wait for up to 100ms for a message
                match bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
                    Some(msg) => {
                        use gst::MessageView;
                        match msg.view() {
                            MessageView::StateChanged(s)
                                if s.src()
                                    .as_ref()
                                    .map(|src| {
                                        std::ptr::eq(
                                            src.as_ptr() as *const std::ffi::c_void,
                                            pipeline.as_ptr() as *const std::ffi::c_void,
                                        )
                                    })
                                    .unwrap_or(false) =>
                            {
                                debug!(
                                    "[VIDEO] {}: Pipeline state changed from {:?} to {:?}",
                                    source_id,
                                    s.old(),
                                    s.current()
                                );
                            }
                            MessageView::Eos(..) => {
                                info!("[VIDEO] {}: End of Stream reached, looping...", source_id);
                                // Use segment-based seeking for seamless audio (like gSlapper)
                                // SEGMENT flag produces gapless looping, FLUSH causes audio gaps
                                if pipeline
                                    .seek_simple(
                                        gst::SeekFlags::FLUSH | gst::SeekFlags::SEGMENT,
                                        gst::ClockTime::ZERO,
                                    )
                                    .is_err()
                                {
                                    tracing::error!("Failed to seek to start for loop");
                                }
                            }
                            MessageView::SegmentDone(..) => {
                                // Seamless loop restart when using segment-based seeking
                                if pipeline
                                    .seek_simple(gst::SeekFlags::SEGMENT, gst::ClockTime::ZERO)
                                    .is_err()
                                {
                                    tracing::error!("Failed to segment seek for loop");
                                }
                            }
                            MessageView::Error(err) => {
                                let error_msg = format!(
                                    "Error from {:?}: {} ({:?})",
                                    err.src().map(|s| s.path_string()),
                                    err.error(),
                                    err.debug()
                                );

                                tracing::error!("{}", error_msg);

                                // Send error event to main thread
                                let _ = frame_tx.blocking_send((
                                    source_id.clone(),
                                    VideoEvent::Error(error_msg),
                                ));

                                // Stop loop
                                break;
                            }
                            _ => (),
                        }
                    }
                    None => {
                        // Timeout, loop again and check is_running
                    }
                }
            }
            info!("Bus watcher thread exiting.");
        });

        self.thread_handle = Some(handle);

        Ok(())
    }
    pub fn stop(&mut self) -> anyhow::Result<()> {
        if !self.is_running.load(Ordering::SeqCst) {
            return Ok(());
        }
        info!("Stopping video playback...");

        // 1. Fade audio to prevent clicks/pops during transition
        self.pipeline.set_property("volume", 0.0);

        // 2. Signal thread to stop
        self.is_running.store(false, Ordering::SeqCst);

        // 3. Pause first (transition to Ready state first helps cleanup)
        let _ = self.pipeline.set_state(gst::State::Paused);

        // 4. Set pipeline to Null (this stops data flow)
        //    Note: We removed the 50ms sleep as it was blocking the Wayland event loop
        //    and causing compositor disconnects when multiple transitions happen quickly
        self.pipeline.set_state(gst::State::Null)?;

        // 5. Join thread
        // NOTE: This can block if the bus watcher thread is stuck waiting on GStreamer messages.
        // In practice, setting is_running=false and pipeline state to Null should cause the
        // bus watcher to exit quickly. If this blocks indefinitely, it indicates a GStreamer
        // issue that should be investigated.
        if let Some(handle) = self.thread_handle.take() {
            match handle.join() {
                Ok(()) => {
                    // Thread exited normally
                }
                Err(_) => {
                    tracing::error!("Bus watcher thread panicked during cleanup");
                }
            }
        }

        Ok(())
    }

    pub fn set_volume(&mut self, volume: f64) {
        self.pipeline.set_property("volume", volume);
    }
}

impl Drop for VideoPlayer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
