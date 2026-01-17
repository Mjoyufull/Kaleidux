use gstreamer as gst;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
use gst::prelude::*;
use tracing::{info, debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

/// Video frame containing RGBA pixel data (tightly packed, no stride padding)
/// Uses Arc<[u8]> to avoid cloning large buffers
#[derive(Clone)]
pub struct VideoFrame {
    /// RGBA pixel data (tightly packed: width * height * 4 bytes)
    pub data: Arc<[u8]>,
    pub width: u32,
    pub height: u32,
    pub session_id: u64,
}

pub enum VideoEvent {
    Frame(VideoFrame),
    Error(String),
}

pub struct VideoPlayer {
    pub pipeline: gst::Element,
    is_running: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>,
    frame_tx: tokio::sync::mpsc::Sender<(Arc<String>, VideoEvent)>,
    source_id: Arc<String>,
    start_time: std::time::Instant,
}

impl VideoPlayer {
    /// Create a new video player with a bounded channel for backpressure
    pub fn new(uri: &str, source_id: Arc<String>, session_id: u64, frame_tx: tokio::sync::mpsc::Sender<(Arc<String>, VideoEvent)>) -> anyhow::Result<Self> {
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
        appsink.set_drop(true); // Drop frames if late
        appsink.set_max_buffers(1); // Match gSlapper: 1 buffer to minimize latency

        // Keep source_id for closure
        let cb_source_id = source_id.clone();

        // Set up new-sample callback
        let frame_tx_clone = frame_tx.clone();
        let first_frame_logged = Arc::new(AtomicBool::new(false));
        let creation_time_ref = creation_start.clone();
        
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
                    let sample = match sink.pull_sample() {
                        Ok(s) => s,
                        Err(_) => return Err(gst::FlowError::Error),
                    };
                    
                    let buffer = match sample.buffer() {
                        Some(b) => b,
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

                    // Map buffer to read pixel data
                    let map = match buffer.map_readable() {
                        Ok(m) => m,
                        Err(_) => return Err(gst::FlowError::Error),
                    };

                    let width = video_info.width() as u32;
                    let height = video_info.height() as u32;
                    let stride = video_info.stride()[0] as usize;
                    let expected_stride = width as usize * 4;

                    let data = if stride != expected_stride {
                        let src = map.as_slice();
                        let mut dst = Vec::with_capacity(expected_stride * height as usize);
                        for row in 0..height as usize {
                            let row_start = row * stride;
                            let row_end = row_start + expected_stride;
                            if row_end <= src.len() {
                                dst.extend_from_slice(&src[row_start..row_end]);
                            }
                        }
                        Arc::from(dst)
                    } else {
                        Arc::from(map.as_slice())
                    };

                    let frame = VideoFrame {
                        data,
                        width,
                        height,
                        session_id,
                    };

                    // Send frame - log if channel is full (frames being dropped)
                    match frame_tx_clone.try_send((source_id.clone(), VideoEvent::Frame(frame))) {
                        Ok(()) => {
                            // Frame sent successfully
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!("[VIDEO] Frame channel full for {}, frame dropped!", source_id);
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            tracing::warn!("[VIDEO] Frame channel closed for {}, stopping", source_id);
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
            gst::StateChangeSuccess::Success => debug!("[VIDEO] {}: Pipeline state -> Ready (pre-buffered)", self.source_id),
            gst::StateChangeSuccess::Async => debug!("[VIDEO] {}: Pipeline state -> Ready (Async, pre-buffering)", self.source_id),
            _ => {}
        }
        Ok(())
    }
    
    pub fn start(&mut self) -> anyhow::Result<()> {
        info!("[VIDEO] {}: Starting playback for {}", self.source_id, self.pipeline.name());
        
        // Start pipeline (or transition from Ready to Playing if pre-buffered)
        let ret = self.pipeline.set_state(gst::State::Playing)?;
        let duration = self.start_time.elapsed();
        match ret {
            gst::StateChangeSuccess::Success => info!("[VIDEO] {}: Pipeline state -> Playing in {:.3}ms", self.source_id, duration.as_secs_f64() * 1000.0),
            gst::StateChangeSuccess::Async => info!("[VIDEO] {}: Pipeline state -> Playing (Async) in {:.3}ms", self.source_id, duration.as_secs_f64() * 1000.0),
            gst::StateChangeSuccess::NoPreroll => info!("[VIDEO] {}: Pipeline state -> Playing (Live) in {:.3}ms", self.source_id, duration.as_secs_f64() * 1000.0),
        }
        
        // Spawn bus watcher
        let bus = self.pipeline.bus().ok_or_else(|| anyhow::anyhow!("Pipeline has no bus"))?;
        let pipeline = self.pipeline.clone();
        
        self.is_running.store(true, Ordering::SeqCst);
        let is_running = self.is_running.clone();
        let frame_tx = self.frame_tx.clone();
        let source_id = self.source_id.clone();
        
        let handle = std::thread::spawn(move || {
            while is_running.load(Ordering::SeqCst) {
                // Wait for up to 100ms for a message
                match bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
                    Some(msg) => {
                        use gst::MessageView;
                        match msg.view() {
                            MessageView::StateChanged(s) if s.src().as_ref().map(|src| src.as_ptr() as usize == pipeline.as_ptr() as usize).unwrap_or(false) => {
                                debug!("[VIDEO] {}: Pipeline state changed from {:?} to {:?}", source_id, s.old(), s.current());
                            }
                            MessageView::Eos(..) => {
                                info!("[VIDEO] {}: End of Stream reached, looping...", source_id);
                                // Use segment-based seeking for seamless audio (like gSlapper)
                                // SEGMENT flag produces gapless looping, FLUSH causes audio gaps
                                if !pipeline.seek_simple(
                                    gst::SeekFlags::FLUSH | gst::SeekFlags::SEGMENT,
                                    gst::ClockTime::ZERO,
                                ).is_ok() {
                                    tracing::error!("Failed to seek to start for loop");
                                }
                            }
                            MessageView::SegmentDone(..) => {
                                // Seamless loop restart when using segment-based seeking
                                if !pipeline.seek_simple(
                                    gst::SeekFlags::SEGMENT,
                                    gst::ClockTime::ZERO,
                                ).is_ok() {
                                    tracing::error!("Failed to segment seek for loop");
                                }
                            }
                            MessageView::Error(err) => {
                                let error_msg = format!("Error from {:?}: {} ({:?})", 
                                    err.src().map(|s| s.path_string()),
                                    err.error(),
                                    err.debug());
                                
                                tracing::error!("{}", error_msg);
                                
                                // Send error event to main thread
                                let _ = frame_tx.blocking_send((source_id.clone(), VideoEvent::Error(error_msg)));
                                
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
