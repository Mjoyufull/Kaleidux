use gst::prelude::*;
use gstreamer as gst;
use gstreamer_allocators as gst_alloc;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
use std::os::unix::io::{FromRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::thread::JoinHandle;
use tokio::sync::Semaphore;
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoMode {
    Auto,
    ForceDmaBuf,
    ForceCuda,
    ForceNv12,
    ForceRgba,
}

static VIDEO_MODE: AtomicU8 = AtomicU8::new(0);
const BUS_WATCHER_POLL_MS: u64 = 25;

pub fn set_video_mode(mode: VideoMode) {
    let val = match mode {
        VideoMode::Auto => 0,
        VideoMode::ForceDmaBuf => 1,
        VideoMode::ForceNv12 => 2,
        VideoMode::ForceRgba => 3,
        VideoMode::ForceCuda => 4,
    };
    VIDEO_MODE.store(val, Ordering::SeqCst);
    info!("[VIDEO] Video mode set to {:?}", mode);
}

pub fn get_video_mode() -> VideoMode {
    match VIDEO_MODE.load(Ordering::SeqCst) {
        1 => VideoMode::ForceDmaBuf,
        2 => VideoMode::ForceNv12,
        3 => VideoMode::ForceRgba,
        4 => VideoMode::ForceCuda,
        _ => VideoMode::Auto,
    }
}

fn audio_enabled_for_volume(volume: f64) -> bool {
    volume > f64::EPSILON
}

fn playbin_flags_for_volume(volume: f64) -> &'static str {
    if audio_enabled_for_volume(volume) {
        "video+audio"
    } else {
        "video"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_volume_disables_audio_pipeline() {
        assert!(!audio_enabled_for_volume(0.0));
        assert_eq!(playbin_flags_for_volume(0.0), "video");
    }

    #[test]
    fn positive_volume_keeps_audio_pipeline() {
        assert!(audio_enabled_for_volume(0.01));
        assert_eq!(playbin_flags_for_volume(0.01), "video+audio");
    }
}

/// Configure GStreamer decoder element ranks based on detected GPU vendor.
/// On NVIDIA: boost nvcodec decoders above VA-API so GStreamer picks native
/// NVDEC (which can export DMA-BUF via cudadownload) instead of the VA-API
/// shim (nvidia-vaapi-driver, which cannot export DMA-BUF).
pub fn configure_hw_decoders() {
    let has_nvidia = std::fs::metadata("/proc/driver/nvidia/gpus").is_ok();

    if has_nvidia {
        let nvcodec_decoders = ["nvh264dec", "nvh265dec", "nvav1dec", "nvvp9dec", "nvvp8dec"];
        let mut boosted = Vec::new();
        for name in &nvcodec_decoders {
            if let Some(factory) = gst::ElementFactory::find(name) {
                factory.set_rank(gst::Rank::PRIMARY + 1);
                boosted.push(*name);
            }
        }

        let vaapi_decoders = ["vah264dec", "vah265dec", "vaav1dec", "vavp9dec", "vavp8dec"];
        let mut demoted = Vec::new();
        for name in &vaapi_decoders {
            if let Some(factory) = gst::ElementFactory::find(name) {
                factory.set_rank(gst::Rank::MARGINAL);
                demoted.push(*name);
            }
        }

        info!(
            "[VIDEO] NVIDIA detected: boosted nvcodec {:?}, demoted VA-API {:?}",
            boosted, demoted
        );
    } else {
        info!("[VIDEO] Non-NVIDIA GPU: VA-API decoders preferred for DMA-BUF zero-copy");
    }
}

#[derive(Debug)]
pub enum VideoFrameFormat {
    Rgba,
    Nv12 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
    /// DMA-BUF zero-copy: file descriptors for each plane, no CPU-side data.
    /// File descriptors are owned and will be closed when dropped.
    DmaBufNv12 {
        y_fd: OwnedFd,
        y_stride: u32,
        y_offset: u32,
        uv_fd: OwnedFd,
        uv_stride: u32,
        uv_offset: u32,
    },
    /// CUDA zero-copy: buffer stays in GPU memory, renderer uses CUDA-Vulkan interop.
    CudaNv12 {
        y_stride: u32,
        uv_offset: u32,
        uv_stride: u32,
    },
}

impl Clone for VideoFrameFormat {
    fn clone(&self) -> Self {
        match self {
            Self::Rgba => Self::Rgba,
            Self::Nv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Self::Nv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            },
            Self::CudaNv12 {
                y_stride,
                uv_offset,
                uv_stride,
            } => Self::CudaNv12 {
                y_stride: *y_stride,
                uv_offset: *uv_offset,
                uv_stride: *uv_stride,
            },
            Self::DmaBufNv12 {
                y_fd,
                y_stride,
                y_offset,
                uv_fd,
                uv_stride,
                uv_offset,
            } => Self::DmaBufNv12 {
                y_fd: y_fd.try_clone().expect("Failed to clone y_fd"),
                y_stride: *y_stride,
                y_offset: *y_offset,
                uv_fd: uv_fd.try_clone().expect("Failed to clone uv_fd"),
                uv_stride: *uv_stride,
                uv_offset: *uv_offset,
            },
        }
    }
}

/// Video frame carrying pixel data in either RGBA or NV12 format.
/// Uses gst::Buffer to avoid copying data.
#[derive(Clone)]
pub struct VideoFrame {
    pub buffer: gst::Buffer,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub format: VideoFrameFormat,
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

/// Extract DMA-BUF file descriptors and plane info from an NV12 GStreamer buffer.
///
/// NV12 buffers may carry 1 or 2 memory blocks:
///   - 1 block: Y and UV packed into a single allocation (different offsets)
///   - 2 blocks: Y and UV in separate DMA-BUF allocations
///
/// Falls back to regular NV12 if fd extraction fails.
fn extract_dmabuf_nv12(
    buffer: &gst::Buffer,
    strides: [i32; 4],
    offsets: [usize; 4],
) -> VideoFrameFormat {
    // Validate strides before casting to u32 (silently wraps if negative)
    if strides[0] < 0 || strides[1] < 0 {
        tracing::warn!(
            "[VIDEO] Negative strides detected ({}, {}), falling back to CPU path",
            strides[0],
            strides[1]
        );
        return VideoFrameFormat::Nv12 {
            y_stride: strides[0].max(0) as u32,
            uv_offset: offsets[1] as u32,
            uv_stride: strides[1].max(0) as u32,
        };
    }
    if buffer.n_memory() >= 2 {
        // Separate DMA-BUFs per plane
        let y_mem = buffer.peek_memory(0);
        let uv_mem = buffer.peek_memory(1);

        if let (Some(y_dmabuf), Some(uv_dmabuf)) = (
            y_mem.downcast_memory_ref::<gst_alloc::DmaBufMemory>(),
            uv_mem.downcast_memory_ref::<gst_alloc::DmaBufMemory>(),
        ) {
            let y_fd = unsafe { OwnedFd::from_raw_fd(y_dmabuf.fd()) };
            let uv_fd = unsafe { OwnedFd::from_raw_fd(uv_dmabuf.fd()) };
            return VideoFrameFormat::DmaBufNv12 {
                y_fd,
                y_stride: strides[0] as u32,
                y_offset: offsets[0] as u32,
                uv_fd,
                uv_stride: strides[1] as u32,
                uv_offset: offsets[1] as u32,
            };
        }
    } else if buffer.n_memory() == 1 {
        // Single DMA-BUF with both planes at different offsets
        let mem = buffer.peek_memory(0);
        if let Some(dmabuf) = mem.downcast_memory_ref::<gst_alloc::DmaBufMemory>() {
            let fd = unsafe { OwnedFd::from_raw_fd(dmabuf.fd()) };
            let fd_uv = fd
                .try_clone()
                .unwrap_or_else(|_| unsafe { OwnedFd::from_raw_fd(dmabuf.fd()) });
            return VideoFrameFormat::DmaBufNv12 {
                y_fd: fd,
                y_stride: strides[0] as u32,
                y_offset: offsets[0] as u32,
                uv_fd: fd_uv,
                uv_stride: strides[1] as u32,
                uv_offset: offsets[1] as u32,
            };
        }
    }

    // Fallback: treat as regular NV12 (CPU-accessible)
    if buffer.n_memory() == 0 {
        tracing::debug!("[VIDEO] No memory blocks on buffer, falling back to CPU path");
    } else {
        tracing::warn!(
            "[VIDEO] DMA-BUF memory detected but fd extraction failed, falling back to NV12 CPU path"
        );
    }
    VideoFrameFormat::Nv12 {
        y_stride: strides[0] as u32,
        uv_offset: offsets[1] as u32,
        uv_stride: strides[1] as u32,
    }
}

pub struct VideoPlayer {
    pub pipeline: gst::Element,
    pub appsink: gst_app::AppSink,
    is_running: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>, // Keep for compatibility, but will use thread pool
    frame_tx: tokio::sync::mpsc::Sender<(Arc<String>, VideoEvent)>,
    source_id: Arc<String>,
    session_id: u64,
    start_time: std::time::Instant,
    first_frame_logged: Arc<AtomicBool>,
}

impl VideoPlayer {
    /// Create a new video player with a bounded channel for backpressure
    pub fn new(
        uri: &str,
        source_id: Arc<String>,
        session_id: u64,
        volume: f64,
        frame_tx: tokio::sync::mpsc::Sender<(Arc<String>, VideoEvent)>,
    ) -> anyhow::Result<Self> {
        let _video_start = std::time::Instant::now();
        let creation_start = std::time::Instant::now();
        // Prefer playbin3 on modern GStreamer for lower-latency URI changes and
        // more efficient internal graph management. Fall back to playbin if it
        // is unavailable on the host system.
        let pipeline_name = if gst::ElementFactory::find("playbin3").is_some() {
            "playbin3"
        } else {
            "playbin"
        };
        let pipeline = gst::ElementFactory::make(pipeline_name)
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

        // Disable subtitles/buffering unconditionally. Also skip audio decoding entirely
        // when the configured volume is zero to avoid extra decoder/buffer state.
        pipeline.set_property_from_str("flags", playbin_flags_for_volume(volume));
        if !audio_enabled_for_volume(volume) {
            pipeline.set_property("mute", true);
        }
        if pipeline_name == "playbin3" {
            pipeline.set_property("instant-uri", true);
        }

        // Create appsink for video frames - configure like gSlapper does
        let appsink = gst::ElementFactory::make("appsink")
            .name("video-sink")
            .build()?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast to AppSink"))?;

        let mode = get_video_mode();
        let caps = match mode {
            VideoMode::ForceRgba => gst::Caps::builder("video/x-raw")
                .field("format", "RGBA")
                .build(),
            VideoMode::ForceNv12 => gst::Caps::builder("video/x-raw")
                .field("format", "NV12")
                .build(),
            VideoMode::ForceDmaBuf => {
                // Strict: DMA-BUF only, no fallback. Will fail with not-negotiated
                // if the decoder/driver can't export DMA-BUF.
                gst::Caps::builder("video/x-raw")
                    .features([gst_alloc::CAPS_FEATURE_MEMORY_DMABUF.as_str()])
                    .field("format", "NV12")
                    .build()
            }
            VideoMode::ForceCuda => {
                // Strict: CUDAMemory only. Requires NVIDIA nvcodec decoders.
                gst::Caps::builder("video/x-raw")
                    .features(["memory:CUDAMemory"])
                    .field("format", "NV12")
                    .build()
            }
            VideoMode::Auto => {
                let has_nvidia = std::fs::metadata("/proc/driver/nvidia/gpus").is_ok();
                if has_nvidia {
                    // NVIDIA: CUDAMemory preferred (zero-copy via CUDA-Vulkan interop),
                    // then DMA-BUF, then NV12 CPU fallback
                    let cuda_caps = gst::Caps::builder("video/x-raw")
                        .features(["memory:CUDAMemory"])
                        .field("format", "NV12")
                        .build();
                    let dmabuf_caps = gst::Caps::builder("video/x-raw")
                        .features([gst_alloc::CAPS_FEATURE_MEMORY_DMABUF.as_str()])
                        .field("format", "NV12")
                        .build();
                    let nv12_caps = gst::Caps::builder("video/x-raw")
                        .field("format", "NV12")
                        .build();
                    let mut caps = cuda_caps;
                    caps.merge(dmabuf_caps);
                    caps.merge(nv12_caps);
                    // Add generic fallback
                    caps.merge(gst::Caps::builder("video/x-raw").build());
                    caps
                } else {
                    // AMD/Intel: DMA-BUF preferred (VA-API zero-copy), NV12 fallback
                    let dmabuf_caps = gst::Caps::builder("video/x-raw")
                        .features([gst_alloc::CAPS_FEATURE_MEMORY_DMABUF.as_str()])
                        .field("format", "NV12")
                        .build();
                    let nv12_caps = gst::Caps::builder("video/x-raw")
                        .field("format", "NV12")
                        .build();
                    let mut caps = dmabuf_caps;
                    caps.merge(nv12_caps);
                    // Add generic fallback
                    caps.merge(gst::Caps::builder("video/x-raw").build());
                    caps
                }
            }
        };

        appsink.set_caps(Some(&caps));
        appsink.set_sync(true); // Sync to clock
        appsink.set_drop(true); // Drop frames if late - CRITICAL for preventing buffer accumulation
        appsink.set_max_buffers(1); // Match gSlapper: 1 buffer to minimize latency and memory
        appsink.set_property("enable-last-sample", false); // Don't retain a full decoded frame.
        appsink.set_property("wait-on-eos", false); // Tear down without waiting on queued buffers.
        appsink.set_property("qos", true); // Let upstream know we're latency-sensitive.
        appsink.set_property_from_str("leaky-type", "downstream");
        // CRITICAL: Enable emit-signals to get callbacks, but ensure we handle them quickly
        // The new_sample callback will be called for each frame

        // Keep source_id for closure
        let cb_source_id = source_id.clone();

        // Set up new-sample callback
        let frame_tx_clone = frame_tx.clone();
        let first_frame_logged = Arc::new(AtomicBool::new(false));
        let callback_first_frame_logged = first_frame_logged.clone();
        let creation_time_ref = creation_start;

        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let source_id = cb_source_id.clone();

                    if !callback_first_frame_logged.load(Ordering::SeqCst) {
                        callback_first_frame_logged.store(true, Ordering::SeqCst);
                        let duration = creation_time_ref.elapsed();
                        info!("[ASSET] {}: First video frame produced in {:.3}ms", source_id, duration.as_secs_f64() * 1000.0);
                    }

                    let session_id = session_id;

                    let sample = match sink.pull_sample() {
                        Ok(s) => s,
                        Err(_) => return Err(gst::FlowError::Error),
                    };
                    let frame = sample_to_video_frame(sample, session_id)?;

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

        info!(
            "VideoPlayer created with playbin + appsink (mode={:?}, audio={}, caps={})",
            mode,
            audio_enabled_for_volume(volume),
            caps
        );

        Ok(Self {
            pipeline,
            appsink,
            is_running: Arc::new(AtomicBool::new(false)),
            thread_handle: None,
            frame_tx,
            source_id,
            session_id,
            start_time: creation_start,
            first_frame_logged,
        })
    }

    /// Pre-buffer video by moving the pipeline to PAUSED and extracting the preroll sample.
    pub fn prebuffer(&mut self) -> anyhow::Result<Option<VideoFrame>> {
        debug!("[VIDEO] {}: Pre-buffering video pipeline", self.source_id);
        let ret = self.pipeline.set_state(gst::State::Paused)?;
        match ret {
            gst::StateChangeSuccess::Success => debug!(
                "[VIDEO] {}: Pipeline state -> Paused (pre-roll complete)",
                self.source_id
            ),
            gst::StateChangeSuccess::Async => debug!(
                "[VIDEO] {}: Pipeline state -> Paused (Async, pre-buffering)",
                self.source_id
            ),
            _ => {}
        }

        let (state_result, current, pending) =
            self.pipeline.state(gst::ClockTime::from_mseconds(1500));
        match state_result {
            Ok(_) => {
                debug!(
                    "[VIDEO] {}: Pre-buffer state settled at {:?} (pending {:?})",
                    self.source_id, current, pending
                );
            }
            Err(e) => {
                debug!(
                    "[VIDEO] {}: Timed out waiting for pre-buffer state ({:?} -> {:?}): {:?}",
                    self.source_id, current, pending, e
                );
            }
        }

        let preroll = self
            .appsink
            .try_pull_preroll(gst::ClockTime::from_mseconds(250))
            .map(|sample| {
                sample_to_video_frame(sample, self.session_id)
                    .map_err(|e| anyhow::anyhow!("failed to decode preroll sample: {:?}", e))
            })
            .transpose()?;

        if preroll.is_some() && !self.first_frame_logged.swap(true, Ordering::SeqCst) {
            let duration = self.start_time.elapsed();
            info!(
                "[ASSET] {}: First video frame produced in {:.3}ms (preroll)",
                self.source_id,
                duration.as_secs_f64() * 1000.0
            );
        }

        Ok(preroll)
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
                // Keep stop/join latency short so stale pipelines release resources quickly.
                match bus.timed_pop(gst::ClockTime::from_mseconds(BUS_WATCHER_POLL_MS)) {
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
        self.request_stop()?;

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

    pub fn request_stop(&self) -> anyhow::Result<()> {
        let was_running = self.is_running.swap(false, Ordering::SeqCst);
        if was_running {
            info!("Stopping video playback...");
            // Fade audio before teardown to prevent clicks on audio-enabled pipelines.
            self.pipeline.set_property("volume", 0.0);
        }

        // Pause first (transition to Ready state first helps cleanup).
        let _ = self.pipeline.set_state(gst::State::Paused);

        // Always force Null, even for players that never fully entered Playing.
        // Prebuffered or failed-start pipelines can still hold decoder state.
        self.pipeline.set_state(gst::State::Null)?;
        Ok(())
    }

    pub fn set_volume(&mut self, volume: f64) {
        self.pipeline.set_property("volume", volume);
    }

    pub fn pause(&self) -> anyhow::Result<()> {
        self.pipeline.set_state(gst::State::Paused)?;
        Ok(())
    }

    pub fn resume(&self) -> anyhow::Result<()> {
        self.pipeline.set_state(gst::State::Playing)?;
        Ok(())
    }
}

fn sample_to_video_frame(
    sample: gst::Sample,
    session_id: u64,
) -> Result<VideoFrame, gst::FlowError> {
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

    // Prefer GstVideoMeta stride/offset (reflects actual memory layout from
    // hardware decoders), fall back to VideoInfo when the meta is absent.
    let (strides, offsets) = unsafe {
        let raw_meta =
            gst_video::ffi::gst_buffer_get_video_meta(buffer.as_ptr() as *mut gst::ffi::GstBuffer);
        if !raw_meta.is_null() {
            let meta = &*raw_meta;
            (meta.stride, meta.offset)
        } else {
            let vi_strides = video_info.stride();
            let vi_offsets = video_info.offset();
            let mut s = [0i32; 4];
            let mut o = [0usize; 4];
            for i in 0..4 {
                s[i] = vi_strides[i];
                o[i] = vi_offsets[i] as usize;
            }
            (s, o)
        }
    };
    let y_stride = strides[0] as u32;

    let is_cuda = caps
        .features(0)
        .is_some_and(|f| f.contains("memory:CUDAMemory"));

    let format = match video_info.format() {
        gst_video::VideoFormat::Nv12 => {
            if is_cuda {
                tracing::debug!(
                    "[VIDEO] CUDA NV12 layout: y_stride={}, uv_offset={}, uv_stride={} ({}x{})",
                    strides[0],
                    offsets[1],
                    strides[1],
                    width,
                    height
                );
                VideoFrameFormat::CudaNv12 {
                    y_stride,
                    uv_offset: offsets[1] as u32,
                    uv_stride: strides[1] as u32,
                }
            } else {
                let is_dmabuf = buffer.n_memory() > 0
                    && buffer
                        .peek_memory(0)
                        .downcast_memory_ref::<gst_alloc::DmaBufMemory>()
                        .is_some();

                if is_dmabuf {
                    extract_dmabuf_nv12(&buffer, strides, offsets)
                } else {
                    VideoFrameFormat::Nv12 {
                        y_stride,
                        uv_offset: offsets[1] as u32,
                        uv_stride: strides[1] as u32,
                    }
                }
            }
        }
        gst_video::VideoFormat::Rgba => VideoFrameFormat::Rgba,
        other => {
            tracing::error!("[VIDEO] Unsupported format {:?}, negotiation failed", other);
            return Err(gst::FlowError::NotNegotiated);
        }
    };

    Ok(VideoFrame {
        buffer,
        width,
        height,
        stride: y_stride,
        format,
        session_id,
    })
}

impl Drop for VideoPlayer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
