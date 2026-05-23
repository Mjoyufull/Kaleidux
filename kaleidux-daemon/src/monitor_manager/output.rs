use crate::cache::FileCache;
use crate::metrics::PerformanceMetrics;
use crate::orchestration::OutputConfig;
use crate::queue::{ContentType, SmartQueue};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

const MIN_CONTENT_LOAD_TIMEOUT: Duration = Duration::from_secs(15);
const CONTENT_LOAD_GRACE: Duration = Duration::from_secs(5);

pub(super) fn content_load_timeout(display_duration: Duration) -> Duration {
    (display_duration + CONTENT_LOAD_GRACE).max(MIN_CONTENT_LOAD_TIMEOUT)
}

fn stable_output_hash(name: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(super) fn independent_phase_offset(name: &str, base_duration: Duration) -> Duration {
    let cap = (base_duration / 12).min(Duration::from_millis(120));
    if cap < Duration::from_millis(8) {
        return Duration::ZERO;
    }

    let cap_nanos = cap.as_nanos();
    let offset_nanos = u128::from(stable_output_hash(name)) % (cap_nanos + 1);
    Duration::from_nanos(offset_nanos as u64)
}

pub struct OutputOrchestrator {
    pub _name: String,
    #[allow(dead_code)]
    pub description: String,
    pub config: OutputConfig,
    pub queue: Option<SmartQueue>,
    pub current_path: Option<PathBuf>,
    #[allow(dead_code)]
    pub next_path: Option<PathBuf>, // Pre-buffered next content path
    #[allow(dead_code)]
    pub next_content_type: Option<ContentType>, // Type of next content
    pub next_change: Option<Instant>,
    pub display_start_time: Option<Instant>, // When content actually started displaying
    pub phase_offset: Duration,
}

impl OutputOrchestrator {
    pub(super) fn cycle_duration(&self) -> Duration {
        self.config.duration.saturating_add(self.phase_offset)
    }

    pub(super) fn apply_config(&mut self, config: OutputConfig) {
        self.phase_offset = independent_phase_offset(&self._name, config.duration);
        self.config = config;
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        if let Some(display_start) = self.display_start_time {
            Some(display_start + self.cycle_duration())
        } else {
            self.next_change
        }
    }

    pub async fn new(
        name: String,
        description: String,
        config: OutputConfig,
        cache: Arc<FileCache>,
        metrics: Option<Arc<PerformanceMetrics>>,
    ) -> Self {
        let queue = if let Some(path) = &config.path {
            info!("[QUEUE] {}: Initializing queue for path: {:?}", name, path);
            match SmartQueue::new_with_cache(
                path,
                config.video_ratio,
                config.sorting,
                cache,
                metrics.clone(),
            )
            .await
            {
                Ok(mut q) => {
                    info!("[QUEUE] {}: Queue initialized successfully", name);
                    if let Some(pl_name) = &config.default_playlist {
                        if let Err(e) = q.set_playlist(Some(pl_name.clone())) {
                            error!(
                                "Failed to set default playlist '{}' for {}: {}",
                                pl_name, name, e
                            );
                        }
                    }
                    Some(q)
                }
                Err(e) => {
                    error!("[QUEUE] {}: Failed to initialize queue: {}", name, e);
                    None
                }
            }
        } else {
            warn!("[QUEUE] {}: No path configured, queue will be None", name);
            None
        };

        let phase_offset = independent_phase_offset(&name, config.duration);

        Self {
            _name: name,
            description,
            config,
            queue,
            current_path: None,
            next_path: None,
            next_content_type: None,
            next_change: None,
            display_start_time: None,
            phase_offset,
        }
    }

    pub fn tick(&mut self) -> Option<(PathBuf, ContentType)> {
        let now = Instant::now();

        // If content is displaying, check if duration has elapsed based on actual display start time
        if let Some(display_start) = self.display_start_time {
            let elapsed = now.saturating_duration_since(display_start);
            if elapsed >= self.cycle_duration() {
                debug!(
                    "Duration expired for {}: {} elapsed (target: {:?})",
                    self._name,
                    format!("{:.2}s", elapsed.as_secs_f64()),
                    self.cycle_duration()
                );
                let result = self.pick_next();
                return result;
            }
        } else if let Some(next) = self.next_change {
            // Fallback: if display_start_time not set yet, use scheduled time
            // This handles the case where content hasn't loaded yet
            if now >= next {
                debug!(
                    "Timer expired for {}: Switching now (next was {:?})",
                    self._name, next
                );
                let result = self.pick_next();
                return result;
            }
        } else if self.current_path.is_none() {
            self.queue.as_ref()?;
            info!(
                "[TICK] {}: Initial tick - picking first content (queue exists)",
                self._name
            );
            let result = self.pick_next();
            return result;
        }
        None
    }

    pub fn pick_next(&mut self) -> Option<(PathBuf, ContentType)> {
        if let Some(queue) = &mut self.queue {
            info!("[PICK] {}: Calling queue.pick_next()", self._name);
            if let Some(path) = queue.pick_next() {
                return self.apply_selected_path(path);
            }
        }
        None
    }

    pub fn pick_next_excluding(
        &mut self,
        excluded: &HashSet<PathBuf>,
    ) -> Option<(PathBuf, ContentType)> {
        if let Some(queue) = &mut self.queue {
            info!(
                "[PICK] {}: Calling queue.pick_next_excluding() with {} excluded",
                self._name,
                excluded.len()
            );
            if let Some(path) = queue.pick_next_excluding(excluded) {
                return self.apply_selected_path(path);
            }
        }
        None
    }

    /// Get the next content path without consuming it (for pre-buffering)
    pub fn peek_next(&self) -> Option<(PathBuf, ContentType)> {
        if let Some(queue) = &self.queue {
            return queue.peek_next();
        }
        None
    }

    /// Mark that transition has completed and content is now displaying (called when transition progress >= 1.0)
    pub fn mark_transition_completed(&mut self) {
        if self.display_start_time.is_none() && self.current_path.is_some() {
            self.display_start_time = Some(Instant::now());
            debug!(
                "Transition completed for {} - duration timer now active ({:.2}s of content display starts now)",
                self._name,
                self.cycle_duration().as_secs_f64()
            );
        }
    }

    pub fn pick_prev(&mut self) -> Option<(PathBuf, ContentType)> {
        if let Some(queue) = &mut self.queue {
            if let Some(path) = queue.pick_prev() {
                let content_type = match crate::queue::SmartQueue::get_content_type(&path) {
                    Some(content_type) => content_type,
                    None => {
                        warn!(
                            "[PICK] {}: Could not determine content type for previous path: {}",
                            self._name,
                            path.display()
                        );
                        return None;
                    }
                };
                self.current_path = Some(path.clone());
                // Reset display start time - will be set when content actually starts displaying
                self.display_start_time = None;
                self.next_change =
                    Some(Instant::now() + content_load_timeout(self.cycle_duration()));
                return Some((path, content_type));
            }
        }
        None
    }

    pub(super) fn apply_selected_path(&mut self, path: PathBuf) -> Option<(PathBuf, ContentType)> {
        info!("[PICK] {}: Selected path: {:?}", self._name, path);
        let content_type = match crate::queue::SmartQueue::get_content_type(&path) {
            Some(content_type) => content_type,
            None => {
                warn!(
                    "[PICK] {}: Skipping selected path with unknown content type: {}",
                    self._name,
                    path.display()
                );
                return None;
            }
        };
        self.current_path = Some(path.clone());
        self.display_start_time = None;
        self.next_change = Some(Instant::now() + content_load_timeout(self.cycle_duration()));

        if let Some((next_p, next_t)) = self.peek_next() {
            self.next_path = Some(next_p);
            self.next_content_type = Some(next_t);
        } else {
            self.next_path = None;
            self.next_content_type = None;
        }

        debug!(
            "Scheduled next change for {} in {:?} (path: {})",
            self._name,
            self.cycle_duration(),
            path.display()
        );
        Some((path, content_type))
    }
}
