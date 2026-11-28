//! Routes duration discoveries and playback windows to the queue owning an output.

use super::MonitorManager;
use crate::media_source::PlaybackWindow;
use crate::queue::SmartQueue;
use std::path::Path;
use std::time::Duration;

impl MonitorManager {
    fn excerpt_queue(&self, name: &str) -> Option<&SmartQueue> {
        if let Some(group) = self.output_groups.get(name) {
            self.group_queues.get(group)
        } else {
            self.shared_queue
                .as_ref()
                .or_else(|| self.outputs.get(name)?.queue.as_ref())
        }
    }

    pub(crate) fn excerpt_request(&self, name: &str) -> (Option<PlaybackWindow>, Option<Duration>) {
        let Some(output) = self.outputs.get(name) else {
            return (None, None);
        };
        let enabled = !output.config.duration.is_zero()
            && output.config.long_video_excerpts
            && self
                .excerpt_queue(name)
                .is_some_and(|queue| queue.pool.len() > 1);
        (
            enabled
                .then(|| {
                    self.excerpt_queue(name)?.current_window().filter(|window| {
                        u128::from(window.end_ns - window.start_ns)
                            == output.config.duration.as_nanos()
                    })
                })
                .flatten(),
            enabled.then_some(output.config.duration),
        )
    }

    pub(crate) fn register_video_duration(&mut self, name: &str, duration_ns: u64) {
        let Some(output) = self.outputs.get(name) else {
            return;
        };
        let Some(path) = output.current_path.clone() else {
            return;
        };
        let length = output.config.duration;
        let enabled = output.config.long_video_excerpts;
        let queue = if let Some(group) = self.output_groups.get(name) {
            self.group_queues.get_mut(group)
        } else if self.shared_queue.is_some() {
            self.shared_queue.as_mut()
        } else {
            self.outputs
                .get_mut(name)
                .and_then(|output| output.queue.as_mut())
        };
        if let Some(queue) = queue {
            queue.configure_excerpts(length, enabled);
            queue.register_video_duration(Path::new(&path), duration_ns);
        }
    }
}
