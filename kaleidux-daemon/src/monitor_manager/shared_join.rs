//! Joins retained cohort content and bounds retries when its source disappeared.

use super::MonitorManager;
use crate::queue::ContentType;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const JOIN_RETRY_DELAY: Duration = Duration::from_secs(5);

impl MonitorManager {
    pub(super) fn shared_join_is_deferred(&self, name: &str, now: Instant) -> bool {
        self.shared_join_retries
            .get(name)
            .is_some_and(|(path, deadline)| {
                *deadline > now
                    && self.outputs.iter().any(|(peer_name, peer)| {
                        peer.current_path.as_ref() == Some(path)
                            && self.shares_playback_with(name, peer_name)
                    })
            })
    }

    fn shared_join_retry_deadline(&self, name: &str, path: &Path, now: Instant) -> Instant {
        self.shared_join_retries
            .get(name)
            .filter(|(failed_path, _)| failed_path == path)
            .map_or(now, |(_, deadline)| *deadline)
    }

    pub(super) fn shared_join_deadline(&self, now: Instant) -> Option<Instant> {
        self.outputs
            .iter()
            .filter(|(name, output)| {
                output.current_path.is_none() && !self.output_power_paused.contains_key(*name)
            })
            .filter_map(|(name, _)| {
                let peer = self.outputs.iter().find(|(peer_name, peer)| {
                    peer.current_path.is_some() && self.shares_playback_with(name, peer_name)
                })?;
                Some(self.shared_join_retry_deadline(name, peer.1.current_path.as_ref()?, now))
            })
            .min()
    }

    pub(super) fn join_shared_outputs(
        &mut self,
        now: Instant,
    ) -> HashMap<String, (PathBuf, ContentType)> {
        let joins: Vec<_> = self
            .outputs
            .iter()
            .filter(|(name, output)| {
                output.current_path.is_none() && !self.output_power_paused.contains_key(*name)
            })
            .filter_map(|(name, _)| {
                let peer = self.outputs.iter().find(|(peer_name, peer)| {
                    peer.current_path.is_some() && self.shares_playback_with(name, peer_name)
                })?;
                let path = peer.1.current_path.as_ref()?;
                (self.shared_join_retry_deadline(name, path, now) <= now).then(|| {
                    (
                        name.clone(),
                        path.clone(),
                        peer.1.next_path.clone(),
                        peer.1.next_content_type,
                    )
                })
            })
            .collect();
        let mut changes = HashMap::new();
        for (name, path, next_path, next_content_type) in joins {
            let Some(kind) = Self::resolve_content_type(&path, "hotplug join") else {
                self.shared_join_retries
                    .insert(name, (path, now + JOIN_RETRY_DELAY));
                continue;
            };
            self.shared_join_retries.remove(&name);
            let output = self.outputs.get_mut(&name).expect("collected live output");
            output.current_path = Some(path.clone());
            output.display_start_time = None;
            output.next_change = Some(now + super::content_load_timeout(output.config.duration));
            output.next_path = next_path;
            output.next_content_type = next_content_type;
            changes.insert(name, (path, kind));
        }
        changes
    }
}
