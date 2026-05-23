use crate::monitor_manager;
use crate::queue;
use crate::renderer;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const LOOKAHEAD_IMAGES: usize = 3;
const MAX_REQUESTS: usize = 6;

#[derive(Debug, Clone)]
pub(crate) struct ImagePrefetchRequest {
    pub(crate) target_output: String,
    pub(crate) path: PathBuf,
    pub(crate) target_width: u32,
    pub(crate) target_height: u32,
    pub(crate) reason: &'static str,
}

pub(crate) fn build_plan(
    monitor_manager: &monitor_manager::MonitorManager,
    renderers: &HashMap<String, renderer::Renderer>,
    trigger_output: &str,
) -> Vec<ImagePrefetchRequest> {
    let Some(orchestrator) = monitor_manager.outputs.get(trigger_output) else {
        return Vec::new();
    };
    let Some(renderer) = renderers.get(trigger_output) else {
        return Vec::new();
    };

    let candidates = collect_output_candidates(orchestrator);
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut requests = Vec::new();
    let mut seen_keys = HashSet::new();
    for (path, reason) in &candidates {
        push_request(
            &mut requests,
            &mut seen_keys,
            trigger_output,
            path,
            renderer.config.width,
            renderer.config.height,
            reason,
        );
    }

    let candidate_paths: HashSet<PathBuf> =
        candidates.into_iter().map(|(path, _reason)| path).collect();

    for (output_name, other_orchestrator) in &monitor_manager.outputs {
        if requests.len() >= MAX_REQUESTS {
            break;
        }
        if output_name == trigger_output
            || other_orchestrator.next_content_type != Some(queue::ContentType::Image)
        {
            continue;
        }

        let Some(next_path) = other_orchestrator.next_path.as_ref() else {
            continue;
        };
        if !candidate_paths.contains(next_path) {
            continue;
        }

        let Some(other_renderer) = renderers.get(output_name) else {
            continue;
        };

        push_request(
            &mut requests,
            &mut seen_keys,
            output_name,
            next_path,
            other_renderer.config.width,
            other_renderer.config.height,
            "shared-next",
        );
    }

    requests.truncate(MAX_REQUESTS);
    sort_requests(&mut requests);
    requests
}

pub(crate) fn sort_requests(requests: &mut [ImagePrefetchRequest]) {
    requests.sort_by(|left, right| {
        let left_priority = request_priority(left.reason);
        let right_priority = request_priority(right.reason);
        let left_area = u64::from(left.target_width) * u64::from(left.target_height);
        let right_area = u64::from(right.target_width) * u64::from(right.target_height);

        left_priority
            .cmp(&right_priority)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| right_area.cmp(&left_area))
            .then_with(|| left.target_output.cmp(&right.target_output))
    });
}

fn collect_output_candidates(
    orchestrator: &monitor_manager::OutputOrchestrator,
) -> Vec<(PathBuf, &'static str)> {
    let mut candidates = Vec::new();
    let mut seen_paths = HashSet::new();

    if orchestrator.next_content_type == Some(queue::ContentType::Image) {
        if let Some(next_path) = orchestrator.next_path.as_ref() {
            if seen_paths.insert(next_path.clone()) {
                candidates.push((next_path.clone(), "next"));
            }
        }
    }

    if let Some(queue) = orchestrator.queue.as_ref() {
        for path in queue.peek_upcoming_images(LOOKAHEAD_IMAGES) {
            if seen_paths.insert(path.clone()) {
                candidates.push((path, "lookahead"));
            }
            if candidates.len() >= LOOKAHEAD_IMAGES {
                break;
            }
        }
    }

    candidates
}

fn push_request(
    requests: &mut Vec<ImagePrefetchRequest>,
    seen_keys: &mut HashSet<(PathBuf, u32, u32)>,
    target_output: &str,
    path: &Path,
    target_width: u32,
    target_height: u32,
    reason: &'static str,
) {
    let request_key = (path.to_path_buf(), target_width, target_height);
    if !seen_keys.insert(request_key) {
        return;
    }

    requests.push(ImagePrefetchRequest {
        target_output: target_output.to_string(),
        path: path.to_path_buf(),
        target_width,
        target_height,
        reason,
    });
}

fn request_priority(reason: &str) -> u8 {
    match reason {
        "next" | "shared-next" => 0,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_immediate_requests_before_lookahead_and_by_size() {
        let mut requests = vec![
            ImagePrefetchRequest {
                target_output: "DP-1".to_string(),
                path: PathBuf::from("/tmp/shared.png"),
                target_width: 1920,
                target_height: 1080,
                reason: "shared-next",
            },
            ImagePrefetchRequest {
                target_output: "DP-1".to_string(),
                path: PathBuf::from("/tmp/lookahead.png"),
                target_width: 2560,
                target_height: 1440,
                reason: "lookahead",
            },
            ImagePrefetchRequest {
                target_output: "DP-2".to_string(),
                path: PathBuf::from("/tmp/shared.png"),
                target_width: 2560,
                target_height: 1440,
                reason: "next",
            },
        ];
        sort_requests(&mut requests);

        assert_eq!(requests[0].reason, "next");
        assert_eq!(
            (requests[0].target_width, requests[0].target_height),
            (2560, 1440)
        );
        assert_eq!(requests[1].reason, "shared-next");
        assert_eq!(requests[2].reason, "lookahead");
    }
}
