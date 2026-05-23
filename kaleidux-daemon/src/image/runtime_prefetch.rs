use crate::background::{self, BackgroundWorkKind};
use crate::image::prefetch::ImagePrefetchRequest;
use crate::image::runtime_cache::request_prepared_image_payload;
use crate::metrics;
use parking_lot::Mutex as ParkingMutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

static IMAGE_PREFETCH_GENERATIONS: once_cell::sync::Lazy<ParkingMutex<HashMap<String, u64>>> =
    once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

pub(crate) fn begin_image_prefetch_generation(name: &str) -> u64 {
    let mut generations = IMAGE_PREFETCH_GENERATIONS.lock();
    let next_generation = generations
        .get(name)
        .copied()
        .unwrap_or(0)
        .saturating_add(1);
    generations.insert(name.to_string(), next_generation);
    next_generation
}

pub(crate) fn image_prefetch_generation_matches(name: &str, generation: u64) -> bool {
    IMAGE_PREFETCH_GENERATIONS
        .lock()
        .get(name)
        .copied()
        .unwrap_or(0)
        == generation
}

pub(crate) fn schedule_image_prefetch_plan(
    trigger_output: &str,
    generation: u64,
    requests: Vec<ImagePrefetchRequest>,
    metrics: Arc<metrics::PerformanceMetrics>,
) {
    if requests.is_empty() || !background::is_accepting_new_work() {
        return;
    }

    let output_name = trigger_output.to_string();
    tokio::spawn(async move {
        for request in requests {
            if !background::is_accepting_new_work() {
                return;
            }
            if !image_prefetch_generation_matches(&output_name, generation) {
                debug!(
                    "[PREFETCH] {}: Aborting superseded prefetch plan generation {}",
                    output_name, generation
                );
                return;
            }

            let prefetch_start = Instant::now();
            match request_prepared_image_payload(
                &request.path,
                request.target_width,
                request.target_height,
                BackgroundWorkKind::ImagePrefetch,
                &metrics,
            )
            .await
            {
                Ok(payload) => debug!(
                    "[PREFETCH] {} -> {}: Warmed {} ({}) as {} {}x{} in {:.1}ms",
                    output_name,
                    request.target_output,
                    request.path.display(),
                    request.reason,
                    payload.profile.format,
                    payload.width,
                    payload.height,
                    prefetch_start.elapsed().as_secs_f64() * 1000.0
                ),
                Err(e) => debug!(
                    "[PREFETCH] {} -> {}: Failed to warm {} ({}): {}",
                    output_name,
                    request.target_output,
                    request.path.display(),
                    request.reason,
                    e
                ),
            }
        }
    });
}
