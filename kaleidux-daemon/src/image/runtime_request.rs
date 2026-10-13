//! Coalesces prepared-image requests and retains source admission until preparation completes.

use super::runtime_source::request_decoded_source_image;
use crate::background::{self, BackgroundWorkKind};
use crate::image::runtime_cache::{
    IMAGE_PREFETCH_CAPACITY_ERROR_PREFIX, acquire_image_work_permit,
    prepare_image_for_output_uncached, prepare_source_image_for_output,
    prepared_image_key_for_identity, prepared_target_dimensions_from_descriptor,
    select_compatible_prepared_key, store_prepared_image_cache_by_key, store_prepared_image_memory,
    try_load_compatible_prepared_image_memory, try_load_prepared_image_cache_by_key,
    try_load_prepared_image_memory,
};
use crate::image::runtime_shared::{publish_shared_result, wait_for_shared_result};
use crate::image::types::{
    DecodedImagePayload, ImageSourceIdentity, InFlightSharedResult, PreparedImageEntry,
    PreparedImageKey,
};
use crate::metrics;
use parking_lot::Mutex as ParkingMutex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub(super) const MAX_SPECULATIVE_FAILURE_RETRIES: u8 = 3;

pub(super) fn rejected_image_work(work_kind: BackgroundWorkKind) -> String {
    if work_kind == BackgroundWorkKind::ImagePrefetch && background::is_accepting_new_work() {
        format!("{IMAGE_PREFETCH_CAPACITY_ERROR_PREFIX} background registry is full")
    } else {
        "image work skipped because shutdown is in progress".to_string()
    }
}

pub(super) async fn spawn_image_blocking<T, F>(
    work_kind: BackgroundWorkKind,
    work: F,
) -> Option<tokio::task::JoinHandle<T>>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    if work_kind == BackgroundWorkKind::ImagePrefetch {
        background::spawn_blocking_tracked(work_kind, work)
    } else {
        background::spawn_blocking_tracked_wait(work_kind, work).await
    }
}

static PREPARED_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<PreparedImageKey, Arc<InFlightSharedResult<PreparedImageEntry>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

struct PreparedInFlightGuard {
    key: PreparedImageKey,
    state: Arc<InFlightSharedResult<PreparedImageEntry>>,
    published: bool,
}

impl PreparedInFlightGuard {
    fn new(key: PreparedImageKey, state: Arc<InFlightSharedResult<PreparedImageEntry>>) -> Self {
        Self {
            key,
            state,
            published: false,
        }
    }

    fn publish(&mut self, result: Result<Arc<PreparedImageEntry>, String>) {
        remove_prepared_flight_if_current(&self.key, &self.state);
        publish_shared_result(&self.state, result);
        self.published = true;
    }
}

impl Drop for PreparedInFlightGuard {
    fn drop(&mut self) {
        if !self.published {
            remove_prepared_flight_if_current(&self.key, &self.state);
            publish_shared_result(
                &self.state,
                Err("image prepare leader cancelled before publishing a result".to_string()),
            );
        }
    }
}

fn remove_prepared_flight_if_current(
    key: &PreparedImageKey,
    state: &Arc<InFlightSharedResult<PreparedImageEntry>>,
) {
    let mut flights = PREPARED_IMAGE_IN_FLIGHT.lock();
    if flights
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, state))
    {
        flights.remove(key);
    }
}

pub(crate) fn find_compatible_prepared_in_flight_state(
    identity: &ImageSourceIdentity,
    target_width: u32,
    target_height: u32,
    max_bytes: usize,
) -> Option<Arc<InFlightSharedResult<PreparedImageEntry>>> {
    let in_flight = PREPARED_IMAGE_IN_FLIGHT.lock();
    let candidate_key =
        select_compatible_prepared_key(in_flight.keys(), identity, target_width, target_height)?;
    if u64::from(candidate_key.target_width) * u64::from(candidate_key.target_height) * 4
        > max_bytes as u64
    {
        return None;
    }
    in_flight.get(&candidate_key).cloned()
}

pub(super) fn should_retry_speculative_failure(work_kind: BackgroundWorkKind, error: &str) -> bool {
    work_kind != BackgroundWorkKind::ImagePrefetch
        && error.starts_with(IMAGE_PREFETCH_CAPACITY_ERROR_PREFIX)
}

pub(crate) async fn request_prepared_image_payload(
    path: &Path,
    target_width: u32,
    target_height: u32,
    work_kind: BackgroundWorkKind,
    max_compatible_bytes: usize,
    metrics: &Arc<metrics::PerformanceMetrics>,
) -> anyhow::Result<DecodedImagePayload> {
    let Some(descriptor) = super::runtime_probe::probe_descriptor(path, work_kind).await? else {
        let fallback_path = path.to_path_buf();
        let permit = acquire_image_work_permit(work_kind, "prepare").await?;
        let Some(handle) = spawn_image_blocking(work_kind, move || {
            let _permit = permit;
            prepare_image_for_output_uncached(&fallback_path, target_width, target_height)
        })
        .await
        else {
            return Err(anyhow::anyhow!(rejected_image_work(work_kind)));
        };
        metrics.record_image_prepared_miss();
        return handle
            .await
            .map_err(|e| anyhow::anyhow!("image prepare task panicked: {}", e))?;
    };

    let (prepared_width, prepared_height) =
        prepared_target_dimensions_from_descriptor(&descriptor, target_width, target_height);
    let key = prepared_image_key_for_identity(
        descriptor.identity.clone(),
        prepared_width,
        prepared_height,
    );

    if let Some(payload) = try_load_prepared_image_memory(&key) {
        metrics.record_image_prepared_memory_hit();
        return Ok(payload);
    }
    if let Some(payload) = try_load_compatible_prepared_image_memory(
        &descriptor.identity,
        prepared_width,
        prepared_height,
    )
    .filter(|payload| payload.data.len() <= max_compatible_bytes)
    {
        metrics.record_image_prepared_compatible_hit();
        return Ok(payload);
    }

    if let Some(state) = find_compatible_prepared_in_flight_state(
        &descriptor.identity,
        prepared_width,
        prepared_height,
        max_compatible_bytes,
    ) {
        metrics.record_image_shared_wait();
        match wait_for_shared_result(state).await {
            Ok(entry) => return Ok(entry.to_payload("prepared-shared-compatible")),
            Err(error) if should_retry_speculative_failure(work_kind, &error) => {}
            Err(error) => return Err(anyhow::anyhow!(error)),
        }
    }

    let mut speculative_retries = 0;
    let (state, leader) = loop {
        let (state, leader) = {
            let mut in_flight = PREPARED_IMAGE_IN_FLIGHT.lock();
            if let Some(existing) = in_flight.get(&key) {
                (existing.clone(), false)
            } else {
                let state = Arc::new(InFlightSharedResult::default());
                in_flight.insert(key.clone(), state.clone());
                (state, true)
            }
        };

        if leader {
            break (state, true);
        }

        metrics.record_image_shared_wait();
        match wait_for_shared_result(state).await {
            Ok(entry) => return Ok(entry.to_payload("prepared-shared")),
            Err(error) if should_retry_speculative_failure(work_kind, &error) => {
                speculative_retries += 1;
                if speculative_retries >= MAX_SPECULATIVE_FAILURE_RETRIES {
                    return Err(anyhow::anyhow!(error));
                }
                continue;
            }
            Err(error) => return Err(anyhow::anyhow!(error)),
        }
    };
    debug_assert!(leader);

    let mut flight = PreparedInFlightGuard::new(key.clone(), state.clone());

    let cache_key = key.clone();
    let cached = if let Some(handle) = spawn_image_blocking(work_kind, move || {
        try_load_prepared_image_cache_by_key(&cache_key)
    })
    .await
    {
        handle.await.unwrap_or_default()
    } else {
        None
    };
    if let Some(payload) = cached {
        metrics.record_image_prepared_disk_hit();
        store_prepared_image_memory(key.clone(), &payload);
        flight.publish(Ok(Arc::new(PreparedImageEntry::from_payload(&payload))));
        return Ok(payload);
    }

    metrics.record_image_prepared_miss();
    let source =
        match request_decoded_source_image(path, descriptor.identity.clone(), work_kind, metrics)
            .await
        {
            Ok(source) => source,
            Err(error) => {
                let message = error.to_string();
                flight.publish(Err(message.clone()));
                return Err(anyhow::anyhow!(message));
            }
        };
    let source_for_prepare = source;
    let cache_key = key.clone();
    let Some(handle) = spawn_image_blocking(work_kind, move || {
        // The lease holds decode admission until its source buffer is no
        // longer needed, including the wait for this tracked preparation.
        let payload = prepare_source_image_for_output(
            &source_for_prepare.source,
            target_width,
            target_height,
        )?;
        store_prepared_image_cache_by_key(&cache_key, &payload);
        Ok::<_, anyhow::Error>(payload)
    })
    .await
    else {
        let msg = rejected_image_work(work_kind);
        flight.publish(Err(msg.clone()));
        return Err(anyhow::anyhow!(msg));
    };

    let payload = match handle.await {
        Ok(Ok(payload)) => payload,
        Ok(Err(e)) => {
            let msg = e.to_string();
            flight.publish(Err(msg.clone()));
            return Err(anyhow::anyhow!(msg));
        }
        Err(e) => {
            let msg = format!("image prepare task panicked: {}", e);
            flight.publish(Err(msg.clone()));
            return Err(anyhow::anyhow!(msg));
        }
    };

    store_prepared_image_memory(key.clone(), &payload);
    flight.publish(Ok(Arc::new(PreparedImageEntry::from_payload(&payload))));
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity(name: &str) -> ImageSourceIdentity {
        ImageSourceIdentity {
            path: std::path::PathBuf::from(name),
            file_len: 1,
            modified_secs: 2,
            modified_nanos: 3,
        }
    }

    #[tokio::test]
    async fn cancelled_prepared_leader_notifies_waiters_and_removes_entry() {
        let key = PreparedImageKey {
            source: test_identity("cancelled-prepared-leader"),
            target_width: 1920,
            target_height: 1080,
        };
        let state = Arc::new(InFlightSharedResult::default());
        PREPARED_IMAGE_IN_FLIGHT
            .lock()
            .insert(key.clone(), state.clone());

        let waiter = tokio::spawn(wait_for_shared_result(state.clone()));
        drop(PreparedInFlightGuard::new(key.clone(), state));

        let error = waiter
            .await
            .expect("waiter task should complete")
            .expect_err("cancelled leader must publish an error");
        assert!(error.contains("cancelled"));
        assert!(!PREPARED_IMAGE_IN_FLIGHT.lock().contains_key(&key));
    }

    #[test]
    fn foreground_work_retries_only_prefetch_capacity_failures() {
        assert!(should_retry_speculative_failure(
            BackgroundWorkKind::ImageDecode,
            "image prefetch capacity: decode skipped because decode workers are busy"
        ));
        assert!(!should_retry_speculative_failure(
            BackgroundWorkKind::ImagePrefetch,
            "image prefetch capacity: decode skipped because decode workers are busy"
        ));
        assert!(!should_retry_speculative_failure(
            BackgroundWorkKind::ImageDecode,
            "image source decode failed"
        ));
    }
}
