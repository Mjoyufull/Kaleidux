use crate::background::{self, BackgroundWorkKind};
use crate::image as image_pipeline;
use crate::image::runtime_cache::{
    acquire_image_work_permit, decode_source_image, load_image_source_descriptor,
    prepare_image_for_output_uncached, prepare_source_image_for_output,
    prepared_image_key_for_identity, prepared_target_dimensions_from_descriptor,
    select_compatible_prepared_key, store_decoded_source_memory, store_prepared_image_cache_by_key,
    store_prepared_image_memory, store_source_descriptor_memory,
    try_load_compatible_prepared_image_memory, try_load_decoded_source_memory,
    try_load_prepared_image_cache_by_key, try_load_prepared_image_memory,
};
use crate::image::runtime_shared::{publish_shared_result, wait_for_shared_result};
use crate::image::types::{
    DecodedImagePayload, DecodedSourceImage, ImageSourceIdentity, InFlightSharedResult,
    PreparedImageEntry, PreparedImageKey,
};
use crate::metrics;
use parking_lot::Mutex as ParkingMutex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

async fn spawn_image_blocking<T, F>(
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

static SOURCE_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<ImageSourceIdentity, Arc<InFlightSharedResult<DecodedSourceImage>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

struct SourceInFlightGuard {
    key: ImageSourceIdentity,
    state: Arc<InFlightSharedResult<DecodedSourceImage>>,
    published: bool,
}

impl SourceInFlightGuard {
    fn new(key: ImageSourceIdentity, state: Arc<InFlightSharedResult<DecodedSourceImage>>) -> Self {
        Self {
            key,
            state,
            published: false,
        }
    }

    fn publish(&mut self, result: Result<Arc<DecodedSourceImage>, String>) {
        publish_shared_result(&self.state, result);
        remove_source_flight_if_current(&self.key, &self.state);
        self.published = true;
    }
}

impl Drop for SourceInFlightGuard {
    fn drop(&mut self) {
        if !self.published {
            publish_shared_result(
                &self.state,
                Err("image source leader cancelled before publishing a result".to_string()),
            );
            remove_source_flight_if_current(&self.key, &self.state);
        }
    }
}

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
        publish_shared_result(&self.state, result);
        remove_prepared_flight_if_current(&self.key, &self.state);
        self.published = true;
    }
}

impl Drop for PreparedInFlightGuard {
    fn drop(&mut self) {
        if !self.published {
            publish_shared_result(
                &self.state,
                Err("image prepare leader cancelled before publishing a result".to_string()),
            );
            remove_prepared_flight_if_current(&self.key, &self.state);
        }
    }
}

fn remove_source_flight_if_current(
    key: &ImageSourceIdentity,
    state: &Arc<InFlightSharedResult<DecodedSourceImage>>,
) {
    let mut flights = SOURCE_IMAGE_IN_FLIGHT.lock();
    if flights
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, state))
    {
        flights.remove(key);
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
) -> Option<Arc<InFlightSharedResult<PreparedImageEntry>>> {
    let in_flight = PREPARED_IMAGE_IN_FLIGHT.lock();
    let candidate_key =
        select_compatible_prepared_key(in_flight.keys(), identity, target_width, target_height)?;
    in_flight.get(&candidate_key).cloned()
}
pub(crate) async fn request_decoded_source_image(
    path: &Path,
    identity: ImageSourceIdentity,
    work_kind: BackgroundWorkKind,
    metrics: &Arc<metrics::PerformanceMetrics>,
) -> anyhow::Result<Arc<DecodedSourceImage>> {
    if let Some(source) = try_load_decoded_source_memory(&identity) {
        metrics.record_image_source_memory_hit();
        return Ok(source);
    }
    metrics.record_image_source_decode_miss();

    let (state, leader) = {
        let mut in_flight = SOURCE_IMAGE_IN_FLIGHT.lock();
        if let Some(existing) = in_flight.get(&identity) {
            (existing.clone(), false)
        } else {
            let state = Arc::new(InFlightSharedResult::default());
            in_flight.insert(identity.clone(), state.clone());
            (state, true)
        }
    };

    if !leader {
        metrics.record_image_shared_wait();
        return wait_for_shared_result(state)
            .await
            .map_err(|e| anyhow::anyhow!(e));
    }

    let mut flight = SourceInFlightGuard::new(identity.clone(), state.clone());

    let _permit = acquire_image_work_permit(work_kind, "decode").await?;
    let decode_path = path.to_path_buf();
    let Some(handle) =
        spawn_image_blocking(work_kind, move || decode_source_image(&decode_path)).await
    else {
        let msg = "image source decode skipped because shutdown is in progress".to_string();
        flight.publish(Err(msg.clone()));
        return Err(anyhow::anyhow!(msg));
    };

    let result = match handle.await {
        Ok(Ok(source)) => Ok(Arc::new(source)),
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(format!("image source decode task panicked: {}", e)),
    };

    flight.publish(result.clone());

    match result {
        Ok(source) => {
            let descriptor =
                image_pipeline::descriptor::from_decoded_source(identity.clone(), &source);
            store_decoded_source_memory(identity, source.clone());
            store_source_descriptor_memory(descriptor);
            Ok(source)
        }
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

pub(crate) async fn request_prepared_image_payload(
    path: &Path,
    target_width: u32,
    target_height: u32,
    work_kind: BackgroundWorkKind,
    metrics: &Arc<metrics::PerformanceMetrics>,
) -> anyhow::Result<DecodedImagePayload> {
    let Some(descriptor) = load_image_source_descriptor(path) else {
        let fallback_path = path.to_path_buf();
        let Some(handle) = spawn_image_blocking(work_kind, move || {
            prepare_image_for_output_uncached(&fallback_path, target_width, target_height)
        })
        .await
        else {
            return Err(anyhow::anyhow!(
                "image prepare skipped because shutdown is in progress"
            ));
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
    ) {
        metrics.record_image_prepared_compatible_hit();
        return Ok(payload);
    }

    if let Some(state) = find_compatible_prepared_in_flight_state(
        &descriptor.identity,
        prepared_width,
        prepared_height,
    ) {
        metrics.record_image_shared_wait();
        let entry = wait_for_shared_result(state)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        return Ok(entry.to_payload("prepared-shared-compatible"));
    }

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

    if !leader {
        metrics.record_image_shared_wait();
        let entry = wait_for_shared_result(state)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        return Ok(entry.to_payload("prepared-shared"));
    }

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
        request_decoded_source_image(path, descriptor.identity.clone(), work_kind, metrics).await?;
    let _permit = acquire_image_work_permit(work_kind, "prepare").await?;
    let source_for_prepare = source.clone();
    let cache_key = key.clone();
    let Some(handle) = spawn_image_blocking(work_kind, move || {
        let payload =
            prepare_source_image_for_output(&source_for_prepare, target_width, target_height)?;
        store_prepared_image_cache_by_key(&cache_key, &payload);
        Ok::<_, anyhow::Error>(payload)
    })
    .await
    else {
        let msg = "image prepare skipped because shutdown is in progress".to_string();
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
    async fn cancelled_source_leader_notifies_waiters_and_removes_entry() {
        let identity = test_identity("cancelled-source-leader");
        let state = Arc::new(InFlightSharedResult::default());
        SOURCE_IMAGE_IN_FLIGHT
            .lock()
            .insert(identity.clone(), state.clone());

        let waiter = tokio::spawn(wait_for_shared_result(state.clone()));
        drop(SourceInFlightGuard::new(identity.clone(), state));

        let error = waiter
            .await
            .expect("waiter task should complete")
            .expect_err("cancelled leader must publish an error");
        assert!(error.contains("cancelled"));
        assert!(!SOURCE_IMAGE_IN_FLIGHT.lock().contains_key(&identity));
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
}
