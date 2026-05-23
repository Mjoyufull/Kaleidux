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

static PREPARED_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<PreparedImageKey, Arc<InFlightSharedResult<PreparedImageEntry>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

static SOURCE_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<ImageSourceIdentity, Arc<InFlightSharedResult<DecodedSourceImage>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

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

    let _permit = acquire_image_work_permit(work_kind, "decode").await?;
    let decode_path = path.to_path_buf();
    let Some(handle) =
        background::spawn_blocking_tracked(work_kind, move || decode_source_image(&decode_path))
    else {
        let msg = "image source decode skipped because shutdown is in progress".to_string();
        publish_shared_result(&state, Err(msg.clone()));
        SOURCE_IMAGE_IN_FLIGHT.lock().remove(&identity);
        return Err(anyhow::anyhow!(msg));
    };

    let result = match handle.await {
        Ok(Ok(source)) => Ok(Arc::new(source)),
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(format!("image source decode task panicked: {}", e)),
    };

    publish_shared_result(&state, result.clone());
    SOURCE_IMAGE_IN_FLIGHT.lock().remove(&identity);

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
        let Some(handle) = background::spawn_blocking_tracked(work_kind, move || {
            prepare_image_for_output_uncached(&fallback_path, target_width, target_height)
        }) else {
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

    if let Some(payload) = try_load_prepared_image_cache_by_key(&key) {
        metrics.record_image_prepared_disk_hit();
        store_prepared_image_memory(key.clone(), &payload);
        publish_shared_result(
            &state,
            Ok(Arc::new(PreparedImageEntry::from_payload(&payload))),
        );
        PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
        return Ok(payload);
    }

    metrics.record_image_prepared_miss();
    let source =
        request_decoded_source_image(path, descriptor.identity.clone(), work_kind, metrics).await?;
    let _permit = acquire_image_work_permit(work_kind, "prepare").await?;
    let source_for_prepare = source.clone();
    let Some(handle) = background::spawn_blocking_tracked(work_kind, move || {
        prepare_source_image_for_output(&source_for_prepare, target_width, target_height)
    }) else {
        let msg = "image prepare skipped because shutdown is in progress".to_string();
        publish_shared_result(&state, Err(msg.clone()));
        PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
        return Err(anyhow::anyhow!(msg));
    };

    let payload = match handle.await {
        Ok(Ok(payload)) => payload,
        Ok(Err(e)) => {
            let msg = e.to_string();
            publish_shared_result(&state, Err(msg.clone()));
            PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
            return Err(anyhow::anyhow!(msg));
        }
        Err(e) => {
            let msg = format!("image prepare task panicked: {}", e);
            publish_shared_result(&state, Err(msg.clone()));
            PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
            return Err(anyhow::anyhow!(msg));
        }
    };

    store_prepared_image_cache_by_key(&key, &payload);
    store_prepared_image_memory(key.clone(), &payload);
    publish_shared_result(
        &state,
        Ok(Arc::new(PreparedImageEntry::from_payload(&payload))),
    );
    PREPARED_IMAGE_IN_FLIGHT.lock().remove(&key);
    Ok(payload)
}
