//! Keeps source decode admission alive through shared output preparation.

use super::runtime_cache::{
    acquire_image_work_permit, decode_source_image, store_decoded_source_memory,
    store_source_descriptor_memory, try_load_decoded_source_memory,
};
use super::runtime_request::{
    MAX_SPECULATIVE_FAILURE_RETRIES, rejected_image_work, should_retry_speculative_failure,
    spawn_image_blocking,
};
use super::runtime_shared::{publish_shared_result, wait_for_shared_result};
use super::types::{DecodedSourceImage, ImageSourceIdentity, InFlightSharedResult};
use crate::background::BackgroundWorkKind;
use crate::image as image_pipeline;
use crate::metrics;
use parking_lot::Mutex as ParkingMutex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct LeasedSourceImage {
    pub(super) source: Arc<DecodedSourceImage>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

static SOURCE_IMAGE_IN_FLIGHT: once_cell::sync::Lazy<
    ParkingMutex<HashMap<ImageSourceIdentity, Arc<InFlightSharedResult<LeasedSourceImage>>>>,
> = once_cell::sync::Lazy::new(|| ParkingMutex::new(HashMap::new()));

struct SourceInFlightGuard {
    key: ImageSourceIdentity,
    state: Arc<InFlightSharedResult<LeasedSourceImage>>,
    published: bool,
}

impl SourceInFlightGuard {
    fn new(key: ImageSourceIdentity, state: Arc<InFlightSharedResult<LeasedSourceImage>>) -> Self {
        Self {
            key,
            state,
            published: false,
        }
    }

    fn publish(&mut self, result: Result<Arc<LeasedSourceImage>, String>) {
        remove_source_flight_if_current(&self.key, &self.state);
        publish_shared_result(&self.state, result);
        self.published = true;
    }
}

impl Drop for SourceInFlightGuard {
    fn drop(&mut self) {
        if !self.published {
            remove_source_flight_if_current(&self.key, &self.state);
            publish_shared_result(
                &self.state,
                Err("image source leader cancelled before publishing a result".to_string()),
            );
        }
    }
}

fn remove_source_flight_if_current(
    key: &ImageSourceIdentity,
    state: &Arc<InFlightSharedResult<LeasedSourceImage>>,
) {
    let mut flights = SOURCE_IMAGE_IN_FLIGHT.lock();
    if flights
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, state))
    {
        flights.remove(key);
    }
}

pub(super) async fn request_decoded_source_image(
    path: &Path,
    identity: ImageSourceIdentity,
    work_kind: BackgroundWorkKind,
    metrics: &Arc<metrics::PerformanceMetrics>,
) -> anyhow::Result<Arc<LeasedSourceImage>> {
    let mut speculative_retries = 0;
    let (state, leader) = loop {
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

        if leader {
            break (state, true);
        }

        metrics.record_image_shared_wait();
        match wait_for_shared_result(state).await {
            Ok(source) => return Ok(source),
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

    let mut flight = SourceInFlightGuard::new(identity.clone(), state.clone());

    let result = decode_source_with_lease(path, &identity, work_kind, metrics)
        .await
        .map_err(|error| error.to_string());

    if let Ok(source) = &result {
        let descriptor =
            image_pipeline::descriptor::from_decoded_source(identity.clone(), &source.source);
        store_decoded_source_memory(identity, source.source.clone());
        store_source_descriptor_memory(descriptor);
    }
    flight.publish(result.clone());

    match result {
        Ok(source) => Ok(source),
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

async fn decode_source_with_lease(
    path: &Path,
    identity: &ImageSourceIdentity,
    work_kind: BackgroundWorkKind,
    metrics: &Arc<metrics::PerformanceMetrics>,
) -> anyhow::Result<Arc<LeasedSourceImage>> {
    let permit = acquire_image_work_permit(work_kind, "decode/prepare").await?;
    let path = path.to_path_buf();
    let identity = identity.clone();
    let metrics = metrics.clone();
    let handle = spawn_image_blocking(work_kind, move || {
        // Look up cached bytes only after admission, so queued requests cannot
        // retain evicted large sources while waiting for another worker slot.
        let source = if let Some(source) = try_load_decoded_source_memory(&identity) {
            metrics.record_image_source_memory_hit();
            source
        } else {
            metrics.record_image_source_decode_miss();
            let decoded = decode_source_image(&path)?;
            tracing::debug!(
                "[MEDIA-IMAGE] format={} width={} height={} decoded_bytes={}",
                decoded.format,
                decoded.width,
                decoded.height,
                decoded.pixels.len()
            );
            Arc::new(decoded)
        };
        Ok::<_, anyhow::Error>(Arc::new(LeasedSourceImage {
            source,
            _permit: permit,
        }))
    })
    .await
    .ok_or_else(|| anyhow::anyhow!(rejected_image_work(work_kind)))?;
    handle
        .await
        .map_err(|error| anyhow::anyhow!("image source decode task panicked: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_source_leader_notifies_waiters_and_removes_entry() {
        let identity = ImageSourceIdentity {
            path: "cancelled-source-leader".into(),
            file_len: 1,
            modified_secs: 2,
            modified_nanos: 3,
        };
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
    async fn source_admission_survives_shared_preparation_waits() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let lease = Arc::new(LeasedSourceImage {
            source: Arc::new(DecodedSourceImage {
                pixels: super::super::types::DecodedSourcePixels::Rgba(vec![0; 4].into()),
                width: 1,
                height: 1,
                format: "test".into(),
                decode: std::time::Duration::ZERO,
                convert: std::time::Duration::ZERO,
            }),
            _permit: permit,
        });
        let preparing = lease.clone();
        drop(lease);
        assert!(semaphore.clone().try_acquire_owned().is_err());
        drop(preparing);
        assert!(semaphore.try_acquire_owned().is_ok());
    }
}
