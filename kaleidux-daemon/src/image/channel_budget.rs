use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PERMIT_BYTES: usize = 64 * 1024;
const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

static IMAGE_CHANNEL_BUDGET: std::sync::LazyLock<Arc<ImageChannelBudget>> =
    std::sync::LazyLock::new(|| Arc::new(ImageChannelBudget::from_env()));

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ImageChannelBudgetSnapshot {
    pub(crate) current_bytes: u64,
    pub(crate) high_water_bytes: u64,
    pub(crate) capacity_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct ImageChannelBudget {
    semaphore: Arc<Semaphore>,
    capacity_units: u32,
    current_bytes: Arc<AtomicU64>,
    high_water_bytes: Arc<AtomicU64>,
}

#[derive(Debug)]
pub(crate) struct ImageChannelPermit {
    _permit: OwnedSemaphorePermit,
    charged_bytes: u64,
    current_bytes: Arc<AtomicU64>,
}

impl Drop for ImageChannelPermit {
    fn drop(&mut self) {
        self.current_bytes
            .fetch_sub(self.charged_bytes, Ordering::AcqRel);
    }
}

impl ImageChannelBudget {
    fn from_env() -> Self {
        let budget_bytes = std::env::var("KALEIDUX_IMAGE_CHANNEL_MAX_MIB")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .and_then(|mib| mib.checked_mul(1024 * 1024))
            .filter(|bytes| *bytes >= PERMIT_BYTES)
            .unwrap_or(DEFAULT_BUDGET_BYTES);
        Self::new(budget_bytes)
    }

    fn new(budget_bytes: usize) -> Self {
        let units = budget_bytes.div_ceil(PERMIT_BYTES).min(u32::MAX as usize) as u32;
        Self {
            semaphore: Arc::new(Semaphore::new(units as usize)),
            capacity_units: units,
            current_bytes: Arc::new(AtomicU64::new(0)),
            high_water_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn acquire(self: &Arc<Self>, bytes: usize) -> Option<ImageChannelPermit> {
        let requested_units = bytes
            .max(1)
            .div_ceil(PERMIT_BYTES)
            .min(self.capacity_units as usize) as u32;
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(requested_units)
            .await
            .ok()?;
        let charged_bytes = u64::from(requested_units) * PERMIT_BYTES as u64;
        let current = self
            .current_bytes
            .fetch_add(charged_bytes, Ordering::AcqRel)
            + charged_bytes;
        self.high_water_bytes.fetch_max(current, Ordering::Relaxed);
        Some(ImageChannelPermit {
            _permit: permit,
            charged_bytes,
            current_bytes: self.current_bytes.clone(),
        })
    }

    fn snapshot(&self) -> ImageChannelBudgetSnapshot {
        ImageChannelBudgetSnapshot {
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
            high_water_bytes: self.high_water_bytes.load(Ordering::Relaxed),
            capacity_bytes: u64::from(self.capacity_units) * PERMIT_BYTES as u64,
        }
    }
}

pub(crate) async fn acquire(bytes: usize) -> Option<ImageChannelPermit> {
    IMAGE_CHANNEL_BUDGET.acquire(bytes).await
}

pub(crate) fn snapshot() -> ImageChannelBudgetSnapshot {
    IMAGE_CHANNEL_BUDGET.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn weighted_permit_tracks_bytes_and_releases_on_drop() {
        let budget = Arc::new(ImageChannelBudget::new(PERMIT_BYTES * 2));
        let permit = budget.acquire(PERMIT_BYTES + 1).await.expect("permit");
        assert_eq!(budget.snapshot().current_bytes, (PERMIT_BYTES * 2) as u64);
        drop(permit);
        assert_eq!(budget.snapshot().current_bytes, 0);
    }
}
