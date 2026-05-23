use crate::image::types::InFlightSharedResult;
use std::sync::Arc;

pub(crate) async fn wait_for_shared_result<T>(
    state: Arc<InFlightSharedResult<T>>,
) -> Result<Arc<T>, String> {
    loop {
        if let Some(result) = state.result.lock().clone() {
            return result;
        }
        state.notify.notified().await;
    }
}

pub(crate) fn publish_shared_result<T>(
    state: &Arc<InFlightSharedResult<T>>,
    result: Result<Arc<T>, String>,
) {
    *state.result.lock() = Some(result);
    state.notify.notify_waiters();
}
