use std::sync::atomic::{AtomicBool, Ordering};

static TRACE_ALL_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn set_trace_all_enabled(enabled: bool) {
    TRACE_ALL_ENABLED.store(enabled, Ordering::Release);
}

pub fn trace_all_enabled() -> bool {
    TRACE_ALL_ENABLED.load(Ordering::Acquire)
        || std::env::var("KLD_TRACE_ALL").ok().is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

pub fn trace_idle_poll_interval() -> std::time::Duration {
    if trace_all_enabled() {
        std::time::Duration::from_millis(1)
    } else {
        std::time::Duration::from_millis(250)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TRACE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn trace_all_forces_one_millisecond_idle_poll() {
        let _guard = TRACE_TEST_LOCK.lock().expect("trace test lock poisoned");
        set_trace_all_enabled(true);
        assert!(trace_all_enabled());
        assert_eq!(
            trace_idle_poll_interval(),
            std::time::Duration::from_millis(1)
        );
        set_trace_all_enabled(false);
    }

    #[test]
    fn normal_idle_poll_stays_performance_safe() {
        let _guard = TRACE_TEST_LOCK.lock().expect("trace test lock poisoned");
        set_trace_all_enabled(false);
        assert_eq!(
            trace_idle_poll_interval(),
            std::time::Duration::from_millis(250)
        );
    }
}
