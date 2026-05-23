use std::time::{Duration, Instant};

pub(crate) fn next_idle_wake_deadline(
    now: Instant,
    periodic_interval: Option<Duration>,
    real_deadline: Option<Instant>,
) -> Option<Instant> {
    match (periodic_interval, real_deadline) {
        (Some(interval), Some(deadline)) => Some(deadline.min(now + interval)),
        (Some(interval), None) => Some(now + interval),
        (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_idle_wake_prefers_pending_switch_deadline() {
        let now = Instant::now();
        let periodic = Some(Duration::from_secs(1));
        let switch_deadline = now + Duration::from_millis(250);

        assert_eq!(
            next_idle_wake_deadline(now, periodic, Some(switch_deadline)),
            Some(switch_deadline)
        );
    }

    #[test]
    fn next_idle_wake_falls_back_to_periodic_when_switch_is_later() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);
        let switch_deadline = now + Duration::from_secs(3);

        assert_eq!(
            next_idle_wake_deadline(now, Some(periodic), Some(switch_deadline)),
            Some(now + periodic)
        );
    }

    #[test]
    fn next_idle_wake_uses_periodic_when_no_switch_is_pending() {
        let now = Instant::now();
        let periodic = Duration::from_secs(1);

        assert_eq!(
            next_idle_wake_deadline(now, Some(periodic), None),
            Some(now + periodic)
        );
    }

    #[test]
    fn next_idle_wake_can_sleep_without_periodic_fallback() {
        let now = Instant::now();

        assert_eq!(next_idle_wake_deadline(now, None, None), None);
    }
}
