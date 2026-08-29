//! Low-frequency Hyprland output-power observation.
//!
//! Wayland does not expose a standard client-side DPMS notification and
//! Hyprland keeps native subsurface swaps serviceable while a monitor is in
//! screensaver-style DPMS off. Querying Hyprland's existing control socket is
//! therefore the narrow compositor-specific fallback: no subprocess, no
//! per-frame work, and no dependency on a user hook.

use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{info, warn};

const ACTIVE_VIDEO_POLL_INTERVAL: Duration = Duration::from_secs(1);
const STATIC_POLL_INTERVAL: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT: Duration = Duration::from_millis(300);
const QUERY_RESULT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const FAIL_OPEN_AFTER: u32 = 5;
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Deserialize)]
struct HyprlandMonitor {
    name: String,
    #[serde(rename = "dpmsStatus")]
    dpms_status: bool,
    #[serde(default)]
    disabled: bool,
}

pub(crate) fn apply_update(ctx: &mut crate::main_loop::MainLoopContext, update: PowerUpdate) {
    for name in &update.powered_off {
        if let Some(player) = ctx.video_players.get(name) {
            if let Err(error) = player.pause() {
                warn!("[DISPLAY-POWER] Failed to pause {name}: {error}");
            }
        }
        ctx.latest_video_frames.clear_source(name);
        if let Some(renderer) = ctx.renderers.get_mut(name) {
            renderer.needs_redraw = false;
            renderer.frame_callback_pending = false;
            renderer.last_frame_request = None;
        }
    }
    for (name, suspended_for) in &update.powered_on {
        if let Some(renderer) = ctx.renderers.get_mut(name) {
            if let Some(start) = renderer.transition_start_time.as_mut() {
                *start += *suspended_for;
            }
            renderer.needs_redraw = true;
        }
        if !ctx.monitor_manager.is_paused()
            && let Some(player) = ctx.video_players.get(name)
        {
            if let Err(error) = player.resume() {
                warn!("[DISPLAY-POWER] Failed to resume {name}: {error}");
            } else {
                player.request_video_frame();
            }
        }
    }

    match update.transition {
        None => {}
        Some(PowerTransition::Suspended) => {
            ctx.display_power_suspended = true;
            ctx.monitor_manager.set_power_suspended(true);
            info!(
                "[DISPLAY-POWER] All compositor outputs are off; decode demand and content scheduling suspended"
            );
        }
        Some(PowerTransition::Resumed { suspended_for }) => {
            ctx.display_power_suspended = false;
            ctx.monitor_manager.set_power_suspended(false);
            info!(
                "[DISPLAY-POWER] Compositor output resumed after {:.1}s; presentation state re-armed",
                suspended_for.as_secs_f64()
            );
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PowerUpdate {
    transition: Option<PowerTransition>,
    powered_off: Vec<String>,
    powered_on: Vec<(String, Duration)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PowerTransition {
    Suspended,
    Resumed { suspended_for: Duration },
}

pub(crate) struct HyprlandPowerMonitor {
    socket_path: Option<PathBuf>,
    powered_outputs: HashMap<String, bool>,
    next_probe: Instant,
    result_check_deadline: Option<Instant>,
    in_flight: Option<tokio::task::JoinHandle<anyhow::Result<HashMap<String, bool>>>>,
    suspended_since: Option<Instant>,
    output_suspended_since: HashMap<String, Instant>,
    consecutive_failures: u32,
    query_failure_reported: bool,
}

impl HyprlandPowerMonitor {
    pub(crate) fn from_environment(now: Instant) -> Self {
        let socket_path = std::env::var_os("KLD_HYPRLAND_POWER_SOCKET")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("XDG_RUNTIME_DIR")
                    .zip(std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE"))
                    .map(|(runtime, signature)| {
                        PathBuf::from(runtime)
                            .join("hypr")
                            .join(signature)
                            .join(".socket.sock")
                    })
            });
        if let Some(path) = &socket_path {
            if path.exists() {
                info!(
                    "[DISPLAY-POWER] Hyprland DPMS observer enabled via {}",
                    path.display()
                );
            } else {
                info!(
                    "[DISPLAY-POWER] Hyprland DPMS socket is not ready yet at {}; observer will retry without blocking startup",
                    path.display()
                );
            }
        }
        Self {
            socket_path,
            powered_outputs: HashMap::new(),
            next_probe: now,
            result_check_deadline: None,
            in_flight: None,
            suspended_since: None,
            output_suspended_since: HashMap::new(),
            consecutive_failures: 0,
            query_failure_reported: false,
        }
    }

    #[cfg(test)]
    fn with_socket(path: PathBuf, now: Instant) -> Self {
        Self {
            socket_path: Some(path),
            powered_outputs: HashMap::new(),
            next_probe: now,
            result_check_deadline: None,
            in_flight: None,
            suspended_since: None,
            output_suspended_since: HashMap::new(),
            consecutive_failures: 0,
            query_failure_reported: false,
        }
    }

    pub(crate) fn next_deadline(
        &self,
        has_outputs: bool,
    ) -> Option<(Instant, crate::observability::wake::DeadlineReason)> {
        (has_outputs && self.socket_path.is_some()).then_some((
            self.result_check_deadline.unwrap_or(self.next_probe),
            crate::observability::wake::DeadlineReason::DisplayPower,
        ))
    }

    pub(crate) fn is_powered(&self, output: &str) -> bool {
        self.powered_outputs.get(output).copied().unwrap_or(true)
    }

    pub(crate) fn is_suspended(&self) -> bool {
        self.suspended_since.is_some()
    }

    pub(crate) fn needs_output_snapshot(&self) -> bool {
        self.in_flight
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }

    /// Drop state for outputs no longer owned by the display backend.
    ///
    /// Hyprland omits unplugged monitors from later query results, so query
    /// replacement alone cannot clear `output_suspended_since`. Pruning at the
    /// same low-frequency result boundary also prevents a re-used connector
    /// name from inheriting an arbitrarily old suspension duration.
    pub(crate) fn retain_outputs(
        &mut self,
        output_names: &[String],
        now: Instant,
    ) -> Option<PowerUpdate> {
        let live = output_names
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        self.powered_outputs
            .retain(|name, _| live.contains(name.as_str()));
        self.output_suspended_since
            .retain(|name, _| live.contains(name.as_str()));

        // The completed query is applied immediately after this prune. Do not
        // infer power for a newly added output that is absent from the prior
        // snapshot: doing so could emit Resumed here and Suspended from that
        // same query. The only transition pruning must synthesize is removal
        // of the final suspended output.
        let transition = match (self.suspended_since, output_names.is_empty()) {
            (Some(started), true) => {
                self.suspended_since = None;
                Some(PowerTransition::Resumed {
                    suspended_for: now.saturating_duration_since(started),
                })
            }
            _ => None,
        };
        transition.map(|transition| PowerUpdate {
            transition: Some(transition),
            ..PowerUpdate::default()
        })
    }

    pub(crate) async fn poll_if_due(
        &mut self,
        output_names: &[String],
        has_active_video: bool,
        now: Instant,
    ) -> Option<PowerUpdate> {
        if let Some(handle) = self.in_flight.as_ref() {
            if !handle.is_finished() {
                if self
                    .result_check_deadline
                    .is_some_and(|deadline| now >= deadline)
                {
                    self.result_check_deadline = Some(now + QUERY_RESULT_POLL_INTERVAL);
                }
                return None;
            }
            let handle = self
                .in_flight
                .take()
                .expect("in-flight query checked above");
            self.result_check_deadline = None;
            let result = match handle.await {
                Ok(result) => result,
                Err(error) => Err(anyhow::anyhow!("query task failed: {error}")),
            };
            return self.apply_query_result(output_names, now, result);
        }
        let path = self.socket_path.as_ref()?.clone();
        if has_active_video
            && self.next_probe.saturating_duration_since(now) > ACTIVE_VIDEO_POLL_INTERVAL
        {
            // Startup can issue its first probe before the initial player is
            // ready. Do not retain that static ten-second cadence after video
            // becomes active.
            self.next_probe = now;
        }
        if now < self.next_probe {
            return None;
        }
        let interval = if has_active_video {
            ACTIVE_VIDEO_POLL_INTERVAL
        } else {
            STATIC_POLL_INTERVAL
        };
        self.next_probe = now + interval;
        self.result_check_deadline = Some(now + QUERY_RESULT_POLL_INTERVAL);
        self.in_flight = Some(tokio::spawn(
            async move { query_powered_outputs(&path).await },
        ));
        None
    }

    fn apply_query_result(
        &mut self,
        output_names: &[String],
        now: Instant,
        result: anyhow::Result<HashMap<String, bool>>,
    ) -> Option<PowerUpdate> {
        let previous = self.powered_outputs.clone();
        match result {
            Ok(powered_outputs) => {
                if self.query_failure_reported {
                    info!("[DISPLAY-POWER] Hyprland DPMS queries recovered");
                    self.query_failure_reported = false;
                }
                self.consecutive_failures = 0;
                self.powered_outputs = powered_outputs;
            }
            Err(error) => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                if !self.query_failure_reported {
                    warn!(
                        "[DISPLAY-POWER] Hyprland DPMS query failed; preserving last known visible state: {error}"
                    );
                    self.query_failure_reported = true;
                }
                if self.consecutive_failures >= FAIL_OPEN_AFTER
                    && (!self.output_suspended_since.is_empty() || self.suspended_since.is_some())
                {
                    let mut update = PowerUpdate::default();
                    for (name, started) in self.output_suspended_since.drain() {
                        update
                            .powered_on
                            .push((name, now.saturating_duration_since(started)));
                    }
                    if let Some(started) = self.suspended_since.take() {
                        update.transition = Some(PowerTransition::Resumed {
                            suspended_for: now.saturating_duration_since(started),
                        });
                    }
                    self.powered_outputs.clear();
                    warn!(
                        "[DISPLAY-POWER] {} consecutive query failures; failing open so rendering cannot remain suspended",
                        self.consecutive_failures
                    );
                    return Some(update);
                }
                return None;
            }
        }

        let mut update = PowerUpdate::default();
        for name in output_names {
            let was_powered = previous.get(name).copied().unwrap_or(true);
            let is_powered = self.powered_outputs.get(name).copied().unwrap_or(true);
            match (was_powered, is_powered) {
                (true, false) => {
                    self.output_suspended_since.insert(name.clone(), now);
                    update.powered_off.push(name.clone());
                }
                (false, true) => {
                    let started = self.output_suspended_since.remove(name).unwrap_or(now);
                    update
                        .powered_on
                        .push((name.clone(), now.saturating_duration_since(started)));
                }
                _ => {}
            }
        }
        let all_off = !output_names.is_empty()
            && output_names.iter().all(|name| {
                self.powered_outputs
                    .get(name)
                    .is_some_and(|powered| !powered)
            });
        match (self.suspended_since, all_off) {
            (None, true) => {
                self.suspended_since = Some(now);
                update.transition = Some(PowerTransition::Suspended);
            }
            (Some(started), false) => {
                self.suspended_since = None;
                update.transition = Some(PowerTransition::Resumed {
                    suspended_for: now.saturating_duration_since(started),
                });
            }
            _ => {}
        }
        (!update.powered_off.is_empty()
            || !update.powered_on.is_empty()
            || update.transition.is_some())
        .then_some(update)
    }
}

async fn query_powered_outputs(path: &Path) -> anyhow::Result<HashMap<String, bool>> {
    let response = tokio::time::timeout(QUERY_TIMEOUT, async {
        let mut stream = UnixStream::connect(path).await?;
        stream.write_all(b"j/monitors").await?;
        stream.shutdown().await?;
        let mut response = Vec::new();
        stream
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut response)
            .await?;
        anyhow::Ok(response)
    })
    .await
    .map_err(|_| anyhow::anyhow!("query timed out after {}ms", QUERY_TIMEOUT.as_millis()))??;
    anyhow::ensure!(
        response.len() <= MAX_RESPONSE_BYTES as usize,
        "response exceeded {} bytes",
        MAX_RESPONSE_BYTES
    );
    let monitors: Vec<HyprlandMonitor> = serde_json::from_slice(&response)?;
    Ok(monitors
        .into_iter()
        .map(|monitor| (monitor.name, monitor.dpms_status && !monitor.disabled))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{HyprlandPowerMonitor, PowerTransition, PowerUpdate, query_powered_outputs};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

    struct SocketFixture {
        directory: std::path::PathBuf,
    }

    impl Drop for SocketFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.directory.join("hypr.sock"));
            let _ = std::fs::remove_dir(&self.directory);
        }
    }

    async fn serve_once(response: &'static [u8]) -> (SocketFixture, std::path::PathBuf) {
        let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "kaleidux-hyprland-power-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).expect("create temporary socket directory");
        let path = directory.join("hypr.sock");
        let listener = UnixListener::bind(&path).expect("bind mock Hyprland socket");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept query");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .await
                .expect("read request");
            assert_eq!(request, b"j/monitors");
            stream.write_all(response).await.expect("write response");
        });
        (SocketFixture { directory }, path)
    }

    async fn poll_until_update(monitor: &mut HyprlandPowerMonitor, now: Instant) -> PowerUpdate {
        for _ in 0..100 {
            if let Some(update) = monitor.poll_if_due(&["eDP-1".to_string()], true, now).await {
                return update;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("power query did not complete")
    }

    #[tokio::test]
    async fn parses_dpms_and_disabled_monitor_state() {
        let (_directory, path) = serve_once(
            br#"[{"name":"eDP-1","dpmsStatus":true,"disabled":false},{"name":"DP-1","dpmsStatus":true,"disabled":true}]"#,
        )
        .await;
        let powered = query_powered_outputs(&path).await.expect("query succeeds");
        assert_eq!(powered.get("eDP-1"), Some(&true));
        assert_eq!(powered.get("DP-1"), Some(&false));
    }

    #[tokio::test]
    async fn reports_one_suspend_and_resume_transition() {
        let now = Instant::now();
        let (_off_directory, off_path) =
            serve_once(br#"[{"name":"eDP-1","dpmsStatus":false}]"#).await;
        let mut monitor = HyprlandPowerMonitor::with_socket(off_path, now);
        let suspended = poll_until_update(&mut monitor, now).await;
        assert_eq!(suspended.transition, Some(PowerTransition::Suspended));
        assert_eq!(suspended.powered_off, ["eDP-1"]);
        assert!(monitor.is_suspended());
        assert!(!monitor.is_powered("eDP-1"));

        let resume_at = now + Duration::from_secs(2);
        let (_on_directory, on_path) = serve_once(br#"[{"name":"eDP-1","dpmsStatus":true}]"#).await;
        monitor.socket_path = Some(on_path);
        let resumed = poll_until_update(&mut monitor, resume_at).await;
        assert_eq!(
            resumed.transition,
            Some(PowerTransition::Resumed {
                suspended_for: Duration::from_secs(2)
            })
        );
        assert_eq!(
            resumed.powered_on,
            [("eDP-1".to_string(), Duration::from_secs(2))]
        );
        assert!(!monitor.is_suspended());
        assert!(monitor.is_powered("eDP-1"));
    }

    #[test]
    fn repeated_query_failures_fail_open() {
        let now = Instant::now();
        let mut monitor = HyprlandPowerMonitor::with_socket("/missing".into(), now);
        let outputs = ["eDP-1".to_string()];
        let suspended = monitor
            .apply_query_result(&outputs, now, Ok([("eDP-1".to_string(), false)].into()))
            .expect("suspend update");
        assert_eq!(suspended.transition, Some(PowerTransition::Suspended));
        for failure in 1..=5 {
            let update = monitor.apply_query_result(
                &outputs,
                now + Duration::from_secs(failure as u64),
                Err(anyhow::anyhow!("mock failure")),
            );
            if failure < 5 {
                assert!(update.is_none());
            } else {
                let update = update.expect("fifth failure fails open");
                assert!(matches!(
                    update.transition,
                    Some(PowerTransition::Resumed { .. })
                ));
                assert_eq!(update.powered_on.len(), 1);
            }
        }
        assert!(monitor.is_powered("eDP-1"));
        assert!(!monitor.is_suspended());
    }

    #[test]
    fn unplugged_output_does_not_leak_suspension_state_into_replug() {
        let now = Instant::now();
        let mut monitor = HyprlandPowerMonitor::with_socket("/missing".into(), now);
        let outputs = ["eDP-1".to_string()];
        let suspended = monitor
            .apply_query_result(&outputs, now, Ok([("eDP-1".to_string(), false)].into()))
            .expect("suspend update");
        assert_eq!(suspended.transition, Some(PowerTransition::Suspended));

        let removed_at = now + Duration::from_secs(2);
        let removed = monitor
            .retain_outputs(&[], removed_at)
            .expect("removing the last off output resumes global scheduling");
        assert_eq!(
            removed.transition,
            Some(PowerTransition::Resumed {
                suspended_for: Duration::from_secs(2)
            })
        );
        assert!(monitor.output_suspended_since.is_empty());
        assert!(monitor.powered_outputs.is_empty());

        let replugged = monitor.apply_query_result(
            &outputs,
            now + Duration::from_secs(60),
            Ok([("eDP-1".to_string(), true)].into()),
        );
        assert!(replugged.is_none());
        assert!(monitor.is_powered("eDP-1"));
    }

    #[test]
    fn newly_docked_off_output_does_not_flap_global_power_state() {
        let now = Instant::now();
        let mut monitor = HyprlandPowerMonitor::with_socket("/missing".into(), now);
        let original = ["eDP-1".to_string()];
        monitor
            .apply_query_result(&original, now, Ok([("eDP-1".to_string(), false)].into()))
            .expect("initial suspend");

        let docked = ["eDP-1".to_string(), "DP-1".to_string()];
        assert!(
            monitor
                .retain_outputs(&docked, now + Duration::from_secs(1))
                .is_none(),
            "pruning the previous snapshot must not guess that a new output is on"
        );
        let update = monitor.apply_query_result(
            &docked,
            now + Duration::from_secs(1),
            Ok([("eDP-1".to_string(), false), ("DP-1".to_string(), false)].into()),
        );
        assert!(
            update.is_some_and(|update| update.transition.is_none()),
            "the already-suspended display remains suspended without a flap"
        );
        assert!(monitor.is_suspended());
    }

    #[tokio::test]
    async fn active_video_shortens_a_static_probe_deadline() {
        let now = Instant::now();
        let mut monitor = HyprlandPowerMonitor::with_socket("/missing".into(), now);
        monitor.next_probe = now + super::STATIC_POLL_INTERVAL;
        assert!(
            monitor
                .poll_if_due(&[], true, now + Duration::from_millis(1))
                .await
                .is_none()
        );
        assert!(monitor.in_flight.is_some());
        assert_eq!(
            monitor.next_probe,
            now + Duration::from_millis(1) + super::ACTIVE_VIDEO_POLL_INTERVAL
        );
    }
}
