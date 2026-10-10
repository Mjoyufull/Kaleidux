#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WakeReason {
    Command,
    VideoFrame,
    WaylandFd,
    Image,
    PlayerReady,
    PlayerEvent,
    Watcher,
    Deadline,
    Immediate,
}

impl WakeReason {
    pub fn as_index(self) -> usize {
        match self {
            Self::Command => 0,
            Self::VideoFrame => 1,
            Self::WaylandFd => 2,
            Self::Image => 3,
            Self::PlayerReady => 4,
            Self::PlayerEvent => 5,
            Self::Watcher => 6,
            Self::Deadline => 7,
            Self::Immediate => 8,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Command => "cmd",
            Self::VideoFrame => "video_frame",
            Self::WaylandFd => "wayland_fd",
            Self::Image => "image",
            Self::PlayerReady => "player_ready",
            Self::PlayerEvent => "player_event",
            Self::Watcher => "watcher",
            Self::Deadline => "deadline",
            Self::Immediate => "immediate",
        }
    }
}

pub const WAKE_REASON_COUNT: usize = 9;
pub const WAKE_REASONS: [WakeReason; WAKE_REASON_COUNT] = [
    WakeReason::Command,
    WakeReason::VideoFrame,
    WakeReason::WaylandFd,
    WakeReason::Image,
    WakeReason::PlayerReady,
    WakeReason::PlayerEvent,
    WakeReason::Watcher,
    WakeReason::Deadline,
    WakeReason::Immediate,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeadlineReason {
    PeriodicFallback,
    ContentSwitch,
    ScriptTick,
    StartupBarrier,
    WaylandRetry,
    PoolCleanup,
    StatsFlush,
    Metrics,
    X11Randr,
    DisplayPower,
}

impl DeadlineReason {
    pub fn as_index(self) -> usize {
        match self {
            Self::PeriodicFallback => 0,
            Self::ContentSwitch => 1,
            Self::ScriptTick => 2,
            Self::StartupBarrier => 3,
            Self::WaylandRetry => 4,
            Self::PoolCleanup => 5,
            Self::StatsFlush => 6,
            Self::Metrics => 7,
            Self::X11Randr => 8,
            Self::DisplayPower => 9,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::PeriodicFallback => "periodic",
            Self::ContentSwitch => "content_switch",
            Self::ScriptTick => "script_tick",
            Self::StartupBarrier => "startup_barrier",
            Self::WaylandRetry => "wayland_retry",
            Self::PoolCleanup => "pool_cleanup",
            Self::StatsFlush => "stats_flush",
            Self::Metrics => "metrics",
            Self::X11Randr => "x11_randr",
            Self::DisplayPower => "display_power",
        }
    }
}

pub const DEADLINE_REASON_COUNT: usize = 10;
pub const DEADLINE_REASONS: [DeadlineReason; DEADLINE_REASON_COUNT] = [
    DeadlineReason::PeriodicFallback,
    DeadlineReason::ContentSwitch,
    DeadlineReason::ScriptTick,
    DeadlineReason::StartupBarrier,
    DeadlineReason::WaylandRetry,
    DeadlineReason::PoolCleanup,
    DeadlineReason::StatsFlush,
    DeadlineReason::Metrics,
    DeadlineReason::X11Randr,
    DeadlineReason::DisplayPower,
];
