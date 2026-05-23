#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WakeReason {
    Command,
    VideoFrame,
    WaylandFd,
    Image,
    PlayerReady,
    PlayerEvent,
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
            Self::Deadline => 6,
            Self::Immediate => 7,
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
            Self::Deadline => "deadline",
            Self::Immediate => "immediate",
        }
    }
}

pub const WAKE_REASON_COUNT: usize = 8;
pub const WAKE_REASONS: [WakeReason; WAKE_REASON_COUNT] = [
    WakeReason::Command,
    WakeReason::VideoFrame,
    WakeReason::WaylandFd,
    WakeReason::Image,
    WakeReason::PlayerReady,
    WakeReason::PlayerEvent,
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
}

impl DeadlineReason {
    pub fn as_index(self) -> usize {
        match self {
            Self::PeriodicFallback => 0,
            Self::ContentSwitch => 1,
            Self::ScriptTick => 2,
            Self::StartupBarrier => 3,
            Self::WaylandRetry => 4,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::PeriodicFallback => "periodic",
            Self::ContentSwitch => "content_switch",
            Self::ScriptTick => "script_tick",
            Self::StartupBarrier => "startup_barrier",
            Self::WaylandRetry => "wayland_retry",
        }
    }
}

pub const DEADLINE_REASON_COUNT: usize = 5;
pub const DEADLINE_REASONS: [DeadlineReason; DEADLINE_REASON_COUNT] = [
    DeadlineReason::PeriodicFallback,
    DeadlineReason::ContentSwitch,
    DeadlineReason::ScriptTick,
    DeadlineReason::StartupBarrier,
    DeadlineReason::WaylandRetry,
];
