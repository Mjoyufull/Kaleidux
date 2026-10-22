//! Coordinates display-loop-owned playback state with asynchronous workers and IPC.

pub(crate) mod channel_drain;
pub(crate) mod commands;
mod commands_reload;
pub(crate) mod housekeeping;
pub(crate) mod idle_runtime;
pub mod idle_wait;
pub(crate) mod ipc;
pub(crate) mod player_drain;
pub(crate) mod player_events;
pub mod startup_barrier;
pub(crate) mod startup_runtime;
pub(crate) mod timing;

pub(crate) mod preview;
