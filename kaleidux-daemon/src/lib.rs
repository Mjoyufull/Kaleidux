#![allow(
    clippy::collapsible_if,
    clippy::items_after_test_module,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

pub mod background;
pub mod cache;
pub mod content;
pub mod cuda_interop;
pub mod image;
pub mod main_loop;
pub mod metrics;
pub mod monitor;
pub mod monitor_manager;
pub mod observability;
pub mod orchestration;
pub mod queue;
pub mod renderer;
pub mod runtime;
pub mod scripting;
pub mod shaders;
pub mod video;
pub mod wayland;
pub mod wayland_loop;
pub mod x11;
pub mod x11_loop;
