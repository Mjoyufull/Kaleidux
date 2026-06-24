#![allow(
    clippy::collapsible_if,
    clippy::items_after_test_module,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

use kaleidux_common::Transition;
use std::time::Instant;
use tracing::{error, info, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt as subscriber_fmt;
use tracing_subscriber::{EnvFilter, Registry, prelude::*};

// Use jemalloc for better memory fragmentation handling in long-running processes
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod background;
mod cache;
mod cuda_interop;
mod main_loop;
mod metrics;
mod monitor;
mod monitor_manager;
mod orchestration;
mod queue;
mod renderer;
mod scripting;
mod shaders;
mod video;
mod wayland;
mod wayland_loop;
mod x11;
mod x11_loop;

use chrono::Local;

struct CustomTimer;

impl tracing_subscriber::fmt::time::FormatTime for CustomTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Local::now();
        write!(w, "{}", now.format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, about, long_about = None)]
struct Args {
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=4))]
    log: Option<u8>,
    #[arg(long)]
    demo: bool,
    #[arg(long)]
    video_mode: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("kaleidux-main")
        .build()?;
    let result = runtime.block_on(async_main());
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

async fn async_main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 1. Initialize Logging
    let log_level = args.log;
    let _guards = {
        let filter = match log_level {
            Some(1) => LevelFilter::WARN,
            Some(2) => LevelFilter::INFO,
            Some(3) => LevelFilter::DEBUG,
            Some(4) => LevelFilter::TRACE,
            None => LevelFilter::WARN,
            Some(_) => LevelFilter::INFO,
        };

        let env_filter = EnvFilter::builder()
            .with_default_directive(filter.into())
            .from_env_lossy()
            .add_directive("wgpu_core=warn".parse().unwrap())
            .add_directive("wgpu_hal=warn".parse().unwrap())
            .add_directive("naga=warn".parse().unwrap())
            .add_directive("calloop=warn".parse().unwrap())
            .add_directive("smithay_client_toolkit=warn".parse().unwrap());

        if let Some(level) = log_level {
            let config_dir = dirs::config_dir()
                .ok_or_else(|| anyhow::anyhow!("Could not find config directory"))?
                .join("kaleidux")
                .join("logs");
            std::fs::create_dir_all(&config_dir)?;

            let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S");
            let log_path = config_dir.join(format!("kaleidux-daemon-{}.log", timestamp));
            let file = std::fs::File::create(&log_path)?;
            println!("Logging to file: {}", log_path.display());
            let (non_blocking_file, file_guard) = tracing_appender::non_blocking(file);
            let (non_blocking_stdout, stdout_guard) =
                tracing_appender::non_blocking(std::io::stdout());

            let file_layer = subscriber_fmt::layer()
                .with_writer(non_blocking_file)
                .with_ansi(false)
                .with_timer(CustomTimer);

            let stdout_layer = subscriber_fmt::layer()
                .with_writer(non_blocking_stdout)
                .with_timer(CustomTimer);

            Registry::default()
                .with(env_filter)
                .with(file_layer)
                .with(stdout_layer)
                .init();

            info!(
                "Kaleidux Daemon starting... (Level {}, File: {})",
                level,
                config_dir.display()
            );
            (Some(file_guard), Some(stdout_guard))
        } else {
            let stderr_layer = subscriber_fmt::layer()
                .with_writer(std::io::stderr)
                .with_timer(CustomTimer);
            Registry::default()
                .with(env_filter)
                .with(stderr_layer)
                .init();
            (None, None)
        }
    };

    // Load compiled WGSL shader strings from disk (P-15 cache layer 1)
    if let Err(e) = crate::shaders::ShaderManager::load_cache() {
        tracing::debug!("[SHADER] Could not load WGSL cache: {}", e);
    }

    // 1b. Set video decode mode from CLI flag
    if let Some(ref mode_str) = args.video_mode {
        let deprecated_cuda_alias = mode_str.eq_ignore_ascii_case("cuda-strict");
        let deprecated_dmabuf_alias = mode_str.eq_ignore_ascii_case("zero-copy");
        let mode = match mode_str.to_lowercase().as_str() {
            "cuda" | "nvdec" | "nvidia" | "cuda-strict" => crate::video::VideoMode::StrictCuda,
            "cpu" => crate::video::VideoMode::ForceCpu,
            "dmabuf" | "dma-buf" | "zero-copy" => crate::video::VideoMode::ForceDmaBuf,
            "nv12" => crate::video::VideoMode::ForceNv12,
            "rgba" => crate::video::VideoMode::ForceRgba,
            "auto" => crate::video::VideoMode::Auto,
            other => {
                let msg = format!(
                    "ERROR: Unknown --video-mode '{}', valid: auto, cpu, cuda, cuda-strict, dmabuf, nv12, rgba",
                    other
                );
                eprintln!("{}", msg);
                error!("{}", msg);
                std::process::exit(1);
            }
        };
        if deprecated_cuda_alias {
            warn!(
                "[VIDEO] --video-mode cuda-strict is deprecated; use --video-mode cuda for strict CUDA-only negotiation"
            );
        }
        if deprecated_dmabuf_alias {
            warn!(
                "[VIDEO] --video-mode zero-copy is deprecated; use --video-mode dmabuf to force the DMA-BUF path or --video-mode auto to let Kaleidux choose the fastest zero-copy path"
            );
        }
        crate::video::set_video_mode(mode);
    }

    // 2. Load Configuration
    let mut config = match orchestration::Config::load().await {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("Failed to load configuration: {}. Using defaults.", e);
            orchestration::Config::default()
        }
    };

    if args.demo {
        info!("Demo mode enabled! Overriding configuration to use current directory...");
        let current_dir = std::env::current_dir()?;
        config.any.path = Some(current_dir);
        config.any.duration = Some(std::time::Duration::from_secs(10));
        config.global.video_ratio = Some(100);
        config.any.transition_time = Some(1500);
        config.any.transition = Some(Transition::Random);
    }

    // 3. Initialize GStreamer
    let gstreamer_start = Instant::now();
    gstreamer::init()?;
    crate::video::configure_hw_decoders();
    crate::video::validate_selected_video_mode(crate::video::get_video_mode())?;
    let gstreamer_duration = gstreamer_start.elapsed();
    info!("GStreamer initialized.");

    // 4. Detect Backend and Run
    let use_x11 = std::env::var("WAYLAND_DISPLAY").is_err() && std::env::var("DISPLAY").is_ok();

    if use_x11 {
        info!("Starting X11 Backend...");
        x11_loop::run(config, log_level, gstreamer_duration).await
    } else {
        info!("Starting Wayland Backend...");
        wayland_loop::run(config, log_level, gstreamer_duration).await
    }
}
