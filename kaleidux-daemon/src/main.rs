use chrono::Local;
use clap::Parser;
use kaleidux_common::Transition;
#[cfg(feature = "backend-appsink")]
use std::time::Instant;
use tracing::{error, info, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt as subscriber_fmt;
use tracing_subscriber::{EnvFilter, Registry, prelude::*};

// Jemalloc is opt-in; production defaults use the system allocator because
// the measured cold-start/RSS trade-off favored it on the reference system.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct CustomTimer;

impl tracing_subscriber::fmt::time::FormatTime for CustomTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Local::now();
        write!(w, "{}", now.format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

#[derive(Parser, Debug)]
#[command(author, about, long_about = None)]
struct Args {
    /// Diagnostic verbosity from 1 (warnings) through 5 (trace-all).
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=5))]
    log: Option<u8>,
    /// Run the built-in transition demo.
    #[arg(long)]
    demo: bool,
    /// Force video memory/decode mode: auto, cpu, cuda, dmabuf, nv12, or rgba.
    #[arg(long)]
    video_mode: Option<String>,
    /// Select backend: auto (FFmpeg, then mpv, then appsink), mpv, appsink, or ffmpeg.
    #[arg(long)]
    video_backend: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .max_blocking_threads(16)
        .thread_name("kaleidux-main")
        .build()?;
    let result = runtime.block_on(async_main());
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

async fn async_main() -> anyhow::Result<()> {
    let args = Args::parse();
    let log_level = args.log;
    kaleidux_daemon::observability::trace_all::set_trace_all_enabled(log_level == Some(5));
    let _guards = init_logging(log_level)?;

    if log_level == Some(5) {
        info!(
            "[TRACE5] trace-all diagnostics enabled; expect very high log volume and CPU overhead"
        );
    }

    if let Err(e) = kaleidux_daemon::shaders::ShaderManager::load_cache() {
        tracing::debug!("[SHADER] Could not load WGSL cache: {}", e);
    }

    apply_video_mode(args.video_mode.as_deref());
    apply_video_backend(args.video_backend.as_deref());
    let config = load_config(args.demo).await?;
    let gstreamer_duration = init_gstreamer()?;

    if selected_display_is_x11()? {
        info!("Starting X11 Backend...");
        #[cfg(feature = "display-x11")]
        {
            kaleidux_daemon::x11_loop::run(config, log_level, gstreamer_duration).await
        }
        #[cfg(not(feature = "display-x11"))]
        unreachable!("display selection rejects a disabled X11 backend");
    } else {
        info!("Starting Wayland Backend...");
        kaleidux_daemon::wayland_loop::run(config, log_level, gstreamer_duration).await
    }
}

fn init_logging(
    log_level: Option<u8>,
) -> anyhow::Result<(
    Option<tracing_appender::non_blocking::WorkerGuard>,
    Option<tracing_appender::non_blocking::WorkerGuard>,
)> {
    let filter = match log_level {
        Some(1) => LevelFilter::WARN,
        Some(2) => LevelFilter::INFO,
        Some(3) => LevelFilter::DEBUG,
        Some(4 | 5) => LevelFilter::TRACE,
        None => LevelFilter::WARN,
        Some(_) => LevelFilter::INFO,
    };

    let env_filter = EnvFilter::builder()
        .with_default_directive(filter.into())
        .from_env_lossy()
        .add_directive("wgpu_core=warn".parse().expect("valid wgpu_core filter"))
        .add_directive("wgpu_hal=warn".parse().expect("valid wgpu_hal filter"))
        .add_directive("naga=warn".parse().expect("valid naga filter"))
        .add_directive("calloop=warn".parse().expect("valid calloop filter"))
        .add_directive("tokio=warn".parse().expect("valid tokio filter"))
        .add_directive("polling=warn".parse().expect("valid polling filter"))
        .add_directive("notify=warn".parse().expect("valid notify filter"))
        .add_directive("mio=warn".parse().expect("valid mio filter"))
        .add_directive(
            "smithay_client_toolkit=warn"
                .parse()
                .expect("valid smithay_client_toolkit filter"),
        );

    if let Some(level) = log_level {
        init_file_logging(level, env_filter)
    } else {
        let stderr_layer = subscriber_fmt::layer()
            .with_writer(std::io::stderr)
            .with_timer(CustomTimer);
        Registry::default()
            .with(env_filter)
            .with(stderr_layer)
            .init();
        Ok((None, None))
    }
}

fn init_file_logging(
    level: u8,
    env_filter: EnvFilter,
) -> anyhow::Result<(
    Option<tracing_appender::non_blocking::WorkerGuard>,
    Option<tracing_appender::non_blocking::WorkerGuard>,
)> {
    let config_dir = dirs::config_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not find config directory"))?
        .join("kaleidux")
        .join("logs");
    std::fs::create_dir_all(&config_dir)?;

    let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S%.3f");
    // Fast supervised restart loops can launch more than once per second.
    // Never truncate a prior diagnostic capture if a name collision occurs.
    let (log_path, file) = (0_u8..=99)
        .find_map(|suffix| {
            let suffix = (suffix != 0).then(|| format!("-{suffix}"));
            let log_path = config_dir.join(format!(
                "kaleidux-daemon-{}-{}{}.log",
                timestamp,
                std::process::id(),
                suffix.as_deref().unwrap_or_default()
            ));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&log_path)
            {
                Ok(file) => Some(Ok((log_path, file))),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                Err(error) => Some(Err(error)),
            }
        })
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("could not allocate a unique daemon log filename"))?;
    println!("Logging to file: {}", log_path.display());
    let (non_blocking_file, file_guard) = tracing_appender::non_blocking(file);

    let file_layer = subscriber_fmt::layer()
        .with_writer(non_blocking_file)
        .with_ansi(false)
        .with_timer(CustomTimer);

    Registry::default().with(env_filter).with(file_layer).init();

    info!(
        "Kaleidux Daemon starting... (Level {}, File: {})",
        level,
        config_dir.display()
    );
    Ok((Some(file_guard), None))
}

fn apply_video_mode(video_mode: Option<&str>) {
    let Some(mode_str) = video_mode else {
        return;
    };

    let deprecated_cuda_alias = mode_str.eq_ignore_ascii_case("cuda-strict");
    let deprecated_dmabuf_alias = mode_str.eq_ignore_ascii_case("zero-copy");
    let mode = match mode_str.to_lowercase().as_str() {
        "cuda" | "nvdec" | "nvidia" | "cuda-strict" => {
            kaleidux_daemon::video::VideoMode::StrictCuda
        }
        "cpu" => kaleidux_daemon::video::VideoMode::ForceCpu,
        "dmabuf" | "dma-buf" | "zero-copy" => kaleidux_daemon::video::VideoMode::ForceDmaBuf,
        "nv12" => kaleidux_daemon::video::VideoMode::ForceNv12,
        "rgba" => kaleidux_daemon::video::VideoMode::ForceRgba,
        "auto" => kaleidux_daemon::video::VideoMode::Auto,
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
    kaleidux_daemon::video::set_video_mode(mode);
}

fn apply_video_backend(video_backend: Option<&str>) {
    let Some(backend_str) = video_backend else {
        if let Ok(value) = std::env::var("KLD_VIDEO_BACKEND") {
            apply_video_backend(Some(value.as_str()));
        }
        return;
    };

    let legacy_mpv_alias = backend_str.eq_ignore_ascii_case("mpv-experimental");
    let legacy_ffmpeg_alias = matches!(
        backend_str.to_lowercase().as_str(),
        "native" | "native-experimental" | "ffmpeg-native"
    );
    let request = match backend_str.to_lowercase().as_str() {
        "auto" => kaleidux_daemon::video::VideoBackendRequest::Auto,
        "appsink" | "gst" | "gstreamer" => {
            kaleidux_daemon::video::VideoBackendRequest::ForceAppsink
        }
        "mpv" | "libmpv" | "mpv-experimental" => {
            kaleidux_daemon::video::VideoBackendRequest::ForceMpv
        }
        "ffmpeg" | "libav" | "native" | "native-experimental" | "ffmpeg-native" => {
            kaleidux_daemon::video::VideoBackendRequest::ForceFfmpeg
        }
        other => {
            let msg = format!(
                "ERROR: Unknown --video-backend '{}', valid: auto, appsink, mpv, ffmpeg",
                other
            );
            eprintln!("{}", msg);
            error!("{}", msg);
            std::process::exit(1);
        }
    };
    if request != kaleidux_daemon::video::VideoBackendRequest::Auto
        && !kaleidux_daemon::video::video_backend_is_enabled(request)
    {
        let msg = format!(
            "ERROR: --video-backend '{}' is disabled in this build; rebuild with --features {} (enabled backends: {:?})",
            backend_str,
            kaleidux_daemon::video::video_backend_feature(request),
            kaleidux_daemon::video::enabled_video_backend_labels(),
        );
        eprintln!("{msg}");
        error!("{msg}");
        std::process::exit(2);
    }
    if legacy_ffmpeg_alias {
        warn!(
            "[VIDEO] --video-backend {} is deprecated; the FFmpeg backend is now selected with --video-backend ffmpeg",
            backend_str
        );
    }
    if legacy_mpv_alias {
        warn!(
            "[VIDEO] --video-backend mpv-experimental is deprecated; use --video-backend mpv to force the compatibility backend"
        );
    }
    kaleidux_daemon::video::set_video_backend_request(request);
}

async fn load_config(demo: bool) -> anyhow::Result<kaleidux_daemon::orchestration::Config> {
    let mut config = match kaleidux_daemon::orchestration::Config::load().await {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("Failed to load configuration: {}. Using defaults.", e);
            kaleidux_daemon::orchestration::Config::default()
        }
    };

    if demo {
        info!("Demo mode enabled! Overriding configuration to use current directory...");
        let current_dir = std::env::current_dir()?;
        config.any.path = Some(current_dir);
        config.any.duration = Some(std::time::Duration::from_secs(10));
        config.global.video_ratio = Some(100);
        config.any.transition_time = Some(1500);
        config.any.transition = Some(Transition::Random);
    }

    Ok(config)
}

fn init_gstreamer() -> anyhow::Result<std::time::Duration> {
    #[cfg(not(feature = "backend-appsink"))]
    {
        info!("GStreamer appsink backend is disabled; skipping initialization.");
        return Ok(std::time::Duration::ZERO);
    }

    #[cfg(feature = "backend-appsink")]
    {
        let gstreamer_start = Instant::now();
        gstreamer::init()?;
        if kaleidux_daemon::observability::trace_all::trace_all_enabled() {
            gstreamer::log::set_active(true);
            gstreamer::log::set_default_threshold(gstreamer::DebugLevel::Trace);
            info!("[TRACE5] GStreamer debug threshold set to TRACE");
        }
        kaleidux_daemon::video::configure_hw_decoders();
        kaleidux_daemon::video::validate_selected_video_mode(
            kaleidux_daemon::video::get_video_mode(),
        )?;
        let gstreamer_duration = gstreamer_start.elapsed();
        info!("GStreamer initialized.");
        Ok(gstreamer_duration)
    }
}

fn selected_display_is_x11() -> anyhow::Result<bool> {
    let x11_requested =
        std::env::var("WAYLAND_DISPLAY").is_err() && std::env::var("DISPLAY").is_ok();
    if x11_requested {
        anyhow::ensure!(
            cfg!(feature = "display-x11"),
            "X11 is the active display but this build disabled it; rebuild with --features display-x11 or use the full build"
        );
        return Ok(true);
    }
    anyhow::ensure!(
        cfg!(feature = "display-wayland"),
        "Wayland is the active display but this build disabled it; rebuild with --features display-wayland or use the full build"
    );
    Ok(false)
}
