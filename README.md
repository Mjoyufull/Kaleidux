<div align="center">

![Kaleidux Logo](./assets/kaleidux.png)

<i>(dynamic desktop kaleidoscope)</i>

<br><br>

[![License](https://img.shields.io/badge/license-AGPL--3.0-red.svg?style=flat-square)](https://github.com/Mjoyufull/Kaleidux/blob/main/LICENSE)
![written in Rust](https://img.shields.io/badge/language-rust-orange.svg?style=flat-square)
![platform](https://img.shields.io/badge/platform-linux-blue.svg?style=flat-square)

<br>
High-performance, hardware-accelerated wallpaper daemon for Linux.<br>
Supports Wayland & X11 with 50+ smooth GLSL transitions.
</div>

## Table of Contents

- [Quickstart](#quickstart)
- [Features](#features)
- [Installation](#installation)
- [Usage Breakdown](#usage-breakdown)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

**More Info:** [Detailed Usage & Advanced Config](./USAGE.MD)

## Quickstart

<img width="1920" height="1080" alt="Screenshot_20260118-203253" src="https://github.com/user-attachments/assets/2487daf1-5dbc-4a57-a7fe-d5d8f7148a77" />

Get up and running in 30 seconds:

```bash
# Install with Nix (recommended)
nix run github:Mjoyufull/Kaleidux

# Or build from source
git clone https://github.com/Mjoyufull/Kaleidux && cd Kaleidux
cargo build --release
sudo cp target/release/kaleidux-daemon /usr/local/bin/
sudo cp target/release/kldctl /usr/local/bin/

# Start the daemon
kaleidux-daemon &

# Skip to next wallpaper
kldctl next
```

## Features

- **Hardware-accelerated video**: Native zero-copy decoding on NVIDIA, AMD, and Intel GPUs.
- **Hardware-accelerated rendering**: Powered by `wgpu` for low CPU overhead during rendering and transitions.
- **Image support**: High-performance image loading and rendering.
- **50+ Transitions**: Huge library of GLSL transitions (fade, cube, doom, wipe, ripple, etc.).
- **Multi-Monitor**: Independent queue management for each output.
- **Monitor Behaviors**: `Independent`, `Synchronized`, or `Grouped` monitor support.
- **Rhai Scripting**: Automate your wallpaper logic with Rust-like scripts.
- **IPC Control**: Control the daemon via `kldctl` (next, prev, pause, status, etc.).

## Installation

### Option 1: Aur (Recommended)

- Installing from the Arch User Repository

```
$ yay -S kaleidux-git
# or
$ paru -S kaleidux-git
```

### Option 2: Nix Flake

- Build and run with Nix flakes:

  ```bash
  nix run github:Mjoyufull/Kaleidux
  ```

- Add to your `flake.nix` inputs:
  ```nix
  {
    inputs.kaleidux.url = "github:Mjoyufull/Kaleidux";
  }
  ```

### Option 3: Build from Source

**Build Requirements:**

- Rust 1.89+ **stable**
- GStreamer 1.20+ with dev plugins
- Wayland and/or X11 development headers

**Arch Linux Setup:**

```bash
sudo pacman -S gstreamer gst-plugins-base gst-plugins-good \
               gst-plugins-bad gst-libav wayland libx11 \
               vulkan-devel pkgconf cmake
```

**Build:**

```bash
git clone https://github.com/Mjoyufull/Kaleidux && cd Kaleidux
cargo build --release
```

## Usage Breakdown

### Daemon (`kaleidux-daemon`)

The core background service handling rendering and display interop.

```bash
Usage: kaleidux-daemon [OPTIONS]

Options:
      --demo              Run in demo mode (rotating built-in shaders)
      --log <LEVEL>       Log verbosity 1–4 (2=INFO); when set, also writes to ~/.config/kaleidux/logs/
      --video-mode <MODE> Force video decode path: auto, cpu, cuda, DMA-BUF, nv12, rgba
  -h, --help              Show help
```

### Controller (`kldctl`)

Swiss Army knife for interacting with the running daemon.

```text
kldctl
├── next [n]      Skip to the next wallpaper
├── prev [p]      Go back to the previous wallpaper
├── query [q]     List connected outputs and current state
├── love <PATH>   Increase selection frequency for a file
├── unlove <PATH> Reset frequency for a file
├── lovelist [ll] List all "loved" wallpapers
├── pause         Pause video playback
├── resume        Resume video playback
├── reload        Reload configuration from disk
├── kill          Stop the daemon gracefully
├── playlist      Manage content playlists
├── blacklist     Manage excluded files
└── history       Show recently played wallpapers
```

### Quick Usage Examples

```bash
# Love the current wallpaper on a specific monitor
kldctl love ~/wallpapers/nature.jpg

# List status of all monitors
kldctl query

# Sync all monitors to the next wallpaper
kldctl next --all
```

## Configuration

Default location: `~/.config/kaleidux/config.toml`

```toml
[global]
monitor-behavior = "independent"
sorting = "loveit"
video-ratio = 50

[any]
transition = { type = "cube", duration = 1000 }
```

See [USAGE.MD](./USAGE.MD) for full configuration reference.

Operational defaults to know:

- Console logging defaults to `WARN`; use `--log 1..4` for progressively more verbose daemon diagnostics.
- Exact output-name sections override regex sections, so `[DP-1]` wins over a matching `["re:.*"]`.
- Independent monitor mode applies a small deterministic phase offset to avoid synchronized image/video swaps across all outputs.
- `loveit` stats are LRU-bounded; loved entries stay weighted while retained in the stats cache and can age out when the cache exceeds capacity.

## Troubleshooting

- **Long Startup**: WGPU may wait for driver initialization on Wayland (~15s).
- **High CPU or blank video wallpaper**: The production path is still appsink/WGPU. On NVIDIA, use `--video-mode cuda` to force the CUDA zero-copy decode path when auto mode cannot infer it. An experimental libmpv backend can be built with `--features mpv-backend` and selected with `--video-backend mpv`; its default software target keeps final composition and transitions under WGPU but is meant for correctness testing, not CPU wins. `KLD_MPV_RENDER_API=gl-overlay` enables a separate native EGL/OpenGL overlay experiment only for diagnostics. That overlay is intentionally not the default because it bypasses WGPU wallpaper composition and shader transitions. Production mpv-level CPU requires GL/WGPU/DMA-BUF interop or a controlled FFmpeg/libav GPU-frame backend.
- **Video looks too choppy in low-power mode**: set `video-fps = "medium"` for 24 FPS publishing, `video-fps = "high"` for 48 FPS, or `video-fps = "unlimited"` to publish every decoded frame. The live FPS-tier benchmark contract is full video on every configured output: low/12 FPS under 4% CPU, medium/24 FPS under 8%, high/48 FPS under 13%, and unlimited/source-rate under 18%, with the same p95 budget per tier. The default `video-fps = "low"` has the most CPU headroom; the Wayland path uses minimal steady-video frame-callback damage by default. Set `KLD_VIDEO_FRAME_CALLBACK_DAMAGE=full` only when debugging compositor damage behavior.
- **Full-rate video CPU tuning**: appsink mailbox backpressure is enabled by default so a decoded sample is not converted again while the renderer already has an unconsumed frame. Finite FPS tiers also install a drop-only `videorate` filter before appsink so excess frames are discarded before callback/conversion work; set `KLD_VIDEO_RATE_FILTER=0` only when debugging that path. Set `KLD_APPSINK_DROP_IF_MAILBOX_PENDING=0` only for visual debugging; local one-video testing showed it raised CPU substantially. Uncapped appsink backpressure uses a slower `KLD_APPSINK_PENDING_REFRESH_MS` default than capped playback, and steady Wayland callback uploads use the same CPU-shield cadence by default; override with `KLD_VIDEO_CALLBACK_UPLOAD_INTERVAL_MS=0` for old full-callback behavior while debugging. Appsink remains the only production video path; the libmpv backend is experimental and still keeps final presentation under the WGPU renderer. Video players are stopped immediately when switching to images so stale decoders do not keep publishing during image prep; set `KLD_STOP_VIDEO_ON_IMAGE_SWITCH=legacy` only when debugging old crossfade behavior.
- **Shader Errors**: Ensure your GPU supports Vulkan or GLSL 450.

## Sub-5 Benchmark Harness

Use the in-tree benchmark harness to evaluate architecture changes against the sub-5% process CPU target:

```bash
bash tools/sub5/run_benchmark.sh three_video_mixed_res kaleidux-daemon-2026-04-26_13-26-27.log
```

Artifacts are written under `unattended_runs/<date>/sub5_<scenario>/` with machine-readable JSON and a short summary. Live matrix runs build `target/release/kaleidux-daemon` first by default so CPU gates do not accidentally test a stale binary; set `KLD_LIVE_MATRIX_SKIP_BUILD=1` only when intentionally reusing an existing release build. `tools/sub5/run_live_matrix.sh fps-tiers` pins `KLD_VIDEO_BACKEND=appsink` unless `KLD_LIVE_MATRIX_VIDEO_BACKEND` is explicitly set, so an experimental mpv shell environment cannot accidentally satisfy or poison the production CPU gate.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md) for guidelines.

## Credits

- [gSlapper](https://github.com/Nomadcxx/gSlapper)
- [wpaperd](https://github.com/danyspin97/wpaperd)
- [mpvpaper](https://github.com/GhostNaN/mpvpaper)
- [GStreamer](https://gstreamer.freedesktop.org/)
- [Clapper](https://github.com/Rafostar/clapper)
- [swww](https://github.com/Horus645/swww)

## License

Kaleidux is licensed under the AGPL-3.0 License.
