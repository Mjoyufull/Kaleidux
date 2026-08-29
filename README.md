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

- Reduced packages are also available as `.#wayland-only`, `.#ffmpeg`,
  `.#mpv`, `.#appsink`, and `.#minimal-static`. On non-NixOS NVIDIA systems,
  run the Nix package through nixGL; the wrapper intentionally does not add
  host `/usr/lib` to `LD_LIBRARY_PATH`, because mixing the host and Nix glibc
  makes the executable unsafe and can fail at startup.

### Option 3: Build from Source

**Build Requirements:**

- Rust 1.89+ **stable**
- mpv/libmpv with development headers
- GStreamer 1.20+ with dev plugins
- Wayland and/or X11 development headers

**Arch Linux Setup:**

```bash
sudo pacman -S mpv gstreamer gst-plugins-base gst-plugins-good \
               gst-plugins-bad gst-libav wayland libx11 \
               vulkan-devel pkgconf cmake
```

**Build:**

```bash
git clone https://github.com/Mjoyufull/Kaleidux && cd Kaleidux
cargo build --release
```

The default release contains all three video backends and both display paths.
Smaller supported builds use explicit feature bundles:

```bash
# All video backends, Wayland only
cargo build --release -p kaleidux-daemon --no-default-features --features wayland-only

# Static images on Wayland; no video backend
cargo build --release -p kaleidux-daemon --no-default-features --features minimal-static

# One video backend on Wayland
cargo build --release -p kaleidux-daemon --no-default-features \
  --features display-wayland,backend-ffmpeg
```

Individual backend features are `backend-appsink`, `backend-mpv`, and
`backend-ffmpeg`; display features are `display-wayland` and `display-x11`.
The optional `jemalloc` feature is available for allocator experiments, but
the measured production default uses the system allocator. Nix exposes the
same variants as `.#wayland-only`, `.#minimal-static`, `.#appsink`, `.#mpv`,
and `.#ffmpeg`. Selecting a backend omitted at build time exits immediately
with the Cargo feature required to enable it.

## Usage Breakdown

### Daemon (`kaleidux-daemon`)

The core background service handling rendering and display interop.

```bash
Usage: kaleidux-daemon [OPTIONS]

Options:
      --demo              Run in demo mode (rotating built-in shaders)
      --log <LEVEL>       Log verbosity 1–4 (2=INFO); when set, also writes to ~/.config/kaleidux/logs/
      --video-mode <MODE> Force video decode path: auto, cpu, cuda, DMA-BUF, nv12, rgba
      --video-backend <BACKEND>
                          Force backend: auto, ffmpeg, mpv, appsink (default: auto)
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
pause-on-fullscreen = false

[any]
transition = { type = "cube", duration = 1000 }
```

See [USAGE.MD](./USAGE.MD) for full configuration reference.

Operational defaults to know:

- Console logging defaults to `WARN`; use `--log 1..4` for progressively more verbose daemon diagnostics.
- `video-fps = "unlimited"` is the production default and follows the source video's cadence; finite FPS modes are compatibility and smoke-test profiles.
- Backend `auto` uses the measured ladder FFmpeg → mpv → appsink. The native FFmpeg path hardware-decodes with libavcodec/VA-API and uses direct or persistently bridged DRM-PRIME presentation on Wayland. MPV retains its OpenGL/Vulkan shared composition path, and appsink remains the broad GStreamer fallback.
- `pause-on-fullscreen = false` keeps Wayland video source-driven so it advances even when the desktop is otherwise idle. Opting in makes steady video wait for compositor frame callbacks and hold the current frame while the wallpaper is occluded; it has no effect on X11.
- The default build includes every backend; reduced packages can omit backends with the Cargo features documented under Installation. Forcing a backend disables automatic demotion and makes failures actionable.
- Exact output-name sections override regex sections, so `[DP-1]` wins over a matching `["re:.*"]`.
- Independent monitor mode applies a small deterministic phase offset to avoid synchronized image/video swaps across all outputs.
- `loveit` stats are LRU-bounded; loved entries stay weighted while retained in the stats cache and can age out when the cache exceeds capacity.
- Prepared-image disk cache defaults are 2 GiB, 4096 entries, 180 days, and a 2 GiB free-space floor. Override them with `KALEIDUX_IMAGE_CACHE_MAX_MIB`, `KALEIDUX_IMAGE_CACHE_MAX_ENTRIES`, `KALEIDUX_IMAGE_CACHE_MAX_AGE_DAYS`, and `KALEIDUX_IMAGE_CACHE_MIN_FREE_MIB`; zero disables the corresponding limit, while `KALEIDUX_IMAGE_CACHE_UNLIMITED=1` is the explicit compatibility mode. `KALEIDUX_IMAGE_CACHE_FSYNC=1` trades write latency for payload durability before atomic rename.
- Decoded images waiting for renderer upload have both a 16-message limit and a 256 MiB weighted byte budget. `KALEIDUX_IMAGE_CHANNEL_MAX_MIB` changes the byte budget; a single image can temporarily consume the whole budget but cannot make the queue unbounded.

## Troubleshooting

- **Long Startup**: WGPU may wait for driver initialization on Wayland (~15s).
- **High CPU or blank video wallpaper**: leave the backend on `auto` first so Kaleidux can demote through FFmpeg, mpv, and appsink when a driver or codec path is unavailable. Force one backend only for diagnosis. `KLD_NATIVE_GL_SURFACE=1` enables the experimental native EGL subsurface path; the reliable composed path is the default because some Wayland compositors stop repainting that subsurface while the desktop is otherwise idle. `KLD_MPV_RENDER_API=gl-overlay` is likewise diagnostic because it bypasses WGPU wallpaper composition and shader transitions.
- **Video looks too choppy in low-power mode**: the production default is `video-fps = "unlimited"`, which publishes at the video's source cadence. `low`, `medium`, and `high` remain finite-rate smoke profiles. The Wayland path follows backend frame demand rather than a fixed capture poll.
- **Full-rate video CPU tuning**: the production path avoids a per-frame CPU readback, CPU pixel conversion, and intermediate GPU blit. Three shared slots are preallocated, their views and bind groups are reused, and steady uniform-buffer writes are suppressed. `KLD_MPV_GL_SYNC=finish` is a slow diagnostic synchronization mode; production uses external semaphores. Video players are stopped immediately when switching to images so stale decoders do not keep publishing during image preparation.
- **Shader Errors**: Ensure your GPU supports Vulkan or GLSL 450.

## Sub-5 Benchmark Harness

Use the in-tree benchmark harness to evaluate architecture changes against the sub-5% process CPU target:

```bash
bash tools/sub5/run_benchmark.sh three_video_mixed_res kaleidux-daemon-2026-04-26_13-26-27.log
```

Artifacts are written under `unattended_runs/<date>/sub5_<scenario>/` with machine-readable JSON and a short summary. Live matrix runs build `target/release/kaleidux-daemon` first by default so CPU gates do not accidentally test a stale binary; set `KLD_LIVE_MATRIX_SKIP_BUILD=1` only when intentionally reusing an existing release build.

The production CPU gate is measured with `video-fps = "unlimited"`; finite FPS
modes are compatibility smoke profiles rather than substitutes for source-rate
performance.

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
