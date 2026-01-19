<div align="center">
![Logo](./assets/kaleidux.png)
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

- **Video Support**: Seamlessly loop videos as wallpapers using GStreamer.
- **Image Support**: High-quality image rendering and transitions.
- **Hardware Accelerated**: Powered by `WGPU` for near-zero CPU overhead during transitions.
- **50+ Transitions**: Huge library of GLSL transitions (fade, cube, doom, wipe, ripple, etc.).
- **Multi-Monitor**: Independent queue management for each output.
- **Monitor Behaviors**: `Independent`, `Synchronized`, or `Grouped` monitor support.
- **Rhai Scripting**: Automate your wallpaper logic with Rust-like scripts.
- **IPC Control**: Control the daemon via `kldctl` (next, prev, pause, status, etc.).

## Installation

### Option 1: Nix Flake (Recommended)

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

### Option 2: Build from Source

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
      --demo       Run in demo mode (rotating built-in shaders)
      --log <PATH> Specify log file path
  -h, --help       Show help
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

## Troubleshooting

- **Long Startup**: WGPU may wait for driver initialization on Wayland (~15s).
- **High CPU**: Video frames currently use a CPU-RGBA roundtrip. Zero-copy is planned.
- **Shader Errors**: Ensure your GPU supports Vulkan or GLSL 450.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md) for guidelines.

## License

Kaleidux is licensed under the AGPL-3.0 License.
