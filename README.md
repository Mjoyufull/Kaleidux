<div align="center">

![Kaleidux Logo](./assets/kaleidux.png)

<i>(dynamic desktop kaleidoscope)</i>

<br><br>

[![License](https://img.shields.io/badge/license-AGPL--3.0-red.svg?style=flat-square)](https://github.com/Mjoyufull/Kaleidux/blob/main/LICENSE)
![written in Rust](https://img.shields.io/badge/language-rust-orange.svg?style=flat-square)
![platform](https://img.shields.io/badge/platform-linux-blue.svg?style=flat-square)

<br>
A hardware-accelerated wallpaper and kaleidoscope daemon for Linux.<br>
Supports Wayland & X11, 50+ GLSL transitions, and FFmpeg, GStreamer, and libmpv backends.
</div>

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Usage Breakdown](#usage-breakdown)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

**More Info:** [Detailed Usage & Advanced Config](./USAGE.md)

<img width="1920" height="1080" alt="Screenshot_20260118-203253" src="https://github.com/user-attachments/assets/2487daf1-5dbc-4a57-a7fe-d5d8f7148a77" />

## Features

- **Hardware-accelerated video**: Native zero-copy decoding on NVIDIA, AMD, and Intel GPUs.
- **Hardware-accelerated rendering**: Powered by `wgpu` for low CPU overhead during rendering and transitions.
- **Image support**: High-performance image loading and rendering.
- **50+ Transitions**: GLSL transitions from [GL Transitions](https://gl-transitions.com/), including fade, cube, doom, wipe, and ripple.
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
- GStreamer 1.24+ with development headers and plugins
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
The default uses the system allocator; `jemalloc` is an optional feature.
Nix exposes the
same variants as `.#wayland-only`, `.#minimal-static`, `.#appsink`, `.#mpv`,
and `.#ffmpeg`. Selecting a backend omitted at build time exits immediately
with the Cargo feature required to enable it.

## Quickstart

After installation, copy [config.example.toml](./config.example.toml) to
`~/.config/kaleidux/config.toml` and set your wallpaper directory.
For a source build, the binaries are in `target/release/`.

```bash
kaleidux-daemon &
kldctl next
kldctl prev
kldctl query
```

Video backend `auto` tries FFmpeg first, then mpv, then appsink.
Videos play at their source frame rate by default, including when the desktop
is idle. Fullscreen pausing is opt-in.

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
transition = { type = "cube" }
transition-time = 1000
```
See [config.example.toml](./config.example.toml) for configuration options and
[transitions_examples.toml](./transitions_examples.toml) for transition parameters.
[USAGE.md](./USAGE.md) covers playback, monitor matching, caching, and diagnostics.

## Troubleshooting

- **Long Startup**: WGPU may wait for driver initialization on Wayland (~15s).
- **Blank video wallpaper**: start with `--video-backend auto`; use `--log 3` to capture diagnostics. See [backend troubleshooting](./USAGE.md#diagnostics).
- **Choppy video**: check `video-fps` in your config; `"unlimited"` follows the source frame rate.
- **Shader Errors**: Ensure your GPU supports Vulkan or GLSL 450.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and [PROJECT_STANDARDS.md](./PROJECT_STANDARDS.md) for guidelines.

## Credits

- [gSlapper](https://github.com/Nomadcxx/gSlapper)
- [wpaperd](https://github.com/danyspin97/wpaperd)
- [mpvpaper](https://github.com/GhostNaN/mpvpaper)
- [GStreamer](https://gstreamer.freedesktop.org/)
- [Clapper](https://github.com/Rafostar/clapper)
- [swww](https://github.com/Horus645/swww)
- [Yin](https://github.com/saverinonrails/yin), a useful modern comparison

## License

Kaleidux is licensed under the [AGPL-3.0 License](./LICENSE).
