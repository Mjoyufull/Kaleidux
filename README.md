# Kaleidux

**Kaleidux** is a high-performance, dynamic wallpaper daemon for Linux, supporting both **Wayland** (via Layer-Shell) and **X11**. It transforms your desktop with smooth, hardware-accelerated transitions between images and videos.

Version: `0.0.1-kneecap`

![License](https://img.shields.io/badge/license-AGPL--3.0-red)
![Language](https://img.shields.io/badge/language-Rust-orange)

## Features

- **Video Support**: Seamlessly loop videos as wallpapers using GStreamer.
- **Image Support**: High-quality image rendering and transitions.
- **Hardware Accelerated**: Powered by `WGPU` for near-zero CPU overhead during transitions.
- **50+ Transitions**: Huge library of GLSL transitions (fade, cube, doom, wipe, ripple, etc.).
- **Multi-Monitor**: Independent queue management for each output.
- **Monitor Behaviors**: `Independent`, `Synchronized`, or `Grouped` monitor support.
- **Rhai Scripting**: Automate your wallpaper logic with Rust-like scripts.
- **IPC Control**: Control the daemon via `kldctl` (next, prev, pause, status, etc.).

## Quick Start

### Installation (Nix)

If you have Flakes enabled:

```bash
nix run github:Mjoyufull/Kaleidux
```

### Installation (Source)

Ensure you have the required GStreamer and graphics dependencies installed:

```bash
# Arch Linux
sudo pacman -S gstreamer gst-plugins-base gst-plugins-good \
               gst-plugins-bad gst-libav wayland libx11 \
               vulkan-devel pkgconf cmake
```

Build with Cargo:

```bash
cargo build --release
```

### Usage

1. **Start the daemon**:

   ```bash
   ./target/release/kaleidux-daemon
   ```

2. **Control with `kldctl`**:

   ```bash
   kldctl next    # Skip to next wallpaper
   kldctl status  # Show what's playing
   kldctl query   # List connected monitors
   ```

3. **Configure**:
   The default config is at `~/.config/kaleidux/config.toml`. See [USAGE.MD](./USAGE.MD) for details.

## Components

- **kaleidux-daemon**: The core background service handling rendering and Wayland/X11 interop.
- **kldctl**: Command-line utility to interact with the running daemon.
- **kaleidux-common**: Shared types and IPC protocol logic.

## Documentation

- [Advanced Usage & Configuration (USAGE.MD)](./USAGE.MD)
- [Example Configuration (config.example.toml)](./config.example.toml)

## License

Kaleidux is licensed under the AGPL-3.0 License.
