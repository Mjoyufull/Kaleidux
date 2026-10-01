# Using Kaleidux

Operational guide and reference for the Kaleidux dynamic wallpaper daemon and `kldctl` control utility.

Start with [config.example.toml](./config.example.toml). The annotated
[transition examples](./transitions_examples.toml) provide copyable effect settings;
the tables below explain their defaults and behavior.

## Table of Contents

- [Quick Start](#quick-start)
  - [Starting the Daemon](#starting-the-daemon)
  - [Basic Control](#basic-control)
  - [Weighting and Favorites](#weighting-and-favorites)
- [Daemon Invocation and CLI Flags](#daemon-invocation-and-cli-flags)
  - [Syntax](#syntax)
  - [Command-Line Options](#command-line-options)
  - [Backend Selection Rules and Constraints](#backend-selection-rules-and-constraints)
  - [Display Backend Selection](#display-backend-selection)
  - [Socket Resolution](#socket-resolution)
- [Client Control Utility (`kldctl`)](#client-control-utility-kldctl)
  - [Global Options](#global-options)
  - [Commands](#commands)
- [Configuration Reference (`config.toml`)](#configuration-reference-configtoml)
  - [Path Resolution and Expansion](#path-resolution-and-expansion)
  - [Precedence and Resolution Hierarchy](#precedence-and-resolution-hierarchy)
  - [Monitor Behaviors](#monitor-behaviors)
  - [`[global]` Settings Reference](#global-settings-reference)
  - [Output Settings Reference](#output-settings-reference)
  - [Complete Configuration Example](#complete-configuration-example)
- [Transitions Reference](#transitions-reference)
  - [Syntax Styles](#syntax-styles)
  - [Parameterless Transitions](#parameterless-transitions)
  - [Parametric Transitions](#parametric-transitions)
  - [Custom GLSL Shaders](#custom-glsl-shaders)
- [Video and Audio Playback](#video-and-audio-playback)
  - [Backend Comparison](#backend-comparison)
  - [Audio Mechanics and Volume Control](#audio-mechanics-and-volume-control)
  - [Frame Rates and Occlusion Pacing](#frame-rates-and-occlusion-pacing)
- [History, Sorting, and Content Selection](#history-sorting-and-content-selection)
  - [History Navigation](#history-navigation)
  - [Sorting Strategies](#sorting-strategies)
- [Automation and Rhai Scripting](#automation-and-rhai-scripting)
  - [Configuration](#configuration)
  - [Lifecycle Hooks and State Management](#lifecycle-hooks-and-state-management)
  - [Registered API Functions](#registered-api-functions)
  - [Execution Semantics and Limits](#execution-semantics-and-limits)
  - [Script Example](#script-example)
  - [Gaming, Sleep, and Battery Hooks](#gaming-sleep-and-battery-hooks)
  - [Desktop Environment Automation](#desktop-environment-automation)
- [Environment Variables Reference](#environment-variables-reference)
  - [Image Cache and Memory Knobs](#image-cache-and-memory-knobs)
  - [Video Backend and Presentation Knobs](#video-backend-and-presentation-knobs)
  - [GStreamer Appsink Knobs](#gstreamer-appsink-knobs)
  - [CUDA and Zero-Copy Knobs](#cuda-and-zero-copy-knobs)
  - [libmpv Backend Knobs](#libmpv-backend-knobs)
  - [Native (FFmpeg) Backend Knobs](#native-ffmpeg-backend-knobs)
  - [Power, Desktop, and Tracing Knobs](#power-desktop-and-tracing-knobs)
- [Troubleshooting and Diagnostics](#troubleshooting-and-diagnostics)
  - [Common Issues and Solutions](#common-issues-and-solutions)
  - [Performance Analysis with `kldctl perf-snapshot`](#performance-analysis-with-kldctl-perf-snapshot)

## Quick Start

### Starting the Daemon

```sh
# Start daemon with default configuration (/home/your-user/.config/kaleidux/config.toml)
kaleidux-daemon

# Start daemon with warning-level console and rotating file logging
kaleidux-daemon --log 1

# Start daemon in transition demo mode (cycles current directory with 10s intervals)
kaleidux-daemon --demo
```

### Basic Control

```sh
# Show connected outputs, resolutions, and active wallpapers
kldctl query

# Advance to next wallpaper on all monitors
kldctl next

# Advance to next wallpaper on a specific monitor
kldctl next -o DP-1

# Step backward to previous wallpaper from history
kldctl prev
kldctl prev -o DP-1

# Pause video playback and freeze wallpaper cycling timer
kldctl pause

# Resume video playback and wallpaper cycling timer
kldctl resume

# Stop active video players and clear sessions
kldctl stop

# Clear wallpaper on DP-1 to a black screen
kldctl clear -o DP-1

# Clear wallpaper on all outputs
kldctl clear

# Validate config.toml syntax without connecting to the daemon
# Note: check-config parses TOML syntax only; it does not validate schema or types
kldctl check-config

# Reload output configuration from disk without restarting daemon
# Note: reload updates monitor and output settings; it does not reload scripts
kldctl reload

# Shut down the daemon gracefully
kldctl kill
```

### Weighting and Favorites

```sh
# Mark a wallpaper as loved (default multiplier: 2.0x selection probability)
kldctl love /home/your-user/Pictures/Wallpapers/fav.jpg

# Set custom multiplier (3.5x more likely to appear under loveit sorting)
kldctl love /home/your-user/Pictures/Wallpapers/fav.jpg -m 3.5

# Reset wallpaper to standard weight (multiplier 1.0)
kldctl unlove /home/your-user/Pictures/Wallpapers/fav.jpg

# Print table of loved wallpapers with weights and pick counts
kldctl lovelist
```

## Daemon Invocation and CLI Flags

### Syntax

```sh
kaleidux-daemon [OPTIONS]
```

### Command-Line Options

| Option | Values | Default | Description |
|---|---|---|---|
| `--log` | `1`, `2`, `3`, `4`, `5` | Unset (`WARN` to stderr) | Diagnostic verbosity and file logging. Level 1 enables `WARN` plus rotating file logs under `~/.config/kaleidux/logs/`. Level 2 adds `INFO`, level 3 adds `DEBUG`, level 4 adds `TRACE`, and level 5 enables `TRACE-ALL` diagnostics (forces GStreamer trace logging and 1 ms idle polling). |
| `--demo` | Flag | `false` | Built-in transition demo. Overrides `[any].path` to current working directory, sets duration to 10 seconds, sets video ratio to 100%, sets transition duration to 1500 ms, and selects `random` transitions. |
| `--video-mode` | `auto`, `cpu`, `cuda`, `dmabuf`, `nv12`, `rgba` | `auto` | Force video memory and decode presentation path for the GStreamer appsink backend. Legacy aliases `cuda-strict` (maps to `cuda`) and `zero-copy` (maps to `dmabuf`) are accepted with a deprecation warning. |
| `--video-backend` | `auto`, `ffmpeg`, `mpv`, `appsink` | `auto` | Select video decoding backend. Aliases: `gst`/`gstreamer` for `appsink`; `libmpv`/`mpv-experimental` for `mpv`; `native`/`native-experimental`/`ffmpeg-native`/`libav` for `ffmpeg`. |

### Backend Selection Rules and Constraints

- `--video-backend auto` probes backends in priority order: native FFmpeg first, then libmpv, then GStreamer appsink when backend initialization fails.
- Forcing `ffmpeg`, `mpv`, or `appsink` disables fallback: if the selected backend fails to initialize, playback fails immediately and logs the error.
- If a forced backend was disabled at compile time (`backend-ffmpeg`, `backend-mpv`, `backend-appsink`), the daemon terminates with exit code 2 and reports the Cargo feature needed to enable it.
- Explicit `--video-mode` settings (`cpu`, `cuda`, `dmabuf`, `nv12`, `rgba`) apply strictly to the appsink backend. Combining an explicit video mode with `--video-backend ffmpeg` or `--video-backend mpv` causes the daemon to exit with code 1.

### Display Backend Selection

Kaleidux inspects environment variables on startup to choose the window system backend:
1. If `WAYLAND_DISPLAY` is unset and `DISPLAY` is set: selects X11 (requires compile-time `display-x11` feature).
2. Otherwise: selects Wayland via `wlr-layer-shell` (requires compile-time `display-wayland` feature).

If the selected display protocol was not compiled into the binary, the daemon aborts with an error indicating the missing feature.

### Socket Resolution

The daemon binds its IPC Unix domain socket in this order:
1. `$XDG_RUNTIME_DIR/kaleidux.sock` if `XDG_RUNTIME_DIR` is set.
2. `/tmp/kaleidux-${USER}.sock` (falling back to `/tmp/kaleidux-kaleidux.sock` if `USER` is unset).

## Client Control Utility (`kldctl`)

### Global Options

```sh
# Specify explicit daemon socket path
kldctl -s /run/user/1000/kaleidux.sock <COMMAND>
kldctl --socket /tmp/kaleidux-your-user.sock <COMMAND>
```

### Commands

#### Output Status and Query

```sh
# Query active outputs, geometry, and current wallpaper file
kldctl query
kldctl status
kldctl q
kldctl st
```

Example output:
```text
Output     | Size       | Current Wallpaper
--------------------------------------------------------
DP-1       | 2560x1440  | /home/your-user/Pictures/mountain.jpg
HDMI-A-1   | 1920x1080  | /home/your-user/Videos/rain.mp4
```

#### Queue Navigation

```sh
# Advance wallpaper on all monitors
kldctl next
kldctl n

# Advance wallpaper on target monitor only
kldctl next -o DP-1
kldctl next --output HDMI-A-1

# Step back to previous wallpaper from queue history
kldctl prev
kldctl p
kldctl prev -o DP-1
```

`kldctl next` follows forward history if you previously stepped back using `kldctl prev` before picking new content from the pool.

#### Playback Control

```sh
# Pause video wallpaper playback and freeze wallpaper cycling timer
kldctl pause

# Resume video wallpaper playback and unfreeze rotation timer
kldctl resume

# Stop active video players and clear active playback sessions
kldctl stop

# Clear wallpaper to a solid black screen on specific output
kldctl clear -o DP-1

# Clear wallpaper to black on all outputs
kldctl clear
```

When `kldctl pause` runs, video decoders pause and the monitor scheduling loop suspends wallpaper switching timers. `kldctl resume` unfreezes the rotation timer, resumes video decoders, and submits a frame request credit to restart display presentation. If compositor outputs are powered down via DPMS, video resume is deferred until outputs power back on.

#### Playlist Management

Playlists define isolated lists of files that can be loaded into output queues.

```sh
# Create a new playlist
kldctl playlist create nature

# Add files to playlist
kldctl playlist add nature /home/your-user/Pictures/forest.jpg
kldctl playlist add nature /home/your-user/Pictures/river.jpg

# List all playlists
kldctl playlist list

# Activate playlist across all queues
kldctl playlist load nature

# Unload playlist and return to standard directory pool discovery
kldctl playlist load

# Remove a file from playlist
kldctl playlist remove nature /home/your-user/Pictures/river.jpg

# Delete a playlist
kldctl playlist delete nature
```

#### Blacklist Management

Blacklisted files are excluded from directory rotation pools and active playlists.

```sh
# Add a file to blacklist
kldctl blacklist add /home/your-user/Pictures/blurry.jpg

# List all blacklisted files
kldctl blacklist list

# Remove file from blacklist
kldctl blacklist remove /home/your-user/Pictures/blurry.jpg
```

#### History Inspection

```sh
# Show recent wallpaper history (most recent last, up to 50 entries)
kldctl history

# Show history for specific output
kldctl history -o DP-1
```

#### Performance Snapshot

```sh
# Print performance counters and subsystem timings from the running daemon
kldctl perf-snapshot
```

The daemon outputs key-value metrics partitioned across subsystem categories:

- `uptime_s`: Elapsed daemon runtime in seconds.
- `memory`: Memory statistics including resident set size (`rss`) and process high-water mark (`peak`).
- `component_cpu`: Rolling average CPU time in milliseconds spent in `renderer`, `video`, `image`, `file_disc`, and `shader` compilation.
- `video_upload`: Subsystem timings for GPU texture uploads via CUDA (`cuda_total`, `map`, `copy`, `sync`, `convert_submit`).
- `video`: Frame counters tracking incoming decodes (`recv`), GPU uploads (`upload`), presented frames (`present`), and frames skipped due to stale timestamps (`stale`).
- `video_backend`: Backend event metrics for decode attempts, published frames, errors, and timeline synchronization block durations (`native_sync_blocked_ns`).
- `present`: Presentation path counters (direct DMA-BUF subsurface, WGPU texture, OpenGL surface) and Wayland frame callback damage counts (`callback_full_damage`, `callback_minimal_damage`).
- `image_cache`: Image memory cache statistics tracking hits, misses, entries, and resident bytes.
- `channel_high_water`: High-water mark for internal actor message queues (`cmd`, `img`, `player_ready`, `player_event`, `mailbox`).
- `background`: Concurrency level of background worker threads (`idle` and `active`).
- `wake`: Main loop wakeup reason counters (`timer`, `fd`, `signal`) and sleep durations.
- `monitor_self_cost`: Scheduling loop overhead for output refresh, GPU surface updates, and metric logging.
- `thread_cpu`: CPU utilization percentages for the `main` loop and `worker` threads.

#### Configuration and Daemon Management

```sh
# Validate syntax of config.toml without daemon connection
# Note: check-config parses toml::Value only; it does not validate schema, keys, or types
kldctl check-config
kldctl cc

# Reload configuration from disk and update running renderers
# Note: reload updates monitor and output settings; it does not reload Rhai scripts
kldctl reload

# Shut down the daemon gracefully
kldctl kill
```

## Configuration Reference (`config.toml`)

Configuration resides at `/home/your-user/.config/kaleidux/config.toml`.

### Path Resolution and Expansion

Kaleidux reads configuration paths without shell tilde (`~`) expansion. The TOML deserializer treats values literally. Paths such as `~/Pictures/Wallpapers` or `~/.config/kaleidux/script.rhai` are evaluated relative to the current working directory of the daemon process, which typically fails when launched via systemd or from other directories.

Always specify absolute filesystem paths (e.g., `/home/your-user/Pictures/Wallpapers`) for directory paths (`path`) and script locations (`script-path`).

### Precedence and Resolution Hierarchy

When an output requires configuration parameters, Kaleidux resolves settings through four layers:

1. Built-in Defaults: Hardcoded system fallback values.
2. `[global]` Table: Base settings for monitor behavior, scripting, volume, sorting, and video rates.
3. `[any]` Table: Wildcard defaults for output paths, durations, transitions, and layers.
4. Per-Output Sections:
   - Exact Output Match (`[<output-name>]`): Sections matching output name (e.g., `[DP-1]`, `[HDMI-A-1]`). Exact names take highest precedence.
   - Regex Output Match (`["re:<pattern>"]`): Sections matched against output descriptions (e.g., `["re:Dell.*"]`). Evaluated in alphabetical order of their TOML section keys.
   - If an exact output name matches, regex matches are ignored.

### Monitor Behaviors

The `monitor-behavior` setting in `[global]` dictates queue organization:

```toml
[global]
# Independent queues per monitor (default)
monitor-behavior = "independent"

# Synchronized: all monitors share one queue and display identical content
# monitor-behavior = "synchronized"

# Grouped: specific monitors share queues
# monitor-behavior = { grouped = [["DP-1", "DP-2"], ["HDMI-A-1"]] }
```

- `independent`: Each output scans its directory, maintains its own 50-item history, and switches on an independent timer with small staggered scheduling offsets to prevent concurrent image decoding.
- `synchronized`: All outputs share one queue, one selection history, and one switch timer. Content switches simultaneously across all outputs.
- `grouped`: Outputs in each subgroup share a queue, history, and switch timer, while separate groups cycle independently.

### `[global]` Settings Reference

| Key | Type | Default | Description |
|---|---|---|---|
| `monitor-behavior` | String or Table | `"independent"` | Output queue coordination: `"independent"`, `"synchronized"`, or `{ grouped = [["DP-1", "DP-2"]] }`. |
| `video-ratio` | Integer `0..=100` | `50` | Default probability percentage of selecting video over image when both exist in the directory (0 = images only, 100 = videos only). |
| `sorting` | String | `"loveit"` | Selection algorithm: `"loveit"`, `"random"`, `"ascending"`, or `"descending"`. |
| `transition-time` | Integer (ms) | `1000` | Default transition duration in milliseconds. |
| `volume` | Integer `0..=100` | `100` | Audio volume percentage. Setting `volume = 0` completely disables audio decoder elements. |
| `script-path` | String (Path) | None | Absolute path to Rhai automation script file. Tilde expansion is not performed. |
| `script-tick-interval` | Integer (seconds) | `1` | Interval between Rhai `on_tick()` hook calls. Must be at least 1. |
| `default-playlist` | String | None | Playlist name to load automatically on startup. |
| `performance` | String | `"balanced"` | Base profile: `"balanced"`, `"quality"`, `"low-power"`, or `"debug"`. |
| `video-fps` | String | `"unlimited"` | Video frame publication cap: `"low"` (12 FPS), `"medium"` (24 FPS), `"high"` (48 FPS), or `"unlimited"`. Under `low-power` profile, defaults to `"low"`. |
| `frame-latency` | Integer | None | Surface presentation frame latency. Defaults to 1 under `low-power`, 2 under `quality`. |
| `pause-on-fullscreen` | Boolean | `false` | Wayland-only occlusion heuristic. When `true`, steady video waits for compositor frame callbacks and freezes while occluded by a fullscreen window. Has no effect on X11. Default is `false`. |

### Output Settings Reference

Applicable within `[any]`, `[<output>]`, and `["re:<pattern>"]`:

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | String (Path) | None | Absolute path to directory containing wallpaper images and videos. Tilde expansion is not performed. |
| `duration` | String (humantime) | `"5m"` | Display duration before switching (e.g., `"30s"`, `"5m"`, `"1h"`, `"2h30m"`). |
| `video-ratio` | Integer `0..=100` | Inherited (`50`) | Probability percentage of selecting video over image. |
| `transition` | String or Table | `{ type = "fade" }` | Transition effect and parameters. |
| `transition-time` | Integer (ms) | Inherited (`1000`) | Transition duration in milliseconds for this output. |
| `volume` | Integer `0..=100` | Inherited (`100`) | Audio volume percentage (0 disables audio decoding). |
| `sorting` | String | Inherited (`"loveit"`) | Output sorting strategy override. |
| `layer` | String | `"background"` | Wayland `wlr-layer-shell` layer: `"background"`, `"bottom"`, `"top"`, or `"overlay"`. |
| `default-playlist` | String | None | Per-output playlist override. |
| `performance` | String | Inherited (`"balanced"`) | Output performance profile override. |
| `video-fps` | String | Inherited | Output video FPS profile override. |
| `frame-latency` | Integer | Inherited | Output frame latency override. |
| `pause-on-fullscreen` | Boolean | Inherited (`false`) | Output fullscreen pause override. |

### Complete Configuration Example

```toml
# /home/your-user/.config/kaleidux/config.toml

[global]
monitor-behavior = "independent"
video-ratio = 40
sorting = "loveit"
transition-time = 1200
volume = 80
script-path = "/home/your-user/.config/kaleidux/automation.rhai"
script-tick-interval = 1
performance = "balanced"
video-fps = "unlimited"
pause-on-fullscreen = false

[any]
path = "/home/your-user/Pictures/Wallpapers"
duration = "10m"
transition = { type = "fade" }
layer = "background"

# Primary monitor with custom 3D cube transition
[DP-1]
path = "/home/your-user/Pictures/Wallpapers/Ultrawide"
duration = "15m"
video-ratio = 70
transition = { type = "cube", persp = 0.5, unzoom = 0.5, reflection = 0.3, floating = 2.0 }
transition-time = 1800
volume = 50

# Secondary monitor with static images only
[HDMI-A-1]
path = "/home/your-user/Pictures/Wallpapers/Portrait"
duration = "30m"
video-ratio = 0
transition = { type = "directional-wipe", direction = [0.0, 1.0], smoothness = 0.4 }
transition-time = 1000

# Regex match for external displays by manufacturer description
["re:Dell.*"]
transition = { type = "cross-zoom", strength = 0.3 }
duration = "20m"
```

## Transitions Reference

Kaleidux compiles WGSL transition shaders executed via WGPU on the GPU. Transitions can be configured using canonical table syntax, simple string syntax, or legacy single-key table syntax.

### Syntax Styles

```toml
# 1. Canonical table syntax (recommended for parametric transitions)
transition = { type = "cube", persp = 0.7, unzoom = 0.3 }
```

```toml
# 2. Simple string syntax (parameterless transitions)
transition = "fade"
```

```toml
transition = { type = "fade" }
```

```toml
# 3. Legacy single-key table syntax
transition = { cube = { persp = 0.7, unzoom = 0.3 } }
```

Set animation duration in milliseconds via `transition-time` separately from the transition definition:

```toml
[any]
transition = { type = "ripple", amplitude = 80.0, speed = 40.0 }
transition-time = 1500
```

### Parameterless Transitions

These transitions require no additional parameters:

`book-flip`, `bow-tie-horizontal`, `bow-tie-vertical`, `burn`, `cannabis-leaf`, `circle`, `color-phase`, `coord-from-in`, `cross-hatch`, `cross-warp`, `displacement`, `dreamy`, `fade`, `glitch-displace`, `glitch-memories`, `heart`, `horizontal-close`, `horizontal-open`, `inverted-page-curl`, `left-right`, `luma`, `multiply-blend`, `overexposure`, `random-noisex`, `rotate`, `scale-in`, `swirl`, `tangent-motion-blur`, `top-bottom`, `vertical-close`, `vertical-open`, `window-blinds`, `wipedown`, `wipeleft`, `wiperight`, `wipeup`, `x-axis-translation`, `zoom-in-circles`, `random`.

Specifying `transition = "random"` causes Kaleidux to select a random transition from the built-in catalog for every wallpaper change.

### Parametric Transitions

| Transition `type` | Parameter Keys and Types | Default Values |
|---|---|---|
| `angular` | `starting_angle: f32` | `starting_angle = 0.0` |
| `bounce` | `shadow_colour: [f32; 4]`, `shadow_height: f32`, `bounces: f32` | `shadow_colour = [0.0, 0.0, 0.0, 0.6]`, `shadow_height = 0.075`, `bounces = 3.0` |
| `bow-tie-with-parameter` | `adjust: f32`, `reverse: bool` | `adjust = 0.5`, `reverse = false` |
| `butterfly-wave-scrawler` | `amplitude: f32`, `waves: f32`, `color_separation: f32` | `amplitude = 1.0`, `waves = 30.0`, `color_separation = 0.3` |
| `circle-crop` | `bgcolor: [f32; 4]` | `bgcolor = [0.0, 0.0, 0.0, 1.0]` |
| `circle-open` | `smoothness: f32`, `opening: bool` | `smoothness = 0.3`, `opening = true` |
| `colour-distance` | `power: f32` | `power = 5.0` |
| `crazy-parametric-fun` | `a: f32`, `b: f32`, `amplitude: f32`, `smoothness: f32` | `a = 4.0`, `b = 1.0`, `amplitude = 120.0`, `smoothness = 0.1` |
| `cross-zoom` | `strength: f32` | `strength = 0.4` |
| `cube` | `persp: f32`, `unzoom: f32`, `reflection: f32`, `floating: f32` | `persp = 0.7`, `unzoom = 0.3`, `reflection = 0.4`, `floating = 3.0` |
| `directional` | `direction: [f32; 2]` | `direction = [0.0, 1.0]` |
| `directional-easing` | `direction: [f32; 2]` | `direction = [0.0, 1.0]` |
| `directional-scaled` | `direction: [f32; 2]`, `scale: f32` | `direction = [0.0, 1.0]`, `scale = 0.7` |
| `directional-warp` | `direction: [f32; 2]` | `direction = [0.0, 1.0]` |
| `directional-wipe` | `direction: [f32; 2]`, `smoothness: f32` | `direction = [1.0, -1.0]`, `smoothness = 0.5` |
| `dissolve` | `line_width: f32`, `spread_clr: [f32; 3]`, `hot_clr: [f32; 3]`, `pow: f32`, `intensity: f32` | `line_width = 0.1`, `spread_clr = [1.0, 0.0, 0.0]`, `hot_clr = [0.9, 0.9, 0.2]`, `pow = 5.0`, `intensity = 1.0` |
| `doom` | `bars: i32`, `amplitude: f32`, `noise: f32`, `frequency: f32`, `drip_scale: f32` | `bars = 30`, `amplitude = 2.0`, `noise = 0.1`, `frequency = 0.5`, `drip_scale = 0.5` |
| `doorway` | `reflection: f32`, `perspective: f32`, `depth: f32` | `reflection = 0.4`, `perspective = 0.4`, `depth = 3.0` |
| `dreamy-zoom` | `rotation: f32`, `scale: f32` | `rotation = 6.0`, `scale = 1.2` |
| `edge` | `thickness: f32`, `brightness: f32` | `thickness = 0.001`, `brightness = 8.0` |
| `fade-color` | `color: [f32; 3]`, `color_phase: f32` | `color = [0.0, 0.0, 0.0]`, `color_phase = 0.4` |
| `fade-grayscale` | `intensity: f32` | `intensity = 0.3` |
| `film-burn` | `seed: f32` | `seed = 2.31` |
| `fly-eye` | `size: f32`, `zoom: f32`, `color_separation: f32` | `size = 0.04`, `zoom = 0.5`, `color_separation = 0.3` |
| `grid-flip` | `size: [i32; 2]`, `pause: f32`, `divider_width: f32`, `bgcolor: [f32; 4]`, `randomness: f32` | `size = [4, 4]`, `pause = 0.1`, `divider_width = 0.05`, `bgcolor = [0.0, 0.0, 0.0, 1.0]`, `randomness = 0.1` |
| `hexagonalize` | `steps: i32`, `horizontal_hexagons: f32` | `steps = 50`, `horizontal_hexagons = 20.0` |
| `kaleidoscope` | `speed: f32`, `angle: f32`, `power: f32` | `speed = 0.5`, `angle = 1.0`, `power = 0.3` |
| `linear-blur` | `intensity: f32` | `intensity = 0.1` |
| `luminance-melt` | `direction: bool`, `luma_threshold: f32` | `direction = true`, `luma_threshold = 0.05` |
| `morph` | `strength: f32` | `strength = 0.1` |
| `mosaic` | `endx: i32`, `endy: i32` | `endx = 2`, `endy = -1` |
| `mosaic-transition` | `mosaic_num: f32` | `mosaic_num = 10.0` |
| `perlin` | `scale: f32`, `smoothness: f32`, `seed: f32` | `scale = 4.0`, `smoothness = 0.01`, `seed = 12.0` |
| `pinwheel` | `speed: f32` | `speed = 2.0` |
| `pixelize` | `squares_min: [i32; 2]`, `steps: i32` | `squares_min = [20, 20]`, `steps = 50` |
| `polar-function` | `segments: i32` | `segments = 5` |
| `polka-dots-curtain` | `dots: f32`, `center: [f32; 2]` | `dots = 20.0`, `center = [0.0, 0.0]` |
| `power-kaleido` | `scale: f32`, `radius: f32`, `angle: f32` | `scale = 2.0`, `radius = 1.5`, `angle = 0.0` |
| `radial` | `smoothness: f32` | `smoothness = 1.0` |
| `random-squares` | `size: [i32; 2]`, `smoothness: f32` | `size = [10, 10]`, `smoothness = 0.5` |
| `rectangle` | `bgcolor: [f32; 4]` | `bgcolor = [0.0, 0.0, 0.0, 1.0]` |
| `rectangle-crop` | `bgcolor: [f32; 4]` | `bgcolor = [0.0, 0.0, 0.0, 1.0]` |
| `ripple` | `amplitude: f32`, `speed: f32` | `amplitude = 100.0`, `speed = 50.0` |
| `rolls` | `rolls_type: i32`, `rot_down: bool` | `rolls_type = 0`, `rot_down = false` |
| `rotate-scale-fade` | `center: [f32; 2]`, `rotations: f32`, `scale: f32`, `back_color: [f32; 4]` | `center = [0.5, 0.5]`, `rotations = 1.0`, `scale = 8.0`, `back_color = [0.15, 0.15, 0.15, 1.0]` |
| `rotate-scale-vanish` | `fade_in_second: bool`, `reverse_effect: bool`, `reverse_rotation: bool` | `fade_in_second = true`, `reverse_effect = false`, `reverse_rotation = false` |
| `simple-zoom` | `zoom_quickness: f32` | `zoom_quickness = 0.8` |
| `simple-zoom-out` | `zoom_quickness: f32`, `fade_edge: bool` | `zoom_quickness = 0.8`, `fade_edge = true` |
| `slides` | `slides_type: i32`, `slides_in: bool` | `slides_type = 0`, `slides_in = false` |
| `squares-wire` | `squares: [i32; 2]`, `direction: [f32; 2]`, `smoothness: f32` | `squares = [10, 10]`, `direction = [1.0, 0.0]`, `smoothness = 1.6` |
| `squeeze` | `color_separation: f32` | `color_separation = 0.1` |
| `static-fade` | `n_noise_pixels: f32`, `static_luminosity: f32` | `n_noise_pixels = 200.0`, `static_luminosity = 0.8` |
| `static-wipe` | `up_to_down: bool`, `max_static_span: f32` | `up_to_down = true`, `max_static_span = 0.5` |
| `stereo-viewer` | `zoom: f32`, `corner_radius: f32` | `zoom = 0.8`, `corner_radius = 0.22` |
| `swap` | `reflection: f32`, `perspective: f32`, `depth: f32` | `reflection = 0.4`, `perspective = 0.2`, `depth = 3.0` |
| `tv-static` | `offset: f32` | `offset = 0.05` |
| `undulating-burn-out` | `smoothness: f32`, `center: [f32; 2]`, `color: [f32; 3]` | `smoothness = 0.03`, `center = [0.5, 0.5]`, `color = [0.0, 0.0, 0.0]` |
| `water-drop` | `amplitude: f32`, `speed: f32` | `amplitude = 30.0`, `speed = 30.0` |
| `wind` | `size: f32` | `size = 0.05` |
| `window-slice` | `count: f32`, `smoothness: f32` | `count = 10.0`, `smoothness = 0.5` |
| `zoom-left-wipe` | `zoom_quickness: f32` | `zoom_quickness = 0.8` |
| `zoom-right-wipe` | `zoom_quickness: f32` | `zoom_quickness = 0.8` |
| `custom` | `shader: String`, `params: Table<String, f32>` | `shader = "..."`, `params = { ... }` |

### Custom GLSL Shaders

Kaleidux supports loading custom GLSL transition fragment shaders stored under `/home/your-user/.config/kaleidux/shaders/<name>.glsl`.

#### TOML Configuration

```toml
[any]
transition = { type = "custom", shader = "custom_wind", params = { size = 0.1 } }
```

The string provided in `shader` corresponds to the filename without the `.glsl` extension. The key-value pairs in `params` are converted into GLSL variable declarations (`float <key> = <val>;`) prepended to the shader source during compilation.

#### Shader Interface Contract

Your GLSL file must define a `vec4 transition(vec2 uv)` function. The built-in runtime prelude provides the following variables and helper functions:

- `progress`: `float` value representing animation progress from `0.0` (start) to `1.0` (complete).
- `screen_aspect` (aliased as `ratio`): `float` aspect ratio (width / height) of the output monitor.
- `prev_aspect`: `float` aspect ratio of the outgoing wallpaper image or video.
- `next_aspect`: `float` aspect ratio of the incoming wallpaper image or video.
- `getFromColor(vec2 uv)`: `vec4` sampling function that samples the outgoing texture using aspect-ratio cover scaling.
- `getToColor(vec2 uv)`: `vec4` sampling function that samples the incoming texture using aspect-ratio cover scaling.

Lines beginning with `uniform ` in user code are commented out during compilation, allowing GL-Transitions shaders to be used directly without editing.

Example custom shader (`/home/your-user/.config/kaleidux/shaders/custom_wind.glsl`):

```glsl
// GLSL transition shader
uniform float size; // Injected from params table: float size = 0.1;

float rand(vec2 co) {
    return fract(sin(dot(co.xy, vec2(12.9898, 78.233))) * 43758.5453);
}

vec4 transition(vec2 uv) {
    float r = rand(vec2(0.0, uv.y));
    float m = smoothstep(0.0, -size, uv.x * (1.0 - size) + size * r - (progress * (1.0 + size)));
    return mix(getFromColor(uv), getToColor(uv), m);
}
```

## Video and Audio Playback

### Backend Comparison

Video backend selection is controlled via the `--video-backend` CLI flag or the `KLD_VIDEO_BACKEND` environment variable. It is not configured via `config.toml`.

| Characteristic | FFmpeg (`ffmpeg`) | libmpv (`mpv`) | GStreamer appsink (`appsink`) |
|---|---|---|---|
| Audio Support | No (video decoding only) | Yes | Yes |
| Hardware Decoders | Vulkan Video, VA-API, NVDEC, QSV, D3D11/12 | VA-API, NVDEC (via mpv hwdec) | VA-API, NVDEC (via GStreamer plugins) |
| Zero-Copy Presentation | DMA-BUF, Vulkan import | Composed GL interop (default) | CUDA-Vulkan interop, DMA-BUF |
| Architecture Profile | Direct libavcodec demux and decode | Offscreen GL rendering into shared texture | GStreamer pipeline graph with autoplugging |
| Typical Workload | Silent looping wallpapers | Audio-enabled wallpapers, mpv filters | Specialized GStreamer plugins or CUDA setups |

Performance and resource utilization depend on hardware decode availability, driver support, and video resolution rather than backend selection alone.

### Audio Mechanics and Volume Control

- Wallpaper audio requires the `mpv` or `appsink` backend. The native FFmpeg backend does not implement audio decoding.
- Audio volume is set via `volume = <0..100>` in `config.toml` (defaults to `100`).
- Setting `volume = 0` completely disables audio decoder elements:
  - In GStreamer, pipeline flags switch to video-only, preventing the autoplugging of audio parsers and decoders.
  - In libmpv, the `audio` property is set to `no` and `ao` is set to `null`.
- Setting a positive volume (`volume = 1..100`) enables the audio pipeline (`flags = video+audio` in appsink, `audio = yes` in mpv).
- Before transitioning or tearing down an audio-enabled pipeline, Kaleidux fades audio volume to zero to prevent audible clicks.

### Frame Rates and Occlusion Pacing

- `video-fps` controls frame publication:
  - `"unlimited"`: Publishes every decoded frame as it arrives.
  - `"high"`: Caps publication at 48 FPS.
  - `"medium"`: Caps publication at 24 FPS.
  - `"low"`: Caps publication at 12 FPS.
- `pause-on-fullscreen = false` is the default. Video continues advancing when covered by windows.
- On Wayland, setting `pause-on-fullscreen = true` causes steady video rendering to pace through compositor frame callbacks. When an application covers the output in fullscreen mode, the compositor ceases frame callbacks to the layer surface, holding the wallpaper on its current frame and eliminating presentation work. This setting has no effect on X11.

## History, Sorting, and Content Selection

### Selecting an Image by Path

```bash
kldctl jump ~/Pictures/wallpapers/forest.png
kldctl set ~/Downloads/new-wallpaper.png -o DP-1
kldctl img ~/Pictures/wallpapers/forest.png
```

`jump PATH` selects an image inside the target slideshow directory, including
subdirectories. `set PATH` inserts an image into the current queue and immediately
advances to it. `img PATH` accepts either: slideshow images are selected in place,
and external images are inserted. Existing entries are not duplicated.

These commands use the normal transition and history: `prev` returns to the
previous wallpaper, and `next` returns to the selected image. An explicit selection
replaces any older forward-history branch. Subsequent automatic selection follows
the configured sorting strategy. Insertions are in-memory only; rebuilding the
queue, changing playlists or restarting can discard external entries. No files are
copied and the configuration is unchanged.

Paths are resolved from the client's working directory; quote paths containing
spaces. Only images are accepted. Missing files, blacklisted images, unknown
outputs and outputs without a slideshow queue return errors. Omit `-o OUTPUT`
to target all outputs; synchronized outputs and groups switch together, as with
`next`. A `jump` targeting multiple directories must be inside each target directory.

### History Navigation

- Each output queue retains a bounded history buffer of up to 50 entries.
- `kldctl prev` pops the current item and moves backward through history.
- `kldctl next` checks forward history first (populated when stepping back via `prev`) before picking new content from the pool.

### Sorting Strategies

Set `sorting` in `[global]` or per-output:

```toml
[global]
sorting = "loveit" # Options: "loveit", "random", "ascending", "descending"
```

1. `loveit` (default):
   - Computes weighted random selection using pick counts, last-seen timestamps, and love multipliers.
   - Files marked via `kldctl love <path> -m <multiplier>` receive weighted preference.
   - Statistics are persisted in an embedded redb database at `~/.cache/kaleidux/cache.redb`.
   - In-memory cache is bounded to 10,000 LRU entries (`STATS_LRU_CAP`). Infrequently seen wallpapers eventually age out of the LRU cache.
2. `random`:
   - Uniform pseudo-random selection across all non-blacklisted files in the directory.
3. `ascending`:
   - Sequential alphabetical playback sorted by file path.
4. `descending`:
   - Sequential reverse-alphabetical playback sorted by file path.

## Automation and Rhai Scripting

Kaleidux embeds the Rhai scripting engine for custom event handling and rotation logic.

### Configuration

Enable scripting in `config.toml` using an absolute path:

```toml
[global]
script-path = "/home/your-user/.config/kaleidux/automation.rhai"
script-tick-interval = 1 # Execute on_tick() every second (must be >= 1)
```

### Lifecycle Hooks and State Management

Your Rhai script can define two lifecycle entry points:

1. `fn init()`: Executed once when the script is loaded on daemon startup.
2. `fn on_tick()`: Executed periodically every `script-tick-interval` seconds.

Top-level statements run once before `init()`. Global variables persist between
ticks, so counters and state machines can live in the script. State is in memory
only and resets when the daemon restarts. A tick runs after the configured interval;
it is not a wall-clock scheduler and missed ticks are not replayed.

Reloading via `kldctl reload` does not reload automation scripts or their tick
interval. Changes to a script, `script-path`, or `script-tick-interval` require
restarting the daemon (`kldctl kill` and restart).

### Registered API Functions

These functions are available in addition to Rhai's standard language operations:

| Function Signature | Description |
|---|---|
| `print(text)` | Emits an INFO-level log message in the daemon log with the prefix `[Script] <text>`. |
| `next(output)` | Advances wallpaper. Pass `"*"` to advance all outputs, or pass a specific output name (e.g., `"DP-1"`). |
| `pause()` | Pauses all video players and suspends automatic wallpaper cycling. |
| `resume()` | Resumes video players and automatic wallpaper cycling. |
| `prev(output)` | Steps backward through history; `"*"` means all outputs. |
| `inhibit(reason)` | Adds a named pause reason without changing manual pause. |
| `uninhibit(reason)` | Removes only that named reason. |
| `load_playlist(name)` | Loads a saved playlist across queues; `""` or `"*"` restores directory selection. |
| `clear(output)` | Clears wallpaper on the named output; `"*"` means all outputs. |

`next()`, `prev()`, and `clear()` without an argument target all outputs.
`load_playlist()` without an argument unloads the playlist. `resume()` clears
manual pause only; named inhibitors can keep playback paused.

### Execution Semantics and Limits

- Commands enqueue work without waiting for the daemon to execute it. Successful
  enqueueing is not confirmation that a playlist exists or a command succeeded.
- A full or closed command channel raises a Rhai error. Invalid inhibitor names
  also raise an error before enqueueing. Use Rhai `try`/`catch` for recovery.
- Each initialization or hook call is limited to 50,000 operations, 32 call
  levels, expression depth 32, 64 KiB per string, and 10,000 entries per array or
  map. Script source is limited to 1 MiB. These are execution and individual-object
  limits, not an aggregate memory quota or a security sandbox.
- File module imports and shell execution are unavailable. Do not busy-wait or
  try to sleep inside a tick; keep state and return until the next tick.
- Uncaught tick errors are logged; the next scheduled tick can run again. A load
  or initialization error is logged at startup and leaves automation inactive.
  Commands queued before an error are not rolled back.

### Script Example

```rhai
// /home/your-user/.config/kaleidux/automation.rhai
let ticks = 0;

fn init() {
    print("Automation script initialized");
}

fn on_tick() {
    ticks += 1;
    if ticks >= 300 {
        next("*");
        ticks = 0;
    }
}
```

At a one-second tick interval this requests a change about every five minutes.
The regular wallpaper timer still runs independently; use `duration` for ordinary
rotation and scripting when a conditional rule is needed.

### Gaming, Sleep, and Battery Hooks

Named inhibitors compose without undoing each other:

```sh
kldctl inhibit gaming
kldctl inhibit sleep
kldctl inhibitors           # Lists named reasons, not manual pause
kldctl uninhibit gaming     # Still paused for sleep
kldctl uninhibit sleep      # Resumes only if manual pause is also clear
```

Names are case-sensitive, nonblank strings of at most 64 UTF-8 bytes without
control characters. At most 64 different names can be held. Repeating `inhibit`
with the same name is idempotent, not reference-counted; releasing a missing name
also succeeds. Use separate names for independent callers.

Inhibitors pause decoding and automatic rotation, retaining the current image or
video frame. Explicit `next`, `prev`, and `clear` still work. Final release starts
a fresh rotation interval; display power state can still delay video resumption.
This does not change `pause-on-fullscreen`, which remains off by default.

Reasons last until released or the daemon exits. There is no timeout, client-PID
tracking, or persistence across daemon restarts. If a hook is killed with SIGKILL,
inspect `kldctl inhibitors` and release its stale reason manually. `kldctl resume`
does not remove named reasons. Daemon-reported errors make `kldctl` exit nonzero.

#### Wrap a Game or Other Foreground Command

The optional [run-paused.sh](./examples/automation/run-paused.sh) wrapper uses a
unique reason, propagates the command's exit status, and releases its reason on
normal exit or INT/TERM/HUP. If Kaleidux is unavailable it warns and runs the
command anyway:

```sh
sh /path/to/Kaleidux/examples/automation/run-paused.sh game-executable --option
```

Steam launch options can use:

```text
sh /path/to/Kaleidux/examples/automation/run-paused.sh %command%
```

Keep the launched process in the foreground. A launcher that exits after spawning
a detached game releases the reason too early. The wrapper forwards signals to
its direct child, not an entire descendant process tree.

#### Low-Battery Pause

The optional [pause-on-battery.sh](./examples/automation/pause-on-battery.sh) reads
Linux power-supply status every 30 seconds and holds its own reason while the
chosen battery is discharging at or below the threshold:

```sh
sh /path/to/Kaleidux/examples/automation/pause-on-battery.sh /sys/class/power_supply/BAT0 20
```

Choose the battery directory your machine exposes. The default is `BAT0` and
20 percent. Charging, sufficient charge, or a failed status read releases only
this hook's reason. Run it in your graphical user session so `kldctl` finds the
same socket; it does not install a service or change system power settings.
It retries the command on each poll, including after daemon restarts. Stopping
the hook normally releases its reason.

#### Sleep and Idle Integrations

Connect `kldctl inhibit sleep` to your idle manager's before-sleep hook and
`kldctl uninhibit sleep` to its after-resume hook. Use a different reason, such as
`idle`, for ordinary idle/resume events. Hooks must run as your desktop user with
the correct `XDG_RUNTIME_DIR`; root system-sleep scripts do not inherit that
session automatically. Kaleidux itself does not suspend the computer.

For [hypridle](https://wiki.hypr.land/hypr-ecosystem/user/hypridle/), merge these
commands into your existing hooks rather than replacing your lock-screen commands:

```text
general {
    before_sleep_cmd = kldctl inhibit sleep
    after_sleep_cmd = kldctl uninhibit sleep
}
listener {
    timeout = 300
    on-timeout = kldctl inhibit idle
    on-resume = kldctl uninhibit idle
}
```

For [Feral GameMode](https://github.com/FeralInteractive/gamemode/blob/master/example/gamemode.ini),
the custom start/end hooks can hold one reason for the GameMode session:

```ini
[custom]
start=kldctl inhibit gamemode
end=kldctl uninhibit gamemode
```

Merge with any existing custom hooks. These examples only control Kaleidux;
they do not install GameMode, change GPU settings, or enable automatic sleep.

### Desktop Environment Automation

#### Systemd User Service

Create `~/.config/systemd/user/kaleidux.service`:

```ini
[Unit]
Description=Kaleidux Dynamic Wallpaper Daemon
PartOf=graphical-session.target
After=graphical-session.target

[Service]
Type=simple
ExecStart=%h/.cargo/bin/kaleidux-daemon --log 1
Restart=on-failure
RestartSec=2

[Install]
WantedBy=graphical-session.target
```

Enable and start:
```sh
systemctl --user daemon-reload
systemctl --user enable --now kaleidux.service
```

#### Window Manager Keybindings (Sway / Hyprland)

In `~/.config/sway/config` or `~/.config/hypr/hyprland.conf`:

```text
# Sway
bindsym $mod+bracketright exec kldctl next
bindsym $mod+bracketleft exec kldctl prev
bindsym $mod+p exec kldctl pause
bindsym $mod+Shift+p exec kldctl resume

# Hyprland
bind = $mainMod, bracketright, exec, kldctl next
bind = $mainMod, bracketleft, exec, kldctl prev
bind = $mainMod, P, exec, kldctl pause
bind = $mainMod SHIFT, P, exec, kldctl resume
```

## Environment Variables Reference

Kaleidux evaluates environment variables at startup and during playback to configure limits, hardware paths, and debugging probes.

### Image Cache and Memory Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KALEIDUX_IMAGE_CACHE_MAX_MIB` | Integer | `2048` | Maximum disk cache capacity in MiB for prepared textures under `~/.cache/kaleidux/prepared-images`. Set to `0` to disable the byte ceiling. |
| `KALEIDUX_IMAGE_CACHE_MAX_ENTRIES` | Integer | `4096` | Maximum number of files in the prepared texture disk cache. Set to `0` to disable count pruning. |
| `KALEIDUX_IMAGE_CACHE_MAX_AGE_DAYS` | Integer | `180` | Maximum age in days before an unread cached image is pruned. Set to `0` to disable age expiration. |
| `KALEIDUX_IMAGE_CACHE_MIN_FREE_MIB` | Integer | `2048` | Free disk space floor in MiB on the cache filesystem. If remaining disk space falls below this, cache writes are halted. Set to `0` to disable. |
| `KALEIDUX_IMAGE_CACHE_UNLIMITED` | Boolean (`1`/`true`) | Unset | Disables all four cache limits above when set to `1`, `true`, `yes`, or `on`. |
| `KALEIDUX_IMAGE_CACHE_FSYNC` | Boolean (`1`/`true`) | Unset | When enabled (`1`, `true`, `yes`, on), executes `fsync` on prepared image cache payloads prior to atomic file rename. |
| `KALEIDUX_IMAGE_CHANNEL_MAX_MIB` | Integer | `256` | Memory budget in MiB for waiting image upload channels (enforced in 64 KiB permit units). |
| `KLD_IMAGE_DECODE_WORKERS` | Integer | `1` | Number of concurrent background worker threads allocated for decoding source images (clamped between 1 and 8). |
| `MALLOC_MMAP_THRESHOLD_` | Integer | Unset | Explicit glibc mmap threshold. Under glibc without jemalloc, Kaleidux calls `mallopt(M_MMAP_THRESHOLD, 128 KiB)` unless this variable, `MALLOC_TRIM_THRESHOLD_`, or `GLIBC_TUNABLES` is set. |
| `MALLOC_TRIM_THRESHOLD_` | Integer | Unset | Explicit glibc trim threshold. Under glibc without jemalloc, Kaleidux calls `mallopt(M_TRIM_THRESHOLD, 128 KiB)` unless this variable, `MALLOC_MMAP_THRESHOLD_`, or `GLIBC_TUNABLES` is set. |
| `GLIBC_TUNABLES` | String | Unset | Inherited glibc tuning string. When set, Kaleidux skips default `mallopt` calls. |

### Video Backend and Presentation Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KLD_VIDEO_BACKEND` | String | `"auto"` | Fallback video backend request (`"auto"`, `"ffmpeg"`, `"mpv"`, `"appsink"`) when the `--video-backend` CLI flag is omitted. |
| `KLD_VIDEO_IMMEDIATE_PRESENT` | Boolean (`1`/`true`) | Unset | When set to `1`, `true`, `yes`, or `on`, forces immediate surface commit on frame arrival without waiting for compositor frame callbacks. |
| `KLD_VIDEO_FRAME_CALLBACK_DAMAGE` | String | `"minimal"` | Sets Wayland surface damage on steady video callbacks. Setting to `"full"`, `"surface"`, `"1"`, `"true"`, `"yes"`, or `"on"` damages the full layer surface instead of minimal damage. |
| `KLD_VIDEO_CALLBACK_UPLOAD_INTERVAL_MS` | Integer | `0` | Minimum elapsed time in milliseconds between texture uploads triggered by compositor frame callbacks. |
| `KLD_VIDEO_RATE_FILTER` | Boolean (`0`/`false`) | `true` | When set to `0`, `false`, `no`, or `off`, disables the GStreamer `videorate` filter element in appsink pipelines. |
| `KLD_LOW_POWER_MAX_PUBLISH_FPS` | Integer | `12` | Overrides the FPS ceiling for `video-fps = "low"` (clamped to max 120; `0` disables cap). Other FPS profiles are unaffected. |
| `KLD_STOP_VIDEO_ON_IMAGE_SWITCH` | Boolean | `true` | When `true` (`1`, `true`, `yes`, `on`, `immediate`), stops active video playback immediately upon switching to an image. Set to `0`, `false`, `no`, `off`, `defer`, or `legacy` to defer teardown. |

### GStreamer Appsink Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KLD_APPSINK_SYNC` | Boolean | `true` | Enables clock synchronization on the GStreamer appsink element. Setting `0`, `false`, `no`, or `off` runs appsink unsynchronized. |
| `KLD_APPSINK_UNSYNC` | Boolean (`1`/`true`) | Unset | When set to `1`, `true`, `yes`, or `on`, overrides `KLD_APPSINK_SYNC` and forces unsynchronized appsink playback. |
| `KLD_APPSINK_PROCESSING_DEADLINE_MS` | Integer | `20` | Buffer-processing time budget in milliseconds. Negative values become zero; late-buffer dropping is controlled separately by maximum lateness. |
| `KLD_APPSINK_MAX_LATENESS_MS` | Integer | `-1` | Maximum allowable buffer lateness in milliseconds (`-1` disables lateness dropping). |
| `KLD_APPSINK_PENDING_REFRESH_MS` | Integer | `32` / `75` | Frame refresh timer in milliseconds for pending mailbox buffers (defaults to 32 ms when FPS is capped, 75 ms when uncapped). Values less than 0 disable the timer. |
| `KLD_APPSINK_DROP_IF_MAILBOX_PENDING` | Boolean | `true` | Drops incoming video frames if the renderer mailbox already holds an unconsumed frame. Set to `0` or `false` to disable. |

### CUDA and Zero-Copy Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KLD_CUDA_SKIP_FRAME_SYNC` | Boolean (`1`/`true`) | Unset | When set to `1`, `true`, `yes`, or `on`, skips CUDA stream synchronization prior to Vulkan texture upload. Default is unset (frame sync enabled). |
| `KLD_CUDA_LAYOUT_PREFER_VIDEOINFO` | Boolean (`1`/`true`) | Unset | When set to `1`, `true`, `yes`, or `on`, forces CUDA layout negotiation to derive geometry from GStreamer VideoInfo instead of negotiated buffer caps. |
| `KLD_NVDEC_OUTPUT_SURFACES` | Integer | `2` | Number of NVDEC output surfaces allocated in the decoder pool (clamped to maximum 16). |

### libmpv Backend Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KLD_MPV_CAPTURE_FPS` | Integer | `48` | Render capture frame rate for mpv offscreen rendering (clamped between 1 and 120). Defaults to output profile `max_publish_fps` if set, otherwise 48. |
| `KLD_MPV_HWDEC` | String | Auto-detected | Hardware decode mode passed to libmpv (`"vaapi"` on Intel/AMD, `"auto-safe"` on NVIDIA or unknown adapters, `"no"` to disable). |
| `KLD_MPV_QUALITY` | String | `"fast"` | Quality profile for libmpv. `"fast"` enables bilinear scaling and disables debanding/dithering; `"default"` uses standard mpv filters. |
| `KLD_MPV_RENDER_TO_OUTPUT` | Boolean | Unset | Overrides `KLD_MPV_RENDER_SIZE_MODE`. When enabled (`1`, `true`, `yes`, `on`), forces mpv to render to output display resolution. When `0`, `false`, `no`, `off`, forces source resolution. |
| `KLD_MPV_RENDER_SIZE_MODE` | String | `"adaptive"` | Sizing mode for offscreen render target: `"adaptive"` / `"bounded"` (bounds while preserving aspect ratio), `"source"` / `"native"`, or `"output"` / `"surface"`. |
| `KLD_MPV_SW_FORMAT` | String | `"rgba"` | Software rendering pixel format (`"rgba"` or `"rgb0"`). |
| `KLD_MPV_GL_SYNC` | String | `"semaphore"` | GL synchronization mode. Default uses semaphore handoff. Set to `"finish"` or `"strict"` to execute synchronous `glFinish`. |
| `KLD_MPV_RENDER_API` | String | Unset (`"gl-composed"`) | Selects mpv presentation API. Unset uses composed GL interop via shared texture. Set to `"gl-overlay"` for native GL overlay diagnostic (bypasses WGPU composition and transitions), or `"software"` for software rendering. |
| `KLD_MPV_LOG_LEVEL` | String | `"warn"` | Sets libmpv internal log request level (`"error"`, `"warn"`, `"info"`, `"debug"`). |
| `KLD_MPV_MSG_LEVEL` | String | `"all=warn"` | Granular libmpv subsystem message level string passed to libmpv initialization. |

### Native (FFmpeg) Backend Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KLD_NATIVE_HWDECODER` | String | `"auto"` | Hardware decoder selection: `"auto"`, `"vulkan"` / `"vulkan-video"`, `"vaapi"`, `"cuda"` / `"nvdec"`, `"qsv"`, `"amf"`, `"videotoolbox"`, `"d3d12"` / `"d3d12va"`, `"d3d11"` / `"d3d11va"`, `"mediacodec"`, or `"none"` / `"software"` to disable hardware decoding. |
| `KLD_NATIVE_VAAPI_DRIVER` | String | Auto-detected | Explicit VA-API driver name passed to libavutil (e.g., `"radeonsi"`, `"iHD"`). If unset, auto-detected from `/sys/class/drm`. |
| `KLD_NATIVE_DRM_DEVICE` | String | Auto-detected | Explicit DRM render node path (e.g., `"/dev/dri/renderD128"`). If unset, auto-detected from `/dev/dri`. |
| `KLD_NATIVE_DECODE_THREADS` | Integer | `1` | Number of decode worker threads configured for software decoding in libavcodec. |
| `KLD_NATIVE_SURFACE_IMPORT` | Boolean | `true` | Enables native DMA-BUF surface import into Vulkan textures. Set to `0`, `false`, `no`, or `off` to disable. |
| `KLD_NATIVE_GL_SURFACE` | Boolean | Unset (`false`) | Enables experimental native OpenGL surface presentation when set to `1`, `true`, `yes`, or `on`. |
| `KLD_NATIVE_WAYLAND_SURFACE` | Boolean | `true` | Enables native DMA-BUF Wayland subsurface presentation. Set to `0`, `false`, `no`, or `off` to disable. |
| `LIBVA_DRIVER_NAME` | String | Host env | Evaluated on startup; if it specifies a mismatched driver for Intel or AMD hardware, Kaleidux unsets it for the daemon process to permit correct driver auto-detection. |

### Power, Desktop, and Tracing Knobs

| Variable | Type | Default | Description |
|---|---|---|---|
| `KLD_HYPRLAND_POWER_SOCKET` | String (Path) | Auto-detected | Path to Hyprland IPC socket for DPMS monitor power state observation (defaults to `$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket.sock`). |
| `KLD_TRACE_ALL` | Boolean (`1`/`true`) | Unset | Enables system-wide trace diagnostics, sets GStreamer log level to TRACE, and forces a 1 ms main loop polling interval. |
| `KLD_TRACE_FRAME_EVENTS` | Boolean (`1`/`true`) | Unset | Enables logging of Wayland surface frame callbacks and commit timestamps. Active if `KLD_TRACE_ALL` is set. |
| `KLD_TRACE_TRANSITION_EVENTS` | Boolean (`1`/`true`) | Unset | Enables logging of WGPU transition shader animation progression. Active if `KLD_TRACE_ALL` is set. |
| `KLD_TRACE_VIDEO_DRAIN` | Boolean (`1`/`true`) | Unset | Enables logging of video frame channel queue drain events. Active if `KLD_TRACE_ALL` is set. |
| `KLD_TRACE_VIDEO_LAYOUT_EVERY_FRAME` | Boolean (`1`/`true`) | Unset | Logs video plane layout and stride dimensions on every decoded frame. |
| `KLD_TRACE_VIDEO_UPLOAD` | Boolean (`1`/`true`) | Unset | Logs GPU texture upload timings for every video frame. Active if `KLD_TRACE_ALL` is set. |

## Troubleshooting and Diagnostics

### Common Issues and Solutions

#### Daemon Fails to Start: Config Parse Errors

```sh
# Run syntax validation against config.toml
kldctl check-config
```

`kldctl check-config` verifies basic TOML syntax by parsing the file into a generic `toml::Value`. It reports syntax errors such as unclosed quotes or invalid bracket nesting. Because it does not deserialize the file into daemon configuration structs, it does not validate table hierarchies, key names, or field data types. Full schema validation occurs when `kaleidux-daemon` starts or when `kldctl reload` runs.

#### Client Cannot Connect to Daemon

```text
Failed to connect to daemon at /run/user/1000/kaleidux.sock: No such file or directory
Is kaleidux-daemon running?
```

1. Verify `kaleidux-daemon` is running via `pgrep -a kaleidux-daemon`.
2. Check socket location. If running inside tmux, screen, or across user boundaries, ensure `XDG_RUNTIME_DIR` matches between the daemon and `kldctl`, or pass `-s /path/to/socket` explicitly.

#### Blank or Black Video Playback

When video files display a black frame or fail to advance:

1. Test individual backends explicitly via the CLI flag to isolate decoder failures:
   ```sh
   kaleidux-daemon --log 3 --video-backend ffmpeg
   kaleidux-daemon --log 3 --video-backend mpv
   kaleidux-daemon --log 3 --video-backend appsink
   ```
2. Disable hardware decoding to check if the issue is driver-specific:
   ```sh
   # Force CPU software decoding for appsink
   kaleidux-daemon --video-mode cpu

   # Force CPU software decoding for FFmpeg
   KLD_NATIVE_HWDECODER=none kaleidux-daemon --video-backend ffmpeg

   # Force CPU software decoding for mpv
   KLD_MPV_HWDEC=no kaleidux-daemon --video-backend mpv
   ```
3. Inspect the active log file in `~/.config/kaleidux/logs/`.

#### Video Plays Without Audio

- Confirm backend: The native FFmpeg backend decodes video streams only. For wallpaper audio, use `--video-backend mpv` or `--video-backend appsink`.
- Check volume configuration: If `volume = 0` in `config.toml`, audio decoding elements are omitted from the pipeline. Set `volume` to a value between 1 and 100.

#### High Memory Retention on Long Runs

Under glibc, adaptive heap fragmentation can cause freed decode buffers to remain unreleased. Kaleidux automatically tunes `mallopt` thresholds to 128 KiB on glibc system allocator builds when allocator variables are unset. If using custom allocators or preloading libraries, ensure `MALLOC_MMAP_THRESHOLD_` and `MALLOC_TRIM_THRESHOLD_` are not set to large values.

#### NixOS / nixGL Driver Compatibility

On non-NixOS Linux distributions running Kaleidux through Nix, graphics drivers must be linked using nixGL:

```sh
nixGL kaleidux-daemon --log 1
```

The nixGL wrapper preserves `/nix/store/` and `/run/opengl-driver` library paths while filtering incompatible host paths. If the shell interpreter fails before the wrapper executes, run `unset LD_LIBRARY_PATH` prior to launching nixGL.

### Performance Analysis with `kldctl perf-snapshot`

Run `kldctl perf-snapshot` while playback is active to inspect runtime metrics:

- `component_cpu`: Compares average CPU time spent across subsystem stages (`renderer`, `video`, `image`, `file_disc`, `shader`).
- `video_upload`: High `copy` or `sync` times indicate stalls on PCI bus transfers or GPU synchronization. Check whether DMA-BUF zero-copy or CUDA interop is active.
- `video`: If `stale` increments continuously, incoming video frames are arriving faster than output presentation intervals, causing the renderer to discard unpresented buffers. Adjust `video-fps` to match output capability.
- `image_cache`: Low hit counts and frequent evictions indicate that `KALEIDUX_IMAGE_CACHE_MAX_MIB` is too small for the active wallpaper pool.
