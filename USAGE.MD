# Kaleidux Advanced Usage Guide

This guide covers advanced configuration, monitor management, and scripting features of Kaleidux.

## Configuration Location

- **Primary Config**: `~/.config/kaleidux/config.toml`
- **Logs**: `~/.config/kaleidux/logs/`
- **Automation Scripts**: `~/.config/kaleidux/automation.rhai` (optional)

## Monitor Behaviors

Kaleidux supports three modes of monitor synchronization (configured in the `[global]` section):

### 1. Independent (Default)

Each monitor runs its own queue and picks wallpapers regardless of other monitors.

```toml
[global]
monitor-behavior = "independent"
```

### 2. Synchronized

All monitors show the exact same wallpaper at the same time. The first monitor to finish loading triggers the transition for all.

```toml
[global]
monitor-behavior = "synchronized"
```

### 3. Grouped

Divide monitors into specific groups. Monitors within a group stay in sync.

```toml
[global]
monitor-behavior = { grouped = [["DP-1", "DP-2"], ["HDMI-A-1"]] }
```

## Content Selection Logic

### Video Ratio

Control the balance between images and videos (0-100).

- `video-ratio = 50`: Equal chance for image or video.
- `video-ratio = 100`: Videos only.

### Sorting Strategies

- `loveit`: Weighted random selection. High "Love" multiplier and recently added files appear more often.
- `random`: Pure random selection.
- `ascending` / `descending`: Alphabetical order based on filename.

## Command Line Interface (kldctl)

`kldctl` is the Swiss Army knife for controlling the daemon.

| Command         | Alias | Description                              |
| --------------- | ----- | ---------------------------------------- |
| `next`          | `n`   | Skip to the next wallpaper               |
| `prev`          | `p`   | Go back to the previous wallpaper        |
| `love <path>`   | -     | Increase frequency for a specific file   |
| `unlove <path>` | -     | Reset frequency for a specific file      |
| `lovelist`      | `ll`  | List all "loved" wallpapers              |
| `pause`         | -     | Pause video playback                     |
| `resume`        | -     | Resume video playback                    |
| `query`         | `q`   | List connected outputs and current state |
| `reload`        | -     | Reload configuration from disk           |
| `kill`          | -     | Exit the daemon gracefully               |

## Rhai Scripting (Automation)

You can automate wallpaper changes using a Rhai script. Set `script-path` in your config.

**Example `automation.rhai`**:

```rust
// Change wallpaper specifically for DP-1 every Sunday
let now = chrono::local();
if now.weekday() == "Sunday" {
    kld.next("DP-1");
}

// Custom log
print("Automated tick at " + now.format("%H:%M"));
```

## Transitions Reference

Kaleidux includes over 50 transitions. Most can be used with a simple name string, or a table to configure parameters,
see the [transition examples](./transitions_examples.toml).

**Example parametric transition:**

```toml
[any]
transition = { type = "cube", persp = 0.7, unzoom = 0.3, reflection = 0.4, floating = 3.0 }
```

**Common parameters:**

- `transition-time`: Global duration in ms (e.g. `1000`).
- `smoothness`: How "hard" the transition edge is.
- `direction`: For wipe/directional effects (e.g. `[1.0, 0.0]`).

## Video Decode Mode

The daemon supports several video frame paths. Use `--video-mode <MODE>` to force a mode, or omit it for automatic selection.

| Mode     | Description                                                                                                                             |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `auto`   | **(Default)** Automatically picks the fastest available mode (e.g., `dmabuf` for Intel/AMD/NVIDIA). Replaces the old `zero-copy` alias. |
| `cuda`   | Force NVIDIA CUDA memory + CUDA--Vulkan interop (true zero-copy). Requires NVIDIA drivers 535+.                                         |
| `dmabuf` | Force DMA-BUF zero-copy path. Supported on Intel, AMD, and NVIDIA (requires NVIDIA 550+).                                               |
| `nv12`   | Request NV12 from the decoder; frames are uploaded to the GPU via CPU (no zero-copy).                                                   |
| `rgba`   | Legacy: decoder outputs RGBA; frames are uploaded via CPU (slowest).                                                                    |

The zero-copy alias is no longer accepted; using --video-mode zero-copy will error. Users should switch to --video-mode auto for the best available zero-copy path.

`kaleidux-daemon` (the daemon binary, typically started as a background service)
provides the core engine, while `kldctl` is the command-line interface for controlling it.
