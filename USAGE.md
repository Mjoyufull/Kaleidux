# Using Kaleidux

## Configuration

The daemon reads `~/.config/kaleidux/config.toml`. Start with
[config.example.toml](./config.example.toml), set `[any].path` to your wallpaper
directory, and run `kldctl reload` after changes.

`[any]` supplies output defaults. Exact output sections such as `[DP-1]` override
matching regex sections such as `["re:.*"]`. Use `kldctl query` to find output
names. Independent monitors have separate queues and a small scheduling offset;
`synchronized` shares content across outputs, while `grouped` shares it within
configured groups.

See [transitions_examples.toml](./transitions_examples.toml) for transition
types and their parameters. Set `transition-time` in milliseconds separately
from the transition table:

```toml
[any]
path = "~/Pictures/Wallpapers"
duration = "5m"
transition = { type = "cube", persp = 0.4, unzoom = 0.8 }
transition-time = 1000
```

## Video playback

`--video-backend auto` tries FFmpeg, then mpv, then GStreamer appsink when backend
initialization fails. Forcing `ffmpeg`, `mpv`, or `appsink` disables automatic
backend fallback. A build without the requested backend reports the feature
needed to enable it.

`video-fps = "unlimited"` follows the source video's frame rate. The finite
settings `low`, `medium`, and `high` cap publication at 12, 24, and 48 FPS.
`pause-on-fullscreen = false` is the default: video continues advancing on an
otherwise idle desktop. On Wayland, setting it to `true` makes steady video
wait for compositor frame callbacks and hold its current frame while occluded.
This setting has no effect on X11.

FFmpeg playback is video-only. Select mpv or appsink if you need wallpaper audio.

Explicit `--video-mode cpu`, `cuda`, `dmabuf`, `nv12`, and `rgba` use the
appsink backend when the backend is `auto`. Combining these modes with forced
FFmpeg or mpv reports an error instead of ignoring the mode.

## History and selection

`kldctl prev` steps back through history; `kldctl next` follows forward history
before selecting new content. `sorting = "loveit"` weights selection using
popularity and recency. Its statistics cache is bounded, so older entries,
including loved entries, can eventually age out.

## Image caching and memory

The prepared-image disk cache defaults to 2 GiB, 4096 entries, a maximum age of
180 days, and a 2 GiB free-space floor. These environment variables override
the limits:

- `KALEIDUX_IMAGE_CACHE_MAX_MIB`
- `KALEIDUX_IMAGE_CACHE_MAX_ENTRIES`
- `KALEIDUX_IMAGE_CACHE_MAX_AGE_DAYS`
- `KALEIDUX_IMAGE_CACHE_MIN_FREE_MIB`

Zero disables the corresponding limit. `KALEIDUX_IMAGE_CACHE_UNLIMITED=1`
disables all four limits. `KALEIDUX_IMAGE_CACHE_FSYNC=1` syncs payloads before
atomic rename, at the cost of extra write latency.

Images waiting for upload have a 16-message limit and a 256 MiB weighted budget.
`KALEIDUX_IMAGE_CHANNEL_MAX_MIB` changes that budget; one oversized image can
occupy the whole budget until it is consumed.

On glibc builds using the system allocator, the daemon sets mmap and trim
thresholds to 128 KiB to reduce retention of freed decode buffers. Explicit
`MALLOC_MMAP_THRESHOLD_`, `MALLOC_TRIM_THRESHOLD_`, or `GLIBC_TUNABLES` settings
disable this tuning. Builds using the optional `jemalloc` feature do not apply it.

## Diagnostics

Console logging defaults to `WARN`. `--log 1` enables warnings and file logging;
levels 2, 3, and 4 add info, debug, and trace output. Logs are written under
`~/.config/kaleidux/logs/`.

For blank or broken video, compare `--video-backend ffmpeg`, `mpv`, and `appsink`
and retain the failing run's log. A forced backend exposes its error without
falling back to another backend.

`KLD_NATIVE_GL_SURFACE=1` and `KLD_MPV_RENDER_API=gl-overlay` select experimental
native presentation paths. The latter bypasses WGPU composition and shader
transitions. `KLD_MPV_GL_SYNC=finish` is a slower synchronization diagnostic,
not the default playback mode.
