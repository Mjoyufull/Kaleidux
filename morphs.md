# Wallpaper filters and morphs

Kaleidux draws the wallpaper and its transition first, then the filters, then
the morphs. Filters sample and replace the previous image. Morphs add colour and
alpha on top and work for labels, HUDs and other widgets.

```sh
kldctl filter mono crt
kldctl morph 'text=Kaleidux' heart
kldctl layers
kldctl filter -o DP-1
kldctl morph -o all
```

Each command replaces its stage on the main output by default. Use `-o all` to
apply it to each connected output or choose one by name. An empty list clears
that stage. Each stage supports eight layers. Changes are live and apply across
wallpaper selections. To restore them on daemon start, use Rhai in the existing
`script-path`:

```rhai
filters(["crt"], 0, "*");
morphs(["text=Kaleidux", "heart"], 0, "eDP-1");
```

`kldctl filter crt --fps 24` animates the CRT grain at up to 24 redraws per second
when wallpaper frames would otherwise be idle. The default 0 freezes shader
time, so a static wallpaper stays idle after the new layer is presented.
Existing video and transition renders also evaluate the layers. Moving layers
are disabled on powered-off outputs.

Loving the current wallpaper briefly shows a glass heart. Set `love-heart =
false` in `[global]` to disable it. `love-heart-position = [0.91, 0.88]` places its
center in output coordinates between 0 and 1. The built-in `heart` layer is a
persistent example. The `text=...` morph accepts 1–48 printable ASCII characters.
For custom typography, generate a shader from vector outlines or glyph bits.
The text bitmap is the public-domain VGA font from
[font8x8](https://github.com/dhepper/font8x8).

## WGSL layer ABI

Install a `.wgsl` file in `$XDG_CONFIG_HOME/kaleidux/filters/` or
`$XDG_CONFIG_HOME/kaleidux/morphs/` and select it by its basename. The CLI also
accepts a local `.wgsl` path. Rhai and direct IPC accept absolute paths or names.

A shader defines one function and any local utility functions:

```wgsl
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let pixel = uv * frame.size;
    let hit = pixel.x > 12.0 && pixel.x < 56.0 && pixel.y > 12.0 && pixel.y < 24.0;
    return vec4<f32>(0.12, 0.62, 1.0, select(0.0, 0.75, hit));
}
```

- `uv` spans the full output from the top left. `frame.size` gives physical pixels.
- `frame.seconds` measures time since installation for animated layers. It is zero for passive layers.
- `frame.progress` goes from 0 to 1 during the transition and is 1 in steady state.
- `frame.params` is reserved for built-ins and zero for custom layers.
- Filters can use `sample(uv)` to read linear RGBA from the previous pass.
- Morphs return linear RGB and straight alpha. Zero alpha leaves the backdrop visible.
- Keep entry points and resource bindings in the host. Each source has a 64 KiB limit.

The ABI is for fragments on the Kaleidux renderer. RetroArch `.slangp` presets
with history, LUTs, or independent pass scaling need ports. Only install shaders
you trust to finish on your GPU. Filters use one or two full-size RGBA
intermediates, about 32 MiB each at 3840×2160. Morphs use the existing output
attachment. The opt-in libmpv GL overlay bypasses this renderer and cannot use
these layers. FFmpeg uses the composed path while layers are active.

## IPC

Send `layers_set` with an output name or null, stage `filter` or `morph`, ordered
`layers` strings, and optional `fps`. `layers_query` returns the active order
and rates:

```json
{"method":"layers_set","params":{"output":"all","stage":"filter","layers":["mono","crt"],"fps":0}}
{"method":"layers_query","params":{"output":"all"}}
```
