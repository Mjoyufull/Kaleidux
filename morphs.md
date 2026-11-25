# Shaders and morphs

Kaleidux draws the wallpaper and its transhader first, then runs the shader
stack, then composites morphs on top. Shaders sample the previous pass.
Morphs add colour and alpha and can also draw with the wallpaper cleared.

```sh
kldctl shader mono crt
kldctl morph 'text=Kaleidux|font=Sans Bold 28|rect=0.03,0.03,0.25,0.07' heart
kldctl morph 'image=/home/me/art.png|rect=0.05,0.7,0.14,0.25' -o DP-1
kldctl layers -o all
kldctl clear
kldctl shader -o DP-1
kldctl morph -o all
```

Commands target the main output by default. Shaders can hold up to eight
passes. An output can have up to 64 morphs sharing a 96 MiB texture budget.
Use `--fps 0` for passive layers that only redraw on
input or wallpaper changes. A positive rate requests periodic redraws.
`--fps unlimited` follows display callbacks on Wayland. `pause` freezes
shader clocks and `resume` continues from the held time.

The bottom-right glass heart marks the current wallpaper as a favorite.
Click to toggle it. When loved, it plays the animation and settles pink.
The animation also plays on arrival at a loved wallpaper by default.
Use `love-heart`, `love-heart-position` and `love-heart-animate-on-show` in
`[global]` to configure it. `kldctl love` and `kldctl unlove` update it too.

Use `rect=x,y,width,height` to place a morph in normalized output
coordinates. Shader morphs receive local UVs. Use `font=` for a Pango system
font description and `colour=#RRGGBB` for text colour. System fonts and
Unicode text use `pango-view`. Simple ASCII labels can use the built-in VGA
bitmap from [font8x8](https://github.com/dhepper/font8x8).

Use `--group NAME` to update a widget independently. Groups draw in
alphabetical order above the default stack. Run `kldctl morph --group NAME`
to clear one group. A layout arranges the supplied layers in order. Explicit
`rect=` values override their cells. Layouts accept `row`, `column` or
`grid:COLUMNS`, with optional normalized `--bounds` and `--gap`.

On Wayland and X11, `action=` binds primary clicks, `secondary=` binds right
clicks, and `scroll-up=` and `scroll-down=` bind vertical scroll. A binding
uses its morph's rectangle or layout cell. Overlapping buttons use the topmost
matching morph. A press and release must land inside the same button.
`kldctl morph-watch` emits the action, gesture, output and local coordinates
as JSON lines.

```sh
kldctl morph --group controls --layout row --bounds 0.05,0.8,0.3,0.07 \
    'text=Previous|action=previous' 'text=Next|action=next|secondary=menu'
kldctl morph-watch -o DP-1
```

`morph_group(json)` in Rhai accepts a layer selection with `stage` set to
`"morph"`, a `group` name and the `layers` array. Its optional `layout` has
`columns`, `bounds` and `gap`. Zero columns uses a row and one uses a column.
Use the existing `on_morph_action(action, output, x, y)` hook or
`morph-watch` to act on your bindings.

## Wallpaper rules

```toml
[[global.layer-rules]]
path = "~/Pictures/night"
shaders = ["crt", "pan"]
morphs = ['text=Night|font=Sans 26|rect=0.03,0.03,0.18,0.07']
fps = 0

[[global.layer-rules]]
path = "~/Pictures/night/city.png"
shaders = ["mono"]
```

The most specific matching path wins for each stage. An empty stack in a
rule suppresses that stage. The manual stack comes back when the wallpaper
leaves the rule.

A standalone `.kldscene.toml` can use a `background` and up to eight
ordered layers in total. Its paths are relative to the scene. Set
`layers` with ordinary morph strings, bitmaps or shader paths. The
background can be omitted. See [reactive.kldscene.toml](./examples/scenes/reactive.kldscene.toml).

On Wayland, `layer = "top"` or `layer = "overlay"` in an output section
lets a morph-only surface show the desktop through its transparent areas.
Leave that output's wallpaper path unset or clear it with `kldctl clear`.

Use `kldctl workshop PROJECT` for an installed Workshop project, and
`kldctl set scene.json` for an unpacked scene. Scene image layers support
position, rotation, scale, opacity and colour tint. The scene reader
supports local bitmap and TEX assets and the basic image material chain.

## Shader files

Use local `.wgsl`, `.frag`, `.glsl`, `.fs` or `.fsh` files. The
`set` and `shader-wallpaper` commands can display a shader on its own.
Named WGSL effects live in `$XDG_CONFIG_HOME/kaleidux/shaders/` and morphs
in `$XDG_CONFIG_HOME/kaleidux/morphs/`.

```wgsl
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let hit = distance(uv, frame.pointer.xy) < 0.06;
    return vec4<f32>(0.12, 0.62, 1.0, select(0.0, 0.75, hit));
}
```

- `uv` starts at the top left. `frame.size` is the physical output size.
- `frame.seconds` is the animation time. `frame.timing.xy` holds the delta and frame number.
- `frame.progress` is the transhader position, from 0 to 1. It is 1 in steady state.
- `frame.pointer` holds x, y and primary and secondary button states. `frame.data` holds four provider-defined numbers.
- `control(0u)` through `control(31u)` read 32 additional values. Update them together with `kldctl react '{"controls":[0.2,0.6]}'`. Missing elements in a supplied array reset to zero.
- `frame.date` holds the year, month, day and seconds since local midnight. `audio_band(0u)` through `audio_band(63u)` read the spectrum.
- A shader in the effects stack can read the previous image with `sample(uv)`. Morphs return linear RGB and straight alpha.

GLSL accepts a fragment `main()` or Shadertoy's `mainImage()`. Shadertoy
inputs include `iResolution`, `iTime`, `iTimeDelta`, `iFrame`, `iFrameRate`,
`iMouse`, and `iDate`. On the effects stack, `iChannel0` is the previous
image. Shader wallpapers and GLSL morphs can bind four local image
channels. Paths in `channel0=` through `channel3=` are relative to the shader
file or absolute. `kldPointer`, `kldData`, `kldAudio(index)` and
`kldControl(index)` are available
in both modes.

`kldctl set` also recognizes screen shaders that declare `uniform sampler2D tex`
and runs them over the active wallpaper. The adapter provides `tex`,
`v_texcoord`, `time` and `full_size`. Set an explicit animation rate with
`kldctl shader --fps unlimited /path/to/shader.frag` when the shader uses time.

```sh
kldctl shader-wallpaper 'desktop.frag|channel0=sky.png|channel1=clouds.png'
kldctl morph 'badge.frag|channel2=/home/me/art.png|rect=0.04,0.08,0.12,0.12'
```

Save a Shadertoy JSON export with inline pass code as `name.shadertoy.json`.
Kaleidux runs Common, Buffer A through D, and Image in that order.
A buffer channel reads the current frame after its producer has run.
Self-reads and reads from later buffers use the previous frame.
Copy image assets next to the export and use relative `filepath` entries
or mirror the export's `media/` paths. Channels support 2D images and
buffers with nearest or linear sampling and clamp or repeat wrapping.
At large resolutions the buffer targets downscale together to fit a
192 MiB budget. Image still renders at output resolution.

```sh
kldctl shader-wallpaper /path/to/scene.shadertoy.json --fps 60
```

Use `kldctl shader /path/to/preset.slangp` for RetroArch Slang presets.
Keep the preset's referenced shaders and images in place. Set parameter
overrides in the preset file. Pass scaling, history, feedback, lookup
textures, and includes use librashader. The output is SDR. Presets requiring
more than 512 MiB of render targets at the active output size fall back
to the unfiltered image. The full and Wayland builds include the
`retroarch-shaders` feature.

Use `kldctl shader /path/to/preset.glslp` for same-size Libretro GLSL
passes. The fragment ABI includes `Texture`, `TextureSize`, `InputSize`,
`OutputSize`, `FrameCount` and a single UV varying. Presets with custom
vertex math, LUTs, feedback, scaling or parameter overrides
need a port. If you use the diagnostic native mpv GL overlay, enable your
layers before setting the video so it starts with composed GL. The glass
heart has its own Wayland surface and also works with the native overlay.

## Input providers and player morph

```sh
kldctl shader 'parallax=/home/me/depth.png|pointer=true'
kldctl shader 'pan|pointer=true'
kldctl react '{"pointer":[0.7,0.4,0,0],"data":[1,0,0,0]}'
kldctl morph spectrum
kldctl morph --group visualizer 'audio-orbit|rect=0.1,0.64,0.19,0.3' --fps unlimited
kldctl morph --group status 'pulse|rect=0.8,0.7,0.1,0.18' --fps 60
python3 examples/reactivity.py --audio -o DP-1
python3 examples/reactivity.py --pointer -o DP-1
python3 examples/player-morph.py --font 'Sans 24'
python3 examples/player-morph.py play-pause
```

`pan` shifts a zoomed wallpaper with the pointer. `parallax=PATH` uses a
16-bit grayscale depth image when one is available. Use `pointer=true` to
receive local pointer events over the layer's rectangle or the whole
wallpaper when it has no rectangle. `kldctl morph 'pointer|rect=0.2,0.2,0.6,0.6'`
sets an input region without drawing artwork. The example provider can
get desktop audio from a PulseAudio or PipeWire monitor via GStreamer.
Other applications can send input through IPC.

Set `desktop-audio = true` in `[global]` to use the desktop audio
monitor directly. Optional `desktop-audio-source` selects a PulseAudio
or PipeWire Pulse source. The default is `@DEFAULT_MONITOR@`.
Capture runs while a visible shader reads spectrum data or a local HTML
wallpaper is playing. It stops when playback is paused or those
wallpapers are cleared. The external provider works without that setting.

Local HTML wallpapers get `wallpaperRegisterAudioListener` and the
`wallpaperPropertyListener` callbacks for FPS, initial properties and
pause. The optional `project.json` beside the page supplies the initial
properties. `kldctl react` sends the same pointer and audio data used by
shaders. The audio bridge supplies 128 values by repeating the 64 bands
for each channel.

The player example reads MPRIS with `playerctl` and local album art.
Pass `--lyrics` with your own timed LRC file to show lyrics. It owns the
`player` group by default. Adjust `--group`, `--rect`, `--font`, `--colour`,
`--dim`, and `--art` for your desktop.
Its `previous`, `play-pause` and `next` buttons use morph actions. Those
actions are also available as commands for compositor bindings.

## Weather effects

Use `rain`, `snow`, `fog` and `lightning` independently or together. A
local provider can call `examples/weather-morph.py` with its current
condition. Supply your own condition-to-layer mapping with `--mapping` or
use `morph --group weather` directly with any custom shaders.

```sh
python3 examples/weather-morph.py 'light rain'
kldctl morph --group weather snow --fps 30
python3 examples/weather-morph.py clear
```

`--mapping` takes a TOML file with a `[conditions]` table. Each condition
maps to an array of ordered morphs. For example `rain = ["rain", "fog"]`.

## Smooth video loops

Set `smooth-loop = { duration-ms = 450 }` in `[global]`, `[any]`, or an
output section to blend the last displayed frame into the next pass of
the video. Add `transition = "fade"` to choose a different transhader.
Set `reveal-map = "path/to/map.png"` to use a reveal map at the seam. Relative
paths start in the config directory.
Use `duration-ms = 0` to disable an inherited setting. Loop blends keep
the current slideshow position and duration. This uses the composed
libmpv path when libmpv renders the wallpaper.

## Rhai and IPC

```rhai
shaders(["crt"], 0, "*");
morphs(["text=Kaleidux", "heart"], -1, "eDP-1");
react("{\"pointer\":[0.7,0.4,0,0],\"output\":\"eDP-1\"}");

fn on_morph_action(action, output, x, y) {
    if action == "next" { next(output); }
}
```

Use `layers_set`, `layers_query` and `layer_inputs` over the Unix socket.
Omitted input values retain their previous values. Rates use 0 for passive
layers and 65535 for unlimited animation.

```json
{"method":"layers_set","params":{"output":"all","stage":"shader","layers":["mono","crt"],"fps":0}}
{"method":"layer_inputs","params":{"output":"eDP-1","audio":[0.1,0.2],"data":[1,0,0,0]}}
{"method":"layers_query","params":{"output":"all"}}
{"method":"morph_watch","params":{"output":"DP-1"}}
```

The watch connection replies with `"Ok"` and then one JSON event per line
until it closes. Omit `output` to follow all outputs. For example, an event
can be `{"output":"DP-1","action":"next","gesture":"primary","position":[0.5,0.25]}`.
