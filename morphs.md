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

Each command replaces its stage on the main output by default. A stage can
hold up to eight passes. Use `--fps 0` for passive layers that only redraw on
input or wallpaper changes. A positive rate requests periodic redraws.
`--fps unlimited` follows display callbacks on Wayland. `pause` freezes
shader clocks and `resume` continues from the held time.

The built-in `heart` morph is persistent. `kldctl love` adds the selected
wallpaper to the love list and briefly shows a glass heart by default.
Configure `love-heart` and `love-heart-position` in `[global]`.

Use `rect=x,y,width,height` to place a morph in normalized output
coordinates. Shader morphs receive local UVs. Use `font=` for a Pango system
font description and `colour=#RRGGBB` for text colour. System fonts and
Unicode text use `pango-view`. Simple ASCII labels can use the built-in VGA
bitmap from [font8x8](https://github.com/dhepper/font8x8).

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
- `frame.date` holds the year, month, day and seconds since local midnight. `audio_band(0u)` through `audio_band(63u)` read the spectrum.
- A shader in the effects stack can read the previous image with `sample(uv)`. Morphs return linear RGB and straight alpha.

GLSL accepts a fragment `main()` or Shadertoy's `mainImage()`. Shadertoy
inputs include `iResolution`, `iTime`, `iTimeDelta`, `iFrame`, `iFrameRate`,
`iMouse`, and `iDate`. On the effects stack, `iChannel0` is the previous
image. Shader wallpapers and GLSL morphs can bind four local image
channels. Paths in `channel0=` through `channel3=` are relative to the shader
file or absolute. `kldPointer`, `kldData`, and `kldAudio(index)` are available
in both modes. Shadertoy Buffer passes and feedback need ports.

```sh
kldctl shader-wallpaper 'desktop.frag|channel0=sky.png|channel1=clouds.png'
kldctl morph 'badge.frag|channel2=/home/me/art.png|rect=0.04,0.08,0.12,0.12'
```

Use `kldctl shader /path/to/preset.glslp` for same-size Libretro GLSL
passes. The fragment ABI includes `Texture`, `TextureSize`, `InputSize`,
`OutputSize`, `FrameCount` and a single UV varying. Presets with custom
vertex math, LUTs, feedback, scaling, `.slangp` or parameter overrides
need a port. The opt-in native mpv GL overlay bypasses the layer renderer.

## Input providers and player morph

```sh
kldctl shader 'parallax=/home/me/depth.png'
kldctl react '{"pointer":[0.7,0.4,0,0],"data":[1,0,0,0]}'
kldctl morph spectrum
python3 examples/reactivity.py --audio -o DP-1
python3 examples/reactivity.py --pointer -o DP-1
python3 examples/player-morph.py --lyrics song.lrc --font 'Sans 24'
python3 examples/player-morph.py play-pause
```

`pan` shifts a zoomed wallpaper with the pointer. `parallax=PATH` uses a
16-bit grayscale depth image when one is available. Both work from the
same pointer input. The example provider can get desktop audio from a
PulseAudio or PipeWire monitor via GStreamer, and pointer position from
Hyprland. Other applications can send input through IPC.

The player example reads MPRIS with `playerctl`, local album art, and a
timed LRC file. It owns a single morph stack while running. Adjust
`--rect`, `--font`, `--colour`, `--dim`, and `--art` for your desktop.
Bind its `previous`, `play-pause` and `next` actions with your compositor.
The panel surface is click-through. For more controls, run the same
commands from a shell or another widget host.

## Rhai and IPC

```rhai
shaders(["crt"], 0, "*");
morphs(["text=Kaleidux", "heart"], -1, "eDP-1");
react("{\"pointer\":[0.7,0.4,0,0],\"output\":\"eDP-1\"}");
```

Use `layers_set`, `layers_query` and `layer_inputs` over the Unix socket.
Omitted input values retain their previous values. Rates use 0 for passive
layers and 65535 for unlimited animation.

```json
{"method":"layers_set","params":{"output":"all","stage":"shader","layers":["mono","crt"],"fps":0}}
{"method":"layer_inputs","params":{"output":"eDP-1","audio":[0.1,0.2],"data":[1,0,0,0]}}
{"method":"layers_query","params":{"output":"all"}}
```
