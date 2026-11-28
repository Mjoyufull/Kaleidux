// Shared layer inputs. Texture UV starts at the upper left of the output.
struct LayerFrame {
    size: vec2<f32>,
    seconds: f32,
    progress: f32,
    params: vec4<f32>,
    timing: vec4<f32>,
    pointer: vec4<f32>,
    data: vec4<f32>,
    date: vec4<f32>,
    audio: array<vec4<f32>, 16>,
    controls: array<vec4<f32>, 8>,
    cursor: vec4<f32>,
}

@group(0) @binding(0) var<uniform> frame: LayerFrame;

fn audio_band(index: u32) -> f32 {
    let band = min(index, 63u);
    return frame.audio[band / 4u][band % 4u];
}

fn control(index: u32) -> f32 {
    let channel = min(index, 31u);
    return frame.controls[channel / 4u][channel % 4u];
}

struct Vertex {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
}

@vertex
fn kld_vertex(@builtin(vertex_index) index: u32) -> Vertex {
    var result: Vertex;
    let uv = vec2<f32>(f32((index << 1u) & 2u), f32(index & 2u));
    result.position = vec4<f32>(uv * vec2<f32>(2.0, -2.0) + vec2<f32>(-1.0, 1.0), 0.0, 1.0);
    result.uv = uv;
    return result;
}

@fragment
fn kld_fragment(input: Vertex) -> @location(0) vec4<f32> {
    return layer(input.uv);
}
