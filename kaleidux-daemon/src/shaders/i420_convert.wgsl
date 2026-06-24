struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
};

@vertex
fn vs_main(@builtin(vertex_index) idx: u32) -> VertexOutput {
    var out: VertexOutput;
    var uv = vec2<f32>(0.0, 0.0);
    if (idx == 0u) { uv = vec2<f32>(0.0, 2.0); }
    if (idx == 1u) { uv = vec2<f32>(0.0, 0.0); }
    if (idx == 2u) { uv = vec2<f32>(2.0, 0.0); }
    let pos = vec2<f32>(uv.x * 2.0 - 1.0, 1.0 - uv.y * 2.0);
    out.position = vec4<f32>(pos, 0.0, 1.0);
    out.uv = uv;
    return out;
}

@group(0) @binding(0) var t_y: texture_2d<f32>;
@group(0) @binding(1) var t_u: texture_2d<f32>;
@group(0) @binding(2) var t_v: texture_2d<f32>;
@group(0) @binding(3) var samp: sampler;

fn bt1886_eotf(c: f32) -> f32 {
    return pow(max(c, 0.0), 2.4);
}

@fragment
fn fs_main(in: VertexOutput) -> @location(0) vec4<f32> {
    let y_raw = textureSample(t_y, samp, in.uv).r;
    let u_raw = textureSample(t_u, samp, in.uv).r;
    let v_raw = textureSample(t_v, samp, in.uv).r;

    let y = (y_raw * 255.0 - 16.0) / 219.0;
    let u = (u_raw * 255.0 - 128.0) / 224.0;
    let v = (v_raw * 255.0 - 128.0) / 224.0;

    let r = clamp(y + 1.5748 * v, 0.0, 1.0);
    let g = clamp(y - 0.1873 * u - 0.4681 * v, 0.0, 1.0);
    let b = clamp(y + 1.8556 * u, 0.0, 1.0);

    return vec4<f32>(bt1886_eotf(r), bt1886_eotf(g), bt1886_eotf(b), 1.0);
}
