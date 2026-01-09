struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
};

@vertex
fn vs_main(@builtin(vertex_index) in_vertex_index: u32) -> VertexOutput {
    var out: VertexOutput;
    // Full screen triangle
    var uv = vec2<f32>(0.0, 0.0);
    if (in_vertex_index == 0u) { uv = vec2<f32>(0.0, 2.0); }
    if (in_vertex_index == 1u) { uv = vec2<f32>(0.0, 0.0); }
    if (in_vertex_index == 2u) { uv = vec2<f32>(2.0, 0.0); }
    
    let pos = vec2<f32>(uv.x * 2.0 - 1.0, 1.0 - uv.y * 2.0);
    out.position = vec4<f32>(pos, 0.0, 1.0);
    out.uv = uv;
    return out;
}

@group(0) @binding(0) var t_diffuse: texture_2d<f32>;
@group(0) @binding(1) var s_diffuse: sampler;

@fragment
fn fs_main(in: VertexOutput) -> @location(0) vec4<f32> {
    return textureSample(t_diffuse, s_diffuse, in.uv);
}
