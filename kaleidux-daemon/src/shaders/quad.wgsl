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

struct TransitionUniforms {
    progress: f32,
    screen_aspect: f32,
    prev_aspect: f32,
    next_aspect: f32,
    // We don't need params here, but struct alignment requires us to respect it
    // if we use the same buffer. However, in WGSL we can just define the relevant part 
    // if we are careful, or define the full struct.
    // Let's define full struct for safety or just use the fields we need if wgpu allows partial.
    // It's safer to define a matching struct layout.
    // params is array<vec4<f32>, 7>.
    // But WGSL array stride rules apply (16 bytes). vec4 is 16 bytes.
    params: array<vec4<f32>, 7>,
}

@group(0) @binding(0) var<uniform> uniforms: TransitionUniforms;
@group(0) @binding(1) var t_diffuse: texture_2d<f32>;
@group(0) @binding(2) var s_diffuse: sampler;

fn cover(uv: vec2<f32>, screen_ratio: f32, content_ratio: f32) -> vec2<f32> {
    let scale = screen_ratio / content_ratio;
    if (scale > 1.0) {
        // Content is "taller" relative to screen (or screen is wider).
        // We match Width. We crop Height (Top/Bottom).
        // scale > 1, so dividing by scale (< 1) shrinks range (zooms in).
        return vec2<f32>(uv.x, (uv.y - 0.5) / scale + 0.5);
    } else {
        // Content is "wider" relative to screen.
        // We match Height. We crop Width (Sides).
        // scale < 1, so multiplying by scale (< 1) shrinks range (zooms in).
        return vec2<f32>((uv.x - 0.5) * scale + 0.5, uv.y);
    }
}

@fragment
fn fs_blit(in: VertexOutput) -> @location(0) vec4<f32> {
    // Determine which aspect ratio to use.
    // If we are in blit pass, we are likely rendering the "current" texture (next),
    // OR the composition texture.
    // Composition texture matches screen aspect, so ratio = screen_aspect.
    // Current texture has next_aspect.
    // How do we know?
    // If progress < 1.0, we are blitting composition result. 
    // BUT we didn't update uniforms specifically for the BLIT pass from composition.
    // We updated uniforms in `render` method:
    // If progress < 1.0 (Transitioning):
    //    Uniforms updated with prev/next aspect.
    //    But we blit composition texture which IS screen size.
    //    If we use `cover` on composition texture (screen aspect) with `next_aspect` (content), it will WRONG.
    //    We need to disable cover if blitting composition.
    //    We can check `progress`.
    // If progress >= 1.0:
    //    Uniforms updated with next_aspect.
    //    We blit current texture. Cover needed.
    
    var uv = in.uv;
    if (uniforms.progress >= 1.0) {
        // Blitting raw content (image/video), apply cover
        uv = cover(uv, uniforms.screen_aspect, uniforms.next_aspect);
    } 
    // Else: Blitting composition texture. UV 0..1 maps 1:1. No cover needed.
    // Note: Transition pass handles cover logic internally via glsl prelude.
    
    return textureSample(t_diffuse, s_diffuse, uv);
}
