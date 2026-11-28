// Reveal each pixel at a time supplied by the map's red channel.
// The green channel optionally accents pixels near the advancing edge.
struct Uniforms {
    progress: f32,
    screen_aspect: f32,
    from_aspect: f32,
    to_aspect: f32,
    params: array<vec4<f32>, 7>,
    placement: vec4<f32>,
}
@group(0) @binding(0) var<uniform> uniforms: Uniforms;
@group(0) @binding(1) var previous: texture_2d<f32>;
@group(0) @binding(2) var incoming: texture_2d<f32>;
@group(0) @binding(3) var linear: sampler;
@group(1) @binding(0) var reveal_map: texture_2d<f32>;

fn content_uv(uv: vec2<f32>, aspect: f32, tiled: f32, extent: vec2<u32>) -> vec2<f32> {
    if tiled > 0.5 {
        return fract(uv * uniforms.placement.xy / vec2<f32>(extent));
    }
    let scale = uniforms.screen_aspect / aspect;
    return (uv - vec2<f32>(0.5)) * vec2<f32>(min(1.0, scale), min(1.0, 1.0 / scale)) + vec2<f32>(0.5);
}

@fragment
fn fs_main(@location(0) uv: vec2<f32>) -> @location(0) vec4<f32> {
    let from_uv = content_uv(uv, uniforms.from_aspect, uniforms.placement.z, textureDimensions(previous));
    let to_uv = content_uv(uv, uniforms.to_aspect, uniforms.placement.w, textureDimensions(incoming));
    let map_size = vec2<f32>(textureDimensions(reveal_map));
    let map_uv = content_uv(uv, map_size.x / map_size.y, uniforms.placement.w, textureDimensions(incoming));
    let field = textureSample(reveal_map, linear, map_uv);
    let from_color = textureSample(previous, linear, from_uv);
    let to_color = textureSample(incoming, linear, to_uv);
    // A short delay at both ends guarantees exact source and destination frames.
    let time = uniforms.progress * 1.04 - 0.02;
    let switched = smoothstep(-0.012, 0.012, time - field.r);
    let edge = field.g * (1.0 - smoothstep(0.0, 0.018, abs(time - field.r)))
        * smoothstep(0.0, 0.1, uniforms.progress) * smoothstep(0.0, 0.1, 1.0 - uniforms.progress);
    let color = mix(from_color, to_color, switched);
    return vec4<f32>(mix(color.rgb, vec3<f32>(1.0), edge * 0.75), color.a);
}
