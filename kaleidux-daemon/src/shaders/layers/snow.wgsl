fn flake(uv: vec2<f32>, scale: f32, speed: f32) -> f32 {
    let p = uv * frame.size / scale + vec2<f32>(sin(uv.y * 12.0 + frame.seconds * 0.6) * 0.25, -frame.seconds * speed);
    let id = floor(p);
    let seed = fract(sin(dot(id, vec2<f32>(17.3, 89.7))) * 43758.5);
    let at = 0.22 + 0.56 * vec2<f32>(seed, fract(seed * 31.7));
    return (1.0 - smoothstep(0.7, 2.0, length(fract(p) - at) * scale)) * (0.25 + seed * 0.55);
}

fn layer(uv: vec2<f32>) -> vec4<f32> {
    return vec4<f32>(0.88, 0.95, 1.0, max(flake(uv, 55.0, 0.65), flake(uv, 83.0, 0.52)));
}
