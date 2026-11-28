fn layer(uv: vec2<f32>) -> vec4<f32> {
    let radius = 0.25 + 0.08 * sin(frame.seconds * 2.0);
    let alpha = 1.0 - smoothstep(radius, radius + 0.01, distance(uv, vec2<f32>(0.5)));
    return vec4<f32>(1.0, 0.3, 0.6, alpha);
}
