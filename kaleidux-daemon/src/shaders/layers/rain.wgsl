fn layer(uv: vec2<f32>) -> vec4<f32> {
    let p = vec2<f32>((uv.x + uv.y * 0.16) * frame.size.x / 36.0, uv.y * frame.size.y / 50.0);
    let column = floor(p.x);
    let seed = fract(sin(column * 173.7) * 43758.5);
    let y = p.y - frame.seconds * mix(12.0, 28.0, seed) + seed * 77.0;
    let x = abs(fract(p.x) - mix(0.17, 0.83, seed));
    let streak = (1.0 - smoothstep(0.014, 0.043, x)) * smoothstep(0.75, 0.99, fract(y));
    return vec4<f32>(0.60, 0.82, 1.0, streak * mix(0.25, 0.58, seed));
}
