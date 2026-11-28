fn layer(uv: vec2<f32>) -> vec4<f32> {
    let cycle = floor(frame.seconds / 9.0);
    let t = frame.seconds % 9.0;
    let seed = fract(sin(cycle * 13.7 + 1.9) * 43758.5);
    let centre = mix(0.18, 0.82, seed);
    let jagged = centre + sin(uv.y * 29.0) * 0.03 + cos(uv.y * 67.0) * 0.019;
    let flash = exp(-t * 26.0) + exp(-abs(t - 0.19) * 44.0) * 0.7;
    let bolt = (1.0 - smoothstep(0.0008, 0.004, abs(uv.x - jagged))) * (1.0 - uv.y);
    return vec4<f32>(0.75, 0.85, 1.0, flash * (0.22 + bolt * 0.76));
}
