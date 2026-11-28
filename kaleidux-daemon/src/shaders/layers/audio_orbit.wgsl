fn layer(uv: vec2<f32>) -> vec4<f32> {
    let p = uv - vec2<f32>(0.5);
    let angle = atan2(p.y, p.x);
    let band = u32(63.0 * abs(angle) / 3.141593);
    let level = sqrt(max(audio_band(band), 0.0));
    let bass = sqrt(max(audio_band(1u), 0.0));
    let r = length(p);
    let wave = abs(r - 0.21 - bass * 0.045 - level * 0.17);
    let core = 1.0 - smoothstep(0.005, 0.014, wave);
    let glow = exp(-wave * 54.0) * (0.12 + level * 0.36);
    let mix_factor = 0.5 + 0.5 * cos(angle * 2.0 - frame.seconds);
    let colour = mix(vec3<f32>(0.14, 0.84, 1.0), vec3<f32>(1.0, 0.25, 0.63), mix_factor);
    return vec4<f32>(colour, clamp(core * 0.82 + glow, 0.0, 1.0));
}
