// Sample depth in the same wallpaper UVs. White moves toward the viewer.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let centre = clamp(frame.pointer.xy, vec2<f32>(0.0), vec2<f32>(1.0));
    let shifted = vec2<f32>(0.5) + (uv - vec2<f32>(0.5)) * 0.84;
    let packed = textureSampleLevel(kld_depth, kld_sampler, shifted, 0.0).rg;
    let depth = dot(packed, vec2<f32>(65280.0, 255.0)) / 65535.0;
    let move = (centre - vec2<f32>(0.5)) * (0.025 + depth * 0.13);
    return sample(shifted + move);
}
