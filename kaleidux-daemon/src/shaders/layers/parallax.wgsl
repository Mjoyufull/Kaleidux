// Sample depth in the same wallpaper UVs. White moves toward the viewer.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let centre = vec2<f32>(0.5);
    let strength = CURSOR_STRENGTH * frame.cursor.z;
    let shifted = centre + (uv - centre) * (1.0 - 0.085 * strength);
    var depth = 1.0 - smoothstep(0.08, 0.7, length(uv - centre));
    if USE_DEPTH {
        let packed = textureSampleLevel(kld_depth, kld_sampler, shifted, 0.0).rg;
        depth = dot(packed, vec2<f32>(65280.0, 255.0)) / 65535.0;
    }
    let displacement = (frame.cursor.xy - centre) * ((depth - 0.5) * 0.14 * strength);
    return sample(shifted + displacement);
}
