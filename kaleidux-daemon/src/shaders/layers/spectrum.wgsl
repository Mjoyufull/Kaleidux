// Show incoming low-frequency energy as a highlight at the bottom.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let band = u32(clamp(uv.x, 0.0, 0.999) * 64.0);
    let level = audio_band(band);
    let top = 1.0 - level * 0.22;
    let alpha = select(0.0, 0.65, uv.y >= top);
    return vec4<f32>(0.15, 0.7, 1.0, alpha);
}
