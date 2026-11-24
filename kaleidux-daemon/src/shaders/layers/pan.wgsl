// Move over the wallpaper as the normalized pointer changes.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let centre = clamp(frame.pointer.xy, vec2<f32>(0.0), vec2<f32>(1.0));
    let displacement = (centre - vec2<f32>(0.5)) * 0.10;
    return sample(vec2<f32>(0.5) + (uv - vec2<f32>(0.5)) * 0.89 + displacement);
}
