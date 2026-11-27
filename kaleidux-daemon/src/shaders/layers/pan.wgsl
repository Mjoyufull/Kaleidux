// Move over the wallpaper as the normalized pointer changes.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let centre = vec2<f32>(0.5);
    let travel = 0.16 * CURSOR_STRENGTH * frame.cursor.z;
    let displacement = (frame.cursor.xy - centre) * travel;
    return sample(centre + (uv - centre) * (1.0 - travel) + displacement);
}
