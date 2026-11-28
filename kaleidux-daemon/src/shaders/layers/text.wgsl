// glyphs and glyph_width are generated for the selected printable ASCII label.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let scale = max(1.0, min(floor(frame.size.x / (f32(glyph_width) + 16.0)),
                             round(frame.size.y / 360.0)));
    let pixel = uv * frame.size / scale - vec2<f32>(12.0);
    if any(pixel < vec2<f32>(-4.0)) || pixel.x >= f32(glyph_width) + 4.0 || pixel.y >= 12.0 {
        return vec4<f32>(0.0);
    }
    if any(pixel < vec2<f32>(0.0)) || pixel.x >= f32(glyph_width) || pixel.y >= 8.0 {
        return vec4<f32>(0.015, 0.017, 0.023, 0.70);
    }
    let point = vec2<u32>(pixel);
    let rows = glyphs[point.x / 8u];
    let word = select(rows.x, rows.y, point.y >= 4u);
    let bit = ((point.y % 4u) * 8u) + (point.x % 8u);
    let ink = 1u & (word >> bit);
    if ink != 0u {
        return vec4<f32>(0.96, 0.97, 1.0, 1.0);
    }
    return vec4<f32>(0.015, 0.017, 0.023, 0.70);
}
