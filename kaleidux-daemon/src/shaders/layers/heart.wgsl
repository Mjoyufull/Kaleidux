// Glass heart silhouette with a short pop when the wallpaper becomes loved.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let center = select(vec2<f32>(0.91, 0.88), frame.params.xy, frame.params.w > 0.0);
    let pulse = select(1.0, frame.progress, frame.params.w > 0.0);
    let loved = frame.params.w == 0.0 || frame.params.z > 0.0;
    let pixels = min(frame.size.x, frame.size.y) * 0.065;
    let growth = 0.56 + 0.44 * pow(max(0.0, sin(3.14159265 * pulse)), 0.6);
    let pop = growth * (1.0 + 0.21 * exp(-7.0 * pulse) * sin(pulse * 22.0));
    let p = (uv - center) * frame.size / (pixels * pop);
    if any(abs(p) > vec2<f32>(1.2)) {
        return vec4<f32>(0.0);
    }
    let q = vec2<f32>(p.x, -p.y + 0.10);
    let base = dot(q, q) - 0.69;
    let shape = base * base * base - q.x * q.x * q.y * q.y * q.y;
    let inside = 1.0 - smoothstep(-0.015, 0.018, shape);
    let ridge = exp(-abs(shape) * 20.0);
    let light = exp(-dot(q - vec2<f32>(-0.32, 0.53), q - vec2<f32>(-0.32, 0.53)) * 12.0);
    let shine = clamp(0.85 * light + 0.55 * ridge, 0.0, 1.0);
    let base_color = select(vec3<f32>(0.32, 0.44, 0.55), vec3<f32>(0.67, 0.025, 0.15), loved);
    let highlight = select(vec3<f32>(0.99, 1.0, 1.0), vec3<f32>(1.0, 0.68, 0.82), loved);
    let alpha = select(0.25 + 0.6 * shine, 0.88 + 0.1 * shine, loved);
    return vec4<f32>(mix(base_color, highlight, shine), inside * alpha);
}
