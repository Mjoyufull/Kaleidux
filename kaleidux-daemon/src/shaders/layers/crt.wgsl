// Port of Chris's Hyprland CRT screen shader. Its contrast and mask operate in sRGB.
fn display_rgb(linear: vec3<f32>) -> vec3<f32> {
    return select(
        (1.055 * pow(max(linear, vec3<f32>(0.0)), vec3<f32>(1.0 / 2.4))) - 0.055,
        12.92 * linear,
        linear < vec3<f32>(0.0031308)
    );
}

fn linear_rgb(display: vec3<f32>) -> vec3<f32> {
    return select(
        pow((display + 0.055) / 1.055, vec3<f32>(2.4)),
        display / 12.92,
        display < vec3<f32>(0.04045)
    );
}

fn noise(p: vec2<f32>) -> f32 {
    let start = fract(p.xyx * 0.1031);
    let v = start + dot(start, start.yzx + 33.33);
    return fract((v.x + v.y) * v.z);
}

fn layer(uv: vec2<f32>) -> vec4<f32> {
    let centered = uv * 2.0 - 1.0;
    let offset = abs(centered.yx) / vec2<f32>(6.0, 4.0);
    let tc = (centered + centered * offset * offset) * 0.5 + 0.5;
    if any(tc < vec2<f32>(0.0)) || any(tc > vec2<f32>(1.0)) {
        return vec4<f32>(0.0, 0.0, 0.0, 1.0);
    }
    let dist = distance(tc, vec2<f32>(0.5));
    let shift = vec2<f32>(0.0015, 0.0) * dist;
    let rgb = display_rgb(vec3<f32>(sample(tc + shift).r, sample(tc).g, sample(tc - shift).b));
    let contrast = clamp((rgb - 0.5) * 1.1 + 0.5, vec3<f32>(0.0), vec3<f32>(1.0));
    let luma = dot(contrast, vec3<f32>(0.299, 0.587, 0.114));
    var color = mix(vec3<f32>(luma), contrast, 1.30);
    color += vec3<f32>(0.035) * (1.0 - smoothstep(0.0, 0.7, dist));
    let scan = pow(abs(sin(tc.y * 260.0 * 3.14159)), (1.0 - 0.4 * luma) * 1.5);
    let mask = 0.85 + 0.15 * sin(tc.x * frame.size.x * 0.5 * 3.14159);
    color *= mix(0.55, 1.0, scan) * mix(mask, 1.0, 0.6 + 0.4 * luma);
    color += vec3<f32>((noise(tc + frame.seconds) - 0.5) * 0.05);
    let vignette = 16.0 * tc.x * tc.y * (1.0 - tc.x) * (1.0 - tc.y);
    color *= pow(vignette, 0.15);
    let finished = clamp(pow(max(color, vec3<f32>(0.0)), vec3<f32>(0.90)) * 1.25,
                         vec3<f32>(0.0), vec3<f32>(1.0));
    return vec4<f32>(linear_rgb(finished), 1.0);
}
