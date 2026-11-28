fn layer(uv: vec2<f32>) -> vec4<f32> {
    let flow = uv.x * 9.0 - frame.seconds * 0.25;
    let bank = 0.66 + sin(flow) * 0.07 + cos(flow * 0.62 + 0.4) * 0.08;
    let layers = 0.6 + 0.24 * sin(flow * 0.83 + uv.y * 33.0);
    let opacity = smoothstep(bank - 0.27, bank + 0.28, uv.y) * layers * 0.44;
    return vec4<f32>(0.71, 0.77, 0.83, opacity);
}
