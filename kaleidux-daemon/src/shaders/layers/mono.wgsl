// Luminance-preserving monochrome in linear light.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let src = sample(uv);
    return vec4<f32>(vec3<f32>(dot(src.rgb, vec3<f32>(0.2126, 0.7152, 0.0722))), src.a);
}
