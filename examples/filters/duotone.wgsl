// Map wallpaper luminance to a blue shadow and a warm highlight.
fn layer(uv: vec2<f32>) -> vec4<f32> {
    let source = sample(uv);
    let luma = dot(source.rgb, vec3<f32>(0.2126, 0.7152, 0.0722));
    let shadow = vec3<f32>(0.012, 0.020, 0.075);
    let highlight = vec3<f32>(1.0, 0.70, 0.35);
    return vec4<f32>(mix(shadow, highlight, smoothstep(0.0, 1.0, luma)), source.a);
}
