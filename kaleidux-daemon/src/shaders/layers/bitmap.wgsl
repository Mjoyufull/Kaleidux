// A prepared image or a label from the system font renderer.
@group(0) @binding(1) var kld_bitmap: texture_2d<f32>;
@group(0) @binding(2) var kld_bitmap_sampler: sampler;

fn layer(uv: vec2<f32>) -> vec4<f32> {
    return textureSampleLevel(kld_bitmap, kld_bitmap_sampler, uv, 0.0);
}
