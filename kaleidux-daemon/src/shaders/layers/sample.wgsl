// The previous stage's linear RGBA pixels. Mip zero and clamp to the output.
@group(0) @binding(1) var kld_input: texture_2d<f32>;
@group(0) @binding(2) var kld_sampler: sampler;
@group(0) @binding(3) var kld_depth: texture_2d<f32>;
fn sample(uv: vec2<f32>) -> vec4<f32> {
    return textureSampleLevel(kld_input, kld_sampler, uv, 0.0);
}
