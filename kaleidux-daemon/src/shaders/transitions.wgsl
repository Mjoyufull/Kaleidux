// Transitions WGSL Compute Shader
// This shader handles mixing two textures based on various transition effects.

struct TransitionUniforms {
    progress: f32,
    transition_type: u32, // 0: Fade, 1: Wipe, 2: Grow, 3: Outer, 4: Wave
    width: u32,
    height: u32,
    angle: f32, // For Wipe/Wave
    pos_x: f32, // For Grow/Outer
    pos_y: f32, // For Grow/Outer
    step: f32,  // For quantization if needed
};

@group(0) @binding(0) var<uniform> uniforms: TransitionUniforms;
@group(0) @binding(1) var prev_tex: texture_2d<f32>;
@group(0) @binding(2) var next_tex: texture_2d<f32>;
@group(0) @binding(3) var out_tex: texture_storage_2d<rgba8unorm, write>;
@group(0) @binding(4) var sampler_linear: sampler;

@compute @workgroup_size(16, 16)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let x = global_id.x;
    let y = global_id.y;

    if (x >= uniforms.width || y >= uniforms.height) {
        return;
    }

    let uv = vec2<f32>(f32(x) / f32(uniforms.width), f32(y) / f32(uniforms.height));
    let prev_color = textureSampleLevel(prev_tex, sampler_linear, uv, 0.0);
    let next_color = textureSampleLevel(next_tex, sampler_linear, uv, 0.0);

    var final_color = prev_color;
    let p = uniforms.progress;

    switch (uniforms.transition_type) {
        case 0u: { // Fade
            final_color = mix(prev_color, next_color, p);
        }
        case 1u: { // Wipe
            // Equation from swww: (x-h)*cos(a) + (y-k)*sin(a) + C = r^2
            // Simplification: dot(uv - center, dir) + p > offset
            let angle_rad = uniforms.angle;
            let dir = vec2<f32>(cos(angle_rad), sin(angle_rad));
            let center = vec2<f32>(0.5, 0.5);
            let dist = dot(uv - center, dir);
            
            // Map progress to the range of possible dot products
            // Max dist is diagonal / 2 = sqrt(2)/2
            let spread = 1.5; // Enough to cover corners
            if (dist + (p * spread - (spread / 2.0)) > 0.0) {
                final_color = next_color;
            }
        }
        case 2u: { // Grow
            let center = vec2<f32>(uniforms.pos_x, uniforms.pos_y);
            let dist = distance(uv, center);
            let max_dist = 1.5; // Cover whole screen
            if (dist < p * max_dist) {
                final_color = next_color;
            }
        }
        case 3u: { // Outer
            let center = vec2<f32>(uniforms.pos_x, uniforms.pos_y);
            let dist = distance(uv, center);
            let max_dist = 1.5;
            if (dist > (1.0 - p) * max_dist) {
                final_color = next_color;
            }
        }
        case 4u: { // Wave
            let angle_rad = uniforms.angle;
            let dir = vec2<f32>(cos(angle_rad), sin(angle_rad));
            let ortho_dir = vec2<f32>(-dir.y, dir.x);
            let center = vec2<f32>(0.5, 0.5);
            
            let dist_along = dot(uv - center, dir);
            let dist_perp = dot(uv - center, ortho_dir);
            
            // Add sine wave distortion
            let wave_offset = sin(dist_perp * 20.0) * 0.05;
            let spread = 1.5;
            if (dist_along + wave_offset + (p * spread - (spread / 2.0)) > 0.0) {
                final_color = next_color;
            }
        }
        case 5u: { // Pixelate
            let squares_min = 20.0; // Min squares
            let steps = 50.0;
            let d = min(p, 1.0 - p);
            let dist = steps * (1.0 - d);
            let size = squares_min + dist;
            
            let block_uv = floor(uv * size) / size;
            let p_col = textureSampleLevel(prev_tex, sampler_linear, block_uv, 0.0);
            let n_col = textureSampleLevel(next_tex, sampler_linear, block_uv, 0.0);
            final_color = mix(p_col, n_col, p);
        }
        case 6u: { // Slide
            let angle_rad = uniforms.angle;
            let dir = vec2<f32>(cos(angle_rad), sin(angle_rad));
            // Slide next_tex in from direction
            // p=0 -> offset=1 (far away)
            // p=1 -> offset=0 (centered)
            let offset = dir * (1.0 - p);
            
            // We want to sample previous image at uv + offset (slide out) ?
            // Or slide Next image OVER Previous?
            // Let's slide Next Over Prev.
            // Next image is at uv - offset?
            // No, standard slide: prev slides out, next slides in.
            
            // Simple Slide Left (if angle=0):
            // final = uv.x < p ? next(uv.x + (1-p)) : prev(uv.x - p)
            // This is complex for arbitrary angle.
            
            // Simpler: Slide Over. Next slides on top.
            // Next image coordinate: uv - (1.0 - p) * dir ?
            // If dir is (1,0) [Right]. Next comes from left?
            // If p=0, sample at uv - (1,0). Out of bounds.
            // If p=1, sample at uv.
            
            let next_uv = uv - (dir * (1.0 - p));
            if (next_uv.x >= 0.0 && next_uv.x <= 1.0 && next_uv.y >= 0.0 && next_uv.y <= 1.0) {
                 final_color = textureSampleLevel(next_tex, sampler_linear, next_uv, 0.0);
            } else {
                 final_color = prev_color;
            }
        }
        default: {
            final_color = next_color;
        }
    }

    textureStore(out_tex, vec2<i32>(i32(x), i32(y)), final_color);
}
