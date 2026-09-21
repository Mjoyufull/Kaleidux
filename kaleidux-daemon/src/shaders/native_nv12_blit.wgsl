struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
};

struct YuvUniforms {
    geometry_and_range: vec4<f32>,
    chroma_and_transfer: vec4<f32>,
    matrix_r: vec4<f32>,
    matrix_g: vec4<f32>,
    matrix_b: vec4<f32>,
    crop_rect: vec4<f32>,
    sampling_geometry: vec4<f32>,
};

@group(0) @binding(0) var<uniform> uniforms: YuvUniforms;
@group(0) @binding(1) var t_y: texture_2d<f32>;
@group(0) @binding(2) var t_uv: texture_2d<f32>;
@group(0) @binding(3) var samp: sampler;

@vertex
fn vs_main(@builtin(vertex_index) index: u32) -> VertexOutput {
    var out: VertexOutput;
    var uv = vec2<f32>(0.0, 0.0);
    if (index == 0u) { uv = vec2<f32>(0.0, 2.0); }
    if (index == 1u) { uv = vec2<f32>(0.0, 0.0); }
    if (index == 2u) { uv = vec2<f32>(2.0, 0.0); }
    out.position = vec4<f32>(uv.x * 2.0 - 1.0, 1.0 - uv.y * 2.0, 0.0, 1.0);
    out.uv = uv;
    return out;
}

fn cover(uv: vec2<f32>, screen_ratio: f32, content_ratio: f32) -> vec2<f32> {
    let scale = screen_ratio / content_ratio;
    if (scale > 1.0) {
        return vec2<f32>(uv.x, (uv.y - 0.5) / scale + 0.5);
    }
    return vec2<f32>((uv.x - 0.5) * scale + 0.5, uv.y);
}

fn source_coordinate(covered: vec2<f32>) -> vec2<f32> {
    let rotation = uniforms.sampling_geometry.x;
    var oriented = covered;
    if (rotation > 0.5 && rotation < 1.5) {
        oriented = vec2<f32>(covered.y, 1.0 - covered.x);
    } else if (rotation >= 1.5 && rotation < 2.5) {
        oriented = vec2<f32>(1.0 - covered.x, 1.0 - covered.y);
    } else if (rotation >= 2.5) {
        oriented = vec2<f32>(1.0 - covered.y, covered.x);
    }
    return uniforms.crop_rect.xy + oriented * uniforms.crop_rect.zw;
}

fn linearize(value: f32, transfer: f32) -> f32 {
    let x = max(value, 0.0);
    if (transfer < 0.5) {
        return select(x / 12.92, pow((x + 0.055) / 1.055, 2.4), x > 0.04045);
    }
    if (transfer < 1.5) {
        // Inverse BT.709 OETF. This is distinct from the BT.1886 display
        // EOTF and preserves the linear toe carried in tagged video.
        return select(x / 4.5, pow((x + 0.099) / 1.099, 1.0 / 0.45), x >= 0.081);
    }
    if (transfer < 2.5) {
        return pow(x, 2.4);
    }
    if (transfer < 3.5) {
        let m1 = 0.1593017578125;
        let m2 = 78.84375;
        let c1 = 0.8359375;
        let c2 = 18.8515625;
        let c3 = 18.6875;
        let p = pow(x, 1.0 / m2);
        return pow(max(p - c1, 0.0) / max(c2 - c3 * p, 0.000001), 1.0 / m1);
    }
    let a = 0.17883277;
    let b = 0.28466892;
    let c = 0.55991073;
    return select((x * x) / 3.0, (exp((x - c) / a) + b) / 12.0, x > 0.5);
}

fn to_bt709(rgb: vec3<f32>, primaries: f32) -> vec3<f32> {
    if (primaries < 0.5) {
        // Linear-light SMPTE-C/BT.601 to BT.709, D65 white.
        return vec3<f32>(
            dot(vec3<f32>(0.939542, 0.050181, 0.010277), rgb),
            dot(vec3<f32>(0.017772, 0.965792, 0.016436), rgb),
            dot(vec3<f32>(-0.001622, -0.004371, 1.005993), rgb)
        );
    }
    if (primaries < 1.5) { return rgb; }
    if (primaries < 2.5) {
        // Linear-light BT.2020 to BT.709/sRGB, D65 white.
        return vec3<f32>(
            dot(vec3<f32>(1.660491, -0.587641, -0.072850), rgb),
            dot(vec3<f32>(-0.124550, 1.132900, -0.008349), rgb),
            dot(vec3<f32>(-0.018151, -0.100579, 1.118730), rgb)
        );
    }
    // DCI-P3/Display-P3 to BT.709. DCI white-point adaptation is deliberately
    // handled by the compositor color-management path when available; this is
    // the bounded SDR fallback.
    return vec3<f32>(
        dot(vec3<f32>(1.224745, -0.224904, 0.000000), rgb),
        dot(vec3<f32>(-0.042058, 1.042081, 0.000000), rgb),
        dot(vec3<f32>(-0.019642, -0.078655, 1.098537), rgb)
    );
}

fn tonemap_hdr_to_sdr(rgb: vec3<f32>, transfer: f32, mastering_peak: f32, sdr_white: f32) -> vec3<f32> {
    if (transfer < 2.5) { return rgb; }
    let signal_peak = select(1000.0, 10000.0, transfer < 3.5);
    let nits_rgb = max(rgb, vec3<f32>(0.0)) * signal_peak;
    let luminance = max(dot(nits_rgb, vec3<f32>(0.2126, 0.7152, 0.0722)), 0.000001);
    let x = luminance / max(sdr_white, 1.0);
    let white = max(mastering_peak / max(sdr_white, 1.0), 1.0);
    // Extended Reinhard maps the mastering peak to display white. Diffuse
    // white is compressed to leave room for highlights on an SDR display.
    let mapped = x * (1.0 + x / (white * white)) / (1.0 + x);
    return nits_rgb * (mapped / luminance);
}

@fragment
fn fs_main(in: VertexOutput) -> @location(0) vec4<f32> {
    let uv = source_coordinate(cover(in.uv, uniforms.geometry_and_range.x, uniforms.geometry_and_range.y));
    let chroma_uv = uv + uniforms.sampling_geometry.yz;
    let y_raw = textureSample(t_y, samp, uv).r;
    let uv_raw = textureSample(t_uv, samp, chroma_uv);
    let y = (y_raw - uniforms.geometry_and_range.z) * uniforms.geometry_and_range.w;
    let u = (uv_raw.r - uniforms.chroma_and_transfer.x) * uniforms.chroma_and_transfer.y;
    let v = (uv_raw.g - uniforms.chroma_and_transfer.x) * uniforms.chroma_and_transfer.y;
    let yuv = vec3<f32>(y, u, v);
    let encoded_rgb = clamp(vec3<f32>(
        dot(uniforms.matrix_r.xyz, yuv),
        dot(uniforms.matrix_g.xyz, yuv),
        dot(uniforms.matrix_b.xyz, yuv)
    ), vec3<f32>(0.0), vec3<f32>(1.0));
    let transfer = uniforms.chroma_and_transfer.z;
    let linear_rgb = vec3<f32>(
        linearize(encoded_rgb.r, transfer),
        linearize(encoded_rgb.g, transfer),
        linearize(encoded_rgb.b, transfer)
    );
    let display_rgb = tonemap_hdr_to_sdr(
        to_bt709(linear_rgb, uniforms.chroma_and_transfer.w),
        transfer,
        uniforms.matrix_r.w,
        uniforms.matrix_g.w
    );
    return vec4<f32>(clamp(display_rgb, vec3<f32>(0.0), vec3<f32>(1.0)), 1.0);
}
