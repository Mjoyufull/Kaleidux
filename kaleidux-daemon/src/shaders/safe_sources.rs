pub(super) const GLSL_PRELUDE: &str = r#"
#version 450
layout(location = 0) in vec2 v_uv;
layout(location = 0) out vec4 o_color;

precision highp float;

layout(set = 0, binding = 0) uniform TransitionUniforms {
    float progress;
    float screen_aspect;
    float prev_aspect;
    float next_aspect;
    vec4 params[7];
};

#define ratio screen_aspect

layout(set = 0, binding = 1) uniform texture2D t_prev;
layout(set = 0, binding = 2) uniform texture2D t_next;
layout(set = 0, binding = 3) uniform sampler s_linear;

// Helper to access flattened params from aligned vec4 array
float getFromParams(int i) {
    // GLSL component access via array indexing
    return params[i / 4][i % 4];
}

// Helper for aspect ratio cover
vec2 cover(vec2 uv, float screen_ratio, float content_ratio) {
    float scale = screen_ratio / content_ratio;
    if (scale > 1.0) {
        // Screen is wider than content: Fit Width, Crop Height (Zoom In Y)
        // We divide Y offset by scale to map 0..1 ScreenY to 0.25..0.75 ContentY
        return vec2(uv.x, (uv.y - 0.5) / scale + 0.5);
    } else {
        // Content is wider than screen: Fit Height, Crop Width (Zoom In X)
        // We multiply X offset by scale (where scale < 1.0) to map 0..1 ScreenX to subset ContentX
        return vec2((uv.x - 0.5) * scale + 0.5, uv.y);
    }
}

vec4 getFromColor(vec2 uv) {
    vec2 uv_c = cover(uv, screen_aspect, prev_aspect);
    return texture(sampler2D(t_prev, s_linear), uv_c);
}

vec4 getToColor(vec2 uv) {
    vec2 uv_c = cover(uv, screen_aspect, next_aspect);
    return texture(sampler2D(t_next, s_linear), uv_c);
}
"#;

pub(super) const CUBE_SAFE_GLSL: &str = r#"
vec2 cube_project(vec2 p, float floating) {
    return p * vec2(1.0, -1.2) + vec2(0.0, -floating / 100.0);
}

bool cube_in_bounds(vec2 p) {
    return p.x > 0.0 && p.y > 0.0 && p.x < 1.0 && p.y < 1.0;
}

vec4 cube_bg_color(vec2 p, vec2 pfr, vec2 pto, float reflection, float floating) {
    vec4 c = vec4(0.0, 0.0, 0.0, 1.0);
    vec2 projected_from = cube_project(pfr, floating);
    if (cube_in_bounds(projected_from)) {
        c += mix(vec4(0.0), getFromColor(projected_from), reflection * (1.0 - projected_from.y));
    }
    vec2 projected_to = cube_project(pto, floating);
    if (cube_in_bounds(projected_to)) {
        c += mix(vec4(0.0), getToColor(projected_to), reflection * (1.0 - projected_to.y));
    }
    return c;
}

vec2 cube_xskew(vec2 p, float perspective, float center) {
    float x = mix(p.x, 1.0 - p.x, center);
    float edge_distance = max(abs(center - 0.5), 0.0001);
    float center_side = step(0.5, center);
    float direction = mix(1.0, -1.0, center_side);
    return (
        (
            vec2(x, (p.y - 0.5 * (1.0 - perspective) * x) / (1.0 + (perspective - 1.0) * x))
            - vec2(0.5 - edge_distance, 0.0)
        ) * vec2(0.5 / edge_distance * direction, 1.0)
        + vec2(center_side, 0.0)
    );
}

vec4 transition(vec2 op) {
    float perspective = getFromParams(0);
    float unzoom = getFromParams(1);
    float reflection = getFromParams(2);
    float floating = getFromParams(3);

    float uz = unzoom * 2.0 * (0.5 - abs(0.5 - progress));
    vec2 p = -uz * 0.5 + (1.0 + uz) * op;

    vec2 from_scale = vec2(max(1.0 - progress, 0.0001), 1.0);
    vec2 to_scale = vec2(max(progress, 0.0001), 1.0);

    vec2 from_p = cube_xskew(
        (p - vec2(progress, 0.0)) / from_scale,
        1.0 - mix(progress, 0.0, perspective),
        0.0
    );
    vec2 to_p = cube_xskew(
        p / to_scale,
        mix(progress * progress, 1.0, perspective),
        1.0
    );

    if (cube_in_bounds(from_p)) {
        return getFromColor(from_p);
    }
    if (cube_in_bounds(to_p)) {
        return getToColor(to_p);
    }
    return cube_bg_color(op, from_p, to_p, reflection, floating);
}
"#;

pub(super) const DISPLACEMENT_SAFE_GLSL: &str = r#"
vec4 transition(vec2 uv) {
    float strength = 0.5;
    float displacement = getToColor(uv).r * strength;
    vec2 uv_from = vec2(uv.x + progress * displacement, uv.y);
    vec2 uv_to = vec2(uv.x - (1.0 - progress) * displacement, uv.y);
    return mix(getFromColor(uv_from), getToColor(uv_to), progress);
}
"#;
