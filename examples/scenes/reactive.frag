void mainImage(out vec4 colour, in vec2 xy) {
    vec2 uv = xy / iResolution.xy;
    vec2 pointer = vec2(kldPointer.x, 1.0 - kldPointer.y);
    vec2 offset = uv - pointer;
    offset.x *= iResolution.x / iResolution.y;
    float glow = exp(-6.0 * length(offset));
    float bass = kldAudio(1);
    vec3 base = mix(vec3(0.02, 0.035, 0.075), vec3(0.05, 0.21, 0.48), uv.y);
    colour = vec4(base + vec3(0.30, 0.13, 0.38) * glow * (0.6 + bass), 1.0);
}
