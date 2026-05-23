use super::{CachedWgslEntry, ShaderManager};
use kaleidux_common::Transition;

#[test]
fn all_random_candidate_shaders_compile() {
    let mut failures = Vec::new();

    for name in Transition::random_candidate_names() {
        let transition = Transition::from_name(name);
        if let Err(e) = ShaderManager::get_builtin_shader(&transition) {
            failures.push(format!("{}: {}", name, e));
        }
    }

    assert!(
        failures.is_empty(),
        "builtin transition shader failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn stale_cache_entry_is_rejected() {
    let transition = Transition::Fade;
    let fingerprint = ShaderManager::builtin_shader_cache_fingerprint_for_transition(&transition)
        .expect("fade fingerprint should be available");

    let matching = CachedWgslEntry {
        fingerprint,
        wgsl: "cached".to_string(),
    };
    let stale = CachedWgslEntry {
        fingerprint: fingerprint ^ 1,
        wgsl: "cached".to_string(),
    };

    assert!(ShaderManager::cache_entry_matches_transition(
        &transition,
        &matching
    ));
    assert!(!ShaderManager::cache_entry_matches_transition(
        &transition,
        &stale
    ));
}

#[test]
fn overexposure_uses_shader_default_strength() {
    assert_eq!(
        ShaderManager::builtin_shader_mapping(&Transition::Overexposure),
        "float strength = 0.6;"
    );
}
