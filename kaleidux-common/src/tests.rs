use super::Transition;

#[test]
fn random_candidate_names_roundtrip_via_parser() {
    for name in Transition::random_candidate_names() {
        let parsed = Transition::from_name(name);
        if *name != "fade" {
            assert_ne!(
                parsed,
                Transition::Fade,
                "transition `{name}` parsed as fallback Fade"
            );
        }
    }
}
