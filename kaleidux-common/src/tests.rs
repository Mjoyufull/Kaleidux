use super::{MAX_INHIBITOR_REASON_LEN, Request, Response, Transition, validate_inhibit_reason};

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

#[test]
fn validate_inhibit_reason_boundaries() {
    assert!(validate_inhibit_reason("").is_err());
    assert!(validate_inhibit_reason("   ").is_err());
    assert!(validate_inhibit_reason("\t\n").is_err());
    assert!(validate_inhibit_reason("game\x00mode").is_err());
    assert!(validate_inhibit_reason("game\nmode").is_err());

    let long_reason = "a".repeat(MAX_INHIBITOR_REASON_LEN + 1);
    assert!(validate_inhibit_reason(&long_reason).is_err());

    let max_reason = "a".repeat(MAX_INHIBITOR_REASON_LEN);
    assert!(validate_inhibit_reason(&max_reason).is_ok());

    assert!(validate_inhibit_reason("gaming").is_ok());
    assert!(validate_inhibit_reason("steam:big-picture").is_ok());
    assert!(validate_inhibit_reason("screen recording").is_ok());
}

#[test]
fn protocol_serde_inhibit_roundtrip() {
    let req = Request::Inhibit {
        reason: "game".to_string(),
    };
    let json = serde_json::to_string(&req).expect("serialize Inhibit");
    assert!(json.contains("\"method\":\"inhibit\""));
    let deserialized: Request = serde_json::from_str(&json).expect("deserialize Inhibit");
    match deserialized {
        Request::Inhibit { reason } => assert_eq!(reason, "game"),
        _ => panic!("deserialized wrong variant"),
    }

    let req2 = Request::Uninhibit {
        reason: "game".to_string(),
    };
    let json2 = serde_json::to_string(&req2).expect("serialize Uninhibit");
    assert!(json2.contains("\"method\":\"uninhibit\""));
    let deserialized2: Request = serde_json::from_str(&json2).expect("deserialize Uninhibit");
    match deserialized2 {
        Request::Uninhibit { reason } => assert_eq!(reason, "game"),
        _ => panic!("deserialized wrong variant"),
    }

    let req3 = Request::Inhibitors;
    let json3 = serde_json::to_string(&req3).expect("serialize Inhibitors");
    assert!(json3.contains("\"method\":\"inhibitors\""));
    let deserialized3: Request = serde_json::from_str(&json3).expect("deserialize Inhibitors");
    assert!(matches!(deserialized3, Request::Inhibitors));

    let alias_json = "{\"method\":\"inhibitors_list\"}";
    let deserialized_alias: Request =
        serde_json::from_str(alias_json).expect("deserialize alias inhibitors_list");
    assert!(matches!(deserialized_alias, Request::Inhibitors));

    let resp = Response::Inhibitors(vec!["game".to_string(), "movie".to_string()]);
    let json_resp = serde_json::to_string(&resp).expect("serialize response");
    let deserialized_resp: Response =
        serde_json::from_str(&json_resp).expect("deserialize response");
    match deserialized_resp {
        Response::Inhibitors(reasons) => assert_eq!(reasons, vec!["game", "movie"]),
        _ => panic!("deserialized wrong response variant"),
    }
}

#[test]
fn omitted_transition_parameters_match_named_defaults() {
    for (tag, name) in [
        ("stereo-viewer", "stereoviewer"),
        ("squares-wire", "squareswire"),
    ] {
        let value = serde_json::json!({"type": tag});
        let parsed: Transition = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, Transition::from_name(name));
    }
}
