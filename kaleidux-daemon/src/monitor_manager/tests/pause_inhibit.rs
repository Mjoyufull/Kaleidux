use super::*;
use kaleidux_common::{MAX_INHIBITOR_REASON_LEN, MAX_INHIBITORS};
use std::time::Duration;

fn make_test_manager() -> MonitorManager {
    let temp = unique_test_dir("pause-inhibit-test");
    let cache = Arc::new(
        FileCache::new_test(&temp.join("cache.redb")).expect("test cache should be created"),
    );
    let output_config = test_output_config(Duration::from_secs(60));
    let mut outputs = HashMap::new();
    outputs.insert(
        "DP-1".to_string(),
        OutputOrchestrator {
            _name: "DP-1".to_string(),
            description: "DP-1 desc".to_string(),
            phase_offset: Duration::ZERO,
            config: output_config.clone(),
            queue: None,
            current_path: Some(PathBuf::from("/tmp/DP-1-current")),
            next_path: Some(PathBuf::from("/tmp/DP-1-next")),
            next_content_type: Some(crate::queue::ContentType::Image),
            next_change: Some(Instant::now() + Duration::from_secs(60)),
            display_start_time: Some(Instant::now()),
        },
    );

    MonitorManager {
        config: config_for_output("DP-1", &output_config),
        outputs,
        shared_queue: None,
        group_queues: HashMap::new(),
        output_groups: HashMap::new(),
        shared_display_start_time: None,
        group_display_start_times: HashMap::new(),
        cache,
        metrics: None,
        manual_paused: false,
        pause_reasons: std::collections::BTreeSet::new(),
        power_suspended: false,
        discovered_files_cache: HashMap::new(),
    }
}

#[test]
fn overlapping_pause_reasons_and_idempotence() {
    let mut mgr = make_test_manager();
    assert!(!mgr.is_paused());
    assert!(mgr.next_switch_deadline().is_some());

    // First inhibitor: transitions to paused
    let trans = mgr.inhibit("game".to_string()).expect("inhibit game");
    assert!(trans, "first inhibitor must transition to paused");
    assert!(mgr.is_paused());
    assert!(mgr.next_switch_deadline().is_none());
    assert_eq!(mgr.inhibitors(), vec!["game".to_string()]);

    // Duplicate inhibitor: idempotent, no transition
    let trans_dup = mgr.inhibit("game".to_string()).expect("inhibit game dup");
    assert!(!trans_dup, "duplicate inhibitor must not re-transition");
    assert!(mgr.is_paused());

    // Second inhibitor: already paused, no transition
    let trans2 = mgr.inhibit("stream".to_string()).expect("inhibit stream");
    assert!(
        !trans2,
        "second inhibitor while already paused must not transition"
    );
    assert!(mgr.is_paused());
    assert_eq!(
        mgr.inhibitors(),
        vec!["game".to_string(), "stream".to_string()]
    );

    // Uninhibit one: still paused by the other!
    let trans_un1 = mgr.uninhibit("game").expect("uninhibit game");
    assert!(
        !trans_un1,
        "removing one reason while others remain must NOT transition to resumed"
    );
    assert!(mgr.is_paused(), "must still be paused by stream");
    assert_eq!(mgr.inhibitors(), vec!["stream".to_string()]);

    // Duplicate uninhibit: idempotent, no transition
    let trans_un_dup = mgr.uninhibit("game").expect("uninhibit game again");
    assert!(
        !trans_un_dup,
        "uninhibit of already removed reason must be idempotent"
    );
    assert!(mgr.is_paused());

    // Uninhibit last: transitions to resumed
    let trans_un2 = mgr.uninhibit("stream").expect("uninhibit stream");
    assert!(
        trans_un2,
        "removing final inhibitor must transition to resumed"
    );
    assert!(!mgr.is_paused());
    assert!(mgr.next_switch_deadline().is_some());
    assert!(mgr.inhibitors().is_empty());
}

#[test]
fn manual_pause_independent_of_named_reasons() {
    let mut mgr = make_test_manager();

    // 1. Manual pause first, then named inhibitor
    assert!(mgr.set_paused(true), "manual pause transitions to paused");
    assert!(mgr.is_paused());
    assert!(mgr.is_manual_paused());

    // Add inhibitor while manually paused
    let trans_inh = mgr.inhibit("movie".to_string()).expect("inhibit movie");
    assert!(
        !trans_inh,
        "inhibiting while already manually paused does not transition"
    );
    assert!(mgr.is_paused());

    // Manual resume must NOT resume because inhibitor is still active
    let trans_res = mgr.set_paused(false);
    assert!(
        !trans_res,
        "manual resume must NOT resume while named inhibitor is active"
    );
    assert!(mgr.is_paused(), "must remain paused due to movie inhibitor");
    assert!(!mgr.is_manual_paused());

    // Clear inhibitor -> now it resumes!
    let trans_uninh = mgr.uninhibit("movie").expect("uninhibit movie");
    assert!(
        trans_uninh,
        "clearing last inhibitor when not manually paused transitions to resumed"
    );
    assert!(!mgr.is_paused());

    // 2. Named inhibitor first, then manual pause
    let trans_inh2 = mgr
        .inhibit("presentation".to_string())
        .expect("inhibit presentation");
    assert!(trans_inh2);
    assert!(mgr.is_paused());

    assert!(
        !mgr.set_paused(true),
        "manual pause while already inhibited does not transition"
    );
    assert!(mgr.is_manual_paused());

    // Clearing inhibitor must NOT resume because manual pause is active
    let trans_uninh2 = mgr
        .uninhibit("presentation")
        .expect("uninhibit presentation");
    assert!(
        !trans_uninh2,
        "clearing inhibitor while manual pause is active must NOT resume"
    );
    assert!(mgr.is_paused(), "must remain paused due to manual pause");

    // Manual resume transitions to resumed
    assert!(
        mgr.set_paused(false),
        "manual resume when no inhibitors active transitions to resumed"
    );
    assert!(!mgr.is_paused());
}

#[test]
fn invalid_inputs_and_overcapacity() {
    let mut mgr = make_test_manager();

    // Invalid reasons
    assert!(
        mgr.inhibit("".to_string()).is_err(),
        "empty reason rejected"
    );
    assert!(
        mgr.inhibit("   ".to_string()).is_err(),
        "whitespace-only reason rejected"
    );
    assert!(
        mgr.inhibit("ctrl\x00char".to_string()).is_err(),
        "null byte rejected"
    );
    assert!(
        mgr.inhibit("new\nline".to_string()).is_err(),
        "newline rejected"
    );

    let long_reason = "x".repeat(MAX_INHIBITOR_REASON_LEN + 1);
    assert!(
        mgr.inhibit(long_reason).is_err(),
        "over-length reason rejected"
    );

    let max_len_reason = "y".repeat(MAX_INHIBITOR_REASON_LEN);
    assert!(
        mgr.inhibit(max_len_reason.clone()).is_ok(),
        "max-length reason accepted"
    );
    assert!(mgr.uninhibit(&max_len_reason).is_ok());

    // Invalid uninhibit
    assert!(mgr.uninhibit("").is_err(), "empty uninhibit rejected");
    assert!(mgr.uninhibit("  ").is_err(), "blank uninhibit rejected");
    assert!(
        mgr.uninhibit(&"z".repeat(MAX_INHIBITOR_REASON_LEN + 1))
            .is_err()
    );

    // Fill to capacity
    for i in 0..MAX_INHIBITORS {
        let reason = format!("reason_{i:03}");
        assert!(mgr.inhibit(reason).is_ok(), "filling up to capacity");
    }
    assert_eq!(mgr.inhibitors().len(), MAX_INHIBITORS);

    // Overcapacity check
    let overcapacity_err = mgr.inhibit("one_too_many".to_string());
    assert!(
        overcapacity_err.is_err(),
        "exceeding MAX_INHIBITORS must fail"
    );

    // Adding an existing reason while at capacity is idempotent and succeeds
    assert!(
        mgr.inhibit("reason_000".to_string()).is_ok(),
        "existing reason at capacity succeeds"
    );

    // Free a slot and add new
    assert!(mgr.uninhibit("reason_000").is_ok());
    assert_eq!(mgr.inhibitors().len(), MAX_INHIBITORS - 1);
    assert!(
        mgr.inhibit("now_fits".to_string()).is_ok(),
        "adding after slot freed succeeds"
    );
}
