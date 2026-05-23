use super::{
    Config, MonitorBehavior, PartialOutputConfig, PerformanceProfile, SortingStrategy,
    VideoFpsProfile,
};
use crate::shaders::Transition;

#[test]
fn parses_legacy_nested_transition_tables() {
    let cfg: PartialOutputConfig = toml::from_str(
        r#"
            transition = { hexagonalize = { steps = 50, horizontal_hexagons = 20.0 } }
            "#,
    )
    .unwrap();

    assert_eq!(
        cfg.transition,
        Some(Transition::Hexagonalize {
            steps: 50,
            horizontal_hexagons: 20.0,
        })
    );
}

#[test]
fn parses_simple_transition_strings() {
    let cfg: PartialOutputConfig = toml::from_str(
        r#"
            transition = "crosszoom"
            "#,
    )
    .unwrap();

    assert_eq!(
        cfg.transition,
        Some(Transition::CrossZoom { strength: 0.4 })
    );
}

#[test]
fn parses_tagged_transition_aliases() {
    let cfg: PartialOutputConfig = toml::from_str(
        r#"
            transition = { type = "randomsquares", size = [8, 8], smoothness = 0.25 }
            "#,
    )
    .unwrap();

    assert_eq!(
        cfg.transition,
        Some(Transition::RandomSquares {
            size: [8, 8],
            smoothness: 0.25,
        })
    );
}

#[test]
fn parses_grouped_monitor_behavior_from_global() {
    let cfg = Config::parse_str(
        r#"
            [global]
            monitor-behavior = { grouped = [["DP-2", "DP-3"], ["HDMI-A-1"]] }

            [any]
            path = "/tmp/walls"
            "#,
    )
    .unwrap();

    assert_eq!(
        cfg.global.monitor_behavior,
        MonitorBehavior::Grouped(vec![
            vec!["DP-2".to_string(), "DP-3".to_string()],
            vec!["HDMI-A-1".to_string()],
        ])
    );
}

#[test]
fn merges_global_any_regex_and_specific_output_configs() {
    let cfg = Config::parse_str(
        r#"
            [global]
            monitor-behavior = "independent"
            volume = 10
            transition-time = 400
            sorting = "ascending"

            [any]
            path = "/tmp/any"
            duration = "45s"
            transition = "fade"

            ["re:Primary.*"]
            path = "/tmp/regex"
            volume = 20
            transition = "circleopen"

            [DP-2]
            path = "/tmp/specific"
            volume = 30
            transition = "crosszoom"
            sorting = "descending"
            "#,
    )
    .unwrap();

    let specific = cfg.get_config_for_output("DP-2", "Primary Display");
    assert_eq!(
        specific.path.unwrap(),
        std::path::PathBuf::from("/tmp/specific")
    );
    assert_eq!(specific.volume, 30);
    assert_eq!(specific.transition, Transition::CrossZoom { strength: 0.4 });
    assert_eq!(specific.transition_time, 400);
    assert_eq!(specific.sorting, SortingStrategy::Descending);

    let regex = cfg.get_config_for_output("HDMI-A-1", "Primary Display");
    assert_eq!(regex.path.unwrap(), std::path::PathBuf::from("/tmp/regex"));
    assert_eq!(regex.volume, 20);
    assert_eq!(
        regex.transition,
        Transition::CircleOpen {
            smoothness: 0.3,
            opening: true,
        }
    );
    assert_eq!(regex.transition_time, 400);
    assert_eq!(regex.sorting, SortingStrategy::Ascending);

    let fallback = cfg.get_config_for_output("DP-3", "Side Display");
    assert_eq!(fallback.path.unwrap(), std::path::PathBuf::from("/tmp/any"));
    assert_eq!(fallback.volume, 10);
    assert_eq!(fallback.transition, Transition::Fade);
    assert_eq!(fallback.transition_time, 400);
    assert_eq!(fallback.sorting, SortingStrategy::Ascending);
}

#[test]
fn low_power_profile_uses_low_fps_defaults() {
    let cfg = Config::parse_str(
        r#"
            [global]
            performance = "low-power"

            [any]
            path = "/tmp/any"
            "#,
    )
    .unwrap();

    let matched = cfg.get_config_for_output("DP-1", "Side Display");
    assert_eq!(matched.performance, PerformanceProfile::LowPower);
    assert_eq!(matched.video_fps, VideoFpsProfile::Low);
    assert_eq!(matched.frame_latency, Some(1));
}

#[test]
fn video_fps_profile_merges_global_any_and_output() {
    let cfg = Config::parse_str(
        r#"
            [global]
            video-fps = "medium"

            [any]
            path = "/tmp/any"
            video-fps = "high"

            [DP-2]
            video-fps = "unlimited"
            "#,
    )
    .unwrap();

    let fallback = cfg.get_config_for_output("DP-1", "Side Display");
    assert_eq!(fallback.video_fps, VideoFpsProfile::High);

    let specific = cfg.get_config_for_output("DP-2", "Primary Display");
    assert_eq!(specific.video_fps, VideoFpsProfile::Unlimited);
}

#[test]
fn performance_profiles_choose_video_fps_defaults() {
    let low = Config::parse_str(
        r#"
            [global]
            performance = "low-power"
            "#,
    )
    .unwrap()
    .get_config_for_output("DP-1", "Side Display");
    assert_eq!(low.video_fps, VideoFpsProfile::Low);

    let quality = Config::parse_str(
        r#"
            [global]
            performance = "quality"
            "#,
    )
    .unwrap()
    .get_config_for_output("DP-1", "Side Display");
    assert_eq!(quality.video_fps, VideoFpsProfile::High);

    let balanced = Config::parse_str(
        r#"
            [global]
            performance = "balanced"
            "#,
    )
    .unwrap()
    .get_config_for_output("DP-1", "Side Display");
    assert_eq!(balanced.video_fps, VideoFpsProfile::Unlimited);
}

#[test]
fn explicit_frame_latency_overrides_profile_default() {
    let cfg = Config::parse_str(
        r#"
            [global]
            performance = "low-power"
            frame-latency = 2
            "#,
    )
    .unwrap();

    let matched = cfg.get_config_for_output("DP-1", "Side Display");
    assert_eq!(matched.performance, PerformanceProfile::LowPower);
    assert_eq!(matched.frame_latency, Some(2));
}

#[test]
fn regex_output_matching_is_lexicographically_deterministic() {
    let cfg = Config::parse_str(
        r#"
            ["re:Primary.*"]
            volume = 40

            ["re:P.*"]
            volume = 25
            "#,
    )
    .unwrap();

    let matched = cfg.get_config_for_output("HDMI-A-1", "Primary Display");
    assert_eq!(matched.volume, 25);
}

#[test]
fn exact_output_override_keeps_zero_global_volume() {
    let cfg = Config::parse_str(
        r#"
            [global]
            monitor-behavior = "independent"
            volume = 0
            sorting = "ascending"

            [any]
            duration = "3m"
            transition = "fade"

            [DP-2]
            path = "/tmp/videos"
            "#,
    )
    .unwrap();

    let matched = cfg.get_config_for_output("DP-2", "Test Display");
    assert_eq!(matched.volume, 0);
    assert_eq!(matched.sorting, SortingStrategy::Ascending);
}
