use super::*;

fn with_env_lock<T>(test: impl FnOnce() -> T) -> T {
    static ENV_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
        once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
    let _guard = ENV_LOCK
        .lock()
        .expect("env test lock should not be poisoned");
    test()
}

fn set_env_var(key: &str, value: impl AsRef<std::ffi::OsStr>) {
    // SAFETY: these tests serialize environment mutation through `ENV_LOCK`.
    unsafe { std::env::set_var(key, value) }
}

fn remove_env_var(key: &str) {
    // SAFETY: these tests serialize environment mutation through `ENV_LOCK`.
    unsafe { std::env::remove_var(key) }
}

fn restore_env_var(key: &str, old_value: Option<std::ffi::OsString>) {
    match old_value {
        Some(value) => set_env_var(key, value),
        None => remove_env_var(key),
    }
}

#[test]
fn video_publish_fps_uses_profile_defaults() {
    with_env_lock(|| {
        let old_value = std::env::var_os("KLD_LOW_POWER_MAX_PUBLISH_FPS");
        remove_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS");

        assert_eq!(configured_max_publish_fps(VideoFpsProfile::Low), Some(12));
        assert_eq!(
            configured_max_publish_fps(VideoFpsProfile::Medium),
            Some(24)
        );
        assert_eq!(configured_max_publish_fps(VideoFpsProfile::High), Some(48));
        assert_eq!(configured_max_publish_fps(VideoFpsProfile::Unlimited), None);

        restore_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS", old_value);
    });
}

#[test]
fn stop_video_on_image_switch_defaults_to_immediate_stop() {
    assert!(parse_stop_video_on_image_switch(None));
    assert!(parse_stop_video_on_image_switch(Some("bad".to_string())));
}

#[test]
fn stop_video_on_image_switch_accepts_legacy_defer_override() {
    assert!(!parse_stop_video_on_image_switch(Some("0".to_string())));
    assert!(!parse_stop_video_on_image_switch(Some(
        "legacy".to_string()
    )));
    assert!(parse_stop_video_on_image_switch(Some(
        "immediate".to_string()
    )));
}

#[test]
fn video_publish_fps_accepts_bounded_override() {
    with_env_lock(|| {
        let old_value = std::env::var_os("KLD_LOW_POWER_MAX_PUBLISH_FPS");

        set_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS", "12");
        assert_eq!(configured_max_publish_fps(VideoFpsProfile::Low), Some(12));
        set_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS", "1000");
        assert_eq!(
            configured_max_publish_fps(VideoFpsProfile::Medium),
            Some(120)
        );
        set_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS", "0");
        assert_eq!(configured_max_publish_fps(VideoFpsProfile::High), None);
        set_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS", "invalid");
        assert_eq!(configured_max_publish_fps(VideoFpsProfile::High), Some(48));

        restore_env_var("KLD_LOW_POWER_MAX_PUBLISH_FPS", old_value);
    });
}
