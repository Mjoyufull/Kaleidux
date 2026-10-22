pub(crate) fn with_video_env_test_lock<T>(test: impl FnOnce() -> T) -> T {
    static ENV_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
        once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
    let _guard = ENV_LOCK
        .lock()
        .expect("env test lock should not be poisoned");
    test()
}

pub(crate) fn set_env_var(key: &str, value: impl AsRef<std::ffi::OsStr>) {
    // SAFETY: environment mutation tests run under `with_video_env_test_lock`, preventing
    // concurrent environment reads/writes in video tests that use these helpers.
    unsafe { std::env::set_var(key, value) }
}

pub(crate) fn remove_env_var(key: &str) {
    // SAFETY: environment mutation tests run under `with_video_env_test_lock`, preventing
    // concurrent environment reads/writes in video tests that use these helpers.
    unsafe { std::env::remove_var(key) }
}
