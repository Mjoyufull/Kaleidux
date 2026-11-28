fn main() {
    println!("cargo:rerun-if-changed=src/video/native_backend/vulkan_frames.c");
    for name in [
        "PKG_CONFIG_PATH",
        "PKG_CONFIG_LIBDIR",
        "PKG_CONFIG_SYSROOT_DIR",
        "PKG_CONFIG",
        "PKG_CONFIG_ALLOW_CROSS",
        "HOST_PKG_CONFIG_PATH",
        "TARGET_PKG_CONFIG_PATH",
        "HOST_PKG_CONFIG_LIBDIR",
        "TARGET_PKG_CONFIG_LIBDIR",
        "HOST_PKG_CONFIG_SYSROOT_DIR",
        "TARGET_PKG_CONFIG_SYSROOT_DIR",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
        if let Ok(target) = std::env::var("TARGET") {
            println!("cargo:rerun-if-env-changed={name}_{target}");
            println!(
                "cargo:rerun-if-env-changed={name}_{}",
                target.replace('-', "_")
            );
        }
    }
    if std::env::var_os("CARGO_FEATURE_BACKEND_FFMPEG").is_none() {
        return;
    }
    let mut build = cc::Build::new();
    build.file("src/video/native_backend/vulkan_frames.c");
    for library in ["libavcodec", "libavutil"] {
        let dependency = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(library)
            .expect("FFmpeg development headers are required for backend-ffmpeg");
        for include in dependency.include_paths {
            build.include(include);
        }
    }
    if let Ok(vulkan) = pkg_config::Config::new()
        .cargo_metadata(false)
        .probe("vulkan")
    {
        build.define("KLD_VULKAN_HEADERS", None);
        for include in vulkan.include_paths {
            build.include(include);
        }
    }
    build.compile("kld_vulkan_frames");
}
