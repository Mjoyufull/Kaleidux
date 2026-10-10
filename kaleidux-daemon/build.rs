fn main() {
    println!("cargo:rerun-if-changed=src/video/native_backend/vulkan_frames.c");
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
