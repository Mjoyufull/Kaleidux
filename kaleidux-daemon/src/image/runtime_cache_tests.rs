#[cfg(test)]
mod tests {
    use crate::background::BackgroundWorkKind;
    use crate::image as image_pipeline;
    use crate::image::runtime_cache::*;
    use crate::image::runtime_switch::*;
    use crate::image::types::{
        DecodedImagePayload, DecodedSourceImage, DecodedSourcePixels, ImageLoadProfile,
        ImageSourceIdentity, PreparedImageKey,
    };
    use crate::main_loop::PendingContentSwitch;
    use crate::metrics;
    use crate::queue;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    fn write_temp_png(width: u32, height: u32) -> PathBuf {
        let unique = format!(
            "kaleidux-image-test-{}-{}.png",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        let image = image::RgbaImage::from_pixel(width, height, image::Rgba([10, 20, 30, 255]));
        image.save(&path).expect("temp png should save");
        path
    }

    #[test]
    fn prepared_image_cache_roundtrip_preserves_rgba_payload() {
        let source_path = write_temp_png(1, 1);

        let payload = DecodedImagePayload {
            data: vec![1, 2, 3, 4, 5, 6, 7, 8].into(),
            width: 1,
            height: 2,
            profile: ImageLoadProfile {
                format: "png-fast".to_string(),
                source_width: 100,
                source_height: 200,
                permit_wait: Duration::ZERO,
                decode: Duration::from_millis(1),
                convert: Duration::ZERO,
                resize: Duration::from_millis(2),
                expand: Duration::ZERO,
                resize_filter: Some("bilinear".to_string()),
            },
        };

        store_prepared_image_cache(&source_path, 1920, 1080, &payload);
        let cached = try_load_prepared_image_cache(&source_path, 1920, 1080)
            .expect("prepared cache should load");

        assert_eq!(cached.width, payload.width);
        assert_eq!(cached.height, payload.height);
        assert_eq!(cached.data, payload.data);
        assert_eq!(cached.profile.source_width, payload.profile.source_width);
        assert_eq!(cached.profile.source_height, payload.profile.source_height);
        assert_eq!(cached.profile.format, "prepared-cache via png-fast");

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1920, 1080) {
            let _ = std::fs::remove_file(cache_path);
        }
        let _ = std::fs::remove_file(source_path);
    }

    #[test]
    fn prepared_image_cache_is_scoped_to_output_target() {
        let source_path = write_temp_png(1, 1);

        let payload = DecodedImagePayload {
            data: vec![9, 8, 7, 6].into(),
            width: 1,
            height: 1,
            profile: ImageLoadProfile {
                format: "png-fast".to_string(),
                source_width: 3840,
                source_height: 2160,
                permit_wait: Duration::ZERO,
                decode: Duration::from_millis(1),
                convert: Duration::ZERO,
                resize: Duration::ZERO,
                expand: Duration::ZERO,
                resize_filter: None,
            },
        };

        store_prepared_image_cache(&source_path, 1920, 1080, &payload);
        assert!(try_load_prepared_image_cache(&source_path, 1366, 768).is_none());

        if let Some(cache_path) = prepared_image_cache_path(&source_path, 1920, 1080) {
            let _ = std::fs::remove_file(cache_path);
        }
        let _ = std::fs::remove_file(source_path);
    }

    #[test]
    fn prepared_target_dimensions_do_not_upscale_small_sources() {
        let path = write_temp_png(800, 600);

        assert_eq!(
            prepared_target_dimensions_from_path(&path, 1920, 1080),
            Some((800, 600))
        );
        assert_eq!(
            prepared_target_dimensions_from_path(&path, 2560, 1440),
            Some((800, 600))
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn prepared_image_memory_cache_reuses_small_source_for_larger_outputs() {
        let path = write_temp_png(800, 600);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        let first = runtime
            .block_on(request_prepared_image_payload(
                &path,
                1920,
                1080,
                BackgroundWorkKind::ImageDecode,
                &Arc::new(metrics::PerformanceMetrics::new()),
            ))
            .expect("first prepared image should load");
        let second = runtime
            .block_on(request_prepared_image_payload(
                &path,
                2560,
                1440,
                BackgroundWorkKind::ImageDecode,
                &Arc::new(metrics::PerformanceMetrics::new()),
            ))
            .expect("second prepared image should reuse cache");

        assert_eq!((first.width, first.height), (800, 600));
        assert_eq!((second.width, second.height), (800, 600));
        assert!(Arc::ptr_eq(&first.data, &second.data));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn compatible_prepared_image_memory_reuses_larger_variant_for_smaller_target() {
        let path = std::env::temp_dir().join(format!(
            "kaleidux-compatible-prepared-test-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"prepared").expect("temp source should be writable");

        let identity = image_pipeline::descriptor::source_identity(&path)
            .expect("temp source should have metadata");
        let key = prepared_image_key_for_identity(identity.clone(), 2560, 1440);
        let payload = DecodedImagePayload {
            data: vec![42; 16].into(),
            width: 2560,
            height: 1440,
            profile: ImageLoadProfile {
                format: "prepared-test".to_string(),
                source_width: 3840,
                source_height: 2160,
                permit_wait: Duration::ZERO,
                decode: Duration::ZERO,
                convert: Duration::ZERO,
                resize: Duration::ZERO,
                expand: Duration::ZERO,
                resize_filter: None,
            },
        };
        store_prepared_image_memory(key, &payload);

        let compatible = try_load_compatible_prepared_image_memory(&identity, 1920, 1080)
            .expect("smaller target should reuse larger prepared payload");
        assert_eq!((compatible.width, compatible.height), (2560, 1440));
        assert_eq!(compatible.data, payload.data);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn compatible_prepared_key_prefers_smallest_larger_variant() {
        let identity = ImageSourceIdentity {
            path: PathBuf::from("/tmp/shared.png"),
            file_len: 10,
            modified_secs: 20,
            modified_nanos: 30,
        };
        let keys = [
            PreparedImageKey {
                source: identity.clone(),
                target_width: 2560,
                target_height: 1440,
            },
            PreparedImageKey {
                source: identity.clone(),
                target_width: 1920,
                target_height: 1080,
            },
            PreparedImageKey {
                source: identity.clone(),
                target_width: 3840,
                target_height: 2160,
            },
        ];

        let selected = select_compatible_prepared_key(keys.iter(), &identity, 1600, 900)
            .expect("a larger compatible prepared key should be selected");
        assert_eq!(selected.target_width, 1920);
        assert_eq!(selected.target_height, 1080);
    }

    #[test]
    fn pending_image_switches_sort_larger_shared_targets_first() {
        let mut pending = vec![
            PendingContentSwitch {
                name: "DP-2".to_string(),
                path: PathBuf::from("/tmp/shared.jpg"),
                content_type: queue::ContentType::Image,
                shared_image_target: None,
                target_width: 1280,
                target_height: 720,
                target_area: 1280 * 720,
            },
            PendingContentSwitch {
                name: "HDMI-A-1".to_string(),
                path: PathBuf::from("/tmp/shared.jpg"),
                content_type: queue::ContentType::Image,
                shared_image_target: None,
                target_width: 1920,
                target_height: 1080,
                target_area: 1920 * 1080,
            },
            PendingContentSwitch {
                name: "DP-3".to_string(),
                path: PathBuf::from("/tmp/video.mp4"),
                content_type: queue::ContentType::Video,
                shared_image_target: None,
                target_width: 0,
                target_height: 0,
                target_area: 0,
            },
        ];

        sort_pending_content_switches(&mut pending);

        assert_eq!(pending[0].name, "HDMI-A-1");
        assert_eq!(pending[1].name, "DP-2");
        assert_eq!(pending[2].name, "DP-3");
    }

    #[test]
    fn shared_image_switches_use_largest_batch_target() {
        let mut pending = vec![
            PendingContentSwitch {
                name: "DP-1".to_string(),
                path: PathBuf::from("/tmp/shared.jpg"),
                content_type: queue::ContentType::Image,
                shared_image_target: None,
                target_width: 1920,
                target_height: 1080,
                target_area: 1920 * 1080,
            },
            PendingContentSwitch {
                name: "DP-2".to_string(),
                path: PathBuf::from("/tmp/shared.jpg"),
                content_type: queue::ContentType::Image,
                shared_image_target: None,
                target_width: 2560,
                target_height: 1440,
                target_area: 2560 * 1440,
            },
        ];

        annotate_shared_image_targets(&mut pending);

        assert_eq!(pending[0].shared_image_target, Some((2560, 1440)));
        assert_eq!(pending[1].shared_image_target, Some((2560, 1440)));
    }

    #[test]
    fn source_descriptor_reuses_decoded_source_dimensions_without_header_parse() {
        let path = std::env::temp_dir().join(format!(
            "kaleidux-image-descriptor-test-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"not-an-image").expect("temp source should be writable");

        let identity = image_pipeline::descriptor::source_identity(&path)
            .expect("temp source should have metadata");
        store_decoded_source_memory(
            identity.clone(),
            Arc::new(DecodedSourceImage {
                pixels: DecodedSourcePixels::Rgb(vec![0; 2 * 2 * 3].into()),
                width: 2,
                height: 2,
                format: "test".to_string(),
                decode: Duration::ZERO,
                convert: Duration::ZERO,
            }),
        );

        let descriptor =
            load_image_source_descriptor(&path).expect("decoded source cache should provide dims");
        assert_eq!(descriptor.identity, identity);
        assert_eq!((descriptor.width, descriptor.height), (2, 2));

        let _ = std::fs::remove_file(path);
    }
}
