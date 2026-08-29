//! Configuration: shape, loading, environment overrides, validation, tuning.
//!
//! This file is the facade. Every type is still reachable as
//! `crate::core::config::Thing`; the submodules below hold the definitions.

mod env;
mod load;
mod schema;
mod tuning;
mod validate;

pub use schema::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use tempfile::NamedTempFile;

    use crate::core::errors::ConfigError;

    use super::schema::default_asset_studio_export_types;

    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn rejects_non_v3_config_version() {
        let config = AppConfig {
            config_version: 1,
            ..AppConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, ConfigError::UnsupportedVersion(1)));
    }

    #[test]
    fn parses_v3_yaml_structure() {
        let yaml = r#"
config_version: 3
server:
  host: 127.0.0.1
  port: 18080
  asset_http_version: http1
  auth:
    enabled: true
    bearer_token: secret
logging:
  level: DEBUG
execution:
  retry:
    attempts: 3
    initial_backoff_ms: 250
    max_backoff_ms: 1000
regions:
  jp:
    enabled: true
    provider:
      kind: colorful_palette
      asset_info_url_template: "https://example.com/{env}/{asset_version}/{asset_hash}"
      asset_bundle_url_template: "https://example.com/assets/{bundle_path}"
      profile: production
      profile_hashes:
        production: abc123
"#;

        let config: AppConfig = yaml_serde::from_str(yaml).unwrap();
        config.validate().unwrap();

        assert_eq!(config.server.port, 18080);
        assert_eq!(config.server.asset_http_version, AssetHttpVersion::Http1);
        assert_eq!(config.logging.level, "DEBUG");
        assert_eq!(config.execution.retry.attempts, 3);
        assert_eq!(config.enabled_regions(), vec!["jp".to_string()]);
        assert_eq!(
            config.regions["jp"].export.asset_studio_types,
            default_asset_studio_export_types()
        );
    }

    #[test]
    fn asset_studio_and_media_defaults_are_valid() {
        let config = AppConfig::default();
        let asset_studio = &config.backends.asset_studio;
        assert_eq!(default_asset_studio_export_types(), vec!["all"]);
        assert_eq!(config.server.asset_http_version, AssetHttpVersion::Auto);
        assert_eq!(MediaBackend::default(), MediaBackend::Ffi);
        assert_eq!(config.backends.media.backend, MediaBackend::Ffi);
        assert_eq!(config.backends.image.backend, ImageBackend::Rust);
        assert_eq!(
            config.backends.image.png_compression,
            ImagePngCompression::Fast
        );
        assert!(config.backends.image.webp_lossless);
        assert_eq!(config.backends.image.jpeg_quality, 95);
        assert_eq!(asset_studio.read_batch_size, 64);
        assert_eq!(asset_studio.image_format, None);
        assert!(asset_studio.read_kinds.is_empty());
        assert_eq!(config.concurrency.download, 32);
        assert_eq!(config.concurrency.post_process, 16);
        assert_eq!(config.concurrency.acb, 12);
        assert_eq!(config.concurrency.usm, 6);
        assert_eq!(config.concurrency.images, 12);
        assert_eq!(config.concurrency.media_encode, 12);
        assert_eq!(config.concurrency.audio_encode, 12);
        assert_eq!(config.concurrency.video_encode, 4);
        assert!(!config.concurrency.auto_tune);
        assert!(config.resources.cpu.budget_auto);
        assert_eq!(config.resources.cpu.budget_ratio, 1.0);
        assert_eq!(config.resources.cpu.reserved, 0);
        assert!(!config.resources.cpu.throttle.enabled);
        assert_eq!(config.resources.cpu.throttle.sample_ms, 250);
    }

    #[test]
    fn parses_asset_studio_options() {
        let yaml = r#"
media:
  backend: ffi
  ffmpeg_path: ffmpeg
image:
  backend: rust
  png_compression: best
  webp_lossless: true
  jpeg_quality: 88
asset_studio:
  read_batch_size: 16
  image_format: raw_rgba
  read_kinds:
    Sprite: image
    Animator: fbx
    all: typetree_json
"#;
        let backends: BackendsConfig = yaml_serde::from_str(yaml).unwrap();
        let asset_studio = &backends.asset_studio;
        assert_eq!(backends.media.backend, MediaBackend::Ffi);
        assert_eq!(backends.image.backend, ImageBackend::Rust);
        assert_eq!(backends.image.png_compression, ImagePngCompression::Best);
        assert_eq!(backends.image.jpeg_quality, 88);
        assert_eq!(asset_studio.read_batch_size, 16);
        assert_eq!(asset_studio.image_format.as_deref(), Some("raw_rgba"));
        assert_eq!(
            asset_studio.read_kinds.get("Animator").map(String::as_str),
            Some("fbx")
        );
        assert_eq!(
            asset_studio.read_kinds.get("all").map(String::as_str),
            Some("typetree_json")
        );
    }

    #[test]
    fn rejects_invalid_media_backend() {
        let err = "sidecar"
            .parse::<MediaBackend>()
            .expect_err("invalid media backend should fail");
        assert!(matches!(
            err,
            ConfigError::InvalidValue { field, value, .. }
                if field == "backends.media.backend" && value == "sidecar"
        ));
    }

    #[test]
    fn image_export_formats_default_to_png_and_dedupe() {
        assert_eq!(
            ImageExportConfig::default().output_formats(),
            vec![ImageOutputFormat::Png]
        );

        let images = ImageExportConfig {
            formats: vec![
                ImageOutputFormat::Jpg,
                ImageOutputFormat::Webp,
                ImageOutputFormat::Jpg,
            ],
        };

        assert_eq!(
            images.output_formats(),
            vec![ImageOutputFormat::Jpg, ImageOutputFormat::Webp]
        );
    }

    #[test]
    fn video_export_formats_default_to_mp4_and_dedupe() {
        assert_eq!(
            VideoExportConfig::default().output_formats(),
            vec![VideoOutputFormat::Mp4]
        );

        let video = VideoExportConfig {
            formats: vec![
                VideoOutputFormat::M2v,
                VideoOutputFormat::Mp4,
                VideoOutputFormat::M2v,
            ],
            direct_mp4: true,
        };
        assert_eq!(
            video.output_formats(),
            vec![VideoOutputFormat::M2v, VideoOutputFormat::Mp4]
        );
        assert!(video.writes_m2v());
        assert!(video.writes_mp4());
    }

    #[test]
    fn audio_export_formats_default_to_mp3_and_dedupe() {
        assert_eq!(
            AudioExportConfig::default().output_formats(),
            vec![AudioOutputFormat::Mp3]
        );

        let audio = AudioExportConfig {
            formats: vec![
                AudioOutputFormat::Wav,
                AudioOutputFormat::Flac,
                AudioOutputFormat::Wav,
                AudioOutputFormat::Mp3,
            ],
        };
        assert_eq!(
            audio.output_formats(),
            vec![
                AudioOutputFormat::Wav,
                AudioOutputFormat::Flac,
                AudioOutputFormat::Mp3
            ]
        );
    }

    #[test]
    fn rejects_legacy_runtime_export_format_fields() {
        assert!(yaml_serde::from_str::<ImageExportConfig>("convert_to_webp: true").is_err());
        assert!(yaml_serde::from_str::<VideoExportConfig>("convert_to_mp4: true").is_err());
        assert!(yaml_serde::from_str::<AudioExportConfig>("convert_to_mp3: true").is_err());
    }

    #[test]
    fn rejects_invalid_image_backend_settings() {
        let mut config = AppConfig::default();
        config.backends.image.jpeg_quality = 0;

        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { field, .. }
                if field == "backends.image.jpeg_quality"
        ));
    }

    #[test]
    fn rejects_zero_asset_studio_read_batch_size() {
        let mut config = AppConfig::default();
        config.backends.asset_studio.read_batch_size = 0;
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "backends.asset_studio.read_batch_size" && value == "0"
        ));
    }

    #[test]
    fn rejects_zero_media_encode_concurrency() {
        let mut config = AppConfig::default();
        config.concurrency.media_encode = 0;
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "concurrency.media_encode" && value == "0"
        ));
    }

    #[test]
    fn rejects_zero_split_media_encode_concurrency() {
        for field in ["audio_encode", "video_encode"] {
            let mut config = AppConfig::default();
            match field {
                "audio_encode" => config.concurrency.audio_encode = 0,
                "video_encode" => config.concurrency.video_encode = 0,
                _ => unreachable!(),
            }
            let err = config.validate().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidValue { field: ref actual, ref value, .. }
                    if actual == &format!("concurrency.{field}") && value == "0"
            ));
        }
    }

    #[test]
    fn rejects_invalid_asset_studio_image_format() {
        let mut config = AppConfig::default();
        config.backends.asset_studio.image_format = Some("gif".to_string());
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "backends.asset_studio.image_format" && value == "gif"
        ));
    }

    #[test]
    fn accepts_raw_rgba_asset_studio_image_format() {
        let mut config = AppConfig::default();
        config.backends.asset_studio.image_format = Some("raw_rgba".to_string());
        config.validate().unwrap();
    }

    #[test]
    fn rejects_unimplemented_pjsk_read_kinds() {
        // Model packages and motion clips flow through the haruki_3d raw-bundle
        // pipeline. Accepting them here would silently drop every
        // Animator/AnimationClip export.
        for (asset_type, kind) in [
            ("Animator", "pjsk_model_package"),
            ("AnimationClip", "pjsk_animation_clip_decoded"),
        ] {
            let mut config = AppConfig::default();
            config
                .backends
                .asset_studio
                .read_kinds
                .insert(asset_type.to_string(), kind.to_string());
            config.validate().unwrap_err();
        }
    }

    #[test]
    fn rejects_invalid_asset_studio_read_kind() {
        let mut config = AppConfig::default();
        config
            .backends
            .asset_studio
            .read_kinds
            .insert("Sprite".to_string(), "thumbnail".to_string());
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "backends.asset_studio.read_kinds.Sprite" && value == "thumbnail"
        ));
    }

    #[test]
    fn parses_configured_asset_studio_export_types() {
        let yaml = r#"
asset_studio_types:
  - monoBehaviour
  - textAsset
  - font
"#;

        let export: RegionExportConfig = yaml_serde::from_str(yaml).unwrap();

        assert_eq!(
            export.asset_studio_types,
            vec![
                "monoBehaviour".to_string(),
                "textAsset".to_string(),
                "font".to_string()
            ]
        );
    }

    #[test]
    fn parses_raw_bundle_export_config() {
        let yaml = r#"
raw_bundles:
  output_dir: /data/assets/jp-assets/AssetBundles
  include:
    - ^live_pv/model/characterv2/
  exclude:
    - /debug/
"#;

        let export: RegionExportConfig = yaml_serde::from_str(yaml).unwrap();
        let raw_bundles = export.raw_bundles.unwrap();

        assert_eq!(
            raw_bundles.output_dir.as_deref(),
            Some("/data/assets/jp-assets/AssetBundles")
        );
        assert_eq!(
            raw_bundles.include,
            vec!["^live_pv/model/characterv2/".to_string()]
        );
        assert_eq!(raw_bundles.exclude, vec!["/debug/".to_string()]);
    }

    #[test]
    fn parses_haruki_3d_export_config() {
        let yaml = r#"
haruki_3d:
  enabled: true
  exporter_path: /app/haruki-3d/exporter/Haruki-3D-Exporter
  master_dir: /app/data/masterdata
  work_dir: /app/data/3d-work
  manifest_file: /app/data/3d-output/haruki-3d-export-manifest.json
  output_dir: /app/data/3d-output
  shared_content_store: /app/data/3d-output-cas
  compiled_content_store: /app/data/3d-compiled-cache
  process_concurrency: 16
  convert_model_textures: true
  role_character3d_ids:
    - 5
  include:
    - ^live_pv/model/characterv2/
  exclude:
    - /debug/
  cleanup_work_dir_after_success: true
  cleanup_work_dir_after_failure: false
"#;

        let export: RegionExportConfig = yaml_serde::from_str(yaml).unwrap();

        assert!(export.haruki_3d.enabled);
        assert_eq!(
            export.haruki_3d.exporter_path,
            "/app/haruki-3d/exporter/Haruki-3D-Exporter"
        );
        assert_eq!(export.haruki_3d.work_dir, "/app/data/3d-work");
        assert_eq!(
            export.haruki_3d.manifest_file,
            "/app/data/3d-output/haruki-3d-export-manifest.json"
        );
        assert_eq!(export.haruki_3d.output_dir, "/app/data/3d-output");
        assert_eq!(
            export.haruki_3d.shared_content_store,
            "/app/data/3d-output-cas"
        );
        assert_eq!(
            export.haruki_3d.compiled_content_store,
            "/app/data/3d-compiled-cache"
        );
        assert_eq!(export.haruki_3d.process_concurrency, 16);
        assert!(export.haruki_3d.convert_model_textures);
        assert_eq!(export.haruki_3d.role_character3d_ids, vec![5]);
        assert_eq!(
            export.haruki_3d.include,
            vec!["^live_pv/model/characterv2/".to_string()]
        );
        assert_eq!(export.haruki_3d.exclude, vec!["/debug/".to_string()]);
        assert!(export.haruki_3d.cleanup_work_dir_after_success);
        assert!(!export.haruki_3d.cleanup_work_dir_after_failure);
    }

    #[test]
    fn example_config_advertises_current_haruki_3d_pipeline_selectors() {
        let config_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("haruki-asset-configs.example.yaml");
        let config = AppConfig::load_from_path(config_path).unwrap();
        let asset_studio = &config.backends.asset_studio;
        assert_eq!(asset_studio.read_kinds.get("Animator"), None);
        assert_eq!(
            asset_studio.read_kinds.get("AnimationClip"),
            None,
            "unconfigured types should use unity-rs TypeTree/raw fallback"
        );

        let jp = config.regions.get("jp").expect("jp region exists");
        assert!(
            jp.export
                .asset_studio_types
                .iter()
                .any(|value| value.eq_ignore_ascii_case("all")),
            "jp asset_studio_types should request every unity-rs readable object"
        );
        assert_eq!(jp.filters.start_app, vec![".*"]);
        assert_eq!(jp.filters.on_demand, vec![".*"]);

        assert!(
            jp.export.raw_bundles.is_none(),
            "the default example must not enable legacy raw bundle retention"
        );

        let haruki_3d = &jp.export.haruki_3d;
        for expected in [
            "live_pv/model/characterv2/body/",
            "live_pv/model/characterv2/face/",
            "live_pv/model/characterv2/head_optional/",
            "live_pv/model/characterv2/color_variation/body/",
            "live_pv/model/characterv2/color_variation/face/",
            "live_pv/model/characterv2/color_variation/head_optional/",
            "live_pv/model/character/head_optional/",
            "live_pv/model/character/color_variation/head_optional/",
            "character/motion/costume_setting/",
        ] {
            assert!(
                haruki_3d
                    .include
                    .iter()
                    .any(|value| value.contains(expected)),
                "haruki_3d.include should retain {expected}"
            );
        }

        assert!(
            haruki_3d.master_dir.contains("haruki-sekai-master/master"),
            "haruki_3d.master_dir should point at the upstream masterdata checkout"
        );
        assert!(
            haruki_3d.output_dir.contains("3d-output"),
            "haruki_3d.output_dir should point at a stable runtime root"
        );
        assert!(
            haruki_3d.manifest_file.contains("3d-output"),
            "haruki_3d.manifest_file should live beside the stable runtime root"
        );
        assert!(
            haruki_3d.role_character3d_ids.contains(&5),
            "haruki_3d.role_character3d_ids should include a v1 smoke role runtime"
        );
        assert_eq!(
            haruki_3d.process_concurrency, 0,
            "haruki_3d.process_concurrency should default to exporter auto in the example config"
        );
        for expected in [
            "live_pv/model/characterv2/body/",
            "live_pv/model/characterv2/face/",
            "live_pv/model/characterv2/head_optional/",
            "live_pv/model/characterv2/color_variation/body/",
            "live_pv/model/characterv2/color_variation/face/",
            "live_pv/model/characterv2/color_variation/head_optional/",
            "live_pv/model/character/head_optional/",
            "live_pv/model/character/color_variation/head_optional/",
            "character/motion/costume_setting/",
        ] {
            assert!(
                haruki_3d
                    .include
                    .iter()
                    .any(|value| value.contains(expected)),
                "haruki_3d.include should stage {expected}"
            );
        }
    }

    #[test]
    fn load_from_path_expands_env_references_across_config_values() {
        let _env_lock = env_lock();
        std::env::set_var(
            "HARUKI_TEST_AES_KEY_HEX",
            "00112233445566778899aabbccddeeff",
        );
        std::env::set_var("HARUKI_TEST_AES_IV_HEX", "0102030405060708090a0b0c0d0e0f10");
        std::env::set_var("HARUKI_TEST_BEARER_TOKEN", "secret-token");

        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 3
server:
  auth:
    bearer_token: "${{env:HARUKI_TEST_BEARER_TOKEN}}"
logging:
  access:
    format: "[${{time}}] ${{status}}"
regions:
  jp:
    enabled: true
    provider:
      kind: colorful_palette
      asset_info_url_template: "https://example.com/{{env}}/{{asset_version}}/{{asset_hash}}"
      asset_bundle_url_template: "https://example.com/assets/{{bundle_path}}"
      profile: production
      profile_hashes:
        production: abc123
    crypto:
      aes_key_hex: "${{env:HARUKI_TEST_AES_KEY_HEX}}"
      aes_iv_hex: "${{env:HARUKI_TEST_AES_IV_HEX}}"
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(
            config.server.auth.bearer_token.as_deref(),
            Some("secret-token")
        );
        assert_eq!(
            config.regions["jp"].crypto.aes_key_hex.as_deref(),
            Some("00112233445566778899aabbccddeeff")
        );
        assert_eq!(
            config.regions["jp"].crypto.aes_iv_hex.as_deref(),
            Some("0102030405060708090a0b0c0d0e0f10")
        );
        assert_eq!(config.logging.access.format, "[${time}] ${status}");

        std::env::remove_var("HARUKI_TEST_AES_KEY_HEX");
        std::env::remove_var("HARUKI_TEST_AES_IV_HEX");
        std::env::remove_var("HARUKI_TEST_BEARER_TOKEN");
    }

    #[test]
    fn load_from_path_applies_asset_studio_env_overrides() {
        let _env_lock = env_lock();
        let old_media_backend = std::env::var("HARUKI_MEDIA_BACKEND").ok();
        let old_read_batch_size = std::env::var("HARUKI_ASSET_STUDIO_READ_BATCH_SIZE").ok();
        let old_image_format = std::env::var("HARUKI_ASSET_STUDIO_IMAGE_FORMAT").ok();
        let old_media_encode_concurrency = std::env::var("HARUKI_MEDIA_ENCODE_CONCURRENCY").ok();
        let old_download_concurrency = std::env::var("HARUKI_DOWNLOAD_CONCURRENCY").ok();
        let old_post_process_concurrency = std::env::var("HARUKI_POST_PROCESS_CONCURRENCY").ok();
        let old_concurrency_auto_tune = std::env::var("HARUKI_CONCURRENCY_AUTO_TUNE").ok();
        let old_cpu_budget_auto = std::env::var("HARUKI_CPU_BUDGET_AUTO").ok();
        let old_cpu_budget_ratio = std::env::var("HARUKI_CPU_BUDGET_RATIO").ok();
        let old_cpu_reserved = std::env::var("HARUKI_CPU_RESERVED").ok();
        let old_cpu_throttle_enabled = std::env::var("HARUKI_CPU_THROTTLE_ENABLED").ok();
        let old_cpu_throttle_sample_ms = std::env::var("HARUKI_CPU_THROTTLE_SAMPLE_MS").ok();
        let old_max_in_flight_bundle_bytes =
            std::env::var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES").ok();
        std::env::set_var("HARUKI_MEDIA_BACKEND", "cli");
        std::env::set_var("HARUKI_ASSET_STUDIO_READ_BATCH_SIZE", "48");
        std::env::set_var("HARUKI_ASSET_STUDIO_IMAGE_FORMAT", "raw_rgba");
        std::env::set_var("HARUKI_MEDIA_ENCODE_CONCURRENCY", "9");
        std::env::set_var("HARUKI_DOWNLOAD_CONCURRENCY", "11");
        std::env::set_var("HARUKI_POST_PROCESS_CONCURRENCY", "13");
        std::env::set_var("HARUKI_CONCURRENCY_AUTO_TUNE", "true");
        std::env::set_var("HARUKI_CPU_BUDGET_AUTO", "true");
        std::env::set_var("HARUKI_CPU_BUDGET_RATIO", "0.5");
        std::env::set_var("HARUKI_CPU_RESERVED", "2");
        std::env::set_var("HARUKI_CPU_THROTTLE_ENABLED", "true");
        std::env::set_var("HARUKI_CPU_THROTTLE_SAMPLE_MS", "500");
        std::env::set_var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES", "1048576");

        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 3
backends:
  asset_studio:
    read_batch_size: 16
    image_format: raw_rgba
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(config.backends.media.backend, MediaBackend::Cli);
        assert_eq!(config.backends.asset_studio.read_batch_size, 48);
        assert_eq!(
            config.backends.asset_studio.image_format.as_deref(),
            Some("raw_rgba")
        );
        assert_eq!(config.concurrency.media_encode, 9);
        assert_eq!(config.concurrency.audio_encode, 9);
        assert_eq!(config.concurrency.video_encode, 9);
        assert_eq!(config.concurrency.download, 11);
        assert_eq!(config.concurrency.post_process, 13);
        assert!(config.concurrency.auto_tune);
        assert!(config.resources.cpu.budget_auto);
        assert_eq!(config.resources.cpu.budget_ratio, 0.5);
        assert_eq!(config.resources.cpu.reserved, 2);
        assert!(config.resources.cpu.throttle.enabled);
        assert_eq!(config.resources.cpu.throttle.sample_ms, 500);
        assert_eq!(
            config.resources.memory.max_in_flight_bundle_bytes,
            1_048_576
        );

        match old_media_backend {
            Some(value) => std::env::set_var("HARUKI_MEDIA_BACKEND", value),
            None => std::env::remove_var("HARUKI_MEDIA_BACKEND"),
        }
        match old_read_batch_size {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_READ_BATCH_SIZE", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_READ_BATCH_SIZE"),
        }
        match old_image_format {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_IMAGE_FORMAT", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_IMAGE_FORMAT"),
        }
        match old_media_encode_concurrency {
            Some(value) => std::env::set_var("HARUKI_MEDIA_ENCODE_CONCURRENCY", value),
            None => std::env::remove_var("HARUKI_MEDIA_ENCODE_CONCURRENCY"),
        }
        match old_download_concurrency {
            Some(value) => std::env::set_var("HARUKI_DOWNLOAD_CONCURRENCY", value),
            None => std::env::remove_var("HARUKI_DOWNLOAD_CONCURRENCY"),
        }
        match old_post_process_concurrency {
            Some(value) => std::env::set_var("HARUKI_POST_PROCESS_CONCURRENCY", value),
            None => std::env::remove_var("HARUKI_POST_PROCESS_CONCURRENCY"),
        }
        match old_concurrency_auto_tune {
            Some(value) => std::env::set_var("HARUKI_CONCURRENCY_AUTO_TUNE", value),
            None => std::env::remove_var("HARUKI_CONCURRENCY_AUTO_TUNE"),
        }
        match old_cpu_budget_auto {
            Some(value) => std::env::set_var("HARUKI_CPU_BUDGET_AUTO", value),
            None => std::env::remove_var("HARUKI_CPU_BUDGET_AUTO"),
        }
        match old_cpu_budget_ratio {
            Some(value) => std::env::set_var("HARUKI_CPU_BUDGET_RATIO", value),
            None => std::env::remove_var("HARUKI_CPU_BUDGET_RATIO"),
        }
        match old_cpu_reserved {
            Some(value) => std::env::set_var("HARUKI_CPU_RESERVED", value),
            None => std::env::remove_var("HARUKI_CPU_RESERVED"),
        }
        match old_cpu_throttle_enabled {
            Some(value) => std::env::set_var("HARUKI_CPU_THROTTLE_ENABLED", value),
            None => std::env::remove_var("HARUKI_CPU_THROTTLE_ENABLED"),
        }
        match old_cpu_throttle_sample_ms {
            Some(value) => std::env::set_var("HARUKI_CPU_THROTTLE_SAMPLE_MS", value),
            None => std::env::remove_var("HARUKI_CPU_THROTTLE_SAMPLE_MS"),
        }
        match old_max_in_flight_bundle_bytes {
            Some(value) => std::env::set_var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES", value),
            None => std::env::remove_var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES"),
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn load_default_reads_config_from_opendal_fs_uri() {
        let _env_lock = env_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("haruki-asset-configs.yaml"),
            r#"
config_version: 3
server:
  port: 19090
regions:
  cn:
    enabled: true
    provider:
      kind: nuverse
      asset_version_url: "https://example.com/version"
      app_version: "5.2.0"
      asset_info_url_template: "https://example.com/info/{asset_version}"
      asset_bundle_url_template: "https://example.com/{bundle_path}"
"#,
        )
        .unwrap();

        let old_config_uri = std::env::var("HARUKI_CONFIG_URI").ok();
        let old_scheme = std::env::var("HARUKI_CONFIG_OPENDAL_SCHEME").ok();
        let old_root = std::env::var("HARUKI_CONFIG_OPENDAL_ROOT").ok();
        std::env::set_var(
            "HARUKI_CONFIG_URI",
            "opendal://config/haruki-asset-configs.yaml",
        );
        std::env::set_var("HARUKI_CONFIG_OPENDAL_SCHEME", "fs");
        std::env::set_var("HARUKI_CONFIG_OPENDAL_ROOT", dir.path());

        let config = AppConfig::load_default().await.unwrap();
        assert_eq!(config.server.port, 19090);
        assert_eq!(config.enabled_regions(), vec!["cn".to_string()]);

        restore_env("HARUKI_CONFIG_URI", old_config_uri);
        restore_env("HARUKI_CONFIG_OPENDAL_SCHEME", old_scheme);
        restore_env("HARUKI_CONFIG_OPENDAL_ROOT", old_root);
    }

    #[test]
    fn load_from_path_applies_double_underscore_env_overrides() {
        let _env_lock = env_lock();
        let old_port = std::env::var("HARUKI__SERVER__PORT").ok();
        let old_provider = std::env::var("HARUKI__REGIONS__JP__UPLOAD__PROVIDERS__0").ok();
        let old_bucket = std::env::var("HARUKI__STORAGE__PROVIDERS__0__OPTIONS__BUCKET").ok();

        std::env::set_var("HARUKI__SERVER__PORT", "19091");
        std::env::set_var("HARUKI__REGIONS__JP__UPLOAD__PROVIDERS__0", "assets");
        std::env::set_var(
            "HARUKI__STORAGE__PROVIDERS__0__OPTIONS__BUCKET",
            "sekai-jp-assets",
        );

        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 3
storage:
  providers:
    - name: assets
      scheme: s3
      options:
        endpoint: https://s3.example.com
regions:
  jp:
    enabled: true
    upload:
      enabled: true
    provider:
      kind: colorful_palette
      asset_info_url_template: "https://example.com/{{env}}/{{asset_version}}/{{asset_hash}}"
      asset_bundle_url_template: "https://example.com/assets/{{bundle_path}}"
      profile: production
      profile_hashes:
        production: abc123
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(config.server.port, 19091);
        assert_eq!(
            config.regions["jp"].upload.providers,
            vec!["assets".to_string()]
        );
        assert_eq!(
            config.storage.providers[0].options.get("bucket"),
            Some(&"sekai-jp-assets".to_string())
        );

        restore_env("HARUKI__SERVER__PORT", old_port);
        restore_env("HARUKI__REGIONS__JP__UPLOAD__PROVIDERS__0", old_provider);
        restore_env("HARUKI__STORAGE__PROVIDERS__0__OPTIONS__BUCKET", old_bucket);
    }

    #[test]
    fn effective_concurrency_auto_tune_respects_configured_caps() {
        let config = ConcurrencyConfig {
            auto_tune: true,
            download: 999,
            upload: 999,
            post_process: 999,
            acb: 999,
            usm: 999,
            hca: 999,
            media_encode: 999,
            audio_encode: 999,
            video_encode: 999,
            images: 999,
        };

        let effective = config.effective();

        assert!(effective.auto_tune);
        assert!(effective.download <= config.download);
        assert!(effective.upload <= config.upload);
        assert!(effective.post_process <= config.post_process);
        assert!(effective.acb <= config.acb);
        assert!(effective.usm <= config.usm);
        assert!(effective.hca <= config.hca);
        assert!(effective.media_encode <= config.media_encode);
        assert!(effective.audio_encode <= config.audio_encode);
        assert!(effective.video_encode <= config.video_encode);
        assert!(effective.images <= config.images);
        assert!(effective.download >= 1);
        assert!(effective.post_process >= 1);
        assert!(effective.media_encode >= 1);
        assert!(effective.audio_encode >= 1);
        assert!(effective.video_encode >= 1);
    }

    #[test]
    fn effective_concurrency_widens_cpu_pools_on_wide_hosts() {
        let config = ConcurrencyConfig {
            auto_tune: true,
            ..ConcurrencyConfig::default()
        };

        let effective = config.effective_for_cpus_with_budget(64, 64);

        // The pools that measurably bound a rule on the 64-core host.
        assert_eq!(effective.audio_encode, 64, "music was pinned at 12 cores");
        assert_eq!(
            effective.post_process, 64,
            "audio and image work runs inside a post-process slot, so this pool \
             caps every other CPU pool and must not be narrower than the budget"
        );
        assert_eq!(effective.video_encode, 16, "x264 is ~6 threads per encoder");
        // The rest of the CPU-bound pools follow the budget.
        assert_eq!(effective.images, 64);
        assert_eq!(effective.acb, 64);
        assert_eq!(effective.hca, 64);
        assert_eq!(effective.media_encode, 64);
        assert_eq!(effective.usm, 32);
        // Network pools stay where the operator put them; the remote endpoint
        // is their ceiling, not this host.
        assert_eq!(effective.download, config.download);
        assert_eq!(effective.upload, config.upload);
    }

    #[test]
    fn effective_concurrency_unchanged_on_hosts_the_defaults_were_tuned_for() {
        // The shipped defaults were hand-tuned on a 10-core host. Widening must
        // be inert at and below that size, so this reproduces the cap-only
        // formula that predates it and demands byte-identical output.
        fn caps_only(
            config: &ConcurrencyConfig,
            cpus: usize,
            cpu_budget: usize,
        ) -> ConcurrencyConfig {
            let cpus = cpus.max(1);
            let cpu_budget = cpu_budget.max(1);
            let oversubscribe = cpu_budget.saturating_mul(2).max(cpu_budget);
            ConcurrencyConfig {
                auto_tune: true,
                download: config.download.min(cpus.saturating_mul(4).max(4)).max(1),
                upload: config.upload.min(cpus.max(2)).max(1),
                post_process: if config.post_process == 0 {
                    0
                } else {
                    config
                        .post_process
                        .min(cpus.saturating_mul(2).max(2))
                        .max(1)
                },
                acb: config.acb.min(oversubscribe).max(1),
                usm: config.usm.min(cpus.max(2)).max(1),
                hca: config
                    .hca
                    .min(cpus.saturating_mul(2).max(2))
                    .min(oversubscribe)
                    .max(1),
                media_encode: config.media_encode.min(oversubscribe).max(1),
                audio_encode: config.audio_encode.min(oversubscribe).max(1),
                video_encode: config.video_encode.min(cpus.div_ceil(4).max(1)).max(1),
                images: config.images.min(oversubscribe).max(1),
            }
        }

        let config = ConcurrencyConfig {
            auto_tune: true,
            ..ConcurrencyConfig::default()
        };

        for cpus in 1..=12 {
            let widened = config.effective_for_cpus_with_budget(cpus, cpus);
            let previous = caps_only(&config, cpus, cpus);
            assert_eq!(
                format!("{widened:?}"),
                format!("{previous:?}"),
                "widening changed behaviour on a {cpus}-core host"
            );
        }
    }

    #[test]
    fn effective_concurrency_widening_respects_a_reduced_cpu_budget() {
        // A half budget on a wide host is an explicit "use half this machine";
        // widening must size to the budget, not to the core count.
        let config = ConcurrencyConfig {
            auto_tune: true,
            ..ConcurrencyConfig::default()
        };

        let effective = config.effective_for_cpus_with_budget(64, 16);

        assert_eq!(effective.audio_encode, 16);
        assert_eq!(effective.images, 16);
        assert_eq!(effective.media_encode, 16);
        assert_eq!(effective.post_process, 16);
    }

    #[test]
    fn effective_concurrency_preserves_zero_post_process_as_auto() {
        let config = ConcurrencyConfig {
            auto_tune: true,
            post_process: 0,
            ..ConcurrencyConfig::default()
        };

        assert_eq!(config.effective_for_cpus_with_budget(8, 8).post_process, 0);
    }

    #[test]
    fn rejects_invalid_cpu_budget_ratio() {
        for ratio in [0.0, -0.5, 1.5] {
            let mut config = AppConfig::default();
            config.resources.cpu.budget_ratio = ratio;
            let err = config.validate().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidValue { ref field, .. }
                    if field == "resources.cpu.budget_ratio"
            ));
        }
    }

    #[test]
    fn load_from_path_errors_when_secret_env_reference_is_missing() {
        std::env::remove_var("HARUKI_TEST_MISSING_AES_KEY");
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 3
regions:
  jp:
    enabled: true
    provider:
      kind: colorful_palette
      asset_info_url_template: "https://example.com/{{env}}/{{asset_version}}/{{asset_hash}}"
      asset_bundle_url_template: "https://example.com/assets/{{bundle_path}}"
      profile: production
      profile_hashes:
        production: abc123
    crypto:
      aes_key_hex: "${{env:HARUKI_TEST_MISSING_AES_KEY}}"
      aes_iv_hex: "0102030405060708090a0b0c0d0e0f10"
"#
        )
        .unwrap();

        let err = AppConfig::load_from_path(file.path()).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::MissingEnvironmentVariable { ref name, .. }
            if name == "HARUKI_TEST_MISSING_AES_KEY"
        ));
    }

    fn restore_env(name: &str, value: Option<String>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }
}
