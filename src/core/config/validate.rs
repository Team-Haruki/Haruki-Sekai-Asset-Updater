//! Rules a config must satisfy to be runnable.
//!
//! Everything here rejects; nothing here mutates shape. A config that parses
//! but cannot run -- an unknown image format, a region whose filters do not
//! compile -- fails at this layer rather than at first use.

use std::collections::BTreeMap;

use crate::core::errors::ConfigError;

use super::schema::{
    AppConfig, AudioExportConfig, AuthConfig, CryptoConfig, Haruki3dExportConfig, ImageBackend,
    ImageBackendConfig, ImageExportConfig, MediaBackend, MediaBackendConfig, RegionConfig,
    VideoExportConfig,
};

pub(super) fn validate_region_names(config: &AppConfig) -> Result<(), ConfigError> {
    for region_name in config.regions.keys() {
        if region_name.to_lowercase() != *region_name {
            return Err(ConfigError::InvalidRegionName(region_name.clone()));
        }
    }
    Ok(())
}

pub(super) fn validate_runtime_settings(config: &AppConfig) -> Result<(), ConfigError> {
    let budget_ratio = config.resources.cpu.budget_ratio;
    if !(0.0..=1.0).contains(&budget_ratio) || budget_ratio == 0.0 {
        return Err(ConfigError::InvalidValue {
            field: "resources.cpu.budget_ratio".to_string(),
            value: budget_ratio.to_string(),
            expected: "a number greater than 0 and less than or equal to 1".to_string(),
        });
    }
    validate_positive_setting(
        "backends.asset_studio.read_batch_size",
        config.backends.asset_studio.read_batch_size,
    )?;
    validate_positive_setting("concurrency.media_encode", config.concurrency.media_encode)?;
    validate_positive_setting("concurrency.audio_encode", config.concurrency.audio_encode)?;
    validate_positive_setting("concurrency.video_encode", config.concurrency.video_encode)?;
    if let Some(image_format) = &config.backends.asset_studio.image_format {
        validate_asset_studio_image_format(image_format)?;
    }
    validate_image_backend(&config.backends.image)
}

pub(super) fn validate_positive_setting(field: &str, value: usize) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::InvalidValue {
            field: field.to_string(),
            value: "0".to_string(),
            expected: "a positive integer".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_auth_config(auth: &AuthConfig) -> Result<(), ConfigError> {
    if !auth.enabled {
        return Ok(());
    }
    let has_credential = auth
        .bearer_token
        .as_deref()
        .is_some_and(|token| !token.is_empty())
        || auth
            .user_agent_prefix
            .as_deref()
            .is_some_and(|prefix| !prefix.is_empty());
    if has_credential {
        return Ok(());
    }
    Err(ConfigError::InvalidValue {
        field: "server.auth".to_string(),
        value: "enabled=true with no credentials".to_string(),
        expected: "a non-empty bearer_token or user_agent_prefix when auth is enabled".to_string(),
    })
}

pub(super) fn validate_regions(config: &AppConfig) -> Result<(), ConfigError> {
    for (region_name, region) in &config.regions {
        validate_image_export_config(region_name, &region.export.images)?;
        validate_video_export_config(region_name, &region.export.video)?;
        validate_audio_export_config(region_name, &region.export.audio)?;
        validate_haruki_3d_export_config(region_name, &region.export.haruki_3d)?;
        if region.enabled {
            validate_region_crypto(region_name, &region.crypto)?;
            validate_region_filter_regexes(region_name, region)?;
        }
    }
    Ok(())
}

pub(super) fn validate_region_crypto(
    region_name: &str,
    crypto: &CryptoConfig,
) -> Result<(), ConfigError> {
    if let Some(key_hex) = &crypto.aes_key_hex {
        validate_aes_hex(
            &format!("regions.{region_name}.crypto.aes_key_hex"),
            key_hex,
            &[16, 24, 32],
        )?;
    }
    if let Some(iv_hex) = &crypto.aes_iv_hex {
        validate_aes_hex(
            &format!("regions.{region_name}.crypto.aes_iv_hex"),
            iv_hex,
            &[16],
        )?;
    }
    Ok(())
}

pub(super) fn validate_aes_hex(
    field: &str,
    value: &str,
    allowed_lengths: &[usize],
) -> Result<(), ConfigError> {
    let bytes = hex::decode(value).map_err(|_| ConfigError::InvalidValue {
        field: field.to_string(),
        value: "<redacted>".to_string(),
        expected: "a valid hexadecimal string".to_string(),
    })?;
    if !allowed_lengths.contains(&bytes.len()) {
        return Err(ConfigError::InvalidValue {
            field: field.to_string(),
            value: format!("{} byte(s)", bytes.len()),
            expected: format!("hex decoding to one of {allowed_lengths:?} bytes"),
        });
    }
    Ok(())
}

pub(super) fn validate_region_filter_regexes(
    region_name: &str,
    region: &RegionConfig,
) -> Result<(), ConfigError> {
    let filters = &region.filters;
    validate_regex_patterns(
        &format!("regions.{region_name}.filters.start_app"),
        &filters.start_app,
    )?;
    validate_regex_patterns(
        &format!("regions.{region_name}.filters.on_demand"),
        &filters.on_demand,
    )?;
    validate_regex_patterns(
        &format!("regions.{region_name}.filters.skip"),
        &filters.skip,
    )?;
    validate_regex_patterns(
        &format!("regions.{region_name}.filters.priority"),
        &filters.priority,
    )?;
    if let Some(raw_bundles) = &region.export.raw_bundles {
        validate_regex_patterns(
            &format!("regions.{region_name}.export.raw_bundles.include"),
            &raw_bundles.include,
        )?;
        validate_regex_patterns(
            &format!("regions.{region_name}.export.raw_bundles.exclude"),
            &raw_bundles.exclude,
        )?;
    }
    Ok(())
}

pub(super) fn validate_regex_patterns(field: &str, patterns: &[String]) -> Result<(), ConfigError> {
    for pattern in patterns {
        regex::Regex::new(pattern).map_err(|source| ConfigError::InvalidValue {
            field: field.to_string(),
            value: pattern.clone(),
            expected: format!("a valid regular expression ({source})"),
        })?;
    }
    Ok(())
}

pub(super) fn normalize_asset_studio_image_format(value: &str) -> Result<String, ConfigError> {
    let normalized = value.trim().to_lowercase();
    validate_asset_studio_image_format(&normalized)?;
    Ok(normalized)
}

pub(super) fn validate_asset_studio_image_format(value: &str) -> Result<(), ConfigError> {
    match value.trim().to_lowercase().as_str() {
        "raw_rgba" => Ok(()),
        other => Err(ConfigError::InvalidValue {
            field: "backends.asset_studio.image_format".to_string(),
            value: other.to_string(),
            expected: "raw_rgba".to_string(),
        }),
    }
}

pub(super) fn validate_image_backend(image: &ImageBackendConfig) -> Result<(), ConfigError> {
    match image.backend {
        ImageBackend::Rust => {}
    }
    if !(1..=100).contains(&image.jpeg_quality) {
        return Err(ConfigError::InvalidValue {
            field: "backends.image.jpeg_quality".to_string(),
            value: image.jpeg_quality.to_string(),
            expected: "an integer from 1 to 100".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_image_export_config(
    region_name: &str,
    images: &ImageExportConfig,
) -> Result<(), ConfigError> {
    let formats = images.output_formats();
    if formats.is_empty() {
        return Err(ConfigError::InvalidValue {
            field: format!("regions.{region_name}.export.images.formats"),
            value: "[]".to_string(),
            expected: "at least one of png, jpg, or webp".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_video_export_config(
    region_name: &str,
    video: &VideoExportConfig,
) -> Result<(), ConfigError> {
    let formats = video.output_formats();
    if formats.is_empty() {
        return Err(ConfigError::InvalidValue {
            field: format!("regions.{region_name}.export.video.formats"),
            value: "[]".to_string(),
            expected: "at least one of m2v or mp4".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_audio_export_config(
    region_name: &str,
    audio: &AudioExportConfig,
) -> Result<(), ConfigError> {
    let formats = audio.output_formats();
    if formats.is_empty() {
        return Err(ConfigError::InvalidValue {
            field: format!("regions.{region_name}.export.audio.formats"),
            value: "[]".to_string(),
            expected: "at least one of wav, flac, or mp3".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_haruki_3d_export_config(
    region_name: &str,
    haruki_3d: &Haruki3dExportConfig,
) -> Result<(), ConfigError> {
    if !haruki_3d.enabled {
        return Ok(());
    }
    for (field, value) in [
        ("exporter_path", &haruki_3d.exporter_path),
        ("master_dir", &haruki_3d.master_dir),
        ("output_dir", &haruki_3d.output_dir),
        ("manifest_file", &haruki_3d.manifest_file),
    ] {
        if value.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                field: format!("regions.{region_name}.export.haruki_3d.{field}"),
                value: value.clone(),
                expected: "a non-empty path".to_string(),
            });
        }
    }
    if haruki_3d.work_dir.trim().is_empty() && haruki_3d.staging_dir.trim().is_empty() {
        return Err(ConfigError::InvalidValue {
            field: format!("regions.{region_name}.export.haruki_3d.work_dir"),
            value: haruki_3d.work_dir.clone(),
            expected: "a non-empty path".to_string(),
        });
    }
    if haruki_3d.include.is_empty() {
        return Err(ConfigError::InvalidValue {
            field: format!("regions.{region_name}.export.haruki_3d.include"),
            value: "[]".to_string(),
            expected: "at least one include pattern".to_string(),
        });
    }
    if let Some(value) = haruki_3d
        .role_character3d_ids
        .iter()
        .find(|value| **value <= 0)
    {
        return Err(ConfigError::InvalidValue {
            field: format!("regions.{region_name}.export.haruki_3d.role_character3d_ids"),
            value: value.to_string(),
            expected: "positive character3d ids".to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_asset_studio_read_kinds(
    read_kinds: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    for (asset_type, kind) in read_kinds {
        if asset_type.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "backends.asset_studio.read_kinds".to_string(),
                value: asset_type.clone(),
                expected: "non-empty AssetStudio type selector".to_string(),
            });
        }
        validate_asset_studio_read_kind(
            &format!("backends.asset_studio.read_kinds.{asset_type}"),
            kind,
        )?;
    }
    Ok(())
}

pub(super) fn warn_media_fallback_backend_options(media: &MediaBackendConfig) {
    match media.backend {
        MediaBackend::Ffi => {}
        MediaBackend::Cli => {
            tracing::warn!("backends.media.backend=cli is a fallback mode; production Linux builds should prefer ffi")
        }
        MediaBackend::Auto => tracing::warn!(
            "backends.media.backend=auto is a fallback mode; production Linux builds should prefer ffi"
        ),
    }
}

pub(super) fn validate_asset_studio_read_kind(field: &str, value: &str) -> Result<(), ConfigError> {
    match value.trim().to_lowercase().as_str() {
        "auto" | "raw" | "typetree_json" | "image" | "image_archive" | "audio" | "video"
        | "font" | "shader" | "text" | "text_bytes" | "mesh" | "obj" | "animator" | "fbx" => {
            Ok(())
        }
        other => Err(ConfigError::InvalidValue {
            field: field.to_string(),
            value: other.to_string(),
            expected: "auto, raw, typetree_json, image, image_archive, audio, video, font, shader, text, text_bytes, mesh, obj, animator, or fbx".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::errors::ConfigError;

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
    fn validates_names_auth_crypto_filters_formats_and_haruki_3d_fields() {
        let mut config = AppConfig::default();
        config
            .regions
            .insert("JP".to_string(), RegionConfig::default());
        assert!(matches!(
            validate_region_names(&config),
            Err(ConfigError::InvalidRegionName(name)) if name == "JP"
        ));

        assert!(validate_auth_config(&AuthConfig::default()).is_ok());
        let mut auth = AuthConfig {
            enabled: true,
            ..AuthConfig::default()
        };
        assert!(validate_auth_config(&auth).is_err());
        auth.bearer_token = Some("token".to_string());
        assert!(validate_auth_config(&auth).is_ok());
        auth.bearer_token = None;
        auth.user_agent_prefix = Some("Haruki".to_string());
        assert!(validate_auth_config(&auth).is_ok());

        assert!(validate_aes_hex("key", "not-hex", &[16]).is_err());
        assert!(validate_aes_hex("key", "00", &[16]).is_err());
        assert!(validate_aes_hex("key", &"00".repeat(16), &[16]).is_ok());
        let crypto = CryptoConfig {
            aes_key_hex: Some("00".repeat(32)),
            aes_iv_hex: Some("00".repeat(16)),
        };
        assert!(validate_region_crypto("jp", &crypto).is_ok());

        let mut region = RegionConfig {
            enabled: true,
            ..RegionConfig::default()
        };
        region.filters.start_app = vec!["[".to_string()];
        assert!(validate_region_filter_regexes("jp", &region).is_err());
        region.filters.start_app = vec!["^start/".to_string()];
        region.filters.on_demand = vec!["^ondemand/".to_string()];
        region.filters.skip = vec!["skip$".to_string()];
        region.filters.priority = vec!["priority".to_string()];
        region.export.raw_bundles = Some(super::super::schema::RawBundleExportConfig {
            output_dir: None,
            include: vec!["^raw/".to_string()],
            exclude: vec!["tmp$".to_string()],
        });
        assert!(validate_region_filter_regexes("jp", &region).is_ok());
        region.export.raw_bundles.as_mut().unwrap().exclude = vec!["(".to_string()];
        assert!(validate_region_filter_regexes("jp", &region).is_err());

        assert!(
            validate_image_export_config("jp", &ImageExportConfig { formats: vec![] }).is_err()
        );
        assert!(validate_video_export_config(
            "jp",
            &VideoExportConfig {
                formats: vec![],
                direct_mp4: false,
            }
        )
        .is_err());
        assert!(
            validate_audio_export_config("jp", &AudioExportConfig { formats: vec![] }).is_err()
        );

        let mut haruki = Haruki3dExportConfig {
            enabled: true,
            ..Haruki3dExportConfig::default()
        };
        for field in ["exporter_path", "master_dir", "output_dir", "manifest_file"] {
            haruki.exporter_path = "/exporter".to_string();
            haruki.master_dir = "/master".to_string();
            haruki.output_dir = "/out".to_string();
            haruki.manifest_file = "/out/manifest".to_string();
            match field {
                "exporter_path" => haruki.exporter_path.clear(),
                "master_dir" => haruki.master_dir.clear(),
                "output_dir" => haruki.output_dir.clear(),
                "manifest_file" => haruki.manifest_file.clear(),
                _ => unreachable!(),
            }
            assert!(validate_haruki_3d_export_config("jp", &haruki).is_err());
        }
        haruki.exporter_path = "/exporter".to_string();
        haruki.master_dir = "/master".to_string();
        haruki.output_dir = "/out".to_string();
        haruki.manifest_file = "/out/manifest".to_string();
        assert!(validate_haruki_3d_export_config("jp", &haruki).is_err());
        haruki.work_dir = "/work".to_string();
        assert!(validate_haruki_3d_export_config("jp", &haruki).is_err());
        haruki.include = vec!["^live/".to_string()];
        haruki.role_character3d_ids = vec![-1];
        assert!(validate_haruki_3d_export_config("jp", &haruki).is_err());
        haruki.role_character3d_ids = vec![1];
        assert!(validate_haruki_3d_export_config("jp", &haruki).is_ok());

        assert!(validate_asset_studio_read_kinds(&BTreeMap::from([(
            "".to_string(),
            "auto".to_string(),
        )]))
        .is_err());
        assert_eq!(
            normalize_asset_studio_image_format(" RAW_RGBA ").unwrap(),
            "raw_rgba"
        );
        warn_media_fallback_backend_options(&MediaBackendConfig {
            backend: MediaBackend::Cli,
            ..MediaBackendConfig::default()
        });
        warn_media_fallback_backend_options(&MediaBackendConfig {
            backend: MediaBackend::Auto,
            ..MediaBackendConfig::default()
        });
    }
}
