//! Rules a config must satisfy to be runnable.
//!
//! Everything here rejects; nothing here mutates shape. A config that parses
//! but cannot run -- an unknown image format, a region whose filters do not
//! compile -- fails at this layer rather than at first use.

use std::collections::BTreeMap;

use crate::core::errors::ConfigError;

use super::schema::*;

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
