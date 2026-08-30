//! `${env:VAR}` expansion and `HARUKI_*` overrides.
//!
//! This runs against the parsed config before validation, so a value that
//! only exists in the environment is still subject to every rule in
//! [`super::validate`].

use std::collections::BTreeMap;
use std::env;

use yaml_serde::{Mapping, Value};

use crate::core::errors::ConfigError;

use super::schema::AppConfig;
use super::validate::normalize_asset_studio_image_format;

pub(super) fn resolve_backend_env_overrides(config: &mut AppConfig) -> Result<(), ConfigError> {
    if let Ok(value) = env::var("HARUKI_MEDIA_BACKEND") {
        config.backends.media.backend = value.parse()?;
    }
    if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_READ_BATCH_SIZE") {
        config.backends.asset_studio.read_batch_size =
            parse_positive_usize("backends.asset_studio.read_batch_size", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_IMAGE_FORMAT") {
        config.backends.asset_studio.image_format =
            non_empty_option(normalize_asset_studio_image_format(&value)?);
    }
    if let Ok(value) = env::var("HARUKI_ASSET_HTTP_VERSION") {
        config.server.asset_http_version = value.parse()?;
    }
    Ok(())
}

pub(super) fn resolve_concurrency_env_overrides(config: &mut AppConfig) -> Result<(), ConfigError> {
    if let Ok(value) = env::var("HARUKI_MEDIA_ENCODE_CONCURRENCY") {
        let parsed = parse_positive_usize("concurrency.media_encode", &value)?;
        config.concurrency.media_encode = parsed;
        config.concurrency.audio_encode = parsed;
        config.concurrency.video_encode = parsed;
    }
    if let Ok(value) = env::var("HARUKI_AUDIO_ENCODE_CONCURRENCY") {
        config.concurrency.audio_encode = parse_positive_usize("concurrency.audio_encode", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_VIDEO_ENCODE_CONCURRENCY") {
        config.concurrency.video_encode = parse_positive_usize("concurrency.video_encode", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_DOWNLOAD_CONCURRENCY") {
        config.concurrency.download = parse_positive_usize("concurrency.download", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_POST_PROCESS_CONCURRENCY") {
        config.concurrency.post_process = parse_positive_usize("concurrency.post_process", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_CONCURRENCY_AUTO_TUNE") {
        config.concurrency.auto_tune = parse_bool_env("concurrency.auto_tune", &value)?;
    }
    Ok(())
}

pub(super) fn resolve_resource_env_overrides(config: &mut AppConfig) -> Result<(), ConfigError> {
    if let Ok(value) = env::var("HARUKI_CPU_BUDGET_AUTO") {
        config.resources.cpu.budget_auto = parse_bool_env("resources.cpu.budget_auto", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_CPU_BUDGET_RATIO") {
        config.resources.cpu.budget_ratio =
            parse_cpu_ratio_env("resources.cpu.budget_ratio", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_CPU_RESERVED") {
        config.resources.cpu.reserved = parse_usize_env("resources.cpu.reserved", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_CPU_THROTTLE_ENABLED") {
        config.resources.cpu.throttle.enabled =
            parse_bool_env("resources.cpu.throttle.enabled", &value)?;
    }
    if let Ok(value) = env::var("HARUKI_CPU_THROTTLE_SAMPLE_MS") {
        config.resources.cpu.throttle.sample_ms =
            parse_positive_usize("resources.cpu.throttle.sample_ms", &value)? as u64;
    }
    if let Ok(value) = env::var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES") {
        config.resources.memory.max_in_flight_bundle_bytes =
            parse_usize_env("resources.memory.max_in_flight_bundle_bytes", &value)?;
    }
    Ok(())
}

pub(super) fn resolve_config_secret_env_overrides(
    config: &mut AppConfig,
) -> Result<(), ConfigError> {
    resolve_secret_env(
        "git_sync.chart_hashes.password",
        &mut config.git_sync.chart_hashes.password,
    )?;
    for (idx, provider) in config.storage.providers.iter_mut().enumerate() {
        resolve_secret_env(
            &format!("storage.providers[{idx}].access_key"),
            &mut provider.access_key,
        )?;
        resolve_secret_env(
            &format!("storage.providers[{idx}].secret_key"),
            &mut provider.secret_key,
        )?;
    }
    for (region_name, region) in &mut config.regions {
        resolve_secret_env(
            &format!("regions.{region_name}.crypto.aes_key_hex"),
            &mut region.crypto.aes_key_hex,
        )?;
        resolve_secret_env(
            &format!("regions.{region_name}.crypto.aes_iv_hex"),
            &mut region.crypto.aes_iv_hex,
        )?;
    }
    Ok(())
}

pub(super) fn resolve_secret_env(
    field: &str,
    value: &mut Option<String>,
) -> Result<(), ConfigError> {
    let Some(raw) = value.as_deref().map(str::trim) else {
        return Ok(());
    };

    let Some(name) = raw
        .strip_prefix("${env:")
        .and_then(|rest| rest.strip_suffix('}'))
        .map(str::trim)
    else {
        return Ok(());
    };

    let resolved = env::var(name).map_err(|_| ConfigError::MissingEnvironmentVariable {
        field: field.to_string(),
        name: name.to_string(),
    })?;
    *value = Some(resolved);
    Ok(())
}

pub(super) fn expand_env_references(value: &mut Value) -> Result<(), ConfigError> {
    match value {
        Value::String(raw) => {
            if let Some(expanded) = expand_env_references_in_string(raw)? {
                *raw = expanded;
            }
        }
        Value::Sequence(items) => {
            for item in items {
                expand_env_references(item)?;
            }
        }
        Value::Mapping(map) => {
            for (_, value) in map.iter_mut() {
                expand_env_references(value)?;
            }
        }
        Value::Tagged(tagged) => expand_env_references(&mut tagged.value)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }

    Ok(())
}

pub(super) fn expand_env_references_in_string(raw: &str) -> Result<Option<String>, ConfigError> {
    let Some(mut start) = raw.find("${env:") else {
        return Ok(None);
    };

    let mut expanded = String::with_capacity(raw.len());
    let mut cursor = 0;

    while start < raw.len() {
        expanded.push_str(&raw[cursor..start]);
        let name_start = start + "${env:".len();
        let Some(relative_end) = raw[name_start..].find('}') else {
            // An unclosed `${env:` is almost always a typo (e.g. a missing `}` on a secret field).
            // Failing loudly beats silently treating it as a literal value, which would surface
            // later as a confusing hex/decrypt error.
            return Err(ConfigError::InvalidValue {
                field: "config file".to_string(),
                value: "${env:...".to_string(),
                expected: "a closed ${env:VAR} reference (missing closing '}')".to_string(),
            });
        };
        let end = name_start + relative_end;
        let name = raw[name_start..end].trim();
        let value = env::var(name).map_err(|_| ConfigError::MissingEnvironmentVariable {
            field: "config file".to_string(),
            name: name.to_string(),
        })?;
        expanded.push_str(&value);
        cursor = end + 1;

        let Some(next) = raw[cursor..].find("${env:") else {
            break;
        };
        start = cursor + next;
    }

    expanded.push_str(&raw[cursor..]);
    Ok(Some(expanded))
}

pub(super) fn apply_env_overrides(root: &mut Value) -> Result<(), ConfigError> {
    let overrides = env::vars()
        .filter(|(name, _)| name.starts_with("HARUKI__"))
        .collect::<BTreeMap<_, _>>();

    for (name, raw_value) in overrides {
        let path = parse_env_override_path(&name)?;
        let value = parse_env_override_value(&raw_value);
        apply_env_override(root, &name, &path, value)?;
    }

    Ok(())
}

pub(super) fn parse_env_override_path(name: &str) -> Result<Vec<String>, ConfigError> {
    let raw_path =
        name.strip_prefix("HARUKI__")
            .ok_or_else(|| ConfigError::InvalidConfigBootstrap {
                name: name.to_string(),
                reason: "override names must start with HARUKI__".to_string(),
            })?;

    if raw_path.is_empty() {
        return Err(ConfigError::InvalidConfigBootstrap {
            name: name.to_string(),
            reason: "override path is empty".to_string(),
        });
    }

    raw_path
        .split("__")
        .map(|segment| {
            if segment.is_empty() {
                Err(ConfigError::InvalidConfigBootstrap {
                    name: name.to_string(),
                    reason: "override path contains an empty segment".to_string(),
                })
            } else {
                Ok(segment.to_ascii_lowercase())
            }
        })
        .collect()
}

pub(super) fn parse_env_override_value(raw: &str) -> Value {
    if raw.is_empty() {
        return Value::String(String::new());
    }

    yaml_serde::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

pub(super) fn apply_env_override(
    root: &mut Value,
    name: &str,
    path: &[String],
    value: Value,
) -> Result<(), ConfigError> {
    if path.is_empty() {
        return Err(ConfigError::InvalidConfigBootstrap {
            name: name.to_string(),
            reason: "override path is empty".to_string(),
        });
    }

    let mut current = root;
    for (idx, segment) in path.iter().enumerate() {
        let is_last = idx + 1 == path.len();
        if is_last {
            set_env_override_leaf(current, segment, value);
            return Ok(());
        }

        current =
            descend_env_override_path(current, segment, path.get(idx + 1).map(String::as_str));
    }

    Ok(())
}

pub(super) fn set_env_override_leaf(current: &mut Value, segment: &str, value: Value) {
    if let Ok(index) = segment.parse::<usize>() {
        match current {
            Value::Sequence(items) => {
                if items.len() <= index {
                    items.resize(index + 1, Value::Null);
                }
                items[index] = value;
                return;
            }
            Value::Null => {
                let mut items = Vec::new();
                items.resize(index + 1, Value::Null);
                items[index] = value;
                *current = Value::Sequence(items);
                return;
            }
            _ => {}
        }
    }

    if !matches!(current, Value::Mapping(_)) {
        *current = Value::Mapping(Mapping::new());
    }

    if let Value::Mapping(map) = current {
        map.insert(Value::String(segment.to_string()), value);
    }
}

pub(super) fn descend_env_override_path<'a>(
    current: &'a mut Value,
    segment: &str,
    next_segment: Option<&str>,
) -> &'a mut Value {
    let next_is_index = next_segment.is_some_and(|next| next.parse::<usize>().is_ok());

    if let Ok(index) = segment.parse::<usize>() {
        if matches!(current, Value::Null) {
            *current = Value::Sequence(Vec::new());
        }
        if let Value::Sequence(items) = current {
            if items.len() <= index {
                items.resize_with(index + 1, Value::default);
            }
            if matches!(items[index], Value::Null) {
                items[index] = env_override_default_child(next_is_index);
            }
            return &mut items[index];
        }
    }

    if !matches!(current, Value::Mapping(_)) {
        *current = Value::Mapping(Mapping::new());
    }

    if let Value::Mapping(map) = current {
        return map
            .entry(Value::String(segment.to_string()))
            .or_insert_with(|| env_override_default_child(next_is_index));
    }

    unreachable!("current value was normalized into a mapping")
}

pub(super) fn env_override_default_child(sequence: bool) -> Value {
    if sequence {
        Value::Sequence(Vec::new())
    } else {
        Value::Mapping(Mapping::new())
    }
}

pub(super) fn non_empty_option(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(super) fn parse_positive_usize(field: &str, value: &str) -> Result<usize, ConfigError> {
    let trimmed = value.trim();
    let parsed = trimmed
        .parse::<usize>()
        .map_err(|_| ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "a positive integer".to_string(),
        })?;
    if parsed == 0 {
        Err(ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "a positive integer".to_string(),
        })
    } else {
        Ok(parsed)
    }
}

pub(super) fn parse_usize_env(field: &str, value: &str) -> Result<usize, ConfigError> {
    let trimmed = value.trim();
    trimmed
        .parse::<usize>()
        .map_err(|_| ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "a non-negative integer".to_string(),
        })
}

pub(super) fn parse_cpu_ratio_env(field: &str, value: &str) -> Result<f64, ConfigError> {
    let trimmed = value.trim();
    trimmed
        .parse::<f64>()
        .map_err(|_| ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "a number greater than 0 and less than or equal to 1".to_string(),
        })
}

pub(super) fn parse_bool_env(field: &str, value: &str) -> Result<bool, ConfigError> {
    let trimmed = value.trim();
    match trimmed.to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "true or false".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;

    use tempfile::NamedTempFile;

    use crate::core::errors::ConfigError;

    use super::super::schema::MediaBackend;
    use super::super::test_support::{env_lock, restore_env};

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

    #[test]
    fn env_override_helpers_cover_invalid_paths_sequences_and_scalar_parsing() {
        assert!(parse_env_override_path("OTHER__VALUE").is_err());
        assert!(parse_env_override_path("HARUKI__").is_err());
        assert!(parse_env_override_path("HARUKI__SERVER____PORT").is_err());
        assert_eq!(
            parse_env_override_path("HARUKI__SERVER__PORT").unwrap(),
            vec!["server".to_string(), "port".to_string()]
        );
        assert_eq!(parse_env_override_value(""), Value::String(String::new()));
        assert_eq!(parse_env_override_value("42"), Value::Number(42.into()));
        assert_eq!(
            parse_env_override_value("[invalid"),
            Value::String("[invalid".to_string())
        );

        let mut root = Value::Null;
        assert!(apply_env_override(&mut root, "HARUKI__", &[], Value::Bool(true)).is_err());
        apply_env_override(
            &mut root,
            "HARUKI__ITEMS__2__NAME",
            &["items".to_string(), "2".to_string(), "name".to_string()],
            Value::String("third".to_string()),
        )
        .unwrap();
        let Value::Mapping(root) = root else {
            panic!("root should be a mapping");
        };
        let Value::Sequence(items) = &root[&Value::String("items".to_string())] else {
            panic!("items should be a sequence");
        };
        assert_eq!(items.len(), 3);

        let mut sequence = Value::Null;
        set_env_override_leaf(&mut sequence, "1", Value::Bool(true));
        assert!(matches!(sequence, Value::Sequence(_)));
        let mut scalar = Value::Bool(false);
        set_env_override_leaf(&mut scalar, "name", Value::String("value".to_string()));
        assert!(matches!(scalar, Value::Mapping(_)));

        assert_eq!(non_empty_option("  ".to_string()), None);
        assert_eq!(
            non_empty_option(" value ".to_string()).as_deref(),
            Some(" value ")
        );
        assert!(parse_positive_usize("field", "bad").is_err());
        assert!(parse_positive_usize("field", "0").is_err());
        assert_eq!(parse_positive_usize("field", " 2 ").unwrap(), 2);
        assert!(parse_usize_env("field", "-1").is_err());
        assert!(parse_cpu_ratio_env("field", "bad").is_err());
        for value in ["0", "false", "no", "off"] {
            assert!(!parse_bool_env("field", value).unwrap());
        }
        assert!(parse_bool_env("field", "maybe").is_err());
        assert!(expand_env_references_in_string("${env:UNCLOSED").is_err());
    }

    #[test]
    fn split_concurrency_and_http_env_overrides_are_applied() {
        let _env_lock = env_lock();
        let names = [
            "HARUKI_AUDIO_ENCODE_CONCURRENCY",
            "HARUKI_VIDEO_ENCODE_CONCURRENCY",
            "HARUKI_ASSET_HTTP_VERSION",
        ];
        let old = names.map(|name| std::env::var(name).ok());
        std::env::set_var(names[0], "3");
        std::env::set_var(names[1], "4");
        std::env::set_var(names[2], "http1");
        let mut config = AppConfig::default();
        resolve_backend_env_overrides(&mut config).unwrap();
        resolve_concurrency_env_overrides(&mut config).unwrap();
        assert_eq!(config.concurrency.audio_encode, 3);
        assert_eq!(config.concurrency.video_encode, 4);
        assert_eq!(
            config.server.asset_http_version,
            super::super::schema::AssetHttpVersion::Http1
        );
        for (name, value) in names.into_iter().zip(old) {
            restore_env(name, value);
        }
    }
}
