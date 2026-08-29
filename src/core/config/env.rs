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
