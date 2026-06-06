use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use yaml_serde::{Mapping, Value};

use crate::core::errors::ConfigError;

const CONFIG_URI_ENV: &str = "HARUKI_CONFIG_URI";
const CONFIG_OPENDAL_SCHEME_ENV: &str = "HARUKI_CONFIG_OPENDAL_SCHEME";
const CONFIG_OPENDAL_ROOT_ENV: &str = "HARUKI_CONFIG_OPENDAL_ROOT";
const CONFIG_OPENDAL_OPTION_PREFIX: &str = "HARUKI_CONFIG_OPENDAL_OPTION_";
const CONFIG_OPENDAL_URI_PREFIX: &str = "opendal://";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigStorageUri {
    provider: String,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub config_version: u32,
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub execution: ExecutionConfig,
    pub tools: ToolsConfig,
    pub concurrency: ConcurrencyConfig,
    pub storage: StorageConfig,
    pub git_sync: GitSyncConfig,
    pub regions: BTreeMap<String, RegionConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            config_version: 2,
            server: ServerConfig::default(),
            logging: LoggingConfig::default(),
            execution: ExecutionConfig::default(),
            tools: ToolsConfig::default(),
            concurrency: ConcurrencyConfig::default(),
            storage: StorageConfig::default(),
            git_sync: GitSyncConfig::default(),
            regions: BTreeMap::new(),
        }
    }
}

impl AppConfig {
    pub async fn load_default() -> Result<Self, ConfigError> {
        if let Some(uri) = env::var(CONFIG_URI_ENV)
            .ok()
            .map(|uri| uri.trim().to_string())
            .filter(|uri| !uri.is_empty())
        {
            return Self::load_from_opendal_uri(&uri).await;
        }

        let candidates = candidate_paths();
        for candidate in &candidates {
            if candidate.exists() {
                return Self::load_from_path(candidate);
            }
        }

        Err(ConfigError::MissingConfigFile(
            candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref().to_path_buf();
        let raw = fs::read_to_string(&path).map_err(|source| ConfigError::Read {
            path: path.clone(),
            source,
        })?;
        Self::load_from_str(path, &raw)
    }

    pub async fn load_from_opendal_uri(uri: &str) -> Result<Self, ConfigError> {
        let storage_uri = parse_config_storage_uri(uri)?;
        let (scheme, options) = config_storage_provider_options()?;

        opendal::init_default_registry();
        let operator = opendal::Operator::via_iter(&scheme, options).map_err(|source| {
            ConfigError::ConfigStorageProvider {
                provider: storage_uri.provider.clone(),
                source,
            }
        })?;
        let bytes = operator.read(&storage_uri.path).await.map_err(|source| {
            ConfigError::ConfigStorageRead {
                uri: uri.to_string(),
                source,
            }
        })?;
        let raw = String::from_utf8(bytes.to_vec()).map_err(|source| ConfigError::InvalidUtf8 {
            path: uri.to_string(),
            source,
        })?;

        Self::load_from_str(PathBuf::from(uri), &raw)
    }

    fn load_from_str(path: PathBuf, raw: &str) -> Result<Self, ConfigError> {
        let mut value: Value = yaml_serde::from_str(raw).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
        expand_env_references(&mut value)?;
        apply_env_overrides(&mut value)?;

        let mut config: Self =
            yaml_serde::from_value(value).map_err(|source| ConfigError::Parse {
                path: path.clone(),
                source,
            })?;
        config.resolve_legacy_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.config_version != 2 {
            return Err(ConfigError::UnsupportedVersion(self.config_version));
        }

        for region_name in self.regions.keys() {
            if region_name.to_lowercase() != *region_name {
                return Err(ConfigError::InvalidRegionName(region_name.clone()));
            }
        }
        if !(0.0..=1.0).contains(&self.concurrency.cpu_budget_ratio)
            || self.concurrency.cpu_budget_ratio == 0.0
        {
            return Err(ConfigError::InvalidValue {
                field: "concurrency.cpu_budget_ratio".to_string(),
                value: self.concurrency.cpu_budget_ratio.to_string(),
                expected: "a number greater than 0 and less than or equal to 1".to_string(),
            });
        }
        if self.tools.asset_studio_ffi_read_batch_size == 0 {
            return Err(ConfigError::InvalidValue {
                field: "tools.asset_studio_ffi_read_batch_size".to_string(),
                value: "0".to_string(),
                expected: "a positive integer".to_string(),
            });
        }
        if self.concurrency.media_encode == 0 {
            return Err(ConfigError::InvalidValue {
                field: "concurrency.media_encode".to_string(),
                value: "0".to_string(),
                expected: "a positive integer".to_string(),
            });
        }
        if let Some(image_format) = &self.tools.asset_studio_ffi_image_format {
            validate_asset_studio_ffi_image_format(image_format)?;
        }
        validate_asset_studio_ffi_read_kinds(&self.tools.asset_studio_ffi_read_kinds)?;
        warn_legacy_backend_options(&self.tools);

        Ok(())
    }

    pub fn effective_concurrency(&self) -> ConcurrencyConfig {
        self.concurrency.effective()
    }

    pub fn effective_cpu_budget(&self) -> usize {
        self.concurrency.effective_cpu_budget()
    }

    pub fn effective_asset_studio_ffi_process_concurrency(&self) -> usize {
        self.effective_asset_studio_ffi_process_concurrency_for_cpus(available_cpu_count())
    }

    pub fn effective_asset_studio_ffi_process_concurrency_for_cpus(&self, cpus: usize) -> usize {
        let configured = self.tools.asset_studio_ffi_process_concurrency;
        if configured > 0 {
            return configured;
        }
        let cpus = cpus.max(1);
        let cpu_budget = self.concurrency.effective_cpu_budget_for_cpus(cpus);
        if self.concurrency.cpu_throttle_enabled {
            return cpus
                .min(cpu_budget.saturating_mul(2).max(cpu_budget))
                .max(1);
        }
        cpu_budget
    }

    pub fn enabled_regions(&self) -> Vec<String> {
        self.regions
            .iter()
            .filter_map(|(name, region)| region.enabled.then_some(name.clone()))
            .collect()
    }

    fn resolve_legacy_env_overrides(&mut self) -> Result<(), ConfigError> {
        if let Ok(value) = env::var("HARUKI_MEDIA_BACKEND") {
            self.tools.media_backend = value.parse()?;
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH",
            "HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH",
        ) {
            self.tools.asset_studio_ffi_library_path = non_empty_option(value);
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_CALL_MODE",
            "HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE",
        ) {
            self.tools.asset_studio_ffi_call_mode = value.parse()?;
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_WORKER_PATH",
            "HARUKI_ASSET_STUDIO_NATIVE_WORKER_PATH",
        ) {
            self.tools.asset_studio_ffi_worker_path = non_empty_option(value);
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY",
            "HARUKI_ASSET_STUDIO_NATIVE_PROCESS_CONCURRENCY",
        ) {
            self.tools.asset_studio_ffi_process_concurrency =
                parse_usize_env("tools.asset_studio_ffi_process_concurrency", &value)?;
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS",
            "HARUKI_ASSET_STUDIO_NATIVE_WORKER_MAX_CALLS",
        ) {
            self.tools.asset_studio_ffi_worker_max_calls =
                parse_usize_env("tools.asset_studio_ffi_worker_max_calls", &value)?;
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE",
            "HARUKI_ASSET_STUDIO_NATIVE_READ_BATCH_SIZE",
        ) {
            self.tools.asset_studio_ffi_read_batch_size =
                parse_positive_usize("tools.asset_studio_ffi_read_batch_size", &value)?;
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT",
            "HARUKI_ASSET_STUDIO_NATIVE_IMAGE_FORMAT",
        ) {
            self.tools.asset_studio_ffi_image_format =
                non_empty_option(normalize_asset_studio_ffi_image_format(&value)?);
        }
        if let Some(value) = env_var_with_legacy(
            "HARUKI_ASSET_STUDIO_FFI_CLI_PARITY_MODE",
            "HARUKI_ASSET_STUDIO_NATIVE_CLI_PARITY_MODE",
        ) {
            self.tools.asset_studio_ffi_cli_parity_mode =
                parse_bool_env("tools.asset_studio_ffi_cli_parity_mode", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_MEDIA_ENCODE_CONCURRENCY") {
            self.concurrency.media_encode =
                parse_positive_usize("concurrency.media_encode", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CONCURRENCY_AUTO_TUNE") {
            self.concurrency.auto_tune = parse_bool_env("concurrency.auto_tune", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_BUDGET_AUTO") {
            self.concurrency.cpu_budget_auto =
                parse_bool_env("concurrency.cpu_budget_auto", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_BUDGET_RATIO") {
            self.concurrency.cpu_budget_ratio =
                parse_cpu_ratio_env("concurrency.cpu_budget_ratio", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_RESERVED") {
            self.concurrency.cpu_reserved = parse_usize_env("concurrency.cpu_reserved", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_THROTTLE_ENABLED") {
            self.concurrency.cpu_throttle_enabled =
                parse_bool_env("concurrency.cpu_throttle_enabled", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_THROTTLE_SAMPLE_MS") {
            self.concurrency.cpu_throttle_sample_ms =
                parse_positive_usize("concurrency.cpu_throttle_sample_ms", &value)? as u64;
        }
        if let Ok(value) = env::var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES") {
            self.execution.max_in_flight_bundle_bytes =
                parse_usize_env("execution.max_in_flight_bundle_bytes", &value)?;
        }
        resolve_secret_env(
            "git_sync.chart_hashes.password",
            &mut self.git_sync.chart_hashes.password,
        )?;

        for (idx, provider) in self.storage.providers.iter_mut().enumerate() {
            resolve_secret_env(
                &format!("storage.providers[{idx}].access_key"),
                &mut provider.access_key,
            )?;
            resolve_secret_env(
                &format!("storage.providers[{idx}].secret_key"),
                &mut provider.secret_key,
            )?;
        }

        for (region_name, region) in self.regions.iter_mut() {
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
}

fn resolve_secret_env(field: &str, value: &mut Option<String>) -> Result<(), ConfigError> {
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

fn parse_config_storage_uri(uri: &str) -> Result<ConfigStorageUri, ConfigError> {
    let Some(raw) = uri.strip_prefix(CONFIG_OPENDAL_URI_PREFIX) else {
        return Err(ConfigError::InvalidConfigUri {
            uri: uri.to_string(),
            reason:
                "only opendal:// config URIs are supported; use HARUKI_CONFIG_PATH for local files"
                    .to_string(),
        });
    };

    let raw = raw.trim_start_matches('/');
    let Some((provider, path)) = raw.split_once('/') else {
        return Err(ConfigError::InvalidConfigUri {
            uri: uri.to_string(),
            reason: "expected opendal://<provider>/<path>".to_string(),
        });
    };
    let provider = provider.trim();
    let path = path.trim().trim_matches('/').replace('\\', "/");

    if provider.is_empty() {
        return Err(ConfigError::InvalidConfigUri {
            uri: uri.to_string(),
            reason: "provider is empty".to_string(),
        });
    }
    if path.is_empty() {
        return Err(ConfigError::InvalidConfigUri {
            uri: uri.to_string(),
            reason: "path is empty".to_string(),
        });
    }

    Ok(ConfigStorageUri {
        provider: provider.to_string(),
        path,
    })
}

fn config_storage_provider_options() -> Result<(String, BTreeMap<String, String>), ConfigError> {
    let scheme = env::var(CONFIG_OPENDAL_SCHEME_ENV)
        .ok()
        .map(|scheme| scheme.trim().to_ascii_lowercase())
        .filter(|scheme| !scheme.is_empty())
        .ok_or_else(|| ConfigError::MissingEnvironmentVariable {
            field: CONFIG_URI_ENV.to_string(),
            name: CONFIG_OPENDAL_SCHEME_ENV.to_string(),
        })?;

    let mut options = BTreeMap::new();
    if let Some(root) = env::var(CONFIG_OPENDAL_ROOT_ENV)
        .ok()
        .map(|root| root.trim().to_string())
        .filter(|root| !root.is_empty())
    {
        options.insert("root".to_string(), root);
    }

    for (name, value) in env::vars().filter(|(name, value)| {
        name.starts_with(CONFIG_OPENDAL_OPTION_PREFIX)
            && name.len() > CONFIG_OPENDAL_OPTION_PREFIX.len()
            && !value.trim().is_empty()
    }) {
        let key = name
            .strip_prefix(CONFIG_OPENDAL_OPTION_PREFIX)
            .expect("prefix was checked")
            .to_ascii_lowercase();
        if key.is_empty() {
            return Err(ConfigError::InvalidConfigBootstrap {
                name,
                reason: "OpenDAL option key is empty".to_string(),
            });
        }
        options.insert(key, value);
    }

    Ok((scheme, options))
}

fn expand_env_references(value: &mut Value) -> Result<(), ConfigError> {
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

fn expand_env_references_in_string(raw: &str) -> Result<Option<String>, ConfigError> {
    let Some(mut start) = raw.find("${env:") else {
        return Ok(None);
    };

    let mut expanded = String::with_capacity(raw.len());
    let mut cursor = 0;

    while start < raw.len() {
        expanded.push_str(&raw[cursor..start]);
        let name_start = start + "${env:".len();
        let Some(relative_end) = raw[name_start..].find('}') else {
            expanded.push_str(&raw[start..]);
            return Ok(Some(expanded));
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

fn apply_env_overrides(root: &mut Value) -> Result<(), ConfigError> {
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

fn parse_env_override_path(name: &str) -> Result<Vec<String>, ConfigError> {
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

fn parse_env_override_value(raw: &str) -> Value {
    if raw.is_empty() {
        return Value::String(String::new());
    }

    yaml_serde::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

fn apply_env_override(
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

fn set_env_override_leaf(current: &mut Value, segment: &str, value: Value) {
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

fn descend_env_override_path<'a>(
    current: &'a mut Value,
    segment: &str,
    next_segment: Option<&str>,
) -> &'a mut Value {
    let default_child = || match next_segment.and_then(|next| next.parse::<usize>().ok()) {
        Some(_) => Value::Sequence(Vec::new()),
        None => Value::Mapping(Mapping::new()),
    };

    if let Ok(index) = segment.parse::<usize>() {
        match current {
            Value::Sequence(items) => {
                if items.len() <= index {
                    items.resize_with(index + 1, Value::default);
                }
                if matches!(items[index], Value::Null) {
                    items[index] = default_child();
                }
                return &mut items[index];
            }
            Value::Null => {
                let mut items = Vec::new();
                items.resize_with(index + 1, Value::default);
                items[index] = default_child();
                *current = Value::Sequence(items);
                if let Value::Sequence(items) = current {
                    return &mut items[index];
                }
            }
            _ => {}
        }
    }

    if !matches!(current, Value::Mapping(_)) {
        *current = Value::Mapping(Mapping::new());
    }

    if let Value::Mapping(map) = current {
        return map
            .entry(Value::String(segment.to_string()))
            .or_insert_with(default_child);
    }

    unreachable!("current value was normalized into a mapping")
}

fn non_empty_option(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn env_var_with_legacy(current: &str, legacy: &str) -> Option<String> {
    env::var(current).ok().or_else(|| env::var(legacy).ok())
}

fn parse_positive_usize(field: &str, value: &str) -> Result<usize, ConfigError> {
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

fn parse_usize_env(field: &str, value: &str) -> Result<usize, ConfigError> {
    let trimmed = value.trim();
    trimmed
        .parse::<usize>()
        .map_err(|_| ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "a non-negative integer".to_string(),
        })
}

fn parse_cpu_ratio_env(field: &str, value: &str) -> Result<f64, ConfigError> {
    let trimmed = value.trim();
    trimmed
        .parse::<f64>()
        .map_err(|_| ConfigError::InvalidValue {
            field: field.to_string(),
            value: trimmed.to_string(),
            expected: "a number greater than 0 and less than or equal to 1".to_string(),
        })
}

fn normalize_asset_studio_ffi_image_format(value: &str) -> Result<String, ConfigError> {
    let normalized = value.trim().to_lowercase();
    validate_asset_studio_ffi_image_format(&normalized)?;
    Ok(normalized)
}

fn validate_asset_studio_ffi_image_format(value: &str) -> Result<(), ConfigError> {
    match value.trim().to_lowercase().as_str() {
        "raw_rgba" => Ok(()),
        other => Err(ConfigError::InvalidValue {
            field: "tools.asset_studio_ffi_image_format".to_string(),
            value: other.to_string(),
            expected: "raw_rgba".to_string(),
        }),
    }
}

fn validate_asset_studio_ffi_read_kinds(
    read_kinds: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    for (asset_type, kind) in read_kinds {
        if asset_type.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "tools.asset_studio_ffi_read_kinds".to_string(),
                value: asset_type.clone(),
                expected: "non-empty AssetStudio type selector".to_string(),
            });
        }
        validate_asset_studio_ffi_read_kind(
            &format!("tools.asset_studio_ffi_read_kinds.{asset_type}"),
            kind,
        )?;
    }
    Ok(())
}

fn warn_legacy_backend_options(tools: &ToolsConfig) {
    match tools.media_backend {
        MediaBackend::Ffi => {}
        MediaBackend::Cli => {
            tracing::warn!("tools.media_backend=cli is legacy; production should use ffi")
        }
        MediaBackend::Auto => tracing::warn!(
            "tools.media_backend=auto is legacy fallback mode; production should use ffi"
        ),
    }
}

fn validate_asset_studio_ffi_read_kind(field: &str, value: &str) -> Result<(), ConfigError> {
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

fn parse_bool_env(field: &str, value: &str) -> Result<bool, ConfigError> {
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

fn candidate_paths() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(path) = env::var("HARUKI_CONFIG_PATH") {
        candidates.push(PathBuf::from(path));
    }
    candidates.push(PathBuf::from("haruki-asset-configs.yaml"));
    candidates.push(PathBuf::from("../haruki-asset-configs.yaml"));
    candidates.push(PathBuf::from("../../haruki-asset-configs.yaml"));
    candidates
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub proxy: Option<String>,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            proxy: None,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AuthConfig {
    pub enabled: bool,
    pub user_agent_prefix: Option<String>,
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
    pub file: Option<String>,
    pub access: AccessLogConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "INFO".to_string(),
            format: LogFormat::Pretty,
            file: None,
            access: AccessLogConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AccessLogConfig {
    pub enabled: bool,
    pub format: String,
    pub file: Option<String>,
}

impl Default for AccessLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            format: "[${time}] ${status} - ${method} ${path} ${latency}\n".to_string(),
            file: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ToolsConfig {
    pub ffmpeg_path: String,
    pub media_backend: MediaBackend,
    #[serde(alias = "asset_studio_native_library_path")]
    pub asset_studio_ffi_library_path: Option<String>,
    #[serde(alias = "asset_studio_native_call_mode")]
    pub asset_studio_ffi_call_mode: AssetStudioFfiCallMode,
    #[serde(alias = "asset_studio_native_worker_path")]
    pub asset_studio_ffi_worker_path: Option<String>,
    #[serde(alias = "asset_studio_native_process_concurrency")]
    pub asset_studio_ffi_process_concurrency: usize,
    #[serde(alias = "asset_studio_native_worker_max_calls")]
    pub asset_studio_ffi_worker_max_calls: usize,
    #[serde(alias = "asset_studio_native_read_batch_size")]
    pub asset_studio_ffi_read_batch_size: usize,
    #[serde(alias = "asset_studio_native_image_format")]
    pub asset_studio_ffi_image_format: Option<String>,
    #[serde(alias = "asset_studio_native_read_kinds")]
    pub asset_studio_ffi_read_kinds: BTreeMap<String, String>,
    #[serde(alias = "asset_studio_native_cli_parity_mode")]
    pub asset_studio_ffi_cli_parity_mode: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MediaBackend {
    Auto,
    #[default]
    Ffi,
    Cli,
}

impl FromStr for MediaBackend {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "ffi" => Ok(Self::Ffi),
            "cli" => Ok(Self::Cli),
            other => Err(ConfigError::InvalidValue {
                field: "tools.media_backend".to_string(),
                value: other.to_string(),
                expected: "auto, ffi, or cli".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStudioFfiCallMode {
    Direct,
    Process,
    #[default]
    Pool,
}

impl FromStr for AssetStudioFfiCallMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "direct" => Ok(Self::Direct),
            "process" => Ok(Self::Process),
            "pool" => Ok(Self::Pool),
            other => Err(ConfigError::InvalidValue {
                field: "tools.asset_studio_ffi_call_mode".to_string(),
                value: other.to_string(),
                expected: "direct, process, or pool".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub timeout_seconds: u64,
    pub allow_cancel: bool,
    pub asset_bundle_cache_dir: Option<String>,
    /// Soft process memory guard for bundle work.  When non-zero, bundle
    /// downloads/native payloads acquire permits by estimated bundle size and
    /// keep them until export/post-process finishes.
    pub max_in_flight_bundle_bytes: usize,
    /// How many successful downloads to accumulate before flushing the download
    /// record to disk mid-run.  Set to `0` to disable mid-run flushing (record
    /// is only written once at the end).  Mirrors Go's `batchSaveSize`.
    pub batch_save_size: usize,
    pub retry: RetryConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 300,
            allow_cancel: true,
            asset_bundle_cache_dir: None,
            max_in_flight_bundle_bytes: 0,
            batch_save_size: 50,
            retry: RetryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    pub attempts: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: 4,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 4_000,
        }
    }
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            ffmpeg_path: "ffmpeg".to_string(),
            media_backend: MediaBackend::Ffi,
            asset_studio_ffi_library_path: None,
            asset_studio_ffi_call_mode: AssetStudioFfiCallMode::Pool,
            asset_studio_ffi_worker_path: None,
            asset_studio_ffi_process_concurrency: 0,
            asset_studio_ffi_worker_max_calls: 256,
            asset_studio_ffi_read_batch_size: 32,
            asset_studio_ffi_image_format: None,
            asset_studio_ffi_read_kinds: BTreeMap::new(),
            asset_studio_ffi_cli_parity_mode: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConcurrencyConfig {
    pub auto_tune: bool,
    pub cpu_budget_auto: bool,
    pub cpu_budget_ratio: f64,
    pub cpu_reserved: usize,
    pub cpu_throttle_enabled: bool,
    pub cpu_throttle_sample_ms: u64,
    pub download: usize,
    pub upload: usize,
    pub acb: usize,
    pub usm: usize,
    pub hca: usize,
    pub media_encode: usize,
    pub images: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            auto_tune: false,
            cpu_budget_auto: true,
            cpu_budget_ratio: 0.75,
            cpu_reserved: 1,
            cpu_throttle_enabled: false,
            cpu_throttle_sample_ms: 250,
            download: 4,
            upload: 4,
            acb: 8,
            usm: 4,
            hca: 16,
            media_encode: 12,
            images: 4,
        }
    }
}

impl ConcurrencyConfig {
    pub fn effective(&self) -> Self {
        if !self.auto_tune {
            return self.clone();
        }
        self.effective_for_cpus(available_cpu_count())
    }

    pub fn effective_for_cpus(&self, cpus: usize) -> Self {
        if !self.auto_tune {
            return self.clone();
        }
        let cpus = cpus.max(1);
        let cpu_budget = self.effective_cpu_budget_for_cpus(cpus);
        Self {
            auto_tune: true,
            cpu_budget_auto: self.cpu_budget_auto,
            cpu_budget_ratio: self.cpu_budget_ratio,
            cpu_reserved: self.cpu_reserved,
            cpu_throttle_enabled: self.cpu_throttle_enabled,
            cpu_throttle_sample_ms: self.cpu_throttle_sample_ms,
            download: self.download.min(cpus.saturating_mul(2).max(2)).max(1),
            upload: self.upload.min(cpus.max(2)).max(1),
            acb: self.acb.min(cpus.max(2)).min(cpu_budget).max(1),
            usm: self.usm.min((cpus / 2).max(1)).max(1),
            hca: self
                .hca
                .min(cpus.saturating_mul(2).max(2))
                .min(cpu_budget)
                .max(1),
            media_encode: self
                .media_encode
                .min(cpus.saturating_sub(1).max(1))
                .min(cpu_budget)
                .max(1),
            images: self.images.min(cpus.max(2)).min(cpu_budget).max(1),
        }
    }

    pub fn effective_cpu_budget(&self) -> usize {
        self.effective_cpu_budget_for_cpus(available_cpu_count())
    }

    pub fn effective_cpu_budget_for_cpus(&self, cpus: usize) -> usize {
        let cpus = cpus.max(1);
        if !self.cpu_budget_auto {
            return cpus;
        }
        ((cpus as f64 * self.cpu_budget_ratio).floor() as usize)
            .saturating_sub(self.cpu_reserved)
            .max(1)
    }
}

fn available_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .max(1)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StorageConfig {
    pub providers: Vec<StorageProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageProviderConfig {
    pub name: Option<String>,
    #[serde(alias = "kind")]
    pub scheme: String,
    pub root: Option<String>,
    pub public_base_url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_storage_options")]
    pub options: BTreeMap<String, String>,
    pub endpoint: String,
    pub tls: bool,
    pub bucket: String,
    pub prefix: Option<String>,
    pub path_style: bool,
    pub region: Option<String>,
    pub public_read: bool,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

impl Default for StorageProviderConfig {
    fn default() -> Self {
        Self {
            name: None,
            scheme: "s3".to_string(),
            root: None,
            public_base_url: None,
            options: BTreeMap::new(),
            endpoint: String::new(),
            tls: true,
            bucket: String::new(),
            prefix: None,
            path_style: true,
            region: None,
            public_read: false,
            access_key: None,
            secret_key: None,
        }
    }
}

fn deserialize_storage_options<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = BTreeMap::<String, Value>::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(key, value)| {
            storage_option_value_to_string(value)
                .map(|value| (key, value))
                .map_err(de::Error::custom)
        })
        .collect()
}

fn storage_option_value_to_string(value: Value) -> Result<String, String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value),
        Value::Sequence(_) | Value::Mapping(_) | Value::Tagged(_) => {
            Err("storage provider options must be scalar values".to_string())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct GitSyncConfig {
    pub chart_hashes: ChartHashConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GitSigningFormat {
    #[default]
    #[serde(alias = "openpgp")]
    Gpg,
    Ssh,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ChartHashConfig {
    pub enabled: bool,
    pub repository_dir: Option<String>,
    pub username: Option<String>,
    pub email: Option<String>,
    pub password: Option<String>,
    pub sign_commits: bool,
    pub signing_format: GitSigningFormat,
    pub signing_key: Option<String>,
    pub signing_program: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RegionConfig {
    pub enabled: bool,
    pub provider: RegionProviderConfig,
    pub crypto: CryptoConfig,
    pub runtime: RegionRuntimeConfig,
    pub paths: RegionPathsConfig,
    pub filters: RegionFiltersConfig,
    pub export: RegionExportConfig,
    pub upload: RegionUploadConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RegionProviderConfig {
    ColorfulPalette {
        asset_info_url_template: String,
        asset_bundle_url_template: String,
        profile: String,
        profile_hashes: BTreeMap<String, String>,
        #[serde(default)]
        required_cookies: bool,
        #[serde(default)]
        cookie_bootstrap_url: Option<String>,
    },
    Nuverse {
        asset_version_url: String,
        app_version: String,
        asset_info_url_template: String,
        asset_bundle_url_template: String,
        #[serde(default)]
        required_cookies: bool,
        #[serde(default)]
        cookie_bootstrap_url: Option<String>,
    },
}

impl Default for RegionProviderConfig {
    fn default() -> Self {
        Self::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: String::new(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CryptoConfig {
    pub aes_key_hex: Option<String>,
    pub aes_iv_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionRuntimeConfig {
    pub unity_version: String,
}

impl Default for RegionRuntimeConfig {
    fn default() -> Self {
        Self {
            unity_version: "2022.3.21f1".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RegionPathsConfig {
    pub asset_save_dir: Option<String>,
    pub downloaded_asset_record_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RegionFiltersConfig {
    pub start_app: Vec<String>,
    pub on_demand: Vec<String>,
    pub skip: Vec<String>,
    pub priority: Vec<String>,
}

pub const DEFAULT_ASSET_STUDIO_EXPORT_TYPES: &[&str] = &[
    "monoBehaviour",
    "textAsset",
    "tex2d",
    "tex2dArray",
    "sprite",
    "audio",
];

fn default_asset_studio_export_types() -> Vec<String> {
    DEFAULT_ASSET_STUDIO_EXPORT_TYPES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RegionExportConfig {
    pub by_category: bool,
    #[serde(default = "default_asset_studio_export_types")]
    pub asset_studio_types: Vec<String>,
    pub usm: UsmExportConfig,
    pub acb: AcbExportConfig,
    pub hca: HcaExportConfig,
    pub images: ImageExportConfig,
    pub video: VideoExportConfig,
    pub audio: AudioExportConfig,
}

impl Default for RegionExportConfig {
    fn default() -> Self {
        Self {
            by_category: false,
            asset_studio_types: default_asset_studio_export_types(),
            usm: UsmExportConfig::default(),
            acb: AcbExportConfig::default(),
            hca: HcaExportConfig::default(),
            images: ImageExportConfig::default(),
            video: VideoExportConfig::default(),
            audio: AudioExportConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UsmExportConfig {
    pub export: bool,
    pub decode: bool,
}

impl Default for UsmExportConfig {
    fn default() -> Self {
        Self {
            export: true,
            decode: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AcbExportConfig {
    pub export: bool,
    pub decode: bool,
}

impl Default for AcbExportConfig {
    fn default() -> Self {
        Self {
            export: true,
            decode: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HcaExportConfig {
    pub decode: bool,
}

impl Default for HcaExportConfig {
    fn default() -> Self {
        Self { decode: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ImageExportConfig {
    pub convert_to_webp: bool,
    pub remove_png: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VideoExportConfig {
    pub convert_to_mp4: bool,
    pub direct_usm_to_mp4_with_ffmpeg: bool,
    pub remove_m2v: bool,
}

impl Default for VideoExportConfig {
    fn default() -> Self {
        Self {
            convert_to_mp4: true,
            direct_usm_to_mp4_with_ffmpeg: false,
            remove_m2v: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AudioExportConfig {
    pub convert_to_mp3: bool,
    pub convert_to_flac: bool,
    pub remove_wav: bool,
}

impl Default for AudioExportConfig {
    fn default() -> Self {
        Self {
            convert_to_mp3: true,
            convert_to_flac: false,
            remove_wav: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RegionUploadConfig {
    pub enabled: bool,
    pub providers: Vec<String>,
    pub public_read: UploadPublicReadConfig,
    pub remove_local_after_upload: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UploadPublicReadConfig {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use tempfile::NamedTempFile;

    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn rejects_non_v2_config_version() {
        let config = AppConfig {
            config_version: 1,
            ..AppConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, ConfigError::UnsupportedVersion(1)));
    }

    #[test]
    fn parses_v2_yaml_structure() {
        let yaml = r#"
config_version: 2
server:
  host: 127.0.0.1
  port: 18080
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
        assert_eq!(config.logging.level, "DEBUG");
        assert_eq!(config.execution.retry.attempts, 3);
        assert_eq!(config.enabled_regions(), vec!["jp".to_string()]);
        assert_eq!(
            config.regions["jp"].export.asset_studio_types,
            default_asset_studio_export_types()
        );
    }

    #[test]
    fn asset_studio_and_media_default_to_ffi() {
        let tools = AppConfig::default().tools;
        assert_eq!(MediaBackend::default(), MediaBackend::Ffi);
        assert_eq!(
            AssetStudioFfiCallMode::default(),
            AssetStudioFfiCallMode::Pool
        );
        assert_eq!(tools.media_backend, MediaBackend::Ffi);
        assert_eq!(
            tools.asset_studio_ffi_call_mode,
            AssetStudioFfiCallMode::Pool
        );
        assert_eq!(tools.asset_studio_ffi_process_concurrency, 0);
        assert_eq!(tools.asset_studio_ffi_worker_max_calls, 256);
        assert_eq!(tools.asset_studio_ffi_read_batch_size, 32);
        assert_eq!(tools.asset_studio_ffi_image_format, None);
        assert!(tools.asset_studio_ffi_read_kinds.is_empty());
        assert_eq!(AppConfig::default().concurrency.images, 4);
        assert_eq!(AppConfig::default().concurrency.media_encode, 12);
        assert!(!AppConfig::default().concurrency.auto_tune);
        assert!(AppConfig::default().concurrency.cpu_budget_auto);
        assert_eq!(AppConfig::default().concurrency.cpu_budget_ratio, 0.75);
        assert_eq!(AppConfig::default().concurrency.cpu_reserved, 1);
        assert!(!AppConfig::default().concurrency.cpu_throttle_enabled);
        assert_eq!(AppConfig::default().concurrency.cpu_throttle_sample_ms, 250);
    }

    #[test]
    fn parses_asset_studio_ffi_options() {
        let yaml = r#"
media_backend: ffi
asset_studio_ffi_library_path: /tmp/libHarukiAssetStudioFFI.so
asset_studio_ffi_call_mode: process
asset_studio_ffi_worker_path: /tmp/assetstudio-ffi-worker
asset_studio_ffi_process_concurrency: 6
asset_studio_ffi_worker_max_calls: 128
asset_studio_ffi_read_batch_size: 16
asset_studio_ffi_image_format: raw_rgba
asset_studio_ffi_read_kinds:
  Sprite: image
  Animator: fbx
  all: typetree_json
"#;
        let tools: ToolsConfig = yaml_serde::from_str(yaml).unwrap();
        assert_eq!(tools.media_backend, MediaBackend::Ffi);
        assert_eq!(
            tools.asset_studio_ffi_library_path.as_deref(),
            Some("/tmp/libHarukiAssetStudioFFI.so")
        );
        assert_eq!(
            tools.asset_studio_ffi_call_mode,
            AssetStudioFfiCallMode::Process
        );
        assert_eq!(
            tools.asset_studio_ffi_worker_path.as_deref(),
            Some("/tmp/assetstudio-ffi-worker")
        );
        assert_eq!(tools.asset_studio_ffi_process_concurrency, 6);
        assert_eq!(tools.asset_studio_ffi_worker_max_calls, 128);
        assert_eq!(tools.asset_studio_ffi_read_batch_size, 16);
        assert_eq!(
            tools.asset_studio_ffi_image_format.as_deref(),
            Some("raw_rgba")
        );
        assert_eq!(
            tools
                .asset_studio_ffi_read_kinds
                .get("Animator")
                .map(String::as_str),
            Some("fbx")
        );
        assert_eq!(
            tools
                .asset_studio_ffi_read_kinds
                .get("all")
                .map(String::as_str),
            Some("typetree_json")
        );
    }

    #[test]
    fn parses_legacy_asset_studio_native_option_aliases() {
        let yaml = r#"
asset_studio_native_library_path: /tmp/libHarukiAssetStudioFFI.so
asset_studio_native_call_mode: direct
asset_studio_native_worker_path: /tmp/assetstudio-native-worker
asset_studio_native_process_concurrency: 3
asset_studio_native_worker_max_calls: 32
asset_studio_native_read_batch_size: 8
asset_studio_native_image_format: raw_rgba
asset_studio_native_read_kinds:
  Sprite: image
"#;
        let tools: ToolsConfig = yaml_serde::from_str(yaml).unwrap();

        assert_eq!(
            tools.asset_studio_ffi_library_path.as_deref(),
            Some("/tmp/libHarukiAssetStudioFFI.so")
        );
        assert_eq!(
            tools.asset_studio_ffi_call_mode,
            AssetStudioFfiCallMode::Direct
        );
        assert_eq!(
            tools.asset_studio_ffi_worker_path.as_deref(),
            Some("/tmp/assetstudio-native-worker")
        );
        assert_eq!(tools.asset_studio_ffi_process_concurrency, 3);
        assert_eq!(tools.asset_studio_ffi_worker_max_calls, 32);
        assert_eq!(tools.asset_studio_ffi_read_batch_size, 8);
        assert_eq!(
            tools.asset_studio_ffi_image_format.as_deref(),
            Some("raw_rgba")
        );
        assert_eq!(
            tools
                .asset_studio_ffi_read_kinds
                .get("Sprite")
                .map(String::as_str),
            Some("image")
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
                if field == "tools.media_backend" && value == "sidecar"
        ));
    }

    #[test]
    fn rejects_invalid_asset_studio_ffi_call_mode() {
        let err = "threaded".parse::<AssetStudioFfiCallMode>().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "tools.asset_studio_ffi_call_mode" && value == "threaded"
        ));
    }

    #[test]
    fn accepts_zero_asset_studio_ffi_process_concurrency_as_auto() {
        let mut config = AppConfig::default();
        config.tools.asset_studio_ffi_process_concurrency = 0;
        config.validate().unwrap();
        assert!(config.effective_asset_studio_ffi_process_concurrency() >= 1);
    }

    #[test]
    fn rejects_zero_asset_studio_ffi_read_batch_size() {
        let mut config = AppConfig::default();
        config.tools.asset_studio_ffi_read_batch_size = 0;
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "tools.asset_studio_ffi_read_batch_size" && value == "0"
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
    fn rejects_invalid_asset_studio_ffi_image_format() {
        let mut config = AppConfig::default();
        config.tools.asset_studio_ffi_image_format = Some("gif".to_string());
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "tools.asset_studio_ffi_image_format" && value == "gif"
        ));
    }

    #[test]
    fn accepts_raw_rgba_asset_studio_ffi_image_format() {
        let mut config = AppConfig::default();
        config.tools.asset_studio_ffi_image_format = Some("raw_rgba".to_string());
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_asset_studio_ffi_read_kind() {
        let mut config = AppConfig::default();
        config
            .tools
            .asset_studio_ffi_read_kinds
            .insert("Sprite".to_string(), "thumbnail".to_string());
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidValue { ref field, ref value, .. }
                if field == "tools.asset_studio_ffi_read_kinds.Sprite" && value == "thumbnail"
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
    fn load_from_path_expands_env_references_across_config_values() {
        let _env_lock = env_lock();
        std::env::set_var(
            "HARUKI_TEST_AES_KEY_HEX",
            "00112233445566778899aabbccddeeff",
        );
        std::env::set_var("HARUKI_TEST_AES_IV_HEX", "0102030405060708090a0b0c0d0e0f10");
        std::env::set_var("HARUKI_TEST_BEARER_TOKEN", "secret-token");
        std::env::set_var(
            "HARUKI_TEST_ASSET_STUDIO_FFI_LIBRARY_PATH",
            "/tmp/libassetstudio-native.so",
        );
        std::env::set_var(
            "HARUKI_TEST_ASSET_STUDIO_FFI_WORKER_PATH",
            "/tmp/assetstudio-ffi-worker",
        );

        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 2
server:
  auth:
    bearer_token: "${{env:HARUKI_TEST_BEARER_TOKEN}}"
logging:
  access:
    format: "[${{time}}] ${{status}}"
tools:
  asset_studio_ffi_library_path: "${{env:HARUKI_TEST_ASSET_STUDIO_FFI_LIBRARY_PATH}}"
  asset_studio_ffi_worker_path: "${{env:HARUKI_TEST_ASSET_STUDIO_FFI_WORKER_PATH}}"
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
        assert_eq!(
            config.tools.asset_studio_ffi_library_path.as_deref(),
            Some("/tmp/libassetstudio-native.so")
        );
        assert_eq!(
            config.tools.asset_studio_ffi_worker_path.as_deref(),
            Some("/tmp/assetstudio-ffi-worker")
        );
        assert_eq!(config.logging.access.format, "[${time}] ${status}");

        std::env::remove_var("HARUKI_TEST_AES_KEY_HEX");
        std::env::remove_var("HARUKI_TEST_AES_IV_HEX");
        std::env::remove_var("HARUKI_TEST_BEARER_TOKEN");
        std::env::remove_var("HARUKI_TEST_ASSET_STUDIO_FFI_LIBRARY_PATH");
        std::env::remove_var("HARUKI_TEST_ASSET_STUDIO_FFI_WORKER_PATH");
    }

    #[test]
    fn load_from_path_applies_asset_studio_env_overrides() {
        let _env_lock = env_lock();
        let old_media_backend = std::env::var("HARUKI_MEDIA_BACKEND").ok();
        let old_native_path = std::env::var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH").ok();
        let old_call_mode = std::env::var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE").ok();
        let old_worker_path = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH").ok();
        let old_process_concurrency =
            std::env::var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY").ok();
        let old_worker_max_calls = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS").ok();
        let old_read_batch_size = std::env::var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE").ok();
        let old_image_format = std::env::var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT").ok();
        let old_media_encode_concurrency = std::env::var("HARUKI_MEDIA_ENCODE_CONCURRENCY").ok();
        let old_concurrency_auto_tune = std::env::var("HARUKI_CONCURRENCY_AUTO_TUNE").ok();
        let old_cpu_budget_auto = std::env::var("HARUKI_CPU_BUDGET_AUTO").ok();
        let old_cpu_budget_ratio = std::env::var("HARUKI_CPU_BUDGET_RATIO").ok();
        let old_cpu_reserved = std::env::var("HARUKI_CPU_RESERVED").ok();
        let old_cpu_throttle_enabled = std::env::var("HARUKI_CPU_THROTTLE_ENABLED").ok();
        let old_cpu_throttle_sample_ms = std::env::var("HARUKI_CPU_THROTTLE_SAMPLE_MS").ok();
        let old_max_in_flight_bundle_bytes =
            std::env::var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES").ok();
        std::env::set_var("HARUKI_MEDIA_BACKEND", "cli");
        std::env::set_var(
            "HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH",
            "/tmp/override-native.so",
        );
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE", "process");
        std::env::set_var(
            "HARUKI_ASSET_STUDIO_FFI_WORKER_PATH",
            "/tmp/override-native-worker",
        );
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY", "7");
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS", "64");
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE", "48");
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT", "raw_rgba");
        std::env::set_var("HARUKI_MEDIA_ENCODE_CONCURRENCY", "9");
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
config_version: 2
tools:
  asset_studio_ffi_library_path: /tmp/config-native.so
  asset_studio_ffi_call_mode: direct
  asset_studio_ffi_worker_path: /tmp/config-native-worker
  asset_studio_ffi_process_concurrency: 2
  asset_studio_ffi_worker_max_calls: 128
  asset_studio_ffi_read_batch_size: 16
  asset_studio_ffi_image_format: raw_rgba
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(config.tools.media_backend, MediaBackend::Cli);
        assert_eq!(
            config.tools.asset_studio_ffi_library_path.as_deref(),
            Some("/tmp/override-native.so")
        );
        assert_eq!(
            config.tools.asset_studio_ffi_call_mode,
            AssetStudioFfiCallMode::Process
        );
        assert_eq!(
            config.tools.asset_studio_ffi_worker_path.as_deref(),
            Some("/tmp/override-native-worker")
        );
        assert_eq!(config.tools.asset_studio_ffi_process_concurrency, 7);
        assert_eq!(config.tools.asset_studio_ffi_worker_max_calls, 64);
        assert_eq!(config.tools.asset_studio_ffi_read_batch_size, 48);
        assert_eq!(
            config.tools.asset_studio_ffi_image_format.as_deref(),
            Some("raw_rgba")
        );
        assert_eq!(config.concurrency.media_encode, 9);
        assert!(config.concurrency.auto_tune);
        assert!(config.concurrency.cpu_budget_auto);
        assert_eq!(config.concurrency.cpu_budget_ratio, 0.5);
        assert_eq!(config.concurrency.cpu_reserved, 2);
        assert!(config.concurrency.cpu_throttle_enabled);
        assert_eq!(config.concurrency.cpu_throttle_sample_ms, 500);
        assert_eq!(config.execution.max_in_flight_bundle_bytes, 1_048_576);

        match old_media_backend {
            Some(value) => std::env::set_var("HARUKI_MEDIA_BACKEND", value),
            None => std::env::remove_var("HARUKI_MEDIA_BACKEND"),
        }
        match old_native_path {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH"),
        }
        match old_call_mode {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE"),
        }
        match old_worker_path {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH"),
        }
        match old_process_concurrency {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY"),
        }
        match old_worker_max_calls {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS"),
        }
        match old_read_batch_size {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE"),
        }
        match old_image_format {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT"),
        }
        match old_media_encode_concurrency {
            Some(value) => std::env::set_var("HARUKI_MEDIA_ENCODE_CONCURRENCY", value),
            None => std::env::remove_var("HARUKI_MEDIA_ENCODE_CONCURRENCY"),
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
    fn load_from_path_accepts_legacy_asset_studio_native_env_overrides() {
        let _env_lock = env_lock();
        let old_current_library = std::env::var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH").ok();
        let old_current_mode = std::env::var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE").ok();
        let old_legacy_library = std::env::var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH").ok();
        let old_legacy_mode = std::env::var("HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE").ok();

        std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH");
        std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE");
        std::env::set_var(
            "HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH",
            "/tmp/legacy-native.so",
        );
        std::env::set_var("HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE", "process");

        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 2
tools:
  asset_studio_ffi_library_path: /tmp/config-ffi.so
  asset_studio_ffi_call_mode: direct
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(
            config.tools.asset_studio_ffi_library_path.as_deref(),
            Some("/tmp/legacy-native.so")
        );
        assert_eq!(
            config.tools.asset_studio_ffi_call_mode,
            AssetStudioFfiCallMode::Process
        );

        match old_current_library {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH"),
        }
        match old_current_mode {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_CALL_MODE"),
        }
        match old_legacy_library {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH"),
        }
        match old_legacy_mode {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE"),
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
config_version: 2
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
config_version: 2
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
            cpu_budget_auto: true,
            cpu_budget_ratio: 0.75,
            cpu_reserved: 1,
            cpu_throttle_enabled: false,
            cpu_throttle_sample_ms: 250,
            download: 999,
            upload: 999,
            acb: 999,
            usm: 999,
            hca: 999,
            media_encode: 999,
            images: 999,
        };

        let effective = config.effective();

        assert!(effective.auto_tune);
        assert!(effective.download <= config.download);
        assert!(effective.upload <= config.upload);
        assert!(effective.acb <= config.acb);
        assert!(effective.usm <= config.usm);
        assert!(effective.hca <= config.hca);
        assert!(effective.media_encode <= config.media_encode);
        assert!(effective.images <= config.images);
        assert!(effective.download >= 1);
        assert!(effective.media_encode >= 1);
    }

    #[test]
    fn effective_cpu_budget_and_native_auto_scale_by_cpu_count() {
        let config = AppConfig::default();
        assert_eq!(config.concurrency.effective_cpu_budget_for_cpus(4), 2);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(4),
            2
        );
        assert_eq!(config.concurrency.effective_cpu_budget_for_cpus(8), 5);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(8),
            5
        );
        assert_eq!(config.concurrency.effective_cpu_budget_for_cpus(10), 6);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(10),
            6
        );
        assert_eq!(config.concurrency.effective_cpu_budget_for_cpus(64), 47);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(64),
            47
        );
    }

    #[test]
    fn explicit_native_concurrency_overrides_auto() {
        let mut config = AppConfig::default();
        config.tools.asset_studio_ffi_process_concurrency = 56;
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(8),
            56
        );
    }

    #[test]
    fn native_auto_oversubscribes_when_cpu_throttle_is_enabled() {
        let mut config = AppConfig::default();
        config.concurrency.cpu_throttle_enabled = true;

        assert_eq!(config.concurrency.effective_cpu_budget_for_cpus(10), 6);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(10),
            10
        );
        assert_eq!(config.concurrency.effective_cpu_budget_for_cpus(64), 47);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(64),
            64
        );
    }

    #[test]
    fn rejects_invalid_cpu_budget_ratio() {
        for ratio in [0.0, -0.5, 1.5] {
            let mut config = AppConfig::default();
            config.concurrency.cpu_budget_ratio = ratio;
            let err = config.validate().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidValue { ref field, .. }
                    if field == "concurrency.cpu_budget_ratio"
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
config_version: 2
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
