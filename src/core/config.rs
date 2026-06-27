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
pub const CURRENT_CONFIG_VERSION: u32 = 3;

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
    pub backends: BackendsConfig,
    pub resources: ResourcesConfig,
    pub concurrency: ConcurrencyConfig,
    pub storage: StorageConfig,
    pub git_sync: GitSyncConfig,
    pub regions: BTreeMap<String, RegionConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            config_version: CURRENT_CONFIG_VERSION,
            server: ServerConfig::default(),
            logging: LoggingConfig::default(),
            execution: ExecutionConfig::default(),
            backends: BackendsConfig::default(),
            resources: ResourcesConfig::default(),
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
        config.resolve_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.config_version != CURRENT_CONFIG_VERSION {
            return Err(ConfigError::UnsupportedVersion(self.config_version));
        }

        for region_name in self.regions.keys() {
            if region_name.to_lowercase() != *region_name {
                return Err(ConfigError::InvalidRegionName(region_name.clone()));
            }
        }
        if !(0.0..=1.0).contains(&self.resources.cpu.budget_ratio)
            || self.resources.cpu.budget_ratio == 0.0
        {
            return Err(ConfigError::InvalidValue {
                field: "resources.cpu.budget_ratio".to_string(),
                value: self.resources.cpu.budget_ratio.to_string(),
                expected: "a number greater than 0 and less than or equal to 1".to_string(),
            });
        }
        if self.backends.asset_studio.read_batch_size == 0 {
            return Err(ConfigError::InvalidValue {
                field: "backends.asset_studio.read_batch_size".to_string(),
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
        if self.concurrency.audio_encode == 0 {
            return Err(ConfigError::InvalidValue {
                field: "concurrency.audio_encode".to_string(),
                value: "0".to_string(),
                expected: "a positive integer".to_string(),
            });
        }
        if self.concurrency.video_encode == 0 {
            return Err(ConfigError::InvalidValue {
                field: "concurrency.video_encode".to_string(),
                value: "0".to_string(),
                expected: "a positive integer".to_string(),
            });
        }
        if let Some(image_format) = &self.backends.asset_studio.image_format {
            validate_asset_studio_ffi_image_format(image_format)?;
        }
        validate_image_backend(&self.backends.image)?;
        // Enabling auth without any credential is fail-open (every request authorized). Reject it
        // so a misconfiguration can't silently disable protection on the mutating endpoints.
        if self.server.auth.enabled {
            let has_credential = self
                .server
                .auth
                .bearer_token
                .as_deref()
                .is_some_and(|token| !token.is_empty())
                || self
                    .server
                    .auth
                    .user_agent_prefix
                    .as_deref()
                    .is_some_and(|prefix| !prefix.is_empty());
            if !has_credential {
                return Err(ConfigError::InvalidValue {
                    field: "server.auth".to_string(),
                    value: "enabled=true with no credentials".to_string(),
                    expected: "a non-empty bearer_token or user_agent_prefix when auth is enabled"
                        .to_string(),
                });
            }
        }
        for (region_name, region) in &self.regions {
            validate_image_export_config(region_name, &region.export.images)?;
            validate_video_export_config(region_name, &region.export.video)?;
            validate_audio_export_config(region_name, &region.export.audio)?;
            validate_haruki_3d_export_config(region_name, &region.export.haruki_3d)?;
            // Fail fast on bad crypto material / filter regexes for regions that are actually in
            // use, instead of blowing up mid-job at decrypt or silently dropping a typo'd filter.
            if region.enabled {
                validate_region_crypto(region_name, &region.crypto)?;
                validate_region_filter_regexes(region_name, region)?;
            }
        }
        validate_asset_studio_ffi_read_kinds(&self.backends.asset_studio.read_kinds)?;
        warn_media_fallback_backend_options(&self.backends.media);

        Ok(())
    }

    pub fn effective_concurrency(&self) -> ConcurrencyConfig {
        self.effective_concurrency_for_cpus(available_cpu_count())
    }

    pub fn effective_concurrency_for_cpus(&self, cpus: usize) -> ConcurrencyConfig {
        self.concurrency.effective_for_cpus_with_budget(
            cpus,
            self.resources.cpu.effective_budget_for_cpus(cpus),
        )
    }

    pub fn effective_cpu_budget(&self) -> usize {
        self.resources.cpu.effective_budget()
    }

    pub fn effective_asset_studio_ffi_process_concurrency(&self) -> usize {
        self.effective_asset_studio_ffi_process_concurrency_for_cpus(available_cpu_count())
    }

    pub fn effective_asset_studio_ffi_process_concurrency_for_cpus(&self, cpus: usize) -> usize {
        let configured = self.backends.asset_studio.process_concurrency;
        if configured > 0 {
            return configured;
        }
        let cpus = cpus.max(1);
        let cpu_budget = self.resources.cpu.effective_budget_for_cpus(cpus);
        if self.resources.cpu.throttle.enabled {
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

    fn resolve_env_overrides(&mut self) -> Result<(), ConfigError> {
        if let Ok(value) = env::var("HARUKI_MEDIA_BACKEND") {
            self.backends.media.backend = value.parse()?;
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH") {
            self.backends.asset_studio.library_path = non_empty_option(value);
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_MODE") {
            self.backends.asset_studio.mode = value.parse()?;
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH") {
            self.backends.asset_studio.worker_path = non_empty_option(value);
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY") {
            self.backends.asset_studio.process_concurrency =
                parse_usize_env("backends.asset_studio.process_concurrency", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS") {
            self.backends.asset_studio.worker_max_calls =
                parse_usize_env("backends.asset_studio.worker_max_calls", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE") {
            self.backends.asset_studio.read_batch_size =
                parse_positive_usize("backends.asset_studio.read_batch_size", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT") {
            self.backends.asset_studio.image_format =
                non_empty_option(normalize_asset_studio_ffi_image_format(&value)?);
        }
        if let Ok(value) = env::var("HARUKI_ASSET_HTTP_VERSION") {
            self.server.asset_http_version = value.parse()?;
        }
        if let Ok(value) = env::var("HARUKI_MEDIA_ENCODE_CONCURRENCY") {
            let parsed = parse_positive_usize("concurrency.media_encode", &value)?;
            self.concurrency.media_encode = parsed;
            self.concurrency.audio_encode = parsed;
            self.concurrency.video_encode = parsed;
        }
        if let Ok(value) = env::var("HARUKI_AUDIO_ENCODE_CONCURRENCY") {
            self.concurrency.audio_encode =
                parse_positive_usize("concurrency.audio_encode", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_VIDEO_ENCODE_CONCURRENCY") {
            self.concurrency.video_encode =
                parse_positive_usize("concurrency.video_encode", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_DOWNLOAD_CONCURRENCY") {
            self.concurrency.download = parse_positive_usize("concurrency.download", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_POST_PROCESS_CONCURRENCY") {
            self.concurrency.post_process =
                parse_positive_usize("concurrency.post_process", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CONCURRENCY_AUTO_TUNE") {
            self.concurrency.auto_tune = parse_bool_env("concurrency.auto_tune", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_BUDGET_AUTO") {
            self.resources.cpu.budget_auto = parse_bool_env("resources.cpu.budget_auto", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_BUDGET_RATIO") {
            self.resources.cpu.budget_ratio =
                parse_cpu_ratio_env("resources.cpu.budget_ratio", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_RESERVED") {
            self.resources.cpu.reserved = parse_usize_env("resources.cpu.reserved", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_THROTTLE_ENABLED") {
            self.resources.cpu.throttle.enabled =
                parse_bool_env("resources.cpu.throttle.enabled", &value)?;
        }
        if let Ok(value) = env::var("HARUKI_CPU_THROTTLE_SAMPLE_MS") {
            self.resources.cpu.throttle.sample_ms =
                parse_positive_usize("resources.cpu.throttle.sample_ms", &value)? as u64;
        }
        if let Ok(value) = env::var("HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES") {
            self.resources.memory.max_in_flight_bundle_bytes =
                parse_usize_env("resources.memory.max_in_flight_bundle_bytes", &value)?;
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

fn validate_region_crypto(region_name: &str, crypto: &CryptoConfig) -> Result<(), ConfigError> {
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

fn validate_aes_hex(
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

fn validate_region_filter_regexes(
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

fn validate_regex_patterns(field: &str, patterns: &[String]) -> Result<(), ConfigError> {
    for pattern in patterns {
        regex::Regex::new(pattern).map_err(|source| ConfigError::InvalidValue {
            field: field.to_string(),
            value: pattern.clone(),
            expected: format!("a valid regular expression ({source})"),
        })?;
    }
    Ok(())
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
            field: "backends.asset_studio.image_format".to_string(),
            value: other.to_string(),
            expected: "raw_rgba".to_string(),
        }),
    }
}

fn validate_image_backend(image: &ImageBackendConfig) -> Result<(), ConfigError> {
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

fn validate_image_export_config(
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

fn validate_video_export_config(
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

fn validate_audio_export_config(
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

fn validate_haruki_3d_export_config(
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

fn dedupe_image_formats(formats: Vec<ImageOutputFormat>) -> Vec<ImageOutputFormat> {
    let mut output = Vec::new();
    for format in formats {
        if !output.contains(&format) {
            output.push(format);
        }
    }
    output
}

fn dedupe_video_formats(formats: Vec<VideoOutputFormat>) -> Vec<VideoOutputFormat> {
    let mut output = Vec::new();
    for format in formats {
        if !output.contains(&format) {
            output.push(format);
        }
    }
    output
}

fn dedupe_audio_formats(formats: Vec<AudioOutputFormat>) -> Vec<AudioOutputFormat> {
    let mut output = Vec::new();
    for format in formats {
        if !output.contains(&format) {
            output.push(format);
        }
    }
    output
}

fn validate_asset_studio_ffi_read_kinds(
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
        validate_asset_studio_ffi_read_kind(
            &format!("backends.asset_studio.read_kinds.{asset_type}"),
            kind,
        )?;
    }
    Ok(())
}

fn warn_media_fallback_backend_options(media: &MediaBackendConfig) {
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

fn validate_asset_studio_ffi_read_kind(field: &str, value: &str) -> Result<(), ConfigError> {
    match value.trim().to_lowercase().as_str() {
        "auto" | "raw" | "typetree_json" | "image" | "image_archive" | "audio" | "video"
        | "font" | "shader" | "text" | "text_bytes" | "mesh" | "obj" | "animator" | "fbx"
        | "pjsk_model_package" | "pjsk_animation_clip_decoded" => Ok(()),
        other => Err(ConfigError::InvalidValue {
            field: field.to_string(),
            value: other.to_string(),
            expected: "auto, raw, typetree_json, image, image_archive, audio, video, font, shader, text, text_bytes, mesh, obj, animator, fbx, pjsk_model_package, or pjsk_animation_clip_decoded".to_string(),
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
    pub asset_http_version: AssetHttpVersion,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            proxy: None,
            asset_http_version: AssetHttpVersion::Auto,
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetHttpVersion {
    #[default]
    Auto,
    Http1,
}

impl FromStr for AssetHttpVersion {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "http1" | "http1_only" | "http/1" | "http/1.1" => Ok(Self::Http1),
            other => Err(ConfigError::InvalidValue {
                field: "server.asset_http_version".to_string(),
                value: other.to_string(),
                expected: "auto or http1".to_string(),
            }),
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BackendsConfig {
    pub asset_studio: AssetStudioBackendConfig,
    pub media: MediaBackendConfig,
    pub image: ImageBackendConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AssetStudioBackendConfig {
    pub library_path: Option<String>,
    pub mode: AssetStudioFfiMode,
    pub worker_path: Option<String>,
    pub process_concurrency: usize,
    pub worker_max_calls: usize,
    pub read_batch_size: usize,
    pub image_format: Option<String>,
    pub read_kinds: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetStudioFfiMode {
    Direct,
    #[default]
    WorkerPool,
}

impl FromStr for AssetStudioFfiMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "direct" => Ok(Self::Direct),
            "worker_pool" | "worker-pool" | "worker" | "pool" => Ok(Self::WorkerPool),
            other => Err(ConfigError::InvalidValue {
                field: "backends.asset_studio.mode".to_string(),
                value: other.to_string(),
                expected: "direct or worker_pool".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MediaBackendConfig {
    pub backend: MediaBackend,
    pub ffmpeg_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ImageBackendConfig {
    pub backend: ImageBackend,
    pub png_compression: ImagePngCompression,
    pub webp_lossless: bool,
    pub jpeg_quality: u8,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageBackend {
    #[default]
    Rust,
}

impl FromStr for ImageBackend {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "rust" => Ok(Self::Rust),
            other => Err(ConfigError::InvalidValue {
                field: "backends.image.backend".to_string(),
                value: other.to_string(),
                expected: "rust".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImagePngCompression {
    #[default]
    Fast,
    Default,
    Best,
}

impl FromStr for ImagePngCompression {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "fast" => Ok(Self::Fast),
            "default" => Ok(Self::Default),
            "best" => Ok(Self::Best),
            other => Err(ConfigError::InvalidValue {
                field: "backends.image.png_compression".to_string(),
                value: other.to_string(),
                expected: "fast, default, or best".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageOutputFormat {
    Png,
    Jpg,
    Webp,
}

impl FromStr for ImageOutputFormat {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "png" => Ok(Self::Png),
            "jpg" | "jpeg" => Ok(Self::Jpg),
            "webp" => Ok(Self::Webp),
            other => Err(ConfigError::InvalidValue {
                field: "export.images.formats".to_string(),
                value: other.to_string(),
                expected: "png, jpg, or webp".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VideoOutputFormat {
    M2v,
    Mp4,
}

impl FromStr for VideoOutputFormat {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "m2v" => Ok(Self::M2v),
            "mp4" => Ok(Self::Mp4),
            other => Err(ConfigError::InvalidValue {
                field: "export.video.formats".to_string(),
                value: other.to_string(),
                expected: "m2v or mp4".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AudioOutputFormat {
    Wav,
    Flac,
    Mp3,
}

impl FromStr for AudioOutputFormat {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_lowercase().as_str() {
            "wav" => Ok(Self::Wav),
            "flac" => Ok(Self::Flac),
            "mp3" => Ok(Self::Mp3),
            other => Err(ConfigError::InvalidValue {
                field: "export.audio.formats".to_string(),
                value: other.to_string(),
                expected: "wav, flac, or mp3".to_string(),
            }),
        }
    }
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
                field: "backends.media.backend".to_string(),
                value: other.to_string(),
                expected: "auto, ffi, or cli".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub timeout_seconds: u64,
    pub allow_cancel: bool,
    /// Soft process memory guard for bundle work.  When non-zero, bundle
    /// downloads/native payloads acquire permits by estimated bundle size and
    /// keep them until export/post-process finishes.
    pub max_in_flight_bundle_bytes: usize,
    /// How many successful downloads to accumulate before flushing the download
    /// record to disk mid-run.  Set to `0` to disable mid-run flushing (record
    /// is only written once at the end).  Mirrors Go's `batchSaveSize`.
    pub batch_save_size: usize,
    /// Maximum number of jobs whose heavy download/export pipeline may run at once. Extra jobs are
    /// accepted (HTTP 202) but queue for a slot instead of all running concurrently. `0` = no limit.
    pub max_concurrent_jobs: usize,
    /// Cap on retained terminal (Completed/Failed/Cancelled) job snapshots kept in memory for the
    /// jobs API. Oldest terminal jobs are evicted beyond this. `0` = keep all (unbounded).
    pub retain_terminal_jobs: usize,
    pub retry: RetryConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 300,
            allow_cancel: true,
            max_in_flight_bundle_bytes: 0,
            batch_save_size: 50,
            max_concurrent_jobs: 4,
            retain_terminal_jobs: 256,
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

impl Default for AssetStudioBackendConfig {
    fn default() -> Self {
        Self {
            library_path: None,
            mode: AssetStudioFfiMode::WorkerPool,
            worker_path: None,
            process_concurrency: 0,
            worker_max_calls: 256,
            read_batch_size: 64,
            image_format: None,
            read_kinds: BTreeMap::new(),
        }
    }
}

impl Default for MediaBackendConfig {
    fn default() -> Self {
        Self {
            backend: MediaBackend::Ffi,
            ffmpeg_path: "ffmpeg".to_string(),
        }
    }
}

impl Default for ImageBackendConfig {
    fn default() -> Self {
        Self {
            backend: ImageBackend::Rust,
            png_compression: ImagePngCompression::Fast,
            webp_lossless: true,
            jpeg_quality: 95,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConcurrencyConfig {
    pub auto_tune: bool,
    pub download: usize,
    pub upload: usize,
    pub post_process: usize,
    pub acb: usize,
    pub usm: usize,
    pub hca: usize,
    /// Legacy aggregate media encode cap. New configs should prefer
    /// audio_encode and video_encode.
    pub media_encode: usize,
    pub audio_encode: usize,
    pub video_encode: usize,
    pub images: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            auto_tune: false,
            download: 32,
            upload: 4,
            post_process: 16,
            acb: 12,
            usm: 6,
            hca: 16,
            media_encode: 12,
            audio_encode: 12,
            video_encode: 4,
            images: 12,
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
        let cpu_budget = ResourcesConfig::default()
            .cpu
            .effective_budget_for_cpus(cpus.max(1));
        self.effective_for_cpus_with_budget(cpus, cpu_budget)
    }

    pub fn effective_for_cpus_with_budget(&self, cpus: usize, cpu_budget: usize) -> Self {
        if !self.auto_tune {
            return self.clone();
        }
        let cpus = cpus.max(1);
        let cpu_budget = cpu_budget.max(1);
        let cpu_oversubscribe = cpu_budget.saturating_mul(2).max(cpu_budget);
        Self {
            auto_tune: true,
            download: self.download.min(cpus.saturating_mul(4).max(4)).max(1),
            upload: self.upload.min(cpus.max(2)).max(1),
            post_process: if self.post_process == 0 {
                0
            } else {
                self.post_process.min(cpus.saturating_mul(2).max(2)).max(1)
            },
            acb: self.acb.min(cpu_oversubscribe).max(1),
            usm: self.usm.min(cpus.max(2)).max(1),
            hca: self
                .hca
                .min(cpus.saturating_mul(2).max(2))
                .min(cpu_oversubscribe)
                .max(1),
            media_encode: self.media_encode.min(cpu_oversubscribe).max(1),
            audio_encode: self.audio_encode.min(cpu_oversubscribe).max(1),
            video_encode: self.video_encode.min(cpus.div_ceil(4).max(1)).max(1),
            images: self.images.min(cpu_oversubscribe).max(1),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ResourcesConfig {
    pub cpu: CpuResourceConfig,
    pub memory: MemoryResourceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CpuResourceConfig {
    pub budget_auto: bool,
    pub budget_ratio: f64,
    pub reserved: usize,
    pub throttle: CpuThrottleConfig,
}

impl Default for CpuResourceConfig {
    fn default() -> Self {
        Self {
            budget_auto: true,
            budget_ratio: 1.0,
            reserved: 0,
            throttle: CpuThrottleConfig::default(),
        }
    }
}

impl CpuResourceConfig {
    pub fn effective_budget(&self) -> usize {
        self.effective_budget_for_cpus(available_cpu_count())
    }

    pub fn effective_budget_for_cpus(&self, cpus: usize) -> usize {
        let cpus = cpus.max(1);
        if !self.budget_auto {
            return cpus;
        }
        ((cpus as f64 * self.budget_ratio).floor() as usize)
            .saturating_sub(self.reserved)
            .max(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CpuThrottleConfig {
    pub enabled: bool,
    pub sample_ms: u64,
}

impl Default for CpuThrottleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sample_ms: 250,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MemoryResourceConfig {
    pub max_in_flight_bundle_bytes: usize,
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
    pub raw_bundles: Option<RawBundleExportConfig>,
    pub haruki_3d: Haruki3dExportConfig,
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
            raw_bundles: None,
            haruki_3d: Haruki3dExportConfig::default(),
            usm: UsmExportConfig::default(),
            acb: AcbExportConfig::default(),
            hca: HcaExportConfig::default(),
            images: ImageExportConfig::default(),
            video: VideoExportConfig::default(),
            audio: AudioExportConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RawBundleExportConfig {
    pub output_dir: Option<String>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Haruki3dExportConfig {
    pub enabled: bool,
    pub exporter_path: String,
    pub master_dir: String,
    pub work_dir: String,
    pub manifest_file: String,
    pub staging_dir: String,
    pub output_dir: String,
    pub role_character3d_ids: Vec<i64>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub cleanup_work_dir_after_success: bool,
    pub cleanup_work_dir_after_failure: bool,
    pub cleanup_staging_after_success: bool,
    pub cleanup_staging_after_failure: bool,
}

impl Default for Haruki3dExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter_path: String::new(),
            master_dir: String::new(),
            work_dir: String::new(),
            manifest_file: String::new(),
            staging_dir: String::new(),
            output_dir: String::new(),
            role_character3d_ids: Vec::new(),
            include: Vec::new(),
            exclude: Vec::new(),
            cleanup_work_dir_after_success: true,
            cleanup_work_dir_after_failure: true,
            cleanup_staging_after_success: true,
            cleanup_staging_after_failure: false,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImageExportConfig {
    pub formats: Vec<ImageOutputFormat>,
}

impl Default for ImageExportConfig {
    fn default() -> Self {
        Self {
            formats: vec![ImageOutputFormat::Png],
        }
    }
}

impl ImageExportConfig {
    pub fn output_formats(&self) -> Vec<ImageOutputFormat> {
        dedupe_image_formats(self.formats.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct VideoExportConfig {
    pub formats: Vec<VideoOutputFormat>,
    pub direct_mp4: bool,
}

impl Default for VideoExportConfig {
    fn default() -> Self {
        Self {
            formats: vec![VideoOutputFormat::Mp4],
            direct_mp4: false,
        }
    }
}

impl VideoExportConfig {
    pub fn output_formats(&self) -> Vec<VideoOutputFormat> {
        dedupe_video_formats(self.formats.clone())
    }

    pub fn writes_m2v(&self) -> bool {
        self.output_formats().contains(&VideoOutputFormat::M2v)
    }

    pub fn writes_mp4(&self) -> bool {
        self.output_formats().contains(&VideoOutputFormat::Mp4)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AudioExportConfig {
    pub formats: Vec<AudioOutputFormat>,
}

impl Default for AudioExportConfig {
    fn default() -> Self {
        Self {
            formats: vec![AudioOutputFormat::Mp3],
        }
    }
}

impl AudioExportConfig {
    pub fn output_formats(&self) -> Vec<AudioOutputFormat> {
        dedupe_audio_formats(self.formats.clone())
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
    fn asset_studio_and_media_default_to_ffi() {
        let config = AppConfig::default();
        let asset_studio = &config.backends.asset_studio;
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
        assert_eq!(asset_studio.process_concurrency, 0);
        assert_eq!(asset_studio.worker_max_calls, 256);
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
        assert_eq!(asset_studio.mode, AssetStudioFfiMode::WorkerPool);
    }

    #[test]
    fn parses_asset_studio_ffi_options() {
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
  library_path: /tmp/libHarukiAssetStudioFFI.so
  mode: direct
  worker_path: /tmp/assetstudio-ffi-worker
  process_concurrency: 6
  worker_max_calls: 128
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
        assert_eq!(
            asset_studio.library_path.as_deref(),
            Some("/tmp/libHarukiAssetStudioFFI.so")
        );
        assert_eq!(asset_studio.mode, AssetStudioFfiMode::Direct);
        assert_eq!(
            asset_studio.worker_path.as_deref(),
            Some("/tmp/assetstudio-ffi-worker")
        );
        assert_eq!(asset_studio.process_concurrency, 6);
        assert_eq!(asset_studio.worker_max_calls, 128);
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
    fn accepts_zero_asset_studio_ffi_process_concurrency_as_auto() {
        let mut config = AppConfig::default();
        config.backends.asset_studio.process_concurrency = 0;
        config.validate().unwrap();
        assert!(config.effective_asset_studio_ffi_process_concurrency() >= 1);
    }

    #[test]
    fn rejects_zero_asset_studio_ffi_read_batch_size() {
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
    fn rejects_invalid_asset_studio_ffi_image_format() {
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
    fn accepts_raw_rgba_asset_studio_ffi_image_format() {
        let mut config = AppConfig::default();
        config.backends.asset_studio.image_format = Some("raw_rgba".to_string());
        config.validate().unwrap();
    }

    #[test]
    fn accepts_pjsk_asset_studio_ffi_read_kinds() {
        let mut config = AppConfig::default();
        config
            .backends
            .asset_studio
            .read_kinds
            .insert("Animator".to_string(), "pjsk_model_package".to_string());
        config.backends.asset_studio.read_kinds.insert(
            "AnimationClip".to_string(),
            "pjsk_animation_clip_decoded".to_string(),
        );

        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_asset_studio_ffi_read_kind() {
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
  exporter_path: /app/bin/Haruki-3D-Exporter
  master_dir: /app/data/masterdata
  work_dir: /app/data/3d-work
  manifest_file: /app/data/3d-output/haruki-3d-export-manifest.json
  output_dir: /app/data/3d-output
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
            "/app/bin/Haruki-3D-Exporter"
        );
        assert_eq!(export.haruki_3d.work_dir, "/app/data/3d-work");
        assert_eq!(
            export.haruki_3d.manifest_file,
            "/app/data/3d-output/haruki-3d-export-manifest.json"
        );
        assert_eq!(export.haruki_3d.output_dir, "/app/data/3d-output");
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
        assert_eq!(
            asset_studio.read_kinds.get("Animator").map(String::as_str),
            Some("pjsk_model_package")
        );
        assert_eq!(
            asset_studio
                .read_kinds
                .get("AnimationClip")
                .map(String::as_str),
            Some("pjsk_animation_clip_decoded")
        );

        let jp = config.regions.get("jp").expect("jp region exists");
        assert!(
            jp.export
                .asset_studio_types
                .iter()
                .any(|value| value.eq_ignore_ascii_case("animator")),
            "jp asset_studio_types should request Animator exports"
        );
        assert!(
            jp.export
                .asset_studio_types
                .iter()
                .any(|value| value.eq_ignore_ascii_case("AnimationClip")),
            "jp asset_studio_types should request AnimationClip exports"
        );

        let raw_bundles = jp
            .export
            .raw_bundles
            .as_ref()
            .expect("jp raw bundle retention configured");
        for expected in [
            "live_pv/model/characterv2/body/",
            "live_pv/model/characterv2/face/",
            "live_pv/model/characterv2/head_optional/",
            "live_pv/model/characterv2/color_variation/body/",
            "live_pv/model/characterv2/color_variation/face/",
            "live_pv/model/characterv2/color_variation/head_optional/",
            "character/motion/costume_setting/",
        ] {
            assert!(
                raw_bundles
                    .include
                    .iter()
                    .any(|value| value.contains(expected)),
                "raw_bundles.include should retain {expected}"
            );
        }

        let haruki_3d = &jp.export.haruki_3d;
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
        for expected in [
            "live_pv/model/characterv2/body/",
            "live_pv/model/characterv2/face/",
            "live_pv/model/characterv2/head_optional/",
            "live_pv/model/characterv2/color_variation/body/",
            "live_pv/model/characterv2/color_variation/face/",
            "live_pv/model/characterv2/color_variation/head_optional/",
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
config_version: 3
server:
  auth:
    bearer_token: "${{env:HARUKI_TEST_BEARER_TOKEN}}"
logging:
  access:
    format: "[${{time}}] ${{status}}"
backends:
  asset_studio:
    library_path: "${{env:HARUKI_TEST_ASSET_STUDIO_FFI_LIBRARY_PATH}}"
    worker_path: "${{env:HARUKI_TEST_ASSET_STUDIO_FFI_WORKER_PATH}}"
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
            config.backends.asset_studio.library_path.as_deref(),
            Some("/tmp/libassetstudio-native.so")
        );
        assert_eq!(
            config.backends.asset_studio.worker_path.as_deref(),
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
        let old_asset_studio_mode = std::env::var("HARUKI_ASSET_STUDIO_FFI_MODE").ok();
        let old_worker_path = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH").ok();
        let old_process_concurrency =
            std::env::var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY").ok();
        let old_worker_max_calls = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS").ok();
        let old_read_batch_size = std::env::var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE").ok();
        let old_image_format = std::env::var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT").ok();
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
        std::env::set_var(
            "HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH",
            "/tmp/override-native.so",
        );
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_MODE", "direct");
        std::env::set_var(
            "HARUKI_ASSET_STUDIO_FFI_WORKER_PATH",
            "/tmp/override-native-worker",
        );
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY", "7");
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS", "64");
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE", "48");
        std::env::set_var("HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT", "raw_rgba");
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
    library_path: /tmp/config-native.so
    worker_path: /tmp/config-native-worker
    process_concurrency: 2
    worker_max_calls: 128
    read_batch_size: 16
    image_format: raw_rgba
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(config.backends.media.backend, MediaBackend::Cli);
        assert_eq!(
            config.backends.asset_studio.library_path.as_deref(),
            Some("/tmp/override-native.so")
        );
        assert_eq!(
            config.backends.asset_studio.mode,
            AssetStudioFfiMode::Direct
        );
        assert_eq!(
            config.backends.asset_studio.worker_path.as_deref(),
            Some("/tmp/override-native-worker")
        );
        assert_eq!(config.backends.asset_studio.process_concurrency, 7);
        assert_eq!(config.backends.asset_studio.worker_max_calls, 64);
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
        match old_native_path {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH"),
        }
        match old_asset_studio_mode {
            Some(value) => std::env::set_var("HARUKI_ASSET_STUDIO_FFI_MODE", value),
            None => std::env::remove_var("HARUKI_ASSET_STUDIO_FFI_MODE"),
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
    fn effective_concurrency_preserves_zero_post_process_as_auto() {
        let config = ConcurrencyConfig {
            auto_tune: true,
            post_process: 0,
            ..ConcurrencyConfig::default()
        };

        assert_eq!(config.effective_for_cpus_with_budget(8, 8).post_process, 0);
    }

    #[test]
    fn effective_cpu_budget_and_native_auto_scale_by_cpu_count() {
        let config = AppConfig::default();
        assert_eq!(config.resources.cpu.effective_budget_for_cpus(4), 4);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(4),
            4
        );
        assert_eq!(config.resources.cpu.effective_budget_for_cpus(8), 8);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(8),
            8
        );
        assert_eq!(config.resources.cpu.effective_budget_for_cpus(10), 10);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(10),
            10
        );
        assert_eq!(config.resources.cpu.effective_budget_for_cpus(64), 64);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(64),
            64
        );
    }

    #[test]
    fn explicit_native_concurrency_overrides_auto() {
        let mut config = AppConfig::default();
        config.backends.asset_studio.process_concurrency = 56;
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(8),
            56
        );
    }

    #[test]
    fn native_auto_oversubscribes_when_cpu_throttle_is_enabled() {
        let mut config = AppConfig::default();
        config.resources.cpu.throttle.enabled = true;

        assert_eq!(config.resources.cpu.effective_budget_for_cpus(10), 10);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(10),
            10
        );
        assert_eq!(config.resources.cpu.effective_budget_for_cpus(64), 64);
        assert_eq!(
            config.effective_asset_studio_ffi_process_concurrency_for_cpus(64),
            64
        );
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
