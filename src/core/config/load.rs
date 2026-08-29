//! Reading a config off disk or out of an `opendal://` source.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::core::errors::ConfigError;

use yaml_serde::Value;

use super::env::{
    apply_env_overrides, expand_env_references, resolve_backend_env_overrides,
    resolve_concurrency_env_overrides, resolve_config_secret_env_overrides,
    resolve_resource_env_overrides,
};
use super::schema::{AppConfig, ConcurrencyConfig, CURRENT_CONFIG_VERSION};
use super::tuning::available_cpu_count;
use super::validate::{
    validate_asset_studio_read_kinds, validate_auth_config, validate_region_names,
    validate_regions, validate_runtime_settings, warn_media_fallback_backend_options,
};

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
        validate_region_names(self)?;
        validate_runtime_settings(self)?;
        validate_auth_config(&self.server.auth)?;
        validate_regions(self)?;
        validate_asset_studio_read_kinds(&self.backends.asset_studio.read_kinds)?;
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

    pub fn enabled_regions(&self) -> Vec<String> {
        self.regions
            .iter()
            .filter_map(|(name, region)| region.enabled.then_some(name.clone()))
            .collect()
    }

    fn resolve_env_overrides(&mut self) -> Result<(), ConfigError> {
        resolve_backend_env_overrides(self)?;
        resolve_concurrency_env_overrides(self)?;
        resolve_resource_env_overrides(self)?;
        resolve_config_secret_env_overrides(self)
    }
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
