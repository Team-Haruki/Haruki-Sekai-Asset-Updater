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

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::schema::{
        default_asset_studio_export_types, AssetHttpVersion, BackendsConfig, ImageBackend,
        ImagePngCompression, MediaBackend,
    };
    use super::super::test_support::{env_lock, restore_env};

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
    #[allow(clippy::await_holding_lock)]
    fn config_uri_parser_and_bootstrap_options_reject_invalid_inputs() {
        for uri in [
            "file:///tmp/config",
            "opendal://missing-path",
            "opendal:///path",
            "opendal://fs/",
        ] {
            assert!(parse_config_storage_uri(uri).is_err(), "{uri}");
        }
        assert_eq!(
            parse_config_storage_uri("opendal://named/a\\b.yaml").unwrap(),
            ConfigStorageUri {
                provider: "named".to_string(),
                path: "a/b.yaml".to_string(),
            }
        );

        let _env_lock = env_lock();
        let old_scheme = std::env::var(CONFIG_OPENDAL_SCHEME_ENV).ok();
        let old_root = std::env::var(CONFIG_OPENDAL_ROOT_ENV).ok();
        std::env::remove_var(CONFIG_OPENDAL_SCHEME_ENV);
        assert!(config_storage_provider_options().is_err());
        std::env::set_var(CONFIG_OPENDAL_SCHEME_ENV, " FS ");
        std::env::set_var(CONFIG_OPENDAL_ROOT_ENV, " /tmp/config-root ");
        let (scheme, options) = config_storage_provider_options().unwrap();
        assert_eq!(scheme, "fs");
        assert_eq!(options["root"], "/tmp/config-root");
        restore_env(CONFIG_OPENDAL_SCHEME_ENV, old_scheme);
        restore_env(CONFIG_OPENDAL_ROOT_ENV, old_root);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn config_loaders_report_file_storage_utf8_and_yaml_errors() {
        let _env_lock = env_lock();
        let dir = tempfile::tempdir().unwrap();
        assert!(AppConfig::load_from_path(dir.path().join("missing.yaml")).is_err());
        let invalid_yaml = dir.path().join("invalid.yaml");
        std::fs::write(&invalid_yaml, "config_version: [").unwrap();
        assert!(AppConfig::load_from_path(&invalid_yaml).is_err());

        let invalid_utf8 = dir.path().join("invalid-utf8.yaml");
        std::fs::write(&invalid_utf8, [0xff, 0xfe]).unwrap();
        let old_scheme = std::env::var(CONFIG_OPENDAL_SCHEME_ENV).ok();
        let old_root = std::env::var(CONFIG_OPENDAL_ROOT_ENV).ok();
        std::env::set_var(CONFIG_OPENDAL_SCHEME_ENV, "fs");
        std::env::set_var(CONFIG_OPENDAL_ROOT_ENV, dir.path());
        assert!(
            AppConfig::load_from_opendal_uri("opendal://config/invalid-utf8.yaml")
                .await
                .is_err()
        );
        assert!(
            AppConfig::load_from_opendal_uri("opendal://config/missing.yaml")
                .await
                .is_err()
        );
        restore_env(CONFIG_OPENDAL_SCHEME_ENV, old_scheme);
        restore_env(CONFIG_OPENDAL_ROOT_ENV, old_root);

        let candidates = candidate_paths();
        assert!(candidates
            .iter()
            .any(|path| path.ends_with("haruki-asset-configs.yaml")));
    }
}
