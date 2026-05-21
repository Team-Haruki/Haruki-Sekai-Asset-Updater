use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use yaml_serde::{Mapping, Value};

use crate::core::errors::ConfigError;

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
    pub fn load_default() -> Result<Self, ConfigError> {
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
        let mut value: Value = yaml_serde::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
        expand_env_references(&mut value)?;
        apply_env_overrides(&mut value)?;

        let config: Self = yaml_serde::from_value(value).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
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

        Ok(())
    }

    pub fn enabled_regions(&self) -> Vec<String> {
        self.regions
            .iter()
            .filter_map(|(name, region)| region.enabled.then_some(name.clone()))
            .collect()
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
            .ok_or_else(|| ConfigError::InvalidEnvironmentOverride {
                name: name.to_string(),
                reason: "override names must start with HARUKI__".to_string(),
            })?;

    if raw_path.is_empty() {
        return Err(ConfigError::InvalidEnvironmentOverride {
            name: name.to_string(),
            reason: "override path is empty".to_string(),
        });
    }

    raw_path
        .split("__")
        .map(|segment| {
            if segment.is_empty() {
                Err(ConfigError::InvalidEnvironmentOverride {
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
        return Err(ConfigError::InvalidEnvironmentOverride {
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
    pub asset_studio_cli_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub timeout_seconds: u64,
    pub allow_cancel: bool,
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
            asset_studio_cli_path: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConcurrencyConfig {
    pub download: usize,
    pub upload: usize,
    pub acb: usize,
    pub usm: usize,
    pub hca: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            download: 4,
            upload: 4,
            acb: 8,
            usm: 4,
            hca: 16,
        }
    }
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
    /// Legacy S3-compatible fields. Prefer `scheme`, `root`, and `options` for
    /// new configs; these are retained to keep existing v2 YAML files working.
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RegionExportConfig {
    pub by_category: bool,
    pub asset_studio: AssetStudioExportConfig,
    pub usm: UsmExportConfig,
    pub acb: AcbExportConfig,
    pub hca: HcaExportConfig,
    pub images: ImageExportConfig,
    pub video: VideoExportConfig,
    pub audio: AudioExportConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AssetStudioExportConfig {
    pub export_types: Vec<String>,
}

impl Default for AssetStudioExportConfig {
    fn default() -> Self {
        Self {
            export_types: vec![
                "monoBehaviour".to_string(),
                "textAsset".to_string(),
                "tex2d".to_string(),
                "tex2dArray".to_string(),
                "audio".to_string(),
            ],
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
    pub remove_local_after_upload: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use tempfile::NamedTempFile;

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
    }

    #[test]
    fn load_from_path_resolves_env_references_in_string_fields() {
        std::env::set_var(
            "HARUKI_TEST_AES_KEY_HEX",
            "00112233445566778899aabbccddeeff",
        );
        std::env::set_var("HARUKI_TEST_AES_IV_HEX", "0102030405060708090a0b0c0d0e0f10");
        std::env::set_var("HARUKI_TEST_BEARER_TOKEN", "secret-token");
        std::env::set_var("HARUKI_TEST_ASSET_STUDIO_CLI_PATH", "/tmp/assetstudio");
        std::env::set_var("HARUKI_TEST_STORAGE_BUCKET", "sekai-jp-assets");

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
  asset_studio_cli_path: "${{env:HARUKI_TEST_ASSET_STUDIO_CLI_PATH}}"
storage:
  providers:
    - scheme: s3
      options:
        bucket: "${{env:HARUKI_TEST_STORAGE_BUCKET}}"
        disable_config_load: true
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
            config.tools.asset_studio_cli_path.as_deref(),
            Some("/tmp/assetstudio")
        );
        assert_eq!(
            config.storage.providers[0].options.get("bucket"),
            Some(&"sekai-jp-assets".to_string())
        );
        assert_eq!(
            config.storage.providers[0]
                .options
                .get("disable_config_load"),
            Some(&"true".to_string())
        );
        assert_eq!(config.logging.access.format, "[${time}] ${status}");

        std::env::remove_var("HARUKI_TEST_AES_KEY_HEX");
        std::env::remove_var("HARUKI_TEST_AES_IV_HEX");
        std::env::remove_var("HARUKI_TEST_BEARER_TOKEN");
        std::env::remove_var("HARUKI_TEST_ASSET_STUDIO_CLI_PATH");
        std::env::remove_var("HARUKI_TEST_STORAGE_BUCKET");
    }

    #[test]
    fn load_from_path_applies_haruki_double_underscore_overrides() {
        std::env::set_var("HARUKI__SERVER__PORT", "19090");
        std::env::set_var("HARUKI__CONCURRENCY__UPLOAD", "9");
        std::env::set_var("HARUKI__REGIONS__JP__UPLOAD__ENABLED", "true");
        std::env::set_var("HARUKI__STORAGE__PROVIDERS__0__NAME", "local-assets");
        std::env::set_var("HARUKI__STORAGE__PROVIDERS__0__SCHEME", "fs");
        std::env::set_var("HARUKI__STORAGE__PROVIDERS__0__ROOT", "./tmp/{region}");
        std::env::set_var(
            "HARUKI__STORAGE__PROVIDERS__0__OPTIONS__ATOMIC_WRITE_DIR",
            "./tmp/.opendal",
        );

        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
config_version: 2
server:
  port: 8080
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
"#
        )
        .unwrap();

        let config = AppConfig::load_from_path(file.path()).unwrap();
        assert_eq!(config.server.port, 19090);
        assert_eq!(config.concurrency.upload, 9);
        assert!(config.regions["jp"].upload.enabled);
        assert_eq!(
            config.storage.providers[0].name.as_deref(),
            Some("local-assets")
        );
        assert_eq!(config.storage.providers[0].scheme, "fs");
        assert_eq!(
            config.storage.providers[0].root.as_deref(),
            Some("./tmp/{region}")
        );
        assert_eq!(
            config.storage.providers[0].options.get("atomic_write_dir"),
            Some(&"./tmp/.opendal".to_string())
        );

        std::env::remove_var("HARUKI__SERVER__PORT");
        std::env::remove_var("HARUKI__CONCURRENCY__UPLOAD");
        std::env::remove_var("HARUKI__REGIONS__JP__UPLOAD__ENABLED");
        std::env::remove_var("HARUKI__STORAGE__PROVIDERS__0__NAME");
        std::env::remove_var("HARUKI__STORAGE__PROVIDERS__0__SCHEME");
        std::env::remove_var("HARUKI__STORAGE__PROVIDERS__0__ROOT");
        std::env::remove_var("HARUKI__STORAGE__PROVIDERS__0__OPTIONS__ATOMIC_WRITE_DIR");
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
}
