use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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
        let mut config: Self = serde_yaml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
        config.resolve_env_references()?;
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

    fn resolve_env_references(&mut self) -> Result<(), ConfigError> {
        resolve_secret_env(
            "server.auth.bearer_token",
            &mut self.server.auth.bearer_token,
        )?;
        resolve_secret_env(
            "tools.asset_studio_cli_path",
            &mut self.tools.asset_studio_cli_path,
        )?;
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Pretty,
    Json,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Pretty
    }
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
    pub retry: RetryConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 300,
            allow_cancel: true,
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
    pub kind: String,
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
            kind: "s3".to_string(),
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct GitSyncConfig {
    pub chart_hashes: ChartHashConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ChartHashConfig {
    pub enabled: bool,
    pub repository_dir: Option<String>,
    pub username: Option<String>,
    pub email: Option<String>,
    pub password: Option<String>,
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
    pub usm: UsmExportConfig,
    pub acb: AcbExportConfig,
    pub hca: HcaExportConfig,
    pub images: ImageExportConfig,
    pub video: VideoExportConfig,
    pub audio: AudioExportConfig,
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

        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();

        assert_eq!(config.server.port, 18080);
        assert_eq!(config.logging.level, "DEBUG");
        assert_eq!(config.execution.retry.attempts, 3);
        assert_eq!(config.enabled_regions(), vec!["jp".to_string()]);
    }

    #[test]
    fn load_from_path_resolves_secret_env_references_only_for_supported_fields() {
        std::env::set_var(
            "HARUKI_TEST_AES_KEY_HEX",
            "00112233445566778899aabbccddeeff",
        );
        std::env::set_var("HARUKI_TEST_AES_IV_HEX", "0102030405060708090a0b0c0d0e0f10");
        std::env::set_var("HARUKI_TEST_BEARER_TOKEN", "secret-token");
        std::env::set_var("HARUKI_TEST_ASSET_STUDIO_CLI_PATH", "/tmp/assetstudio");

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
        assert_eq!(config.logging.access.format, "[${time}] ${status}");

        std::env::remove_var("HARUKI_TEST_AES_KEY_HEX");
        std::env::remove_var("HARUKI_TEST_AES_IV_HEX");
        std::env::remove_var("HARUKI_TEST_BEARER_TOKEN");
        std::env::remove_var("HARUKI_TEST_ASSET_STUDIO_CLI_PATH");
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
