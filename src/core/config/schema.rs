//! Every configuration type, its default, and its `FromStr`.
//!
//! Shape only: loading lives in [`super::load`], environment overrides in
//! [`super::env`], rejection rules in [`super::validate`], and host-derived
//! pool sizing in [`super::tuning`].

use std::collections::BTreeMap;
use std::str::FromStr;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use yaml_serde::Value;

use crate::core::errors::ConfigError;

pub const CURRENT_CONFIG_VERSION: u32 = 3;

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
    pub read_batch_size: usize,
    pub image_format: Option<String>,
    pub read_kinds: BTreeMap<String, String>,
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
    /// Optional persistent cache for deobfuscated AssetBundles. Cache entries
    /// are stored below `<root>/<region>/<bundle path>` and reused by update
    /// and prefetch runs before the network is consulted.
    pub asset_bundle_cache_dir: Option<String>,
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
            asset_bundle_cache_dir: None,
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

pub const DEFAULT_ASSET_STUDIO_EXPORT_TYPES: &[&str] = &["all"];

pub(super) fn default_asset_studio_export_types() -> Vec<String> {
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
    pub shared_content_store: String,
    pub compiled_content_store: String,
    pub process_concurrency: usize,
    pub convert_model_textures: bool,
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
            shared_content_store: String::new(),
            compiled_content_store: String::new(),
            process_concurrency: 0,
            convert_model_textures: false,
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

// Deduplication normalises a configured list; it does not reject anything, so
// it sits with the shape rather than with the rules in `super::validate`.
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
