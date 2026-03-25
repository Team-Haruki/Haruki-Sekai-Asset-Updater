use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("config_version must be 2, got {0}")]
    UnsupportedVersion(u32),
    #[error("no v2 config file found; tried: {0}")]
    MissingConfigFile(String),
    #[error("invalid region key `{0}`; region keys must be lowercase")]
    InvalidRegionName(String),
    #[error("missing required environment variable `{name}` referenced by `{field}`")]
    MissingEnvironmentVariable { field: String, name: String },
}

#[derive(Debug, Error)]
pub enum RegionError {
    #[error("region `{0}` not found")]
    NotFound(String),
    #[error("region `{0}` is disabled")]
    Disabled(String),
}

#[derive(Debug, Error)]
pub enum DownloadRecordError {
    #[error("failed to read download record {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse download record {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to create parent directory for {path}: {source}")]
    CreateParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write download record {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize download record for {path}: {source}")]
    Serialize {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("path {0} is not valid UTF-8 for cridecoder file APIs")]
    NonUtf8Path(PathBuf),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("ACB extraction failed: {0}")]
    Acb(String),
    #[error("USM extraction failed: {0}")]
    Usm(String),
    #[error("USM metadata read failed: {0}")]
    Metadata(String),
    #[error("HCA decode failed: {0}")]
    Hca(String),
}

#[derive(Debug, Error)]
pub enum PlanningError {
    #[error(transparent)]
    Region(#[from] RegionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("region `{region}` is missing a downloaded_asset_record_file path")]
    MissingDownloadRecordPath { region: String },
}

#[derive(Debug, Error)]
pub enum GitSyncError {
    #[error("git chart-hash sync is disabled")]
    Disabled,
    #[error("git chart-hash sync requires repository_dir")]
    MissingRepositoryDir,
    #[error("git operation failed: {0}")]
    Git(#[from] git2::Error),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize chart hashes for {path}: {source}")]
    Serialize {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("repository workdir is unavailable")]
    MissingWorkdir,
    #[error("repository HEAD is detached or unnamed")]
    MissingBranch,
    #[error("remote `origin` not found")]
    MissingOrigin,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage provider `{provider}` is missing an endpoint")]
    MissingEndpoint { provider: String },
    #[error("storage provider `{provider}` is missing a bucket")]
    MissingBucket { provider: String },
    #[error("upload root {0} is not relative to the extracted save path")]
    InvalidRelativePath(String),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("s3 upload failed for provider `{provider}` file `{path}`: {source}")]
    Upload {
        provider: String,
        path: PathBuf,
        #[source]
        source: aws_sdk_s3::Error,
    },
    #[error("task join failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Error)]
pub enum ExportPipelineError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("image codec error at {path}: {source}")]
    Image {
        path: PathBuf,
        #[source]
        source: image::ImageError,
    },
    #[error("failed to spawn command `{program}`: {source}")]
    Spawn {
        program: String,
        #[source]
        source: std::io::Error,
    },
    #[error("command `{program}` failed with status {status}: {stderr}")]
    CommandFailed {
        program: String,
        status: String,
        stderr: String,
    },
    #[error("failed to spawn worker `{worker}`: {source}")]
    WorkerSpawn {
        worker: String,
        #[source]
        source: std::io::Error,
    },
    #[error("worker `{worker}` panicked: {message}")]
    WorkerPanic { worker: String, message: String },
}

#[derive(Debug, Error)]
pub enum AssetExecutionError {
    #[error(transparent)]
    Region(#[from] RegionError),
    #[error(transparent)]
    DownloadRecord(#[from] DownloadRecordError),
    #[error(transparent)]
    ExportPipeline(#[from] ExportPipelineError),
    #[error(transparent)]
    GitSync(#[from] GitSyncError),
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to initialize HTTP client: {0}")]
    HttpClient(String),
    #[error("HTTP request to {url} returned status {status}")]
    HttpStatus { url: String, status: u16 },
    #[error("region `{region}` is missing asset_save_dir")]
    MissingAssetSaveDir { region: String },
    #[error("colorful_palette region `{region}` requires asset_version and asset_hash")]
    MissingAssetVersionOrHash { region: String },
    #[error("colorful_palette region `{region}` is missing profile hash for `{profile}`")]
    MissingProfileHash { region: String, profile: String },
    #[error("region `{region}` is missing AES key or IV configuration")]
    MissingCryptoConfig { region: String },
    #[error("invalid AES key hex: {0}")]
    InvalidAesKeyHex(String),
    #[error("invalid AES IV hex: {0}")]
    InvalidAesIvHex(String),
    #[error("invalid AES IV length: got {got}, want 16")]
    InvalidAesIvLength { got: usize },
    #[error("encrypted content cannot be empty")]
    EmptyEncryptedContent,
    #[error("encrypted content length is not a multiple of AES block size")]
    InvalidEncryptedBlockSize,
    #[error("failed to decrypt or deserialize asset info: {0}")]
    AssetInfoDecode(String),
    #[error("failed to create temp directory for {path}: {source}")]
    CreateTempDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write temp file {path}: {source}")]
    WriteTempFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("job execution cancelled")]
    Cancelled,
}
