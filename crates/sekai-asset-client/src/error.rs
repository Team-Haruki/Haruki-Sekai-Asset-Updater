use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientErrorCategory {
    TransientNetwork,
    PermanentHttp,
    SizeExceeded,
    SizeMismatch,
    FileWrite,
    Configuration,
    Manifest,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to initialize the asset HTTP client: {0}")]
    BuildClient(String),
    #[error("asset HTTP request to {url} failed: {source}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("asset HTTP request to {url} returned status {status}")]
    HttpStatus { url: String, status: u16 },
    #[error(
        "asset response from {url} exceeded the {limit}-byte limit (declared {declared:?}, observed {observed})"
    )]
    ResponseTooLarge {
        url: String,
        limit: u64,
        declared: Option<u64>,
        observed: u64,
    },
    #[error(
        "bundle size mismatch for {bundle}: manifest expected {expected}, wire response was {wire}, decoded file was {decoded} bytes"
    )]
    BundleSizeMismatch {
        bundle: String,
        expected: u64,
        wire: u64,
        decoded: u64,
    },
    #[error("failed to create asset directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create temporary asset file {path}: {source}")]
    CreateTempFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write asset file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to atomically replace asset file {path}: {source}")]
    RenameFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("provider mismatch: client uses {client:?}, request uses {request:?}")]
    ProviderMismatch {
        client: sekai_asset_pipeline::ProviderKind,
        request: sekai_asset_pipeline::ProviderKind,
    },
    #[error("ColorfulPalette release resolution requires asset_version and asset_hash")]
    MissingReleaseInput,
    #[error("Nuverse version response from {url} was empty or not valid UTF-8")]
    InvalidReleaseResponse { url: String },
    #[error("manifest crypto configuration is missing an AES key or IV")]
    MissingManifestCrypto,
    #[error(transparent)]
    Pipeline(#[from] sekai_asset_pipeline::PipelineError),
}

impl ClientError {
    pub const fn category(&self) -> ClientErrorCategory {
        match self {
            Self::Network { .. } => ClientErrorCategory::TransientNetwork,
            Self::HttpStatus { status, .. }
                if *status >= 500 || *status == 429 || *status == 408 =>
            {
                ClientErrorCategory::TransientNetwork
            }
            Self::HttpStatus { .. } => ClientErrorCategory::PermanentHttp,
            Self::ResponseTooLarge { .. } => ClientErrorCategory::SizeExceeded,
            Self::BundleSizeMismatch { .. } => ClientErrorCategory::SizeMismatch,
            Self::CreateDirectory { .. }
            | Self::CreateTempFile { .. }
            | Self::WriteFile { .. }
            | Self::RenameFile { .. } => ClientErrorCategory::FileWrite,
            Self::BuildClient(_)
            | Self::ProviderMismatch { .. }
            | Self::MissingReleaseInput
            | Self::InvalidReleaseResponse { .. }
            | Self::MissingManifestCrypto => ClientErrorCategory::Configuration,
            Self::Pipeline(_) => ClientErrorCategory::Manifest,
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(self.category(), ClientErrorCategory::TransientNetwork)
    }
}
