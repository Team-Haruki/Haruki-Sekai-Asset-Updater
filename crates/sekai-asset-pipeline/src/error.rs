use std::path::PathBuf;

use thiserror::Error;

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
pub enum ExportPipelineError {
    #[error(transparent)]
    Contract(#[from] PipelineError),
    #[error(transparent)]
    Codec(#[from] CodecError),
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
    #[error("media conversion failed: {message}")]
    Media { message: String },
    #[error("unity-rs export failed: {message}")]
    UnityRs { message: String },
    #[error("input {path} is not a recognized Unity asset or container")]
    UnrecognizedUnityInput { path: PathBuf },
    #[error("invalid artifact path {path}: {reason}")]
    InvalidArtifactPath { path: PathBuf, reason: String },
    #[error("failed to serialize Unity export JSON: {source}")]
    JsonSerialize {
        #[source]
        source: sonic_rs::Error,
    },
    #[error("failed to parse Unity export JSON: {source}")]
    JsonParse {
        #[source]
        source: sonic_rs::Error,
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

/// Failures produced by the reusable bundle pipeline boundary.
#[derive(Debug, Error)]
pub enum PipelineError {
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
    #[error("invalid raw bundle path `{bundle}`: {reason}")]
    InvalidBundlePath { bundle: String, reason: String },
}
