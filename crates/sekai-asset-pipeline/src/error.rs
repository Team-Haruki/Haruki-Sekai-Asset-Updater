use thiserror::Error;

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
