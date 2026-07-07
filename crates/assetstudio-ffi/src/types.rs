use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) const NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC: u32 = 0x4250_4148;
pub(crate) const NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION: u16 = 2;
pub(crate) const NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN: usize = 20;
#[derive(Debug, Error)]
pub enum AssetStudioFfiError {
    #[error("{message}")]
    AssetStudioFfi { message: String },
    #[error("failed to serialize FFI request: {source}")]
    FfiSerialize { source: sonic_rs::Error },
    #[error("failed to spawn `{program}`: {source}")]
    Spawn { program: String, source: io::Error },
    #[error("command `{program}` failed with status {status}: {stderr}")]
    CommandFailed {
        program: String,
        status: String,
        stderr: String,
    },
    #[error("I/O error at `{}`: {source}", path.display())]
    Io { path: PathBuf, source: io::Error },
}

impl AssetStudioFfiError {
    pub fn message(message: impl Into<String>) -> Self {
        Self::AssetStudioFfi {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextOpenRequest {
    pub input_path: String,
    pub asset_types: Vec<String>,
    pub unity_version: Option<String>,
    pub filter_exclude_mode: bool,
    pub filter_with_regex: bool,
    pub filter_by_name: Option<String>,
    pub filter_by_container: Option<String>,
    pub filter_by_path_ids: Vec<i64>,
    pub load_all_assets: bool,
    pub include_assets: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiAssetInfo {
    pub index: usize,
    pub name: Option<String>,
    pub container: Option<String>,
    #[serde(rename = "type")]
    pub asset_type: Option<String>,
    pub type_id: i32,
    pub path_id: i64,
    #[serde(default)]
    pub unique_id: Option<String>,
    pub size: i64,
    pub source_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextOpenResponse {
    pub success: bool,
    pub context_id: i64,
    pub assets_file_count: usize,
    pub exportable_asset_count: usize,
    pub unity_version: Option<String>,
    #[serde(default)]
    pub assets: Vec<AssetStudioFfiAssetInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    #[serde(default)]
    pub metrics: HashMap<String, u64>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub object_index_count: usize,
    #[serde(default)]
    pub returned_asset_count: usize,
    #[serde(default)]
    pub has_more_assets: bool,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextCloseRequest {
    pub context_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextListObjectsRequest {
    pub context_id: i64,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextListObjectsResponse {
    pub success: bool,
    pub context_id: i64,
    pub offset: usize,
    pub limit: usize,
    pub next_offset: Option<usize>,
    pub total_count: usize,
    #[serde(default)]
    pub returned_count: usize,
    #[serde(default)]
    pub assets: Vec<AssetStudioFfiAssetInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextReadObjectsRequest {
    pub context_id: i64,
    pub objects: Vec<AssetStudioFfiContextReadObjectItemRequest>,
    /// Expected upper bound for the packed payload block in bytes. When it exceeds
    /// the worker's spill threshold, the worker maps a spill file up front and the
    /// native library writes payloads straight into the mapping. 0 (the default for
    /// older callers) keeps the in-memory path.
    #[serde(default)]
    pub payload_capacity_hint: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextReadObjectItemRequest {
    pub path_id: i64,
    pub kind: String,
    pub image_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiObjectReadResponse {
    pub success: bool,
    pub asset: Option<AssetStudioFfiAssetInfo>,
    pub payload_kind: Option<String>,
    pub payload_len: i64,
    pub suggested_extension: Option<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiObjectReadBatchResponse {
    pub success: bool,
    #[serde(default)]
    pub reads: Vec<AssetStudioFfiObjectReadResponse>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    #[serde(default)]
    pub payload_kind_counts: HashMap<String, usize>,
    #[serde(default)]
    pub payload_bytes_by_kind: HashMap<String, u64>,
    pub payload_len: i64,
    #[serde(default)]
    pub object_count: usize,
    #[serde(default)]
    pub payload_bundle_version: u32,
    #[serde(default)]
    pub payload_bundle_entry_count: usize,
    #[serde(default)]
    pub payload_bundle_bytes: i64,
    #[serde(default)]
    pub payload_data_bytes: u64,
    #[serde(default)]
    pub failed_count: usize,
    #[serde(default)]
    pub read_payload_ms: u64,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AssetStudioFfiObjectReadOutput {
    pub response: AssetStudioFfiObjectReadResponse,
    /// Shared slice of the read-batch payload bundle; cloning is a refcount bump.
    pub payload: bytes::Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioFfiContextCloseResponse {
    pub success: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetStudioFfiOperation {
    ContextOpen,
    ContextListObjects,
    ContextClose,
    ContextReadObjects,
}

impl AssetStudioFfiOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContextOpen => "context_open",
            Self::ContextListObjects => "context_list_objects",
            Self::ContextClose => "context_close",
            Self::ContextReadObjects => "context_read_objects",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum AssetStudioFfiRequest {
    ContextOpen(AssetStudioFfiContextOpenRequest),
    ContextListObjects(AssetStudioFfiContextListObjectsRequest),
    ContextClose(AssetStudioFfiContextCloseRequest),
    ContextReadObjects(AssetStudioFfiContextReadObjectsRequest),
}

impl AssetStudioFfiRequest {
    pub fn operation(&self) -> AssetStudioFfiOperation {
        match self {
            Self::ContextOpen(_) => AssetStudioFfiOperation::ContextOpen,
            Self::ContextListObjects(_) => AssetStudioFfiOperation::ContextListObjects,
            Self::ContextClose(_) => AssetStudioFfiOperation::ContextClose,
            Self::ContextReadObjects(_) => AssetStudioFfiOperation::ContextReadObjects,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", content = "response", rename_all = "snake_case")]
pub enum AssetStudioFfiResponse {
    ContextOpen(AssetStudioFfiContextOpenResponse),
    ContextListObjects(AssetStudioFfiContextListObjectsResponse),
    ContextClose(AssetStudioFfiContextCloseResponse),
    ContextReadObjects(AssetStudioFfiObjectReadBatchResponse),
}

impl AssetStudioFfiResponse {
    pub fn into_context_open(
        self,
    ) -> Result<AssetStudioFfiContextOpenResponse, AssetStudioFfiError> {
        match self {
            Self::ContextOpen(response) => Ok(response),
            other => Err(unexpected_native_response("context_open", &other)),
        }
    }

    pub fn into_context_list_objects(
        self,
    ) -> Result<AssetStudioFfiContextListObjectsResponse, AssetStudioFfiError> {
        match self {
            Self::ContextListObjects(response) => Ok(response),
            other => Err(unexpected_native_response("context_list_objects", &other)),
        }
    }

    pub fn into_context_close(
        self,
    ) -> Result<AssetStudioFfiContextCloseResponse, AssetStudioFfiError> {
        match self {
            Self::ContextClose(response) => Ok(response),
            other => Err(unexpected_native_response("context_close", &other)),
        }
    }

    pub fn into_object_read_batch(
        self,
    ) -> Result<AssetStudioFfiObjectReadBatchResponse, AssetStudioFfiError> {
        match self {
            Self::ContextReadObjects(response) => Ok(response),
            other => Err(unexpected_native_response("context_read_objects", &other)),
        }
    }
}

fn unexpected_native_response(
    expected: &'static str,
    response: &AssetStudioFfiResponse,
) -> AssetStudioFfiError {
    AssetStudioFfiError::AssetStudioFfi {
        message: format!("ffi worker returned unexpected response for {expected}: {response:?}"),
    }
}
