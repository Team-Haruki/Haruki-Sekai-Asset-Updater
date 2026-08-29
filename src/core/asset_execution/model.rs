//! The types the execution path moves around.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::OwnedSemaphorePermit;

use crate::core::config::RegionConfig;
use crate::core::download_records::DownloadRecord;
use crate::core::export_pipeline::UnityAssetBundlePayloadExport;
use crate::core::models::AssetUpdateRequest;

#[cfg(test)]
impl BundleFetchSource {
    pub(super) const fn as_str(&self) -> &'static str {
        match self {
            Self::CacheHit => "cache_hit",
            Self::CacheMiss => "cache_miss",
            Self::Network => "network",
        }
    }
}

#[derive(Debug)]
pub(super) enum BundleFetchSource {
    CacheHit,
    CacheMiss,
    Network,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BundleCacheEntryStatus {
    Current,
    Stale,
    Missing,
}

#[derive(Debug)]
pub(super) struct BundleFetch {
    /// Deobfuscated bytes ready to pass directly to unity-rs.
    pub(super) body: Vec<u8>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) source: BundleFetchSource,
}

pub(super) enum BundleWorkOutput {
    Completed,
    NativePostProcess(Box<NativeBundlePostProcessJob>),
}

pub(super) struct NativeBundlePostProcessJob {
    pub(super) bundle_path: String,
    pub(super) bundle_hash: String,
    pub(super) export_started: Instant,
    pub(super) payload_export: UnityAssetBundlePayloadExport,
    pub(super) backlog_wait_ms: u128,
    pub(super) _backlog_permit: Option<OwnedSemaphorePermit>,
    pub(super) _memory_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, Default)]
pub struct Haruki3dExportSummary {
    pub matched_bundles: usize,
    pub downloaded_bundles: usize,
}

#[derive(Debug, Clone)]
pub struct AssetExecutionContext {
    pub(super) client: reqwest::Client,
    pub(super) region_name: String,
    pub(super) region: RegionConfig,
    pub(super) request: AssetUpdateRequest,
    pub(super) retry: crate::core::config::RetryConfig,
    pub(super) runtime_cookie: Option<String>,
    pub(super) resolved_asset_version: Option<String>,
}

pub(super) struct Haruki3dExportPlan {
    pub(super) config: crate::core::config::Haruki3dExportConfig,
    pub(super) info: AssetBundleInfo,
    pub(super) tasks: Vec<DownloadTask>,
    pub(super) pending_tasks: Vec<DownloadTask>,
    pub(super) pending_paths: HashSet<String>,
    pub(super) downloaded_assets: DownloadRecord,
    pub(super) record_path: PathBuf,
    pub(super) dependency_index_path: PathBuf,
    pub(super) asset_root: PathBuf,
    pub(super) work_run_dir: PathBuf,
}

pub(super) struct BundleWritePlan {
    pub(super) raw_target: Option<PathBuf>,
    pub(super) haruki_3d_target: Option<PathBuf>,
    pub(super) temp_target: Option<PathBuf>,
    pub(super) bundle_hash_index: Option<Arc<std::sync::Mutex<DownloadRecord>>>,
    pub(super) bundle_hash_index_key: String,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadTask {
    pub(super) download_path: String,
    pub(super) bundle_path: String,
    pub(super) bundle_hash: String,
    pub(super) category: AssetCategory,
    pub(super) file_size: i64,
    pub(super) priority: usize,
    pub(super) export_payloads: bool,
    pub(super) stage_haruki_3d: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBundleInfo {
    #[serde(rename = "version")]
    pub version: Option<String>,
    #[serde(rename = "os")]
    pub os: Option<String>,
    #[serde(rename = "bundles")]
    pub bundles: HashMap<String, AssetBundleDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBundleDetail {
    #[serde(rename = "bundleName", deserialize_with = "de_null_as_empty_string")]
    pub bundle_name: String,
    #[serde(rename = "cacheFileName", deserialize_with = "de_null_as_empty_string")]
    pub cache_file_name: String,
    #[serde(
        rename = "cacheDirectoryName",
        deserialize_with = "de_null_as_empty_string"
    )]
    pub cache_directory_name: String,
    // nuverse regions use `crc` instead of `hash`; the server may send nil here.
    #[serde(rename = "hash", deserialize_with = "de_null_as_empty_string")]
    pub hash: String,
    #[serde(rename = "category")]
    pub category: AssetCategory,
    #[serde(rename = "crc")]
    pub crc: i64,
    #[serde(rename = "fileSize")]
    pub file_size: i64,
    #[serde(rename = "dependencies")]
    pub dependencies: Vec<String>,
    #[serde(rename = "paths", default)]
    pub paths: Vec<String>,
    #[serde(rename = "isBuiltin")]
    pub is_builtin: bool,
    #[serde(rename = "isRelocate")]
    pub is_relocate: Option<bool>,
    #[serde(rename = "md5Hash")]
    pub md5_hash: Option<String>,
    #[serde(rename = "downloadPath")]
    pub download_path: Option<String>,
}

/// Deserializes a msgpack/JSON null or missing value as an empty String.
/// Go silently coerces nil → zero value for non-pointer types; this helper
/// mirrors that behavior for String fields.
pub(super) fn de_null_as_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum AssetCategory {
    StartApp,
    OnDemand,
    LivePv,
    Other(String),
}

impl<'de> Deserialize<'de> for AssetCategory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Treat nil/null as Other("") — matches Go's zero-value coercion.
        let raw = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
        Ok(match raw.as_str() {
            "StartApp" | "startApp" => Self::StartApp,
            "OnDemand" | "onDemand" => Self::OnDemand,
            "Live_pv" | "live_pv" | "LivePv" | "livePv" => Self::LivePv,
            other => Self::Other(other.to_string()),
        })
    }
}

pub(super) fn asset_category_name(category: &AssetCategory) -> &'static str {
    match category {
        AssetCategory::StartApp => "StartApp",
        AssetCategory::OnDemand | AssetCategory::LivePv | AssetCategory::Other(_) => "OnDemand",
    }
}
