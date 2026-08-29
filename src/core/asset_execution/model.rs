//! The types the execution path moves around.

use std::collections::HashSet;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::OwnedSemaphorePermit;

use crate::core::config::RegionConfig;
use crate::core::download_records::DownloadRecord;
use crate::core::export_pipeline::UnityAssetBundlePayloadExport;
use crate::core::models::AssetUpdateRequest;

pub use sekai_asset_pipeline::{
    asset_category_name, AssetBundleDetail, AssetBundleInfo, AssetCategory, ResolvedBundle,
};

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
    /// Resolved when the run was prepared, so execution cannot disagree with
    /// the plan the caller was shown.
    pub(super) download_record_file: String,
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
pub(crate) struct DownloadTask {
    pub(crate) bundle: ResolvedBundle,
    pub(crate) priority: usize,
    pub(crate) export_payloads: bool,
    pub(crate) stage_haruki_3d: bool,
}

impl Deref for DownloadTask {
    type Target = ResolvedBundle;

    fn deref(&self) -> &Self::Target {
        &self.bundle
    }
}
