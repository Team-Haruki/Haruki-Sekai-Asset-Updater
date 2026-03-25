use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetUpdateRequest {
    pub region: String,
    pub asset_version: Option<String>,
    pub asset_hash: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlPreview {
    pub provider_kind: String,
    pub asset_info_url: Option<String>,
    pub asset_version_lookup_url: Option<String>,
    pub asset_bundle_url_template: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Planning,
    WaitingForPipeline,
    Running,
    Cancelled,
    Failed,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobPhase {
    Accepted,
    Planning,
    FetchingAssetInfo,
    PlanningDownloads,
    DownloadingBundles,
    PersistingState,
    SyncingChartHashes,
    Cancelled,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobFailureKind {
    Validation,
    Configuration,
    Network,
    Decode,
    Export,
    Storage,
    GitSync,
    Timeout,
    Cancelled,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFailure {
    pub kind: JobFailureKind,
    pub message: String,
    pub retryable: bool,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgressEvent {
    pub at: DateTime<Utc>,
    pub phase: JobPhase,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgressSnapshot {
    pub phase: JobPhase,
    pub current_step: String,
    pub total_downloads: usize,
    pub completed_downloads: usize,
    pub failed_downloads: usize,
    pub recent_events: Vec<JobProgressEvent>,
}

impl Default for JobProgressSnapshot {
    fn default() -> Self {
        Self {
            phase: JobPhase::Accepted,
            current_step: "job accepted".to_string(),
            total_downloads: 0,
            completed_downloads: 0,
            failed_downloads: 0,
            recent_events: vec![JobProgressEvent {
                at: Utc::now(),
                phase: JobPhase::Accepted,
                message: "job accepted".to_string(),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTargetPlan {
    pub provider_kind: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub base_url: String,
    pub public_read: bool,
    pub path_style: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartHashSyncPlan {
    pub repository_dir: String,
    pub output_file: String,
    pub branch_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub region: String,
    pub dry_run: bool,
    pub codec_backend: String,
    pub url_preview: UrlPreview,
    pub download_record_file: String,
    pub upload_targets: Vec<StorageTargetPlan>,
    pub chart_hash_sync: Option<ChartHashSyncPlan>,
    pub pending_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSummary {
    pub discovered_bundles: usize,
    pub queued_downloads: usize,
    pub completed_downloads: usize,
    pub failed_downloads: usize,
    pub updated_record_entries: usize,
    pub chart_hash_sync_performed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub id: Uuid,
    pub region: String,
    pub asset_version: Option<String>,
    pub asset_hash: Option<String>,
    pub dry_run: bool,
    pub status: JobStatus,
    pub message: String,
    pub preview: Option<UrlPreview>,
    pub plan: Option<ExecutionPlan>,
    pub execution: Option<ExecutionSummary>,
    pub failure: Option<JobFailure>,
    pub progress: JobProgressSnapshot,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl JobSnapshot {
    pub fn new(request: &AssetUpdateRequest) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            region: request.region.clone(),
            asset_version: request.asset_version.clone(),
            asset_hash: request.asset_hash.clone(),
            dry_run: request.dry_run,
            status: JobStatus::Queued,
            message: "job accepted".to_string(),
            preview: None,
            plan: None,
            execution: None,
            failure: None,
            progress: JobProgressSnapshot::default(),
            created_at: now,
            updated_at: now,
        }
    }
}
