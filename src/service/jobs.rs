use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::time::{sleep, timeout, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::core::asset_execution::{AssetExecutionContext, ExecutionProgressUpdate};
use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::{AssetExecutionError, RegionError};
use crate::core::models::{
    AssetUpdateMode, AssetUpdateRequest, ExecutionPlan, ExecutionSummary, JobFailure,
    JobFailureKind, JobPhase, JobProgressEvent, JobSnapshot, JobStatus,
};
use crate::core::pipeline::build_execution_plan;
use crate::core::regions::{build_url_preview, select_region};

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobListEntry {
    pub id: Uuid,
    pub parent_job_id: Option<Uuid>,
    pub kind: String,
    pub region: String,
    pub status: JobStatus,
    pub dry_run: bool,
    pub asset_version: Option<String>,
    pub asset_hash: Option<String>,
    pub message: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct JobListSummary {
    pub total: usize,
    pub queued: Vec<Uuid>,
    pub running: Vec<Uuid>,
    pub completed: Vec<Uuid>,
    pub failed: Vec<Uuid>,
    pub cancelled: Vec<Uuid>,
    pub jobs: Vec<JobListEntry>,
}

/// Per-region execution locks, keyed by lowercased region name.
type RegionLockMap = Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>;

#[derive(Clone)]
struct PlanningContext {
    jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    config: Arc<AppConfig>,
    cancel_flags: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    job_semaphore: Option<Arc<Semaphore>>,
    region_locks: RegionLockMap,
    haruki_3d_active: Arc<RwLock<HashSet<String>>>,
}

enum PlanningDisposition {
    Stop,
    Execute,
}

#[derive(Clone)]
pub struct JobManager {
    config: Arc<AppConfig>,
    jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    cancel_flags: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    /// Bounds how many jobs run their heavy pipeline concurrently. `None` = unlimited.
    job_semaphore: Option<Arc<Semaphore>>,
    /// Per-region locks serializing execution so concurrent same-region jobs can't clobber each
    /// other's download record (lost updates) or share a temp path mid-export.
    region_locks: RegionLockMap,
    haruki_3d_active: Arc<RwLock<HashSet<String>>>,
}

impl JobManager {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let job_semaphore = match config.execution.max_concurrent_jobs {
            0 => None,
            limit => Some(Arc::new(Semaphore::new(limit))),
        };
        Self {
            config,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            cancel_flags: Arc::new(RwLock::new(HashMap::new())),
            job_semaphore,
            region_locks: Arc::new(RwLock::new(HashMap::new())),
            haruki_3d_active: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn submit(&self, request: AssetUpdateRequest) -> Result<JobSnapshot, RegionError> {
        select_region(&self.config, &request.region)?;

        let mut snapshot = JobSnapshot::new(&request);
        snapshot.message = "job accepted and queued for planning".to_string();

        {
            let mut jobs = self.jobs.write().await;
            jobs.insert(snapshot.id, snapshot.clone());
            // Evict the oldest terminal snapshots so the jobs map (and `GET /v2/jobs`) can't grow
            // without bound on a long-running service.
            prune_terminal_jobs(&mut jobs, self.config.execution.retain_terminal_jobs);
        }
        {
            let mut flags = self.cancel_flags.write().await;
            flags.insert(snapshot.id, Arc::new(AtomicBool::new(false)));
        }

        info!(
            job_id = %snapshot.id,
            region = %snapshot.region,
            asset_version = ?snapshot.asset_version,
            asset_hash = ?snapshot.asset_hash,
            dry_run = snapshot.dry_run,
            "job accepted and queued"
        );

        self.spawn_planning(snapshot.id, request);
        Ok(snapshot)
    }

    pub async fn get(&self, id: Uuid) -> Option<JobSnapshot> {
        let jobs = self.jobs.read().await;
        jobs.get(&id).cloned()
    }

    pub async fn list(&self) -> JobListSummary {
        let jobs = self.jobs.read().await;
        let mut entries: Vec<JobListEntry> = jobs
            .values()
            .map(|job| JobListEntry {
                id: job.id,
                parent_job_id: job.parent_job_id,
                kind: job.kind.clone(),
                region: job.region.clone(),
                status: job.status.clone(),
                dry_run: job.dry_run,
                asset_version: job.asset_version.clone(),
                asset_hash: job.asset_hash.clone(),
                message: job.message.clone(),
                created_at: job.created_at,
                updated_at: job.updated_at,
            })
            .collect();
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.created_at));

        let mut summary = JobListSummary::default();
        for entry in &entries {
            match entry.status {
                JobStatus::Queued => summary.queued.push(entry.id),
                JobStatus::Planning | JobStatus::WaitingForPipeline | JobStatus::Running => {
                    summary.running.push(entry.id)
                }
                JobStatus::Completed => summary.completed.push(entry.id),
                JobStatus::Failed => summary.failed.push(entry.id),
                JobStatus::Cancelled => summary.cancelled.push(entry.id),
            }
        }
        summary.total = entries.len();
        summary.jobs = entries;
        summary
    }

    pub async fn cancel(&self, id: Uuid) -> Option<Result<JobSnapshot, String>> {
        // Look up the cancel flag without `?`: terminal jobs have had their flag pruned, but the
        // snapshot still lives in the jobs map. Drive the not-found vs already-terminal decision
        // off the jobs map so a finished job reports "already terminal" instead of a spurious 404.
        let cancel_flag = {
            let flags = self.cancel_flags.read().await;
            flags.get(&id).cloned()
        };

        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(&id)?;
        match job.status {
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
                Some(Err("job is already in a terminal state".to_string()))
            }
            _ => {
                if let Some(cancel_flag) = &cancel_flag {
                    cancel_flag.store(true, Ordering::SeqCst);
                }
                job.status = JobStatus::Cancelled;
                job.message = "cancellation requested".to_string();
                job.failure = Some(JobFailure {
                    kind: JobFailureKind::Cancelled,
                    message: "cancellation requested".to_string(),
                    retryable: false,
                    at: chrono::Utc::now(),
                });
                push_progress_event(
                    job,
                    JobPhase::Cancelled,
                    "cancellation requested".to_string(),
                );
                job.updated_at = chrono::Utc::now();
                warn!(job_id = %id, "cancellation requested");
                Some(Ok(job.clone()))
            }
        }
    }

    fn spawn_planning(&self, id: Uuid, request: AssetUpdateRequest) {
        let context = PlanningContext {
            jobs: self.jobs.clone(),
            config: self.config.clone(),
            cancel_flags: self.cancel_flags.clone(),
            job_semaphore: self.job_semaphore.clone(),
            region_locks: self.region_locks.clone(),
            haruki_3d_active: self.haruki_3d_active.clone(),
        };
        tokio::spawn(run_planning(context, id, request));
    }
}

async fn run_planning(context: PlanningContext, id: Uuid, request: AssetUpdateRequest) {
    let cancel_flag = context.cancel_flag(id).await;
    if !begin_planning(&context.jobs, id).await {
        remove_cancel_flag(&context.cancel_flags, id).await;
        return;
    }
    sleep(Duration::from_millis(10)).await;
    if is_cancelled(&cancel_flag) {
        finish_cancelled(
            &context.jobs,
            id,
            "job cancelled before planning finished".to_string(),
        )
        .await;
        remove_cancel_flag(&context.cancel_flags, id).await;
        return;
    }
    let result = run_planned_job(&context, id, &request, cancel_flag).await;
    if let Err(message) = result {
        finish_failed(&context.jobs, id, message).await;
    }
    remove_cancel_flag(&context.cancel_flags, id).await;
}

impl PlanningContext {
    async fn cancel_flag(&self, id: Uuid) -> Option<Arc<AtomicBool>> {
        self.cancel_flags.read().await.get(&id).cloned()
    }
}

async fn begin_planning(jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>, id: Uuid) -> bool {
    let mut job_map = jobs.write().await;
    let Some(job) = job_map.get_mut(&id) else {
        return false;
    };
    if job.status == JobStatus::Cancelled {
        job.updated_at = chrono::Utc::now();
        return false;
    }
    job.status = JobStatus::Planning;
    job.message = "preparing region-specific execution context".to_string();
    push_progress_event(
        job,
        JobPhase::Planning,
        "preparing region-specific execution context".to_string(),
    );
    job.updated_at = chrono::Utc::now();
    true
}

async fn run_planned_job(
    context: &PlanningContext,
    id: Uuid,
    request: &AssetUpdateRequest,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<(), String> {
    let plan = build_execution_plan(&context.config, request).map_err(|err| err.to_string())?;
    match apply_execution_plan(&context.jobs, id, request, plan).await {
        PlanningDisposition::Stop => Ok(()),
        PlanningDisposition::Execute => {
            execute_planned_job(context, id, request, cancel_flag).await
        }
    }
}

async fn apply_execution_plan(
    jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
    request: &AssetUpdateRequest,
    plan: ExecutionPlan,
) -> PlanningDisposition {
    let mut job_map = jobs.write().await;
    let Some(job) = job_map.get_mut(&id) else {
        return PlanningDisposition::Stop;
    };
    if job.status == JobStatus::Cancelled {
        job.updated_at = chrono::Utc::now();
        return PlanningDisposition::Stop;
    }
    job.preview = Some(plan.url_preview.clone());
    job.plan = Some(plan);
    if request.dry_run {
        complete_dry_run_job(job, id);
        PlanningDisposition::Stop
    } else {
        mark_job_execution_started(job, id);
        PlanningDisposition::Execute
    }
}

fn complete_dry_run_job(job: &mut JobSnapshot, id: Uuid) {
    job.status = JobStatus::Completed;
    job.message = "dry-run plan completed".to_string();
    push_progress_event(
        job,
        JobPhase::Completed,
        "dry-run plan completed".to_string(),
    );
    let now = chrono::Utc::now();
    job.updated_at = now;
    info!(
        job_id = %id,
        region = %job.region,
        elapsed_ms = job_elapsed_ms(job, now),
        completed = job.progress.completed_downloads,
        failed = job.progress.failed_downloads,
        total = job.progress.total_downloads,
        "dry-run plan completed"
    );
}

fn mark_job_execution_started(job: &mut JobSnapshot, id: Uuid) {
    job.status = JobStatus::Running;
    job.message = "job planned; starting execution".to_string();
    push_progress_event(
        job,
        JobPhase::PlanningDownloads,
        "job planned; starting execution".to_string(),
    );
    job.updated_at = chrono::Utc::now();
    info!(job_id = %id, region = %job.region, "job planned; starting execution");
}

async fn execute_planned_job(
    context: &PlanningContext,
    id: Uuid,
    request: &AssetUpdateRequest,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<(), String> {
    let region = select_region(&context.config, &request.region)
        .map_err(|err| err.to_string())?
        .clone();
    let executor = AssetExecutionContext::new(&context.config, &request.region, &region, request)
        .map_err(|err| err.to_string())?;
    let execution_result =
        run_asset_execution(context, request, executor, cancel_flag.clone(), id).await;
    handle_asset_execution_result(context, id, request, &region, cancel_flag, execution_result)
        .await
}

async fn run_asset_execution(
    context: &PlanningContext,
    request: &AssetUpdateRequest,
    executor: AssetExecutionContext,
    cancel_flag: Option<Arc<AtomicBool>>,
    id: Uuid,
) -> Result<Result<ExecutionSummary, AssetExecutionError>, tokio::time::error::Elapsed> {
    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_task = tokio::spawn(progress_consumer(context.jobs.clone(), id, progress_rx));
    let region_lock = acquire_region_lock(&context.region_locks, &request.region).await;
    let _region_guard = region_lock.lock().await;
    let _job_permit = acquire_job_permit(&context.job_semaphore).await;
    let execution = run_asset_update(
        executor,
        context.config.clone(),
        request.mode.clone(),
        progress_tx,
        cancel_flag,
    );
    let result = timeout(
        Duration::from_secs(context.config.execution.timeout_seconds),
        execution,
    )
    .await;
    let _ = progress_task.await;
    result
}

async fn acquire_job_permit(
    semaphore: &Option<Arc<Semaphore>>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    match semaphore {
        Some(semaphore) => semaphore.clone().acquire_owned().await.ok(),
        None => None,
    }
}

async fn run_asset_update(
    executor: AssetExecutionContext,
    config: Arc<AppConfig>,
    mode: AssetUpdateMode,
    progress_tx: mpsc::UnboundedSender<ExecutionProgressUpdate>,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<ExecutionSummary, AssetExecutionError> {
    match mode {
        AssetUpdateMode::Update => {
            executor
                .execute(&config, Some(progress_tx), cancel_flag)
                .await
        }
        AssetUpdateMode::PrefetchRawBundles => {
            executor
                .prefetch_asset_bundles(&config, Some(progress_tx), cancel_flag)
                .await
        }
    }
}

async fn handle_asset_execution_result(
    context: &PlanningContext,
    id: Uuid,
    request: &AssetUpdateRequest,
    region: &RegionConfig,
    cancel_flag: Option<Arc<AtomicBool>>,
    result: Result<Result<ExecutionSummary, AssetExecutionError>, tokio::time::error::Elapsed>,
) -> Result<(), String> {
    match result {
        Ok(Ok(summary)) => {
            finish_successful_execution(context, id, request, region, &cancel_flag, summary).await
        }
        Ok(Err(AssetExecutionError::Cancelled)) => {
            finish_cancelled(&context.jobs, id, "job cancelled".to_string()).await;
            Ok(())
        }
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err(format!(
            "job execution timed out after {} seconds",
            context.config.execution.timeout_seconds
        )),
    }
}

async fn finish_successful_execution(
    context: &PlanningContext,
    id: Uuid,
    request: &AssetUpdateRequest,
    region: &RegionConfig,
    cancel_flag: &Option<Arc<AtomicBool>>,
    summary: ExecutionSummary,
) -> Result<(), String> {
    if is_cancelled(cancel_flag) {
        finish_cancelled(&context.jobs, id, "job cancelled".to_string()).await;
        return Ok(());
    }
    if complete_job_snapshot(&context.jobs, id, request, region, summary).await {
        return Ok(());
    }
    if request.mode == AssetUpdateMode::Update && region.export.haruki_3d.enabled {
        spawn_haruki_3d_child_job(
            context.jobs.clone(),
            context.cancel_flags.clone(),
            context.haruki_3d_active.clone(),
            context.config.clone(),
            id,
            request.clone(),
        )
        .await;
    }
    Ok(())
}

async fn complete_job_snapshot(
    jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
    request: &AssetUpdateRequest,
    region: &RegionConfig,
    summary: ExecutionSummary,
) -> bool {
    let mut job_map = jobs.write().await;
    let Some(job) = job_map.get_mut(&id) else {
        return false;
    };
    if job.status == JobStatus::Cancelled {
        job.updated_at = chrono::Utc::now();
        return true;
    }
    let completed = summary.completed_downloads;
    let failed = summary.failed_downloads;
    let total = summary.queued_downloads;
    job.execution = Some(summary);
    job.preview = Some(build_url_preview(region, request));
    let now = chrono::Utc::now();
    job.updated_at = now;
    if total > 0 && completed == 0 && failed > 0 {
        mark_all_downloads_failed(job, id, now, completed, failed, total);
    } else {
        mark_job_completed(job, id, now, completed, failed, total);
    }
    false
}

fn mark_all_downloads_failed(
    job: &mut JobSnapshot,
    id: Uuid,
    now: chrono::DateTime<chrono::Utc>,
    completed: usize,
    failed: usize,
    total: usize,
) {
    let message = format!("all {failed} bundle download(s) failed");
    job.status = JobStatus::Failed;
    job.failure = Some(classify_failure(&message));
    job.message = message.clone();
    push_progress_event(job, JobPhase::Failed, message);
    error!(
        job_id = %id,
        region = %job.region,
        elapsed_ms = job_elapsed_ms(job, now),
        completed,
        failed,
        total,
        "job failed: all downloads failed"
    );
}

fn mark_job_completed(
    job: &mut JobSnapshot,
    id: Uuid,
    now: chrono::DateTime<chrono::Utc>,
    completed: usize,
    failed: usize,
    total: usize,
) {
    job.status = JobStatus::Completed;
    job.failure = None;
    job.message = "job completed".to_string();
    push_progress_event(job, JobPhase::Completed, "job completed".to_string());
    info!(
        job_id = %id,
        region = %job.region,
        elapsed_ms = job_elapsed_ms(job, now),
        completed,
        failed,
        total,
        "job completed"
    );
}

async fn finish_failed(jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>, id: Uuid, message: String) {
    let mut job_map = jobs.write().await;
    if let Some(job) = job_map.get_mut(&id) {
        // Don't clobber an already-terminal job. In particular a user cancellation that races with
        // an execution timeout/error must stay Cancelled rather than flipping to Failed.
        if matches!(
            job.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        ) {
            job.updated_at = chrono::Utc::now();
            warn!(
                job_id = %id,
                status = ?job.status,
                error = %message,
                "execution error after job reached a terminal state; preserving terminal status"
            );
            return;
        }
        job.status = JobStatus::Failed;
        job.message = message.clone();
        job.failure = Some(classify_failure(&message));
        push_progress_event(job, JobPhase::Failed, message);
        let now = chrono::Utc::now();
        job.updated_at = now;
        error!(
            job_id = %id,
            region = %job.region,
            elapsed_ms = job_elapsed_ms(job, now),
            completed = job.progress.completed_downloads,
            failed = job.progress.failed_downloads,
            total = job.progress.total_downloads,
            error = %job.message,
            "job failed"
        );
    } else {
        error!(job_id = %id, error = %message, "job failed");
    }
}

async fn spawn_haruki_3d_child_job(
    jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    cancel_flags: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    haruki_3d_active: Arc<RwLock<HashSet<String>>>,
    config: Arc<AppConfig>,
    parent_id: Uuid,
    request: AssetUpdateRequest,
) {
    let mut child = JobSnapshot::new(&request);
    child.parent_job_id = Some(parent_id);
    child.kind = "haruki_3d_export".to_string();
    child.message = "Haruki 3D export queued".to_string();
    let child_id = child.id;
    {
        let mut job_map = jobs.write().await;
        job_map.insert(child_id, child);
    }
    {
        let mut flags = cancel_flags.write().await;
        flags.insert(child_id, Arc::new(AtomicBool::new(false)));
    }

    tokio::spawn(async move {
        let active_key = match select_region(&config, &request.region) {
            Ok(region) => format!(
                "{}:{}",
                request.region,
                if region.export.haruki_3d.work_dir.trim().is_empty() {
                    &region.export.haruki_3d.staging_dir
                } else {
                    &region.export.haruki_3d.work_dir
                }
            ),
            Err(err) => {
                finish_failed(&jobs, child_id, err.to_string()).await;
                remove_cancel_flag(&cancel_flags, child_id).await;
                return;
            }
        };
        {
            let mut active = haruki_3d_active.write().await;
            if !active.insert(active_key.clone()) {
                let mut job_map = jobs.write().await;
                if let Some(job) = job_map.get_mut(&child_id) {
                    job.status = JobStatus::Completed;
                    let message =
                        "Haruki 3D export skipped; another export is already running".to_string();
                    job.message = message.clone();
                    push_progress_event(job, JobPhase::Completed, message);
                    job.updated_at = chrono::Utc::now();
                }
                remove_cancel_flag(&cancel_flags, child_id).await;
                return;
            }
        }
        let cancel_flag = {
            let flags = cancel_flags.read().await;
            flags.get(&child_id).cloned()
        };
        {
            let mut job_map = jobs.write().await;
            if let Some(job) = job_map.get_mut(&child_id) {
                job.status = JobStatus::Running;
                job.message = "Haruki 3D export running".to_string();
                push_progress_event(
                    job,
                    JobPhase::PlanningDownloads,
                    "Haruki 3D export running".to_string(),
                );
                job.updated_at = chrono::Utc::now();
            }
        }

        let (progress_tx, progress_rx) = mpsc::unbounded_channel();
        let progress_task = tokio::spawn(progress_consumer(jobs.clone(), child_id, progress_rx));
        let result = async {
            let region = select_region(&config, &request.region)?.clone();
            let executor = AssetExecutionContext::new(&config, &request.region, &region, &request)?;
            executor
                .run_haruki_3d_background_export(&config, Some(progress_tx), cancel_flag.clone())
                .await
        }
        .await;
        let _ = progress_task.await;

        match result {
            Ok(summary) => {
                let mut job_map = jobs.write().await;
                if let Some(job) = job_map.get_mut(&child_id) {
                    if job.status == JobStatus::Cancelled {
                        job.updated_at = chrono::Utc::now();
                    } else {
                        job.status = JobStatus::Completed;
                        let message = format!(
                            "Haruki 3D export completed; matched {} bundle(s), downloaded {} bundle(s)",
                            summary.matched_bundles, summary.downloaded_bundles
                        );
                        job.message = message.clone();
                        push_progress_event(job, JobPhase::Completed, message);
                        job.updated_at = chrono::Utc::now();
                    }
                }
            }
            Err(AssetExecutionError::Cancelled) => {
                finish_cancelled(&jobs, child_id, "Haruki 3D export cancelled".to_string()).await;
            }
            Err(err) => {
                finish_failed(&jobs, child_id, err.to_string()).await;
            }
        }
        {
            let mut active = haruki_3d_active.write().await;
            active.remove(&active_key);
        }
        remove_cancel_flag(&cancel_flags, child_id).await;
    });
}

async fn finish_cancelled(
    jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
    message: String,
) {
    let mut job_map = jobs.write().await;
    if let Some(job) = job_map.get_mut(&id) {
        job.status = JobStatus::Cancelled;
        job.message = message.clone();
        job.failure = Some(JobFailure {
            kind: JobFailureKind::Cancelled,
            message: message.clone(),
            retryable: false,
            at: chrono::Utc::now(),
        });
        push_progress_event(job, JobPhase::Cancelled, message);
        let now = chrono::Utc::now();
        job.updated_at = now;
        warn!(
            job_id = %id,
            region = %job.region,
            elapsed_ms = job_elapsed_ms(job, now),
            completed = job.progress.completed_downloads,
            failed = job.progress.failed_downloads,
            total = job.progress.total_downloads,
            reason = %job.message,
            "job cancelled"
        );
    } else {
        warn!(job_id = %id, reason = %message, "job cancelled");
    }
}

async fn progress_consumer(
    jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
    mut rx: mpsc::UnboundedReceiver<ExecutionProgressUpdate>,
) {
    while let Some(update) = rx.recv().await {
        let mut job_map = jobs.write().await;
        if let Some(job) = job_map.get_mut(&id) {
            if job.status == JobStatus::Cancelled {
                continue;
            }
            match update {
                ExecutionProgressUpdate::Phase { phase, message } => {
                    tracing::debug!(
                        job_id = %id,
                        phase = ?phase,
                        message = %message,
                        "job phase advanced"
                    );
                    push_progress_event(job, phase, message);
                }
                ExecutionProgressUpdate::DownloadsPlanned { total } => {
                    job.progress.total_downloads = total;
                    tracing::info!(
                        job_id = %id,
                        region = %job.region,
                        total,
                        "asset bundle downloads planned"
                    );
                    push_progress_event(
                        job,
                        JobPhase::PlanningDownloads,
                        format!("planned {total} bundle download(s)"),
                    );
                }
                ExecutionProgressUpdate::BundleStarted { bundle } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        "bundle processing started"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("downloading bundle `{bundle}`"),
                    );
                }
                ExecutionProgressUpdate::BundleDownloaded {
                    bundle,
                    bytes,
                    elapsed_ms,
                } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        bytes,
                        elapsed_ms,
                        "bundle downloaded"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("downloaded bundle `{bundle}` ({bytes} bytes) in {elapsed_ms} ms"),
                    );
                }
                ExecutionProgressUpdate::BundleTempWritten { bundle, elapsed_ms } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        elapsed_ms,
                        "bundle temp file written"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("wrote bundle `{bundle}` temp file in {elapsed_ms} ms"),
                    );
                }
                ExecutionProgressUpdate::BundleExported { bundle, elapsed_ms } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        elapsed_ms,
                        "bundle exported"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("exported bundle `{bundle}` in {elapsed_ms} ms"),
                    );
                }
                ExecutionProgressUpdate::BundleUnityRsExportPhases { bundle, phase_ms } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        phases = %format_unity_rs_export_phases(&phase_ms),
                        "unity-rs export phases"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!(
                            "unity-rs export phases for `{bundle}`: {}",
                            format_unity_rs_export_phases(&phase_ms)
                        ),
                    );
                }
                ExecutionProgressUpdate::BundleUnityRsSkippedObjectReads { bundle, count } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        count,
                        "unity-rs skipped object reads"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("unity-rs skipped {count} object read(s) for `{bundle}`"),
                    );
                }
                ExecutionProgressUpdate::BundleUnityRsObjectReadPlan { bundle, plan } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        planned = plan.planned_objects,
                        read = plan.successful_reads,
                        skipped = plan.skipped_reads,
                        batches = plan.batch_count,
                        payload_bytes = plan.payload_bundle_bytes,
                        "unity-rs object read plan"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!(
                            "unity-rs object reads for `{bundle}`: planned={}, read={}, skipped={}, batches={}, payload={} bytes",
                            plan.planned_objects,
                            plan.successful_reads,
                            plan.skipped_reads,
                            plan.batch_count,
                            plan.payload_bundle_bytes
                        ),
                    );
                }
                ExecutionProgressUpdate::SchedulerTelemetry { bundle, phase_ms } => {
                    tracing::debug!(
                        job_id = %id,
                        bundle = bundle.as_deref().unwrap_or(""),
                        phase_ms = ?phase_ms,
                        "asset pipeline scheduler telemetry"
                    );
                }
                ExecutionProgressUpdate::BundleCompleted { bundle } => {
                    job.progress.completed_downloads += 1;
                    tracing::info!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        completed = job.progress.completed_downloads,
                        total = job.progress.total_downloads,
                        "bundle completed"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("completed bundle `{bundle}`"),
                    );
                }
                ExecutionProgressUpdate::BundleFailed { bundle, error } => {
                    job.progress.failed_downloads += 1;
                    tracing::warn!(
                        job_id = %id,
                        region = %job.region,
                        bundle = %bundle,
                        failed = job.progress.failed_downloads,
                        total = job.progress.total_downloads,
                        error = %error,
                        "bundle failed"
                    );
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("failed bundle `{bundle}`: {error}"),
                    );
                }
                ExecutionProgressUpdate::RecordSaved { entries } => {
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        entries,
                        "download record saved"
                    );
                    push_progress_event(
                        job,
                        JobPhase::PersistingState,
                        format!("saved downloaded asset record with {entries} entries"),
                    );
                }
                ExecutionProgressUpdate::ChartHashSyncFinished { performed } => {
                    let message = if performed {
                        "chart hash sync completed".to_string()
                    } else {
                        "chart hash sync skipped".to_string()
                    };
                    tracing::debug!(
                        job_id = %id,
                        region = %job.region,
                        performed,
                        "chart hash sync finished"
                    );
                    push_progress_event(job, JobPhase::SyncingChartHashes, message);
                }
            }
            job.updated_at = chrono::Utc::now();
        }
    }
}

fn format_unity_rs_export_phases(phase_ms: &HashMap<String, u64>) -> String {
    let mut phases: Vec<_> = phase_ms.iter().collect();
    phases.sort_by_key(|(phase, _)| *phase);
    phases
        .into_iter()
        .map(|(phase, elapsed_ms)| format!("{phase}={elapsed_ms}ms"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn job_elapsed_ms(job: &JobSnapshot, now: chrono::DateTime<chrono::Utc>) -> i64 {
    now.signed_duration_since(job.created_at)
        .num_milliseconds()
        .max(0)
}

fn push_progress_event(job: &mut JobSnapshot, phase: JobPhase, message: String) {
    job.progress.phase = phase.clone();
    job.progress.current_step = message.clone();
    job.progress.recent_events.push(JobProgressEvent {
        at: chrono::Utc::now(),
        phase,
        message,
    });
    if job.progress.recent_events.len() > 20 {
        let overflow = job.progress.recent_events.len() - 20;
        job.progress.recent_events.drain(0..overflow);
    }
}

fn classify_failure(message: &str) -> JobFailure {
    let lowered = message.to_lowercase();
    let (kind, retryable) = if lowered.contains("timed out") {
        (JobFailureKind::Timeout, true)
    } else if lowered.contains("cancelled") {
        (JobFailureKind::Cancelled, false)
    } else if lowered.contains("http") || lowered.contains("request") || lowered.contains("status")
    {
        (JobFailureKind::Network, true)
    } else if lowered.contains("decrypt") || lowered.contains("msgpack") || lowered.contains("aes")
    {
        (JobFailureKind::Decode, false)
    } else if lowered.contains("s3 upload")
        || lowered.contains("bucket")
        || lowered.contains("storage")
    {
        (JobFailureKind::Storage, true)
    } else if lowered.contains("git") || lowered.contains("chart hash") {
        (JobFailureKind::GitSync, true)
    } else if lowered.contains("assetstudio")
        || lowered.contains("ffmpeg")
        || lowered.contains("media conversion")
        || lowered.contains("export")
    {
        (JobFailureKind::Export, true)
    } else if lowered.contains("config")
        || lowered.contains("missing")
        || lowered.contains("region")
    {
        (JobFailureKind::Configuration, false)
    } else {
        (JobFailureKind::Internal, false)
    };

    JobFailure {
        kind,
        message: message.to_string(),
        retryable,
        at: chrono::Utc::now(),
    }
}

fn is_cancelled(flag: &Option<Arc<AtomicBool>>) -> bool {
    flag.as_ref()
        .is_some_and(|flag| flag.load(Ordering::SeqCst))
}

async fn remove_cancel_flag(cancel_flags: &Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>, id: Uuid) {
    let mut flags = cancel_flags.write().await;
    flags.remove(&id);
}

async fn acquire_region_lock(region_locks: &RegionLockMap, region: &str) -> Arc<Mutex<()>> {
    let key = region.to_ascii_lowercase();
    {
        let locks = region_locks.read().await;
        if let Some(lock) = locks.get(&key) {
            return lock.clone();
        }
    }
    let mut locks = region_locks.write().await;
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Evict the oldest terminal (Completed/Failed/Cancelled) job snapshots so the in-memory map stays
/// bounded. Non-terminal jobs are always retained. `retain == 0` disables eviction.
fn prune_terminal_jobs(jobs: &mut HashMap<Uuid, JobSnapshot>, retain: usize) {
    if retain == 0 {
        return;
    }
    let mut terminal: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> = jobs
        .iter()
        .filter(|(_, job)| {
            matches!(
                job.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
            )
        })
        .map(|(id, job)| (*id, job.updated_at))
        .collect();
    if terminal.len() <= retain {
        return;
    }
    // Keep the most recently updated `retain`; drop the rest (oldest first).
    terminal.sort_by_key(|(_, updated_at)| std::cmp::Reverse(*updated_at));
    for (id, _) in terminal.into_iter().skip(retain) {
        jobs.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::models::AssetUpdateMode;

    fn queued_job(region: &str) -> JobSnapshot {
        JobSnapshot::new(&request(region))
    }

    fn request(region: &str) -> AssetUpdateRequest {
        AssetUpdateRequest {
            region: region.to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
            mode: AssetUpdateMode::Update,
        }
    }

    fn manager() -> JobManager {
        JobManager::new(Arc::new(AppConfig::default()))
    }

    /// A finished job answers "already terminal", not 404. Its cancel flag is
    /// pruned when it finishes while the snapshot stays in the map, so driving
    /// the decision off the flag would report a job that plainly exists as
    /// missing.
    #[tokio::test]
    async fn cancelling_a_terminal_job_reports_terminal_not_missing() {
        let manager = manager();
        let mut job = queued_job("jp");
        job.status = JobStatus::Completed;
        let id = job.id;
        manager.jobs.write().await.insert(id, job);

        match manager.cancel(id).await {
            Some(Err(message)) => assert!(message.contains("terminal"), "{message}"),
            other => panic!("expected a terminal-state error, got {other:?}"),
        }
        assert!(
            manager.cancel(Uuid::new_v4()).await.is_none(),
            "an unknown job must be absent, not terminal"
        );
    }

    #[tokio::test]
    async fn cancelling_a_running_job_marks_it_and_raises_the_flag() {
        let manager = manager();
        let mut job = queued_job("jp");
        job.status = JobStatus::Running;
        let id = job.id;
        let flag = Arc::new(AtomicBool::new(false));
        manager.jobs.write().await.insert(id, job);
        manager.cancel_flags.write().await.insert(id, flag.clone());

        let cancelled = manager.cancel(id).await.unwrap().unwrap();

        assert_eq!(cancelled.status, JobStatus::Cancelled);
        assert!(flag.load(Ordering::SeqCst), "the worker must see the flag");
        assert_eq!(
            manager.jobs.read().await[&id]
                .failure
                .as_ref()
                .unwrap()
                .kind,
            JobFailureKind::Cancelled
        );
    }

    /// The race this guards: a cancel lands while the pipeline is finishing, so
    /// the execution path arrives with a successful summary for a job the user
    /// has already been told is cancelled. Reporting it completed would be a
    /// lie, and spawning the 3D child job off it would do real work.
    #[tokio::test]
    async fn completion_does_not_resurrect_a_cancelled_job() {
        let jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>> = Arc::new(RwLock::new(HashMap::new()));
        let mut job = queued_job("jp");
        job.status = JobStatus::Cancelled;
        let id = job.id;
        jobs.write().await.insert(id, job);

        let request = request("jp");
        let region = RegionConfig::default();
        let stopped = complete_job_snapshot(
            &jobs,
            id,
            &request,
            &region,
            ExecutionSummary {
                discovered_bundles: 10,
                queued_downloads: 10,
                completed_downloads: 10,
                failed_downloads: 0,
                updated_record_entries: 10,
                chart_hash_sync_performed: false,
            },
        )
        .await;

        assert!(stopped, "a cancelled job must stop the completion path");
        assert_eq!(jobs.read().await[&id].status, JobStatus::Cancelled);
    }

    #[test]
    fn pruning_keeps_running_jobs_and_the_newest_terminal_ones() {
        let mut jobs = HashMap::new();
        let mut ids = Vec::new();
        for index in 0..4 {
            let mut job = queued_job("jp");
            job.status = JobStatus::Completed;
            job.updated_at = chrono::Utc::now() + chrono::Duration::seconds(index);
            ids.push(job.id);
            jobs.insert(job.id, job);
        }
        let mut running = queued_job("jp");
        running.status = JobStatus::Running;
        running.updated_at = chrono::Utc::now() - chrono::Duration::days(1);
        let running_id = running.id;
        jobs.insert(running_id, running);

        prune_terminal_jobs(&mut jobs, 2);

        assert!(
            jobs.contains_key(&running_id),
            "a running job is never evicted, however old"
        );
        assert!(jobs.contains_key(&ids[3]) && jobs.contains_key(&ids[2]));
        assert!(!jobs.contains_key(&ids[0]) && !jobs.contains_key(&ids[1]));

        let before = jobs.len();
        prune_terminal_jobs(&mut jobs, 0);
        assert_eq!(jobs.len(), before, "retain == 0 disables eviction");
    }

    /// The lock is what stops two same-region jobs from clobbering each other's
    /// download record, so it has to be the same lock even when the region
    /// arrives spelled differently.
    #[tokio::test]
    async fn region_locks_are_shared_per_region_and_case_insensitive() {
        let locks: RegionLockMap = Arc::new(RwLock::new(HashMap::new()));

        let jp = acquire_region_lock(&locks, "jp").await;
        let jp_upper = acquire_region_lock(&locks, "JP").await;
        let en = acquire_region_lock(&locks, "en").await;

        assert!(Arc::ptr_eq(&jp, &jp_upper), "`JP` and `jp` are one region");
        assert!(!Arc::ptr_eq(&jp, &en));
        assert_eq!(locks.read().await.len(), 2);
    }

    /// Failures are classified by scanning the message for keywords, in a fixed
    /// priority order. This pins that order, including where it gets the answer
    /// wrong: an S3 upload failure whose OpenDAL source mentions an HTTP status
    /// is reported as `Network`, because the http/request/status arm is tested
    /// before the storage arm. The kinds are what the API reports, so the
    /// misread is visible to callers.
    #[test]
    fn failure_classification_follows_keyword_priority() {
        assert_eq!(
            classify_failure("operation timed out").kind,
            JobFailureKind::Timeout
        );
        assert_eq!(
            classify_failure("storage upload failed for provider `s3`").kind,
            JobFailureKind::Storage
        );
        assert_eq!(
            classify_failure(
                "storage upload failed for provider `s3` file `a.png`: \
                 Unexpected (permanent), response: HTTP status 403"
            )
            .kind,
            JobFailureKind::Network,
            "the same storage failure classifies differently once its source \
             mentions HTTP -- keyword priority, not error type, decides"
        );
        assert_eq!(
            classify_failure("something nobody anticipated").kind,
            JobFailureKind::Internal
        );
        assert!(!classify_failure("something nobody anticipated").retryable);
    }
}
