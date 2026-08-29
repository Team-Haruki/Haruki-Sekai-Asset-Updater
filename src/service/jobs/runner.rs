//! Planning a request and running it to completion.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::time::{sleep, timeout, Duration};
use uuid::Uuid;

use super::haruki_3d::spawn_haruki_3d_child_job;
use super::progress::{progress_consumer, push_progress_event};
use super::state::{
    complete_dry_run_job, complete_job_snapshot, finish_cancelled, finish_failed, is_cancelled,
    mark_job_execution_started,
};
use super::store::remove_cancel_flag;
use super::store::{acquire_region_lock, RegionLockMap};
use crate::core::asset_execution::{AssetExecutionContext, ExecutionProgressUpdate};
use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::AssetExecutionError;
use crate::core::models::{
    AssetUpdateMode, AssetUpdateRequest, ExecutionPlan, ExecutionSummary, JobPhase, JobSnapshot,
    JobStatus,
};
use crate::core::pipeline::build_execution_plan;
use crate::core::pipeline::prepare_asset_run;

#[derive(Clone)]
pub(super) struct PlanningContext {
    pub(super) jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    pub(super) config: Arc<AppConfig>,
    pub(super) cancel_flags: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    pub(super) job_semaphore: Option<Arc<Semaphore>>,
    pub(super) region_locks: RegionLockMap,
    pub(super) haruki_3d_active: Arc<RwLock<HashSet<String>>>,
}

pub(super) enum PlanningDisposition {
    Stop,
    Execute,
}

pub(super) async fn run_planning(context: PlanningContext, id: Uuid, request: AssetUpdateRequest) {
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

pub(super) async fn begin_planning(
    jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
) -> bool {
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

pub(super) async fn run_planned_job(
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

pub(super) async fn apply_execution_plan(
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

pub(super) async fn execute_planned_job(
    context: &PlanningContext,
    id: Uuid,
    request: &AssetUpdateRequest,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> Result<(), String> {
    let prepared = prepare_asset_run(&context.config, request).map_err(|err| err.to_string())?;
    let region = prepared.region.clone();
    let executor = AssetExecutionContext::new(&context.config, &prepared, request)
        .map_err(|err| err.to_string())?;
    let execution_result =
        run_asset_execution(context, request, executor, cancel_flag.clone(), id).await;
    handle_asset_execution_result(context, id, request, &region, cancel_flag, execution_result)
        .await
}

pub(super) async fn run_asset_execution(
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

pub(super) async fn acquire_job_permit(
    semaphore: &Option<Arc<Semaphore>>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    match semaphore {
        Some(semaphore) => semaphore.clone().acquire_owned().await.ok(),
        None => None,
    }
}

pub(super) async fn run_asset_update(
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

pub(super) async fn handle_asset_execution_result(
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

pub(super) async fn finish_successful_execution(
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
