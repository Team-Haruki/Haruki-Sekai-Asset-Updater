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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    use super::super::test_support::{queued_job, request};
    use crate::core::config::{CryptoConfig, RegionPathsConfig, RegionProviderConfig};
    use crate::core::models::UrlPreview;

    fn context_with_jobs(jobs: HashMap<Uuid, JobSnapshot>) -> PlanningContext {
        PlanningContext {
            jobs: Arc::new(RwLock::new(jobs)),
            config: Arc::new(AppConfig::default()),
            cancel_flags: Arc::new(RwLock::new(HashMap::new())),
            job_semaphore: None,
            region_locks: Arc::new(RwLock::new(HashMap::new())),
            haruki_3d_active: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    fn executable_context(job: JobSnapshot) -> (PlanningContext, AssetUpdateRequest) {
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template:
                    "http://127.0.0.1/info/{env}/{hash}/{asset_version}/{asset_hash}".to_string(),
                asset_bundle_url_template: "http://127.0.0.1/bundle/{bundle_path}".to_string(),
                profile: "production".to_string(),
                profile_hashes: std::collections::BTreeMap::from([(
                    "production".to_string(),
                    "profile".to_string(),
                )]),
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: CryptoConfig {
                aes_key_hex: Some("00112233445566778899aabbccddeeff".to_string()),
                aes_iv_hex: Some("0102030405060708090a0b0c0d0e0f10".to_string()),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some("assets".to_string()),
                downloaded_asset_record_file: Some("record.json".to_string()),
            },
            ..RegionConfig::default()
        };
        let config = AppConfig {
            regions: std::collections::BTreeMap::from([("jp".to_string(), region)]),
            ..AppConfig::default()
        };
        let mut request = request("jp");
        request.asset_version = Some("1".to_string());
        request.asset_hash = Some("hash".to_string());
        let mut context = context_with_jobs(HashMap::from([(job.id, job)]));
        context.config = Arc::new(config);
        (context, request)
    }

    fn plan(dry_run: bool) -> ExecutionPlan {
        ExecutionPlan {
            region: "jp".to_string(),
            dry_run,
            codec_backend: "test".to_string(),
            url_preview: UrlPreview {
                provider_kind: "test".to_string(),
                asset_info_url: None,
                asset_version_lookup_url: None,
                asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
                notes: vec![],
            },
            download_record_file: "record.json".to_string(),
            upload_targets: vec![],
            chart_hash_sync: None,
            pending_steps: vec![],
        }
    }

    fn summary(completed: usize, failed: usize) -> ExecutionSummary {
        ExecutionSummary {
            discovered_bundles: completed + failed,
            queued_downloads: completed + failed,
            completed_downloads: completed,
            failed_downloads: failed,
            updated_record_entries: completed,
            chart_hash_sync_performed: false,
        }
    }

    #[tokio::test]
    async fn planning_transitions_cover_missing_cancelled_dry_and_execute_jobs() {
        let jobs = Arc::new(RwLock::new(HashMap::new()));
        assert!(!begin_planning(&jobs, Uuid::new_v4()).await);

        let mut cancelled = queued_job("jp");
        cancelled.status = JobStatus::Cancelled;
        let cancelled_id = cancelled.id;
        jobs.write().await.insert(cancelled_id, cancelled);
        assert!(!begin_planning(&jobs, cancelled_id).await);

        let queued = queued_job("jp");
        let queued_id = queued.id;
        jobs.write().await.insert(queued_id, queued);
        assert!(begin_planning(&jobs, queued_id).await);
        assert_eq!(jobs.read().await[&queued_id].status, JobStatus::Planning);

        assert!(matches!(
            apply_execution_plan(&jobs, Uuid::new_v4(), &request("jp"), plan(false)).await,
            PlanningDisposition::Stop
        ));
        assert!(matches!(
            apply_execution_plan(&jobs, cancelled_id, &request("jp"), plan(false)).await,
            PlanningDisposition::Stop
        ));

        let mut dry_request = request("jp");
        dry_request.dry_run = true;
        let dry = queued_job("jp");
        let dry_id = dry.id;
        jobs.write().await.insert(dry_id, dry);
        assert!(matches!(
            apply_execution_plan(&jobs, dry_id, &dry_request, plan(true)).await,
            PlanningDisposition::Stop
        ));
        assert_eq!(jobs.read().await[&dry_id].status, JobStatus::Completed);

        let execute = queued_job("jp");
        let execute_id = execute.id;
        jobs.write().await.insert(execute_id, execute);
        assert!(matches!(
            apply_execution_plan(&jobs, execute_id, &request("jp"), plan(false)).await,
            PlanningDisposition::Execute
        ));
        assert_eq!(jobs.read().await[&execute_id].status, JobStatus::Running);
    }

    #[tokio::test]
    async fn permits_and_execution_results_cover_every_terminal_shape() {
        assert!(acquire_job_permit(&None).await.is_none());
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = acquire_job_permit(&Some(semaphore.clone())).await;
        assert!(permit.is_some());
        drop(permit);
        semaphore.close();
        assert!(acquire_job_permit(&Some(semaphore)).await.is_none());

        let job = queued_job("jp");
        let id = job.id;
        let context = context_with_jobs(HashMap::from([(id, job)]));
        let request = request("jp");
        let region = RegionConfig::default();

        handle_asset_execution_result(
            &context,
            id,
            &request,
            &region,
            None,
            Ok(Err(AssetExecutionError::Cancelled)),
        )
        .await
        .unwrap();
        assert_eq!(context.jobs.read().await[&id].status, JobStatus::Cancelled);

        let error = handle_asset_execution_result(
            &context,
            id,
            &request,
            &region,
            None,
            Ok(Err(AssetExecutionError::BlockingTask("broken".to_string()))),
        )
        .await
        .unwrap_err();
        assert!(error.contains("broken"));

        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .unwrap_err();
        let timeout_error =
            handle_asset_execution_result(&context, id, &request, &region, None, Err(elapsed))
                .await
                .unwrap_err();
        assert!(timeout_error.contains("timed out"));
    }

    #[tokio::test]
    async fn successful_execution_honours_flags_and_updates_the_snapshot() {
        let running = queued_job("jp");
        let id = running.id;
        let context = context_with_jobs(HashMap::from([(id, running)]));
        let jp_request = request("jp");
        let region = RegionConfig::default();
        finish_successful_execution(&context, id, &jp_request, &region, &None, summary(1, 0))
            .await
            .unwrap();
        assert_eq!(context.jobs.read().await[&id].status, JobStatus::Completed);

        let cancelled = queued_job("en");
        let cancelled_id = cancelled.id;
        context.jobs.write().await.insert(cancelled_id, cancelled);
        let flag = Arc::new(AtomicBool::new(true));
        finish_successful_execution(
            &context,
            cancelled_id,
            &request("en"),
            &region,
            &Some(flag.clone()),
            summary(1, 0),
        )
        .await
        .unwrap();
        assert!(flag.load(Ordering::SeqCst));
        assert_eq!(
            context.jobs.read().await[&cancelled_id].status,
            JobStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn planning_entry_cleans_flags_when_the_job_is_missing_or_pre_cancelled() {
        let id = Uuid::new_v4();
        let context = context_with_jobs(HashMap::new());
        context
            .cancel_flags
            .write()
            .await
            .insert(id, Arc::new(AtomicBool::new(false)));
        run_planning(context.clone(), id, request("jp")).await;
        assert!(!context.cancel_flags.read().await.contains_key(&id));

        let mut cancelled = queued_job("jp");
        cancelled.status = JobStatus::Cancelled;
        let cancelled_id = cancelled.id;
        context.jobs.write().await.insert(cancelled_id, cancelled);
        context
            .cancel_flags
            .write()
            .await
            .insert(cancelled_id, Arc::new(AtomicBool::new(true)));
        run_planning(context.clone(), cancelled_id, request("jp")).await;
        assert!(!context
            .cancel_flags
            .read()
            .await
            .contains_key(&cancelled_id));
    }

    #[tokio::test]
    async fn planning_entry_handles_cancellation_and_invalid_requests() {
        let job = queued_job("jp");
        let id = job.id;
        let context = context_with_jobs(HashMap::from([(id, job)]));
        context
            .cancel_flags
            .write()
            .await
            .insert(id, Arc::new(AtomicBool::new(true)));
        run_planning(context.clone(), id, request("jp")).await;
        assert_eq!(context.jobs.read().await[&id].status, JobStatus::Cancelled);
        assert!(!context.cancel_flags.read().await.contains_key(&id));

        let invalid = queued_job("missing");
        let invalid_id = invalid.id;
        let invalid_context = context_with_jobs(HashMap::from([(invalid_id, invalid)]));
        run_planning(invalid_context.clone(), invalid_id, request("missing")).await;
        assert_eq!(
            invalid_context.jobs.read().await[&invalid_id].status,
            JobStatus::Failed
        );
    }

    #[tokio::test]
    async fn planned_dry_run_and_both_execution_modes_use_the_shared_dispatch() {
        let job = queued_job("jp");
        let id = job.id;
        let (context, mut request) = executable_context(job);
        request.dry_run = true;
        run_planned_job(&context, id, &request, None).await.unwrap();
        assert_eq!(context.jobs.read().await[&id].status, JobStatus::Completed);

        let flag = Some(Arc::new(AtomicBool::new(true)));
        for mode in [AssetUpdateMode::Update, AssetUpdateMode::PrefetchRawBundles] {
            let prepared = prepare_asset_run(&context.config, &request).unwrap();
            let executor =
                AssetExecutionContext::new(&context.config, &prepared, &request).unwrap();
            let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
            assert!(matches!(
                run_asset_update(
                    executor,
                    context.config.clone(),
                    mode,
                    progress_tx,
                    flag.clone(),
                )
                .await,
                Err(AssetExecutionError::Cancelled)
            ));
        }
    }
}
