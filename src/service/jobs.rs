use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::core::asset_execution::{AssetExecutionContext, ExecutionProgressUpdate};
use crate::core::config::AppConfig;
use crate::core::errors::{AssetExecutionError, RegionError};
use crate::core::models::{
    AssetUpdateRequest, JobFailure, JobFailureKind, JobPhase, JobProgressEvent, JobSnapshot,
    JobStatus,
};
use crate::core::pipeline::build_execution_plan;
use crate::core::regions::{build_url_preview, select_region};

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobListEntry {
    pub id: Uuid,
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

#[derive(Clone)]
pub struct JobManager {
    config: Arc<AppConfig>,
    jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    cancel_flags: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
}

impl JobManager {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self {
            config,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            cancel_flags: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn submit(&self, request: AssetUpdateRequest) -> Result<JobSnapshot, RegionError> {
        select_region(&self.config, &request.region)?;

        let mut snapshot = JobSnapshot::new(&request);
        snapshot.message = "job accepted and queued for planning".to_string();

        {
            let mut jobs = self.jobs.write().await;
            jobs.insert(snapshot.id, snapshot.clone());
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
        let cancel_flag = {
            let flags = self.cancel_flags.read().await;
            flags.get(&id).cloned()
        }?;

        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(&id)?;
        match job.status {
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
                Some(Err("job is already in a terminal state".to_string()))
            }
            _ => {
                cancel_flag.store(true, Ordering::SeqCst);
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
        let jobs = self.jobs.clone();
        let config = self.config.clone();
        let cancel_flags = self.cancel_flags.clone();

        tokio::spawn(async move {
            let cancel_flag = {
                let flags = cancel_flags.read().await;
                flags.get(&id).cloned()
            };

            {
                let mut job_map = jobs.write().await;
                if let Some(job) = job_map.get_mut(&id) {
                    if job.status == JobStatus::Cancelled {
                        job.updated_at = chrono::Utc::now();
                        return;
                    }
                    job.status = JobStatus::Planning;
                    job.message = "preparing region-specific execution context".to_string();
                    push_progress_event(
                        job,
                        JobPhase::Planning,
                        "preparing region-specific execution context".to_string(),
                    );
                    job.updated_at = chrono::Utc::now();
                }
            }

            sleep(Duration::from_millis(10)).await;

            if is_cancelled(&cancel_flag) {
                finish_cancelled(
                    &jobs,
                    id,
                    "job cancelled before planning finished".to_string(),
                )
                .await;
                remove_cancel_flag(&cancel_flags, id).await;
                return;
            }

            let planning_message = match build_execution_plan(&config, &request) {
                Ok(plan) => {
                    let cancelled_before_execution = {
                        let mut job_map = jobs.write().await;
                        if let Some(job) = job_map.get_mut(&id) {
                            if job.status == JobStatus::Cancelled {
                                job.updated_at = chrono::Utc::now();
                                true
                            } else {
                                job.preview = Some(plan.url_preview.clone());
                                job.plan = Some(plan.clone());
                                if request.dry_run {
                                    job.status = JobStatus::Completed;
                                    job.message = "dry-run plan completed".to_string();
                                    push_progress_event(
                                        job,
                                        JobPhase::Completed,
                                        "dry-run plan completed".to_string(),
                                    );
                                    info!(job_id = %id, region = %job.region, "dry-run plan completed");
                                } else {
                                    job.status = JobStatus::Running;
                                    job.message = "job planned; starting execution".to_string();
                                    push_progress_event(
                                        job,
                                        JobPhase::PlanningDownloads,
                                        "job planned; starting execution".to_string(),
                                    );
                                    info!(job_id = %id, region = %job.region, "job planned; starting execution");
                                }
                                job.updated_at = chrono::Utc::now();
                                false
                            }
                        } else {
                            false
                        }
                    };
                    if cancelled_before_execution {
                        remove_cancel_flag(&cancel_flags, id).await;
                        return;
                    }
                    if request.dry_run {
                        None
                    } else {
                        let (progress_tx, progress_rx) = mpsc::unbounded_channel();
                        let progress_jobs = jobs.clone();
                        let progress_task =
                            tokio::spawn(progress_consumer(progress_jobs, id, progress_rx));
                        let region = match select_region(&config, &request.region) {
                            Ok(region) => region.clone(),
                            Err(err) => {
                                finish_failed(&jobs, id, err.to_string()).await;
                                return;
                            }
                        };
                        let executor = match AssetExecutionContext::new(
                            &config,
                            &request.region,
                            &region,
                            &request,
                        ) {
                            Ok(executor) => executor,
                            Err(err) => {
                                finish_failed(&jobs, id, err.to_string()).await;
                                return;
                            }
                        };
                        let execution_result = timeout(
                            Duration::from_secs(config.execution.timeout_seconds),
                            executor.execute(
                                &config,
                                Some(progress_tx),
                                cancel_flag.clone(),
                                Some(id.to_string()),
                            ),
                        )
                        .await;
                        let _ = progress_task.await;
                        match execution_result {
                            Ok(Ok(summary)) => {
                                if is_cancelled(&cancel_flag) {
                                    finish_cancelled(&jobs, id, "job cancelled".to_string()).await;
                                    remove_cancel_flag(&cancel_flags, id).await;
                                    return;
                                }
                                let cancelled_after_execution = {
                                    let mut job_map = jobs.write().await;
                                    if let Some(job) = job_map.get_mut(&id) {
                                        if job.status == JobStatus::Cancelled {
                                            job.updated_at = chrono::Utc::now();
                                            true
                                        } else {
                                            job.status = JobStatus::Completed;
                                            job.execution = Some(summary);
                                            job.failure = None;
                                            job.preview =
                                                Some(build_url_preview(&region, &request));
                                            job.message = "job completed".to_string();
                                            push_progress_event(
                                                job,
                                                JobPhase::Completed,
                                                "job completed".to_string(),
                                            );
                                            job.updated_at = chrono::Utc::now();
                                            info!(job_id = %id, region = %job.region, "job completed");
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                };
                                if cancelled_after_execution {
                                    remove_cancel_flag(&cancel_flags, id).await;
                                    return;
                                }
                                None
                            }
                            Ok(Err(AssetExecutionError::Cancelled)) => {
                                finish_cancelled(&jobs, id, "job cancelled".to_string()).await;
                                remove_cancel_flag(&cancel_flags, id).await;
                                return;
                            }
                            Ok(Err(err)) => Some(err.to_string()),
                            Err(_) => Some(format!(
                                "job execution timed out after {} seconds",
                                config.execution.timeout_seconds
                            )),
                        }
                    }
                }
                Err(err) => Some(err.to_string()),
            };

            if let Some(message) = planning_message {
                error!(job_id = %id, error = %message, "job failed");
                let mut job_map = jobs.write().await;
                if let Some(job) = job_map.get_mut(&id) {
                    job.status = JobStatus::Failed;
                    job.message = message.clone();
                    job.failure = Some(classify_failure(&message));
                    job.updated_at = chrono::Utc::now();
                }
            }
            remove_cancel_flag(&cancel_flags, id).await;
        });
    }
}

async fn finish_failed(jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>, id: Uuid, message: String) {
    error!(job_id = %id, error = %message, "job failed");
    let mut job_map = jobs.write().await;
    if let Some(job) = job_map.get_mut(&id) {
        job.status = JobStatus::Failed;
        job.message = message.clone();
        job.failure = Some(classify_failure(&message));
        push_progress_event(job, JobPhase::Failed, message);
        job.updated_at = chrono::Utc::now();
    }
}

async fn finish_cancelled(
    jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
    message: String,
) {
    warn!(job_id = %id, reason = %message, "job cancelled");
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
        job.updated_at = chrono::Utc::now();
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
                    push_progress_event(job, phase, message);
                }
                ExecutionProgressUpdate::DownloadsPlanned { total } => {
                    job.progress.total_downloads = total;
                    push_progress_event(
                        job,
                        JobPhase::PlanningDownloads,
                        format!("planned {total} bundle download(s)"),
                    );
                }
                ExecutionProgressUpdate::BundleStarted { bundle } => {
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("downloading bundle `{bundle}`"),
                    );
                }
                ExecutionProgressUpdate::BundleCompleted { bundle } => {
                    job.progress.completed_downloads += 1;
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("completed bundle `{bundle}`"),
                    );
                }
                ExecutionProgressUpdate::BundleFailed { bundle, error } => {
                    job.progress.failed_downloads += 1;
                    push_progress_event(
                        job,
                        JobPhase::DownloadingBundles,
                        format!("failed bundle `{bundle}`: {error}"),
                    );
                }
                ExecutionProgressUpdate::RecordSaved { entries } => {
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
                    push_progress_event(job, JobPhase::SyncingChartHashes, message);
                }
            }
            job.updated_at = chrono::Utc::now();
        }
    }
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
