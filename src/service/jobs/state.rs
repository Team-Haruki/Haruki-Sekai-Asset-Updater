//! Every transition a job snapshot can make, and the guards on them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::failure::classify_failure;
use super::progress::push_progress_event;
use crate::core::config::RegionConfig;
use crate::core::models::{
    AssetUpdateRequest, ExecutionSummary, JobFailure, JobFailureKind, JobPhase, JobSnapshot,
    JobStatus,
};
use crate::core::regions::build_url_preview;

pub(super) fn complete_dry_run_job(job: &mut JobSnapshot, id: Uuid) {
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

pub(super) fn mark_job_execution_started(job: &mut JobSnapshot, id: Uuid) {
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

pub(super) async fn complete_job_snapshot(
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

pub(super) fn mark_all_downloads_failed(
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

pub(super) fn mark_job_completed(
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

pub(super) async fn finish_failed(
    jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    id: Uuid,
    message: String,
) {
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

pub(super) async fn finish_cancelled(
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

pub(super) fn job_elapsed_ms(job: &JobSnapshot, now: chrono::DateTime<chrono::Utc>) -> i64 {
    now.signed_duration_since(job.created_at)
        .num_milliseconds()
        .max(0)
}

pub(super) fn is_cancelled(flag: &Option<Arc<AtomicBool>>) -> bool {
    flag.as_ref()
        .is_some_and(|flag| flag.load(Ordering::SeqCst))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::models::ExecutionSummary;

    use super::super::test_support::{queued_job, request};

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
}
