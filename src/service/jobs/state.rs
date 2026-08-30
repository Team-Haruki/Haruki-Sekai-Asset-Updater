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
    async fn completion_transitions_cover_success_failure_and_missing_jobs() {
        let jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>> = Arc::new(RwLock::new(HashMap::new()));
        let success = queued_job("jp");
        let success_id = success.id;
        let failed = queued_job("en");
        let failed_id = failed.id;
        jobs.write().await.insert(success_id, success);
        jobs.write().await.insert(failed_id, failed);
        let region = RegionConfig::default();

        assert!(
            !complete_job_snapshot(
                &jobs,
                Uuid::new_v4(),
                &request("jp"),
                &region,
                summary(1, 0),
            )
            .await
        );
        assert!(
            !complete_job_snapshot(&jobs, success_id, &request("jp"), &region, summary(1, 0),)
                .await
        );
        assert!(
            !complete_job_snapshot(&jobs, failed_id, &request("en"), &region, summary(0, 2),).await
        );

        let jobs = jobs.read().await;
        assert_eq!(jobs[&success_id].status, JobStatus::Completed);
        assert_eq!(jobs[&failed_id].status, JobStatus::Failed);
        assert!(jobs[&failed_id].failure.is_some());
    }

    #[tokio::test]
    async fn direct_state_helpers_preserve_terminal_jobs_and_classify_failures() {
        let mut dry_run = queued_job("jp");
        let dry_run_id = dry_run.id;
        complete_dry_run_job(&mut dry_run, dry_run_id);
        assert_eq!(dry_run.status, JobStatus::Completed);

        let mut started = queued_job("jp");
        let started_id = started.id;
        mark_job_execution_started(&mut started, started_id);
        assert_eq!(started.status, JobStatus::Running);

        let jobs = Arc::new(RwLock::new(HashMap::from([(started_id, started)])));
        finish_failed(&jobs, started_id, "network timeout".to_string()).await;
        assert_eq!(jobs.read().await[&started_id].status, JobStatus::Failed);
        finish_failed(&jobs, started_id, "later error".to_string()).await;
        assert_eq!(jobs.read().await[&started_id].message, "network timeout");
        finish_failed(&jobs, Uuid::new_v4(), "missing".to_string()).await;

        let cancelled = queued_job("tw");
        let cancelled_id = cancelled.id;
        jobs.write().await.insert(cancelled_id, cancelled);
        finish_cancelled(&jobs, cancelled_id, "user request".to_string()).await;
        let jobs_guard = jobs.read().await;
        assert_eq!(jobs_guard[&cancelled_id].status, JobStatus::Cancelled);
        assert_eq!(
            jobs_guard[&cancelled_id].failure.as_ref().unwrap().kind,
            JobFailureKind::Cancelled
        );
        drop(jobs_guard);
        finish_cancelled(&jobs, Uuid::new_v4(), "missing".to_string()).await;
    }

    #[test]
    fn cancellation_and_elapsed_helpers_handle_all_states() {
        assert!(!is_cancelled(&None));
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!is_cancelled(&Some(flag.clone())));
        flag.store(true, Ordering::SeqCst);
        assert!(is_cancelled(&Some(flag)));

        let mut job = queued_job("jp");
        job.created_at = chrono::Utc::now() + chrono::Duration::seconds(1);
        assert_eq!(job_elapsed_ms(&job, chrono::Utc::now()), 0);
    }
}
