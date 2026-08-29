//! The job manager: the public surface, and the locks and limits it owns.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{RwLock, Semaphore};
use tracing::{info, warn};
use uuid::Uuid;

use super::progress::push_progress_event;
use super::runner::{run_planning, PlanningContext};
use super::store::{prune_terminal_jobs, JobListEntry, JobListSummary, RegionLockMap};
use crate::core::config::AppConfig;
use crate::core::errors::RegionError;
use crate::core::models::{
    AssetUpdateRequest, JobFailure, JobFailureKind, JobPhase, JobSnapshot, JobStatus,
};
use crate::core::regions::select_region;

#[derive(Clone)]
pub struct JobManager {
    pub(super) config: Arc<AppConfig>,
    pub(super) jobs: Arc<RwLock<HashMap<Uuid, JobSnapshot>>>,
    pub(super) cancel_flags: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    /// Bounds how many jobs run their heavy pipeline concurrently. `None` = unlimited.
    pub(super) job_semaphore: Option<Arc<Semaphore>>,
    /// Per-region locks serializing execution so concurrent same-region jobs can't clobber each
    /// other's download record (lost updates) or share a temp path mid-export.
    pub(super) region_locks: RegionLockMap,
    pub(super) haruki_3d_active: Arc<RwLock<HashSet<String>>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::test_support::{manager, queued_job};

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
}
