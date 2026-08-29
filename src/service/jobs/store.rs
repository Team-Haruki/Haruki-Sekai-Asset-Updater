//! The in-memory job map: listing it, pruning it, and the shapes listing returns.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::Mutex;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::models::{JobSnapshot, JobStatus};

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

pub(super) async fn remove_cancel_flag(
    cancel_flags: &Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    id: Uuid,
) {
    let mut flags = cancel_flags.write().await;
    flags.remove(&id);
}

/// Evict the oldest terminal (Completed/Failed/Cancelled) job snapshots so the in-memory map stays
/// bounded. Non-terminal jobs are always retained. `retain == 0` disables eviction.
pub(super) fn prune_terminal_jobs(jobs: &mut HashMap<Uuid, JobSnapshot>, retain: usize) {
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

/// Per-region execution locks, keyed by lowercased region name.
pub(super) type RegionLockMap = Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>;

pub(super) async fn acquire_region_lock(
    region_locks: &RegionLockMap,
    region: &str,
) -> Arc<Mutex<()>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::test_support::queued_job;

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
}
