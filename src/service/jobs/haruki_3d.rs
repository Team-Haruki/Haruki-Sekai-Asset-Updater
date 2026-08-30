//! The 3D export child job spawned after a successful update.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use super::progress::{progress_consumer, push_progress_event};
use super::state::{finish_cancelled, finish_failed};
use super::store::remove_cancel_flag;
use crate::core::asset_execution::AssetExecutionContext;
use crate::core::config::AppConfig;
use crate::core::errors::AssetExecutionError;
use crate::core::models::{AssetUpdateRequest, JobPhase, JobSnapshot, JobStatus};
use crate::core::pipeline::prepare_asset_run;
use crate::core::regions::select_region;

pub(super) async fn spawn_haruki_3d_child_job(
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
            let prepared = prepare_asset_run(&config, &request)?;
            let _region = prepared.region.clone();
            let executor = AssetExecutionContext::new(&config, &prepared, &request)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use super::super::test_support::request;
    use crate::core::config::RegionConfig;

    async fn wait_for_child(jobs: &Arc<RwLock<HashMap<Uuid, JobSnapshot>>>) -> JobSnapshot {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(job) = jobs
                    .read()
                    .await
                    .values()
                    .next()
                    .filter(|job| {
                        matches!(
                            job.status,
                            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
                        )
                    })
                    .cloned()
                {
                    return job;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    type JobStore = Arc<RwLock<HashMap<Uuid, JobSnapshot>>>;
    type CancelFlagStore = Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>;
    type ActiveRegionStore = Arc<RwLock<HashSet<String>>>;

    fn stores() -> (JobStore, CancelFlagStore, ActiveRegionStore) {
        (
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )
    }

    #[tokio::test]
    async fn child_job_reports_unknown_region_and_removes_its_cancel_flag() {
        let (jobs, flags, active) = stores();
        spawn_haruki_3d_child_job(
            jobs.clone(),
            flags.clone(),
            active,
            Arc::new(AppConfig::default()),
            Uuid::new_v4(),
            request("missing"),
        )
        .await;
        let child = wait_for_child(&jobs).await;
        assert_eq!(child.status, JobStatus::Failed);
        assert_eq!(child.kind, "haruki_3d_export");
        assert!(child.parent_job_id.is_some());
        assert!(!flags.read().await.contains_key(&child.id));
    }

    #[tokio::test]
    async fn child_job_skips_an_already_active_work_tree() {
        let (jobs, flags, active) = stores();
        let mut region = RegionConfig {
            enabled: true,
            ..RegionConfig::default()
        };
        region.export.haruki_3d.enabled = true;
        region.export.haruki_3d.staging_dir = "shared-work".to_string();
        active.write().await.insert("jp:shared-work".to_string());
        let config = AppConfig {
            regions: BTreeMap::from([("jp".to_string(), region)]),
            ..AppConfig::default()
        };
        spawn_haruki_3d_child_job(
            jobs.clone(),
            flags.clone(),
            active.clone(),
            Arc::new(config),
            Uuid::new_v4(),
            request("jp"),
        )
        .await;
        let child = wait_for_child(&jobs).await;
        assert_eq!(child.status, JobStatus::Completed);
        assert!(child.message.contains("already running"));
        assert!(!flags.read().await.contains_key(&child.id));
        assert!(active.read().await.contains("jp:shared-work"));
    }

    #[tokio::test]
    async fn child_job_cleans_active_state_after_pipeline_setup_failure() {
        let (jobs, flags, active) = stores();
        let mut region = RegionConfig {
            enabled: true,
            ..RegionConfig::default()
        };
        region.export.haruki_3d.enabled = true;
        region.export.haruki_3d.staging_dir = "failing-work".to_string();
        let config = AppConfig {
            regions: BTreeMap::from([("jp".to_string(), region)]),
            ..AppConfig::default()
        };
        spawn_haruki_3d_child_job(
            jobs.clone(),
            flags.clone(),
            active.clone(),
            Arc::new(config),
            Uuid::new_v4(),
            request("jp"),
        )
        .await;
        let child = wait_for_child(&jobs).await;
        assert_eq!(child.status, JobStatus::Failed);
        assert!(active.read().await.is_empty());
        assert!(!flags.read().await.contains_key(&child.id));
    }
}
