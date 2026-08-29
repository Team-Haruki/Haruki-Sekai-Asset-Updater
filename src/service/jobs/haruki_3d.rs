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
