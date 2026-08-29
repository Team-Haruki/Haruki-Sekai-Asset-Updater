//! Consuming progress updates from a running execution.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::core::asset_execution::ExecutionProgressUpdate;
use crate::core::models::{JobPhase, JobProgressEvent, JobSnapshot, JobStatus};

pub(super) async fn progress_consumer(
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

pub(super) fn format_unity_rs_export_phases(phase_ms: &HashMap<String, u64>) -> String {
    let mut phases: Vec<_> = phase_ms.iter().collect();
    phases.sort_by_key(|(phase, _)| *phase);
    phases
        .into_iter()
        .map(|(phase, elapsed_ms)| format!("{phase}={elapsed_ms}ms"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn push_progress_event(job: &mut JobSnapshot, phase: JobPhase, message: String) {
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
