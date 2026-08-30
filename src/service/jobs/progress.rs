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

#[cfg(test)]
mod tests {
    use super::*;
    use sekai_asset_pipeline::NativeObjectReadPlanStats;

    use super::super::test_support::queued_job;

    #[tokio::test]
    async fn consumer_applies_every_progress_update_shape() {
        let mut job = queued_job("jp");
        let id = job.id;
        job.status = JobStatus::Running;
        let jobs = Arc::new(RwLock::new(HashMap::from([(id, job)])));
        let (sender, receiver) = mpsc::unbounded_channel();
        let consumer = tokio::spawn(progress_consumer(jobs.clone(), id, receiver));
        let plan = NativeObjectReadPlanStats {
            planned_objects: 4,
            successful_reads: 3,
            skipped_reads: 1,
            batch_count: 2,
            payload_bundle_bytes: 128,
            ..NativeObjectReadPlanStats::default()
        };
        let updates = vec![
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::FetchingAssetInfo,
                message: "fetching".to_string(),
            },
            ExecutionProgressUpdate::DownloadsPlanned { total: 2 },
            ExecutionProgressUpdate::BundleStarted {
                bundle: "a".to_string(),
            },
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: "a".to_string(),
                bytes: 64,
                elapsed_ms: 3,
            },
            ExecutionProgressUpdate::BundleExported {
                bundle: "a".to_string(),
                elapsed_ms: 4,
            },
            ExecutionProgressUpdate::BundleUnityRsExportPhases {
                bundle: "a".to_string(),
                phase_ms: HashMap::from([("read".to_string(), 2), ("export".to_string(), 1)]),
            },
            ExecutionProgressUpdate::BundleUnityRsSkippedObjectReads {
                bundle: "a".to_string(),
                count: 1,
            },
            ExecutionProgressUpdate::BundleUnityRsObjectReadPlan {
                bundle: "a".to_string(),
                plan,
            },
            ExecutionProgressUpdate::SchedulerTelemetry {
                bundle: Some("a".to_string()),
                phase_ms: HashMap::from([("wait".to_string(), 1)]),
            },
            ExecutionProgressUpdate::BundleCompleted {
                bundle: "a".to_string(),
            },
            ExecutionProgressUpdate::BundleFailed {
                bundle: "b".to_string(),
                error: "broken".to_string(),
            },
            ExecutionProgressUpdate::RecordSaved { entries: 2 },
            ExecutionProgressUpdate::ChartHashSyncFinished { performed: false },
            ExecutionProgressUpdate::ChartHashSyncFinished { performed: true },
        ];
        for update in updates {
            sender.send(update).unwrap();
        }
        drop(sender);
        consumer.await.unwrap();

        let jobs = jobs.read().await;
        let job = &jobs[&id];
        assert_eq!(job.progress.total_downloads, 2);
        assert_eq!(job.progress.completed_downloads, 1);
        assert_eq!(job.progress.failed_downloads, 1);
        assert_eq!(job.progress.phase, JobPhase::SyncingChartHashes);
        assert_eq!(job.progress.current_step, "chart hash sync completed");
        assert!(job.progress.recent_events.iter().any(|event| {
            event.message == "unity-rs export phases for `a`: export=1ms, read=2ms"
        }));
    }

    #[tokio::test]
    async fn consumer_ignores_updates_for_cancelled_or_missing_jobs() {
        let mut cancelled = queued_job("en");
        cancelled.status = JobStatus::Cancelled;
        let id = cancelled.id;
        let jobs = Arc::new(RwLock::new(HashMap::from([(id, cancelled)])));
        for target in [id, Uuid::new_v4()] {
            let (sender, receiver) = mpsc::unbounded_channel();
            sender
                .send(ExecutionProgressUpdate::DownloadsPlanned { total: 99 })
                .unwrap();
            drop(sender);
            progress_consumer(jobs.clone(), target, receiver).await;
        }
        assert_eq!(jobs.read().await[&id].progress.total_downloads, 0);
    }

    #[test]
    fn recent_progress_is_bounded_and_phase_formatting_is_stable() {
        let mut job = queued_job("tw");
        for index in 0..25 {
            push_progress_event(&mut job, JobPhase::Planning, format!("event {index}"));
        }
        assert_eq!(job.progress.recent_events.len(), 20);
        assert_eq!(job.progress.recent_events[0].message, "event 5");
        assert_eq!(
            format_unity_rs_export_phases(&HashMap::from([
                ("z".to_string(), 2),
                ("a".to_string(), 1),
            ])),
            "a=1ms, z=2ms"
        );
    }
}
