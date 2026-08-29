//! Progress updates emitted while a job runs.

use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedSender;

use super::model::AssetExecutionContext;
use crate::core::export_pipeline::NativeObjectReadPlanStats;
use crate::core::models::JobPhase;

#[derive(Debug, Clone)]
pub enum ExecutionProgressUpdate {
    Phase {
        phase: JobPhase,
        message: String,
    },
    DownloadsPlanned {
        total: usize,
    },
    BundleStarted {
        bundle: String,
    },
    BundleDownloaded {
        bundle: String,
        bytes: usize,
        elapsed_ms: u128,
    },
    BundleTempWritten {
        bundle: String,
        elapsed_ms: u128,
    },
    BundleExported {
        bundle: String,
        elapsed_ms: u128,
    },
    BundleUnityRsExportPhases {
        bundle: String,
        phase_ms: HashMap<String, u64>,
    },
    BundleUnityRsSkippedObjectReads {
        bundle: String,
        count: usize,
    },
    BundleUnityRsObjectReadPlan {
        bundle: String,
        plan: NativeObjectReadPlanStats,
    },
    SchedulerTelemetry {
        bundle: Option<String>,
        phase_ms: HashMap<String, u64>,
    },
    BundleCompleted {
        bundle: String,
    },
    BundleFailed {
        bundle: String,
        error: String,
    },
    RecordSaved {
        entries: usize,
    },
    ChartHashSyncFinished {
        performed: bool,
    },
}

impl AssetExecutionContext {
    pub(super) fn send_progress(
        sender: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        update: ExecutionProgressUpdate,
    ) {
        if let Some(sender) = sender {
            let _ = sender.send(update);
        }
    }
}
