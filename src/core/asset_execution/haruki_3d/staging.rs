//! Staging bundles into the work tree, and cleaning up after a failure.

//! The 3D export pipeline: staging, dependency indexes, exporter invocation.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use super::super::model::{AssetExecutionContext, DownloadTask, Haruki3dExportPlan};
use super::super::progress::ExecutionProgressUpdate;
use super::exporter::missing_haruki_3d_bundle_paths;
use super::tasks::bundle_dependency_closure;
use crate::core::download_records::{save_download_record, DownloadRecord};
use crate::core::errors::AssetExecutionError;
use sekai_asset_pipeline::raw_bundle_output_path;

impl AssetExecutionContext {
    pub(crate) fn write_haruki_3d_work_bundle(
        path: &Path,
        deobfuscated: &[u8],
    ) -> Result<(), AssetExecutionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                AssetExecutionError::CreateHaruki3dStagingDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        std::fs::write(path, deobfuscated).map_err(|source| {
            AssetExecutionError::WriteHaruki3dStagingBundle {
                path: path.to_path_buf(),
                source,
            }
        })
    }

    pub(super) fn pending_haruki_3d_tasks(
        tasks: &[DownloadTask],
        downloaded_assets: &DownloadRecord,
        can_reuse: bool,
    ) -> Vec<DownloadTask> {
        tasks
            .iter()
            .filter(|task| {
                !can_reuse
                    || downloaded_assets
                        .get(&task.bundle_path)
                        .is_none_or(|hash| hash != &task.revision)
            })
            .cloned()
            .collect()
    }

    pub(super) fn prepare_haruki_3d_staging(
        &self,
        plan: &Haruki3dExportPlan,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        cancel_flag: &Option<Arc<AtomicBool>>,
    ) -> Result<(), AssetExecutionError> {
        self.verify_pending_haruki_3d_bundles(plan, progress, cancel_flag)?;
        self.create_haruki_3d_sparse_placeholders(plan)?;
        self.update_haruki_3d_sparse_marker(plan)
    }

    pub(super) fn verify_pending_haruki_3d_bundles(
        &self,
        plan: &Haruki3dExportPlan,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        cancel_flag: &Option<Arc<AtomicBool>>,
    ) -> Result<(), AssetExecutionError> {
        for task in &plan.pending_tasks {
            self.ensure_not_cancelled(cancel_flag)?;
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleStarted {
                    bundle: task.bundle_path.clone(),
                },
            );
            let output_path = raw_bundle_output_path(&plan.asset_root, &task.bundle_path)?;
            if !output_path.exists() {
                return Err(AssetExecutionError::MissingHaruki3dStagingBundle {
                    path: output_path,
                });
            }
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleCompleted {
                    bundle: task.bundle_path.clone(),
                },
            );
        }
        Ok(())
    }

    pub(super) fn create_haruki_3d_sparse_placeholders(
        &self,
        plan: &Haruki3dExportPlan,
    ) -> Result<(), AssetExecutionError> {
        for task in &plan.tasks {
            if plan.pending_paths.contains(&task.bundle_path) {
                continue;
            }
            let output_path = raw_bundle_output_path(&plan.asset_root, &task.bundle_path)?;
            if !output_path.exists() {
                Self::write_haruki_3d_work_bundle(&output_path, &[])?;
            }
        }
        Ok(())
    }

    pub(super) fn update_haruki_3d_sparse_marker(
        &self,
        plan: &Haruki3dExportPlan,
    ) -> Result<(), AssetExecutionError> {
        let marker = plan.asset_root.join(".haruki-sparse-input");
        if plan.pending_tasks.len() < plan.tasks.len() {
            return Self::write_haruki_3d_work_bundle(&marker, &[]);
        }
        if marker.exists() {
            std::fs::remove_file(&marker).map_err(|source| {
                AssetExecutionError::RemoveHaruki3dStagingDir {
                    path: marker,
                    source,
                }
            })?;
        }
        Ok(())
    }

    pub(super) fn invalidate_missing_haruki_3d_bundles(
        &self,
        plan: &mut Haruki3dExportPlan,
        stderr: &str,
    ) -> Result<(), AssetExecutionError> {
        let task_paths: HashSet<_> = plan
            .tasks
            .iter()
            .map(|task| task.bundle_path.as_str())
            .collect();
        let recovery_paths = missing_haruki_3d_bundle_paths(stderr)
            .into_iter()
            .filter(|path| task_paths.contains(path.as_str()))
            .flat_map(|path| {
                std::iter::once(path.clone()).chain(bundle_dependency_closure(&plan.info, &path))
            })
            .collect::<HashSet<_>>();
        let removed = recovery_paths
            .iter()
            .filter(|path| plan.downloaded_assets.remove(path.as_str()).is_some())
            .count();
        if removed > 0 {
            save_download_record(&plan.record_path, &plan.downloaded_assets)?;
            tracing::warn!(
                region = %self.region_name,
                removed,
                "invalidated missing sparse 3D bundles for targeted retry"
            );
        }
        Ok(())
    }

    pub(super) fn cleanup_failed_haruki_3d_export(
        &self,
        plan: &Haruki3dExportPlan,
    ) -> Result<(), AssetExecutionError> {
        if plan.config.cleanup_work_dir_after_failure {
            Self::remove_haruki_3d_work_dir(&plan.work_run_dir)?;
        }
        Ok(())
    }

    pub(super) fn remove_haruki_3d_work_dir(
        work_run_dir: &Path,
    ) -> Result<(), AssetExecutionError> {
        if !work_run_dir.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(work_run_dir).map_err(|source| {
            AssetExecutionError::RemoveHaruki3dStagingDir {
                path: work_run_dir.to_path_buf(),
                source,
            }
        })
    }
}
