//! The 3D export pipeline: staging, dependency indexes, exporter invocation.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;

use super::model::{
    AssetBundleInfo, AssetExecutionContext, DownloadTask, Haruki3dExportPlan, Haruki3dExportSummary,
};
use super::planning::{download_path_for_region, raw_bundle_output_path};
use super::progress::ExecutionProgressUpdate;
use crate::core::config::{AppConfig, RegionProviderConfig};
use crate::core::download_records::{load_download_record, save_download_record, DownloadRecord};
use crate::core::errors::AssetExecutionError;
use crate::core::models::JobPhase;
use crate::core::regions::{compile_patterns, matches_any};

pub(super) fn missing_haruki_3d_bundle_paths(stderr: &str) -> Vec<String> {
    const PREFIX: &str = "HARUKI_3D_MISSING_BUNDLE=";
    let mut paths: Vec<_> = stderr
        .lines()
        .filter_map(|line| line.trim().strip_prefix(PREFIX))
        .filter(|path| !path.is_empty() && raw_bundle_output_path(Path::new(""), path).is_ok())
        .map(str::to_string)
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

pub(super) fn exporter_metric_lines(stdout: &[u8]) -> String {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter(|line| line.contains(" metrics:") || line.starts_with("Planned "))
        .collect::<Vec<_>>()
        .join(" | ")
}

pub(super) fn bundle_dependency_closure(info: &AssetBundleInfo, bundle_name: &str) -> Vec<String> {
    let mut closure = HashSet::new();
    let mut pending = vec![bundle_name.to_string()];
    while let Some(current) = pending.pop() {
        let Some(detail) = info.bundles.get(&current) else {
            continue;
        };
        for dependency in &detail.dependencies {
            if closure.insert(dependency.clone()) {
                pending.push(dependency.clone());
            }
        }
    }
    closure.remove(bundle_name);
    let mut result: Vec<_> = closure.into_iter().collect();
    result.sort();
    result
}

impl AssetExecutionContext {
    pub(super) fn append_haruki_3d_download_tasks(
        &self,
        mut tasks: Vec<DownloadTask>,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
        can_reuse_download_record: bool,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        tasks.extend(self.build_haruki_3d_download_tasks(
            info,
            downloaded_assets,
            can_reuse_download_record,
        )?);
        tasks.sort_by(|a, b| {
            a.bundle_path
                .cmp(&b.bundle_path)
                .then_with(|| a.priority.cmp(&b.priority))
        });
        tasks.dedup_by(|a, b| {
            if a.bundle_path != b.bundle_path {
                return false;
            }
            b.export_payloads |= a.export_payloads;
            b.stage_haruki_3d |= a.stage_haruki_3d;
            true
        });
        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.bundle_path.cmp(&b.bundle_path))
        });
        Ok(tasks)
    }

    pub(super) fn matches_haruki_3d_filters(&self, bundle_path: &str) -> bool {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled || haruki_3d.include.is_empty() {
            return false;
        }
        let include_patterns = compile_patterns(&haruki_3d.include);
        let exclude_patterns = compile_patterns(&haruki_3d.exclude);
        matches_any(&include_patterns, bundle_path) && !matches_any(&exclude_patterns, bundle_path)
    }

    pub(super) fn haruki_3d_work_asset_root(&self) -> Option<PathBuf> {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled {
            return None;
        }
        let run_id = self
            .resolved_asset_version
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("current")
            .replace(['/', '\\', ':'], "_");
        Some(
            self.haruki_3d_state_root()?
                .join(run_id)
                .join("AssetBundles"),
        )
    }

    pub(super) fn haruki_3d_state_root(&self) -> Option<PathBuf> {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled {
            return None;
        }
        Some(Path::new(&Self::haruki_3d_work_dir(haruki_3d)).join(&self.region_name))
    }

    pub(super) fn haruki_3d_download_record_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("downloaded_assets.json"))
    }

    pub(super) fn haruki_3d_bundle_hash_index_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("bundle_sha256.json"))
    }

    pub(super) fn haruki_3d_bundle_dependency_index_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("bundle_dependencies.json"))
    }

    pub(super) fn haruki_3d_work_dir(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
    ) -> String {
        if !haruki_3d.work_dir.trim().is_empty() {
            haruki_3d.work_dir.clone()
        } else {
            haruki_3d.staging_dir.clone()
        }
    }

    pub(super) fn write_haruki_3d_work_bundle(
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

    pub async fn run_haruki_3d_background_export(
        mut self,
        _app_config: &AppConfig,
        progress: Option<UnboundedSender<ExecutionProgressUpdate>>,
        cancel_flag: Option<Arc<AtomicBool>>,
    ) -> Result<Haruki3dExportSummary, AssetExecutionError> {
        if !self.region.export.haruki_3d.enabled {
            return Ok(Haruki3dExportSummary::default());
        }
        self.ensure_not_cancelled(&cancel_flag)?;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::FetchingAssetInfo,
                message: "fetching asset bundle info for Haruki 3D export".to_string(),
            },
        );
        let Some(mut plan) = self.prepare_haruki_3d_export_plan().await? else {
            return Ok(Haruki3dExportSummary::default());
        };
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::DownloadsPlanned {
                total: plan.pending_tasks.len(),
            },
        );
        self.prepare_haruki_3d_staging(&plan, &progress, &cancel_flag)?;
        self.run_haruki_3d_export_plan(&mut plan, &progress).await?;
        Self::finish_haruki_3d_export_plan(&plan)?;
        Ok(Haruki3dExportSummary {
            matched_bundles: plan.tasks.len(),
            downloaded_bundles: plan.pending_tasks.len(),
        })
    }

    pub(super) async fn prepare_haruki_3d_export_plan(
        &mut self,
    ) -> Result<Option<Haruki3dExportPlan>, AssetExecutionError> {
        if self.requires_cookies() {
            self.fetch_runtime_cookies().await?;
        }
        let info = self.fetch_asset_bundle_info().await?;
        let tasks = self.build_haruki_3d_tasks(&info);
        let dependency_index_path = self.required_haruki_3d_dependency_index_path()?;
        Self::save_haruki_3d_dependency_index(&dependency_index_path, &info, &tasks)?;
        let record_path = self.required_haruki_3d_download_record_path()?;
        let downloaded_assets = load_download_record(&record_path)?;
        let can_reuse = self.can_reuse_haruki_3d_download_record().await;
        let pending_tasks = Self::pending_haruki_3d_tasks(&tasks, &downloaded_assets, can_reuse);
        let pending_paths = pending_tasks
            .iter()
            .map(|task| task.bundle_path.clone())
            .collect();
        let Some(asset_root) = self.haruki_3d_work_asset_root() else {
            return Ok(None);
        };
        let work_run_dir = asset_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| asset_root.clone());
        Ok(Some(Haruki3dExportPlan {
            config: self.region.export.haruki_3d.clone(),
            info,
            tasks,
            pending_tasks,
            pending_paths,
            downloaded_assets,
            record_path,
            dependency_index_path,
            asset_root,
            work_run_dir,
        }))
    }

    pub(super) fn required_haruki_3d_dependency_index_path(
        &self,
    ) -> Result<PathBuf, AssetExecutionError> {
        self.haruki_3d_bundle_dependency_index_path()
            .ok_or_else(|| {
                AssetExecutionError::BlockingTask(
                    "3D bundle dependency index path is unavailable".to_string(),
                )
            })
    }

    pub(super) fn required_haruki_3d_download_record_path(
        &self,
    ) -> Result<PathBuf, AssetExecutionError> {
        self.haruki_3d_download_record_path().ok_or_else(|| {
            AssetExecutionError::BlockingTask("3D download record path is unavailable".to_string())
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
                        .is_none_or(|hash| hash != &task.bundle_hash)
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

    pub(super) async fn run_haruki_3d_export_plan(
        &self,
        plan: &mut Haruki3dExportPlan,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
    ) -> Result<(), AssetExecutionError> {
        let hash_index_path = self.haruki_3d_bundle_hash_index_path().ok_or_else(|| {
            AssetExecutionError::BlockingTask(
                "3D bundle hash index path is unavailable".to_string(),
            )
        })?;
        let commands = Self::build_haruki_3d_exporter_commands(
            &plan.config,
            &plan.asset_root,
            &hash_index_path,
            &plan.dependency_index_path,
        );
        for args in commands {
            if let Err(error) = self
                .run_haruki_3d_exporter_stage(&plan.config, &args, progress)
                .await
            {
                self.handle_haruki_3d_export_failure(plan, &error)?;
                return Err(error);
            }
        }
        let catalog_args = Self::build_haruki_3d_runtime_catalog_command(&plan.config);
        if let Err(error) = self
            .run_haruki_3d_exporter_stage(&plan.config, &catalog_args, progress)
            .await
        {
            self.cleanup_failed_haruki_3d_export(plan)?;
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn handle_haruki_3d_export_failure(
        &self,
        plan: &mut Haruki3dExportPlan,
        error: &AssetExecutionError,
    ) -> Result<(), AssetExecutionError> {
        if let AssetExecutionError::Haruki3dExporterFailed { stderr, .. } = error {
            self.invalidate_missing_haruki_3d_bundles(plan, stderr)?;
        }
        self.cleanup_failed_haruki_3d_export(plan)
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

    pub(super) fn finish_haruki_3d_export_plan(
        plan: &Haruki3dExportPlan,
    ) -> Result<(), AssetExecutionError> {
        let completed_record = plan
            .tasks
            .iter()
            .map(|task| (task.bundle_path.clone(), task.bundle_hash.clone()))
            .collect();
        save_download_record(&plan.record_path, &completed_record)?;
        if plan.config.cleanup_work_dir_after_success {
            Self::remove_haruki_3d_work_dir(&plan.work_run_dir)?;
        }
        Ok(())
    }

    pub(super) async fn run_haruki_3d_exporter_stage(
        &self,
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
        args: &[String],
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
    ) -> Result<(), AssetExecutionError> {
        let stage = args.first().map(String::as_str).unwrap_or("unknown");
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::Exporting3dRuntime,
                message: format!("running Haruki 3D exporter: {stage}"),
            },
        );
        let exporter_started = Instant::now();
        let output = tokio::process::Command::new(&haruki_3d.exporter_path)
            .args(args)
            .output()
            .await
            .map_err(|source| AssetExecutionError::Haruki3dExporterSpawn {
                program: haruki_3d.exporter_path.clone(),
                source,
            })?;
        if !output.status.success() {
            return Err(AssetExecutionError::Haruki3dExporterFailed {
                program: haruki_3d.exporter_path.clone(),
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        tracing::info!(
            region = %self.region_name,
            %stage,
            elapsed_ms = exporter_started.elapsed().as_millis(),
            "Haruki 3D exporter stage completed"
        );
        let metrics = exporter_metric_lines(&output.stdout);
        if !metrics.is_empty() {
            tracing::info!(region = %self.region_name, %metrics, "Haruki 3D exporter metrics");
        }
        tracing::debug!(
            region = %self.region_name,
            stdout = %String::from_utf8_lossy(&output.stdout).trim(),
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "Haruki 3D exporter stage output"
        );
        Ok(())
    }

    pub(super) fn build_haruki_3d_runtime_catalog_command(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
    ) -> Vec<String> {
        vec![
            "--emit-runtime-role-catalog".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
        ]
    }

    pub(super) fn build_haruki_3d_costume_registry_command(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
        asset_root: &Path,
    ) -> Vec<String> {
        vec![
            "--emit-costume-registries".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--asset-root".to_string(),
            asset_root.to_string_lossy().into_owned(),
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
            "--convert-model-textures".to_string(),
            haruki_3d.convert_model_textures.to_string(),
        ]
    }

    pub(super) fn build_haruki_3d_exporter_commands(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
        asset_root: &Path,
        bundle_hash_index: &Path,
        bundle_dependency_index: &Path,
    ) -> Vec<Vec<String>> {
        let asset_root_arg = asset_root.to_string_lossy().to_string();
        let model_texture_args = || {
            vec![
                "--convert-model-textures".to_string(),
                haruki_3d.convert_model_textures.to_string(),
            ]
        };
        let mut part_args: Vec<String> = [
            "--emit-part-packages".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--asset-root".to_string(),
            asset_root_arg.clone(),
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
            "--manifest".to_string(),
            haruki_3d.manifest_file.clone(),
            "--part-package-process-concurrency".to_string(),
            haruki_3d.process_concurrency.to_string(),
        ]
        .into_iter()
        .chain(model_texture_args())
        .collect();
        if !haruki_3d.shared_content_store.trim().is_empty() {
            part_args.push("--shared-content-store".to_string());
            part_args.push(haruki_3d.shared_content_store.clone());
        }
        if !haruki_3d.compiled_content_store.trim().is_empty() {
            part_args.push("--compiled-content-store".to_string());
            part_args.push(haruki_3d.compiled_content_store.clone());
        }
        part_args.push("--bundle-hash-index".to_string());
        part_args.push(bundle_hash_index.to_string_lossy().into_owned());
        part_args.push("--bundle-dependency-index".to_string());
        part_args.push(bundle_dependency_index.to_string_lossy().into_owned());
        let mut role_args = vec![
            "--emit-role-runtimes".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--asset-root".to_string(),
            asset_root_arg,
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
        ];
        role_args.push("--part-package-process-concurrency".to_string());
        role_args.push(haruki_3d.process_concurrency.to_string());
        for id in &haruki_3d.role_character3d_ids {
            role_args.push("--role-character3d-id".to_string());
            role_args.push(id.to_string());
        }
        role_args.extend(model_texture_args());
        vec![
            part_args,
            role_args,
            Self::build_haruki_3d_costume_registry_command(haruki_3d, asset_root),
        ]
    }

    pub(super) fn build_haruki_3d_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        self.build_haruki_3d_filter_tasks(info)
    }

    pub(super) fn build_haruki_3d_download_tasks(
        &self,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
        can_reuse_download_record: bool,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        if self.haruki_3d_work_asset_root().is_none() {
            return Ok(Vec::new());
        }
        let mut tasks = Vec::new();
        for task in self.build_haruki_3d_filter_tasks(info) {
            let has_current_record = can_reuse_download_record
                && downloaded_assets
                    .get(&task.bundle_path)
                    .is_some_and(|existing| existing == &task.bundle_hash);
            if !has_current_record {
                tasks.push(DownloadTask {
                    stage_haruki_3d: true,
                    ..task
                });
            }
        }
        Ok(tasks)
    }

    pub(super) async fn can_reuse_haruki_3d_download_record(&self) -> bool {
        let manifest_file = self.region.export.haruki_3d.manifest_file.clone();
        tokio::task::spawn_blocking(move || {
            let Ok(bytes) = std::fs::read(manifest_file) else {
                return false;
            };
            sonic_rs::from_slice::<HashMap<String, sonic_rs::Value>>(&bytes)
                .is_ok_and(|entries| !entries.is_empty())
        })
        .await
        .unwrap_or(false)
    }

    pub(super) fn build_haruki_3d_filter_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        let mut selected: HashSet<String> = info
            .bundles
            .keys()
            .filter(|bundle_name| self.matches_haruki_3d_filters(bundle_name))
            .cloned()
            .collect();
        let mut pending: Vec<String> = selected.iter().cloned().collect();
        while let Some(bundle_name) = pending.pop() {
            let Some(detail) = info.bundles.get(&bundle_name) else {
                continue;
            };
            for dependency in &detail.dependencies {
                if info.bundles.contains_key(dependency) && selected.insert(dependency.clone()) {
                    pending.push(dependency.clone());
                }
            }
        }
        let mut tasks = Vec::new();
        for (bundle_name, detail) in &info.bundles {
            if !selected.contains(bundle_name) {
                continue;
            }
            let bundle_hash = match self.region.provider {
                RegionProviderConfig::Nuverse { .. } => detail.crc.to_string(),
                RegionProviderConfig::ColorfulPalette { .. } => detail.hash.clone(),
            };
            tasks.push(DownloadTask {
                download_path: download_path_for_region(&self.region.provider, bundle_name, detail),
                bundle_path: bundle_name.clone(),
                bundle_hash,
                category: detail.category.clone(),
                file_size: detail.file_size,
                priority: usize::MAX,
                export_payloads: false,
                stage_haruki_3d: true,
            });
        }
        tasks.sort_by(|a, b| a.bundle_path.cmp(&b.bundle_path));
        tasks
    }

    pub(super) fn save_haruki_3d_dependency_index(
        path: &Path,
        info: &AssetBundleInfo,
        tasks: &[DownloadTask],
    ) -> Result<(), AssetExecutionError> {
        let index: HashMap<String, Vec<String>> = tasks
            .iter()
            .map(|task| {
                (
                    task.bundle_path.clone(),
                    bundle_dependency_closure(info, &task.bundle_path),
                )
            })
            .collect();
        let bytes = sonic_rs::to_vec_pretty(&index)
            .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?;
        Self::write_haruki_3d_work_bundle(path, &bytes)
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
