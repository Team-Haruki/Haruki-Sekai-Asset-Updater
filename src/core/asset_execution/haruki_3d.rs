//! The 3D export pipeline.
//!
//! This file orchestrates a plan; the submodules do the work.

mod exporter;
mod paths;
mod staging;
mod tasks;

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use super::model::{AssetExecutionContext, Haruki3dExportPlan, Haruki3dExportSummary};
use super::progress::ExecutionProgressUpdate;
use crate::core::config::AppConfig;
use crate::core::download_records::{load_download_record, save_download_record};
use crate::core::errors::AssetExecutionError;
use crate::core::models::JobPhase;

impl AssetExecutionContext {
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
        self.fetch_runtime_cookies_if_required().await?;
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

    pub(super) fn finish_haruki_3d_export_plan(
        plan: &Haruki3dExportPlan,
    ) -> Result<(), AssetExecutionError> {
        let completed_record = plan
            .tasks
            .iter()
            .map(|task| (task.bundle_path.clone(), task.revision.clone()))
            .collect();
        save_download_record(&plan.record_path, &completed_record)?;
        if plan.config.cleanup_work_dir_after_success {
            Self::remove_haruki_3d_work_dir(&plan.work_run_dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::pipeline::prepare_asset_run;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::path::Path;

    use tempfile::tempdir;

    use crate::core::config::{AppConfig, RegionProviderConfig};
    use crate::core::download_records::DownloadRecord;
    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::super::model::{AssetExecutionContext, DownloadTask, Haruki3dExportPlan};
    use super::exporter::{exporter_metric_lines, missing_haruki_3d_bundle_paths};
    use super::tasks::bundle_dependency_closure;

    use super::super::test_support::test_region;
    use sekai_asset_pipeline::{
        raw_bundle_output_path, AssetBundleDetail, AssetBundleInfo, AssetCategory, ResolvedBundle,
    };

    #[test]
    fn haruki_3d_work_root_is_disabled_by_default() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(
            &config,
            &prepare_asset_run(&config, &request).unwrap(),
            &request,
        )
        .unwrap();

        assert!(executor.haruki_3d_work_asset_root().is_none());
    }

    #[test]
    fn haruki_3d_export_tasks_include_unrecorded_candidates() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.haruki_3d = crate::core::config::Haruki3dExportConfig {
            enabled: true,
            exporter_path: "/bin/true".to_string(),
            master_dir: "/data/master".to_string(),
            work_dir: temp.path().join("3d-work").to_string_lossy().into_owned(),
            manifest_file: temp
                .path()
                .join("manifest.json")
                .to_string_lossy()
                .into_owned(),
            output_dir: temp.path().join("out").to_string_lossy().into_owned(),
            include: vec!["^live_pv/model/characterv2/".to_string()],
            exclude: Vec::new(),
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("6.0.9".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(
            &config,
            &prepare_asset_run(&config, &request).unwrap(),
            &request,
        )
        .unwrap();
        let matched = "live_pv/model/characterv2/body/01_0001.bundle".to_string();
        let missing_from_record = "live_pv/model/characterv2/body/02_0001.bundle".to_string();
        let dependency = "common/materials/character.bundle".to_string();
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    matched.clone(),
                    AssetBundleDetail {
                        bundle_name: matched.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "new-hash".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: vec![dependency.clone()],
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
                (
                    missing_from_record.clone(),
                    AssetBundleDetail {
                        bundle_name: missing_from_record.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "missing-from-record".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: Vec::new(),
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
                (
                    dependency.clone(),
                    AssetBundleDetail {
                        bundle_name: dependency.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "dependency-hash".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: Vec::new(),
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
            ]),
        };
        let tasks = executor.build_haruki_3d_tasks(&info);

        assert_eq!(tasks.len(), 3);
        assert!(tasks.iter().any(|task| task.bundle_path == matched));
        assert!(tasks
            .iter()
            .any(|task| task.bundle_path == missing_from_record));
        assert!(tasks.iter().any(|task| task.bundle_path == dependency));
        assert_eq!(
            executor.haruki_3d_work_asset_root().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("6.0.9")
                .join("AssetBundles")
        );
        assert_eq!(
            executor.haruki_3d_download_record_path().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("downloaded_assets.json")
        );
        assert_eq!(
            executor.haruki_3d_bundle_hash_index_path().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("bundle_sha256.json")
        );
    }

    #[test]
    fn haruki_3d_background_export_publishes_registry_after_runtime_packages() {
        let config = crate::core::config::Haruki3dExportConfig {
            master_dir: "/master".to_string(),
            output_dir: "/runtime".to_string(),
            manifest_file: "/runtime/manifest.json".to_string(),
            shared_content_store: "/runtime-cas".to_string(),
            compiled_content_store: "/runtime-compiled".to_string(),
            process_concurrency: 16,
            role_character3d_ids: vec![5, 7],
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let commands = AssetExecutionContext::build_haruki_3d_exporter_commands(
            &config,
            Path::new("/work/AssetBundles"),
            Path::new("/work/bundle_sha256.json"),
            Path::new("/work/bundle_dependencies.json"),
        );
        assert_eq!(
            AssetExecutionContext::build_haruki_3d_runtime_catalog_command(&config),
            vec![
                "--emit-runtime-role-catalog",
                "--master",
                "/master",
                "--out",
                "/runtime",
            ]
        );

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0][0], "--emit-part-packages");
        assert_eq!(commands[1][0], "--emit-role-runtimes");
        assert_eq!(commands[2][0], "--emit-costume-registries");
        for command in &commands {
            assert!(
                !command.iter().any(|arg| arg == "--runtime-json-output"),
                "Haruki 3D exporter command should use the exporter's fixed msgpack-br runtime format: {command:?}"
            );
            assert!(
                command
                    .windows(2)
                    .any(|pair| pair == ["--convert-model-textures", "false"]),
                "Haruki 3D exporter command should disable redundant model texture conversion: {command:?}"
            );
        }
        assert!(
            commands[0]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "16"]),
            "part package command should pass haruki_3d.process_concurrency"
        );
        assert!(commands[0]
            .windows(2)
            .any(|pair| pair == ["--shared-content-store", "/runtime-cas"]));
        assert!(commands[0]
            .windows(2)
            .any(|pair| pair == ["--compiled-content-store", "/runtime-compiled"]));
        assert!(commands[0]
            .windows(2)
            .any(|pair| pair == ["--bundle-hash-index", "/work/bundle_sha256.json"]));
        assert!(commands[0].windows(2).any(|pair| pair
            == [
                "--bundle-dependency-index",
                "/work/bundle_dependencies.json"
            ]));
        assert!(
            commands[1]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "16"]),
            "role runtime command should pass haruki_3d.process_concurrency"
        );
        assert_eq!(
            commands[1]
                .iter()
                .filter(|value| value.as_str() == "--role-character3d-id")
                .count(),
            2
        );
        assert!(commands[1].contains(&"5".to_string()));
        assert!(commands[1].contains(&"7".to_string()));
    }

    #[test]
    fn haruki_3d_background_export_runs_role_runtimes_without_role_id_filter() {
        let config = crate::core::config::Haruki3dExportConfig {
            master_dir: "/master".to_string(),
            output_dir: "/runtime".to_string(),
            manifest_file: "/runtime/manifest.json".to_string(),
            process_concurrency: 48,
            role_character3d_ids: Vec::new(),
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let commands = AssetExecutionContext::build_haruki_3d_exporter_commands(
            &config,
            Path::new("/work/AssetBundles"),
            Path::new("/work/bundle_sha256.json"),
            Path::new("/work/bundle_dependencies.json"),
        );

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[1][0], "--emit-role-runtimes");
        assert!(
            commands[1]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "48"]),
            "role runtime command should still pass haruki_3d.process_concurrency"
        );
        assert_eq!(
            commands[1]
                .iter()
                .filter(|value| value.as_str() == "--role-character3d-id")
                .count(),
            0,
            "empty role_character3d_ids should let the exporter choose its default role set"
        );
    }

    #[tokio::test]
    async fn three_d_only_completion_does_not_pollute_standard_download_record() {
        let mut record = DownloadRecord::new();
        let mut completed = 0;
        let mut completed_standard = 0;
        let mut pending_save_count = 0;

        AssetExecutionContext::record_completed_bundle(
            &None,
            "/unused/downloaded_assets.json",
            &mut record,
            &mut completed,
            &mut completed_standard,
            &mut pending_save_count,
            0,
            "jp",
            None,
            None,
            "live_pv/model/characterv2/body/01".to_string(),
            "3d-hash".to_string(),
            false,
        )
        .await;

        assert_eq!(completed, 1);
        assert_eq!(completed_standard, 0);
        assert!(record.is_empty());
    }

    #[test]
    fn bundle_dependency_closure_is_recursive_and_cycle_safe() {
        let detail = |name: &str, dependencies: &[&str]| AssetBundleDetail {
            bundle_name: name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: String::new(),
            category: AssetCategory::OnDemand,
            crc: 0,
            file_size: 0,
            dependencies: dependencies.iter().map(|value| value.to_string()).collect(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: None,
            os: None,
            bundles: HashMap::from([
                ("body".to_string(), detail("body", &["material", "shared"])),
                ("material".to_string(), detail("material", &["texture"])),
                ("texture".to_string(), detail("texture", &["body"])),
                ("shared".to_string(), detail("shared", &[])),
            ]),
        };

        assert_eq!(
            bundle_dependency_closure(&info, "body"),
            vec![
                "material".to_string(),
                "shared".to_string(),
                "texture".to_string()
            ]
        );
    }

    #[test]
    fn exporter_metrics_keep_summary_lines_only() {
        let stdout = b"Started worker\nPart export metrics: built=3, restored=7\nnoise\nPart export parent metrics: totalMs=42\n";
        assert_eq!(
            exporter_metric_lines(stdout),
            "Part export metrics: built=3, restored=7 | Part export parent metrics: totalMs=42"
        );
    }

    #[test]
    fn sparse_recovery_parses_only_safe_missing_bundle_markers() {
        let stderr = "failure\nHARUKI_3D_MISSING_BUNDLE=live_pv/model/body/0001\n\
HARUKI_3D_MISSING_BUNDLE=../escape\nHARUKI_3D_MISSING_BUNDLE=live_pv/model/body/0001\n";
        assert_eq!(
            missing_haruki_3d_bundle_paths(stderr),
            vec!["live_pv/model/body/0001".to_string()]
        );
    }

    #[test]
    fn staging_lifecycle_verifies_placeholders_records_and_cleanup() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.haruki_3d.enabled = true;
        region.export.haruki_3d.cleanup_work_dir_after_failure = true;
        region.export.haruki_3d.cleanup_work_dir_after_success = true;
        let config = AppConfig {
            regions: BTreeMap::from([("jp".to_string(), region.clone())]),
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(
            &config,
            &prepare_asset_run(&config, &request).unwrap(),
            &request,
        )
        .unwrap();
        let task = |path: &str, revision: &str| DownloadTask {
            bundle: ResolvedBundle {
                bundle_path: path.to_string(),
                download_path: path.to_string(),
                revision: revision.to_string(),
                category: AssetCategory::OnDemand,
                file_size: 4,
            },
            priority: 0,
            export_payloads: false,
            stage_haruki_3d: true,
        };
        let body = task("live/body", "body-v2");
        let material = task("common/material", "material-v1");
        let tasks = vec![body.clone(), material.clone()];
        let record_path = temp.path().join("downloaded.json");
        let asset_root = temp.path().join("run/AssetBundles");
        let work_run_dir = temp.path().join("run");
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "live/body".to_string(),
                    AssetBundleDetail {
                        bundle_name: "live/body".to_string(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "body-v2".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 4,
                        dependencies: vec!["common/material".to_string()],
                        paths: vec![],
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
                (
                    "common/material".to_string(),
                    AssetBundleDetail {
                        bundle_name: "common/material".to_string(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "material-v1".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 4,
                        dependencies: vec![],
                        paths: vec![],
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
            ]),
        };
        let mut plan = Haruki3dExportPlan {
            config: region.export.haruki_3d.clone(),
            info,
            tasks: tasks.clone(),
            pending_tasks: vec![body.clone()],
            pending_paths: HashSet::from([body.bundle_path.clone()]),
            downloaded_assets: DownloadRecord::from([
                (body.bundle_path.clone(), "body-v1".to_string()),
                (material.bundle_path.clone(), material.revision.clone()),
            ]),
            record_path: record_path.clone(),
            dependency_index_path: temp.path().join("dependencies.json"),
            asset_root: asset_root.clone(),
            work_run_dir: work_run_dir.clone(),
        };

        assert_eq!(
            AssetExecutionContext::pending_haruki_3d_tasks(&tasks, &plan.downloaded_assets, true)
                .len(),
            1
        );
        assert_eq!(
            AssetExecutionContext::pending_haruki_3d_tasks(&tasks, &plan.downloaded_assets, false)
                .len(),
            2
        );

        let source = temp.path().join("source.bundle");
        std::fs::write(&source, b"body").unwrap();
        let body_path = raw_bundle_output_path(&asset_root, &body.bundle_path).unwrap();
        AssetExecutionContext::copy_haruki_3d_work_bundle(&source, &body_path).unwrap();
        let (progress, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        executor
            .prepare_haruki_3d_staging(&plan, &Some(progress), &None)
            .unwrap();
        assert!(body_path.exists());
        assert!(raw_bundle_output_path(&asset_root, &material.bundle_path)
            .unwrap()
            .exists());
        assert!(asset_root.join(".haruki-sparse-input").exists());
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_ok());

        plan.pending_tasks = tasks.clone();
        plan.pending_paths = tasks.iter().map(|task| task.bundle_path.clone()).collect();
        executor.update_haruki_3d_sparse_marker(&plan).unwrap();
        assert!(!asset_root.join(".haruki-sparse-input").exists());

        plan.downloaded_assets
            .insert(body.bundle_path.clone(), body.revision.clone());
        crate::core::download_records::save_download_record(&record_path, &plan.downloaded_assets)
            .unwrap();
        executor
            .invalidate_missing_haruki_3d_bundles(&mut plan, "HARUKI_3D_MISSING_BUNDLE=live/body")
            .unwrap();
        assert!(!plan.downloaded_assets.contains_key("live/body"));
        assert!(!plan.downloaded_assets.contains_key("common/material"));

        std::fs::create_dir_all(&work_run_dir).unwrap();
        std::fs::write(work_run_dir.join("temporary"), b"x").unwrap();
        executor.cleanup_failed_haruki_3d_export(&plan).unwrap();
        assert!(!work_run_dir.exists());
        AssetExecutionContext::remove_haruki_3d_work_dir(&work_run_dir).unwrap();

        std::fs::create_dir_all(&work_run_dir).unwrap();
        AssetExecutionContext::finish_haruki_3d_export_plan(&plan).unwrap();
        assert!(!work_run_dir.exists());
        let completed = crate::core::download_records::load_download_record(&record_path).unwrap();
        assert_eq!(completed["live/body"], "body-v2");
        assert_eq!(completed["common/material"], "material-v1");
    }
}
