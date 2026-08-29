//! Invoking the external 3D exporter and reading what it reported.

//! The 3D export pipeline: staging, dependency indexes, exporter invocation.

use std::path::Path;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;

use super::super::model::AssetExecutionContext;
use super::super::planning::raw_bundle_output_path;
use super::super::progress::ExecutionProgressUpdate;
use crate::core::errors::AssetExecutionError;
use crate::core::models::JobPhase;

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

impl AssetExecutionContext {
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
}
