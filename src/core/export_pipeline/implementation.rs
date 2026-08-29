use std::path::{Path, PathBuf};

use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::ExportPipelineError;

mod options;

use self::options::pipeline_options;

pub(crate) use sekai_asset_pipeline::{flat_pipeline_enabled, NativeSemanticExportPathRegistry};
pub use sekai_asset_pipeline::{
    get_export_group, NativeObjectReadPlanStats, NativeObjectTypeReadStats,
    NativeSkippedObjectRead, PostProcessSummary, UnityAssetBundlePayloadExport,
};

/// Compatibility entry point for the service's full application config.
pub async fn extract_unity_asset_bundle(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
) -> Result<PostProcessSummary, ExportPipelineError> {
    let payload_export = export_unity_asset_bundle_payloads(
        app_config,
        region,
        asset_bundle_file,
        export_path,
        output_dir,
        category,
    )
    .await?;
    let mut summary = post_process_exported_files(
        app_config,
        region,
        &payload_export.export_path,
        payload_export.native_scoped_post_process,
        &payload_export.native_written_files,
        payload_export.native_acb_sources,
    )
    .await?;
    summary.unity_rs_export_phase_ms = payload_export.unity_rs_export_phase_ms;
    summary.unity_rs_skipped_object_reads = payload_export.unity_rs_skipped_object_reads;
    summary.unity_rs_object_read_plan = payload_export.unity_rs_object_read_plan;
    Ok(summary)
}

pub async fn export_unity_asset_bundle_payloads(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
) -> Result<UnityAssetBundlePayloadExport, ExportPipelineError> {
    let options = pipeline_options(app_config, region);
    sekai_asset_pipeline::export_unity_asset_bundle_payloads(
        &options,
        asset_bundle_file,
        export_path,
        output_dir,
        category,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn export_unity_asset_bundle_payloads_with_registry(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
    path_registry: &NativeSemanticExportPathRegistry,
) -> Result<UnityAssetBundlePayloadExport, ExportPipelineError> {
    let options = pipeline_options(app_config, region);
    sekai_asset_pipeline::export_unity_asset_bundle_payloads_with_registry(
        &options,
        asset_bundle_file,
        export_path,
        output_dir,
        category,
        path_registry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn post_process_exported_files(
    app_config: &AppConfig,
    region: &RegionConfig,
    export_path: &Path,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<sekai_asset_pipeline::NativeInMemoryMediaSource>,
) -> Result<PostProcessSummary, ExportPipelineError> {
    let options = pipeline_options(app_config, region);
    let mut summary = sekai_asset_pipeline::post_process_exported_files(
        &options,
        export_path,
        scoped_post_process,
        scoped_files,
        acb_sources,
    )
    .await?;

    if region.upload.enabled {
        summary.publishable_files = if scoped_post_process {
            sekai_asset_pipeline::scoped_upload_files(scoped_files, &summary.generated_files)
        } else {
            sekai_asset_pipeline::scan_all_files(export_path)?
        };
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::post_process_exported_files;
    use crate::core::config::{AppConfig, RegionConfig};

    #[test]
    fn application_upload_setting_controls_publishable_inventory() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("sample.json");
        fs::write(&file, b"{}").unwrap();
        let config = AppConfig::default();
        let mut region = RegionConfig::default();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        region.upload.enabled = true;
        let enabled = runtime
            .block_on(post_process_exported_files(
                &config,
                &region,
                dir.path(),
                false,
                &[],
                Vec::new(),
            ))
            .unwrap();
        assert_eq!(enabled.publishable_files, vec![file]);

        region.upload.enabled = false;
        let disabled = runtime
            .block_on(post_process_exported_files(
                &config,
                &region,
                dir.path(),
                false,
                &[],
                Vec::new(),
            ))
            .unwrap();
        assert!(disabled.publishable_files.is_empty());
    }
}
