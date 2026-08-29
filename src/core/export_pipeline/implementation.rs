use std::collections::HashMap;
use std::path::Path;

use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::ExportPipelineError;

mod assetstudio;
mod images;
mod limits;
mod media_postprocess;
mod paths;
mod payload;
mod selectors;
mod tasks;
mod types;

use self::assetstudio::run_unity_rs_object_export;
use self::limits::configure_cpu_budget_throttle;
pub(crate) use self::payload::flat_pipeline_enabled;
pub(crate) use self::types::NativeSemanticExportPathRegistry;

pub use self::media_postprocess::post_process_exported_files;
pub use self::types::{
    NativeObjectReadPlanStats, NativeObjectTypeReadStats, NativeSkippedObjectRead,
    PostProcessSummary, UnityAssetBundlePayloadExport,
};

pub fn get_export_group(export_path: &str) -> &'static str {
    if export_path.is_empty() {
        return "container";
    }

    let normalized = export_path
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_lowercase();

    for prefix in [
        "event/center",
        "event/thumbnail",
        "gacha/icon",
        "fix_prefab/mc_new",
        "mysekai/character/",
    ] {
        if normalized.starts_with(prefix) {
            return "containerFull";
        }
    }

    "container"
}

/// Exports one bundle and post-processes it, without publishing.
///
/// No production path uses this -- the service goes through
/// `export_unity_asset_bundle_payloads` and post-processes on its own
/// schedule -- but the tests drive a whole bundle through it.
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
    let path_registry = NativeSemanticExportPathRegistry::default();
    export_unity_asset_bundle_payloads_with_registry(
        app_config,
        region,
        asset_bundle_file,
        export_path,
        output_dir,
        category,
        &path_registry,
    )
    .await
}

pub(crate) async fn export_unity_asset_bundle_payloads_with_registry(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
    path_registry: &NativeSemanticExportPathRegistry,
) -> Result<UnityAssetBundlePayloadExport, ExportPipelineError> {
    configure_cpu_budget_throttle(&app_config.resources, app_config.effective_cpu_budget());
    let exclude_path_prefix = if region.export.by_category {
        "assets/sekai/assetbundle/resources".to_string()
    } else if export_path.starts_with("mysekai") {
        "assets/sekai/assetbundle/resources/ondemand".to_string()
    } else {
        format!(
            "assets/sekai/assetbundle/resources/{}",
            category.to_lowercase()
        )
    };

    let actual_export_path = if region.export.by_category {
        output_dir.join(category.to_lowercase()).join(export_path)
    } else {
        output_dir.join(export_path)
    };
    let mut post_process_export_path = actual_export_path.clone();

    let native_object_summary = run_unity_rs_object_export(
        app_config,
        region,
        asset_bundle_file,
        output_dir,
        export_path,
        &exclude_path_prefix,
        path_registry,
    )
    .await?;
    if region.export.by_category {
        post_process_export_path = output_dir.to_path_buf();
    }

    Ok(UnityAssetBundlePayloadExport {
        export_path: post_process_export_path,
        export_root: output_dir.to_path_buf(),
        native_scoped_post_process: true,
        native_written_files: native_object_summary.written_files,
        native_acb_sources: native_object_summary.acb_sources,
        unity_rs_export_phase_ms: native_object_summary.phase_ms,
        unity_rs_skipped_object_reads: native_object_summary.skipped_object_reads,
        unity_rs_object_read_plan: native_object_summary.object_read_plan,
    })
}

pub(super) fn merge_phase_ms(target: &mut HashMap<String, u64>, source: &HashMap<String, u64>) {
    for (key, value) in source {
        *target.entry(format!("read_object.{key}")).or_default() += *value;
    }
}

pub(super) fn record_max_phase_ms(target: &mut HashMap<String, u64>, phase: &str, value: u64) {
    let current = target.entry(phase.to_string()).or_default();
    *current = (*current).max(value);
}

#[cfg(test)]
mod tests;
