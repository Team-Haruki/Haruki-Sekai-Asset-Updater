use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use image::codecs::jpeg::JpegEncoder;
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use image::codecs::webp::WebPEncoder;
use image::{ExtendedColorType, ImageEncoder, ImageReader};
use serde::Serialize;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, warn};

use haruki_assetstudio_ffi::{
    configured_worker_path, AssetStudioFfiAssetInfo, AssetStudioFfiContextCloseRequest,
    AssetStudioFfiContextListObjectsRequest, AssetStudioFfiContextListObjectsResponse,
    AssetStudioFfiContextOpenRequest, AssetStudioFfiContextOpenResponse,
    AssetStudioFfiContextReadObjectItemRequest, AssetStudioFfiContextReadObjectsRequest,
    AssetStudioFfiError, AssetStudioFfiObjectReadBatchResponse, AssetStudioFfiObjectReadOutput,
    AssetStudioFfiRequest, AssetStudioWorkerPool, NativeBatchPhaseStats, WorkerLease,
    WorkerLeaseStats, WorkerOutput,
};
#[cfg(test)]
use haruki_assetstudio_ffi::{AssetStudioFfiObjectReadResponse, AssetStudioFfiResponse};

use crate::core::cleanup::remove_file_if_exists;
use crate::core::codec;
use crate::core::config::{
    AppConfig, AudioOutputFormat, ImageBackendConfig, ImageOutputFormat, ImagePngCompression,
    MediaBackend, RegionConfig, ResourcesConfig, DEFAULT_ASSET_STUDIO_EXPORT_TYPES,
};
use crate::core::errors::ExportPipelineError;
use crate::core::media::{
    convert_hca_bytes_to_flac_with_backend, convert_hca_bytes_to_mp3_with_backend,
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, convert_wav_bytes_to_flac_with_backend,
    convert_wav_bytes_to_mp3_with_backend, FrameRate,
};
use crate::core::storage::{upload_to_all_storages, StorageUploadOptions};

mod assetstudio;
mod images;
mod limits;
mod media_postprocess;
mod paths;
mod payload;
mod tasks;
mod types;

use self::assetstudio::*;
use self::images::*;
use self::limits::*;
use self::media_postprocess::*;
use self::paths::*;
pub(crate) use self::payload::flush_pending_native_image_writes;
use self::payload::*;
use self::tasks::*;
use self::types::*;

pub use self::media_postprocess::post_process_exported_files;
pub use self::types::{
    NativeInMemoryMediaSource, NativeObjectReadPlanStats, NativeSkippedObjectRead,
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

pub async fn extract_unity_asset_bundle(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
) -> Result<PostProcessSummary, ExportPipelineError> {
    let mut payload_export = export_unity_asset_bundle_payloads(
        app_config,
        region,
        asset_bundle_file,
        export_path,
        output_dir,
        category,
    )
    .await?;
    let image_phase_ms = flush_pending_native_image_writes(
        app_config,
        std::mem::take(&mut payload_export.pending_image_writes),
    )?;
    let mut summary = post_process_exported_files(
        app_config,
        region_name,
        region,
        &payload_export.export_path,
        output_dir,
        payload_export.native_scoped_post_process,
        &payload_export.native_written_files,
        payload_export.native_acb_sources,
    )
    .await?;
    summary.ffi_export_phase_ms = payload_export.ffi_export_phase_ms;
    summary.post_process_phase_ms.extend(image_phase_ms);
    summary.ffi_skipped_object_reads = payload_export.ffi_skipped_object_reads;
    summary.ffi_object_read_plan = payload_export.ffi_object_read_plan;
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

    let native_library_path = configured_path(
        app_config.backends.asset_studio.library_path.as_deref(),
    )
    .ok_or_else(|| ExportPipelineError::AssetStudioFfi {
        message: "backends.asset_studio.library_path is required".to_string(),
    })?;
    let native_object_summary = run_assetstudio_ffi_object_export(
        app_config,
        region,
        asset_bundle_file,
        output_dir,
        export_path,
        &exclude_path_prefix,
        native_library_path,
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
        pending_image_writes: native_object_summary.pending_image_writes,
        ffi_export_phase_ms: native_object_summary.phase_ms,
        ffi_skipped_object_reads: native_object_summary.skipped_object_reads,
        ffi_object_read_plan: native_object_summary.object_read_plan,
    })
}

pub(super) fn merge_phase_ms(target: &mut HashMap<String, u64>, source: &HashMap<String, u64>) {
    for (key, value) in source {
        *target.entry(format!("read_object.{key}")).or_default() += *value;
    }
}

pub(super) fn merge_prefixed_phase_ms(
    target: &mut HashMap<String, u64>,
    prefix: &str,
    source: &HashMap<String, u64>,
) {
    for (key, value) in source {
        *target.entry(format!("{prefix}.{key}")).or_default() += *value;
    }
}

pub(super) fn merge_prefixed_usize_counts(
    target: &mut HashMap<String, u64>,
    prefix: &str,
    source: &HashMap<String, usize>,
) {
    for (key, value) in source {
        *target.entry(format!("{prefix}.{key}")).or_default() += *value as u64;
    }
}

pub(super) fn merge_prefixed_u64_counts(
    target: &mut HashMap<String, u64>,
    prefix: &str,
    source: &HashMap<String, u64>,
) {
    for (key, value) in source {
        *target.entry(format!("{prefix}.{key}")).or_default() += *value;
    }
}

pub(super) fn record_max_phase_ms(target: &mut HashMap<String, u64>, phase: &str, value: u64) {
    let current = target.entry(phase.to_string()).or_default();
    *current = (*current).max(value);
}

pub(super) fn merge_optional_max_phase_ms(
    target: &mut HashMap<String, u64>,
    phase: &str,
    value: Option<u64>,
) {
    if let Some(value) = value {
        record_max_phase_ms(target, phase, value);
    }
}

pub(super) fn parse_assetstudio_ffi_context_open_worker_output(
    output: WorkerOutput,
) -> Result<AssetStudioFfiContextOpenResponse, ExportPipelineError> {
    let response = output.response.into_context_open()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio ffi context open warning");
    }
    if output.status_success && response.success {
        debug!(
            context_id = response.context_id,
            assets = response.assets.len(),
            duration_ms = response.duration_ms,
            phase_ms = ?response.phase_ms,
            "assetstudio ffi context opened"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioFfi {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context open failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

pub(super) fn parse_assetstudio_ffi_context_list_objects_worker_output(
    output: WorkerOutput,
) -> Result<AssetStudioFfiContextListObjectsResponse, ExportPipelineError> {
    let response = output.response.into_context_list_objects()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio ffi context list objects warning");
    }
    if output.status_success && response.success {
        debug!(
            context_id = response.context_id,
            offset = response.offset,
            limit = response.limit,
            returned = response.assets.len(),
            total = response.total_count,
            duration_ms = response.duration_ms,
            "assetstudio ffi context listed objects"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioFfi {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context_list_objects failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

pub(super) fn parse_assetstudio_ffi_context_close_worker_output(
    output: WorkerOutput,
) -> Result<(), ExportPipelineError> {
    let response = output.response.into_context_close()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio ffi context close warning");
    }
    if output.status_success && response.success {
        Ok(())
    } else {
        Err(ExportPipelineError::AssetStudioFfi {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context close failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

pub(super) fn is_native_worker_signal_failure(error: &ExportPipelineError) -> bool {
    matches!(
        error,
        ExportPipelineError::CommandFailed {
            program,
            status,
            ..
        } if program.contains("assetstudio_ffi_worker")
            && (status.contains("signal:") || status.contains("SIGSEGV"))
    )
}

#[cfg(test)]
mod tests;
