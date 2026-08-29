use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};
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
use tracing::{debug, warn};

use unity_rs_core::loader::AssetLoadOptions;
use unity_rs_core::mesh::MeshReadLimits;
use unity_rs_core::monobehaviour::{
    read_mono_behaviour_json, MonoBehaviourReadLimits, MONO_BEHAVIOUR_CLASS_ID,
};
use unity_rs_core::shader::SHADER_CLASS_ID;
use unity_rs_core::simple_assets::{
    SimpleAssetReadLimits, AUDIO_CLIP_CLASS_ID, FONT_CLASS_ID, MOVIE_TEXTURE_CLASS_ID,
    VIDEO_CLIP_CLASS_ID,
};
use unity_rs_core::sprite::{SpriteReadLimits, SPRITE_CLASS_ID};
use unity_rs_core::studio::{Studio, StudioObject};
use unity_rs_core::texture::{
    read_texture2d, write_rgba_ir, write_rgba_ir_display_order, TextureReadLimits,
    TEXTURE_2D_CLASS_ID,
};
use unity_rs_core::texture_array::{
    read_texture2d_array, write_texture2d_array_rgba_bundle, TextureArrayReadLimits,
    TEXTURE_2D_ARRAY_CLASS_ID,
};

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

use self::assetstudio::{
    assetstudio_export_type_selector, assetstudio_type_selector_matches,
    normalize_assetstudio_type_name, run_unity_rs_object_export,
};
#[cfg(test)]
use self::assetstudio::{
    assetstudio_object_mode_supported_type, native_image_format_for_asset,
    native_object_read_subchunks, native_read_batch_size_for_assets, native_read_kind_for_asset,
    native_skipped_unsupported_asset, select_native_object_readable_assets,
    sort_native_object_reads_for_failure_isolation,
};
use self::images::{
    convert_native_surrogate_images_to_png, encode_dynamic_image, encode_native_rgba_ir,
    handle_png_conversion, write_encoded_image,
};
#[cfg(test)]
use self::limits::sum_process_tree_cpu_percent;
use self::limits::{
    acquire_cpu_budget_permit, acquire_cpu_budget_permit_blocking,
    acquire_image_memory_permit_blocking, configure_cpu_budget_throttle, CpuBudgetPermit,
};
#[cfg(test)]
use self::media_postprocess::{
    acquire_media_encode_permit, process_usm_file, process_usm_input_with_metrics,
    scoped_upload_files, share_acb_waveforms, should_keep_music_long_hca_track, MediaEncodeKind,
};
use self::paths::{
    assetbundle_typetree_output_path, native_object_output_path, strip_container_prefix,
};
#[cfg(test)]
use self::paths::{assetstudio_fix_file_name, native_object_output_extension};
pub(crate) use self::payload::flat_pipeline_enabled;
use self::payload::{
    decode_image_payload_bytes, image_format_extension, image_output_file_for_format,
    is_playable_mono_typetree, native_rgba_ir_contiguous_pixels, safe_payload_bundle_path,
    write_assetstudio_playable_payloads, write_native_object_payload, NativeRgbaIr,
};
#[cfg(test)]
use self::payload::{
    parse_payload_bundle, parse_payload_bundle_borrowed, playable_container_output_path,
    remove_byte_identical_semantic_duplicates, text_asset_public_bytes_target,
    write_assetstudio_export_manifest_entry, write_native_image_payload_final_files,
    write_native_image_payload_final_files_with_backend, write_native_payload_file,
};
#[cfg(test)]
use self::tasks::usm_segment_key;
use self::tasks::{
    asset_studio_export_type_list, merge_usm_inputs, panic_message,
    post_process_files_by_extension, prepare_usm_processing_inputs, remove_export_file_if_exists,
    run_path_tasks, run_tasks, scan_all_files, UsmProcessingInput,
};
pub(crate) use self::types::NativeSemanticExportPathRegistry;
#[cfg(test)]
use self::types::UNITY_ENGINE_FAST_IMAGE_FORMAT;
use self::types::{
    NativeAssetStudioExportManifestEntry, NativeImageEncodeSettings, NativeObjectExportOptions,
    NativeObjectExportSummary, NativePayloadSignature, NativePlayableExport,
    NativePlayableExportObject, NativeSemanticExportClaim, NativeSemanticExportPathState,
    NativeSemanticPathClaim, UnityAssetInfo, UnityObjectReadOutput, UnityObjectReadResponse,
    ASSETSTUDIO_MANIFEST_APPEND_LOCKS, ASSETSTUDIO_MANIFEST_LOCKS,
    ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS, UNITY_ENGINE_DEFAULT_IMAGE_FORMAT,
    UNITY_ENGINE_IMAGE_SURROGATE_FORMAT, UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC,
    UNITY_ENGINE_PAYLOAD_BUNDLE_V2_HEADER_LEN, UNITY_ENGINE_PAYLOAD_BUNDLE_V2_MAGIC,
    UNITY_ENGINE_PAYLOAD_BUNDLE_V2_VERSION, UNITY_ENGINE_RGBA_IR_HEADER_LEN,
    UNITY_ENGINE_RGBA_IR_MAGIC,
};

pub use self::media_postprocess::post_process_exported_files;
pub use self::types::{
    NativeInMemoryMediaSource, NativeObjectReadPlanStats, NativeObjectTypeReadStats,
    NativeSkippedObjectRead, PostProcessSummary, UnityAssetBundlePayloadExport,
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
        region_name,
        region,
        &payload_export.export_path,
        output_dir,
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
