//! Direct object loading and export through `unity-rs-core`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::path::Path;
use std::time::Instant;

use super::limits::acquire_cpu_budget_permit;
use tracing::warn;
use unity_rs_core::file_type::{detect_file_type, FileType, HEADER_SCAN_LENGTH};
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
use unity_rs_core::texture::{read_texture2d, TextureReadLimits, TEXTURE_2D_CLASS_ID};
use unity_rs_core::texture_array::{
    read_texture2d_array, write_texture2d_array_rgba_bundle, TextureArrayReadLimits,
    TEXTURE_2D_ARRAY_CLASS_ID,
};

use crate::{
    ExportPipelineError, PipelineOptions as AppConfig, PipelineRegionOptions as RegionConfig,
};

use super::merge_phase_ms;
use super::payload::playable::{is_playable_mono_typetree, write_assetstudio_playable_payloads};
use super::payload::write_native_object_payload;
use super::selectors::{
    asset_studio_export_type_list, assetstudio_type_selector_matches,
    normalize_assetstudio_type_name,
};
use super::types::{
    DecodedRgbaSurface, NativeImageEncodeSettings, NativeObjectExportOptions,
    NativeObjectExportSummary, NativeObjectPayload, NativeObjectReadPlanStats,
    NativeObjectTypeReadStats, NativeSemanticExportPathRegistry, NativeSemanticExportPathState,
    NativeSkippedObjectRead, UnityAssetInfo, UnityObjectReadOutput, UnityObjectReadResponse,
    UNITY_ENGINE_DEFAULT_IMAGE_FORMAT,
};

pub(super) async fn run_unity_rs_object_export(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    path_registry: &NativeSemanticExportPathRegistry,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    let cpu_slot = acquire_cpu_budget_permit(app_config.effective_cpu_budget()).await?;
    let cpu_wait_ms = cpu_slot.wait_ms;
    let input_path = asset_bundle_file.to_path_buf();
    let output_dir = output_dir.to_path_buf();
    let export_path = export_path.to_string();
    let strip_path_prefix = strip_path_prefix.to_string();
    let region = region.clone();
    let read_kinds = app_config.backends.asset_studio.read_kinds.clone();
    let image_format = app_config
        .backends
        .asset_studio
        .image_format
        .clone()
        .unwrap_or_else(|| UNITY_ENGINE_DEFAULT_IMAGE_FORMAT.to_string());
    let read_batch_size = app_config.backends.asset_studio.read_batch_size;
    let path_registry = path_registry.clone();
    // This whole closure already runs under one CPU-budget permit, so taking a
    // second one per image would deadlock against itself.
    let image_encode = NativeImageEncodeSettings {
        backend: app_config.backends.image.clone(),
        cpu_budget: None,
        memory_limit_bytes: app_config.resources.memory.max_in_flight_bundle_bytes,
    };

    tokio::task::spawn_blocking(move || {
        let _cpu_permit = cpu_slot.permit;
        let options = NativeObjectExportOptions {
            output_dir: &output_dir,
            export_path: &export_path,
            strip_path_prefix: &strip_path_prefix,
            region: &region,
            read_kinds: &read_kinds,
            image_format: &image_format,
            read_batch_size,
            image_encode: &image_encode,
        };
        let mut summary = call_unity_rs_object_export(&input_path, &options, &path_registry)?;
        summary
            .phase_ms
            .insert("cpu_budget.wait".to_string(), cpu_wait_ms);
        Ok(summary)
    })
    .await
    .map_err(|source| ExportPipelineError::WorkerPanic {
        worker: "unity-rs direct export".to_string(),
        message: source.to_string(),
    })?
}

fn call_unity_rs_object_export(
    input_path: &Path,
    options: &NativeObjectExportOptions<'_>,
    path_registry: &NativeSemanticExportPathRegistry,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    let open_started = Instant::now();
    validate_unity_input(input_path)?;
    let unity_version_override = (!options.region.runtime.unity_version.trim().is_empty())
        .then(|| options.region.runtime.unity_version.parse())
        .transpose()
        .map_err(
            |error: unity_rs_core::unity_version::ParseUnityVersionError| {
                ExportPipelineError::UnityRs {
                    message: error.to_string(),
                }
            },
        )?;
    let studio = Studio::open_with_options(
        input_path,
        AssetLoadOptions {
            unity_version_override,
            ..AssetLoadOptions::default()
        },
    )
    .map_err(unity_rs_error)?;
    let object_refs = studio.objects().collect::<Vec<_>>();
    let assets = object_refs
        .iter()
        .enumerate()
        .map(|(index, object)| unity_asset_info(index, *object))
        .collect::<Vec<_>>();
    let mut object_read_plan = NativeObjectReadPlanStats {
        inspected_objects: assets.len(),
        ..NativeObjectReadPlanStats::default()
    };
    for asset in &assets {
        object_type_read_stats_mut(&mut object_read_plan, asset).inspected_objects += 1;
    }
    let mut summary = NativeObjectExportSummary {
        written_files: Vec::new(),
        acb_sources: Vec::new(),
        phase_ms: HashMap::from([("unity_rs.open".to_string(), elapsed_millis(open_started))]),
        skipped_object_reads: Vec::new(),
        object_read_plan,
    };
    summary
        .phase_ms
        .insert("unity_rs.files".to_string(), studio.file_count() as u64);
    let configured_asset_types = asset_studio_export_type_list(options.region);
    let mut readable_assets =
        select_native_object_readable_assets(&assets, &configured_asset_types, &mut summary);
    sort_native_object_reads_for_failure_isolation(&mut readable_assets);

    let read_batch_size =
        native_read_batch_size_for_assets(options.read_batch_size, &readable_assets);
    let mut path_state = NativeSemanticExportPathState::with_registry(path_registry.clone());
    let mut playable_outputs = Vec::new();
    for asset_chunk in readable_assets.chunks(read_batch_size) {
        summary.object_read_plan.batch_count += 1;
        let batch_started = Instant::now();
        for asset in asset_chunk {
            let object = object_refs[asset.index];
            let read_kind = native_read_kind_for_asset(asset, options.read_kinds);
            match read_unity_rs_object(&studio, object, asset, &read_kind, options.image_format) {
                Ok(read_output) => {
                    summary.object_read_plan.successful_reads += 1;
                    summary.object_read_plan.payload_bundle_bytes +=
                        read_output.payload.len() as u64;
                    let type_stats =
                        object_type_read_stats_mut(&mut summary.object_read_plan, asset);
                    type_stats.successful_reads += 1;
                    type_stats.payload_bytes += read_output.payload.len() as u64;
                    merge_phase_ms(&mut summary.phase_ms, &read_output.response.phase_ms);
                    if is_playable_mono_typetree(asset, &read_output) {
                        playable_outputs.push(((*asset).clone(), read_output));
                    } else {
                        write_native_object_payload(options, &mut path_state, asset, &read_output)?;
                    }
                }
                Err(error) => {
                    warn!(
                        path_id = asset.path_id,
                        asset_type = asset.asset_type.as_deref().unwrap_or(""),
                        name = asset.name.as_deref().unwrap_or(""),
                        error = %error,
                        "unity-rs object read failed; skipping object"
                    );
                    summary.skipped_object_reads.push(NativeSkippedObjectRead {
                        path_id: asset.path_id,
                        asset_type: asset.asset_type.clone(),
                        name: asset.name.clone(),
                        container: asset.container.clone(),
                        error: error.to_string(),
                    });
                    summary.object_read_plan.failed_reads += 1;
                    summary.object_read_plan.skipped_reads += 1;
                    let type_stats =
                        object_type_read_stats_mut(&mut summary.object_read_plan, asset);
                    type_stats.failed_reads += 1;
                    type_stats.skipped_reads += 1;
                }
            }
        }
        *summary
            .phase_ms
            .entry("unity_rs.read_batches".to_string())
            .or_default() += elapsed_millis(batch_started);
    }
    write_assetstudio_playable_payloads(options, &mut path_state, playable_outputs)?;
    summary.written_files = path_state.written_files;
    summary.acb_sources = path_state.acb_sources;
    path_state.image_encode.merge_into(&mut summary.phase_ms);
    Ok(summary)
}

fn validate_unity_input(input_path: &Path) -> Result<(), ExportPipelineError> {
    let mut file = std::fs::File::open(input_path).map_err(|source| ExportPipelineError::Io {
        path: input_path.to_path_buf(),
        source,
    })?;
    let file_size = file
        .metadata()
        .map_err(|source| ExportPipelineError::Io {
            path: input_path.to_path_buf(),
            source,
        })?
        .len();
    let header_length = usize::try_from(file_size)
        .unwrap_or(usize::MAX)
        .min(HEADER_SCAN_LENGTH);
    let mut header = vec![0; header_length];
    file.read_exact(&mut header)
        .map_err(|source| ExportPipelineError::Io {
            path: input_path.to_path_buf(),
            source,
        })?;

    if detect_file_type(&header, file_size).file_type == FileType::ResourceFile {
        return Err(ExportPipelineError::UnrecognizedUnityInput {
            path: input_path.to_path_buf(),
        });
    }
    Ok(())
}

fn unity_asset_info(index: usize, object: StudioObject<'_>) -> UnityAssetInfo {
    let asset_type = unity_class_name(object.class_id());
    UnityAssetInfo {
        index,
        name: object
            .name()
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .or_else(|| Some(format!("{asset_type}_#{index}"))),
        container: object
            .container()
            .filter(|container| !container.is_empty())
            .map(str::to_string),
        asset_type: Some(asset_type),
        type_id: object.class_id(),
        path_id: object.path_id(),
        unique_id: Some(format!("_#{index}")),
        size: i64::try_from(object.byte_size()).unwrap_or(i64::MAX),
        source_file: Some(object.source_path().to_string()),
    }
}

fn read_unity_rs_object(
    studio: &Studio,
    object: StudioObject<'_>,
    asset: &UnityAssetInfo,
    kind: &str,
    image_format: &str,
) -> Result<UnityObjectReadOutput, ExportPipelineError> {
    const MAX_PAYLOAD_BYTES: u64 = 1024 * 1024 * 1024;
    let started = Instant::now();
    let normalized = if kind.trim().is_empty() {
        "auto"
    } else {
        kind.trim()
    };
    let (payload, payload_kind, suggested_extension): (NativeObjectPayload, &str, String) =
        match (object.class_id(), normalized) {
            (49, "auto" | "text_bytes") => (
                object
                    .read_text_bytes(MAX_PAYLOAD_BYTES as usize)
                    .map_err(unity_rs_error)?
                    .into(),
                "text_bytes",
                ".bytes".to_string(),
            ),
            (TEXTURE_2D_CLASS_ID, "auto" | "image") => {
                require_raw_rgba(image_format, "Texture2D")?;
                let limits = TextureReadLimits {
                    maximum_payload_bytes: MAX_PAYLOAD_BYTES,
                    maximum_output_bytes: MAX_PAYLOAD_BYTES.saturating_sub(36),
                    ..TextureReadLimits::default()
                };
                let loaded = &studio.collection().serialized_files()[object.file_index()].file;
                let texture =
                    read_texture2d(studio.collection(), loaded, object.object_index(), limits)
                        .map_err(unity_rs_error)?;
                if texture.data.is_empty() {
                    // Unity legitimately serializes empty dynamic-font atlases and fills them at
                    // runtime. There is no image to encode, but retaining the complete object keeps
                    // an `all` export lossless and avoids misclassifying the placeholder as a decoder
                    // failure.
                    (
                        object
                            .read_raw(MAX_PAYLOAD_BYTES)
                            .map_err(unity_rs_error)?
                            .into(),
                        "raw",
                        ".dat".to_string(),
                    )
                } else {
                    let image = texture
                        .decode_mip_rgba8(0, limits)
                        .map_err(unity_rs_error)?;
                    let mut surface = DecodedRgbaSurface {
                        width: image.width,
                        height: image.height,
                        pixels: image.pixels,
                    };
                    surface.flip_vertically();
                    (
                        NativeObjectPayload::Rgba(Box::new(surface)),
                        "image_raw_rgba",
                        ".rgba".to_string(),
                    )
                }
            }
            (TEXTURE_2D_ARRAY_CLASS_ID, "auto" | "image" | "image_archive") => {
                require_raw_rgba(image_format, "Texture2DArray")?;
                let limits = TextureArrayReadLimits {
                    maximum_payload_bytes: MAX_PAYLOAD_BYTES,
                    maximum_output_bytes: MAX_PAYLOAD_BYTES,
                    maximum_bundle_bytes: MAX_PAYLOAD_BYTES,
                    ..TextureArrayReadLimits::default()
                };
                let loaded = &studio.collection().serialized_files()[object.file_index()].file;
                let texture = read_texture2d_array(
                    studio.collection(),
                    loaded,
                    object.object_index(),
                    limits,
                )
                .map_err(unity_rs_error)?;
                let mut payload = Vec::new();
                write_texture2d_array_rgba_bundle(&texture, limits, &mut payload)
                    .map_err(unity_rs_error)?;
                (payload.into(), "image_array_bundle_raw_rgba", String::new())
            }
            (SPRITE_CLASS_ID, "auto" | "image") => {
                require_raw_rgba(image_format, "Sprite")?;
                let image_bytes = MAX_PAYLOAD_BYTES.saturating_sub(36);
                let sprite_limits = SpriteReadLimits {
                    maximum_output_pixels: image_bytes / 4,
                    maximum_output_bytes: image_bytes,
                    ..SpriteReadLimits::default()
                };
                let texture_limits = TextureReadLimits {
                    maximum_payload_bytes: MAX_PAYLOAD_BYTES,
                    maximum_output_bytes: image_bytes,
                    ..TextureReadLimits::default()
                };
                let image = object
                    .decode_sprite(sprite_limits, texture_limits)
                    .map_err(unity_rs_error)?;
                // Sprite cropping already flipped these rows, which is why this
                // path used `write_rgba_ir_display_order`; no second flip.
                (
                    NativeObjectPayload::Rgba(Box::new(DecodedRgbaSurface {
                        width: image.width,
                        height: image.height,
                        pixels: image.pixels,
                    })),
                    "image_raw_rgba",
                    ".rgba".to_string(),
                )
            }
            (SHADER_CLASS_ID, "auto" | "shader" | "text") => (
                object
                    .read_shader_text(MAX_PAYLOAD_BYTES)
                    .map_err(unity_rs_error)?
                    .into(),
                "shader_text",
                ".shader".to_string(),
            ),
            (unity_rs_core::mesh::MESH_CLASS_ID, "auto" | "mesh" | "obj") => (
                object
                    .read_mesh_obj(MeshReadLimits {
                        maximum_output_bytes: MAX_PAYLOAD_BYTES,
                        ..MeshReadLimits::default()
                    })
                    .map_err(unity_rs_error)?
                    .into(),
                "mesh_obj",
                ".obj".to_string(),
            ),
            (MONO_BEHAVIOUR_CLASS_ID, "auto" | "typetree_json") => {
                let loaded = &studio.collection().serialized_files()[object.file_index()].file;
                let limits = MonoBehaviourReadLimits {
                    maximum_json_bytes: MAX_PAYLOAD_BYTES as usize,
                    ..MonoBehaviourReadLimits::default()
                };
                match read_mono_behaviour_json(loaded, object.object_index(), false, limits) {
                    Ok(json) => (
                        json.into_bytes().into(),
                        "typetree_json",
                        ".json".to_string(),
                    ),
                    Err(unity_rs_core::Error::Unsupported(_)) => (
                        object
                            .read_raw(MAX_PAYLOAD_BYTES)
                            .map_err(unity_rs_error)?
                            .into(),
                        "raw",
                        ".dat".to_string(),
                    ),
                    Err(error) => return Err(unity_rs_error(error)),
                }
            }
            (AUDIO_CLIP_CLASS_ID, "auto" | "audio" | "raw") => {
                let simple = object
                    .read_audio_clip(SimpleAssetReadLimits::default())
                    .map_err(unity_rs_error)?;
                let extension = simple.raw_extension;
                let payload = simple
                    .payload
                    .read_to_vec(MAX_PAYLOAD_BYTES)
                    .map_err(unity_rs_error)?;
                (payload.into(), "audio_raw", extension)
            }
            (VIDEO_CLIP_CLASS_ID, "auto" | "video" | "raw") => read_simple_asset(
                object.read_video_clip(SimpleAssetReadLimits::default()),
                MAX_PAYLOAD_BYTES,
            )?,
            (MOVIE_TEXTURE_CLASS_ID, "auto" | "video" | "raw") => read_simple_asset(
                object.read_movie_texture(SimpleAssetReadLimits::default()),
                MAX_PAYLOAD_BYTES,
            )?,
            (FONT_CLASS_ID, "auto" | "font" | "raw") => read_simple_asset(
                object.read_font(SimpleAssetReadLimits::default()),
                MAX_PAYLOAD_BYTES,
            )?,
            (_, "raw") => (
                object
                    .read_raw(MAX_PAYLOAD_BYTES)
                    .map_err(unity_rs_error)?
                    .into(),
                "raw",
                ".dat".to_string(),
            ),
            (_, "auto" | "typetree_json") => {
                match object.read_type_tree_json(false, MAX_PAYLOAD_BYTES as usize) {
                    Ok(payload) => (payload.into(), "typetree_json", ".json".to_string()),
                    Err(_) => (
                        object
                            .read_raw(MAX_PAYLOAD_BYTES)
                            .map_err(unity_rs_error)?
                            .into(),
                        "raw",
                        ".dat".to_string(),
                    ),
                }
            }
            _ => {
                return Err(ExportPipelineError::UnityRs {
                    message: format!(
                        "requested kind `{normalized}` is unsupported for {}",
                        asset.asset_type.as_deref().unwrap_or("unknown object")
                    ),
                });
            }
        };
    let duration_ms = elapsed_millis(started);
    Ok(UnityObjectReadOutput {
        response: UnityObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some(payload_kind.to_string()),
            payload_len: i64::try_from(payload.len()).unwrap_or(i64::MAX),
            suggested_extension: Some(suggested_extension),
            warnings: Vec::new(),
            phase_ms: HashMap::from([("unity_rs.read_object".to_string(), duration_ms)]),
            error: None,
            duration_ms: Some(duration_ms),
        },
        payload,
    })
}

fn read_simple_asset(
    result: unity_rs_core::Result<unity_rs_core::simple_assets::SimpleBinaryAsset>,
    maximum_bytes: u64,
) -> Result<(NativeObjectPayload, &'static str, String), ExportPipelineError> {
    let simple = result.map_err(unity_rs_error)?;
    let payload = simple
        .payload
        .read_to_vec(maximum_bytes)
        .map_err(unity_rs_error)?;
    Ok((
        payload.into(),
        simple.payload_kind,
        simple.suggested_extension,
    ))
}

fn require_raw_rgba(image_format: &str, asset_type: &str) -> Result<(), ExportPipelineError> {
    if image_format == UNITY_ENGINE_DEFAULT_IMAGE_FORMAT {
        return Ok(());
    }
    Err(ExportPipelineError::UnityRs {
        message: format!("{asset_type} reads only support raw_rgba"),
    })
}

fn unity_rs_error(error: unity_rs_core::Error) -> ExportPipelineError {
    ExportPipelineError::UnityRs {
        message: error.to_string(),
    }
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn unity_class_name(class_id: i32) -> String {
    match class_id {
        1 => "GameObject",
        4 => "Transform",
        21 => "Material",
        23 => "MeshRenderer",
        TEXTURE_2D_CLASS_ID => "Texture2D",
        33 => "MeshFilter",
        43 => "Mesh",
        SHADER_CLASS_ID => "Shader",
        49 => "TextAsset",
        AUDIO_CLIP_CLASS_ID => "AudioClip",
        95 => "Animator",
        MONO_BEHAVIOUR_CLASS_ID => "MonoBehaviour",
        FONT_CLASS_ID => "Font",
        137 => "SkinnedMeshRenderer",
        MOVIE_TEXTURE_CLASS_ID => "MovieTexture",
        TEXTURE_2D_ARRAY_CLASS_ID => "Texture2DArray",
        SPRITE_CLASS_ID => "Sprite",
        VIDEO_CLIP_CLASS_ID => "VideoClip",
        other => return format!("ClassID{other}"),
    }
    .to_string()
}

pub(super) fn native_read_batch_size_for_assets(
    configured_size: usize,
    assets: &[&UnityAssetInfo],
) -> usize {
    let configured_size = configured_size.max(1);
    if assets.is_empty() {
        return 1;
    }

    let image_count = assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                assetstudio_type_selector_matches("Texture2D", asset_type)
                    || assetstudio_type_selector_matches("Sprite", asset_type)
            })
        })
        .count();
    let typetree_count = assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                assetstudio_type_selector_matches("MonoBehaviour", asset_type)
            })
        })
        .count();

    let tuned_size = if image_count * 2 >= assets.len() {
        configured_size.max(64)
    } else if typetree_count * 2 >= assets.len() {
        configured_size.min(32)
    } else {
        configured_size
    };
    tuned_size.max(1).min(assets.len().max(1))
}

#[cfg(test)]
pub(super) fn native_object_read_subchunks<'a>(
    asset_chunk: &'a [&'a UnityAssetInfo],
    image_format: &str,
) -> Vec<&'a [&'a UnityAssetInfo]> {
    let mut subchunks = Vec::new();
    let mut group_start = 0usize;
    for (index, asset) in asset_chunk.iter().enumerate() {
        if !is_unity_engine_non_bmp_image_read(asset, image_format) {
            continue;
        }
        if group_start < index {
            subchunks.push(&asset_chunk[group_start..index]);
        }
        subchunks.push(&asset_chunk[index..index + 1]);
        group_start = index + 1;
    }
    if group_start < asset_chunk.len() {
        subchunks.push(&asset_chunk[group_start..]);
    }
    subchunks
}

#[cfg(test)]
pub(super) fn is_unity_engine_non_bmp_image_read(
    asset: &UnityAssetInfo,
    image_format: &str,
) -> bool {
    is_native_image_asset(asset) && native_image_format_for_asset(asset, image_format) != "bmp"
}

pub(super) fn select_native_object_readable_assets<'a>(
    assets: &'a [UnityAssetInfo],
    configured_asset_types: &[String],
    summary: &mut NativeObjectExportSummary,
) -> Vec<&'a UnityAssetInfo> {
    let mut readable_assets = Vec::new();
    let texture2d_array_containers = texture2d_array_parent_containers(assets);
    for asset in assets {
        if !assetstudio_object_mode_type_enabled(asset, configured_asset_types) {
            continue;
        }
        if is_texture2d_array_image_with_parent(asset, &texture2d_array_containers) {
            summary.skipped_object_reads.push(NativeSkippedObjectRead {
                path_id: asset.path_id,
                asset_type: asset.asset_type.clone(),
                name: asset.name.clone(),
                container: asset.container.clone(),
                error: "Texture2DArrayImage is covered by its Texture2DArray parent".to_string(),
            });
            object_type_read_stats_mut(&mut summary.object_read_plan, asset).skipped_reads += 1;
            continue;
        }
        if !is_native_object_supported_asset(asset) {
            if let Some(skipped) = native_skipped_unsupported_asset(asset) {
                warn!(
                    path_id = skipped.path_id,
                    asset_type = skipped.asset_type.as_deref().unwrap_or(""),
                    name = skipped.name.as_deref().unwrap_or(""),
                    "unity-rs object type is not readable yet; skipping object"
                );
                summary.skipped_object_reads.push(skipped);
                object_type_read_stats_mut(&mut summary.object_read_plan, asset).skipped_reads += 1;
            }
            continue;
        }
        let type_stats = object_type_read_stats_mut(&mut summary.object_read_plan, asset);
        type_stats.planned_objects += 1;
        type_stats.readable_objects += 1;
        readable_assets.push(asset);
    }
    summary.object_read_plan.planned_objects = readable_assets.len();
    summary.object_read_plan.readable_objects = readable_assets.len();
    summary.object_read_plan.skipped_reads = summary.skipped_object_reads.len();
    readable_assets
}

fn object_type_read_stats_mut<'a>(
    plan: &'a mut NativeObjectReadPlanStats,
    asset: &UnityAssetInfo,
) -> &'a mut NativeObjectTypeReadStats {
    let asset_type = asset.asset_type.as_deref().unwrap_or("Unknown");
    plan.by_type.entry(asset_type.to_string()).or_default()
}

pub(super) fn sort_native_object_reads_for_failure_isolation(assets: &mut Vec<&UnityAssetInfo>) {
    assets.sort_by_key(|asset| {
        let priority = if is_native_image_asset(asset) { 1 } else { 0 };
        (priority, asset.index)
    });
}

pub(super) fn is_native_image_asset(asset: &UnityAssetInfo) -> bool {
    asset.asset_type.as_deref().is_some_and(|asset_type| {
        assetstudio_type_selector_matches("Texture2D", asset_type)
            || assetstudio_type_selector_matches("Texture2DArray", asset_type)
            || assetstudio_type_selector_matches("Sprite", asset_type)
    })
}

pub(super) fn texture2d_array_parent_containers(assets: &[UnityAssetInfo]) -> HashSet<String> {
    assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                normalize_assetstudio_type_name(asset_type) == "texture2darray"
            })
        })
        .filter_map(normalized_native_asset_container)
        .collect()
}

pub(super) fn is_texture2d_array_image_with_parent(
    asset: &UnityAssetInfo,
    parent_containers: &HashSet<String>,
) -> bool {
    asset.asset_type.as_deref().is_some_and(|asset_type| {
        normalize_assetstudio_type_name(asset_type) == "texture2darrayimage"
    }) && normalized_native_asset_container(asset)
        .is_some_and(|container| parent_containers.contains(&container))
}

pub(super) fn normalized_native_asset_container(asset: &UnityAssetInfo) -> Option<String> {
    asset
        .container
        .as_deref()
        .map(|container| container.replace('\\', "/"))
        .map(|container| container.trim().to_string())
        .filter(|container| !container.is_empty())
}

#[cfg(test)]
pub(super) fn native_image_format_for_asset(_asset: &UnityAssetInfo, _configured: &str) -> String {
    UNITY_ENGINE_DEFAULT_IMAGE_FORMAT.to_string()
}

pub(super) fn is_native_object_supported_asset(asset: &UnityAssetInfo) -> bool {
    asset
        .asset_type
        .as_deref()
        .is_some_and(assetstudio_object_mode_supported_type)
}

pub(super) fn assetstudio_object_mode_type_enabled(
    asset: &UnityAssetInfo,
    configured_asset_types: &[String],
) -> bool {
    let Some(asset_type) = asset.asset_type.as_deref() else {
        return false;
    };
    configured_asset_types
        .iter()
        .any(|configured| assetstudio_type_selector_matches(configured, asset_type))
}

pub(super) fn native_read_kind_for_asset(
    asset: &UnityAssetInfo,
    configured_kinds: &BTreeMap<String, String>,
) -> String {
    let asset_type = asset.asset_type.as_deref().unwrap_or_default();
    configured_kinds
        .iter()
        .filter(|(selector, _)| !selector.trim().eq_ignore_ascii_case("all"))
        .find_map(|(selector, kind)| {
            assetstudio_type_selector_matches(selector, asset_type)
                .then(|| normalize_native_read_kind(kind))
        })
        .or_else(|| {
            configured_kinds
                .iter()
                .find(|(selector, _)| selector.trim().eq_ignore_ascii_case("all"))
                .map(|(_, kind)| normalize_native_read_kind(kind))
        })
        .unwrap_or_else(|| default_native_read_kind(asset_type).to_string())
}

pub(super) fn normalize_native_read_kind(kind: &str) -> String {
    kind.trim().to_lowercase()
}

pub(super) fn default_native_read_kind(asset_type: &str) -> &'static str {
    match normalize_assetstudio_type_name(asset_type).as_str() {
        "texture2d" | "texture2darray" | "texture2darrayimage" | "sprite" => "image",
        "textasset" => "text_bytes",
        "monobehaviour" | "monobehavior" => "typetree_json",
        "audioclip" => "audio",
        "videoclip" | "movietexture" => "video",
        "font" => "font",
        "shader" => "shader",
        "mesh" => "obj",
        _ => "typetree_json",
    }
}

pub(super) fn native_skipped_unsupported_asset(
    asset: &UnityAssetInfo,
) -> Option<NativeSkippedObjectRead> {
    let asset_type = asset.asset_type.as_deref()?;
    let error = if assetstudio_object_mode_known_unreadable_type(asset_type) {
        format!("native object mode does not support reading {asset_type} yet")
    } else {
        format!("native object mode has no read strategy for {asset_type}")
    };
    Some(NativeSkippedObjectRead {
        path_id: asset.path_id,
        asset_type: asset.asset_type.clone(),
        name: asset.name.clone(),
        container: asset.container.clone(),
        error,
    })
}

pub(super) fn assetstudio_object_mode_supported_type(asset_type: &str) -> bool {
    !asset_type.trim().is_empty()
}

pub(super) fn assetstudio_object_mode_known_unreadable_type(asset_type: &str) -> bool {
    matches!(
        asset_type,
        "Animation"
            | "AnimationClip"
            | "AnimatorController"
            | "AssetBundle"
            | "AudioListener"
            | "Avatar"
            | "Camera"
            | "Canvas"
            | "CanvasRenderer"
            | "Cubemap"
            | "GameObject"
            | "Material"
            | "MeshFilter"
            | "MeshRenderer"
            | "MonoScript"
            | "ParticleSystem"
            | "ParticleSystemRenderer"
            | "PlayableDirector"
            | "RectTransform"
            | "ShaderVariantCollection"
            | "SkinnedMeshRenderer"
            | "SortingGroup"
            | "SpriteMask"
            | "SpriteRenderer"
            | "TextMesh"
            | "Texture3D"
            | "Transform"
    )
}
