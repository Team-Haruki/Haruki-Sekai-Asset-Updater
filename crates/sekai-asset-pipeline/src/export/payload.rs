//! Writing one read object's payload to its output files.
//!
//! This file dispatches by payload kind; the submodules do the work.

pub(super) mod bundle;
pub(super) mod dedup;
pub(super) mod image_files;
pub(super) mod manifest;
pub(super) mod naming;
pub(super) mod playable;

use std::path::{Path, PathBuf};

use tracing::debug;

use crate::{ExportPipelineError, PipelineRegionOptions as RegionConfig};

use self::bundle::write_payload_bundle;
use self::dedup::remove_byte_identical_semantic_duplicates;
use self::image_files::{
    write_native_image_payload_bundle_final_files_now, write_native_image_payload_final_files_now,
    write_native_image_surface_final_files_now,
};
use self::manifest::write_assetstudio_export_manifest_entry;
use self::naming::{
    is_text_asset_acb_target, is_text_asset_decoded_usm_target,
    native_image_surrogate_public_target, text_asset_public_bytes_target,
};
use super::paths::{assetbundle_typetree_output_path, native_object_output_path};
use super::types::{
    NativeImageEncodeSettings, NativeInMemoryMediaSource, NativeObjectExportOptions,
    NativeSemanticExportPathState, NativeSemanticPathClaim, UnityAssetInfo, UnityObjectReadOutput,
};

pub(super) fn write_native_object_payload(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    asset: &UnityAssetInfo,
    read_output: &UnityObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    if read_output.payload.is_empty()
        || read_output.response.payload_kind.as_deref() == Some("unsupported")
    {
        return Ok(());
    }

    let Some(target) = claim_native_payload_target(options, path_state, asset, read_output)? else {
        return Ok(());
    };
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let payload_kind = read_output.response.payload_kind.as_deref().unwrap_or("");
    if is_text_asset_acb_target(asset, &target) {
        path_state.acb_sources.push(NativeInMemoryMediaSource {
            target: target.clone(),
            // Deliberate copy: ACB sources outlive the whole export into the media
            // post-process stage, so they must not pin the read-batch bundle.
            payload: read_output.payload.bytes().to_vec(),
        });
        return Ok(());
    }

    let written_files = write_native_payload_by_kind(
        path_state,
        &target,
        read_output,
        options.region,
        options.image_encode,
    )?;
    let manifest_target = if payload_kind == "image_bmp" || payload_kind == "image_raw_rgba" {
        native_image_surrogate_public_target(&target, options.region)
    } else {
        target.clone()
    };
    let manifest_written_files = written_files.clone();
    path_state.written_files.extend(written_files);
    if is_text_asset_decoded_usm_target(asset, &target, options.region) {
        return Ok(());
    }
    write_native_payload_manifest(
        options.output_dir,
        options.region,
        &manifest_target,
        manifest_written_files,
        asset,
        read_output,
    )
}

fn claim_native_payload_target(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    asset: &UnityAssetInfo,
    read_output: &UnityObjectReadOutput,
) -> Result<Option<PathBuf>, ExportPipelineError> {
    let target = native_object_output_path(
        options.output_dir,
        options.export_path,
        options.strip_path_prefix,
        options.region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        read_output.response.suggested_extension.as_deref(),
    );
    let target = text_asset_public_bytes_target(&target, asset).unwrap_or(target);
    let target = assetbundle_typetree_output_path(
        options.output_dir,
        options.export_path,
        options.strip_path_prefix,
        options.region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        read_output.payload.bytes(),
    )?
    .unwrap_or(target);
    match path_state.claim_payload(target, asset, read_output) {
        NativeSemanticPathClaim::Claimed(target) => Ok(Some(target)),
        NativeSemanticPathClaim::Duplicate { existing } => {
            debug!(
                asset_type = asset.asset_type.as_deref().unwrap_or(""),
                name = asset.name.as_deref().unwrap_or(""),
                container = asset.container.as_deref().unwrap_or(""),
                output_path = %existing.display(),
                "skipping byte-identical duplicate native assetstudio object"
            );
            Ok(None)
        }
    }
}

fn write_native_payload_by_kind(
    path_state: &mut NativeSemanticExportPathState,
    target: &Path,
    read_output: &UnityObjectReadOutput,
    region: &RegionConfig,
    image_encode: &NativeImageEncodeSettings,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let payload_kind = read_output.response.payload_kind.as_deref().unwrap_or("");
    let written_files = if payload_kind == "image_array_bundle_raw_rgba" {
        write_native_image_payload_bundle_final_files_now(
            path_state,
            target,
            read_output.payload.shared_bytes(),
            region,
            image_encode,
        )?
    } else if payload_kind.starts_with("image_array_bundle_")
        || payload_kind == "animator_bundle_fbx"
    {
        write_payload_bundle(target, read_output.payload.bytes())?
    } else if matches!(payload_kind, "image_bmp" | "image_raw_rgba") {
        match read_output.payload.surface() {
            Some(surface) => write_native_image_surface_final_files_now(
                path_state,
                target,
                surface,
                region,
                image_encode,
            )?,
            None => write_native_image_payload_final_files_now(
                path_state,
                target,
                read_output.payload.bytes(),
                region,
                image_encode,
            )?,
        }
    } else {
        write_native_payload_file(target, read_output.payload.bytes())?;
        vec![target.to_path_buf()]
    };
    if !matches!(
        payload_kind,
        "image_bmp" | "image_raw_rgba" | "image_array_bundle_raw_rgba"
    ) {
        for written_file in &written_files {
            remove_byte_identical_semantic_duplicates(written_file, &path_state.registry)?;
        }
    }
    Ok(written_files)
}

fn write_native_payload_manifest(
    output_dir: &Path,
    region: &RegionConfig,
    manifest_target: &Path,
    written_files: Vec<PathBuf>,
    asset: &UnityAssetInfo,
    read_output: &UnityObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    let payload_kind = read_output.response.payload_kind.as_deref().unwrap_or("");
    if payload_kind.starts_with("image_array_bundle_") {
        for written_file in written_files {
            let target = native_image_surrogate_public_target(&written_file, region);
            write_assetstudio_export_manifest_entry(output_dir, &target, asset, read_output)?;
        }
        return Ok(());
    }
    write_assetstudio_export_manifest_entry(output_dir, manifest_target, asset, read_output)
}

pub(super) fn write_native_payload_file(
    target: &Path,
    payload: &[u8],
) -> Result<(), ExportPipelineError> {
    match std::fs::write(target, payload) {
        Ok(()) => Ok(()),
        Err(source) => Err(ExportPipelineError::Io {
            path: target.to_path_buf(),
            source,
        }),
    }
}

/// Benchmark switch for the flat execution shape.
///
/// The shipped pipeline is staged: a bundle is downloaded and read in one pool,
/// then handed to a second pool for image encoding, and every CPU-heavy section
/// takes a permit from one global budget. The Python front-end this service is
/// benchmarked against is instead a flat pool -- N workers, each doing one
/// bundle end to end, nothing shared. That reaches 9.8 of 10 cores where the
/// staged shape reaches 8.4, so this switch exists to measure how much of the
/// difference is the shape itself rather than the work.
///
/// Off unless `HARUKI_FLAT_PIPELINE=1`. Not a supported production mode: it
/// drops the CPU budget entirely, which is what bounds this service when it
/// shares a host.
pub(super) fn flat_pipeline_enabled() -> bool {
    static FLAT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAT.get_or_init(|| {
        std::env::var("HARUKI_FLAT_PIPELINE")
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    })
}
