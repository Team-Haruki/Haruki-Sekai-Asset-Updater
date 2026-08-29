//! Encoding one image into every configured output format.

use std::borrow::Cow;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Instant;

use image::ImageReader;

use crate::{
    ExportPipelineError, ImageEncodingOptions as ImageBackendConfig,
    PipelineRegionOptions as RegionConfig,
};

use super::super::images::{
    decode_image_payload_bytes, encode_dynamic_image, encode_native_rgba_ir,
    parse_native_rgba_ir_payload, write_encoded_image, NativeRgbaIr,
};
use super::super::limits::{
    acquire_cpu_budget_permit_blocking, acquire_image_memory_permit_blocking,
};
use super::super::paths::image_output_file_for_format;
use super::super::types::{
    DecodedRgbaSurface, NativeImageEncodeSettings, NativeSemanticExportPathRegistry,
    NativeSemanticExportPathState, UNITY_ENGINE_RGBA_IR_MAGIC,
};
use super::bundle::{parse_payload_bundle_shared, payload_bundle_entry_target};
use super::dedup::remove_byte_identical_semantic_duplicates;

/// Encodes and writes one image where it was decoded.
///
/// This used to push the decoded RGBA onto `pending_image_writes` and encode it
/// later, in a stage the bundle reached only after waiting for a post-process
/// slot. Measured on 48 cores over 16 844 JP bundles, that queue held the
/// dominant share of a 23 GB peak RSS: an RGBA surface is 2.5-4x its encoded
/// form and up to `download + post_process * 2` bundles' worth were resident at
/// once. Encoding here bounds live pixel data by the number of bundles actually
/// being read instead of by the depth of a queue.
pub(crate) fn write_native_image_payload_final_files_now(
    path_state: &mut NativeSemanticExportPathState,
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
    image_encode: &NativeImageEncodeSettings,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let started = Instant::now();
    let written = write_native_image_payload_final_files_with_limits(
        target,
        payload,
        region,
        &image_encode.backend,
        &path_state.registry,
        image_encode.cpu_budget,
        image_encode.memory_limit_bytes,
    )?;
    path_state
        .image_encode
        .record(&region.export.images.output_formats(), started);
    Ok(written)
}

#[cfg(test)]
pub(crate) fn write_native_image_payload_final_files(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let path_registry = NativeSemanticExportPathRegistry::default();
    write_native_image_payload_final_files_with_registry(
        target,
        payload,
        region,
        &ImageBackendConfig::default(),
        &path_registry,
    )
}

#[cfg(test)]
pub(crate) fn write_native_image_payload_final_files_with_backend(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
    image_backend: &ImageBackendConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let path_registry = NativeSemanticExportPathRegistry::default();
    write_native_image_payload_final_files_with_registry(
        target,
        payload,
        region,
        image_backend,
        &path_registry,
    )
}

pub(super) fn image_payload_scratch_bytes(
    target: &Path,
    payload: &[u8],
) -> Result<usize, ExportPipelineError> {
    let rgba_bytes = if payload.starts_with(UNITY_ENGINE_RGBA_IR_MAGIC) {
        let raw_rgba = parse_native_rgba_ir_payload(payload, target)?;
        raw_rgba.row_bytes.saturating_mul(raw_rgba.height_usize)
    } else {
        let (width, height) = ImageReader::new(Cursor::new(payload))
            .with_guessed_format()
            .map_err(|source| ExportPipelineError::Io {
                path: target.to_path_buf(),
                source,
            })?
            .into_dimensions()
            .map_err(|source| ExportPipelineError::Image {
                path: target.to_path_buf(),
                source,
            })?;
        usize::try_from(width)
            .unwrap_or(usize::MAX)
            .saturating_mul(usize::try_from(height).unwrap_or(usize::MAX))
            .saturating_mul(4)
    };

    // One decoded/borrowed RGBA surface plus one conversion or encoded output.
    // The compressed input is included because it remains live for the job.
    Ok(payload.len().saturating_add(rgba_bytes.saturating_mul(2)))
}

pub(super) fn write_native_image_payload_final_files_with_limits(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
    image_backend: &ImageBackendConfig,
    path_registry: &NativeSemanticExportPathRegistry,
    cpu_budget: Option<usize>,
    image_memory_limit_bytes: usize,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let scratch_bytes = image_payload_scratch_bytes(target, payload)?;
    let _memory_permit =
        acquire_image_memory_permit_blocking(image_memory_limit_bytes, scratch_bytes);
    let raw_rgba = payload
        .starts_with(UNITY_ENGINE_RGBA_IR_MAGIC)
        .then(|| parse_native_rgba_ir_payload(payload, target))
        .transpose()?;
    encode_image_outputs(
        target,
        region,
        image_backend,
        path_registry,
        cpu_budget,
        raw_rgba.as_ref(),
        payload,
    )
}

/// Encodes one decoded image into every configured output format.
///
/// Takes either an RGBA view or the original bytes: a texture arrives already
/// decoded, while other image kinds still need `image` to parse them.
pub(super) fn encode_image_outputs(
    target: &Path,
    region: &RegionConfig,
    image_backend: &ImageBackendConfig,
    path_registry: &NativeSemanticExportPathRegistry,
    cpu_budget: Option<usize>,
    raw_rgba: Option<&NativeRgbaIr<'_>>,
    payload: &[u8],
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let formats = region.export.images.output_formats();
    let mut image: Option<image::DynamicImage> = None;
    let mut written_files = Vec::with_capacity(formats.len());

    for format in formats {
        let output = image_output_file_for_format(target, format);
        let bytes = {
            let _cpu_permit = cpu_budget
                .map(acquire_cpu_budget_permit_blocking)
                .transpose()?
                .map(|guard| guard.permit);
            if let Some(raw_rgba) = raw_rgba {
                encode_native_rgba_ir(raw_rgba, &output, format, image_backend)?
            } else {
                let dynamic_image = match image.as_ref() {
                    Some(image) => Cow::Borrowed(image),
                    None => {
                        image = Some(decode_image_payload_bytes(payload, target)?);
                        Cow::Borrowed(image.as_ref().unwrap())
                    }
                };
                encode_dynamic_image(&dynamic_image, &output, format, image_backend)?
            }
        };
        write_encoded_image(&output, &bytes)?;
        remove_byte_identical_semantic_duplicates(&output, path_registry)?;
        written_files.push(output);
    }

    Ok(written_files)
}

/// Encodes a texture that is already decoded.
///
/// The byte path exists for image kinds that arrive encoded; a `Texture2D` or
/// `Sprite` no longer serialises itself into `HARUKI_RGBAIR_V1` just to be
/// parsed back here, so its pixels are read where they were decoded.
pub(crate) fn write_native_image_surface_final_files_now(
    path_state: &mut NativeSemanticExportPathState,
    target: &Path,
    surface: &DecodedRgbaSurface,
    region: &RegionConfig,
    image_encode: &NativeImageEncodeSettings,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let started = Instant::now();
    let _memory_permit =
        acquire_image_memory_permit_blocking(image_encode.memory_limit_bytes, surface.pixels.len());
    let row_bytes = surface.width as usize * 4;
    let raw_rgba = NativeRgbaIr {
        width: surface.width,
        height: surface.height,
        stride: row_bytes,
        row_bytes,
        height_usize: surface.height as usize,
        pixels: &surface.pixels,
    };
    let written = encode_image_outputs(
        target,
        region,
        &image_encode.backend,
        &path_state.registry,
        image_encode.cpu_budget,
        Some(&raw_rgba),
        &[],
    )?;
    path_state
        .image_encode
        .record(&region.export.images.output_formats(), started);
    Ok(written)
}

#[cfg(test)]
pub(super) fn write_native_image_payload_final_files_with_registry(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
    image_backend: &ImageBackendConfig,
    path_registry: &NativeSemanticExportPathRegistry,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    write_native_image_payload_final_files_with_limits(
        target,
        payload,
        region,
        image_backend,
        path_registry,
        None,
        0,
    )
}

pub(crate) fn write_native_image_payload_bundle_final_files_now(
    path_state: &mut NativeSemanticExportPathState,
    target: &Path,
    payload: &bytes::Bytes,
    region: &RegionConfig,
    image_encode: &NativeImageEncodeSettings,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let entries = parse_payload_bundle_shared(payload)?;
    let mut written_files = Vec::with_capacity(entries.len());
    for (name, bytes) in entries {
        let entry_target = payload_bundle_entry_target(target, &name).with_extension("png");
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        written_files.extend(write_native_image_payload_final_files_now(
            path_state,
            &entry_target,
            &bytes,
            region,
            image_encode,
        )?);
    }
    Ok(written_files)
}
