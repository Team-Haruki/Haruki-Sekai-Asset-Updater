use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use image::ImageReader;
use tracing::debug;

use crate::core::config::{ImageBackendConfig, ImageOutputFormat, RegionConfig};
use crate::core::errors::ExportPipelineError;

use super::images::{
    decode_image_payload_bytes, encode_dynamic_image, encode_native_rgba_ir,
    parse_native_rgba_ir_payload, write_encoded_image, NativeRgbaIr,
};
use super::limits::{acquire_cpu_budget_permit_blocking, acquire_image_memory_permit_blocking};
use super::paths::{
    assetbundle_typetree_output_path, image_output_file_for_format, native_object_output_path,
    safe_payload_bundle_path, strip_container_prefix,
};
use super::selectors::assetstudio_type_selector_matches;
use super::types::{
    image_format_extension, DecodedRgbaSurface, NativeAssetStudioExportManifestEntry,
    NativeImageEncodeSettings, NativeInMemoryMediaSource, NativeObjectExportOptions,
    NativeObjectPayload, NativePayloadSignature, NativePlayableExport, NativePlayableExportObject,
    NativeSemanticExportClaim, NativeSemanticExportPathRegistry, NativeSemanticExportPathState,
    NativeSemanticPathClaim, UnityAssetInfo, UnityObjectReadOutput,
    ASSETSTUDIO_MANIFEST_APPEND_LOCKS, ASSETSTUDIO_MANIFEST_LOCKS,
    UNITY_ENGINE_IMAGE_SURROGATE_FORMAT, UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC,
    UNITY_ENGINE_PAYLOAD_BUNDLE_V2_HEADER_LEN, UNITY_ENGINE_PAYLOAD_BUNDLE_V2_MAGIC,
    UNITY_ENGINE_PAYLOAD_BUNDLE_V2_VERSION, UNITY_ENGINE_RGBA_IR_MAGIC,
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

pub(super) fn is_playable_mono_typetree(
    asset: &UnityAssetInfo,
    read_output: &UnityObjectReadOutput,
) -> bool {
    asset
        .asset_type
        .as_deref()
        .is_some_and(|asset_type| assetstudio_type_selector_matches("MonoBehaviour", asset_type))
        && read_output.response.payload_kind.as_deref() == Some("typetree_json")
        && asset.container.as_deref().is_some_and(|container| {
            container
                .replace('\\', "/")
                .to_ascii_lowercase()
                .ends_with(".playable")
        })
}

pub(super) fn write_assetstudio_playable_payloads(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    playable_outputs: Vec<(UnityAssetInfo, UnityObjectReadOutput)>,
) -> Result<(), ExportPipelineError> {
    let mut by_container: BTreeMap<String, Vec<(UnityAssetInfo, UnityObjectReadOutput)>> =
        BTreeMap::new();
    for (asset, read_output) in playable_outputs {
        let Some(container) = asset
            .container
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.replace('\\', "/"))
        else {
            write_native_object_payload(options, path_state, &asset, &read_output)?;
            continue;
        };
        by_container
            .entry(container)
            .or_default()
            .push((asset, read_output));
    }

    for (container, mut entries) in by_container {
        entries.sort_by(|(left, _), (right, _)| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.index.cmp(&right.index))
        });
        let mut objects = Vec::with_capacity(entries.len());
        for (asset, read_output) in &entries {
            let data: sonic_rs::Value = sonic_rs::from_slice(read_output.payload.bytes())
                .map_err(|source| ExportPipelineError::JsonParse { source })?;
            objects.push(NativePlayableExportObject {
                name: asset.name.clone(),
                asset_type: asset.asset_type.clone(),
                data,
            });
        }
        let playable = NativePlayableExport {
            container: container.clone(),
            object_count: objects.len(),
            objects,
        };
        let payload = sonic_rs::to_vec_pretty(&playable)
            .map_err(|source| ExportPipelineError::JsonSerialize { source })?;
        let (first_asset, first_read_output) =
            entries
                .first()
                .ok_or_else(|| ExportPipelineError::UnityRs {
                    message: format!("playable export has no objects for container {container}"),
                })?;
        let target = playable_container_output_path(
            options.output_dir,
            options.export_path,
            options.strip_path_prefix,
            options.region.export.by_category,
            &container,
        );
        let target = match path_state.claim_generated_payload(target, first_asset, &payload) {
            NativeSemanticPathClaim::Claimed(target) => target,
            NativeSemanticPathClaim::Duplicate { existing } => {
                debug!(
                    container,
                    output_path = %existing.display(),
                    "skipping byte-identical duplicate generated playable"
                );
                continue;
            }
        };
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        write_native_payload_file(&target, &payload)?;
        remove_byte_identical_semantic_duplicates(&target, &path_state.registry)?;
        path_state.written_files.push(target.clone());
        write_assetstudio_export_manifest_entry(
            options.output_dir,
            &target,
            first_asset,
            first_read_output,
        )?;
    }
    Ok(())
}

pub(super) fn playable_container_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    container: &str,
) -> PathBuf {
    let relative = strip_container_prefix(container, strip_path_prefix);
    let mut path = if by_category {
        output_dir.join(&relative)
    } else {
        let file_name = Path::new(&relative)
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("timeline.playable"));
        output_dir.join(export_path).join(file_name)
    };
    path.set_extension("json");
    path
}

impl NativeSemanticExportPathState {
    pub(super) fn with_registry(registry: NativeSemanticExportPathRegistry) -> Self {
        Self {
            registry,
            ..Self::default()
        }
    }

    pub(super) fn claim_payload(
        &mut self,
        path: PathBuf,
        asset: &UnityAssetInfo,
        read_output: &UnityObjectReadOutput,
    ) -> NativeSemanticPathClaim {
        self.claim_with_signature(path, asset, payload_signature(&read_output.payload))
    }

    pub(super) fn claim_generated_payload(
        &mut self,
        path: PathBuf,
        asset: &UnityAssetInfo,
        payload: &[u8],
    ) -> NativeSemanticPathClaim {
        self.claim_with_signature(path, asset, native_payload_signature(payload))
    }

    fn claim_with_signature(
        &mut self,
        path: PathBuf,
        asset: &UnityAssetInfo,
        signature: NativePayloadSignature,
    ) -> NativeSemanticPathClaim {
        let mut claims = self.registry.claims.lock().unwrap();
        let mut ordinal = 1usize;
        loop {
            let candidate = semantic_duplicate_path(&path, ordinal);
            if let Some(existing_claim) = claims.get(&candidate) {
                if signature == existing_claim.signature {
                    return NativeSemanticPathClaim::Duplicate {
                        existing: candidate,
                    };
                }
            }
            if !claims.contains_key(&candidate) {
                claims.insert(candidate.clone(), NativeSemanticExportClaim { signature });
                if ordinal > 1 {
                    debug!(
                        asset_type = asset.asset_type.as_deref().unwrap_or(""),
                        name = asset.name.as_deref().unwrap_or(""),
                        container = asset.container.as_deref().unwrap_or(""),
                        output_path = %candidate.display(),
                        "semantic export path collision; using deterministic duplicate suffix"
                    );
                }
                return NativeSemanticPathClaim::Claimed(candidate);
            }
            ordinal += 1;
        }
    }
}

pub(super) fn native_payload_signature(payload: &[u8]) -> NativePayloadSignature {
    NativePayloadSignature {
        payload_len: payload.len(),
        payload_fingerprint: native_payload_fingerprint(payload),
    }
}

/// The content signature of whatever the read produced.
///
/// Kept here rather than on the payload type so `types` stays a leaf: it would
/// otherwise have to import this module, which imports it.
pub(super) fn payload_signature(payload: &NativeObjectPayload) -> NativePayloadSignature {
    match payload {
        NativeObjectPayload::Bytes(bytes) => native_payload_signature(bytes),
        NativeObjectPayload::Rgba(surface) => {
            native_surface_signature(surface.width, surface.height, &surface.pixels)
        }
    }
}

/// Signs a decoded surface without serialising it.
///
/// The dimensions go in alongside the pixels because the `HARUKI_RGBAIR_V1`
/// header used to carry them: two images with the same bytes at different
/// dimensions must not sign the same.
pub(super) fn native_surface_signature(
    width: u32,
    height: u32,
    pixels: &[u8],
) -> NativePayloadSignature {
    let mut fingerprint = native_payload_fingerprint(pixels);
    fingerprint[0] ^= u64::from(width) << 32 | u64::from(height);
    NativePayloadSignature {
        payload_len: pixels.len(),
        payload_fingerprint: fingerprint,
    }
}

/// 128-bit content fingerprint used to tell a byte-identical duplicate apart
/// from a different payload that lands on the same semantic path.
///
/// This runs over every exported payload, including the full RGBA image
/// intermediate, so it is squarely on the hot path: an image rule pushes tens
/// of gigabytes through it. Two independently seeded aHash passes are used
/// instead of a hand-rolled byte-at-a-time chain, which measured 39 CPU-seconds
/// (32% of the whole `^character/member_cutout` rule) on 34.9 GB of payload.
///
/// The seeds are fixed so a run is reproducible. The values never leave the
/// process — the claim map lives for exactly one job — so they do not need to
/// stay stable across releases.
pub(super) fn native_payload_fingerprint(payload: &[u8]) -> [u64; 2] {
    const SEEDS_LEFT: [u64; 4] = [
        0xcbf2_9ce4_8422_2325,
        0x0000_0100_0000_01b3,
        0x9e37_79b9_7f4a_7c15,
        0xff51_afd7_ed55_8ccd,
    ];
    const SEEDS_RIGHT: [u64; 4] = [
        0x8422_2325_cbf2_9ce4,
        0xc4ce_b9fe_1a85_ec53,
        0x1656_67b1_9e37_79f9,
        0x2545_f491_4f6c_dd1d,
    ];

    fn hash_with(seeds: [u64; 4], payload: &[u8]) -> u64 {
        let state = ahash::RandomState::with_seeds(seeds[0], seeds[1], seeds[2], seeds[3]);
        let mut hasher = state.build_hasher();
        hasher.write(payload);
        hasher.finish()
    }

    [
        hash_with(SEEDS_LEFT, payload),
        hash_with(SEEDS_RIGHT, payload),
    ]
}

pub(super) fn semantic_duplicate_path(path: &Path, ordinal: usize) -> PathBuf {
    if ordinal <= 1 {
        return path.to_path_buf();
    }

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    let extension = path.extension().and_then(|value| value.to_str());
    let stem = format!("{stem}__dup{ordinal}");
    match extension {
        Some(extension) if !extension.is_empty() => parent.join(format!("{stem}.{extension}")),
        _ => parent.join(stem),
    }
}

pub(super) fn write_assetstudio_export_manifest_entry(
    output_dir: &Path,
    target: &Path,
    asset: &UnityAssetInfo,
    read_output: &UnityObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    let manifest_root = output_dir.to_path_buf();
    std::fs::create_dir_all(&manifest_root).map_err(|source| ExportPipelineError::Io {
        path: manifest_root.clone(),
        source,
    })?;
    let manifest_path = manifest_root.join(".assetstudio-export-manifest.jsonl");
    let public_target = assetstudio_manifest_public_target(target, read_output)?;
    let path = public_target
        .strip_prefix(&manifest_root)
        .unwrap_or(&public_target)
        .to_string_lossy()
        .replace('\\', "/");
    let entry = NativeAssetStudioExportManifestEntry {
        path,
        asset_type: asset.asset_type.clone(),
        name: asset.name.clone(),
        container: asset.container.clone(),
        payload_kind: read_output.response.payload_kind.clone(),
        suggested_extension: manifest_suggested_extension(&public_target, read_output),
    };
    let line = sonic_rs::to_string(&entry)
        .map_err(|source| ExportPipelineError::JsonSerialize { source })?;
    let locks = ASSETSTUDIO_MANIFEST_APPEND_LOCKS.get_or_init(|| {
        (0..ASSETSTUDIO_MANIFEST_LOCKS)
            .map(|_| Mutex::new(()))
            .collect()
    });
    let lock_index = manifest_lock_index(&manifest_path);
    let _guard = locks[lock_index]
        .lock()
        .map_err(|source| ExportPipelineError::UnityRs {
            message: format!("assetstudio export manifest lock poisoned: {source}"),
        })?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .map_err(|source| ExportPipelineError::Io {
            path: manifest_path.clone(),
            source,
        })?;
    writeln!(file, "{line}").map_err(|source| ExportPipelineError::Io {
        path: manifest_path,
        source,
    })?;
    Ok(())
}

pub(super) fn assetstudio_manifest_public_target(
    target: &Path,
    read_output: &UnityObjectReadOutput,
) -> Result<PathBuf, ExportPipelineError> {
    match read_output.response.payload_kind.as_deref() {
        Some("image_bmp") | Some("image_raw_rgba") => {
            if target
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("bmp"))
            {
                Ok(target.with_extension("png"))
            } else {
                Ok(target.to_path_buf())
            }
        }
        Some("animator_bundle_fbx") => {
            let entries = parse_payload_bundle_borrowed(read_output.payload.bytes())?;
            let entry_name = entries
                .iter()
                .map(|(name, _)| name.as_str())
                .find(|name| {
                    Path::new(name)
                        .extension()
                        .and_then(|extension| extension.to_str())
                        .is_some_and(|extension| extension.eq_ignore_ascii_case("fbx"))
                })
                .or_else(|| entries.first().map(|(name, _)| name.as_str()))
                .unwrap_or("payload.bin");
            Ok(payload_bundle_entry_target(target, entry_name))
        }
        _ => Ok(target.to_path_buf()),
    }
}

pub(super) fn manifest_suggested_extension(
    public_target: &Path,
    read_output: &UnityObjectReadOutput,
) -> Option<String> {
    public_target
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.trim().is_empty())
        .map(|extension| format!(".{extension}"))
        .or_else(|| read_output.response.suggested_extension.clone())
}

pub(super) fn manifest_lock_index(path: &Path) -> usize {
    let mut hash = 0usize;
    for byte in path.to_string_lossy().bytes() {
        hash = hash.wrapping_mul(131).wrapping_add(byte as usize);
    }
    hash % ASSETSTUDIO_MANIFEST_LOCKS
}

pub(super) fn text_asset_public_bytes_target(
    target: &Path,
    asset: &UnityAssetInfo,
) -> Option<PathBuf> {
    if asset.asset_type.as_deref() != Some("TextAsset") {
        return None;
    }
    let file_name = target.file_name()?.to_str()?;
    if let Some(media_name) = file_name
        .strip_suffix(".acb.bytes")
        .map(|stem| format!("{stem}.acb"))
        .or_else(|| {
            file_name
                .strip_suffix(".usm.bytes")
                .map(|stem| format!("{stem}.usm"))
        })
    {
        return Some(target.with_file_name(media_name));
    }

    let stem = file_name.strip_suffix(".bytes")?;
    if text_asset_is_music_score(target, asset) {
        Some(target.with_file_name(format!("{stem}.txt")))
    } else {
        Some(target.with_file_name(stem))
    }
}

pub(super) fn text_asset_is_music_score(target: &Path, asset: &UnityAssetInfo) -> bool {
    let target_path = target.to_string_lossy().replace('\\', "/");
    let container_path = asset.container.as_deref().unwrap_or("").replace('\\', "/");
    target_path.contains("/music/music_score/") || container_path.contains("/music/music_score/")
}

pub(super) fn is_text_asset_acb_target(asset: &UnityAssetInfo, target: &Path) -> bool {
    asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("acb"))
}

pub(super) fn is_text_asset_decoded_usm_target(
    asset: &UnityAssetInfo,
    target: &Path,
    region: &RegionConfig,
) -> bool {
    region.export.usm.export
        && region.export.usm.decode
        && asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("usm"))
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

/// Encodes and writes one image where it was decoded.
///
/// This used to push the decoded RGBA onto `pending_image_writes` and encode it
/// later, in a stage the bundle reached only after waiting for a post-process
/// slot. Measured on 48 cores over 16 844 JP bundles, that queue held the
/// dominant share of a 23 GB peak RSS: an RGBA surface is 2.5-4x its encoded
/// form and up to `download + post_process * 2` bundles' worth were resident at
/// once. Encoding here bounds live pixel data by the number of bundles actually
/// being read instead of by the depth of a queue.
pub(super) fn write_native_image_payload_final_files_now(
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
pub(super) fn write_native_image_payload_final_files(
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
pub(super) fn write_native_image_payload_final_files_with_backend(
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

fn image_payload_scratch_bytes(
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

fn write_native_image_payload_final_files_with_limits(
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
fn encode_image_outputs(
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
pub(super) fn write_native_image_surface_final_files_now(
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
pub(crate) fn flat_pipeline_enabled() -> bool {
    static FLAT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAT.get_or_init(|| {
        std::env::var("HARUKI_FLAT_PIPELINE")
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    })
}

pub(super) fn native_image_surrogate_public_target(
    target: &Path,
    region: &RegionConfig,
) -> PathBuf {
    if !target
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case(UNITY_ENGINE_IMAGE_SURROGATE_FORMAT)
        })
    {
        return target.to_path_buf();
    }
    let format = region
        .export
        .images
        .output_formats()
        .into_iter()
        .next()
        .unwrap_or(ImageOutputFormat::Png);
    target.with_extension(image_format_extension(format))
}

pub(super) fn write_payload_bundle(
    target: &Path,
    payload: &[u8],
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let entries = parse_payload_bundle_borrowed(payload)?;
    let mut written_files = Vec::with_capacity(entries.len());
    for (name, bytes) in entries {
        let entry_target = payload_bundle_entry_target(target, &name);
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        write_native_payload_file(&entry_target, bytes)?;
        written_files.push(entry_target);
    }
    Ok(written_files)
}

pub(super) fn remove_byte_identical_semantic_duplicates(
    target: &Path,
    path_registry: &NativeSemanticExportPathRegistry,
) -> Result<usize, ExportPipelineError> {
    let Some(target_stem) = target.file_stem().and_then(|value| value.to_str()) else {
        return Ok(0);
    };
    if semantic_duplicate_ordinal(target_stem).is_some() {
        return Ok(0);
    }
    let claims = path_registry.claims.lock().unwrap();
    let mut removed = 0usize;
    let mut ordinal = 2usize;
    loop {
        let duplicate = semantic_duplicate_path(target, ordinal);
        let metadata = match std::fs::symlink_metadata(&duplicate) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => break,
            Err(source) => {
                return Err(ExportPipelineError::Io {
                    path: duplicate,
                    source,
                });
            }
        };
        if claims.contains_key(&duplicate) {
            ordinal += 1;
            continue;
        }
        let file_type = metadata.file_type();
        if !file_type.is_file() || !files_are_byte_identical(target, &duplicate)? {
            ordinal += 1;
            continue;
        }
        std::fs::remove_file(&duplicate).map_err(|source| ExportPipelineError::Io {
            path: duplicate.clone(),
            source,
        })?;
        debug!(
            output_path = %target.display(),
            duplicate_path = %duplicate.display(),
            "removed byte-identical legacy semantic duplicate"
        );
        removed += 1;
        ordinal += 1;
    }
    Ok(removed)
}

pub(super) fn semantic_duplicate_ordinal(stem: &str) -> Option<usize> {
    stem.rsplit_once("__dup")
        .and_then(|(_, ordinal)| semantic_duplicate_ordinal_digits(ordinal))
}

pub(super) fn semantic_duplicate_ordinal_digits(value: &str) -> Option<usize> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse::<usize>().ok())
        .flatten()
        .filter(|ordinal| *ordinal > 1)
}

pub(super) fn files_are_byte_identical(
    left: &Path,
    right: &Path,
) -> Result<bool, ExportPipelineError> {
    let left_file = std::fs::File::open(left).map_err(|source| ExportPipelineError::Io {
        path: left.to_path_buf(),
        source,
    })?;
    let right_file = std::fs::File::open(right).map_err(|source| ExportPipelineError::Io {
        path: right.to_path_buf(),
        source,
    })?;
    let left_len = left_file
        .metadata()
        .map_err(|source| ExportPipelineError::Io {
            path: left.to_path_buf(),
            source,
        })?
        .len();
    let right_len = right_file
        .metadata()
        .map_err(|source| ExportPipelineError::Io {
            path: right.to_path_buf(),
            source,
        })?
        .len();
    if left_len != right_len {
        return Ok(false);
    }

    let mut left_reader = std::io::BufReader::new(left_file);
    let mut right_reader = std::io::BufReader::new(right_file);
    let mut left_buffer = [0u8; 64 * 1024];
    let mut right_buffer = [0u8; 64 * 1024];
    loop {
        let left_read =
            left_reader
                .read(&mut left_buffer)
                .map_err(|source| ExportPipelineError::Io {
                    path: left.to_path_buf(),
                    source,
                })?;
        let right_read =
            right_reader
                .read(&mut right_buffer)
                .map_err(|source| ExportPipelineError::Io {
                    path: right.to_path_buf(),
                    source,
                })?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

pub(super) fn write_native_image_payload_bundle_final_files_now(
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

pub(super) fn payload_bundle_entry_target(target: &Path, entry_name: &str) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new(""));
    let stem = target
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    parent.join(stem).join(safe_payload_bundle_path(entry_name))
}

#[cfg(test)]
pub(super) fn parse_payload_bundle(
    payload: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, ExportPipelineError> {
    Ok(parse_payload_bundle_borrowed(payload)?
        .into_iter()
        .map(|(name, bytes)| (name, bytes.to_vec()))
        .collect())
}

/// Bundle parse that returns refcounted sub-slices of the backing buffer instead
/// of borrowed slices, so entries can outlive the parse without a heap copy.
pub(super) fn parse_payload_bundle_shared(
    payload: &bytes::Bytes,
) -> Result<Vec<(String, bytes::Bytes)>, ExportPipelineError> {
    let base = payload.as_ptr() as usize;
    Ok(parse_payload_bundle_borrowed(payload)?
        .into_iter()
        .map(|(name, slice)| {
            // Sub-slices returned by the borrowed parser always point inside
            // `payload`, so the offset arithmetic cannot underflow or overflow.
            let start = slice.as_ptr() as usize - base;
            (name, payload.slice(start..start + slice.len()))
        })
        .collect())
}

pub(super) fn parse_payload_bundle_borrowed(
    payload: &[u8],
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut cursor = 0usize;
    if payload.len() >= 4
        && u32::from_le_bytes(payload[0..4].try_into().unwrap())
            == UNITY_ENGINE_PAYLOAD_BUNDLE_V2_MAGIC
    {
        cursor += 4;
        let version = read_bundle_u16(payload, &mut cursor)?;
        if version != UNITY_ENGINE_PAYLOAD_BUNDLE_V2_VERSION {
            return Err(ExportPipelineError::UnityRs {
                message: format!("native payload bundle has unsupported version {version}"),
            });
        }
        let header_len = read_bundle_u16(payload, &mut cursor)? as usize;
        if header_len < UNITY_ENGINE_PAYLOAD_BUNDLE_V2_HEADER_LEN || header_len > payload.len() {
            return Err(ExportPipelineError::UnityRs {
                message: format!("native payload bundle has invalid header length {header_len}"),
            });
        }
        let count = read_bundle_u32(payload, &mut cursor)? as usize;
        let expected_payload_data_bytes = read_bundle_u64(payload, &mut cursor)?;
        cursor = header_len;
        return parse_payload_bundle_interleaved_entries(
            payload,
            cursor,
            count,
            Some(expected_payload_data_bytes),
        );
    }

    if payload.starts_with(UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC) {
        cursor += UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC.len();
        let count = read_bundle_u32(payload, &mut cursor)? as usize;
        return parse_payload_bundle_grouped_entries(payload, cursor, count);
    }

    Err(ExportPipelineError::UnityRs {
        message: "native payload bundle has invalid magic".to_string(),
    })
}

pub(super) fn parse_payload_bundle_interleaved_entries(
    payload: &[u8],
    mut cursor: usize,
    count: usize,
    expected_payload_data_bytes: Option<u64>,
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut entries = Vec::with_capacity(count);
    let mut observed_payload_data_bytes = 0u64;
    for _ in 0..count {
        let name_len = read_bundle_u32(payload, &mut cursor)? as usize;
        let data_len = read_bundle_u64(payload, &mut cursor)?;
        let data_len_usize =
            usize::try_from(data_len).map_err(|_| ExportPipelineError::UnityRs {
                message: "native payload bundle entry data is too large".to_string(),
            })?;
        if payload.len().saturating_sub(cursor) < name_len {
            return Err(ExportPipelineError::UnityRs {
                message: "native payload bundle has truncated entry name".to_string(),
            });
        }
        let name = std::str::from_utf8(&payload[cursor..cursor + name_len])
            .map_err(|source| ExportPipelineError::UnityRs {
                message: format!("native payload bundle entry name is not utf-8: {source}"),
            })?
            .to_string();
        cursor += name_len;
        if payload.len().saturating_sub(cursor) < data_len_usize {
            return Err(ExportPipelineError::UnityRs {
                message: "native payload bundle has truncated entry data".to_string(),
            });
        }
        entries.push((name, &payload[cursor..cursor + data_len_usize]));
        cursor += data_len_usize;
        observed_payload_data_bytes = observed_payload_data_bytes.saturating_add(data_len);
    }
    finish_payload_bundle_parse(
        payload,
        cursor,
        observed_payload_data_bytes,
        expected_payload_data_bytes,
    )?;
    Ok(entries)
}

pub(super) fn parse_payload_bundle_grouped_entries(
    payload: &[u8],
    mut cursor: usize,
    count: usize,
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut headers = Vec::with_capacity(count);
    let mut observed_payload_data_bytes = 0u64;
    for _ in 0..count {
        let name_len = read_bundle_u32(payload, &mut cursor)? as usize;
        let data_len = read_bundle_u64(payload, &mut cursor)?;
        if payload.len().saturating_sub(cursor) < name_len {
            return Err(ExportPipelineError::UnityRs {
                message: "native payload bundle has truncated entry name".to_string(),
            });
        }
        let name = std::str::from_utf8(&payload[cursor..cursor + name_len])
            .map_err(|source| ExportPipelineError::UnityRs {
                message: format!("native payload bundle entry name is not utf-8: {source}"),
            })?
            .to_string();
        cursor += name_len;
        headers.push((name, data_len));
        observed_payload_data_bytes = observed_payload_data_bytes.saturating_add(data_len);
    }

    let mut entries = Vec::with_capacity(count);
    for (name, data_len) in headers {
        let data_len_usize =
            usize::try_from(data_len).map_err(|_| ExportPipelineError::UnityRs {
                message: "native payload bundle entry data is too large".to_string(),
            })?;
        if payload.len().saturating_sub(cursor) < data_len_usize {
            return Err(ExportPipelineError::UnityRs {
                message: "native payload bundle has truncated entry data".to_string(),
            });
        }
        entries.push((name, &payload[cursor..cursor + data_len_usize]));
        cursor += data_len_usize;
    }

    finish_payload_bundle_parse(payload, cursor, observed_payload_data_bytes, None)?;
    Ok(entries)
}

pub(super) fn finish_payload_bundle_parse(
    payload: &[u8],
    cursor: usize,
    observed_payload_data_bytes: u64,
    expected_payload_data_bytes: Option<u64>,
) -> Result<(), ExportPipelineError> {
    if cursor != payload.len() {
        return Err(ExportPipelineError::UnityRs {
            message: format!(
                "native payload bundle has {} trailing byte(s)",
                payload.len().saturating_sub(cursor)
            ),
        });
    }
    if let Some(expected_payload_data_bytes) = expected_payload_data_bytes {
        if observed_payload_data_bytes != expected_payload_data_bytes {
            return Err(ExportPipelineError::UnityRs {
                message: format!(
                    "native payload bundle data byte count mismatch: expected {expected_payload_data_bytes}, got {observed_payload_data_bytes}"
                ),
            });
        }
    }
    Ok(())
}

pub(super) fn read_bundle_u32(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u32, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 4 {
        return Err(ExportPipelineError::UnityRs {
            message: "native payload bundle has truncated u32".to_string(),
        });
    }
    let value = u32::from_le_bytes(payload[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

pub(super) fn read_bundle_u16(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u16, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 2 {
        return Err(ExportPipelineError::UnityRs {
            message: "native payload bundle has truncated u16".to_string(),
        });
    }
    let value = u16::from_le_bytes(payload[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(value)
}

pub(super) fn read_bundle_u64(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u64, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 8 {
        return Err(ExportPipelineError::UnityRs {
            message: "native payload bundle has truncated u64".to_string(),
        });
    }
    let value = u64::from_le_bytes(payload[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}
