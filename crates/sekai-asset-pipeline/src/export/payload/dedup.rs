//! Claiming a semantic output path, and telling a duplicate from a collision.

use std::hash::{BuildHasher, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};

use tracing::debug;

use crate::ExportPipelineError;

use super::super::types::{
    NativeObjectPayload, NativePayloadSignature, NativeSemanticExportClaim,
    NativeSemanticExportPathRegistry, NativeSemanticExportPathState, NativeSemanticPathClaim,
    UnityAssetInfo, UnityObjectReadOutput,
};

impl NativeSemanticExportPathState {
    pub(crate) fn with_registry(registry: NativeSemanticExportPathRegistry) -> Self {
        Self {
            registry,
            ..Self::default()
        }
    }

    pub(crate) fn claim_payload(
        &mut self,
        path: PathBuf,
        asset: &UnityAssetInfo,
        read_output: &UnityObjectReadOutput,
    ) -> NativeSemanticPathClaim {
        self.claim_with_signature(path, asset, payload_signature(&read_output.payload))
    }

    pub(crate) fn claim_generated_payload(
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

pub(crate) fn native_payload_signature(payload: &[u8]) -> NativePayloadSignature {
    NativePayloadSignature {
        payload_len: payload.len(),
        payload_fingerprint: native_payload_fingerprint(payload),
    }
}

/// The content signature of whatever the read produced.
///
/// Kept here rather than on the payload type so `types` stays a leaf: it would
/// otherwise have to import this module, which imports it.
pub(crate) fn payload_signature(payload: &NativeObjectPayload) -> NativePayloadSignature {
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

pub(crate) fn remove_byte_identical_semantic_duplicates(
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
