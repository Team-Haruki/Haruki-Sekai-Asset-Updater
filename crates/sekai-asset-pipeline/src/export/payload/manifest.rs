//! The per-export manifest written beside the exported files.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::ExportPipelineError;

use super::super::types::{
    NativeAssetStudioExportManifestEntry, UnityAssetInfo, UnityObjectReadOutput,
    ASSETSTUDIO_MANIFEST_APPEND_LOCKS, ASSETSTUDIO_MANIFEST_LOCKS,
};
use super::bundle::{parse_payload_bundle_borrowed, payload_bundle_entry_target};

pub(crate) fn write_assetstudio_export_manifest_entry(
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
