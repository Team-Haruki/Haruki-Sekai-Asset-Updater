//! The container format used when one object yields many payloads.

use std::path::{Path, PathBuf};

use crate::ExportPipelineError;

use super::super::paths::safe_payload_bundle_path;
use super::super::types::{
    UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC, UNITY_ENGINE_PAYLOAD_BUNDLE_V2_HEADER_LEN,
    UNITY_ENGINE_PAYLOAD_BUNDLE_V2_MAGIC, UNITY_ENGINE_PAYLOAD_BUNDLE_V2_VERSION,
};
use super::write_native_payload_file;

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
pub(crate) fn parse_payload_bundle(
    payload: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, ExportPipelineError> {
    Ok(parse_payload_bundle_borrowed(payload)?
        .into_iter()
        .map(|(name, bytes)| (name, bytes.to_vec()))
        .collect())
}

/// Bundle parse that returns refcounted sub-slices of the backing buffer instead
/// of borrowed slices, so entries can outlive the parse without a heap copy.
pub(crate) fn parse_payload_bundle_shared(
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

pub(crate) fn parse_payload_bundle_borrowed(
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
