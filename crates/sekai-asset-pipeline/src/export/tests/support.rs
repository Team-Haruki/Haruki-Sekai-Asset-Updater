//! Fixtures shared by the export tests.

use std::path::PathBuf;

pub(super) use crate::test_support::processing_pipeline_options;

pub(super) fn sample_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("HARUKI_CODEC_SAMPLE_DIR")
        .map(PathBuf::from)
        .map(|dir| dir.join(name))
}

pub(super) fn make_native_rgba_ir_payload(width: u32, height: u32, pixels: &[u8]) -> Vec<u8> {
    let stride = width * 4;
    let mut payload = Vec::new();
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_RGBA_IR_MAGIC);
    payload.extend_from_slice(&width.to_le_bytes());
    payload.extend_from_slice(&height.to_le_bytes());
    payload.extend_from_slice(&stride.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(pixels);
    payload
}
