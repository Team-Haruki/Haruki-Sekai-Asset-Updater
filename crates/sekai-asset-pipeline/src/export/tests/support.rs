//! Fixtures shared by the export tests.

use std::io::Cursor;
use std::path::{Path, PathBuf};

pub(super) use crate::test_support::processing_pipeline_options;

pub(super) fn sample_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("HARUKI_CODEC_SAMPLE_DIR")
        .map(PathBuf::from)
        .map(|dir| dir.join(name))
}

pub(super) fn synthetic_hca() -> Vec<u8> {
    let sample_rate = 44_100;
    let channels = 1;
    let samples = (0..4_096)
        .map(|index| {
            let time = index as f32 / sample_rate as f32;
            (time * 440.0 * std::f32::consts::TAU).sin() * 0.25
        })
        .collect::<Vec<_>>();
    let mut encoder = cridecoder::HcaEncoder::new(cridecoder::HcaEncoderConfig {
        channels,
        sample_rate,
        bitrate: 64_000,
        ..cridecoder::HcaEncoderConfig::default()
    })
    .unwrap();
    let mut encoded = Vec::new();
    encoder
        .encode(&samples, &mut Cursor::new(&mut encoded))
        .unwrap();
    encoded
}

pub(super) fn synthetic_acb(prefix: &str, track_count: usize) -> Vec<u8> {
    let hca = synthetic_hca();
    let mut builder = cridecoder::AcbBuilder::new();
    for cue_id in 0..track_count {
        builder.add_track(cridecoder::TrackInput::new(
            format!("{prefix}_{cue_id}"),
            cue_id as u32,
            hca.clone(),
        ));
    }
    let mut encoded = Vec::new();
    builder.build(&mut Cursor::new(&mut encoded), None).unwrap();
    encoded
}

pub(super) fn synthetic_usm(name: &str) -> Vec<u8> {
    let video = vec![0x00, 0x00, 0x01, 0xb3, 0x2d, 0x02, 0x40, 0x33];
    let builder = cridecoder::UsmBuilder::new(name).video(video);
    let mut encoded = Cursor::new(Vec::new());
    builder.build(&mut encoded).unwrap();
    encoded.into_inner()
}

pub(super) fn fake_ffmpeg_script(dir: &Path) -> PathBuf {
    let path = dir.join("fake_ffmpeg.sh");
    std::fs::write(
        &path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\n: > \"$out\"\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        // The generated script is only for this test process; do not expose it to other users.
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
    }
    path
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
