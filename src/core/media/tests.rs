use std::fs;
use std::path::PathBuf;

use tempfile::tempdir;

#[cfg(feature = "media-ffi")]
use super::convert_wav_bytes_to_mp3_with_backend;
use super::{
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, FrameRate,
};
use crate::core::config::MediaBackend;
use crate::core::config::RetryConfig;

#[test]
fn frame_rate_formats_like_go_helper() {
    assert_eq!(
        FrameRate {
            numerator: 30000,
            denominator: 1001
        }
        .to_string(),
        "30000/1001"
    );
    assert_eq!(
        FrameRate {
            numerator: 60,
            denominator: 1
        }
        .to_string(),
        "60"
    );
}

#[test]
fn convert_usm_to_mp4_builds_ffmpeg_command() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg.sh");
    fs::write(&input, b"dummy").unwrap();
    fs::write(
        &script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(output.exists());
}

#[test]
fn convert_m2v_to_mp4_removes_original_when_requested() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.m2v");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg.sh");
    fs::write(&input, b"dummy").unwrap();
    fs::write(
        &script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_m2v_to_mp4_with_backend(
            &input,
            &output,
            true,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            Some(FrameRate {
                numerator: 30000,
                denominator: 1001,
            }),
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(!input.exists());
    assert!(output.exists());
}

#[test]
fn convert_usm_to_mp4_retries_after_command_failure() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg_retry.sh");
    let marker_path = dir.path().join("attempts.txt");
    fs::write(&input, b"dummy").unwrap();
    fs::write(
            &script_path,
            format!(
                "#!/bin/sh\nset -eu\nMARKER=\"{}\"\nif [ ! -f \"$MARKER\" ]; then\n  echo first > \"$MARKER\"\n  echo transient >&2\n  exit 1\nfi\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
                marker_path.display()
            ),
        )
        .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            &RetryConfig {
                attempts: 2,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(marker_path.exists());
    assert!(output.exists());
}

#[test]
fn auto_backend_falls_back_to_cli() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg.sh");
    fs::write(&input, b"dummy").unwrap();
    fs::write(
        &script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Auto,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(output.exists());
}

#[test]
fn cli_bytes_input_uses_system_temp_dir() {
    let dir = tempdir().unwrap();
    let output_dir = dir.path().join("exports");
    fs::create_dir_all(&output_dir).unwrap();
    let output = output_dir.join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg.sh");
    let input_log = dir.path().join("input_path.txt");
    fs::write(
            &script_path,
            format!(
                "#!/bin/sh\nset -eu\ninput=\"\"\nout=\"\"\nprev=\"\"\nfor arg in \"$@\"; do\n  if [ \"$prev\" = \"-i\" ]; then input=\"$arg\"; fi\n  out=\"$arg\"\n  prev=\"$arg\"\ndone\nprintf '%s' \"$input\" > \"{}\"\n: > \"$out\"\n",
                input_log.display()
            ),
        )
        .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).unwrap();
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_m2v_bytes_to_mp4_with_backend(
            b"dummy m2v",
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            None,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    let temp_input = PathBuf::from(fs::read_to_string(&input_log).unwrap());
    assert!(output.exists());
    assert!(!temp_input.exists());
    assert!(!temp_input.starts_with(&output_dir));
}

#[cfg(feature = "media-ffi")]
#[test]
fn ffi_backend_transcodes_wav_bytes_to_mp3() {
    let dir = tempdir().unwrap();
    let output = dir.path().join("sample.mp3");
    let wav = test_wav_bytes();

    convert_wav_bytes_to_mp3_with_backend(
        &wav,
        &output,
        "ffmpeg",
        MediaBackend::Ffi,
        &RetryConfig {
            attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
    )
    .unwrap();

    assert!(fs::metadata(output).unwrap().len() > 0);
}

#[cfg(feature = "media-ffi")]
fn test_wav_bytes() -> Vec<u8> {
    let sample_rate = 44_100_u32;
    let channels = 1_u16;
    let bits_per_sample = 16_u16;
    let samples = sample_rate / 10;
    let block_align = channels * bits_per_sample / 8;
    let byte_rate = sample_rate * u32::from(block_align);
    let data_len = samples * u32::from(block_align);
    let mut wav = Vec::with_capacity(44 + data_len as usize);

    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&channels.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&bits_per_sample.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());

    for index in 0..samples {
        let t = index as f32 / sample_rate as f32;
        let sample = (t * 440.0 * std::f32::consts::TAU).sin();
        let value = (sample * i16::MAX as f32 * 0.25) as i16;
        wav.extend_from_slice(&value.to_le_bytes());
    }
    wav
}
