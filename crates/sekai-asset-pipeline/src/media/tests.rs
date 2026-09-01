use std::fs;
use std::io::Write;
use std::path::PathBuf;

use tempfile::tempdir;

use super::{
    convert_hca_bytes_to_flac_with_backend, convert_hca_bytes_to_mp3_with_backend,
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, convert_wav_bytes_to_flac_with_backend,
    convert_wav_bytes_to_mp3_with_backend, convert_wav_to_flac_with_backend,
    convert_wav_to_mp3_with_backend, is_retryable_command_error, FrameRate,
};
use crate::{ExportPipelineError, MediaBackend, RetryOptions as RetryConfig};

fn write_executable_script(path: &std::path::Path, script: impl AsRef<[u8]>) {
    let mut file = fs::File::create(path).unwrap();
    file.write_all(script.as_ref()).unwrap();
    file.sync_all().unwrap();
    drop(file);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).unwrap().permissions();
        // The generated script is only for this test process; do not expose it to other users.
        perms.set_mode(0o700);
        fs::set_permissions(path, perms).unwrap();
    }
}

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
    write_executable_script(
        &script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            &RetryConfig {
                attempts: 4,
                initial_backoff_ms: 5,
                max_backoff_ms: 20,
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
    write_executable_script(
        &script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    );

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
                attempts: 4,
                initial_backoff_ms: 5,
                max_backoff_ms: 20,
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
    write_executable_script(
        &script_path,
        format!(
            "#!/bin/sh\nset -eu\nMARKER=\"{}\"\nif [ ! -f \"$MARKER\" ]; then\n  echo first > \"$MARKER\"\n  echo 'Connection reset by peer' >&2\n  exit 1\nfi\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
            marker_path.display()
        ),
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            &RetryConfig {
                attempts: 4,
                initial_backoff_ms: 1,
                max_backoff_ms: 5,
            },
        ))
        .unwrap();

    assert!(marker_path.exists());
    assert!(output.exists());
}

#[cfg(unix)]
#[test]
fn executable_file_busy_spawn_errors_are_retryable() {
    let error = ExportPipelineError::Spawn {
        program: "ffmpeg".to_string(),
        source: std::io::Error::from_raw_os_error(libc::ETXTBSY),
    };

    assert!(is_retryable_command_error(&error));
}

#[test]
fn convert_usm_to_mp4_does_not_retry_deterministic_failure() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg_fatal.sh");
    let marker_path = dir.path().join("attempts.txt");
    fs::write(&input, b"dummy").unwrap();
    // Always fails with a deterministic (non-transient) error, recording each invocation.
    write_executable_script(
        &script_path,
        format!(
            "#!/bin/sh\nset -eu\necho x >> \"{}\"\necho 'Invalid data found when processing input' >&2\nexit 1\n",
            marker_path.display()
        ),
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(convert_usm_to_mp4_with_backend(
        &input,
        &output,
        &script_path.to_string_lossy(),
        MediaBackend::Cli,
        &RetryConfig {
            attempts: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
    ));

    assert!(result.is_err());
    // A deterministic failure must run exactly once (no wasted retries).
    let attempts = fs::read_to_string(&marker_path).unwrap();
    assert_eq!(attempts.lines().count(), 1);
    assert!(!output.exists());
}

#[test]
fn auto_backend_falls_back_to_cli() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg.sh");
    fs::write(&input, b"dummy").unwrap();
    write_executable_script(
        &script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Auto,
            &RetryConfig {
                attempts: 4,
                initial_backoff_ms: 5,
                max_backoff_ms: 20,
            },
        ))
        .unwrap();

    assert!(output.exists());
}

#[test]
fn cli_audio_backends_cover_files_bytes_and_hca_inputs() {
    let dir = tempdir().unwrap();
    let wav = test_wav_bytes();
    let wav_file = dir.path().join("input.wav");
    fs::write(&wav_file, &wav).unwrap();
    let ffmpeg = dir.path().join("fake_ffmpeg.sh");
    write_executable_script(
        &ffmpeg,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\n: > \"$out\"\n",
    );
    let retry = RetryConfig {
        attempts: 4,
        initial_backoff_ms: 5,
        max_backoff_ms: 20,
    };
    let ffmpeg = ffmpeg.to_string_lossy();

    convert_wav_to_mp3_with_backend(
        &wav_file,
        &dir.path().join("file.mp3"),
        &ffmpeg,
        MediaBackend::Cli,
        &retry,
    )
    .unwrap();
    convert_wav_bytes_to_mp3_with_backend(
        &wav,
        &dir.path().join("bytes.mp3"),
        &ffmpeg,
        MediaBackend::Cli,
        &retry,
    )
    .unwrap();
    convert_wav_to_flac_with_backend(
        &wav_file,
        &dir.path().join("file.flac"),
        &ffmpeg,
        MediaBackend::Cli,
        &retry,
    )
    .unwrap();
    convert_wav_bytes_to_flac_with_backend(
        &wav,
        &dir.path().join("bytes.flac"),
        &ffmpeg,
        MediaBackend::Cli,
        &retry,
    )
    .unwrap();

    let hca = synthetic_hca();
    convert_hca_bytes_to_mp3_with_backend(
        &hca,
        &dir.path().join("hca.mp3"),
        &ffmpeg,
        MediaBackend::Cli,
        &retry,
    )
    .unwrap();
    convert_hca_bytes_to_flac_with_backend(
        &hca,
        &dir.path().join("hca.flac"),
        &ffmpeg,
        MediaBackend::Cli,
        &retry,
    )
    .unwrap();

    for name in [
        "file.mp3",
        "bytes.mp3",
        "file.flac",
        "bytes.flac",
        "hca.mp3",
        "hca.flac",
    ] {
        assert!(dir.path().join(name).exists());
    }
}

#[cfg(not(feature = "media-ffi"))]
#[test]
fn disabled_ffi_backend_reports_every_unsupported_conversion() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("input.bin");
    fs::write(&input, b"input").unwrap();
    let retry = RetryConfig {
        attempts: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    assert!(runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &dir.path().join("usm.mp4"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ))
        .is_err());
    assert!(runtime
        .block_on(convert_m2v_to_mp4_with_backend(
            &input,
            &dir.path().join("m2v.mp4"),
            false,
            "ffmpeg",
            MediaBackend::Ffi,
            None,
            &retry,
        ))
        .is_err());
    assert!(runtime
        .block_on(convert_m2v_bytes_to_mp4_with_backend(
            b"m2v",
            &dir.path().join("bytes.mp4"),
            "ffmpeg",
            MediaBackend::Ffi,
            None,
            &retry,
        ))
        .is_err());

    let sync_results = [
        convert_wav_to_mp3_with_backend(
            &input,
            &dir.path().join("wav.mp3"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ),
        convert_wav_bytes_to_mp3_with_backend(
            b"wav",
            &dir.path().join("bytes.mp3"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ),
        convert_hca_bytes_to_mp3_with_backend(
            b"hca",
            &dir.path().join("hca.mp3"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ),
        convert_wav_to_flac_with_backend(
            &input,
            &dir.path().join("wav.flac"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ),
        convert_wav_bytes_to_flac_with_backend(
            b"wav",
            &dir.path().join("bytes.flac"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ),
        convert_hca_bytes_to_flac_with_backend(
            b"hca",
            &dir.path().join("hca.flac"),
            "ffmpeg",
            MediaBackend::Ffi,
            &retry,
        ),
    ];
    assert!(sync_results.into_iter().all(|result| result.is_err()));
}

#[cfg(feature = "media-ffi")]
#[test]
fn ffi_usm_to_mp4_handles_real_sample_when_available() {
    let Some(sample) = std::env::var_os("HARUKI_USM_SAMPLE").map(PathBuf::from) else {
        return;
    };
    if !sample.exists() {
        return;
    }

    let dir = tempdir().unwrap();
    let output = dir.path().join("sample.mp4");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &sample,
            &output,
            "ffmpeg",
            MediaBackend::Ffi,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(output.exists());
    assert!(fs::metadata(&output).unwrap().len() > 0);
    if let Some(copy_to) = std::env::var_os("HARUKI_USM_OUTPUT").map(PathBuf::from) {
        fs::copy(&output, copy_to).unwrap();
    }
}

#[test]
fn cli_bytes_input_uses_system_temp_dir() {
    let dir = tempdir().unwrap();
    let output_dir = dir.path().join("exports");
    fs::create_dir_all(&output_dir).unwrap();
    let output = output_dir.join("sample.mp4");
    let script_path = dir.path().join("fake_ffmpeg.sh");
    let input_log = dir.path().join("input_path.txt");
    write_executable_script(
        &script_path,
        format!(
            "#!/bin/sh\nset -eu\ninput=\"\"\nout=\"\"\nprev=\"\"\nfor arg in \"$@\"; do\n  if [ \"$prev\" = \"-i\" ]; then input=\"$arg\"; fi\n  out=\"$arg\"\n  prev=\"$arg\"\ndone\nprintf '%s' \"$input\" > \"{}\"\n: > \"$out\"\n",
            input_log.display()
        ),
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_m2v_bytes_to_mp4_with_backend(
            b"dummy m2v",
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            None,
            &RetryConfig {
                attempts: 4,
                initial_backoff_ms: 5,
                max_backoff_ms: 20,
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

fn synthetic_hca() -> Vec<u8> {
    let sample_rate = 44_100;
    let samples = (0..4_096)
        .map(|index| {
            let time = index as f32 / sample_rate as f32;
            (time * 440.0 * std::f32::consts::TAU).sin() * 0.25
        })
        .collect::<Vec<_>>();
    let mut encoder = cridecoder::HcaEncoder::new(cridecoder::HcaEncoderConfig {
        channels: 1,
        sample_rate,
        bitrate: 64_000,
        ..cridecoder::HcaEncoderConfig::default()
    })
    .unwrap();
    let mut encoded = Vec::new();
    encoder
        .encode(&samples, &mut std::io::Cursor::new(&mut encoded))
        .unwrap();
    encoded
}
