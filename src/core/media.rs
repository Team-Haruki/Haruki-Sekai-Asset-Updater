use std::fmt::{Display, Formatter};
use std::path::Path;

use tokio::process::Command;

use crate::core::cleanup::remove_file_if_exists;
use crate::core::config::{MediaBackend, RetryConfig};
use crate::core::errors::ExportPipelineError;
use crate::core::retry::{retry_async, retry_sync};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameRate {
    pub numerator: i32,
    pub denominator: i32,
}

impl FrameRate {
    pub fn from_tuple((numerator, denominator): (i32, i32)) -> Self {
        Self {
            numerator,
            denominator,
        }
    }
}

impl Display for FrameRate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.denominator <= 1 {
            write!(f, "{}", self.numerator)
        } else {
            write!(f, "{}/{}", self.numerator, self.denominator)
        }
    }
}

pub async fn convert_usm_to_mp4(
    usm_file: &Path,
    mp4_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    convert_usm_to_mp4_with_backend(usm_file, mp4_file, ffmpeg_path, MediaBackend::Cli, retry).await
}

pub async fn convert_usm_to_mp4_with_backend(
    usm_file: &Path,
    mp4_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend(
        backend,
        retry,
        "usm->mp4",
        || media_ffi::convert_usm_to_mp4(usm_file, mp4_file),
        || async { convert_usm_to_mp4_cli(usm_file, mp4_file, ffmpeg_path, retry).await },
    )
    .await
}

async fn convert_usm_to_mp4_cli(
    usm_file: &Path,
    mp4_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    retry_async(
        retry,
        "ffmpeg usm->mp4",
        |_| async {
            run_ffmpeg(
                ffmpeg_path,
                &[
                    "-i",
                    &usm_file.to_string_lossy(),
                    "-c:v",
                    "libx264",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-movflags",
                    "+faststart",
                    "-y",
                    &mp4_file.to_string_lossy(),
                ],
            )
            .run_async()
            .await
        },
        is_retryable_command_error,
    )
    .await
}

pub async fn convert_m2v_to_mp4(
    m2v_file: &Path,
    mp4_file: &Path,
    delete_original: bool,
    ffmpeg_path: &str,
    frame_rate: Option<FrameRate>,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    convert_m2v_to_mp4_with_backend(
        m2v_file,
        mp4_file,
        delete_original,
        ffmpeg_path,
        MediaBackend::Cli,
        frame_rate,
        retry,
    )
    .await
}

pub async fn convert_m2v_to_mp4_with_backend(
    m2v_file: &Path,
    mp4_file: &Path,
    delete_original: bool,
    ffmpeg_path: &str,
    backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend(
        backend,
        retry,
        "m2v->mp4",
        || media_ffi::convert_m2v_to_mp4(m2v_file, mp4_file, frame_rate),
        || async {
            convert_m2v_to_mp4_cli(
                m2v_file,
                mp4_file,
                delete_original,
                ffmpeg_path,
                frame_rate,
                retry,
            )
            .await
        },
    )
    .await?;
    if delete_original {
        remove_file_if_exists(m2v_file).map_err(|source| ExportPipelineError::Io {
            path: m2v_file.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

pub async fn convert_m2v_bytes_to_mp4_with_backend(
    m2v_bytes: &[u8],
    mp4_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend(
        backend,
        retry,
        "m2v-bytes->mp4",
        || media_ffi::convert_m2v_bytes_to_mp4(m2v_bytes, mp4_file, frame_rate),
        || async {
            let temp_path = write_cli_input_temp_file(".m2v", m2v_bytes)?;
            convert_m2v_to_mp4_cli(&temp_path, mp4_file, false, ffmpeg_path, frame_rate, retry)
                .await?;
            remove_file_if_exists(&temp_path).map_err(|source| ExportPipelineError::Io {
                path: temp_path,
                source,
            })
        },
    )
    .await
}

async fn convert_m2v_to_mp4_cli(
    m2v_file: &Path,
    mp4_file: &Path,
    delete_original: bool,
    ffmpeg_path: &str,
    frame_rate: Option<FrameRate>,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    let mut args = Vec::new();
    if let Some(rate) = frame_rate {
        args.push("-r".to_string());
        args.push(rate.to_string());
    }
    args.push("-i".to_string());
    args.push(m2v_file.to_string_lossy().to_string());
    args.push("-c:v".to_string());
    args.push("libx264".to_string());
    if let Some(rate) = frame_rate {
        args.push("-r".to_string());
        args.push(rate.to_string());
    }
    args.push("-y".to_string());
    args.push(mp4_file.to_string_lossy().to_string());

    retry_async(
        retry,
        "ffmpeg m2v->mp4",
        |_| async {
            let refs: Vec<&str> = args.iter().map(String::as_str).collect();
            run_ffmpeg(ffmpeg_path, &refs).run_async().await
        },
        is_retryable_command_error,
    )
    .await?;
    if delete_original {
        remove_file_if_exists(m2v_file).map_err(|source| ExportPipelineError::Io {
            path: m2v_file.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

pub fn convert_wav_to_mp3(
    wav_file: &Path,
    mp3_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    convert_wav_to_mp3_with_backend(wav_file, mp3_file, ffmpeg_path, MediaBackend::Cli, retry)
}

pub fn convert_wav_to_mp3_with_backend(
    wav_file: &Path,
    mp3_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend_sync(
        backend,
        retry,
        "wav->mp3",
        || media_ffi::convert_wav_to_mp3(wav_file, mp3_file),
        || convert_wav_to_mp3_cli(wav_file, mp3_file, ffmpeg_path, retry),
    )
}

pub fn convert_wav_bytes_to_mp3_with_backend(
    wav_bytes: &[u8],
    mp3_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend_sync(
        backend,
        retry,
        "wav-bytes->mp3",
        || media_ffi::convert_wav_bytes_to_mp3(wav_bytes, mp3_file),
        || convert_wav_bytes_to_mp3_cli(wav_bytes, mp3_file, ffmpeg_path, retry),
    )
}

pub fn convert_hca_bytes_to_mp3_with_backend(
    hca_bytes: &[u8],
    mp3_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend_sync(
        backend,
        retry,
        "hca-bytes->mp3",
        || media_ffi::convert_hca_bytes_to_mp3(hca_bytes, mp3_file),
        || {
            let wav_bytes = crate::core::codec::decode_hca_bytes_to_wav_bytes(hca_bytes)?;
            convert_wav_bytes_to_mp3_cli(&wav_bytes, mp3_file, ffmpeg_path, retry)
        },
    )
}

fn convert_wav_to_mp3_cli(
    wav_file: &Path,
    mp3_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    retry_sync(
        retry,
        "ffmpeg wav->mp3",
        |_| {
            run_ffmpeg_sync(
                ffmpeg_path,
                &[
                    "-i",
                    &wav_file.to_string_lossy(),
                    "-b:a",
                    "320k",
                    "-y",
                    &mp3_file.to_string_lossy(),
                ],
            )
        },
        is_retryable_command_error,
    )
}

fn convert_wav_bytes_to_mp3_cli(
    wav_bytes: &[u8],
    mp3_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    let temp_path = write_cli_input_temp_file(".wav", wav_bytes)?;
    convert_wav_to_mp3_cli(&temp_path, mp3_file, ffmpeg_path, retry)?;
    remove_file_if_exists(&temp_path).map_err(|source| ExportPipelineError::Io {
        path: temp_path,
        source,
    })
}

pub fn convert_wav_to_flac(
    wav_file: &Path,
    flac_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    convert_wav_to_flac_with_backend(wav_file, flac_file, ffmpeg_path, MediaBackend::Cli, retry)
}

pub fn convert_wav_to_flac_with_backend(
    wav_file: &Path,
    flac_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend_sync(
        backend,
        retry,
        "wav->flac",
        || media_ffi::convert_wav_to_flac(wav_file, flac_file),
        || convert_wav_to_flac_cli(wav_file, flac_file, ffmpeg_path, retry),
    )
}

pub fn convert_wav_bytes_to_flac_with_backend(
    wav_bytes: &[u8],
    flac_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend_sync(
        backend,
        retry,
        "wav-bytes->flac",
        || media_ffi::convert_wav_bytes_to_flac(wav_bytes, flac_file),
        || convert_wav_bytes_to_flac_cli(wav_bytes, flac_file, ffmpeg_path, retry),
    )
}

pub fn convert_hca_bytes_to_flac_with_backend(
    hca_bytes: &[u8],
    flac_file: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    run_media_backend_sync(
        backend,
        retry,
        "hca-bytes->flac",
        || media_ffi::convert_hca_bytes_to_flac(hca_bytes, flac_file),
        || {
            let wav_bytes = crate::core::codec::decode_hca_bytes_to_wav_bytes(hca_bytes)?;
            convert_wav_bytes_to_flac_cli(&wav_bytes, flac_file, ffmpeg_path, retry)
        },
    )
}

fn convert_wav_to_flac_cli(
    wav_file: &Path,
    flac_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    retry_sync(
        retry,
        "ffmpeg wav->flac",
        |_| {
            run_ffmpeg_sync(
                ffmpeg_path,
                &[
                    "-i",
                    &wav_file.to_string_lossy(),
                    "-compression_level",
                    "12",
                    "-y",
                    &flac_file.to_string_lossy(),
                ],
            )
        },
        is_retryable_command_error,
    )
}

fn convert_wav_bytes_to_flac_cli(
    wav_bytes: &[u8],
    flac_file: &Path,
    ffmpeg_path: &str,
    retry: &RetryConfig,
) -> Result<(), ExportPipelineError> {
    let temp_path = write_cli_input_temp_file(".wav", wav_bytes)?;
    convert_wav_to_flac_cli(&temp_path, flac_file, ffmpeg_path, retry)?;
    remove_file_if_exists(&temp_path).map_err(|source| ExportPipelineError::Io {
        path: temp_path,
        source,
    })
}

fn write_cli_input_temp_file(
    suffix: &str,
    bytes: &[u8],
) -> Result<std::path::PathBuf, ExportPipelineError> {
    let temp_root = std::env::temp_dir();
    let mut temp_file = tempfile::Builder::new()
        .suffix(suffix)
        .tempfile_in(&temp_root)
        .map_err(|source| ExportPipelineError::Io {
            path: temp_root.clone(),
            source,
        })?;
    std::io::Write::write_all(&mut temp_file, bytes).map_err(|source| ExportPipelineError::Io {
        path: temp_file.path().to_path_buf(),
        source,
    })?;
    temp_file
        .into_temp_path()
        .keep()
        .map_err(|error| ExportPipelineError::Io {
            path: error.path.to_path_buf(),
            source: error.error,
        })
}

async fn run_media_backend<Ffi, Cli, CliFuture>(
    backend: MediaBackend,
    retry: &RetryConfig,
    operation: &str,
    mut ffi: Ffi,
    cli: Cli,
) -> Result<(), ExportPipelineError>
where
    Ffi: FnMut() -> Result<(), ExportPipelineError>,
    Cli: FnOnce() -> CliFuture,
    CliFuture: std::future::Future<Output = Result<(), ExportPipelineError>>,
{
    match backend {
        MediaBackend::Cli => cli().await,
        MediaBackend::Ffi => retry_sync(
            retry,
            &format!("ffmpeg ffi {operation}"),
            |_| ffi(),
            is_retryable_command_error,
        ),
        MediaBackend::Auto => match retry_sync(
            retry,
            &format!("ffmpeg ffi {operation}"),
            |_| ffi(),
            is_retryable_command_error,
        ) {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "FFmpeg FFI media backend failed; falling back to CLI"
                );
                cli().await
            }
        },
    }
}

fn run_media_backend_sync<Ffi, Cli>(
    backend: MediaBackend,
    retry: &RetryConfig,
    operation: &str,
    mut ffi: Ffi,
    cli: Cli,
) -> Result<(), ExportPipelineError>
where
    Ffi: FnMut() -> Result<(), ExportPipelineError>,
    Cli: FnOnce() -> Result<(), ExportPipelineError>,
{
    match backend {
        MediaBackend::Cli => cli(),
        MediaBackend::Ffi => retry_sync(
            retry,
            &format!("ffmpeg ffi {operation}"),
            |_| ffi(),
            is_retryable_command_error,
        ),
        MediaBackend::Auto => match retry_sync(
            retry,
            &format!("ffmpeg ffi {operation}"),
            |_| ffi(),
            is_retryable_command_error,
        ) {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "FFmpeg FFI media backend failed; falling back to CLI"
                );
                cli()
            }
        },
    }
}

fn run_ffmpeg<'a>(ffmpeg_path: &'a str, args: &'a [&'a str]) -> FfmpegCommand<'a> {
    FfmpegCommand { ffmpeg_path, args }
}

fn run_ffmpeg_sync(ffmpeg_path: &str, args: &[&str]) -> Result<(), ExportPipelineError> {
    let output = std::process::Command::new(ffmpeg_path)
        .args(args)
        .output()
        .map_err(|source| ExportPipelineError::Spawn {
            program: ffmpeg_path.to_string(),
            source,
        })?;
    map_command_output(ffmpeg_path, output)
}

struct FfmpegCommand<'a> {
    ffmpeg_path: &'a str,
    args: &'a [&'a str],
}

impl<'a> FfmpegCommand<'a> {
    async fn run_async(self) -> Result<(), ExportPipelineError> {
        let output = Command::new(self.ffmpeg_path)
            .args(self.args)
            .output()
            .await
            .map_err(|source| ExportPipelineError::Spawn {
                program: self.ffmpeg_path.to_string(),
                source,
            })?;
        map_command_output(self.ffmpeg_path, output)
    }
}

fn is_retryable_command_error(err: &ExportPipelineError) -> bool {
    match err {
        ExportPipelineError::Spawn { source, .. } => matches!(
            source.kind(),
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionRefused
        ),
        ExportPipelineError::CommandFailed { .. } => true,
        _ => false,
    }
}

fn map_command_output(
    program: &str,
    output: std::process::Output,
) -> Result<(), ExportPipelineError> {
    if output.status.success() {
        Ok(())
    } else {
        Err(ExportPipelineError::CommandFailed {
            program: program.to_string(),
            status: output.status.to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }
}

#[cfg(feature = "media-ffi")]
#[path = "media_ffi.rs"]
mod media_ffi;

#[cfg(not(feature = "media-ffi"))]
mod media_ffi {
    use std::path::Path;

    use super::FrameRate;
    use crate::core::errors::ExportPipelineError;

    pub fn convert_usm_to_mp4(
        _usm_file: &Path,
        _mp4_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_m2v_to_mp4(
        _m2v_file: &Path,
        _mp4_file: &Path,
        _frame_rate: Option<FrameRate>,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_m2v_bytes_to_mp4(
        _m2v_bytes: &[u8],
        _mp4_file: &Path,
        _frame_rate: Option<FrameRate>,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_wav_to_mp3(
        _wav_file: &Path,
        _mp3_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_wav_bytes_to_mp3(
        _wav_bytes: &[u8],
        _mp3_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_hca_bytes_to_mp3(
        _hca_bytes: &[u8],
        _mp3_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_wav_to_flac(
        _wav_file: &Path,
        _flac_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_wav_bytes_to_flac(
        _wav_bytes: &[u8],
        _flac_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }

    pub fn convert_hca_bytes_to_flac(
        _hca_bytes: &[u8],
        _flac_file: &Path,
    ) -> Result<(), ExportPipelineError> {
        Err(ExportPipelineError::Media {
            message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature"
                .to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    #[cfg(feature = "media-ffi")]
    use super::convert_wav_bytes_to_mp3_with_backend;
    use super::{
        convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4, convert_usm_to_mp4,
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
            .block_on(convert_usm_to_mp4(
                &input,
                &output,
                &script_path.to_string_lossy(),
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
            .block_on(convert_m2v_to_mp4(
                &input,
                &output,
                true,
                &script_path.to_string_lossy(),
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
            .block_on(convert_usm_to_mp4(
                &input,
                &output,
                &script_path.to_string_lossy(),
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
}
