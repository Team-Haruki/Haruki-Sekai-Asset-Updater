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
mod ffi;
#[cfg(feature = "media-ffi")]
use ffi as media_ffi;

#[cfg(not(feature = "media-ffi"))]
mod ffi_disabled;
#[cfg(not(feature = "media-ffi"))]
use ffi_disabled as media_ffi;

#[cfg(test)]
mod tests;
