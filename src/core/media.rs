use std::fmt::{Display, Formatter};
use std::path::Path;

use tokio::process::Command;

use crate::core::config::RetryConfig;
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
    if delete_original && m2v_file.exists() {
        std::fs::remove_file(m2v_file).map_err(|source| ExportPipelineError::Io {
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

pub fn convert_wav_to_flac(
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

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{convert_m2v_to_mp4, convert_usm_to_mp4, FrameRate};
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
}
