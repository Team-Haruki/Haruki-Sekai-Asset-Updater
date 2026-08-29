//! USM video: extracting its streams and getting them into an MP4.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::core::cleanup::remove_file_if_exists;
use crate::core::codec;
use crate::core::config::{MediaBackend, RegionConfig};
use crate::core::errors::ExportPipelineError;
use crate::core::media::{
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, FrameRate,
};

use super::super::tasks::UsmProcessingInput;
use super::encode_slots::{
    acquire_media_encode_permit_async, record_usm_video_encode_acquire, MediaEncodeKind,
};
use super::timing::add_elapsed_phase_ms;

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_usm_file(
    usm_file: &Path,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    Ok(process_usm_input_with_metrics(
        &UsmProcessingInput::Path(usm_file.to_path_buf()),
        export_path,
        region,
        ffmpeg_path,
        media_backend,
        retry,
        video_encode_concurrency,
        cpu_budget,
    )
    .await?
    .generated_files)
}

#[derive(Debug, Default)]
pub(crate) struct UsmPostProcessOutput {
    pub(crate) generated_files: Vec<PathBuf>,
    pub(crate) phase_ms: HashMap<String, u64>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_usm_input_with_metrics(
    usm_input: &UsmProcessingInput,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
) -> Result<UsmPostProcessOutput, ExportPipelineError> {
    let mut output = UsmPostProcessOutput::default();
    let output_name = usm_input.output_name()?;
    let writes_mp4 = region.export.video.writes_mp4();
    let writes_m2v = region.export.video.writes_m2v();

    if skip_invalid_usm_input(usm_input, &mut output)? {
        return Ok(output);
    }

    if process_direct_usm_path(
        usm_input,
        export_path,
        &output_name,
        region,
        ffmpeg_path,
        media_backend,
        retry,
        video_encode_concurrency,
        cpu_budget,
        &mut output,
    )
    .await?
    {
        return Ok(output);
    }

    let frame_rate = usm_frame_rate(usm_input);

    if process_video_only_usm(
        usm_input,
        export_path,
        &output_name,
        writes_mp4,
        writes_m2v,
        ffmpeg_path,
        media_backend,
        frame_rate,
        retry,
        video_encode_concurrency,
        cpu_budget,
        &mut output,
    )
    .await?
    {
        return Ok(output);
    }

    if matches!(usm_input, UsmProcessingInput::Bytes { .. }) {
        process_memory_usm(
            usm_input,
            export_path,
            &output_name,
            writes_mp4,
            writes_m2v,
            ffmpeg_path,
            media_backend,
            frame_rate,
            retry,
            video_encode_concurrency,
            cpu_budget,
            &mut output,
        )
        .await?;
        return Ok(output);
    }

    process_path_usm(
        usm_input,
        export_path,
        &output_name,
        writes_mp4,
        writes_m2v,
        ffmpeg_path,
        media_backend,
        frame_rate,
        retry,
        video_encode_concurrency,
        cpu_budget,
        &mut output,
    )
    .await?;
    Ok(output)
}

pub(super) fn skip_invalid_usm_input(
    usm_input: &UsmProcessingInput,
    output: &mut UsmPostProcessOutput,
) -> Result<bool, ExportPipelineError> {
    if usm_input_has_crid_magic(usm_input)? {
        return Ok(false);
    }
    if let Some(usm_file) = usm_input.path() {
        tracing::warn!(
            path = %usm_file.display(),
            "skipping .usm post-process input without CRID magic"
        );
        output.generated_files.push(usm_file.to_path_buf());
    } else {
        tracing::warn!("skipping in-memory .usm post-process input without CRID magic");
        usm_input.cleanup_sources()?;
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_direct_usm_path(
    usm_input: &UsmProcessingInput,
    export_path: &Path,
    output_name: &str,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    output: &mut UsmPostProcessOutput,
) -> Result<bool, ExportPipelineError> {
    if !region.export.video.writes_mp4()
        || region.export.video.writes_m2v()
        || !region.export.video.direct_mp4
    {
        return Ok(false);
    }
    let Some(usm_file) = usm_input.path() else {
        return Ok(false);
    };
    let mp4 = export_path.join(format!("{output_name}.mp4"));
    let encode_slot = acquire_media_encode_permit_async(
        MediaEncodeKind::Video,
        video_encode_concurrency,
        cpu_budget,
    )
    .await?;
    record_usm_video_encode_acquire(&mut output.phase_ms, &encode_slot);
    let phase_started = Instant::now();
    convert_usm_to_mp4_with_backend(usm_file, &mp4, ffmpeg_path, media_backend, retry).await?;
    drop(encode_slot.cpu_permit);
    drop(encode_slot.permit);
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.usm.convert_mp4",
        phase_started,
    );
    usm_input.cleanup_sources()?;
    output.generated_files.push(mp4);
    Ok(true)
}

pub(super) fn usm_frame_rate(usm_input: &UsmProcessingInput) -> Option<FrameRate> {
    let UsmProcessingInput::Path(usm_file) = usm_input else {
        return None;
    };
    codec::read_usm_metadata(usm_file)
        .ok()
        .as_ref()
        .and_then(|metadata| metadata.video_frame_rate())
        .filter(|(_, denominator)| *denominator > 0)
        .map(FrameRate::from_tuple)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_video_only_usm(
    usm_input: &UsmProcessingInput,
    export_path: &Path,
    output_name: &str,
    writes_mp4: bool,
    writes_m2v: bool,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    output: &mut UsmPostProcessOutput,
) -> Result<bool, ExportPipelineError> {
    if !writes_mp4 || writes_m2v {
        return Ok(false);
    }
    let phase_started = Instant::now();
    let streams = export_usm_input_to_memory(usm_input, false)?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.usm.extract",
        phase_started,
    );
    let Some(video) = streams
        .into_iter()
        .find(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
    else {
        return Ok(false);
    };
    let mp4 = export_path.join(format!("{output_name}.mp4"));
    convert_usm_m2v_bytes(
        &video.data,
        &mp4,
        ffmpeg_path,
        media_backend,
        frame_rate,
        retry,
        video_encode_concurrency,
        cpu_budget,
        &mut output.phase_ms,
    )
    .await?;
    usm_input.cleanup_sources()?;
    output.generated_files.push(mp4);
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_memory_usm(
    usm_input: &UsmProcessingInput,
    export_path: &Path,
    output_name: &str,
    writes_mp4: bool,
    writes_m2v: bool,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    output: &mut UsmPostProcessOutput,
) -> Result<(), ExportPipelineError> {
    let phase_started = Instant::now();
    let mut streams = export_usm_input_to_memory(usm_input, true)?;
    let mut generated = write_usm_streams(export_path, &streams)?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.usm.extract",
        phase_started,
    );
    let video = writes_mp4
        .then(|| {
            streams
                .iter()
                .position(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
                .map(|position| streams.swap_remove(position))
        })
        .flatten();
    if let Some(video) = video {
        let mp4 = export_path.join(format!("{output_name}.mp4"));
        convert_usm_m2v_bytes(
            &video.data,
            &mp4,
            ffmpeg_path,
            media_backend,
            frame_rate,
            retry,
            video_encode_concurrency,
            cpu_budget,
            &mut output.phase_ms,
        )
        .await?;
        generated.push(mp4);
        if !writes_m2v {
            generated.retain(|path| !has_extension(path, "m2v"));
            let m2v = export_path.join(format!("{}.m2v", video.name));
            remove_file_if_exists(&m2v)
                .map_err(|source| ExportPipelineError::Io { path: m2v, source })?;
        }
    }
    usm_input.cleanup_sources()?;
    output.generated_files = generated;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_path_usm(
    usm_input: &UsmProcessingInput,
    export_path: &Path,
    output_name: &str,
    writes_mp4: bool,
    writes_m2v: bool,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    output: &mut UsmPostProcessOutput,
) -> Result<(), ExportPipelineError> {
    let usm_file = usm_input
        .path()
        .expect("non-memory USM processing requires a path");
    let phase_started = Instant::now();
    let extracted = codec::export_usm(usm_file, export_path)?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.usm.extract",
        phase_started,
    );
    let mut generated = extracted.clone();
    if writes_mp4 {
        for extracted_file in extracted
            .into_iter()
            .filter(|path| has_extension(path, "m2v"))
        {
            let mp4 = export_path.join(format!("{output_name}.mp4"));
            convert_usm_m2v_path(
                &extracted_file,
                &mp4,
                !writes_m2v,
                ffmpeg_path,
                media_backend,
                frame_rate,
                retry,
                video_encode_concurrency,
                cpu_budget,
                &mut output.phase_ms,
            )
            .await?;
            generated.push(mp4);
            if !writes_m2v {
                generated.retain(|path| path != &extracted_file);
            }
        }
    }
    usm_input.cleanup_sources()?;
    output.generated_files = generated;
    Ok(())
}

pub(super) fn has_extension(path: &Path, extension: &str) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn convert_usm_m2v_bytes(
    video: &[u8],
    mp4: &Path,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    phase_ms: &mut HashMap<String, u64>,
) -> Result<(), ExportPipelineError> {
    let encode_slot = acquire_media_encode_permit_async(
        MediaEncodeKind::Video,
        video_encode_concurrency,
        cpu_budget,
    )
    .await?;
    record_usm_video_encode_acquire(phase_ms, &encode_slot);
    let phase_started = Instant::now();
    convert_m2v_bytes_to_mp4_with_backend(
        video,
        mp4,
        ffmpeg_path,
        media_backend,
        frame_rate,
        retry,
    )
    .await?;
    drop(encode_slot.cpu_permit);
    drop(encode_slot.permit);
    add_elapsed_phase_ms(phase_ms, "post_process.usm.convert_mp4", phase_started);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn convert_usm_m2v_path(
    m2v: &Path,
    mp4: &Path,
    remove_source: bool,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    frame_rate: Option<FrameRate>,
    retry: &crate::core::config::RetryConfig,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    phase_ms: &mut HashMap<String, u64>,
) -> Result<(), ExportPipelineError> {
    let encode_slot = acquire_media_encode_permit_async(
        MediaEncodeKind::Video,
        video_encode_concurrency,
        cpu_budget,
    )
    .await?;
    record_usm_video_encode_acquire(phase_ms, &encode_slot);
    let phase_started = Instant::now();
    convert_m2v_to_mp4_with_backend(
        m2v,
        mp4,
        remove_source,
        ffmpeg_path,
        media_backend,
        frame_rate,
        retry,
    )
    .await?;
    drop(encode_slot.cpu_permit);
    drop(encode_slot.permit);
    add_elapsed_phase_ms(phase_ms, "post_process.usm.convert_mp4", phase_started);
    Ok(())
}

pub(super) fn usm_input_has_crid_magic(
    usm_input: &UsmProcessingInput,
) -> Result<bool, ExportPipelineError> {
    match usm_input {
        UsmProcessingInput::Path(usm_file) => {
            codec::file_has_usm_magic(usm_file).map_err(ExportPipelineError::from)
        }
        UsmProcessingInput::Bytes { data, .. } => Ok(codec::has_usm_magic(data)),
    }
}

pub(super) fn export_usm_input_to_memory(
    usm_input: &UsmProcessingInput,
    export_audio: bool,
) -> Result<Vec<cridecoder::ExtractedUsmStream>, ExportPipelineError> {
    match usm_input {
        UsmProcessingInput::Path(usm_file) => {
            let usm_reader =
                std::fs::File::open(usm_file).map_err(|source| ExportPipelineError::Io {
                    path: usm_file.to_path_buf(),
                    source,
                })?;
            let fallback_name = usm_file
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("input.usm");
            codec::export_usm_reader_to_memory(usm_reader, fallback_name.as_bytes(), export_audio)
                .map_err(ExportPipelineError::from)
        }
        UsmProcessingInput::Bytes {
            fallback_name,
            data,
            ..
        } => codec::export_usm_to_memory(data, fallback_name.as_bytes(), export_audio)
            .map_err(ExportPipelineError::from),
    }
}

pub(super) fn write_usm_streams(
    export_path: &Path,
    streams: &[cridecoder::ExtractedUsmStream],
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let mut generated = Vec::with_capacity(streams.len());
    for stream in streams {
        let path = export_path.join(format!("{}.{}", stream.name, stream.extension));
        std::fs::write(&path, &stream.data).map_err(|source| ExportPipelineError::Io {
            path: path.clone(),
            source,
        })?;
        generated.push(path);
    }
    Ok(generated)
}
