//! Post-processing a bundle's exported files: media conversion.
//!
//! This file orchestrates; the submodules do the work.

pub(super) mod acb;
pub(super) mod encode_slots;
pub(super) mod hca;
pub(super) mod model;
mod timing;
pub(super) mod usm;

use std::path::{Path, PathBuf};
use std::time::Instant;

use super::images::handle_png_conversion;

use crate::{
    ExportPipelineError, MediaBackend, PipelineOptions, PipelineRegionOptions,
    RetryOptions as RetryConfig,
};

use self::acb::handle_acb_files_owned;
use self::model::OwnedAcbPostProcessOptions;
use self::timing::{merge_raw_phase_ms, record_phase_ms};
use self::usm::{process_usm_input_with_metrics, UsmPostProcessOutput};
use super::images::convert_native_surrogate_images_to_png;
use super::limits::configure_cpu_budget_throttle;
use super::record_max_phase_ms;
use super::tasks::{
    merge_usm_inputs, post_process_files_by_extension, prepare_usm_processing_inputs, run_tasks,
    UsmProcessingInput,
};
use super::types::{NativeInMemoryMediaSource, PostProcessSummary};

#[allow(clippy::too_many_arguments)]
pub async fn post_process_exported_files(
    app_config: &PipelineOptions,
    export_path: &Path,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<PostProcessSummary, ExportPipelineError> {
    let region = &app_config.region;
    configure_cpu_budget_throttle(&app_config.resources, app_config.effective_cpu_budget());
    if !export_path.exists() {
        return Ok(PostProcessSummary {
            export_root: export_path.to_path_buf(),
            ..PostProcessSummary::default()
        });
    }

    let mut summary = PostProcessSummary {
        export_root: export_path.to_path_buf(),
        ..PostProcessSummary::default()
    };
    let concurrency = app_config.effective_concurrency();
    let cpu_budget = app_config.effective_cpu_budget();
    summary.post_process_phase_ms.insert(
        "media_scheduler.auto_tune".to_string(),
        u64::from(concurrency.auto_tune),
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.download_concurrency".to_string(),
        concurrency.download as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.acb_concurrency".to_string(),
        concurrency.acb as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.usm_concurrency".to_string(),
        concurrency.usm as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.hca_concurrency".to_string(),
        concurrency.hca as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.media_encode_concurrency".to_string(),
        concurrency.media_encode as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.audio_encode_concurrency".to_string(),
        concurrency.audio_encode as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.video_encode_concurrency".to_string(),
        concurrency.video_encode as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.image_concurrency".to_string(),
        concurrency.images as u64,
    );
    summary
        .post_process_phase_ms
        .insert("media_scheduler.cpu_budget".to_string(), cpu_budget as u64);
    summary.post_process_phase_ms.insert(
        "media_scheduler.cpu_throttle_enabled".to_string(),
        u64::from(app_config.resources.cpu.throttle.enabled),
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.cpu_throttle_target_percent".to_string(),
        (cpu_budget * 100) as u64,
    );

    let phase_started = Instant::now();
    let surrogate_png_files = convert_native_surrogate_images_to_png(
        export_path,
        scoped_files,
        concurrency.images,
        cpu_budget,
        scoped_post_process,
    )?;
    summary.generated_files.extend(surrogate_png_files.clone());
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.native_surrogate_images",
        phase_started,
    );

    let acb_options = OwnedAcbPostProcessOptions {
        output_dir: export_path.to_path_buf(),
        region: region.clone(),
        ffmpeg_path: app_config.backends.media.ffmpeg_path.clone(),
        media_backend: app_config.backends.media.backend,
        retry: app_config.execution.retry.clone(),
        hca_concurrency: concurrency.hca,
        audio_encode_concurrency: concurrency.audio_encode,
        cpu_budget,
    };
    let acb_concurrency = concurrency.acb;
    let acb_scoped_files = scoped_files.to_vec();
    let usm_output = async {
        let phase_started = Instant::now();
        let mut output = handle_usm_files(
            export_path,
            region,
            &app_config.backends.media.ffmpeg_path,
            app_config.backends.media.backend,
            &app_config.execution.retry,
            concurrency.usm,
            concurrency.video_encode,
            cpu_budget,
            scoped_post_process,
            scoped_files,
        )
        .await?;
        record_phase_ms(&mut output.phase_ms, "post_process.usm", phase_started);
        Ok::<_, ExportPipelineError>(output)
    };
    let acb_output = tokio::task::spawn_blocking(move || {
        let phase_started = Instant::now();
        let mut output = handle_acb_files_owned(
            &acb_options,
            acb_concurrency,
            scoped_post_process,
            &acb_scoped_files,
            acb_sources,
        )?;
        record_phase_ms(&mut output.phase_ms, "post_process.acb", phase_started);
        Ok::<_, ExportPipelineError>(output)
    });
    let (usm_output, acb_output) = tokio::join!(usm_output, acb_output);
    let usm_output = usm_output?;
    summary.generated_files.extend(usm_output.generated_files);
    merge_raw_phase_ms(&mut summary.post_process_phase_ms, &usm_output.phase_ms);

    let acb_output = acb_output.map_err(|source| ExportPipelineError::WorkerPanic {
        worker: "acb post-process".to_string(),
        message: source.to_string(),
    })??;
    summary.generated_files.extend(acb_output.generated_files);
    merge_raw_phase_ms(&mut summary.post_process_phase_ms, &acb_output.phase_ms);

    let phase_started = Instant::now();
    let mut scoped_png_files = scoped_files.to_vec();
    scoped_png_files.extend(surrogate_png_files);
    summary.generated_files.extend(
        handle_png_conversion(
            export_path,
            &scoped_png_files,
            region,
            &app_config.backends.image,
            concurrency.images,
            cpu_budget,
            scoped_post_process,
        )
        .await?,
    );
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.png_conversion",
        phase_started,
    );

    Ok(summary)
}

pub fn scoped_upload_files(scoped_files: &[PathBuf], generated_files: &[PathBuf]) -> Vec<PathBuf> {
    let mut files = scoped_files
        .iter()
        .chain(generated_files)
        // Transcoding can remove source USM/ACB/PNG files. Do not schedule stale paths, and do not
        // discover unrelated files produced by another concurrently running bundle.
        .filter(|path| path.is_file())
        .cloned()
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    files
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_usm_files(
    export_path: &Path,
    region: &PipelineRegionOptions,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &RetryConfig,
    usm_concurrency: usize,
    video_encode_concurrency: usize,
    cpu_budget: usize,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
) -> Result<UsmPostProcessOutput, ExportPipelineError> {
    let mut output = UsmPostProcessOutput::default();
    let usm_files =
        post_process_files_by_extension(export_path, scoped_post_process, scoped_files, "usm")?;
    output.phase_ms.insert(
        "media_scheduler.usm_file_count".to_string(),
        usm_files.len() as u64,
    );
    if !region.export.usm.export || !region.export.usm.decode || usm_files.is_empty() {
        output
            .phase_ms
            .insert("media_scheduler.usm_worker_count".to_string(), 0);
        output
            .phase_ms
            .insert("media_scheduler.usm_merged_count".to_string(), 0);
        return Ok(output);
    }

    let prepared_usm_inputs = prepare_usm_processing_inputs(usm_files)?;
    let merged_count = prepared_usm_inputs.merged_count;
    let usm_inputs = prepared_usm_inputs.files;

    if scoped_post_process {
        output.phase_ms.insert(
            "media_scheduler.usm_merged_count".to_string(),
            merged_count as u64,
        );
        output.phase_ms.insert(
            "media_scheduler.usm_configured_concurrency".to_string(),
            usm_concurrency.max(1) as u64,
        );
        let worker_count = usm_concurrency.max(1).min(usm_inputs.len());
        output.phase_ms.insert(
            "media_scheduler.usm_worker_count".to_string(),
            worker_count as u64,
        );
        if usm_inputs.len() == 1 {
            let usm_input = usm_inputs
                .into_iter()
                .next()
                .expect("single scoped USM is present");
            let output_dir = usm_input.output_dir();
            let file_output = process_usm_input_with_metrics(
                &usm_input,
                &output_dir,
                region,
                ffmpeg_path,
                media_backend,
                retry,
                video_encode_concurrency,
                cpu_budget,
            )
            .await?;
            output.generated_files.extend(file_output.generated_files);
            merge_raw_phase_ms(&mut output.phase_ms, &file_output.phase_ms);
            return Ok(output);
        }
        let region = region.clone();
        let ffmpeg_path = ffmpeg_path.to_string();
        let retry = retry.clone();
        let outputs = run_tasks(usm_inputs, worker_count, move |usm_input| {
            let output_dir = usm_input.output_dir();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| ExportPipelineError::UnityRs {
                    message: format!("failed to create USM post-process runtime: {source}"),
                })?;
            runtime.block_on(process_usm_input_with_metrics(
                &usm_input,
                &output_dir,
                &region,
                &ffmpeg_path,
                media_backend,
                &retry,
                video_encode_concurrency,
                cpu_budget,
            ))
        })?;
        for file_output in outputs {
            output.generated_files.extend(file_output.generated_files);
            merge_raw_phase_ms(&mut output.phase_ms, &file_output.phase_ms);
        }
        return Ok(output);
    }

    let usm_input = if usm_inputs.len() == 1 {
        output.phase_ms.insert(
            "media_scheduler.usm_merged_count".to_string(),
            merged_count as u64,
        );
        usm_inputs
            .into_iter()
            .next()
            .expect("single USM is present")
    } else {
        output.phase_ms.insert(
            "media_scheduler.usm_merged_count".to_string(),
            (merged_count + usm_inputs.len()) as u64,
        );
        UsmProcessingInput::Path(merge_usm_inputs(export_path, usm_inputs)?)
    };
    output
        .phase_ms
        .insert("media_scheduler.usm_worker_count".to_string(), 1);
    output.phase_ms.insert(
        "media_scheduler.usm_configured_concurrency".to_string(),
        usm_concurrency.max(1) as u64,
    );

    let file_output = process_usm_input_with_metrics(
        &usm_input,
        export_path,
        region,
        ffmpeg_path,
        media_backend,
        retry,
        video_encode_concurrency,
        cpu_budget,
    )
    .await?;
    output.generated_files.extend(file_output.generated_files);
    merge_raw_phase_ms(&mut output.phase_ms, &file_output.phase_ms);
    Ok(output)
}
