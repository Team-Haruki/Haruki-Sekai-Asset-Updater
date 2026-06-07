use super::*;

#[allow(clippy::too_many_arguments)]
pub async fn post_process_exported_files(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    export_path: &Path,
    upload_root: &Path,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<PostProcessSummary, ExportPipelineError> {
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

    if region.upload.enabled {
        let phase_started = Instant::now();
        let files = scan_all_files(export_path)?;
        upload_to_all_storages(
            &app_config.storage,
            region_name,
            upload_root,
            &files,
            StorageUploadOptions {
                selected_providers: &region.upload.providers,
                public_read_include: &region.upload.public_read.include,
                public_read_exclude: &region.upload.public_read.exclude,
                remove_local: region.upload.remove_local_after_upload,
                concurrency: concurrency.upload,
                retry: &app_config.execution.retry,
            },
        )
        .await?;
        summary.uploaded_files = files;
        record_phase_ms(
            &mut summary.post_process_phase_ms,
            "post_process.upload",
            phase_started,
        );
    }

    Ok(summary)
}

pub(super) fn record_phase_ms(target: &mut HashMap<String, u64>, phase: &str, started: Instant) {
    target.insert(
        phase.to_string(),
        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

pub(super) fn add_elapsed_phase_ms(
    target: &mut HashMap<String, u64>,
    phase: &str,
    started: Instant,
) {
    add_phase_ms(
        target,
        phase,
        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

pub(super) fn add_phase_ms(target: &mut HashMap<String, u64>, phase: &str, elapsed_ms: u64) {
    *target.entry(phase.to_string()).or_default() += elapsed_ms;
}

pub(super) fn merge_raw_phase_ms(target: &mut HashMap<String, u64>, source: &HashMap<String, u64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_default() += *value;
    }
}

pub(super) struct MediaEncodeLimiter {
    pub(super) max: usize,
    pub(super) state: Mutex<usize>,
    pub(super) available: Condvar,
}

type MediaEncodeLimiterKey = (MediaEncodeKind, usize);
type MediaEncodeLimiterMap = HashMap<MediaEncodeLimiterKey, Arc<MediaEncodeLimiter>>;

pub(super) struct MediaEncodePermit {
    pub(super) limiter: Arc<MediaEncodeLimiter>,
}

impl Drop for MediaEncodePermit {
    fn drop(&mut self) {
        let mut active = self.limiter.state.lock().unwrap();
        *active = active.saturating_sub(1);
        self.limiter.available.notify_one();
    }
}

pub(super) struct MediaEncodeAcquire {
    pub(super) permit: MediaEncodePermit,
    pub(super) cpu_permit: CpuBudgetPermit,
    pub(super) kind: MediaEncodeKind,
    pub(super) wait_ms: u64,
    pub(super) cpu_budget_wait_ms: u64,
    pub(super) active: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum MediaEncodeKind {
    Audio,
    Video,
}

impl MediaEncodeKind {
    fn as_metric_prefix(self) -> &'static str {
        match self {
            Self::Audio => "audio_encode",
            Self::Video => "video_encode",
        }
    }
}

pub(super) fn acquire_media_encode_permit(
    kind: MediaEncodeKind,
    concurrency: usize,
    cpu_budget: usize,
) -> Result<MediaEncodeAcquire, ExportPipelineError> {
    let limiter = media_encode_limiter(kind, concurrency);
    let wait_started = Instant::now();
    let mut active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    *active += 1;
    let active_count = *active;
    drop(active);
    let cpu_slot = acquire_cpu_budget_permit_blocking(cpu_budget)?;
    Ok(MediaEncodeAcquire {
        permit: MediaEncodePermit { limiter },
        cpu_permit: cpu_slot.permit,
        kind,
        wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        cpu_budget_wait_ms: cpu_slot.wait_ms,
        active: active_count,
    })
}

pub(super) async fn acquire_media_encode_permit_async(
    kind: MediaEncodeKind,
    concurrency: usize,
    cpu_budget: usize,
) -> Result<MediaEncodeAcquire, ExportPipelineError> {
    tokio::task::spawn_blocking(move || acquire_media_encode_permit(kind, concurrency, cpu_budget))
        .await
        .map_err(|source| ExportPipelineError::WorkerPanic {
            worker: format!("{} limiter", kind.as_metric_prefix()),
            message: source.to_string(),
        })?
}

pub(super) fn media_encode_limiter(
    kind: MediaEncodeKind,
    concurrency: usize,
) -> Arc<MediaEncodeLimiter> {
    let concurrency = concurrency.max(1);
    static LIMITERS: OnceLock<Mutex<MediaEncodeLimiterMap>> = OnceLock::new();
    let limiters = LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut limiters = limiters.lock().unwrap();
    limiters
        .entry((kind, concurrency))
        .or_insert_with(|| {
            Arc::new(MediaEncodeLimiter {
                max: concurrency,
                state: Mutex::new(0),
                available: Condvar::new(),
            })
        })
        .clone()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_usm_files(
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
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
                .map_err(|source| ExportPipelineError::AssetStudioFfi {
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

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn process_usm_file(
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
pub(super) struct UsmPostProcessOutput {
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) phase_ms: HashMap<String, u64>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_usm_input_with_metrics(
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

    if !usm_input_has_crid_magic(usm_input)? {
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
        return Ok(output);
    }

    if let Some(usm_file) = usm_input.path() {
        if writes_mp4 && !writes_m2v && region.export.video.direct_mp4 {
            let mp4 = export_path.join(format!("{output_name}.mp4"));
            let encode_slot = acquire_media_encode_permit_async(
                MediaEncodeKind::Video,
                video_encode_concurrency,
                cpu_budget,
            )
            .await?;
            record_usm_video_encode_acquire(&mut output.phase_ms, &encode_slot);
            let phase_started = Instant::now();
            convert_usm_to_mp4_with_backend(usm_file, &mp4, ffmpeg_path, media_backend, retry)
                .await?;
            drop(encode_slot.cpu_permit);
            drop(encode_slot.permit);
            add_elapsed_phase_ms(
                &mut output.phase_ms,
                "post_process.usm.convert_mp4",
                phase_started,
            );
            usm_input.cleanup_sources()?;
            output.generated_files.push(mp4);
            return Ok(output);
        }
    }

    let frame_rate = match usm_input {
        UsmProcessingInput::Path(usm_file) => codec::read_usm_metadata(usm_file)
            .ok()
            .as_ref()
            .and_then(|metadata| metadata.video_frame_rate())
            .filter(|(_, denominator)| *denominator > 0)
            .map(FrameRate::from_tuple),
        UsmProcessingInput::Bytes { .. } => None,
    };

    if writes_mp4 && !writes_m2v {
        let phase_started = Instant::now();
        let streams = export_usm_input_to_memory(usm_input, false)?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.usm.extract",
            phase_started,
        );
        if let Some(video) = streams
            .iter()
            .find(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
        {
            let mp4 = export_path.join(format!("{output_name}.mp4"));
            let encode_slot = acquire_media_encode_permit_async(
                MediaEncodeKind::Video,
                video_encode_concurrency,
                cpu_budget,
            )
            .await?;
            record_usm_video_encode_acquire(&mut output.phase_ms, &encode_slot);
            let phase_started = Instant::now();
            convert_m2v_bytes_to_mp4_with_backend(
                &video.data,
                &mp4,
                ffmpeg_path,
                media_backend,
                frame_rate,
                retry,
            )
            .await?;
            drop(encode_slot.cpu_permit);
            drop(encode_slot.permit);
            add_elapsed_phase_ms(
                &mut output.phase_ms,
                "post_process.usm.convert_mp4",
                phase_started,
            );
            usm_input.cleanup_sources()?;
            output.generated_files.push(mp4);
            return Ok(output);
        }
    }

    if matches!(usm_input, UsmProcessingInput::Bytes { .. }) {
        let phase_started = Instant::now();
        let streams = export_usm_input_to_memory(usm_input, true)?;
        let mut generated = write_usm_streams(export_path, &streams)?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.usm.extract",
            phase_started,
        );

        if writes_mp4 {
            if let Some(video) = streams
                .iter()
                .find(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
            {
                let mp4 = export_path.join(format!("{output_name}.mp4"));
                let encode_slot = acquire_media_encode_permit_async(
                    MediaEncodeKind::Video,
                    video_encode_concurrency,
                    cpu_budget,
                )
                .await?;
                record_usm_video_encode_acquire(&mut output.phase_ms, &encode_slot);
                let phase_started = Instant::now();
                convert_m2v_bytes_to_mp4_with_backend(
                    &video.data,
                    &mp4,
                    ffmpeg_path,
                    media_backend,
                    frame_rate,
                    retry,
                )
                .await?;
                drop(encode_slot.cpu_permit);
                drop(encode_slot.permit);
                add_elapsed_phase_ms(
                    &mut output.phase_ms,
                    "post_process.usm.convert_mp4",
                    phase_started,
                );
                generated.push(mp4);
                if !writes_m2v {
                    generated.retain(|path| {
                        !path
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .map(|ext| ext.eq_ignore_ascii_case("m2v"))
                            .unwrap_or(false)
                    });
                    remove_file_if_exists(&export_path.join(format!("{}.m2v", video.name)))
                        .map_err(|source| ExportPipelineError::Io {
                            path: export_path.join(format!("{}.m2v", video.name)),
                            source,
                        })?;
                }
            }
        }

        usm_input.cleanup_sources()?;
        output.generated_files = generated;
        return Ok(output);
    }

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
        for extracted_file in extracted {
            if extracted_file
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("m2v"))
                .unwrap_or(false)
            {
                let mp4 = export_path.join(format!("{output_name}.mp4"));
                let encode_slot = acquire_media_encode_permit_async(
                    MediaEncodeKind::Video,
                    video_encode_concurrency,
                    cpu_budget,
                )
                .await?;
                record_usm_video_encode_acquire(&mut output.phase_ms, &encode_slot);
                let phase_started = Instant::now();
                convert_m2v_to_mp4_with_backend(
                    &extracted_file,
                    &mp4,
                    !writes_m2v,
                    ffmpeg_path,
                    media_backend,
                    frame_rate,
                    retry,
                )
                .await?;
                drop(encode_slot.cpu_permit);
                drop(encode_slot.permit);
                add_elapsed_phase_ms(
                    &mut output.phase_ms,
                    "post_process.usm.convert_mp4",
                    phase_started,
                );
                generated.push(mp4);
                if !writes_m2v {
                    generated.retain(|path| path != &extracted_file);
                }
            }
        }
    }

    usm_input.cleanup_sources()?;
    output.generated_files = generated;
    Ok(output)
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
            let usm_bytes = std::fs::read(usm_file).map_err(|source| ExportPipelineError::Io {
                path: usm_file.to_path_buf(),
                source,
            })?;
            let fallback_name = usm_file
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("input.usm");
            codec::export_usm_to_memory(&usm_bytes, fallback_name.as_bytes(), export_audio)
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

#[allow(dead_code)]
pub(super) async fn process_usm_file_with_metrics(
    usm_file: &Path,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
) -> Result<UsmPostProcessOutput, ExportPipelineError> {
    process_usm_input_with_metrics(
        &UsmProcessingInput::Path(usm_file.to_path_buf()),
        export_path,
        region,
        ffmpeg_path,
        media_backend,
        retry,
        1,
        1,
    )
    .await
}

pub(super) fn handle_acb_files_owned(
    options: &OwnedAcbPostProcessOptions,
    acb_concurrency: usize,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let borrowed = AcbPostProcessOptions {
        output_dir: &options.output_dir,
        region: &options.region,
        ffmpeg_path: &options.ffmpeg_path,
        media_backend: options.media_backend,
        retry: &options.retry,
        hca_concurrency: options.hca_concurrency,
        audio_encode_concurrency: options.audio_encode_concurrency,
        cpu_budget: options.cpu_budget,
    };
    handle_acb_files(
        &borrowed,
        acb_concurrency,
        scoped_post_process,
        scoped_files,
        acb_sources,
    )
}

pub(super) fn handle_acb_files(
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_files = post_process_files_by_extension(
        options.output_dir,
        scoped_post_process,
        scoped_files,
        "acb",
    )?;
    if !options.region.export.acb.export
        || !options.region.export.acb.decode
        || (acb_files.is_empty() && acb_sources.is_empty())
    {
        return Ok(AcbPostProcessOutput::default());
    }

    if acb_files.len() + acb_sources.len() == 1 || !options.region.export.hca.decode {
        return handle_acb_files_batched(acb_files, acb_sources, options, acb_concurrency);
    }
    handle_acb_files_streaming(acb_files, acb_sources, options, acb_concurrency)
}

pub(super) fn handle_acb_files_batched(
    acb_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_inputs = acb_extraction_inputs(acb_files, acb_sources);
    let acb_file_count = acb_inputs.len();
    let output_dir = options.output_dir.to_path_buf();
    let region = options.region.clone();
    let ffmpeg_path = options.ffmpeg_path.to_string();
    let retry = options.retry.clone();
    let media_backend = options.media_backend;
    let hca_concurrency = options.hca_concurrency;
    let audio_encode_concurrency = options.audio_encode_concurrency;
    let cpu_budget = options.cpu_budget;
    let extracted = run_tasks(acb_inputs, acb_concurrency, move |acb_input| {
        let options = AcbPostProcessOptions {
            output_dir: &output_dir,
            region: &region,
            ffmpeg_path: &ffmpeg_path,
            media_backend,
            retry: &retry,
            hca_concurrency,
            audio_encode_concurrency,
            cpu_budget,
        };
        extract_acb_tracks_from_input(acb_input, &options)
    })?;
    let mut merged = AcbPostProcessOutput::default();
    merged.phase_ms.insert(
        "media_scheduler.acb_file_count".to_string(),
        acb_file_count as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.acb_worker_count".to_string(),
        acb_concurrency.max(1).min(acb_file_count) as u64,
    );
    let mut hca_tracks = Vec::new();
    let mut source_files = Vec::new();
    for output in extracted {
        merged.generated_files.extend(output.generated_files);
        merge_raw_phase_ms(&mut merged.phase_ms, &output.phase_ms);
        let track_output_dir = output.output_dir.clone();
        hca_tracks.extend(
            output
                .hca_tracks
                .into_iter()
                .map(|track| HcaTrackProcessJob {
                    track,
                    output_dir: track_output_dir.clone(),
                }),
        );
        if let Some(source_file) = output.source_file {
            source_files.push(source_file);
        }
    }

    let phase_started = Instant::now();
    let hca_output = process_hca_tracks(hca_tracks, options)?;
    merged.generated_files.extend(hca_output.generated_files);
    merge_raw_phase_ms(&mut merged.phase_ms, &hca_output.phase_ms);
    add_elapsed_phase_ms(
        &mut merged.phase_ms,
        "post_process.acb.hca_tracks_wall",
        phase_started,
    );

    for source_file in source_files {
        let phase_started = Instant::now();
        remove_export_file_if_exists(&source_file)?;
        add_elapsed_phase_ms(
            &mut merged.phase_ms,
            "post_process.acb.remove_source",
            phase_started,
        );
    }
    Ok(merged)
}

pub(super) fn handle_acb_files_streaming(
    acb_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_inputs = acb_extraction_inputs(acb_files, acb_sources);
    let acb_file_count = acb_inputs.len();
    let acb_worker_count = acb_concurrency.max(1).min(acb_file_count);
    let hca_worker_count = options.hca_concurrency.max(1);
    let queue_capacity = hca_worker_count.saturating_mul(2).max(1);
    let (track_sender, track_receiver) =
        std::sync::mpsc::sync_channel::<HcaTrackProcessJob>(queue_capacity);
    let track_receiver = Arc::new(Mutex::new(track_receiver));
    let results = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let phase_ms = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let source_files = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let first_error = Arc::new(Mutex::new(None::<ExportPipelineError>));
    let hca_track_count = Arc::new(AtomicUsize::new(0));
    let hca_started = Instant::now();
    let mut hca_handles = Vec::with_capacity(hca_worker_count);

    for _ in 0..hca_worker_count {
        let track_receiver = track_receiver.clone();
        let results = results.clone();
        let phase_ms = phase_ms.clone();
        let first_error = first_error.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let audio_encode_concurrency = options.audio_encode_concurrency;
        let cpu_budget = options.cpu_budget;
        let handle = std::thread::Builder::new()
            .name("hca-memory-export".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_track = track_receiver.lock().unwrap().recv();
                let Ok(track_job) = next_track else {
                    break;
                };

                let track_options = HcaTrackProcessOptions {
                    output_dir: &track_job.output_dir,
                    region: &region,
                    ffmpeg_path: &ffmpeg_path,
                    media_backend,
                    retry: &retry,
                    audio_encode_concurrency,
                    cpu_budget,
                };
                match process_hca_track(track_job.track, &track_options) {
                    Ok(track_output) => {
                        results.lock().unwrap().extend(track_output.generated_files);
                        merge_raw_phase_ms(&mut phase_ms.lock().unwrap(), &track_output.phase_ms);
                    }
                    Err(err) => {
                        set_first_error(&first_error, err);
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::Io {
                path: output_dir_for_error,
                source,
            })?;
        hca_handles.push(handle);
    }

    let acb_queue = Arc::new(Mutex::new(VecDeque::from(acb_inputs)));
    let mut acb_handles = Vec::with_capacity(acb_worker_count);
    for _ in 0..acb_worker_count {
        let acb_queue = acb_queue.clone();
        let track_sender = track_sender.clone();
        let results = results.clone();
        let phase_ms = phase_ms.clone();
        let source_files = source_files.clone();
        let first_error = first_error.clone();
        let hca_track_count = hca_track_count.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let worker_output_dir = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let hca_concurrency = options.hca_concurrency;
        let audio_encode_concurrency = options.audio_encode_concurrency;
        let cpu_budget = options.cpu_budget;
        let handle = std::thread::Builder::new()
            .name("acb-track-extract".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_acb = acb_queue.lock().unwrap().pop_front();
                let Some(acb_input) = next_acb else {
                    break;
                };

                let worker_options = AcbPostProcessOptions {
                    output_dir: &worker_output_dir,
                    region: &region,
                    ffmpeg_path: &ffmpeg_path,
                    media_backend,
                    retry: &retry,
                    hca_concurrency,
                    audio_encode_concurrency,
                    cpu_budget,
                };
                match extract_acb_tracks_from_input(acb_input, &worker_options) {
                    Ok(output) => {
                        results.lock().unwrap().extend(output.generated_files);
                        merge_raw_phase_ms(&mut phase_ms.lock().unwrap(), &output.phase_ms);
                        if let Some(source_file) = output.source_file {
                            source_files.lock().unwrap().push(source_file);
                        }
                        let track_output_dir = output.output_dir;
                        for track in output.hca_tracks {
                            hca_track_count.fetch_add(1, Ordering::Relaxed);
                            let job = HcaTrackProcessJob {
                                track,
                                output_dir: track_output_dir.clone(),
                            };
                            if !send_hca_track(&track_sender, job, &first_error) {
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        set_first_error(&first_error, err);
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::Io {
                path: output_dir_for_error,
                source,
            })?;
        acb_handles.push(handle);
    }
    drop(track_sender);

    for handle in acb_handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: "acb track extract".to_string(),
                message: panic_message(panic),
            })?;
    }
    for handle in hca_handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: "hca memory export".to_string(),
                message: panic_message(panic),
            })?;
    }

    if let Some(err) = first_error.lock().unwrap().take() {
        return Err(err);
    }

    let mut merged = AcbPostProcessOutput::default();
    merged.phase_ms.insert(
        "media_scheduler.acb_file_count".to_string(),
        acb_file_count as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.acb_worker_count".to_string(),
        acb_worker_count as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.hca_track_count".to_string(),
        hca_track_count.load(Ordering::Relaxed) as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.hca_worker_count".to_string(),
        hca_worker_count as u64,
    );
    merge_raw_phase_ms(&mut merged.phase_ms, &phase_ms.lock().unwrap());
    add_elapsed_phase_ms(
        &mut merged.phase_ms,
        "post_process.acb.hca_tracks_wall",
        hca_started,
    );
    merged.generated_files = results.lock().unwrap().clone();

    for source_file in source_files.lock().unwrap().iter() {
        let phase_started = Instant::now();
        remove_export_file_if_exists(source_file)?;
        add_elapsed_phase_ms(
            &mut merged.phase_ms,
            "post_process.acb.remove_source",
            phase_started,
        );
    }
    Ok(merged)
}

pub(super) fn set_first_error(
    first_error: &Arc<Mutex<Option<ExportPipelineError>>>,
    err: ExportPipelineError,
) {
    let mut first = first_error.lock().unwrap();
    if first.is_none() {
        *first = Some(err);
    }
}

pub(super) fn send_hca_track(
    sender: &std::sync::mpsc::SyncSender<HcaTrackProcessJob>,
    track: HcaTrackProcessJob,
    first_error: &Arc<Mutex<Option<ExportPipelineError>>>,
) -> bool {
    let mut track = Some(track);
    loop {
        if first_error.lock().unwrap().is_some() {
            return false;
        }
        match sender.try_send(track.take().expect("track is retained until sent")) {
            Ok(()) => return true,
            Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                track = Some(returned);
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return false,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AcbPostProcessOutput {
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) phase_ms: HashMap<String, u64>,
}

pub(super) struct HcaTrackProcessJob {
    pub(super) track: cridecoder::ExtractedAcbTrack,
    pub(super) output_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub(super) enum AcbExtractionInput {
    File(PathBuf),
    Memory(NativeInMemoryMediaSource),
}

#[derive(Clone)]
pub(super) struct OwnedAcbPostProcessOptions {
    pub(super) output_dir: PathBuf,
    pub(super) region: RegionConfig,
    pub(super) ffmpeg_path: String,
    pub(super) media_backend: MediaBackend,
    pub(super) retry: crate::core::config::RetryConfig,
    pub(super) hca_concurrency: usize,
    pub(super) audio_encode_concurrency: usize,
    pub(super) cpu_budget: usize,
}

#[derive(Clone)]
pub(super) struct AcbPostProcessOptions<'a> {
    pub(super) output_dir: &'a Path,
    pub(super) region: &'a RegionConfig,
    pub(super) ffmpeg_path: &'a str,
    pub(super) media_backend: MediaBackend,
    pub(super) retry: &'a crate::core::config::RetryConfig,
    pub(super) hca_concurrency: usize,
    pub(super) audio_encode_concurrency: usize,
    pub(super) cpu_budget: usize,
}

#[derive(Debug, Default)]
pub(super) struct AcbTrackExtractionOutput {
    pub(super) hca_tracks: Vec<cridecoder::ExtractedAcbTrack>,
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) source_file: Option<PathBuf>,
    pub(super) output_dir: PathBuf,
    pub(super) phase_ms: HashMap<String, u64>,
}

pub(super) fn acb_extraction_inputs(
    acb_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Vec<AcbExtractionInput> {
    acb_files
        .into_iter()
        .map(AcbExtractionInput::File)
        .chain(acb_sources.into_iter().map(AcbExtractionInput::Memory))
        .collect()
}

pub(super) fn extract_acb_tracks_from_input(
    input: AcbExtractionInput,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    match input {
        AcbExtractionInput::File(acb_file) => extract_acb_tracks_from_file(&acb_file, options),
        AcbExtractionInput::Memory(source) => {
            extract_acb_tracks_from_memory_source(source, options)
        }
    }
}

pub(super) fn extract_acb_tracks_from_file(
    acb_file: &Path,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    let phase_started = Instant::now();
    let acb_reader = std::fs::File::open(acb_file).map_err(|source| ExportPipelineError::Io {
        path: acb_file.to_path_buf(),
        source,
    })?;
    let open_file_ms = phase_started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;

    let mut output = extract_acb_tracks_from_reader(
        acb_reader,
        acb_file,
        Some(acb_file.to_path_buf()),
        options,
    )?;
    *output
        .phase_ms
        .entry("post_process.acb.open_file".to_string())
        .or_default() += open_file_ms;
    Ok(output)
}

pub(super) fn extract_acb_tracks_from_memory_source(
    source: NativeInMemoryMediaSource,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    extract_acb_tracks_from_reader(Cursor::new(source.payload), &source.target, None, options)
}

pub(super) fn extract_acb_tracks_from_reader<R>(
    acb_reader: R,
    source_hint: &Path,
    source_file: Option<PathBuf>,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError>
where
    R: Read + Seek,
{
    let output_dir = source_hint
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| options.output_dir.to_path_buf());
    let mut output = AcbTrackExtractionOutput {
        source_file,
        output_dir,
        ..AcbTrackExtractionOutput::default()
    };

    let phase_started = Instant::now();
    let mut hca_tracks = codec::export_acb_to_memory(acb_reader, Some(source_hint))?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.extract_tracks",
        phase_started,
    );

    let phase_started = Instant::now();
    let acb_path_lower = source_hint
        .to_string_lossy()
        .replace('\\', "/")
        .to_lowercase();
    if acb_path_lower.contains("music/long") {
        hca_tracks.retain(|track| should_keep_music_long_hca_track(&track.name, &track.extension));
    }
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.filter_tracks",
        phase_started,
    );

    if !options.region.export.hca.decode {
        return Ok(output);
    }

    output.hca_tracks = hca_tracks;
    Ok(output)
}

pub(super) fn should_keep_music_long_hca_track(name: &str, extension: &str) -> bool {
    let lower = format!("{name}.{extension}").to_lowercase();
    !(lower.ends_with("_vr.hca") || lower.ends_with("_screen.hca"))
}

pub(super) fn process_hca_tracks(
    mut hca_tracks: Vec<HcaTrackProcessJob>,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let mut output = AcbPostProcessOutput::default();
    if hca_tracks.is_empty() {
        return Ok(output);
    }
    output.phase_ms.insert(
        "media_scheduler.hca_track_count".to_string(),
        hca_tracks.len() as u64,
    );

    if hca_tracks.len() == 1 {
        output
            .phase_ms
            .insert("media_scheduler.hca_worker_count".to_string(), 1);
        let track = hca_tracks.pop().expect("single track is present");
        let track_output = process_hca_track_job_on_large_stack(track, options)?;
        output.generated_files.extend(track_output.generated_files);
        merge_raw_phase_ms(&mut output.phase_ms, &track_output.phase_ms);
        return Ok(output);
    }

    let worker_count = options.hca_concurrency.max(1).min(hca_tracks.len());
    output.phase_ms.insert(
        "media_scheduler.hca_worker_count".to_string(),
        worker_count as u64,
    );
    let queue = Arc::new(Mutex::new(VecDeque::from(hca_tracks)));
    let results = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let phase_ms = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let first_error = Arc::new(Mutex::new(None::<ExportPipelineError>));
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let queue = queue.clone();
        let results = results.clone();
        let phase_ms = phase_ms.clone();
        let first_error = first_error.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let audio_encode_concurrency = options.audio_encode_concurrency;
        let cpu_budget = options.cpu_budget;
        let handle = std::thread::Builder::new()
            .name("hca-memory-export".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_track = queue.lock().unwrap().pop_front();
                let Some(track_job) = next_track else {
                    break;
                };

                let track_options = HcaTrackProcessOptions {
                    output_dir: &track_job.output_dir,
                    region: &region,
                    ffmpeg_path: &ffmpeg_path,
                    media_backend,
                    retry: &retry,
                    audio_encode_concurrency,
                    cpu_budget,
                };
                match process_hca_track(track_job.track, &track_options) {
                    Ok(track_output) => {
                        results.lock().unwrap().extend(track_output.generated_files);
                        merge_raw_phase_ms(&mut phase_ms.lock().unwrap(), &track_output.phase_ms);
                    }
                    Err(err) => {
                        *first_error.lock().unwrap() = Some(err);
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::Io {
                path: output_dir_for_error,
                source,
            })?;
        handles.push(handle);
    }

    for handle in handles {
        if let Err(payload) = handle.join() {
            return Err(ExportPipelineError::Io {
                path: options.output_dir.to_path_buf(),
                source: std::io::Error::other(format!("hca worker panicked: {payload:?}")),
            });
        }
    }

    if let Some(err) = first_error.lock().unwrap().take() {
        return Err(err);
    }
    output.generated_files = results.lock().unwrap().clone();
    merge_raw_phase_ms(&mut output.phase_ms, &phase_ms.lock().unwrap());
    Ok(output)
}

#[derive(Debug, Default)]
pub(super) struct HcaTrackProcessOutput {
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) phase_ms: HashMap<String, u64>,
}

pub(super) fn process_hca_track_job_on_large_stack(
    track: HcaTrackProcessJob,
    options: &AcbPostProcessOptions<'_>,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let output_dir_for_error = track.output_dir.clone();
    let region = options.region.clone();
    let ffmpeg_path = options.ffmpeg_path.to_string();
    let media_backend = options.media_backend;
    let retry = options.retry.clone();
    let audio_encode_concurrency = options.audio_encode_concurrency;
    let cpu_budget = options.cpu_budget;
    let handle = std::thread::Builder::new()
        .name("hca-memory-export".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let track_options = HcaTrackProcessOptions {
                output_dir: &track.output_dir,
                region: &region,
                ffmpeg_path: &ffmpeg_path,
                media_backend,
                retry: &retry,
                audio_encode_concurrency,
                cpu_budget,
            };
            process_hca_track(track.track, &track_options)
        })
        .map_err(|source| ExportPipelineError::Io {
            path: output_dir_for_error,
            source,
        })?;
    handle
        .join()
        .map_err(|panic| ExportPipelineError::WorkerPanic {
            worker: "hca memory export".to_string(),
            message: panic_message(panic),
        })?
}

pub(super) struct HcaTrackProcessOptions<'a> {
    pub(super) output_dir: &'a Path,
    pub(super) region: &'a RegionConfig,
    pub(super) ffmpeg_path: &'a str,
    pub(super) media_backend: MediaBackend,
    pub(super) retry: &'a crate::core::config::RetryConfig,
    pub(super) audio_encode_concurrency: usize,
    pub(super) cpu_budget: usize,
}

pub(super) fn record_hca_media_encode_acquire(
    phase_ms: &mut HashMap<String, u64>,
    encode_slot: &MediaEncodeAcquire,
) {
    debug_assert_eq!(encode_slot.kind, MediaEncodeKind::Audio);
    add_phase_ms(
        phase_ms,
        "post_process.hca.audio_pool_wait",
        encode_slot.wait_ms,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.audio_encode_wait",
        encode_slot.wait_ms,
    );
    record_max_phase_ms(
        phase_ms,
        "media_scheduler.audio_encode_active_peak",
        encode_slot.active as u64,
    );
    add_phase_ms(
        phase_ms,
        // Compatibility metric for older bench readers.
        "media_scheduler.media_encode_wait",
        encode_slot.wait_ms,
    );
    record_max_phase_ms(
        phase_ms,
        "media_scheduler.media_encode_active_peak",
        encode_slot.active as u64,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.cpu_budget_wait",
        encode_slot.cpu_budget_wait_ms,
    );
    add_phase_ms(phase_ms, "cpu_budget.wait", encode_slot.cpu_budget_wait_ms);
}

pub(super) fn record_usm_video_encode_acquire(
    phase_ms: &mut HashMap<String, u64>,
    encode_slot: &MediaEncodeAcquire,
) {
    debug_assert_eq!(encode_slot.kind, MediaEncodeKind::Video);
    add_phase_ms(
        phase_ms,
        "post_process.usm.video_pool_wait",
        encode_slot.wait_ms,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.video_encode_wait",
        encode_slot.wait_ms,
    );
    record_max_phase_ms(
        phase_ms,
        "media_scheduler.video_encode_active_peak",
        encode_slot.active as u64,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.cpu_budget_wait",
        encode_slot.cpu_budget_wait_ms,
    );
    add_phase_ms(phase_ms, "cpu_budget.wait", encode_slot.cpu_budget_wait_ms);
}

pub(super) fn process_hca_track(
    track: cridecoder::ExtractedAcbTrack,
    options: &HcaTrackProcessOptions<'_>,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let mut output = HcaTrackProcessOutput::default();
    let hca_name = format!("{}.{}", track.name, track.extension);
    if !track.extension.eq_ignore_ascii_case("hca") {
        let phase_started = Instant::now();
        let output_path = options.output_dir.join(hca_name);
        std::fs::write(&output_path, track.data).map_err(|source| ExportPipelineError::Io {
            path: output_path.clone(),
            source,
        })?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.write_non_hca",
            phase_started,
        );
        output.generated_files.push(output_path);
        return Ok(output);
    }

    let audio_formats = options.region.export.audio.output_formats();
    if audio_formats.is_empty() {
        return Ok(output);
    }
    let keep_wav = audio_formats.contains(&AudioOutputFormat::Wav);
    let encode_mp3 = audio_formats.contains(&AudioOutputFormat::Mp3);
    let encode_flac = audio_formats.contains(&AudioOutputFormat::Flac);
    let needs_wav_bytes = (keep_wav && (encode_mp3 || encode_flac)) || (encode_mp3 && encode_flac);
    let wav_file = options.output_dir.join(format!("{}.wav", track.name));

    if keep_wav && !encode_mp3 && !encode_flac {
        let cpu_slot = acquire_cpu_budget_permit_blocking(options.cpu_budget)?;
        add_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.cpu_budget_wait",
            cpu_slot.wait_ms,
        );
        add_phase_ms(&mut output.phase_ms, "cpu_budget.wait", cpu_slot.wait_ms);
        let phase_started = Instant::now();
        codec::decode_hca_bytes_to_wav(&track.data, &wav_file)?;
        drop(cpu_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.decode_write_wav",
            phase_started,
        );
        output.generated_files.push(wav_file);
        return Ok(output);
    }

    if !encode_mp3 && !encode_flac {
        return Ok(output);
    }

    let wav_bytes = if needs_wav_bytes {
        let cpu_slot = acquire_cpu_budget_permit_blocking(options.cpu_budget)?;
        add_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.cpu_budget_wait",
            cpu_slot.wait_ms,
        );
        add_phase_ms(&mut output.phase_ms, "cpu_budget.wait", cpu_slot.wait_ms);
        let phase_started = Instant::now();
        let wav_bytes = codec::decode_hca_bytes_to_wav_bytes(&track.data)?;
        drop(cpu_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.decode_wav",
            phase_started,
        );

        if keep_wav {
            let phase_started = Instant::now();
            std::fs::write(&wav_file, &wav_bytes).map_err(|source| ExportPipelineError::Io {
                path: wav_file.clone(),
                source,
            })?;
            add_elapsed_phase_ms(
                &mut output.phase_ms,
                "post_process.hca.write_wav",
                phase_started,
            );
            output.generated_files.push(wav_file.clone());
        }
        Some(wav_bytes)
    } else {
        None
    };

    if encode_mp3 {
        let mp3 = options.output_dir.join(format!("{}.mp3", track.name));
        let encode_slot = acquire_media_encode_permit(
            MediaEncodeKind::Audio,
            options.audio_encode_concurrency,
            options.cpu_budget,
        )?;
        record_hca_media_encode_acquire(&mut output.phase_ms, &encode_slot);
        let phase_started = Instant::now();
        if let Some(wav_bytes) = wav_bytes.as_deref() {
            convert_wav_bytes_to_mp3_with_backend(
                wav_bytes,
                &mp3,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        } else {
            convert_hca_bytes_to_mp3_with_backend(
                &track.data,
                &mp3,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        }
        drop(encode_slot.cpu_permit);
        drop(encode_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.convert_mp3",
            phase_started,
        );
        output.generated_files.push(mp3);
    }

    if encode_flac {
        let flac = options.output_dir.join(format!("{}.flac", track.name));
        let encode_slot = acquire_media_encode_permit(
            MediaEncodeKind::Audio,
            options.audio_encode_concurrency,
            options.cpu_budget,
        )?;
        record_hca_media_encode_acquire(&mut output.phase_ms, &encode_slot);
        let phase_started = Instant::now();
        if let Some(wav_bytes) = wav_bytes.as_deref() {
            convert_wav_bytes_to_flac_with_backend(
                wav_bytes,
                &flac,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        } else {
            convert_hca_bytes_to_flac_with_backend(
                &track.data,
                &flac,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        }
        drop(encode_slot.cpu_permit);
        drop(encode_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.convert_flac",
            phase_started,
        );
        output.generated_files.push(flac);
    }

    Ok(output)
}
