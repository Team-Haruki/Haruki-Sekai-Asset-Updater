//! ACB archives: extracting their waveforms and streaming them onward.

use std::collections::{HashMap, VecDeque};
use std::io::{Cursor, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{codec, ExportPipelineError};

use super::super::tasks::{
    panic_message, post_process_files_by_extension, remove_export_file_if_exists, run_tasks,
};
use super::super::types::NativeInMemoryMediaSource;
use super::hca::{process_hca_track, process_hca_tracks};
use super::model::{
    AcbExtractionInput, AcbPostProcessOptions, AcbPostProcessOutput, AcbTrackExtractionOutput,
    HcaTrackProcessJob, HcaTrackProcessOptions, HcaTrackProcessOutput, OwnedAcbPostProcessOptions,
    SharedAcbTrack,
};
use super::timing::{add_elapsed_phase_ms, merge_raw_phase_ms};

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
    let state = AcbStreamingState::default();
    let hca_started = Instant::now();
    let hca_handles =
        spawn_hca_stream_workers(hca_worker_count, track_receiver, state.clone(), options)?;

    let acb_queue = Arc::new(Mutex::new(VecDeque::from(acb_inputs)));
    let acb_handles = spawn_acb_stream_workers(
        acb_worker_count,
        acb_queue,
        track_sender.clone(),
        state.clone(),
        options,
    )?;
    drop(track_sender);
    join_stream_workers(acb_handles, "acb track extract")?;
    join_stream_workers(hca_handles, "hca memory export")?;
    finish_acb_streaming(
        state,
        acb_file_count,
        acb_worker_count,
        hca_worker_count,
        hca_started,
    )
}

#[derive(Clone, Default)]
pub(super) struct AcbStreamingState {
    pub(super) results: Arc<Mutex<Vec<PathBuf>>>,
    pub(super) phase_ms: Arc<Mutex<HashMap<String, u64>>>,
    pub(super) source_files: Arc<Mutex<Vec<PathBuf>>>,
    pub(super) first_error: Arc<Mutex<Option<ExportPipelineError>>>,
    pub(super) hca_track_count: Arc<AtomicUsize>,
}

pub(super) fn spawn_hca_stream_workers(
    worker_count: usize,
    receiver: Arc<Mutex<std::sync::mpsc::Receiver<HcaTrackProcessJob>>>,
    state: AcbStreamingState,
    options: &AcbPostProcessOptions<'_>,
) -> Result<Vec<std::thread::JoinHandle<()>>, ExportPipelineError> {
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let receiver = receiver.clone();
        let state = state.clone();
        let owned_options = OwnedAcbPostProcessOptions::from(options);
        handles.push(
            std::thread::Builder::new()
                .name("hca-memory-export".to_string())
                .stack_size(32 * 1024 * 1024)
                .spawn(move || run_hca_stream_worker(receiver, state, owned_options))
                .map_err(|source| ExportPipelineError::Io {
                    path: options.output_dir.to_path_buf(),
                    source,
                })?,
        );
    }
    Ok(handles)
}

pub(super) fn run_hca_stream_worker(
    receiver: Arc<Mutex<std::sync::mpsc::Receiver<HcaTrackProcessJob>>>,
    state: AcbStreamingState,
    options: OwnedAcbPostProcessOptions,
) {
    while state.first_error.lock().unwrap().is_none() {
        let Ok(job) = receiver.lock().unwrap().recv() else {
            break;
        };
        let track_options = HcaTrackProcessOptions {
            output_dir: &job.output_dir,
            region: &options.region,
            ffmpeg_path: &options.ffmpeg_path,
            media_backend: options.media_backend,
            retry: &options.retry,
            audio_encode_concurrency: options.audio_encode_concurrency,
            cpu_budget: options.cpu_budget,
        };
        match process_hca_track(job.track, &track_options) {
            Ok(output) => merge_hca_stream_output(&state, output),
            Err(err) => {
                set_first_error(&state.first_error, err);
                break;
            }
        }
    }
}

pub(super) fn merge_hca_stream_output(state: &AcbStreamingState, output: HcaTrackProcessOutput) {
    state.results.lock().unwrap().extend(output.generated_files);
    merge_raw_phase_ms(&mut state.phase_ms.lock().unwrap(), &output.phase_ms);
}

pub(super) fn spawn_acb_stream_workers(
    worker_count: usize,
    queue: Arc<Mutex<VecDeque<AcbExtractionInput>>>,
    sender: std::sync::mpsc::SyncSender<HcaTrackProcessJob>,
    state: AcbStreamingState,
    options: &AcbPostProcessOptions<'_>,
) -> Result<Vec<std::thread::JoinHandle<()>>, ExportPipelineError> {
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let queue = queue.clone();
        let sender = sender.clone();
        let state = state.clone();
        let owned_options = OwnedAcbPostProcessOptions::from(options);
        handles.push(
            std::thread::Builder::new()
                .name("acb-track-extract".to_string())
                .stack_size(32 * 1024 * 1024)
                .spawn(move || run_acb_stream_worker(queue, sender, state, owned_options))
                .map_err(|source| ExportPipelineError::Io {
                    path: options.output_dir.to_path_buf(),
                    source,
                })?,
        );
    }
    Ok(handles)
}

pub(super) fn run_acb_stream_worker(
    queue: Arc<Mutex<VecDeque<AcbExtractionInput>>>,
    sender: std::sync::mpsc::SyncSender<HcaTrackProcessJob>,
    state: AcbStreamingState,
    options: OwnedAcbPostProcessOptions,
) {
    while state.first_error.lock().unwrap().is_none() {
        let Some(input) = queue.lock().unwrap().pop_front() else {
            break;
        };
        let worker_options = options.as_borrowed();
        match extract_acb_tracks_from_input(input, &worker_options) {
            Ok(output) => forward_acb_stream_output(output, &sender, &state),
            Err(err) => {
                set_first_error(&state.first_error, err);
                break;
            }
        }
    }
}

pub(super) fn forward_acb_stream_output(
    output: AcbTrackExtractionOutput,
    sender: &std::sync::mpsc::SyncSender<HcaTrackProcessJob>,
    state: &AcbStreamingState,
) {
    state.results.lock().unwrap().extend(output.generated_files);
    merge_raw_phase_ms(&mut state.phase_ms.lock().unwrap(), &output.phase_ms);
    if let Some(source_file) = output.source_file {
        state.source_files.lock().unwrap().push(source_file);
    }
    for track in output.hca_tracks {
        state.hca_track_count.fetch_add(1, Ordering::Relaxed);
        let job = HcaTrackProcessJob {
            track,
            output_dir: output.output_dir.clone(),
        };
        if !send_hca_track(sender, job, &state.first_error) {
            break;
        }
    }
}

pub(super) fn join_stream_workers(
    handles: Vec<std::thread::JoinHandle<()>>,
    worker: &str,
) -> Result<(), ExportPipelineError> {
    for handle in handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: worker.to_string(),
                message: panic_message(panic),
            })?;
    }
    Ok(())
}

pub(super) fn finish_acb_streaming(
    state: AcbStreamingState,
    acb_file_count: usize,
    acb_worker_count: usize,
    hca_worker_count: usize,
    hca_started: Instant,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    if let Some(err) = state.first_error.lock().unwrap().take() {
        return Err(err);
    }
    let mut output = AcbPostProcessOutput::default();
    add_acb_streaming_metrics(
        &mut output,
        &state,
        acb_file_count,
        acb_worker_count,
        hca_worker_count,
        hca_started,
    );
    output.generated_files = std::mem::take(&mut *state.results.lock().unwrap());
    remove_acb_source_files(&state.source_files, &mut output.phase_ms)?;
    Ok(output)
}

pub(super) fn add_acb_streaming_metrics(
    output: &mut AcbPostProcessOutput,
    state: &AcbStreamingState,
    acb_file_count: usize,
    acb_worker_count: usize,
    hca_worker_count: usize,
    hca_started: Instant,
) {
    for (key, value) in [
        ("media_scheduler.acb_file_count", acb_file_count),
        ("media_scheduler.acb_worker_count", acb_worker_count),
        (
            "media_scheduler.hca_track_count",
            state.hca_track_count.load(Ordering::Relaxed),
        ),
        ("media_scheduler.hca_worker_count", hca_worker_count),
    ] {
        output.phase_ms.insert(key.to_string(), value as u64);
    }
    merge_raw_phase_ms(&mut output.phase_ms, &state.phase_ms.lock().unwrap());
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.hca_tracks_wall",
        hca_started,
    );
}

pub(super) fn remove_acb_source_files(
    source_files: &Arc<Mutex<Vec<PathBuf>>>,
    phase_ms: &mut HashMap<String, u64>,
) -> Result<(), ExportPipelineError> {
    for source_file in source_files.lock().unwrap().iter() {
        let phase_started = Instant::now();
        remove_export_file_if_exists(source_file)?;
        add_elapsed_phase_ms(phase_ms, "post_process.acb.remove_source", phase_started);
    }
    Ok(())
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

pub(crate) fn share_acb_waveforms(
    waveforms: Vec<cridecoder::UniqueWaveform>,
) -> Vec<SharedAcbTrack> {
    let mut tracks = Vec::new();
    for waveform in waveforms {
        let data = Arc::<[u8]>::from(waveform.data);
        tracks.extend(waveform.cues.into_iter().map(|cue| SharedAcbTrack {
            name: cue.name,
            extension: waveform.extension.clone(),
            data: data.clone(),
        }));
    }
    tracks
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

    let hca_tracks = extract_and_filter_acb_tracks(acb_reader, source_hint, &mut output.phase_ms)?;

    if !options.region.export.hca.decode {
        return Ok(output);
    }

    output.hca_tracks = hca_tracks;
    Ok(output)
}

pub(super) fn extract_and_filter_acb_tracks<R>(
    acb_reader: R,
    source_hint: &Path,
    phase_ms: &mut HashMap<String, u64>,
) -> Result<Vec<SharedAcbTrack>, ExportPipelineError>
where
    R: Read + Seek,
{
    let phase_started = Instant::now();
    let tracks = share_acb_waveforms(codec::export_acb_unique_to_memory(
        acb_reader,
        Some(source_hint),
    )?);
    add_elapsed_phase_ms(phase_ms, "post_process.acb.extract_tracks", phase_started);
    let phase_started = Instant::now();
    let tracks = filter_music_long_tracks(source_hint, tracks);
    add_elapsed_phase_ms(phase_ms, "post_process.acb.filter_tracks", phase_started);
    Ok(tracks)
}

pub(super) fn filter_music_long_tracks(
    source_hint: &Path,
    mut tracks: Vec<SharedAcbTrack>,
) -> Vec<SharedAcbTrack> {
    let source = source_hint
        .to_string_lossy()
        .replace('\\', "/")
        .to_lowercase();
    if source.contains("music/long") {
        tracks.retain(|track| should_keep_music_long_hca_track(&track.name, &track.extension));
    }
    tracks
}

pub(crate) fn should_keep_music_long_hca_track(name: &str, extension: &str) -> bool {
    let lower = format!("{name}.{extension}").to_lowercase();
    !(lower.ends_with("_vr.hca") || lower.ends_with("_screen.hca"))
}
