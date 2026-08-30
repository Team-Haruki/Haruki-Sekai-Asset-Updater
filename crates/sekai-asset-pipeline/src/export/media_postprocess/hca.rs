//! One HCA track: decoding it and encoding the configured audio formats.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::codec;
use crate::media::{
    convert_hca_bytes_to_flac_with_backend, convert_hca_bytes_to_mp3_with_backend,
    convert_wav_bytes_to_flac_with_backend, convert_wav_bytes_to_mp3_with_backend,
};
use crate::{AudioFormat as AudioOutputFormat, ExportPipelineError};

use super::super::limits::acquire_cpu_budget_permit_blocking;
use super::super::tasks::panic_message;
use super::encode_slots::{
    acquire_media_encode_permit, record_hca_media_encode_acquire, MediaEncodeKind,
};
use super::model::{
    AcbPostProcessOptions, AcbPostProcessOutput, HcaTrackProcessJob, HcaTrackProcessOptions,
    HcaTrackProcessOutput, OwnedAcbPostProcessOptions, SharedAcbTrack,
};
use super::timing::{add_elapsed_phase_ms, add_phase_ms, merge_raw_phase_ms};

#[derive(Clone)]
struct HcaWorkerState {
    queue: Arc<Mutex<VecDeque<HcaTrackProcessJob>>>,
    results: Arc<Mutex<Vec<PathBuf>>>,
    phase_ms: Arc<Mutex<HashMap<String, u64>>>,
    first_error: Arc<Mutex<Option<ExportPipelineError>>>,
}

pub(in crate::export) fn process_hca_tracks(
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
    let state = HcaWorkerState {
        queue: Arc::new(Mutex::new(VecDeque::from(hca_tracks))),
        results: Arc::new(Mutex::new(Vec::new())),
        phase_ms: Arc::new(Mutex::new(HashMap::new())),
        first_error: Arc::new(Mutex::new(None)),
    };
    let worker_options = OwnedAcbPostProcessOptions::from(options);
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let state = state.clone();
        let worker_options = worker_options.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let handle = std::thread::Builder::new()
            .name("hca-memory-export".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || run_hca_worker(state, worker_options))
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

    if let Some(err) = state.first_error.lock().unwrap().take() {
        return Err(err);
    }
    // Workers have joined, so this holds the only reference; move the vec out instead of cloning.
    output.generated_files = std::mem::take(&mut *state.results.lock().unwrap());
    merge_raw_phase_ms(&mut output.phase_ms, &state.phase_ms.lock().unwrap());
    Ok(output)
}

fn run_hca_worker(state: HcaWorkerState, options: OwnedAcbPostProcessOptions) {
    loop {
        if state.first_error.lock().unwrap().is_some() {
            break;
        }

        let Some(track_job) = state.queue.lock().unwrap().pop_front() else {
            break;
        };
        let track_options = HcaTrackProcessOptions {
            output_dir: &track_job.output_dir,
            region: &options.region,
            ffmpeg_path: &options.ffmpeg_path,
            media_backend: options.media_backend,
            retry: &options.retry,
            audio_encode_concurrency: options.audio_encode_concurrency,
            cpu_budget: options.cpu_budget,
        };
        match process_hca_track(track_job.track, &track_options) {
            Ok(track_output) => {
                state
                    .results
                    .lock()
                    .unwrap()
                    .extend(track_output.generated_files);
                merge_raw_phase_ms(&mut state.phase_ms.lock().unwrap(), &track_output.phase_ms);
            }
            Err(err) => {
                *state.first_error.lock().unwrap() = Some(err);
                break;
            }
        }
    }
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

pub(in crate::export) fn process_hca_track(
    track: SharedAcbTrack,
    options: &HcaTrackProcessOptions<'_>,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let mut output = HcaTrackProcessOutput::default();
    if !track.extension.eq_ignore_ascii_case("hca") {
        return write_non_hca_track(track, options);
    }

    let Some(formats) = HcaFormatPlan::from_options(options) else {
        return Ok(output);
    };
    let wav_file = options.output_dir.join(format!("{}.wav", track.name));

    if formats.wav_only() {
        decode_hca_to_wav_file(&track.data, &wav_file, options.cpu_budget, &mut output)?;
        return Ok(output);
    }

    if !formats.encodes_audio() {
        return Ok(output);
    }

    let wav_bytes = prepare_hca_wav_bytes(
        &track.data,
        &wav_file,
        formats,
        options.cpu_budget,
        &mut output,
    )?;

    if formats.encode_mp3 {
        encode_hca_mp3(&track, wav_bytes.as_deref(), options, &mut output)?;
    }
    if formats.encode_flac {
        encode_hca_flac(&track, wav_bytes.as_deref(), options, &mut output)?;
    }

    Ok(output)
}

#[derive(Clone, Copy)]
pub(super) struct HcaFormatPlan {
    pub(super) keep_wav: bool,
    pub(super) encode_mp3: bool,
    pub(super) encode_flac: bool,
}

impl HcaFormatPlan {
    fn from_options(options: &HcaTrackProcessOptions<'_>) -> Option<Self> {
        let formats = options.region.export.audio.output_formats();
        (!formats.is_empty()).then(|| Self {
            keep_wav: formats.contains(&AudioOutputFormat::Wav),
            encode_mp3: formats.contains(&AudioOutputFormat::Mp3),
            encode_flac: formats.contains(&AudioOutputFormat::Flac),
        })
    }

    fn wav_only(self) -> bool {
        self.keep_wav && !self.encode_mp3 && !self.encode_flac
    }

    fn encodes_audio(self) -> bool {
        self.encode_mp3 || self.encode_flac
    }

    fn needs_wav_bytes(self) -> bool {
        (self.keep_wav && self.encodes_audio()) || (self.encode_mp3 && self.encode_flac)
    }
}

pub(super) fn write_non_hca_track(
    track: SharedAcbTrack,
    options: &HcaTrackProcessOptions<'_>,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let mut output = HcaTrackProcessOutput::default();
    let phase_started = Instant::now();
    let output_path = options
        .output_dir
        .join(format!("{}.{}", track.name, track.extension));
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
    Ok(output)
}

pub(super) fn record_hca_cpu_wait(output: &mut HcaTrackProcessOutput, wait_ms: u64) {
    add_phase_ms(
        &mut output.phase_ms,
        "post_process.hca.cpu_budget_wait",
        wait_ms,
    );
    add_phase_ms(&mut output.phase_ms, "cpu_budget.wait", wait_ms);
}

pub(super) fn decode_hca_to_wav_file(
    data: &[u8],
    wav_file: &Path,
    cpu_budget: usize,
    output: &mut HcaTrackProcessOutput,
) -> Result<(), ExportPipelineError> {
    let cpu_slot = acquire_cpu_budget_permit_blocking(cpu_budget)?;
    record_hca_cpu_wait(output, cpu_slot.wait_ms);
    let phase_started = Instant::now();
    codec::decode_hca_bytes_to_wav(data, wav_file)?;
    drop(cpu_slot.permit);
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.hca.decode_write_wav",
        phase_started,
    );
    output.generated_files.push(wav_file.to_path_buf());
    Ok(())
}

pub(super) fn prepare_hca_wav_bytes(
    data: &[u8],
    wav_file: &Path,
    formats: HcaFormatPlan,
    cpu_budget: usize,
    output: &mut HcaTrackProcessOutput,
) -> Result<Option<Vec<u8>>, ExportPipelineError> {
    if !formats.needs_wav_bytes() {
        return Ok(None);
    }
    let cpu_slot = acquire_cpu_budget_permit_blocking(cpu_budget)?;
    record_hca_cpu_wait(output, cpu_slot.wait_ms);
    let phase_started = Instant::now();
    let wav_bytes = codec::decode_hca_bytes_to_wav_bytes(data)?;
    drop(cpu_slot.permit);
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.hca.decode_wav",
        phase_started,
    );
    if formats.keep_wav {
        let phase_started = Instant::now();
        std::fs::write(wav_file, &wav_bytes).map_err(|source| ExportPipelineError::Io {
            path: wav_file.to_path_buf(),
            source,
        })?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.write_wav",
            phase_started,
        );
        output.generated_files.push(wav_file.to_path_buf());
    }
    Ok(Some(wav_bytes))
}

pub(super) fn encode_hca_mp3(
    track: &SharedAcbTrack,
    wav_bytes: Option<&[u8]>,
    options: &HcaTrackProcessOptions<'_>,
    output: &mut HcaTrackProcessOutput,
) -> Result<(), ExportPipelineError> {
    let mp3 = options.output_dir.join(format!("{}.mp3", track.name));
    let encode_slot = acquire_media_encode_permit(
        MediaEncodeKind::Audio,
        options.audio_encode_concurrency,
        options.cpu_budget,
    )?;
    record_hca_media_encode_acquire(&mut output.phase_ms, &encode_slot);
    let phase_started = Instant::now();
    if let Some(wav_bytes) = wav_bytes {
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
    Ok(())
}

pub(super) fn encode_hca_flac(
    track: &SharedAcbTrack,
    wav_bytes: Option<&[u8]>,
    options: &HcaTrackProcessOptions<'_>,
    output: &mut HcaTrackProcessOutput,
) -> Result<(), ExportPipelineError> {
    let flac = options.output_dir.join(format!("{}.flac", track.name));
    let encode_slot = acquire_media_encode_permit(
        MediaEncodeKind::Audio,
        options.audio_encode_concurrency,
        options.cpu_budget,
    )?;
    record_hca_media_encode_acquire(&mut output.phase_ms, &encode_slot);
    let phase_started = Instant::now();
    if let Some(wav_bytes) = wav_bytes {
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
    Ok(())
}
