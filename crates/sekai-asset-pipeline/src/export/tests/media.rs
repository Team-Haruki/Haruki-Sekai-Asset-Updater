//! Post-processing audio and video.

use std::fs;
use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    AudioFormat as AudioOutputFormat, MediaBackend, RetryOptions as RetryConfig,
    VideoFormat as VideoOutputFormat,
};

use super::super::media_postprocess::acb::{
    handle_acb_files, share_acb_waveforms, should_keep_music_long_hca_track,
};
use super::super::media_postprocess::encode_slots::{acquire_media_encode_permit, MediaEncodeKind};
use super::super::media_postprocess::hca::{process_hca_track, process_hca_tracks};
use super::super::media_postprocess::model::{
    AcbPostProcessOptions, HcaTrackProcessJob, HcaTrackProcessOptions, SharedAcbTrack,
};
use super::super::media_postprocess::usm::{
    convert_usm_m2v_bytes, convert_usm_m2v_path, has_extension, process_direct_usm_path,
    process_usm_file, process_usm_input_with_metrics, usm_frame_rate, usm_input_has_crid_magic,
    write_usm_streams, UsmPostProcessOutput,
};
use super::super::media_postprocess::{
    handle_usm_files, post_process_exported_files, scoped_upload_files,
};
use super::super::tasks::UsmProcessingInput;
use super::super::types::NativeInMemoryMediaSource;
use super::support::*;

#[test]
fn usm_post_process_skips_non_crid_inputs() {
    let dir = tempdir().unwrap();
    let usm = dir.path().join("not_really_usm.usm");
    fs::write(&usm, b"not-crid").unwrap();

    let region = processing_pipeline_options().region;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let output = runtime
        .block_on(process_usm_input_with_metrics(
            &UsmProcessingInput::Path(usm.clone()),
            dir.path(),
            &region,
            "ffmpeg",
            MediaBackend::Ffi,
            &RetryConfig::default(),
            1,
            1,
        ))
        .unwrap();

    assert!(usm.exists());
    assert_eq!(output.generated_files, vec![usm]);
}

#[test]
fn in_memory_non_crid_usm_cleans_merged_sources() {
    let dir = tempdir().unwrap();
    let first = dir.path().join("segment_001.usm");
    let second = dir.path().join("segment_002.usm");
    fs::write(&first, b"first").unwrap();
    fs::write(&second, b"second").unwrap();
    let input = UsmProcessingInput::Bytes {
        output_dir: dir.path().to_path_buf(),
        output_name: "segment".to_string(),
        fallback_name: "segment.usm".to_string(),
        data: b"not-crid".to_vec(),
        source_files: vec![first.clone(), second.clone()],
    };
    let region = processing_pipeline_options().region;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let output = runtime
        .block_on(process_usm_input_with_metrics(
            &input,
            dir.path(),
            &region,
            "ffmpeg",
            MediaBackend::Ffi,
            &RetryConfig::default(),
            1,
            1,
        ))
        .unwrap();

    assert!(output.generated_files.is_empty());
    assert!(!first.exists());
    assert!(!second.exists());
}

#[test]
fn scoped_upload_inventory_excludes_unrelated_and_removed_files() {
    let dir = tempdir().unwrap();
    let scoped = dir.path().join("scoped.json");
    let generated = dir.path().join("generated.png");
    let removed_source = dir.path().join("source.usm");
    let unrelated = dir.path().join("other-bundle.json");
    fs::write(&scoped, b"{}").unwrap();
    fs::write(&generated, b"png").unwrap();
    fs::write(&unrelated, b"{}").unwrap();

    let files = scoped_upload_files(
        &[scoped.clone(), removed_source, generated.clone()],
        std::slice::from_ref(&generated),
    );

    assert_eq!(files, vec![generated, scoped]);
    assert!(!files.contains(&unrelated));
}

#[test]
fn post_process_sample_files_without_transcoding_if_present() {
    std::thread::Builder::new()
        .name("export-pipeline-sample".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let Some(source_usm) = sample_path("0703.usm") else {
                return;
            };
            let Some(source_acb) = sample_path("se_0126_01.acb") else {
                return;
            };
            if !source_usm.exists() || !source_acb.exists() {
                return;
            }

            let dir = tempdir().unwrap();
            let usm = dir.path().join("0703.usm");
            let acb = dir.path().join("se_0126_01.acb");
            fs::copy(source_usm, &usm).unwrap();
            fs::copy(source_acb, &acb).unwrap();

            let options = processing_pipeline_options();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let summary = runtime
                .block_on(post_process_exported_files(
                    &options,
                    dir.path(),
                    false,
                    &[],
                    Vec::new(),
                ))
                .unwrap();

            assert!(dir.path().join("0703.m2v").exists());
            assert!(dir.path().join("se_0126_01_BGM.wav").exists());
            assert!(!summary.generated_files.is_empty());
            assert_eq!(
                summary
                    .post_process_phase_ms
                    .get("media_scheduler.usm_file_count"),
                Some(&1)
            );
            assert_eq!(
                summary
                    .post_process_phase_ms
                    .get("media_scheduler.usm_worker_count"),
                Some(&1)
            );
            assert!(summary
                .post_process_phase_ms
                .contains_key("post_process.usm.extract"));
            assert!(summary
                .post_process_phase_ms
                .contains_key("post_process.acb.hca_tracks_wall"));
            assert!(summary
                .post_process_phase_ms
                .contains_key("media_scheduler.hca_track_count"));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn direct_usm_to_mp4_uses_input_stem_for_output_name() {
    std::thread::Builder::new()
        .name("direct-usm-output-name".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let dir = tempdir().unwrap();
            let usm = dir.path().join("0703.usm");
            fs::write(&usm, b"CRID synthetic direct-conversion input").unwrap();
            let script_path = fake_ffmpeg_script(dir.path());

            let mut region = processing_pipeline_options().region;
            region.export.video.formats = vec![VideoOutputFormat::Mp4];
            region.export.video.direct_mp4 = true;

            let runtime = tokio::runtime::Runtime::new().unwrap();
            let generated = runtime
                .block_on(process_usm_file(
                    &usm,
                    dir.path(),
                    &region,
                    &script_path.to_string_lossy(),
                    MediaBackend::Cli,
                    &RetryConfig {
                        attempts: 1,
                        initial_backoff_ms: 1,
                        max_backoff_ms: 1,
                    },
                    1,
                    1,
                ))
                .unwrap();

            assert!(dir.path().join("0703.mp4").exists());
            assert!(!dir
                .path()
                .join("0312_バイオレンストリガー_ゲーム尺.mp4")
                .exists());
            assert_eq!(generated, vec![dir.path().join("0703.mp4")]);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn synthetic_hca_tracks_cover_audio_plans_and_parallel_workers() {
    let dir = tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_script(dir.path());
    let retry = RetryConfig {
        attempts: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
    };
    let mut region = processing_pipeline_options().region;

    let non_hca_jobs = ["first", "second"]
        .into_iter()
        .map(|name| HcaTrackProcessJob {
            track: SharedAcbTrack {
                name: name.to_string(),
                extension: "adx".to_string(),
                data: Arc::from(&b"raw track"[..]),
            },
            output_dir: dir.path().to_path_buf(),
        })
        .collect();
    let options = AcbPostProcessOptions {
        output_dir: dir.path(),
        region: &region,
        ffmpeg_path: &ffmpeg.to_string_lossy(),
        media_backend: MediaBackend::Cli,
        retry: &retry,
        hca_concurrency: 2,
        audio_encode_concurrency: 2,
        cpu_budget: 2,
    };
    let output = process_hca_tracks(non_hca_jobs, &options).unwrap();
    assert_eq!(output.generated_files.len(), 2);
    assert_eq!(output.phase_ms["media_scheduler.hca_worker_count"], 2);

    let hca = Arc::<[u8]>::from(synthetic_hca());
    region.export.audio.formats.clear();
    let no_format_options = HcaTrackProcessOptions {
        output_dir: dir.path(),
        region: &region,
        ffmpeg_path: &ffmpeg.to_string_lossy(),
        media_backend: MediaBackend::Cli,
        retry: &retry,
        audio_encode_concurrency: 1,
        cpu_budget: 1,
    };
    let output = process_hca_track(
        SharedAcbTrack {
            name: "disabled".to_string(),
            extension: "hca".to_string(),
            data: hca.clone(),
        },
        &no_format_options,
    )
    .unwrap();
    assert!(output.generated_files.is_empty());

    region.export.audio.formats = vec![
        AudioOutputFormat::Wav,
        AudioOutputFormat::Mp3,
        AudioOutputFormat::Flac,
    ];
    let all_format_options = AcbPostProcessOptions {
        output_dir: dir.path(),
        region: &region,
        ffmpeg_path: &ffmpeg.to_string_lossy(),
        media_backend: MediaBackend::Cli,
        retry: &retry,
        hca_concurrency: 1,
        audio_encode_concurrency: 1,
        cpu_budget: 1,
    };
    let output = process_hca_tracks(
        vec![HcaTrackProcessJob {
            track: SharedAcbTrack {
                name: "all_formats".to_string(),
                extension: "hca".to_string(),
                data: hca.clone(),
            },
            output_dir: dir.path().to_path_buf(),
        }],
        &all_format_options,
    )
    .unwrap();
    assert_eq!(output.generated_files.len(), 3);
    for extension in ["wav", "mp3", "flac"] {
        assert!(dir.path().join(format!("all_formats.{extension}")).exists());
    }

    for (name, format, extension) in [
        ("direct_mp3", AudioOutputFormat::Mp3, "mp3"),
        ("direct_flac", AudioOutputFormat::Flac, "flac"),
    ] {
        region.export.audio.formats = vec![format];
        let options = HcaTrackProcessOptions {
            output_dir: dir.path(),
            region: &region,
            ffmpeg_path: &ffmpeg.to_string_lossy(),
            media_backend: MediaBackend::Cli,
            retry: &retry,
            audio_encode_concurrency: 1,
            cpu_budget: 1,
        };
        let output = process_hca_track(
            SharedAcbTrack {
                name: name.to_string(),
                extension: "hca".to_string(),
                data: hca.clone(),
            },
            &options,
        )
        .unwrap();
        assert_eq!(
            output.generated_files,
            vec![dir.path().join(format!("{name}.{extension}"))]
        );
    }
}

#[test]
fn synthetic_acb_inputs_cover_batched_and_streaming_processing() {
    std::thread::Builder::new()
        .name("synthetic-acb-processing".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let dir = tempdir().unwrap();
            let first = dir.path().join("first.acb");
            fs::write(&first, synthetic_acb("first", 2)).unwrap();
            let mut region = processing_pipeline_options().region;
            region.export.hca.decode = false;
            let retry = RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            };
            let options = AcbPostProcessOptions {
                output_dir: dir.path(),
                region: &region,
                ffmpeg_path: "ffmpeg",
                media_backend: MediaBackend::Cli,
                retry: &retry,
                hca_concurrency: 2,
                audio_encode_concurrency: 1,
                cpu_budget: 2,
            };
            let output =
                handle_acb_files(&options, 2, true, std::slice::from_ref(&first), vec![]).unwrap();
            assert!(output.generated_files.is_empty());
            assert!(!first.exists());

            let second = dir.path().join("second.acb");
            fs::write(&second, synthetic_acb("second", 2)).unwrap();
            let memory_target = dir.path().join("memory.acb");
            region.export.hca.decode = true;
            region.export.audio.formats = vec![AudioOutputFormat::Wav];
            let options = AcbPostProcessOptions {
                output_dir: dir.path(),
                region: &region,
                ffmpeg_path: "ffmpeg",
                media_backend: MediaBackend::Cli,
                retry: &retry,
                hca_concurrency: 2,
                audio_encode_concurrency: 1,
                cpu_budget: 2,
            };
            let output = handle_acb_files(
                &options,
                2,
                true,
                std::slice::from_ref(&second),
                vec![NativeInMemoryMediaSource {
                    target: memory_target,
                    payload: synthetic_acb("memory", 2),
                }],
            )
            .unwrap();
            assert_eq!(output.generated_files.len(), 4);
            assert_eq!(output.phase_ms["media_scheduler.acb_worker_count"], 2);
            assert_eq!(output.phase_ms["media_scheduler.hca_track_count"], 4);
            assert!(!second.exists());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn synthetic_post_process_covers_scoped_and_merged_orchestration() {
    std::thread::Builder::new()
        .name("synthetic-post-process".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let dir = tempdir().unwrap();
            let missing = dir.path().join("missing");
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let mut options = processing_pipeline_options();
            let empty = runtime
                .block_on(post_process_exported_files(
                    &options,
                    &missing,
                    false,
                    &[],
                    vec![],
                ))
                .unwrap();
            assert_eq!(empty.export_root, missing);
            assert!(empty.generated_files.is_empty());

            let acb = dir.path().join("synthetic.acb");
            let first_usm = dir.path().join("first.usm");
            let second_usm = dir.path().join("second.usm");
            fs::write(&acb, synthetic_acb("pipeline", 2)).unwrap();
            fs::write(&first_usm, b"not crid one").unwrap();
            fs::write(&second_usm, b"not crid two").unwrap();
            options.concurrency.acb = 2;
            options.concurrency.usm = 2;
            options.concurrency.hca = 2;
            options.cpu_budget = 2;
            options.region.export.audio.formats = vec![AudioOutputFormat::Wav];

            let scoped = vec![acb.clone(), first_usm.clone(), second_usm.clone()];
            let summary = runtime
                .block_on(post_process_exported_files(
                    &options,
                    dir.path(),
                    true,
                    &scoped,
                    vec![],
                ))
                .unwrap();
            assert_eq!(
                summary.post_process_phase_ms["media_scheduler.usm_file_count"],
                2
            );
            assert_eq!(
                summary.post_process_phase_ms["media_scheduler.usm_worker_count"],
                2
            );
            assert!(summary
                .post_process_phase_ms
                .contains_key("post_process.acb"));
            assert!(summary
                .generated_files
                .iter()
                .any(|path| { path.extension().and_then(|value| value.to_str()) == Some("wav") }));

            let region = options.region.clone();
            let output = runtime
                .block_on(handle_usm_files(
                    dir.path(),
                    &region,
                    "ffmpeg",
                    MediaBackend::Ffi,
                    &RetryConfig::default(),
                    2,
                    1,
                    1,
                    false,
                    &[],
                ))
                .unwrap();
            assert_eq!(output.phase_ms["media_scheduler.usm_worker_count"], 1);
            assert_eq!(output.phase_ms["media_scheduler.usm_merged_count"], 2);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn synthetic_usm_covers_memory_path_and_video_conversion_routes() {
    let dir = tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_script(dir.path());
    let retry = RetryConfig {
        attempts: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
    };
    let usm = synthetic_usm("memory_video");
    assert!(crate::codec::export_usm_to_memory(&usm, b"fallback.usm", true).is_err());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut region = processing_pipeline_options().region;
    for formats in [
        vec![VideoOutputFormat::M2v],
        vec![VideoOutputFormat::Mp4],
        vec![VideoOutputFormat::M2v, VideoOutputFormat::Mp4],
    ] {
        region.export.video.formats = formats;
        let memory_input = UsmProcessingInput::Bytes {
            output_dir: dir.path().to_path_buf(),
            output_name: "memory_video".to_string(),
            fallback_name: "memory_video.usm".to_string(),
            data: usm.clone(),
            source_files: vec![],
        };
        assert!(runtime
            .block_on(process_usm_input_with_metrics(
                &memory_input,
                dir.path(),
                &region,
                &ffmpeg.to_string_lossy(),
                MediaBackend::Cli,
                &retry,
                1,
                1,
            ))
            .is_err());
    }

    let path_input = dir.path().join("path_video.usm");
    fs::write(&path_input, &usm).unwrap();
    assert!(runtime
        .block_on(process_usm_input_with_metrics(
            &UsmProcessingInput::Path(path_input.clone()),
            dir.path(),
            &region,
            &ffmpeg.to_string_lossy(),
            MediaBackend::Cli,
            &retry,
            1,
            1,
        ))
        .is_err());
    assert!(path_input.exists());
}

#[test]
fn usm_helpers_write_streams_convert_video_and_cover_route_guards() {
    let dir = tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_script(dir.path());
    let retry = RetryConfig {
        attempts: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
    };
    let streams = vec![
        cridecoder::ExtractedUsmStream {
            name: "clip".to_string(),
            extension: "M2V".to_string(),
            data: b"video".to_vec(),
        },
        cridecoder::ExtractedUsmStream {
            name: "clip".to_string(),
            extension: "wav".to_string(),
            data: b"audio".to_vec(),
        },
    ];
    let generated = write_usm_streams(dir.path(), &streams).unwrap();
    assert_eq!(generated.len(), 2);
    assert!(has_extension(&generated[0], "m2v"));
    assert!(!has_extension(&generated[1], "m2v"));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bytes_mp4 = dir.path().join("bytes.mp4");
    let mut phase_ms = std::collections::HashMap::new();
    runtime
        .block_on(convert_usm_m2v_bytes(
            b"video",
            &bytes_mp4,
            &ffmpeg.to_string_lossy(),
            MediaBackend::Cli,
            Some(crate::FrameRate::from_tuple((30_000, 1_001))),
            &retry,
            1,
            1,
            &mut phase_ms,
        ))
        .unwrap();
    assert!(bytes_mp4.exists());
    assert!(phase_ms.contains_key("post_process.usm.convert_mp4"));

    let m2v = dir.path().join("path.m2v");
    fs::write(&m2v, b"video").unwrap();
    let path_mp4 = dir.path().join("path.mp4");
    runtime
        .block_on(convert_usm_m2v_path(
            &m2v,
            &path_mp4,
            true,
            &ffmpeg.to_string_lossy(),
            MediaBackend::Cli,
            None,
            &retry,
            1,
            1,
            &mut phase_ms,
        ))
        .unwrap();
    assert!(path_mp4.exists());
    assert!(!m2v.exists());

    let crid = dir.path().join("guard.usm");
    fs::write(&crid, b"CRIDbroken").unwrap();
    let path_input = UsmProcessingInput::Path(crid);
    let bytes_input = UsmProcessingInput::Bytes {
        output_dir: dir.path().to_path_buf(),
        output_name: "guard".to_string(),
        fallback_name: "guard.usm".to_string(),
        data: b"CRIDbroken".to_vec(),
        source_files: vec![],
    };
    assert!(usm_input_has_crid_magic(&path_input).unwrap());
    assert!(usm_input_has_crid_magic(&bytes_input).unwrap());
    assert_eq!(usm_frame_rate(&bytes_input), None);

    let mut region = processing_pipeline_options().region;
    region.export.video.formats = vec![VideoOutputFormat::M2v];
    let mut output = UsmPostProcessOutput::default();
    assert!(!runtime
        .block_on(process_direct_usm_path(
            &path_input,
            dir.path(),
            "guard",
            &region,
            &ffmpeg.to_string_lossy(),
            MediaBackend::Cli,
            &retry,
            1,
            1,
            &mut output,
        ))
        .unwrap());
    region.export.video.formats = vec![VideoOutputFormat::Mp4];
    region.export.video.direct_mp4 = true;
    assert!(!runtime
        .block_on(process_direct_usm_path(
            &bytes_input,
            dir.path(),
            "guard",
            &region,
            &ffmpeg.to_string_lossy(),
            MediaBackend::Cli,
            &retry,
            1,
            1,
            &mut output,
        ))
        .unwrap());
}

#[test]
fn music_long_hca_filter_drops_duplicate_vr_and_screen_tracks() {
    assert!(should_keep_music_long_hca_track("0001", "hca"));
    assert!(!should_keep_music_long_hca_track("0001_VR", "hca"));
    assert!(!should_keep_music_long_hca_track("0001_SCREEN", "HCA"));
}

#[test]
fn acb_cues_share_one_waveform_allocation() {
    let tracks = share_acb_waveforms(vec![cridecoder::UniqueWaveform {
        extension: "hca".to_string(),
        subkey: 0,
        data: vec![1, 2, 3, 4],
        cues: vec![
            cridecoder::AcbCueRef {
                name: "first".to_string(),
                cue_id: 1,
            },
            cridecoder::AcbCueRef {
                name: "second".to_string(),
                cue_id: 2,
            },
        ],
    }]);

    assert_eq!(tracks.len(), 2);
    assert!(Arc::ptr_eq(&tracks[0].data, &tracks[1].data));
}

#[test]
fn media_encode_limiters_are_split_by_audio_and_video() {
    let audio = acquire_media_encode_permit(MediaEncodeKind::Audio, 1, 100).unwrap();
    let video = acquire_media_encode_permit(MediaEncodeKind::Video, 1, 100).unwrap();

    assert_eq!(audio.active, 1);
    assert_eq!(video.active, 1);
}
