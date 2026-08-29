//! Post-processing audio and video.

use std::fs;
use std::sync::Arc;

use tempfile::tempdir;

use sekai_asset_pipeline::{
    MediaBackend, RetryOptions as RetryConfig, VideoFormat as VideoOutputFormat,
};

use super::super::media_postprocess::acb::{share_acb_waveforms, should_keep_music_long_hca_track};
use super::super::media_postprocess::encode_slots::{acquire_media_encode_permit, MediaEncodeKind};
use super::super::media_postprocess::usm::{process_usm_file, process_usm_input_with_metrics};
use super::super::media_postprocess::{post_process_exported_files, scoped_upload_files};
use super::super::tasks::UsmProcessingInput;
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

            let (config, region) = processing_config();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let summary = runtime
                .block_on(post_process_exported_files(
                    &config,
                    &region,
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
                let Some(source_usm) = sample_path("0703.usm") else {
                    return;
                };
                if !source_usm.exists() {
                    return;
                }

                let dir = tempdir().unwrap();
                let usm = dir.path().join("0703.usm");
                fs::copy(source_usm, &usm).unwrap();
                let script_path = dir.path().join("fake_ffmpeg.sh");

                let script = "#!/bin/sh\nset -eu\nOUT=\"\"\nfor arg in \"$@\"; do\n  OUT=\"$arg\"\ndone\n: > \"$OUT\"\n";
                fs::write(&script_path, script).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = fs::metadata(&script_path).unwrap().permissions();
                    perms.set_mode(0o755);
                    fs::set_permissions(&script_path, perms).unwrap();
                }

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
                assert!(!dir.path().join("0312_バイオレンストリガー_ゲーム尺.mp4").exists());
                assert_eq!(generated, vec![dir.path().join("0703.mp4")]);
            })
            .unwrap()
            .join()
            .unwrap();
}

/// Post-processing converts; it does not publish. With uploads enabled it
/// reports what a caller could publish and touches no storage provider; with
/// them disabled it reports nothing. Both directions matter: a list that is
/// always populated would have the caller uploading from a region that asked
/// not to be.
#[test]
fn post_processing_reports_publishable_files_without_uploading() {
    let dir = tempdir().unwrap();
    let png = dir.path().join("sample.png");
    fs::write(&png, b"not really a png").unwrap();

    let (config, mut region) = processing_config();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    region.upload.enabled = true;
    let enabled = runtime
        .block_on(post_process_exported_files(
            &config,
            &region,
            dir.path(),
            false,
            &[],
            Vec::new(),
        ))
        .unwrap();
    assert!(
        enabled.publishable_files.contains(&png),
        "an enabled region must report its files as publishable"
    );

    region.upload.enabled = false;
    let disabled = runtime
        .block_on(post_process_exported_files(
            &config,
            &region,
            dir.path(),
            false,
            &[],
            Vec::new(),
        ))
        .unwrap();
    assert!(
        disabled.publishable_files.is_empty(),
        "a region with uploads off must publish nothing"
    );
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
