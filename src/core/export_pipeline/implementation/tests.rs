#[cfg(unix)]
use super::sum_process_tree_cpu_percent;

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use sonic_rs::JsonValueTrait;
use tempfile::tempdir;

use crate::core::config::{
    AppConfig, ChartHashConfig, GitSyncConfig, ImageBackendConfig, ImageOutputFormat, MediaBackend,
    RegionConfig, RegionExportConfig, RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    RegionUploadConfig, RetryConfig, StorageConfig,
};
use crate::core::errors::ExportPipelineError;

use super::{
    acquire_cpu_budget_permit_blocking, acquire_media_encode_permit,
    assetstudio_export_type_selector, assetstudio_fix_file_name,
    assetstudio_object_mode_supported_type, assetstudio_type_selector_matches,
    convert_native_surrogate_images_to_png, extract_unity_asset_bundle,
    flush_pending_native_image_writes, get_export_group, handle_png_conversion,
    native_object_output_extension, native_object_output_path, native_read_batch_size_for_assets,
    native_read_kind_for_asset, native_skipped_unsupported_asset,
    parse_assetstudio_ffi_context_list_objects_worker_output,
    parse_assetstudio_ffi_object_read_batch_worker_output_recoverable,
    parse_assetstudio_ffi_object_read_worker_output_recoverable, parse_payload_bundle,
    parse_payload_bundle_borrowed, playable_container_output_path, post_process_exported_files,
    prepare_usm_processing_inputs, process_usm_file, process_usm_input_with_metrics,
    record_native_object_read_batch_diagnostics, run_path_tasks, safe_payload_bundle_path,
    scan_all_files, select_native_object_readable_assets, should_keep_music_long_hca_track,
    text_asset_public_bytes_target, usm_segment_key, write_assetstudio_export_manifest_entry,
    write_native_image_payload_final_files, write_native_image_payload_final_files_with_backend,
    write_native_object_payload, AssetStudioFfiAssetInfo, AssetStudioFfiObjectReadOutput,
    AssetStudioFfiObjectReadResponse, AssetStudioFfiResponse, MediaEncodeKind,
    NativeBatchPhaseStats, NativeObjectExportOptions, NativeObjectExportSummary,
    NativeObjectReadBatchParseOutput, NativeObjectReadParseResult, NativeObjectReadPlanStats,
    NativeSemanticExportPathState, UsmProcessingInput, WorkerOutput,
    ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS, NATIVE_AOT_DEFAULT_IMAGE_FORMAT,
    NATIVE_AOT_FAST_IMAGE_FORMAT, NATIVE_AOT_IMAGE_SURROGATE_FORMAT,
};

fn sample_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("HARUKI_CODEC_SAMPLE_DIR")
        .map(PathBuf::from)
        .map(|dir| dir.join(name))
}

fn processing_config() -> (AppConfig, RegionConfig) {
    let mut profile_hashes = BTreeMap::new();
    profile_hashes.insert("production".to_string(), "abc".to_string());

    let region = RegionConfig {
        enabled: true,
        provider: RegionProviderConfig::ColorfulPalette {
            asset_info_url_template:
                "https://example.com/{env}/{hash}/{asset_version}/{asset_hash}".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes,
            required_cookies: false,
            cookie_bootstrap_url: None,
        },
        runtime: RegionRuntimeConfig {
            unity_version: "2022.3.21f1".to_string(),
        },
        paths: RegionPathsConfig {
            asset_save_dir: Some("./Data/jp-assets".to_string()),
            downloaded_asset_record_file: Some(
                "./Data/jp-assets/downloaded_assets.json".to_string(),
            ),
        },
        export: RegionExportConfig {
            audio: crate::core::config::AudioExportConfig {
                formats: vec![crate::core::config::AudioOutputFormat::Wav],
            },
            video: crate::core::config::VideoExportConfig {
                formats: vec![crate::core::config::VideoOutputFormat::M2v],
                direct_mp4: false,
            },
            ..RegionExportConfig::default()
        },
        upload: RegionUploadConfig {
            enabled: false,
            providers: Vec::new(),
            public_read: crate::core::config::UploadPublicReadConfig::default(),
            remove_local_after_upload: false,
        },
        ..RegionConfig::default()
    };

    let config = AppConfig {
        backends: crate::core::config::BackendsConfig {
            media: crate::core::config::MediaBackendConfig {
                ffmpeg_path: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
                ..crate::core::config::MediaBackendConfig::default()
            },
            ..crate::core::config::BackendsConfig::default()
        },
        storage: StorageConfig {
            providers: Vec::new(),
        },
        git_sync: GitSyncConfig {
            chart_hashes: ChartHashConfig::default(),
        },
        ..AppConfig::default()
    };

    (config, region)
}

fn make_native_rgba_ir_payload(width: u32, height: u32, pixels: &[u8]) -> Vec<u8> {
    let stride = width * 4;
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_RGBA_IR_MAGIC);
    payload.extend_from_slice(&width.to_le_bytes());
    payload.extend_from_slice(&height.to_le_bytes());
    payload.extend_from_slice(&stride.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(pixels);
    payload
}

#[test]
fn get_export_group_matches_go_rules() {
    assert_eq!(get_export_group(""), "container");
    assert_eq!(get_export_group("event/center/foo"), "containerFull");
    assert_eq!(get_export_group("event/thumbnail/foo"), "containerFull");
    assert_eq!(get_export_group("gacha/icon/foo"), "containerFull");
    assert_eq!(get_export_group("fix_prefab/mc_new/x"), "containerFull");
    assert_eq!(get_export_group("mysekai/character/a"), "containerFull");
    assert_eq!(get_export_group("other/path"), "container");
}

#[test]
fn prepare_usm_processing_inputs_merges_numbered_segments() {
    let dir = tempdir().unwrap();
    let a = dir.path().join("traffic_jam-001.usm");
    let b = dir.path().join("traffic_jam-002.usm");
    let c = dir.path().join("traffic_jam-003.usm");
    fs::write(&a, b"CRI").unwrap();
    fs::write(&b, b"DPA").unwrap();
    fs::write(&c, b"YLD").unwrap();

    let prepared = prepare_usm_processing_inputs(vec![c.clone(), a.clone(), b.clone()]).unwrap();

    let merged = dir.path().join("traffic_jam.usm");
    assert_eq!(prepared.files.len(), 1);
    assert_eq!(prepared.merged_count, 3);
    match &prepared.files[0] {
        UsmProcessingInput::Bytes {
            output_dir,
            output_name,
            fallback_name,
            data,
            source_files,
        } => {
            assert_eq!(output_dir, dir.path());
            assert_eq!(output_name, "traffic_jam");
            assert_eq!(fallback_name, "traffic_jam.usm");
            assert_eq!(data, b"CRIDPAYLD");
            assert_eq!(source_files, &vec![a.clone(), b.clone(), c.clone()]);
        }
        other => panic!("expected in-memory segmented USM input, got {other:?}"),
    }
    assert!(!merged.exists());
    assert!(a.exists());
    assert!(b.exists());
    assert!(c.exists());
}

#[test]
fn prepare_usm_processing_inputs_merges_numbered_segments_with_duplicate_suffixes() {
    let dir = tempdir().unwrap();
    let a = dir.path().join("link_ppr_ed1-001.usm");
    let b = dir.path().join("link_ppr_ed1-002__dup8.usm");
    let c = dir.path().join("link_ppr_ed1-003__dup8.usm");
    fs::write(&a, b"CRID").unwrap();
    fs::write(&b, b"CONT").unwrap();
    fs::write(&c, b"TAIL").unwrap();

    assert_eq!(
        usm_segment_key(&b),
        Some((dir.path().to_path_buf(), "link_ppr_ed1".to_string(), 2))
    );

    let prepared = prepare_usm_processing_inputs(vec![c.clone(), a.clone(), b.clone()]).unwrap();

    assert_eq!(prepared.files.len(), 1);
    assert_eq!(prepared.merged_count, 3);
    match &prepared.files[0] {
        UsmProcessingInput::Bytes {
            output_dir,
            output_name,
            fallback_name,
            data,
            source_files,
        } => {
            assert_eq!(output_dir, dir.path());
            assert_eq!(output_name, "link_ppr_ed1");
            assert_eq!(fallback_name, "link_ppr_ed1.usm");
            assert_eq!(data, b"CRIDCONTTAIL");
            assert_eq!(source_files, &vec![a.clone(), b.clone(), c.clone()]);
        }
        other => panic!("expected in-memory segmented USM input, got {other:?}"),
    }
}

#[test]
fn prepare_usm_processing_inputs_keeps_non_contiguous_segments() {
    let dir = tempdir().unwrap();
    let a = dir.path().join("traffic_jam-001.usm");
    let c = dir.path().join("traffic_jam-003.usm");
    fs::write(&a, b"A").unwrap();
    fs::write(&c, b"C").unwrap();

    let prepared = prepare_usm_processing_inputs(vec![c.clone(), a.clone()]).unwrap();

    assert_eq!(
        prepared.files,
        vec![
            UsmProcessingInput::Path(a.clone()),
            UsmProcessingInput::Path(c.clone())
        ]
    );
    assert_eq!(prepared.merged_count, 0);
    assert!(a.exists());
    assert!(c.exists());
}

#[test]
fn usm_post_process_skips_non_crid_inputs() {
    let dir = tempdir().unwrap();
    let usm = dir.path().join("not_really_usm.usm");
    fs::write(&usm, b"not-crid").unwrap();

    let (_, region) = processing_config();
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
fn segmented_usm_post_process_uses_memory_without_merged_file() {
    std::thread::Builder::new()
        .name("segmented-usm-memory".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let Some(source_usm) = sample_path("0703.usm") else {
                return;
            };
            if !source_usm.exists() {
                return;
            }

            let dir = tempdir().unwrap();
            let bytes = fs::read(&source_usm).unwrap();
            let split_at = bytes.len() / 2;
            let a = dir.path().join("sample-001.usm");
            let b = dir.path().join("sample-002.usm");
            fs::write(&a, &bytes[..split_at]).unwrap();
            fs::write(&b, &bytes[split_at..]).unwrap();

            let prepared = prepare_usm_processing_inputs(vec![b.clone(), a.clone()]).unwrap();
            assert_eq!(prepared.files.len(), 1);
            assert!(matches!(
                prepared.files[0],
                UsmProcessingInput::Bytes { .. }
            ));
            assert!(!dir.path().join("sample.usm").exists());

            let (_, region) = processing_config();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let output = runtime
                .block_on(process_usm_input_with_metrics(
                    &prepared.files[0],
                    dir.path(),
                    &region,
                    "ffmpeg",
                    MediaBackend::Auto,
                    &RetryConfig::default(),
                    1,
                    1,
                ))
                .unwrap();

            assert!(!dir.path().join("sample.usm").exists());
            assert!(!a.exists());
            assert!(!b.exists());
            assert!(output.generated_files.iter().any(|path| {
                path.extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("m2v"))
                    .unwrap_or(false)
                    && path.exists()
            }));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn scan_all_files_finds_nested_files() {
    let dir = tempdir().unwrap();
    let sub = dir.path().join("sub");
    fs::create_dir_all(&sub).unwrap();
    let a = dir.path().join("a.txt");
    let b = sub.join("b.txt");
    fs::write(&a, b"a").unwrap();
    fs::write(&b, b"b").unwrap();

    let mut files = scan_all_files(dir.path()).unwrap();
    files.sort();
    assert_eq!(files, vec![a, b]);
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
                    "jp",
                    &region,
                    dir.path(),
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
fn native_backend_requires_library_path_when_selected() {
    let dir = tempdir().unwrap();
    let fake_bundle = dir.path().join("bundle.bin");
    fs::write(&fake_bundle, b"bundle").unwrap();
    let output_dir = dir.path().join("out");
    let (config, region) = processing_config();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let err = runtime
        .block_on(extract_unity_asset_bundle(
            &config,
            "jp",
            &region,
            &fake_bundle,
            "event_story/foo",
            &output_dir,
            "StartApp",
        ))
        .unwrap_err();

    assert!(matches!(
        err,
        ExportPipelineError::AssetStudioFfi { ref message }
            if message.contains("backends.asset_studio.library_path")
    ));
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

                let (_config, mut region) = processing_config();
                region.export.video.formats = vec![crate::core::config::VideoOutputFormat::Mp4];
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

#[test]
fn png_to_webp_uses_pure_rust_encoder() {
    let dir = tempdir().unwrap();
    let png = dir.path().join("sample.png");
    let image = image::RgbaImage::from_pixel(2, 3, image::Rgba([255, 0, 0, 255]));
    image.save(&png).unwrap();

    let (_config, mut region) = processing_config();
    region.export.images.formats = vec![ImageOutputFormat::Webp];

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let generated = runtime
        .block_on(handle_png_conversion(
            dir.path(),
            &[],
            &region,
            &ImageBackendConfig::default(),
            2,
            2,
            false,
        ))
        .unwrap();

    let webp = dir.path().join("sample.webp");
    assert_eq!(generated, vec![webp.clone()]);
    assert!(!png.exists());
    assert!(webp.exists());

    let decoded = image::ImageReader::open(&webp).unwrap().decode().unwrap();
    assert_eq!(decoded.width(), 2);
    assert_eq!(decoded.height(), 3);
}

#[test]
fn native_aot_default_image_format_preserves_alpha() {
    assert_eq!(NATIVE_AOT_DEFAULT_IMAGE_FORMAT, "raw_rgba");
    assert_eq!(
        NATIVE_AOT_FAST_IMAGE_FORMAT,
        NATIVE_AOT_DEFAULT_IMAGE_FORMAT
    );
    assert_eq!(NATIVE_AOT_IMAGE_SURROGATE_FORMAT, "bmp");
}

#[test]
fn native_image_format_always_uses_raw_rgba() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("normal".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 43,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    assert_eq!(
        super::native_image_format_for_asset(&asset, "raw_rgba"),
        "raw_rgba"
    );
    assert_eq!(super::native_image_format_for_asset(&asset, ""), "raw_rgba");
    assert_eq!(
        super::native_image_format_for_asset(&asset, "png"),
        "raw_rgba"
    );
}

#[test]
fn native_object_read_subchunks_split_non_bmp_images() {
    let texture = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("normal".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 10,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let sprite = AssetStudioFfiAssetInfo {
        index: 1,
        name: Some("full".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
        asset_type: Some("Sprite".to_string()),
        type_id: 213,
        path_id: 11,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let mono = AssetStudioFfiAssetInfo {
        index: 2,
        name: Some("data".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/data.json".into()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 12,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let assets = vec![&texture, &sprite, &mono];

    let source_chunks = super::native_object_read_subchunks(&assets, "raw_rgba");
    assert_eq!(source_chunks.len(), 3);
    assert_eq!(source_chunks[0][0].path_id, 10);
    assert_eq!(source_chunks[1][0].path_id, 11);
    assert_eq!(source_chunks[2][0].path_id, 12);

    let configured_chunks = super::native_object_read_subchunks(&assets, "bmp");
    assert_eq!(configured_chunks.len(), 3);
    assert_eq!(configured_chunks[0][0].path_id, 10);
    assert_eq!(configured_chunks[1][0].path_id, 11);
    assert_eq!(configured_chunks[2][0].path_id, 12);
}

#[test]
fn native_image_format_ignores_container_extension() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("banner".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/banner.jpg.bytes".into()),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 43,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    assert_eq!(
        super::native_image_format_for_asset(&asset, "raw_rgba"),
        "raw_rgba"
    );
    assert_eq!(
        super::native_image_format_for_asset(&asset, "jpg"),
        "raw_rgba"
    );
}

#[test]
fn native_raw_rgba_payload_is_encoded_to_png() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("normal.png");
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_RGBA_IR_MAGIC);
    payload.extend_from_slice(&2u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&[255, 0, 0, 255, 0, 255, 0, 128]);

    let (_config, region) = processing_config();
    let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();
    assert_eq!(written, vec![target.clone()]);
    let decoded = image::ImageReader::open(&target).unwrap().decode().unwrap();
    let rgba = decoded.to_rgba8();
    assert_eq!(rgba.width(), 2);
    assert_eq!(rgba.height(), 1);
    assert_eq!(rgba.get_pixel(0, 0).0, [255, 0, 0, 255]);
    assert_eq!(rgba.get_pixel(1, 0).0, [0, 255, 0, 128]);
}

#[test]
fn native_surrogate_bmp_is_converted_to_png() {
    let dir = tempdir().unwrap();
    let bmp = dir.path().join("sample.bmp");
    let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&bmp, image::ImageFormat::Bmp)
        .unwrap();

    let generated = convert_native_surrogate_images_to_png(dir.path(), &[], 2, 2, false).unwrap();

    let png = dir.path().join("sample.png");
    assert_eq!(generated, vec![png.clone()]);
    assert!(!bmp.exists());
    assert!(png.exists());

    let decoded = image::ImageReader::open(&png).unwrap().decode().unwrap();
    assert_eq!(decoded.width(), 3);
    assert_eq!(decoded.height(), 2);
}

#[test]
fn scoped_native_surrogate_conversion_ignores_unlisted_bmp_files() {
    let dir = tempdir().unwrap();
    let own_bmp = dir.path().join("own.bmp");
    let other_bmp = dir.path().join("other.bmp");
    let image = image::RgbaImage::from_pixel(1, 1, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&own_bmp, image::ImageFormat::Bmp)
        .unwrap();
    image
        .save_with_format(&other_bmp, image::ImageFormat::Bmp)
        .unwrap();

    let generated = convert_native_surrogate_images_to_png(
        dir.path(),
        std::slice::from_ref(&own_bmp),
        2,
        2,
        true,
    )
    .unwrap();

    assert_eq!(generated, vec![dir.path().join("own.png")]);
    assert!(!own_bmp.exists());
    assert!(other_bmp.exists());
    assert!(!dir.path().join("other.png").exists());
}

#[test]
fn surrogate_conversion_sniffs_png_payload_with_bmp_extension() {
    let dir = tempdir().unwrap();
    let disguised = dir.path().join("disguised.bmp");
    let image = image::RgbaImage::from_pixel(2, 2, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&disguised, image::ImageFormat::Png)
        .unwrap();

    let generated = convert_native_surrogate_images_to_png(
        dir.path(),
        std::slice::from_ref(&disguised),
        1,
        1,
        true,
    )
    .unwrap();

    let png = dir.path().join("disguised.png");
    assert_eq!(generated, vec![png.clone()]);
    assert!(png.exists());
    assert!(!disguised.exists());
}

#[test]
fn native_image_payload_writes_png_directly_without_bmp_surrogate() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("source.bmp");
    let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&source, image::ImageFormat::Bmp)
        .unwrap();
    let payload = fs::read(source).unwrap();
    let (_config, region) = processing_config();
    let target = dir.path().join("normal.png");

    let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();

    assert_eq!(written, vec![target.clone()]);
    assert!(target.exists());
    assert!(!dir.path().join("normal.bmp").exists());
}

#[test]
fn native_image_payload_writes_webp_from_memory_when_configured() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("source.bmp");
    let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&source, image::ImageFormat::Bmp)
        .unwrap();
    let payload = fs::read(source).unwrap();
    let (_config, mut region) = processing_config();
    region.export.images.formats = vec![ImageOutputFormat::Webp];
    let target = dir.path().join("normal.png");
    let webp = dir.path().join("normal.webp");

    let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();

    assert_eq!(written, vec![webp.clone()]);
    assert!(webp.exists());
    assert!(!target.exists());
    assert!(!dir.path().join("normal.bmp").exists());
}

#[test]
fn native_raw_rgba_payload_writes_configured_image_formats_directly() {
    let dir = tempdir().unwrap();
    let payload = make_native_rgba_ir_payload(
        2,
        2,
        &[255, 0, 0, 255, 0, 255, 0, 128, 0, 0, 255, 255, 7, 8, 9, 64],
    );
    let (_config, mut region) = processing_config();
    region.export.images.formats = vec![
        ImageOutputFormat::Png,
        ImageOutputFormat::Jpg,
        ImageOutputFormat::Webp,
    ];
    let target = dir.path().join("normal.png");
    let jpg = dir.path().join("normal.jpg");
    let webp = dir.path().join("normal.webp");

    let written = write_native_image_payload_final_files_with_backend(
        &target,
        &payload,
        &region,
        &ImageBackendConfig::default(),
    )
    .unwrap();

    assert_eq!(written, vec![target.clone(), jpg.clone(), webp.clone()]);
    assert!(target.exists());
    assert!(jpg.exists());
    assert!(webp.exists());
}

#[test]
fn native_image_object_payload_is_flushed_after_export_queue() {
    let dir = tempdir().unwrap();
    let (config, region) = processing_config();
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "character/member/test",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("normal".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/character/member/test/normal.png"
                .to_string(),
        ),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 123,
        unique_id: None,
        size: 16,
        source_file: None,
    };
    let payload = make_native_rgba_ir_payload(
        2,
        2,
        &[255, 0, 0, 255, 0, 255, 0, 255, 0, 0, 255, 255, 7, 8, 9, 255],
    );
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("image_raw_rgba".to_string()),
            payload_len: payload.len() as i64,
            suggested_extension: Some(".png".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload,
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected = dir.path().join("character/member/test/normal.png");
    assert_eq!(path_state.written_files, vec![expected.clone()]);
    assert_eq!(path_state.pending_image_writes.len(), 1);
    assert!(!expected.exists());

    let phase_ms =
        flush_pending_native_image_writes(&config, path_state.pending_image_writes).unwrap();

    assert!(expected.exists());
    assert_eq!(phase_ms.get("image_encode.count"), Some(&1));
    assert_eq!(phase_ms.get("image_encode.format.png"), Some(&1));
}

#[test]
fn text_asset_acb_payload_is_queued_as_memory_source_without_writing_file() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "sound/foo",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "bmp",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("se_0126_01".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/ondemand/sound/se_0126_01.acb.bytes".to_string(),
        ),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 123,
        unique_id: None,
        size: 4,
        source_file: None,
    };
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("text_bytes".to_string()),
            payload_len: 4,
            suggested_extension: Some(".bytes".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: b"acb!".to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected_target = dir.path().join("ondemand/sound/se_0126_01.acb");
    assert!(!expected_target.exists());
    assert!(path_state.written_files.is_empty());
    assert_eq!(path_state.acb_sources.len(), 1);
    assert_eq!(path_state.acb_sources[0].target, expected_target);
    assert_eq!(path_state.acb_sources[0].payload, b"acb!");

    assert!(!dir
        .path()
        .join(".assetstudio-export-manifest.jsonl")
        .exists());
}

#[test]
fn music_score_text_asset_manifest_uses_public_txt_extension() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "music/music_score/0002_01",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("append".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/music/music_score/0002_01/append.bytes"
                .to_string(),
        ),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 123,
        unique_id: None,
        size: 4,
        source_file: None,
    };
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("text_bytes".to_string()),
            payload_len: 4,
            suggested_extension: Some(".bytes".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: b"score".to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected = dir
        .path()
        .join("startapp/music/music_score/0002_01/append.txt");
    assert!(expected.exists());
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
    assert_eq!(
        entry.get("path").and_then(|value| value.as_str()),
        Some("startapp/music/music_score/0002_01/append.txt")
    );
    assert_eq!(
        entry
            .get("suggested_extension")
            .and_then(|value| value.as_str()),
        Some(".txt")
    );
}

#[test]
fn decoded_usm_text_asset_is_not_recorded_as_final_manifest_entry() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    region.export.usm.export = true;
    region.export.usm.decode = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "event/opening",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("opening-001.usm".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/ondemand/event/opening/opening-001.usm.bytes"
                .to_string(),
        ),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 123,
        unique_id: None,
        size: 4,
        source_file: None,
    };
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("text_bytes".to_string()),
            payload_len: 4,
            suggested_extension: Some(".bytes".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: b"usm!".to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    assert!(dir
        .path()
        .join("ondemand/event/opening/opening-001.usm")
        .exists());
    assert!(!dir
        .path()
        .join(".assetstudio-export-manifest.jsonl")
        .exists());
}

#[test]
fn assetbundle_typetree_routes_to_container_bundle_record_path() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "actionset/group0",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "bmp",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("actionset/group0".to_string()),
        container: None,
        asset_type: Some("AssetBundle".to_string()),
        type_id: 142,
        path_id: 1,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let payload = br#"{
            "m_Name":"actionset/group0",
            "m_AssetBundleName":"actionset/group0",
            "m_Container":[
                {
                    "key":"assets/sekai/assetbundle/resources/startapp/actionset/group0/as_2_007.asset",
                    "value":{"asset":{"m_FileID":0,"m_PathID":1}}
                }
            ]
        }"#;
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: payload.len() as i64,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: payload.to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected = dir.path().join("startapp/actionset/group0/_bundle.json");
    assert!(expected.exists());
    assert!(!dir.path().join("actionset/group0.json").exists());
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
    assert_eq!(
        entry.get("path").and_then(|value| value.as_str()),
        Some("startapp/actionset/group0/_bundle.json")
    );
}

#[test]
fn assetbundle_typetree_mixed_categories_use_stable_bundle_fallback_path() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "crystal_shop/thumbnail/mysekai_mission_pass5",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "bmp",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("crystal_shop/thumbnail/mysekai_mission_pass5".to_string()),
        container: None,
        asset_type: Some("AssetBundle".to_string()),
        type_id: 142,
        path_id: 1,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let payload = br#"{
            "m_Name":"crystal_shop/thumbnail/mysekai_mission_pass5",
            "m_AssetBundleName":"crystal_shop/thumbnail/mysekai_mission_pass5",
            "m_Container":[
                {
                    "key":"assets/sekai/assetbundle/resources/startapp/crystal_shop/thumbnail/mysekai_mission_pass5/banner.asset",
                    "value":{"asset":{"m_FileID":0,"m_PathID":1}}
                },
                {
                    "key":"assets/sekai/assetbundle/resources/ondemand/crystal_shop/thumbnail/mysekai_mission_pass5/detail.asset",
                    "value":{"asset":{"m_FileID":0,"m_PathID":2}}
                }
            ]
        }"#;
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: payload.len() as i64,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: payload.to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected = dir
        .path()
        .join("crystal_shop/thumbnail/mysekai_mission_pass5/_bundle.json");
    assert!(expected.exists());
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
    assert_eq!(
        entry.get("path").and_then(|value| value.as_str()),
        Some("crystal_shop/thumbnail/mysekai_mission_pass5/_bundle.json")
    );
}

#[test]
fn monoscript_typetree_routes_to_container_subasset_path() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "actionset/group0",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "bmp",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("ActionSetData".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/actionset/group0/shoppingmall_staff.asset"
                .to_string(),
        ),
        asset_type: Some("MonoScript".to_string()),
        type_id: 115,
        path_id: 2,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let payload = br#"{"m_Name":"ActionSetData"}"#;
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: payload.len() as i64,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: payload.to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected = dir
        .path()
        .join("startapp/actionset/group0/shoppingmall_staff.assets/monoscript/ActionSetData.json");
    assert!(expected.exists());
    assert!(!dir
        .path()
        .join("startapp/actionset/group0/shoppingmall_staff.json")
        .exists());
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
    assert_eq!(
        entry.get("path").and_then(|value| value.as_str()),
        Some("startapp/actionset/group0/shoppingmall_staff.assets/monoscript/ActionSetData.json")
    );
}

#[test]
fn music_long_hca_filter_drops_duplicate_vr_and_screen_tracks() {
    assert!(should_keep_music_long_hca_track("0001", "hca"));
    assert!(!should_keep_music_long_hca_track("0001_VR", "hca"));
    assert!(!should_keep_music_long_hca_track("0001_SCREEN", "HCA"));
}

#[test]
fn run_path_tasks_processes_every_input() {
    let seen = Arc::new(AtomicUsize::new(0));
    let paths = vec![PathBuf::from("a"), PathBuf::from("b"), PathBuf::from("c")];

    let generated = run_path_tasks(paths, 2, {
        let seen = seen.clone();
        move |path| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(vec![path])
        }
    })
    .unwrap();

    assert_eq!(seen.load(Ordering::SeqCst), 3);
    assert_eq!(generated.len(), 3);
}

#[test]
fn run_path_tasks_returns_first_error() {
    let err = run_path_tasks(vec![PathBuf::from("boom")], 1, |_| {
        Err(ExportPipelineError::CommandFailed {
            program: "test".to_string(),
            status: "1".to_string(),
            stderr: "failed".to_string(),
        })
    })
    .unwrap_err();

    assert!(matches!(err, ExportPipelineError::CommandFailed { .. }));
}

#[test]
fn cpu_budget_permit_limits_blocking_work() {
    let budget = 97;
    let permits = (0..budget)
        .map(|_| acquire_cpu_budget_permit_blocking(budget).unwrap().permit)
        .collect::<Vec<_>>();
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _permit = acquire_cpu_budget_permit_blocking(budget).unwrap().permit;
        tx.send(()).unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    drop(permits);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    handle.join().unwrap();
}

#[test]
fn media_encode_limiters_are_split_by_audio_and_video() {
    let audio = acquire_media_encode_permit(MediaEncodeKind::Audio, 1, 100).unwrap();
    let video = acquire_media_encode_permit(MediaEncodeKind::Video, 1, 100).unwrap();

    assert_eq!(audio.active, 1);
    assert_eq!(video.active, 1);
}

#[cfg(unix)]
#[test]
fn sums_process_tree_cpu_percent() {
    let output = "\
            100     1   1.0\n\
            101   100  20.5\n\
            102   101  30.0\n\
            103     1  99.0\n\
        ";

    assert_eq!(sum_process_tree_cpu_percent(100, output), 51.5);
}

#[test]
fn native_object_mode_supports_assetstudio_export_type_parity() {
    for asset_type in [
        "Texture2D",
        "Texture2DArray",
        "Sprite",
        "TextAsset",
        "MonoBehaviour",
        "Font",
        "Shader",
        "AudioClip",
        "VideoClip",
        "MovieTexture",
        "Mesh",
        "Animator",
        "ParticleSystem",
        "AnimatorController",
        "GameObject",
        "Material",
    ] {
        assert!(
            assetstudio_object_mode_supported_type(asset_type),
            "{asset_type} should be accepted by native object mode"
        );
    }

    assert!(!assetstudio_object_mode_supported_type(" "));
}

#[test]
fn native_object_mode_selectors_match_short_aliases_and_class_names() {
    assert!(assetstudio_type_selector_matches("tex2d", "Texture2D"));
    assert!(assetstudio_type_selector_matches(
        "monoBehaviour",
        "MonoBehaviour"
    ));
    assert!(assetstudio_type_selector_matches(
        "mono_behavior",
        "MonoBehaviour"
    ));
    assert!(assetstudio_type_selector_matches(
        "shader",
        "ShaderVariantCollection"
    ));
    assert!(assetstudio_type_selector_matches(
        "animator",
        "AnimatorController"
    ));
    assert!(assetstudio_type_selector_matches(
        "ParticleSystem",
        "ParticleSystem"
    ));
    assert!(assetstudio_type_selector_matches("all", "GameObject"));
    assert!(!assetstudio_type_selector_matches("sprite", "Texture2D"));
}

#[test]
fn native_object_mode_uses_configured_read_kind_with_specific_precedence() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("controller".to_string()),
        container: Some("assets/foo.controller".to_string()),
        asset_type: Some("AnimatorController".to_string()),
        type_id: 91,
        path_id: 7,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let mut read_kinds = BTreeMap::new();
    read_kinds.insert("all".to_string(), "raw".to_string());
    read_kinds.insert("animator".to_string(), "typetree_json".to_string());

    assert_eq!(
        native_read_kind_for_asset(&asset, &read_kinds),
        "typetree_json"
    );
}

#[test]
fn native_object_mode_defaults_read_kind_by_asset_type() {
    let mut asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("asset".to_string()),
        container: Some("assets/foo".to_string()),
        asset_type: Some("Sprite".to_string()),
        type_id: 213,
        path_id: 7,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    assert_eq!(
        native_read_kind_for_asset(&asset, &BTreeMap::new()),
        "image"
    );

    asset.asset_type = Some("TextAsset".to_string());
    assert_eq!(
        native_read_kind_for_asset(&asset, &BTreeMap::new()),
        "text_bytes"
    );

    asset.asset_type = Some("ParticleSystem".to_string());
    assert_eq!(
        native_read_kind_for_asset(&asset, &BTreeMap::new()),
        "typetree_json"
    );
}

#[test]
fn native_object_output_extension_prefers_payload_kind() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("asset".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/bar.bytes".to_string()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 7,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    assert_eq!(
        native_object_output_extension(&asset, Some("typetree_json"), Some(".bytes")),
        "json"
    );
    assert_eq!(
        native_object_output_extension(&asset, Some("raw"), Some(".json")),
        "dat"
    );
    assert_eq!(
        native_object_output_extension(&asset, Some("animator_bundle_fbx"), Some(".fbx")),
        ""
    );
}

#[test]
fn text_asset_public_bytes_target_strips_bytes_suffixes() {
    let mut asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("asset".to_string()),
        container: Some("assets/foo".to_string()),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 7,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    assert_eq!(
        text_asset_public_bytes_target(Path::new("out/foo.acb.bytes"), &asset).unwrap(),
        PathBuf::from("out/foo.acb")
    );
    assert_eq!(
        text_asset_public_bytes_target(Path::new("out/foo.usm.bytes"), &asset).unwrap(),
        PathBuf::from("out/foo.usm")
    );
    assert_eq!(
        text_asset_public_bytes_target(Path::new("out/foo.bytes"), &asset).unwrap(),
        PathBuf::from("out/foo")
    );
    assert_eq!(
        text_asset_public_bytes_target(Path::new("out/banner.jpg.bytes"), &asset).unwrap(),
        PathBuf::from("out/banner.jpg")
    );

    asset.container = Some(
        "assets/sekai/assetbundle/resources/ondemand/music/music_score/001/append.bytes"
            .to_string(),
    );
    assert_eq!(
        text_asset_public_bytes_target(
            Path::new("out/ondemand/music/music_score/001/append.bytes"),
            &asset
        )
        .unwrap(),
        PathBuf::from("out/ondemand/music/music_score/001/append.txt")
    );

    asset.asset_type = Some("MonoBehaviour".to_string());
    assert!(text_asset_public_bytes_target(Path::new("out/foo.usm.bytes"), &asset).is_none());
}

#[test]
fn mono_behaviour_primary_asset_uses_container_json_path() {
    let asset = AssetStudioFfiAssetInfo {
            index: 0,
            name: Some("005005_minori02_kari".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member/res005_no005/005005_minori02_kari.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 42,
            unique_id: None,
            size: 42,
            source_file: None,
        };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "character/member/res005_no005",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("typetree_json"),
        Some(".json"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/startapp/character/member/res005_no005/005005_minori02_kari.json")
    );
}

#[test]
fn mono_behaviour_bundledata_uses_container_json_path() {
    let asset = AssetStudioFfiAssetInfo {
            index: 0,
            name: Some("SoundBundleBuildData".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/ondemand/music/long/0001_01/soundbundlebuilddata.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 42,
            unique_id: None,
            size: 42,
            source_file: None,
        };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "music/long/0001_01",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("typetree_json"),
        Some(".json"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/ondemand/music/long/0001_01/soundbundlebuilddata.json")
    );
}

#[test]
fn mono_script_stays_in_container_subasset_path() {
    let asset = AssetStudioFfiAssetInfo {
            index: 0,
            name: Some("ScenarioSceneData".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member/res005_no005/005005_minori02_kari.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoScript".to_string()),
            type_id: 115,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "character/member/res005_no005",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("typetree_json"),
        Some(".json"),
    );

    assert_eq!(
            target,
            PathBuf::from(
                "/tmp/out/startapp/character/member/res005_no005/005005_minori02_kari.assets/monoscript/ScenarioSceneData.json"
            )
        );
}

#[test]
fn member_cutout_sprite_objects_use_resolved_cutout_path() {
    let asset = AssetStudioFfiAssetInfo {
            index: 0,
            name: Some("deck".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member_cutout/res001_no001/normal.png"
                    .to_string(),
            ),
            asset_type: Some("Sprite".to_string()),
            type_id: 213,
            path_id: 42,
            unique_id: None,
            size: 42,
            source_file: None,
        };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "character/member_cutout/res001_no001",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("image_png"),
        Some(".png"),
    );

    assert_eq!(
        target,
        PathBuf::from(
            "/tmp/out/startapp/character/member_cutout/res001_no001/normal.assets/sprite/deck.png"
        )
    );
}

#[test]
fn member_cutout_texture_objects_use_resolved_cutout_path() {
    let asset = AssetStudioFfiAssetInfo {
            index: 0,
            name: Some("normal".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member_cutout/res001_no001/normal.png"
                    .to_string(),
            ),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "character/member_cutout/res001_no001",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("image_png"),
        Some(".png"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/startapp/character/member_cutout/res001_no001/normal.png")
    );
}

#[test]
fn by_category_object_paths_follow_container_category_not_info_category() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("normal".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/mysekai/foo/normal.png".to_string(),
        ),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 43,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "mysekai/foo",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("image_png"),
        Some(".png"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/startapp/mysekai/foo/normal.png")
    );
}

#[test]
fn manifest_records_native_surrogate_image_public_png_path() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("startapp/foo/normal.bmp");
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("normal".to_string()),
        container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 43,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("image_bmp".to_string()),
            payload_len: 4,
            suggested_extension: Some(".bmp".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: Vec::new(),
    };

    write_assetstudio_export_manifest_entry(dir.path(), &target, &asset, &read_output).unwrap();

    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
    assert_eq!(
        entry.get("path").and_then(|value| value.as_str()),
        Some("startapp/foo/normal.png")
    );
    assert_eq!(
        entry
            .get("suggested_extension")
            .and_then(|value| value.as_str()),
        Some(".png")
    );
}

#[test]
fn manifest_records_animator_bundle_public_fbx_path() {
    let dir = tempdir().unwrap();
    let target = dir
        .path()
        .join("ondemand/foo/foo.assets/animator/model.prefab");
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("model".to_string()),
        container: Some("assets/sekai/assetbundle/resources/ondemand/foo/model.prefab".into()),
        asset_type: Some("Animator".to_string()),
        type_id: 95,
        path_id: 43,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
    payload.extend_from_slice(&1u32.to_le_bytes());
    let entry_name = "FBX_Animator/model/model.fbx";
    payload.extend_from_slice(&(entry_name.len() as u32).to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(entry_name.as_bytes());
    payload.extend_from_slice(b"fbx");
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("animator_bundle_fbx".to_string()),
            payload_len: payload.len() as i64,
            suggested_extension: Some(".fbx".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload,
    };

    write_assetstudio_export_manifest_entry(dir.path(), &target, &asset, &read_output).unwrap();

    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
    assert_eq!(
        entry.get("path").and_then(|value| value.as_str()),
        Some("ondemand/foo/foo.assets/animator/model/FBX_Animator/model/model.fbx")
    );
    assert_eq!(
        entry
            .get("suggested_extension")
            .and_then(|value| value.as_str()),
        Some(".fbx")
    );
}

#[test]
fn non_character_sprite_objects_route_under_container_sprite_directory() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("deck".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/event/foo/normal.png".to_string(),
        ),
        asset_type: Some("Sprite".to_string()),
        type_id: 213,
        path_id: 44,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "event/foo",
        "assets/sekai/assetbundle/resources/startapp/",
        true,
        &asset,
        Some("image_png"),
        Some(".png"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/event/foo/normal.assets/sprite/deck.png")
    );
}

#[test]
fn mesh_objects_route_under_container_mesh_directory() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("body".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/mysekai/effect/common/fbx/model.prefab"
                .to_string(),
        ),
        asset_type: Some("Mesh".to_string()),
        type_id: 43,
        path_id: 45,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "mysekai/effect/common/fbx",
        "assets/sekai/assetbundle/resources/startapp/",
        true,
        &asset,
        Some("mesh_obj"),
        Some(".obj"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/mysekai/effect/common/fbx/model.assets/mesh/body.obj")
    );
}

#[test]
fn font_objects_use_named_file_in_container_parent_directory() {
    let asset = AssetStudioFfiAssetInfo {
            index: 0,
            name: Some("FOT-RodinNTLGPro-DB".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/custom_profile/font/fot-yurukastd-ub.prefab"
                    .to_string(),
            ),
            asset_type: Some("Font".to_string()),
            type_id: 128,
            path_id: 45,
            unique_id: None,
            size: 42,
            source_file: None,
        };

    let target = native_object_output_path(
        Path::new("/tmp/out"),
        "custom_profile/font",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("font"),
        Some(".otf"),
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/startapp/custom_profile/font/FOT-RodinNTLGPro-DB.otf")
    );
}

#[test]
fn semantic_export_path_state_disambiguates_without_path_id() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("shared".to_string()),
        container: Some("assets/shared.prefab".to_string()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 12345,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let mut state = NativeSemanticExportPathState::default();
    let base = PathBuf::from("/tmp/out/shared.assets/monobehaviour/shared.json");

    let first = state.claim(base.clone(), &asset);
    let second = state.claim(base, &asset);

    assert_eq!(
        first,
        PathBuf::from("/tmp/out/shared.assets/monobehaviour/shared.json")
    );
    assert_eq!(
        second,
        PathBuf::from("/tmp/out/shared.assets/monobehaviour/shared__dup2.json")
    );
    assert!(!second.to_string_lossy().contains("12345"));
}

#[test]
fn native_object_export_skips_byte_identical_semantic_duplicates() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "character/member/res004_no026",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("004026_shiho01".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/startapp/character/member/res004_no026/004026_shiho01.asset"
                .to_string(),
        ),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 1,
        unique_id: None,
        size: 16,
        source_file: None,
    };
    let read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: 16,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: br#"{"m_Name":"004026_shiho01"}"#.to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();
    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let expected = dir
        .path()
        .join("startapp/character/member/res004_no026/004026_shiho01.json");
    assert!(expected.exists());
    assert!(!dir
        .path()
        .join("startapp/character/member/res004_no026/004026_shiho01__dup2.json")
        .exists());
    assert_eq!(path_state.written_files, vec![expected]);
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    assert_eq!(manifest.lines().count(), 1);
}

#[test]
fn native_object_export_keeps_distinct_semantic_duplicates() {
    let dir = tempdir().unwrap();
    let (_config, mut region) = processing_config();
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "mysekai/site/field/grasslands",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("SiteObjectView".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/ondemand/mysekai/site/field/grasslands/grasslands.prefab"
                .to_string(),
        ),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 1,
        unique_id: None,
        size: 16,
        source_file: None,
    };
    let mut read_output = AssetStudioFfiObjectReadOutput {
        response: AssetStudioFfiObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: 16,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: br#"{"m_GameObject":1}"#.to_vec(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();
    read_output.payload = br#"{"m_GameObject":2}"#.to_vec();
    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    let first = dir
        .path()
        .join("ondemand/mysekai/site/field/grasslands/grasslands.assets/monobehaviour/SiteObjectView.json");
    let second = dir
        .path()
        .join("ondemand/mysekai/site/field/grasslands/grasslands.assets/monobehaviour/SiteObjectView__dup2.json");
    assert!(first.exists());
    assert!(second.exists());
    assert_eq!(path_state.written_files, vec![first, second]);
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    assert_eq!(manifest.lines().count(), 2);
}

#[test]
fn semantic_file_stem_compresses_repeated_clone_suffixes() {
    let name = "CharacterMotionClip(Clone)(Clone)(Clone)(Clone)";

    assert_eq!(
        assetstudio_fix_file_name(name),
        "CharacterMotionClip__clone4"
    );
}

#[test]
fn semantic_file_stem_truncates_long_names_without_path_id_or_hash() {
    let name = format!("{}{}", "VeryLongName".repeat(40), "(Clone)(Clone)");
    let fixed = assetstudio_fix_file_name(&name);

    assert!(fixed.ends_with("__truncated"));
    assert!(fixed.chars().count() <= ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS);
    assert!(!fixed.contains("12345"));
}

#[test]
fn playable_container_routes_to_single_public_json_path() {
    let target = playable_container_output_path(
        Path::new("/tmp/out"),
        "virtual_live/mc/timeline/foo",
        "assets/sekai/assetbundle/resources/ondemand/",
        true,
        "assets/sekai/assetbundle/resources/ondemand/virtual_live/mc/timeline/foo/foo.playable",
    );

    assert_eq!(
        target,
        PathBuf::from("/tmp/out/virtual_live/mc/timeline/foo/foo.json")
    );
}

#[test]
fn native_object_mode_records_known_unreadable_types() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("variants".to_string()),
        container: Some("assets/foo.shadervariants".to_string()),
        asset_type: Some("ShaderVariantCollection".to_string()),
        type_id: 200,
        path_id: 7,
        unique_id: None,
        size: 42,
        source_file: None,
    };

    let skipped = native_skipped_unsupported_asset(&asset).unwrap();
    assert_eq!(skipped.path_id, 7);
    assert_eq!(
        skipped.asset_type.as_deref(),
        Some("ShaderVariantCollection")
    );
    assert!(skipped.error.contains("ShaderVariantCollection"));
}

#[test]
fn native_object_mode_records_unknown_unreadable_types() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("custom".to_string()),
        container: Some("assets/foo.custom".to_string()),
        asset_type: Some("CustomRenderThing".to_string()),
        type_id: 114514,
        path_id: 9,
        unique_id: None,
        size: 128,
        source_file: None,
    };

    let skipped = native_skipped_unsupported_asset(&asset).unwrap();
    assert_eq!(skipped.path_id, 9);
    assert_eq!(skipped.asset_type.as_deref(), Some("CustomRenderThing"));
    assert!(skipped.error.contains("no read strategy"));
    assert!(skipped.error.contains("CustomRenderThing"));
}

#[test]
fn assetstudio_type_names_accept_short_and_class_aliases() {
    assert_eq!(assetstudio_export_type_selector("Texture2D"), Some("tex2d"));
    assert_eq!(assetstudio_export_type_selector("tex2d"), Some("tex2d"));
    assert_eq!(
        assetstudio_export_type_selector("Texture2DArray"),
        Some("tex2dArray")
    );
    assert_eq!(
        assetstudio_export_type_selector("MonoBehavior"),
        Some("monoBehaviour")
    );
    assert_eq!(assetstudio_export_type_selector("AudioClip"), Some("audio"));
    assert_eq!(
        assetstudio_export_type_selector("MovieTexture"),
        Some("movieTexture")
    );
    assert_eq!(
        assetstudio_export_type_selector("Animator"),
        Some("animator")
    );
    assert_eq!(assetstudio_export_type_selector("GameObject"), None);
}

#[test]
fn native_payload_bundle_parser_reads_multiple_entries() {
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
    payload.extend_from_slice(&2u32.to_le_bytes());
    payload.extend_from_slice(&("layer_0000.bmp".len() as u32).to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"layer_0000.bmp");
    payload.extend_from_slice(b"one");
    payload.extend_from_slice(&("nested/layer_0001.bmp".len() as u32).to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"nested/layer_0001.bmp");
    payload.extend_from_slice(b"two");

    let entries = parse_payload_bundle(&payload).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "layer_0000.bmp");
    assert_eq!(entries[0].1, b"one");
    assert_eq!(entries[1].0, "nested/layer_0001.bmp");
    assert_eq!(entries[1].1, b"two");
}

#[test]
fn native_payload_bundle_parser_reads_v2_header() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&super::NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC.to_le_bytes());
    payload.extend_from_slice(&super::NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION.to_le_bytes());
    payload
        .extend_from_slice(&(super::NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN as u16).to_le_bytes());
    payload.extend_from_slice(&2u32.to_le_bytes());
    payload.extend_from_slice(&6u64.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"7");
    payload.extend_from_slice(b"abc");
    payload.extend_from_slice(&5u32.to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"b.bin");
    payload.extend_from_slice(b"def");

    let entries = parse_payload_bundle(&payload).unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], ("7".to_string(), b"abc".to_vec()));
    assert_eq!(entries[1], ("b.bin".to_string(), b"def".to_vec()));
}

#[test]
fn native_payload_bundle_parser_reads_legacy_grouped_entries() {
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
    payload.extend_from_slice(&2u32.to_le_bytes());
    payload.extend_from_slice(&("layer_0000.bmp".len() as u32).to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"layer_0000.bmp");
    payload.extend_from_slice(&("nested/layer_0001.bmp".len() as u32).to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"nested/layer_0001.bmp");
    payload.extend_from_slice(b"one");
    payload.extend_from_slice(b"two");

    let entries = parse_payload_bundle(&payload).unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "layer_0000.bmp");
    assert_eq!(entries[0].1, b"one");
    assert_eq!(entries[1].0, "nested/layer_0001.bmp");
    assert_eq!(entries[1].1, b"two");
}

#[test]
fn native_payload_bundle_borrowed_parser_reuses_payload_slices() {
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&("asset.bin".len() as u32).to_le_bytes());
    payload.extend_from_slice(&4u64.to_le_bytes());
    payload.extend_from_slice(b"asset.bin");
    let data_start = payload.len();
    payload.extend_from_slice(b"data");

    let entries = parse_payload_bundle_borrowed(&payload).unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "asset.bin");
    assert_eq!(entries[0].1, b"data");
    assert_eq!(entries[0].1.as_ptr(), payload[data_start..].as_ptr());
}

#[test]
fn native_payload_bundle_paths_are_relative_and_safe() {
    assert_eq!(
        safe_payload_bundle_path("FBX_Animator/model/model.fbx"),
        PathBuf::from("FBX_Animator/model/model.fbx")
    );
    assert_eq!(
        safe_payload_bundle_path("../escape/asset.bin"),
        PathBuf::from("escape/asset.bin")
    );
    assert_eq!(
        safe_payload_bundle_path("/abs.bin"),
        PathBuf::from("abs.bin")
    );
    assert_eq!(safe_payload_bundle_path(".."), PathBuf::from("payload.bin"));
}

#[test]
fn native_read_batch_size_auto_tunes_by_workload() {
    let texture = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("texture".to_string()),
        container: None,
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 1,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let sprite = AssetStudioFfiAssetInfo {
        asset_type: Some("Sprite".to_string()),
        path_id: 2,
        ..texture.clone()
    };
    let mono = AssetStudioFfiAssetInfo {
        asset_type: Some("MonoBehaviour".to_string()),
        path_id: 3,
        ..texture.clone()
    };
    let text = AssetStudioFfiAssetInfo {
        asset_type: Some("TextAsset".to_string()),
        path_id: 4,
        ..texture.clone()
    };

    let image_assets = (0..80)
        .map(|index| if index % 2 == 0 { &texture } else { &sprite })
        .collect::<Vec<_>>();
    let mono_assets = (0..80)
        .map(|index| if index < 60 { &mono } else { &text })
        .collect::<Vec<_>>();

    assert_eq!(native_read_batch_size_for_assets(32, &image_assets), 64);
    assert_eq!(native_read_batch_size_for_assets(16, &image_assets), 64);
    assert_eq!(native_read_batch_size_for_assets(128, &mono_assets), 32);
    assert_eq!(native_read_batch_size_for_assets(48, &mono_assets), 32);
    assert_eq!(native_read_batch_size_for_assets(0, &[&text]), 1);
}

#[test]
fn readable_assets_skip_texture2d_array_images_when_parent_is_present() {
    let parent = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("tex_array".to_string()),
        container: Some("assets/sekai/assetbundle/resources/ondemand/fx/tex_array.png".to_string()),
        asset_type: Some("Texture2DArray".to_string()),
        type_id: 187,
        path_id: 1,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let child = AssetStudioFfiAssetInfo {
        index: 1,
        name: Some("tex_array_1".to_string()),
        asset_type: Some("Texture2DArrayImage".to_string()),
        path_id: 2,
        ..parent.clone()
    };
    let standalone_child = AssetStudioFfiAssetInfo {
        index: 2,
        name: Some("other_array_1".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/ondemand/fx/other_array.png".to_string(),
        ),
        asset_type: Some("Texture2DArrayImage".to_string()),
        path_id: 3,
        ..parent.clone()
    };
    let mut summary = NativeObjectExportSummary::default();
    let assets = vec![parent, child, standalone_child];
    let readable =
        select_native_object_readable_assets(&assets, &["all".to_string()], &mut summary);

    let path_ids = readable
        .iter()
        .map(|asset| asset.path_id)
        .collect::<Vec<_>>();
    assert_eq!(path_ids, vec![1, 3]);
    assert_eq!(summary.skipped_object_reads.len(), 1);
    assert_eq!(summary.skipped_object_reads[0].path_id, 2);
    assert_eq!(
        summary.skipped_object_reads[0].error,
        "Texture2DArrayImage is covered by its Texture2DArray parent"
    );
    assert_eq!(summary.object_read_plan.planned_objects, 2);
    assert_eq!(summary.object_read_plan.skipped_reads, 1);
}

#[test]
fn context_list_objects_worker_output_parses_pages() {
    let output = WorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioFfiResponse::ContextListObjects(
                sonic_rs::from_str(r#"{"success":true,"context_id":11,"assets":[{"index":0,"name":"asset","container":"assets/a.bytes","asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null}],"offset":0,"limit":1,"next_offset":1,"total_count":2,"returned_count":1,"warnings":["paged"],"error":null,"duration_ms":3}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: None,
        };

    let parsed = parse_assetstudio_ffi_context_list_objects_worker_output(output).unwrap();

    assert!(parsed.success);
    assert_eq!(parsed.assets.len(), 1);
    assert_eq!(parsed.assets[0].path_id, 7);
    assert_eq!(parsed.next_offset, Some(1));
    assert_eq!(parsed.total_count, 2);
    assert_eq!(parsed.returned_count, 1);
    assert_eq!(parsed.warnings, ["paged"]);
    assert_eq!(parsed.duration_ms, Some(3));
}

#[test]
fn object_read_failure_is_recoverable_for_single_asset() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("bad".to_string()),
        container: None,
        asset_type: Some("Shader".to_string()),
        type_id: 48,
        path_id: 42,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let output = WorkerOutput {
            status: "100".to_string(),
            status_success: false,
            response: AssetStudioFfiResponse::ContextReadObject(
                sonic_rs::from_str(r#"{"success":false,"asset":null,"payload_kind":null,"payload_len":0,"suggested_extension":null,"warnings":[],"phase_ms":{},"error":"boom","duration_ms":1}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: None,
        };

    let parsed =
        parse_assetstudio_ffi_object_read_worker_output_recoverable(output, &asset).unwrap();
    let NativeObjectReadParseResult::Skipped(skipped) = parsed else {
        panic!("expected skipped object read");
    };
    assert_eq!(skipped.path_id, 42);
    assert_eq!(skipped.asset_type.as_deref(), Some("Shader"));
    assert_eq!(skipped.name.as_deref(), Some("bad"));
    assert_eq!(skipped.error, "boom");
}

#[test]
fn object_read_prefers_in_memory_worker_payload() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("ok".to_string()),
        container: None,
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 7,
        unique_id: None,
        size: 3,
        source_file: None,
    };
    let output = WorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioFfiResponse::ContextReadObject(
                sonic_rs::from_str(r#"{"success":true,"asset":{"index":0,"name":"ok","container":null,"asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{},"error":null,"duration_ms":1}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: b"abc".to_vec(),
            payload_file: None,
        };

    let parsed =
        parse_assetstudio_ffi_object_read_worker_output_recoverable(output, &asset).unwrap();
    let NativeObjectReadParseResult::Read(read) = parsed else {
        panic!("expected successful object read");
    };
    assert_eq!(read.payload, b"abc");
    assert_eq!(read.response.payload_len, 3);
}

#[test]
fn object_read_loads_payload_file_and_removes_it() {
    let dir = tempdir().unwrap();
    let payload_file = dir.path().join("payload.bin");
    fs::write(&payload_file, b"abc").unwrap();
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("ok".to_string()),
        container: None,
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 7,
        unique_id: None,
        size: 3,
        source_file: None,
    };
    let output = WorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioFfiResponse::ContextReadObject(
                sonic_rs::from_str(r#"{"success":true,"asset":{"index":0,"name":"ok","container":null,"asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{},"error":null,"duration_ms":1}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: Some(payload_file.clone()),
        };

    let parsed =
        parse_assetstudio_ffi_object_read_worker_output_recoverable(output, &asset).unwrap();
    let NativeObjectReadParseResult::Read(read) = parsed else {
        panic!("expected successful object read");
    };
    assert_eq!(read.payload, b"abc");
    assert!(!payload_file.exists());
}

#[test]
fn object_read_batch_preserves_diagnostics_and_payloads() {
    let good_asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("ok".to_string()),
        container: Some("assets/ok.bytes".to_string()),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 7,
        unique_id: None,
        size: 3,
        source_file: None,
    };
    let failed_asset = AssetStudioFfiAssetInfo {
        index: 1,
        name: Some("bad".to_string()),
        container: Some("assets/bad.shader".to_string()),
        asset_type: Some("Shader".to_string()),
        type_id: 48,
        path_id: 8,
        unique_id: None,
        size: 0,
        source_file: None,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"7");
    payload.extend_from_slice(b"abc");
    let output = WorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioFfiResponse::ContextReadObjects(
                sonic_rs::from_str(r#"{"success":true,"reads":[{"success":true,"asset":{"index":0,"name":"ok","container":"assets/ok.bytes","asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{"read_object.read_payload":4},"error":null,"duration_ms":5},{"success":false,"asset":null,"payload_kind":null,"payload_len":0,"suggested_extension":null,"warnings":[],"phase_ms":{},"error":"shader unsupported","duration_ms":1}],"warnings":["batch warning"],"payload_len":3,"object_count":2,"payload_bundle_bytes":123,"failed_count":1,"read_payload_ms":4,"worker_id":"worker-a","call_seq":42,"phase_stats":{"read_payload":{"p50_ms":2,"p95_ms":7}},"error":null,"duration_ms":6}"#).unwrap(),
            ),
            stderr: String::new(),
            payload,
            payload_file: None,
        };
    let assets = [&good_asset, &failed_asset];

    let parsed =
        parse_assetstudio_ffi_object_read_batch_worker_output_recoverable(output, &assets).unwrap();

    assert_eq!(parsed.object_count, 2);
    assert_eq!(parsed.payload_bundle_bytes, 123);
    assert_eq!(parsed.failed_count, 1);
    assert_eq!(parsed.read_payload_ms, 4);
    assert_eq!(parsed.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(parsed.call_seq, Some(42));
    assert_eq!(
        parsed
            .phase_stats
            .get("read_payload")
            .map(|stats| stats.p95_ms),
        Some(7)
    );
    assert_eq!(parsed.results.len(), 2);
    let NativeObjectReadParseResult::Read(read) = &parsed.results[0] else {
        panic!("expected successful batch object read");
    };
    assert_eq!(read.payload, b"abc");
    assert_eq!(read.response.payload_len, 3);
    let NativeObjectReadParseResult::Skipped(skipped) = &parsed.results[1] else {
        panic!("expected skipped batch object read");
    };
    assert_eq!(skipped.path_id, 8);
    assert_eq!(skipped.error, "shader unsupported");
}

#[test]
fn object_read_batch_diagnostics_record_max_phase_stats() {
    let asset = AssetStudioFfiAssetInfo {
        index: 0,
        name: Some("ok".to_string()),
        container: Some("assets/ok.bytes".to_string()),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 7,
        unique_id: None,
        size: 3,
        source_file: None,
    };
    let mut summary = NativeObjectExportSummary {
        written_files: Vec::new(),
        acb_sources: Vec::new(),
        pending_image_writes: Vec::new(),
        phase_ms: HashMap::from([
            ("read_batch.read_payload.p50".to_string(), 5),
            ("read_batch.read_payload.p95".to_string(), 5),
        ]),
        skipped_object_reads: Vec::new(),
        object_read_plan: NativeObjectReadPlanStats::default(),
    };
    let read_outputs = NativeObjectReadBatchParseOutput {
        results: Vec::new(),
        object_count: 1,
        payload_bundle_version: 2,
        payload_bundle_entry_count: 1,
        payload_bundle_bytes: 10,
        payload_data_bytes: 3,
        failed_count: 0,
        read_payload_ms: 3,
        worker_id: Some("worker-a".to_string()),
        call_seq: Some(1),
        phase_ms: HashMap::from([("read_objects".to_string(), 4)]),
        asset_type_counts: HashMap::from([("TextAsset".to_string(), 1)]),
        payload_kind_counts: HashMap::from([("text_bytes".to_string(), 1)]),
        payload_bytes_by_kind: HashMap::from([("text_bytes".to_string(), 3)]),
        phase_stats: HashMap::from([(
            "read_payload".to_string(),
            NativeBatchPhaseStats {
                p50_ms: 2,
                p95_ms: 9,
            },
        )]),
    };

    record_native_object_read_batch_diagnostics(&mut summary, &[&asset], &read_outputs);

    assert_eq!(summary.object_read_plan.payload_bundle_bytes, 10);
    assert_eq!(summary.object_read_plan.read_payload_ms, 3);
    assert_eq!(
        summary.phase_ms.get("read_batch.read_payload.p50"),
        Some(&5)
    );
    assert_eq!(
        summary.phase_ms.get("read_batch.read_payload.p95"),
        Some(&9)
    );
    assert_eq!(
        summary.phase_ms.get("read_batch.phase.read_objects"),
        Some(&4)
    );
    assert_eq!(
        summary
            .phase_ms
            .get("read_batch.asset_type_count.TextAsset"),
        Some(&1)
    );
    assert_eq!(
        summary
            .phase_ms
            .get("read_batch.payload_kind_count.text_bytes"),
        Some(&1)
    );
    assert_eq!(
        summary
            .phase_ms
            .get("read_batch.payload_bytes_by_kind.text_bytes"),
        Some(&3)
    );
}
