//! Reading objects out of a bundle, and the limits on doing so.

#[cfg(unix)]
use super::super::limits::sum_process_tree_cpu_percent;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use tempfile::tempdir;

use crate::test_support::empty_unity_fs_bundle;
use crate::{ExportPipelineError, MediaBackend, RetryOptions as RetryConfig};

use super::super::assetstudio::{
    assetstudio_object_mode_supported_type, native_read_batch_size_for_assets,
    native_read_kind_for_asset, native_skipped_unsupported_asset,
    select_native_object_readable_assets, sort_native_object_reads_for_failure_isolation,
};
use super::super::extract_unity_asset_bundle;
use super::super::limits::{
    acquire_cpu_budget_permit_blocking, acquire_image_memory_permit_blocking,
};
use super::super::media_postprocess::usm::process_usm_input_with_metrics;
use super::super::tasks::{
    prepare_usm_processing_inputs, run_path_tasks, scan_all_files, usm_segment_key,
    UsmProcessingInput,
};
use super::super::types::{NativeObjectExportSummary, UnityAssetInfo};
use super::support::*;

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

            let region = processing_pipeline_options().region;
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
fn linked_unity_rs_backend_rejects_an_unrecognized_input() {
    let dir = tempdir().unwrap();
    let fake_bundle = dir.path().join("bundle.bin");
    fs::write(&fake_bundle, b"bundle").unwrap();
    let output_dir = dir.path().join("out");
    let options = processing_pipeline_options();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let error = runtime
        .block_on(extract_unity_asset_bundle(
            &options,
            &fake_bundle,
            "event_story/foo",
            &output_dir,
            "StartApp",
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        ExportPipelineError::UnrecognizedUnityInput { .. }
    ));
}

/// A recognized, structurally valid Unity container may legitimately contain
/// no serialized files or objects. That remains a successful empty export.
#[test]
fn linked_unity_rs_backend_handles_a_valid_empty_container() {
    let dir = tempdir().unwrap();
    let bundle = dir.path().join("empty.bundle");
    fs::write(&bundle, empty_unity_fs_bundle()).unwrap();
    let output_dir = dir.path().join("out");
    let options = processing_pipeline_options();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let summary = runtime
        .block_on(extract_unity_asset_bundle(
            &options,
            &bundle,
            "event_story/foo",
            &output_dir,
            "StartApp",
        ))
        .unwrap();

    assert!(summary.unity_rs_object_read_plan.is_empty());
}

#[test]
fn native_image_format_always_uses_raw_rgba() {
    let asset = UnityAssetInfo {
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
        super::super::assetstudio::native_image_format_for_asset(&asset, "raw_rgba"),
        "raw_rgba"
    );
    assert_eq!(
        super::super::assetstudio::native_image_format_for_asset(&asset, ""),
        "raw_rgba"
    );
    assert_eq!(
        super::super::assetstudio::native_image_format_for_asset(&asset, "png"),
        "raw_rgba"
    );
}

#[test]
fn native_object_read_subchunks_split_non_bmp_images() {
    let texture = UnityAssetInfo {
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
    let sprite = UnityAssetInfo {
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
    let mono = UnityAssetInfo {
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

    let source_chunks =
        super::super::assetstudio::native_object_read_subchunks(&assets, "raw_rgba");
    assert_eq!(source_chunks.len(), 3);
    assert_eq!(source_chunks[0][0].path_id, 10);
    assert_eq!(source_chunks[1][0].path_id, 11);
    assert_eq!(source_chunks[2][0].path_id, 12);

    let configured_chunks = super::super::assetstudio::native_object_read_subchunks(&assets, "bmp");
    assert_eq!(configured_chunks.len(), 3);
    assert_eq!(configured_chunks[0][0].path_id, 10);
    assert_eq!(configured_chunks[1][0].path_id, 11);
    assert_eq!(configured_chunks[2][0].path_id, 12);
}

#[test]
fn native_image_format_ignores_container_extension() {
    let asset = UnityAssetInfo {
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
        super::super::assetstudio::native_image_format_for_asset(&asset, "raw_rgba"),
        "raw_rgba"
    );
    assert_eq!(
        super::super::assetstudio::native_image_format_for_asset(&asset, "jpg"),
        "raw_rgba"
    );
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
fn image_memory_permit_is_weighted_and_process_wide() {
    let limit = 1_234_567;
    let first = acquire_image_memory_permit_blocking(limit, 800_000);
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _second = acquire_image_memory_permit_blocking(limit, 800_000);
        tx.send(()).unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    drop(first);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    handle.join().unwrap();
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
fn native_object_mode_uses_configured_read_kind_with_specific_precedence() {
    let asset = UnityAssetInfo {
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
    let mut asset = UnityAssetInfo {
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

    asset.asset_type = Some("Animator".to_string());
    assert_eq!(
        native_read_kind_for_asset(&asset, &BTreeMap::new()),
        "typetree_json"
    );

    asset.asset_type = Some("ShaderVariantCollection".to_string());
    assert_eq!(
        native_read_kind_for_asset(&asset, &BTreeMap::new()),
        "typetree_json"
    );
}

#[test]
fn native_object_mode_records_known_unreadable_types() {
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
fn native_read_batch_size_auto_tunes_by_workload() {
    let texture = UnityAssetInfo {
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
    let sprite = UnityAssetInfo {
        asset_type: Some("Sprite".to_string()),
        path_id: 2,
        ..texture.clone()
    };
    let mono = UnityAssetInfo {
        asset_type: Some("MonoBehaviour".to_string()),
        path_id: 3,
        ..texture.clone()
    };
    let text = UnityAssetInfo {
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
fn native_object_reads_sort_images_after_metadata_assets() {
    let texture = UnityAssetInfo {
        index: 1,
        name: Some("texture_00".to_string()),
        container: Some("assets/live2d/texture_00.png".to_string()),
        asset_type: Some("Texture2D".to_string()),
        type_id: 28,
        path_id: 1,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let model = UnityAssetInfo {
        index: 2,
        name: Some("model3".to_string()),
        container: Some("assets/live2d/model3.json".to_string()),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 2,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let build_motion = UnityAssetInfo {
        index: 3,
        name: Some("BuildMotionData".to_string()),
        container: Some("assets/live2d/motions/buildmotiondata.asset".to_string()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 3,
        unique_id: None,
        size: 42,
        source_file: None,
    };
    let mut reads = vec![&texture, &model, &build_motion];

    sort_native_object_reads_for_failure_isolation(&mut reads);

    assert_eq!(
        reads
            .iter()
            .map(|asset| asset.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["model3", "BuildMotionData", "texture_00"]
    );
}

#[test]
fn readable_assets_skip_texture2d_array_images_when_parent_is_present() {
    let parent = UnityAssetInfo {
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
    let child = UnityAssetInfo {
        index: 1,
        name: Some("tex_array_1".to_string()),
        asset_type: Some("Texture2DArrayImage".to_string()),
        path_id: 2,
        ..parent.clone()
    };
    let standalone_child = UnityAssetInfo {
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
    assert_eq!(
        summary.object_read_plan.by_type["Texture2DArray"].planned_objects,
        1
    );
    assert_eq!(
        summary.object_read_plan.by_type["Texture2DArrayImage"].planned_objects,
        1
    );
    assert_eq!(
        summary.object_read_plan.by_type["Texture2DArrayImage"].skipped_reads,
        1
    );
}
