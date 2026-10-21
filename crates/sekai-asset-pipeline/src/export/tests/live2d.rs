//! Live2D motions: MOC3 ids, binding resolution and `motion3.json` output.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Cursor;

use tempfile::tempdir;

use crate::test_support::{
    processing_pipeline_options, synthetic_moc3, text_assets_and_clips_unity_fs_bundle,
    unity_crc32, SyntheticClipBinding,
};

use super::super::extract_unity_asset_bundle;
use super::super::live2d_motion::{
    container_key, motion_meta_fade_times, read_moc3_identifiers, target_path_hashes,
    with_motion_fade_times, BundleLive2dMotions, Live2dMotionTargetIndex, Moc3Identifiers,
};
use super::super::paths::native_object_output_path;
use super::super::tasks::scan_all_files;
use super::super::types::{UnityAssetInfo, UnityObjectReadOutput, UnityObjectReadResponse};
use super::super::unity::{default_native_read_kind, native_read_kind_for_asset};
use unity_rs_core::live2d_motion::CubismMotionTargetNames;

fn asset(type_id: i32, asset_type: &str, container: Option<&str>) -> UnityAssetInfo {
    UnityAssetInfo {
        index: 0,
        name: Some("w-cool-tilthead01".to_string()),
        container: container.map(str::to_string),
        asset_type: Some(asset_type.to_string()),
        type_id,
        path_id: 1,
        unique_id: None,
        size: 0,
        source_file: None,
    }
}

fn read_output(payload_kind: &str, payload: &[u8]) -> UnityObjectReadOutput {
    UnityObjectReadOutput {
        response: UnityObjectReadResponse {
            success: true,
            asset: None,
            payload_kind: Some(payload_kind.to_string()),
            payload_len: payload.len() as i64,
            suggested_extension: None,
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: payload.to_vec().into(),
    }
}

#[test]
fn reads_moc3_identifier_tables_in_either_byte_order() {
    for big_endian in [false, true] {
        let moc = synthetic_moc3(&["ParamAngleX", "ParamBreath"], &["PartArm"], big_endian);
        let identifiers = read_moc3_identifiers(&mut Cursor::new(moc)).unwrap();
        assert_eq!(
            identifiers,
            Some(Moc3Identifiers {
                parameters: vec!["ParamAngleX".to_string(), "ParamBreath".to_string()],
                parts: vec!["PartArm".to_string()],
            })
        );
    }
}

#[test]
fn moc3_reader_rejects_other_payloads() {
    let short = read_moc3_identifiers(&mut Cursor::new(b"MOC3".to_vec())).unwrap();
    assert_eq!(short, None);
    let mut not_moc = synthetic_moc3(&["ParamAngleX"], &[], false);
    not_moc[..4].copy_from_slice(b"UNTY");
    assert_eq!(
        read_moc3_identifiers(&mut Cursor::new(not_moc)).unwrap(),
        None
    );

    let mut absurd = synthetic_moc3(&[], &[], false);
    absurd[268 + 20..268 + 24].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(read_moc3_identifiers(&mut Cursor::new(absurd)).is_err());

    let mut truncated = synthetic_moc3(&["ParamAngleX"], &[], false);
    truncated.truncate(truncated.len() - 1);
    assert!(read_moc3_identifiers(&mut Cursor::new(truncated)).is_err());
}

#[test]
fn target_index_scans_exported_models_once_and_merges_job_models() {
    let dir = tempdir().unwrap();
    let flat = dir.path().join("live2d/model/v1/main/10_an/10an_normal");
    let by_category = dir
        .path()
        .join("startapp/live2d/model/v1/main/20_mizuki/20mizuki_normal");
    fs::create_dir_all(&flat).unwrap();
    fs::create_dir_all(&by_category).unwrap();
    fs::write(
        flat.join("an.moc3"),
        synthetic_moc3(&["ParamAngleX"], &["PartArm"], false),
    )
    .unwrap();
    fs::write(
        by_category.join("mizuki.moc3"),
        synthetic_moc3(&["ParamAngleY", "ParamAngleX"], &[], false),
    )
    .unwrap();
    fs::write(by_category.join("broken.moc3"), b"MOC3 but cut short").unwrap();
    let mut truncated = synthetic_moc3(&["ParamCut"], &[], false);
    truncated.truncate(truncated.len() - 1);
    fs::write(by_category.join("truncated.moc3"), truncated).unwrap();
    fs::write(by_category.join("texture_00.png"), b"not a model").unwrap();

    let index = Live2dMotionTargetIndex::default();
    assert!(!index.add_moc3(b"not a model"));
    assert!(index.add_moc3(&synthetic_moc3(&["ParamEyeLOpen"], &[], false)));
    let names = index.snapshot(dir.path());
    assert_eq!(
        names.parameters,
        vec!["ParamAngleX", "ParamAngleY", "ParamEyeLOpen"]
    );
    assert_eq!(names.parts, vec!["PartArm"]);

    // A second snapshot does not rescan, but sees models added since.
    fs::write(
        flat.join("late.moc3"),
        synthetic_moc3(&["ParamLate"], &[], false),
    )
    .unwrap();
    let clone = index.clone();
    assert!(clone.add_moc3(&synthetic_moc3(&["ParamBodyAngleX"], &[], false)));
    let names = index.snapshot(dir.path());
    assert!(names.parameters.contains(&"ParamBodyAngleX".to_string()));
    assert!(!names.parameters.contains(&"ParamLate".to_string()));
}

#[test]
fn target_hashes_cover_full_paths_and_their_suffixes() {
    assert_eq!(unity_crc32(b"123456789"), 0xcbf4_3926);
    let hashes = target_path_hashes(&CubismMotionTargetNames {
        parameters: vec!["ParamAngleX".to_string()],
        parts: vec!["PartArm".to_string()],
    });
    for path in [
        "Parameters/ParamAngleX",
        "ParamAngleX",
        "Parts/PartArm",
        "PartArm",
    ] {
        assert!(hashes.contains(&unity_crc32(path.as_bytes())), "{path}");
    }
    assert_eq!(hashes.len(), 4);
}

#[test]
fn clips_and_meta_objects_pair_by_container_without_extension() {
    let base = "assets/sekai/assetbundle/resources/startapp/live2d/motion/v1/main/10_an/10an_motion_base/motion/w-cool-tilthead01";
    assert_eq!(container_key(&format!("{base}.anim")), Some(base));
    assert_eq!(container_key(&format!("{base}.asset")), Some(base));
    assert_eq!(container_key("no_extension"), None);
    assert_eq!(container_key(".anim"), None);
    assert_eq!(container_key("motion/.anim"), None);
}

#[test]
fn fade_times_come_from_the_meta_object_and_keep_every_other_byte() {
    let meta = br#"{"m_Name":"w-cool-tilthead01","FadeInTime":0.5,"FadeOutTime":0.25}"#;
    let fade = motion_meta_fade_times(meta).unwrap();
    assert_eq!(motion_meta_fade_times(br#"{"FadeInTime":0.5}"#), None);
    assert_eq!(
        motion_meta_fade_times(br#"{"FadeInTime":"0.5","FadeOutTime":0.5}"#),
        None
    );
    assert_eq!(motion_meta_fade_times(b"not json"), None);

    let motion = b"{\n  \"Version\": 3,\n  \"Meta\": {\n    \"Duration\": 1.0,\n    \"FadeInTime\": 0.0,\n    \"FadeOutTime\": 0.0,\n    \"CurveCount\": 1\n  },\n  \"Curves\": [{ \"FadeInTime\": -1.0 }]\n}";
    let patched = with_motion_fade_times(motion, Some(&fade)).unwrap();
    let expected = String::from_utf8_lossy(motion)
        .replacen("\"FadeInTime\": 0.0", "\"FadeInTime\": 0.5", 1)
        .replacen("\"FadeOutTime\": 0.0", "\"FadeOutTime\": 0.25", 1);
    assert_eq!(String::from_utf8(patched).unwrap(), expected);
    assert_eq!(with_motion_fade_times(motion, None).unwrap(), motion);
    assert!(with_motion_fade_times(b"{\"Meta\":{}}", Some(&fade)).is_err());
}

#[test]
fn only_typetree_meta_objects_with_a_container_record_fades() {
    let index = Live2dMotionTargetIndex::default();
    let dir = tempdir().unwrap();
    let mut motions = BundleLive2dMotions::new(&index, dir.path());
    let meta = br#"{"FadeInTime":0.5,"FadeOutTime":0.5}"#;
    let container = Some("assets/motion/w-cool-tilthead01.asset");
    motions.observe(
        &asset(114, "MonoBehaviour", None),
        &read_output("typetree_json", meta),
    );
    motions.observe(
        &asset(114, "MonoBehaviour", container),
        &read_output("raw", meta),
    );
    assert!(motions.fades_is_empty());
    motions.observe(
        &asset(114, "MonoBehaviour", container),
        &read_output("typetree_json", meta),
    );
    assert!(!motions.fades_is_empty());
}

#[test]
fn motions_sit_beside_their_clip_container() {
    let dir = tempdir().unwrap();
    let clip = asset(
        74,
        "AnimationClip",
        Some("assets/sekai/assetbundle/resources/startapp/live2d/motion/v1/main/10_an/10an_motion_base/motion/w-cool-tilthead01.anim"),
    );
    for by_category in [false, true] {
        let path = native_object_output_path(
            dir.path(),
            "live2d/motion/10an_motion_base",
            "assets/sekai/assetbundle/resources/startapp",
            by_category,
            &clip,
            Some("live2d_motion3"),
            Some(".motion3.json"),
        );
        assert!(
            path.ends_with("10an_motion_base/motion/w-cool-tilthead01.motion3.json")
                || path.ends_with("10an_motion_base/w-cool-tilthead01.motion3.json"),
            "{}",
            path.display()
        );
    }
}

#[test]
fn animation_clips_read_as_motions_by_default() {
    assert_eq!(default_native_read_kind("AnimationClip"), "motion3");
    let clip = asset(74, "AnimationClip", None);
    assert_eq!(
        native_read_kind_for_asset(&clip, &BTreeMap::new()),
        "motion3"
    );
    let typetree = BTreeMap::from([("AnimationClip".to_string(), "typetree_json".to_string())]);
    assert_eq!(
        native_read_kind_for_asset(&clip, &typetree),
        "typetree_json"
    );
}

#[test]
fn exports_cubism_clips_as_motion3_and_skips_other_clips() {
    let dir = tempdir().unwrap();
    let bundle = dir.path().join("motion.bundle");
    let moc = synthetic_moc3(&["ParamAngleX"], &["PartArm"], false);
    let cubism = [
        SyntheticClipBinding {
            path: unity_crc32(b"Parameters/ParamAngleX"),
            type_id: 114,
        },
        SyntheticClipBinding {
            path: unity_crc32(b"Parts/PartArm"),
            type_id: 114,
        },
        SyntheticClipBinding {
            path: unity_crc32(b"Parameters/ParamNotInAnyModel"),
            type_id: 114,
        },
    ];
    let cubism_frames = [
        (
            -f32::MAX,
            vec![
                (0, [0.0, 0.0, 0.0, 0.0]),
                (1, [0.0, 0.0, 0.0, 1.0]),
                (2, [0.0, 0.0, 0.0, 0.0]),
            ],
        ),
        (
            0.0,
            vec![
                (0, [0.0, 0.0, 30.0, 0.0]),
                (1, [0.0, 0.0, 0.0, 1.0]),
                (2, [0.0, 0.0, 0.0, 0.0]),
            ],
        ),
        (
            1.0,
            vec![
                (0, [0.0, 0.0, 0.0, 30.0]),
                (1, [0.0, 0.0, 0.0, 1.0]),
                (2, [0.0, 0.0, 0.0, 0.0]),
            ],
        ),
        // Unity closes a streamed clip with a key-less sentinel frame.
        (f32::MAX, Vec::new()),
    ];
    let ui = [SyntheticClipBinding {
        path: unity_crc32(b"Canvas/Button"),
        type_id: 1,
    }];
    let ui_frames = [
        (-f32::MAX, vec![(0, [0.0, 0.0, 0.0, 0.0])]),
        (0.0, vec![(0, [0.0, 0.0, 1.0, 0.0])]),
        (1.0, vec![(0, [0.0, 0.0, 0.0, 1.0])]),
        (f32::MAX, Vec::new()),
    ];
    fs::write(
        &bundle,
        text_assets_and_clips_unity_fs_bundle(
            &[("model.moc3", &moc)],
            &[
                ("w-cool-tilthead01", &cubism, &cubism_frames, 1.0),
                ("ui_fade", &ui, &ui_frames, 1.0),
            ],
        ),
    )
    .unwrap();
    let mut options = processing_pipeline_options();
    options.region.export.asset_studio_types =
        vec!["textAsset".to_string(), "AnimationClip".to_string()];

    let summary = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(extract_unity_asset_bundle(
            &options,
            &bundle,
            "live2d/motion/10an_motion_base",
            &dir.path().join("out"),
            "StartApp",
        ))
        .unwrap();

    assert_eq!(summary.unity_rs_object_read_plan.failed_reads, 0);
    let files = scan_all_files(&summary.export_root).unwrap();
    let motion_path = files
        .iter()
        .find(|path| path.ends_with("w-cool-tilthead01.motion3.json"))
        .expect("motion3 written");
    assert!(!files
        .iter()
        .any(|path| path.to_string_lossy().contains("ui_fade")));
    let motion: sonic_rs::Value = sonic_rs::from_slice(&fs::read(motion_path).unwrap()).unwrap();
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};
    assert_eq!(motion["Meta"]["Duration"].as_f64(), Some(1.0));
    let curves = motion["Curves"].as_array().unwrap();
    let ids = curves
        .iter()
        .map(|curve| {
            (
                curve["Target"].as_str().unwrap().to_string(),
                curve["Id"].as_str().unwrap().to_string(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            ("Parameter".to_string(), "ParamAngleX".to_string()),
            ("PartOpacity".to_string(), "PartArm".to_string()),
        ]
    );
    let segments = curves[0]["Segments"].as_array().unwrap();
    assert_eq!(segments.first().and_then(|value| value.as_f64()), Some(0.0));
    assert_eq!(segments.last().and_then(|value| value.as_f64()), Some(30.0));
}
