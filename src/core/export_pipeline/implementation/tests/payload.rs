//! Writing a read object's payload, and telling duplicates apart.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use sonic_rs::JsonValueTrait;
use tempfile::tempdir;

use sekai_asset_pipeline::{
    ImageEncodingOptions as ImageBackendConfig, ImageFormat as ImageOutputFormat,
};

use super::super::payload::bundle::{parse_payload_bundle, parse_payload_bundle_borrowed};
use super::super::payload::dedup::payload_signature;
use super::super::payload::image_files::{
    write_native_image_payload_final_files, write_native_image_payload_final_files_with_backend,
    write_native_image_surface_final_files_now,
};
use super::super::payload::manifest::write_assetstudio_export_manifest_entry;
use super::super::payload::naming::{
    playable_container_output_path, text_asset_public_bytes_target,
};
use super::super::payload::playable::write_assetstudio_playable_payloads;
use super::super::payload::{write_native_object_payload, write_native_payload_file};
use super::super::types::{
    DecodedRgbaSurface, NativeImageEncodeSettings, NativeObjectExportOptions, NativeObjectPayload,
    NativeSemanticExportPathRegistry, NativeSemanticExportPathState, NativeSemanticPathClaim,
    UnityAssetInfo, UnityObjectReadOutput, UnityObjectReadResponse,
};
use super::support::*;

#[test]
fn native_raw_rgba_payload_is_encoded_to_png() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("normal.png");
    let mut payload = Vec::new();
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_RGBA_IR_MAGIC);
    payload.extend_from_slice(&2u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&[255, 0, 0, 255, 0, 255, 0, 128]);

    let region = processing_pipeline_options().region;
    let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();
    assert_eq!(written, vec![target.clone()]);
    let decoded = image::ImageReader::open(&target).unwrap().decode().unwrap();
    let rgba = decoded.to_rgba8();
    assert_eq!(rgba.width(), 2);
    assert_eq!(rgba.height(), 1);
    assert_eq!(rgba.get_pixel(0, 0).0, [255, 0, 0, 255]);
    assert_eq!(rgba.get_pixel(1, 0).0, [0, 255, 0, 128]);
}

/// The decoded surface must encode to the same bytes the serialise-and-parse
/// round trip produced. This is the whole claim of removing that round trip:
/// the `HARUKI_RGBAIR_V1` form existed to cross a process boundary, not to
/// change the image.
#[test]
fn decoded_surface_encodes_identically_to_the_serialised_round_trip() {
    let pixels: Vec<u8> = (0..4 * 3 * 4).map(|index| (index % 251) as u8).collect();
    let region = processing_pipeline_options().region;

    let via_bytes = tempdir().unwrap();
    let bytes_target = via_bytes.path().join("image.png");
    let payload = make_native_rgba_ir_payload(4, 3, &pixels);
    let written_bytes =
        write_native_image_payload_final_files(&bytes_target, &payload, &region).unwrap();

    let via_surface = tempdir().unwrap();
    let surface_target = via_surface.path().join("image.png");
    let surface = DecodedRgbaSurface {
        width: 4,
        height: 3,
        pixels: pixels.clone(),
    };
    let mut path_state =
        NativeSemanticExportPathState::with_registry(NativeSemanticExportPathRegistry::default());
    let written_surface = write_native_image_surface_final_files_now(
        &mut path_state,
        &surface_target,
        &surface,
        &region,
        &NativeImageEncodeSettings::default(),
    )
    .unwrap();

    assert_eq!(written_bytes.len(), 1);
    assert_eq!(written_surface.len(), 1);
    assert_eq!(
        fs::read(&bytes_target).unwrap(),
        fs::read(&surface_target).unwrap(),
        "the decoded surface must encode byte-for-byte like the round trip"
    );
    assert_eq!(path_state.image_encode.count, 1);
}

/// A decoded surface must sign itself by its pixels.
///
/// It first signed by `bytes()`, which is empty for a surface, so every texture
/// carried the same signature: two competing for one semantic path looked
/// byte-identical and one was dropped. That cost 1 317 files on the JP image
/// rule and nothing in the suite noticed.
#[test]
fn decoded_surfaces_sign_by_pixels_and_dimensions() {
    let surface = |width, height, fill| {
        NativeObjectPayload::Rgba(Box::new(DecodedRgbaSurface {
            width,
            height,
            pixels: vec![fill; (width * height * 4) as usize],
        }))
    };

    let red = payload_signature(&surface(2, 2, 1));
    let same = payload_signature(&surface(2, 2, 1));
    let other_pixels = payload_signature(&surface(2, 2, 2));
    let other_shape = payload_signature(&surface(4, 1, 1));
    let empty = payload_signature(&NativeObjectPayload::Bytes(bytes::Bytes::new()));

    assert_eq!(red, same, "the same surface must sign the same");
    assert_ne!(red, other_pixels, "different pixels must sign differently");
    assert_ne!(
        red, other_shape,
        "the same bytes at different dimensions must sign differently"
    );
    assert_ne!(red, empty, "a surface must not sign like an empty payload");
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
    let region = processing_pipeline_options().region;
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
    let mut region = processing_pipeline_options().region;
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
    let mut region = processing_pipeline_options().region;
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
fn native_image_object_payload_is_encoded_and_written_during_export() {
    let dir = tempdir().unwrap();
    let region = processing_pipeline_options().region;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "character/member/test",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: payload.into(),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

    // The encode happens where the image was decoded, so the PNG is on disk
    // before the export returns and no RGBA is left queued for a later stage.
    let expected = dir.path().join("character/member/test/normal.png");
    assert_eq!(path_state.written_files, vec![expected.clone()]);
    assert!(expected.exists());

    let decoded = image::open(&expected).unwrap().to_rgba8();
    assert_eq!(decoded.dimensions(), (2, 2));
    assert_eq!(decoded.get_pixel(0, 0).0, [255, 0, 0, 255]);

    // The encode telemetry the removed flush stage used to publish is now
    // accumulated as the images are written.
    let mut phase_ms = HashMap::new();
    path_state.image_encode.merge_into(&mut phase_ms);
    assert_eq!(phase_ms.get("image_encode.count"), Some(&1));
    assert_eq!(phase_ms.get("image_encode.format.png"), Some(&1));
}

#[test]
fn text_asset_acb_payload_is_queued_as_memory_source_without_writing_file() {
    let dir = tempdir().unwrap();
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: NativeObjectPayload::Bytes(bytes::Bytes::from_static(b"acb!")),
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
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: NativeObjectPayload::Bytes(bytes::Bytes::from_static(b"score")),
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
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: NativeObjectPayload::Bytes(bytes::Bytes::from_static(b"usm!")),
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
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: payload.to_vec().into(),
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
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: payload.to_vec().into(),
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
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: payload.to_vec().into(),
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
fn text_asset_public_bytes_target_strips_bytes_suffixes() {
    let mut asset = UnityAssetInfo {
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
fn manifest_records_native_surrogate_image_public_png_path() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("startapp/foo/normal.bmp");
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: NativeObjectPayload::Bytes(bytes::Bytes::new()),
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
    let asset = UnityAssetInfo {
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
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC);
    payload.extend_from_slice(&1u32.to_le_bytes());
    let entry_name = "FBX_Animator/model/model.fbx";
    payload.extend_from_slice(&(entry_name.len() as u32).to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(entry_name.as_bytes());
    payload.extend_from_slice(b"fbx");
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: payload.into(),
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
fn semantic_export_path_state_disambiguates_without_path_id() {
    let asset = UnityAssetInfo {
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

    let first = state.claim_generated_payload(base.clone(), &asset, b"first");
    let second = state.claim_generated_payload(base, &asset, b"second");

    assert_eq!(
        first,
        NativeSemanticPathClaim::Claimed(PathBuf::from(
            "/tmp/out/shared.assets/monobehaviour/shared.json"
        ))
    );
    assert_eq!(
        second,
        NativeSemanticPathClaim::Claimed(PathBuf::from(
            "/tmp/out/shared.assets/monobehaviour/shared__dup2.json"
        ))
    );
    let NativeSemanticPathClaim::Claimed(second) = second else {
        unreachable!("distinct payload must claim a path")
    };
    assert!(!second.to_string_lossy().contains("12345"));
}

#[test]
fn semantic_export_path_state_reuses_preexisting_base_path() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("shared.json");
    fs::write(&base, b"old payload").unwrap();
    let asset = UnityAssetInfo {
        index: 0,
        name: Some("shared".to_string()),
        container: Some("assets/shared.prefab".to_string()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 1,
        unique_id: None,
        size: 11,
        source_file: None,
    };
    let mut state = NativeSemanticExportPathState::default();

    let claimed = state.claim_generated_payload(base.clone(), &asset, b"new payload");

    assert_eq!(claimed, NativeSemanticPathClaim::Claimed(base));
    assert!(!dir.path().join("shared__dup2.json").exists());
}

#[test]
fn native_payload_write_removes_only_byte_identical_legacy_duplicates() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("shared.json");
    let duplicate = dir.path().join("shared__dup2.json");
    let distinct_duplicate = dir.path().join("shared__dup3.json");
    fs::write(&base, b"old").unwrap();
    fs::write(&duplicate, b"new").unwrap();
    fs::write(&distinct_duplicate, b"distinct").unwrap();

    let registry = NativeSemanticExportPathRegistry::default();
    write_native_payload_file(&base, b"new").unwrap();
    super::super::payload::dedup::remove_byte_identical_semantic_duplicates(&base, &registry)
        .unwrap();

    assert_eq!(fs::read(&base).unwrap(), b"new");
    assert!(!duplicate.exists());
    assert_eq!(fs::read(&distinct_duplicate).unwrap(), b"distinct");
}

#[test]
fn legacy_duplicate_cleanup_preserves_current_job_claims() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("shared.json");
    let duplicate = dir.path().join("shared__dup2.json");
    fs::write(&base, b"same").unwrap();
    fs::write(&duplicate, b"same").unwrap();
    let registry = NativeSemanticExportPathRegistry::default();
    let mut state = NativeSemanticExportPathState::with_registry(registry.clone());
    let asset = UnityAssetInfo {
        index: 0,
        name: Some("shared".to_string()),
        container: Some("assets/shared.prefab".to_string()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 1,
        unique_id: None,
        size: 4,
        source_file: None,
    };
    assert_eq!(
        state.claim_generated_payload(base.clone(), &asset, b"first"),
        NativeSemanticPathClaim::Claimed(base.clone())
    );
    assert_eq!(
        state.claim_generated_payload(base.clone(), &asset, b"second"),
        NativeSemanticPathClaim::Claimed(duplicate.clone())
    );

    let removed =
        super::super::payload::dedup::remove_byte_identical_semantic_duplicates(&base, &registry)
            .unwrap();

    assert_eq!(removed, 0);
    assert!(duplicate.exists());
}

#[test]
fn native_image_write_removes_byte_identical_legacy_duplicate() {
    let dir = tempdir().unwrap();
    let region = processing_pipeline_options().region;
    let target = dir.path().join("normal.png");
    let duplicate = dir.path().join("normal__dup2.png");
    let payload = make_native_rgba_ir_payload(1, 1, &[255, 0, 0, 255]);

    write_native_image_payload_final_files(&duplicate, &payload, &region).unwrap();
    write_native_image_payload_final_files(&target, &payload, &region).unwrap();

    assert!(target.exists());
    assert!(!duplicate.exists());
}

#[test]
fn playable_export_dedupes_identical_payloads_across_bundle_states() {
    let dir = tempdir().unwrap();
    let mut region = processing_pipeline_options().region;
    region.export.by_category = true;
    let read_kinds = BTreeMap::new();
    let options = NativeObjectExportOptions {
        output_dir: dir.path(),
        export_path: "virtual_live/mc/timeline/foo",
        strip_path_prefix: "assets/sekai/assetbundle/resources",
        region: &region,
        read_kinds: &read_kinds,
        image_format: "raw_rgba",
        read_batch_size: 16,
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let asset = UnityAssetInfo {
        index: 0,
        name: Some("AudienceClip(Clone)".to_string()),
        container: Some(
            "assets/sekai/assetbundle/resources/ondemand/virtual_live/mc/timeline/foo/foo.playable"
                .to_string(),
        ),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 1,
        unique_id: None,
        size: 2,
        source_file: None,
    };
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: 2,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: b"{}".to_vec().into(),
    };
    let registry = NativeSemanticExportPathRegistry::default();
    let mut first_state = NativeSemanticExportPathState::with_registry(registry.clone());
    let mut second_state = NativeSemanticExportPathState::with_registry(registry);

    write_assetstudio_playable_payloads(
        &options,
        &mut first_state,
        vec![(asset.clone(), read_output.clone())],
    )
    .unwrap();
    write_assetstudio_playable_payloads(&options, &mut second_state, vec![(asset, read_output)])
        .unwrap();

    let expected = dir
        .path()
        .join("ondemand/virtual_live/mc/timeline/foo/foo.json");
    assert_eq!(first_state.written_files, vec![expected.clone()]);
    assert!(second_state.written_files.is_empty());
    assert!(expected.exists());
    assert!(!dir
        .path()
        .join("ondemand/virtual_live/mc/timeline/foo/foo__dup2.json")
        .exists());
    let manifest =
        fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
    assert_eq!(manifest.lines().count(), 1);
}

#[test]
fn native_object_export_skips_byte_identical_semantic_duplicates() {
    let dir = tempdir().unwrap();
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: NativeObjectPayload::Bytes(bytes::Bytes::from_static(
            br#"{"m_Name":"004026_shiho01"}"#,
        )),
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
    let mut region = processing_pipeline_options().region;
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
        image_encode: &NativeImageEncodeSettings::default(),
    };
    let mut path_state = NativeSemanticExportPathState::default();
    let asset = UnityAssetInfo {
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
    let mut read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
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
        payload: NativeObjectPayload::Bytes(bytes::Bytes::from_static(br#"{"m_GameObject":1}"#)),
    };

    write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();
    read_output.payload =
        NativeObjectPayload::Bytes(bytes::Bytes::from_static(br#"{"m_GameObject":2}"#));
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
fn native_payload_bundle_parser_reads_multiple_entries() {
    let mut payload = Vec::new();
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC);
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
fn native_payload_bundle_parser_reads_v2_header() {
    let mut payload = Vec::new();
    payload.extend_from_slice(
        &super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_V2_MAGIC.to_le_bytes(),
    );
    payload.extend_from_slice(
        &super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_V2_VERSION.to_le_bytes(),
    );
    payload.extend_from_slice(
        &(super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_V2_HEADER_LEN as u16).to_le_bytes(),
    );
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
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC);
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
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC);
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
fn semantic_export_path_registry_dedupes_across_bundle_states() {
    let registry = NativeSemanticExportPathRegistry::default();
    let mut first_state = NativeSemanticExportPathState::with_registry(registry.clone());
    let mut second_state = NativeSemanticExportPathState::with_registry(registry);
    let asset = UnityAssetInfo {
        index: 0,
        name: Some("hard".to_string()),
        container: Some("assets/music/score/hard.txt".to_string()),
        asset_type: Some("TextAsset".to_string()),
        type_id: 49,
        path_id: 1,
        unique_id: None,
        size: 5,
        source_file: None,
    };
    let read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("text_bytes".to_string()),
            payload_len: 5,
            suggested_extension: Some(".txt".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: b"score".to_vec().into(),
    };
    let base = PathBuf::from("/tmp/out/music/score/hard.txt");

    let first = first_state.claim_payload(base.clone(), &asset, &read_output);
    let second = second_state.claim_payload(base.clone(), &asset, &read_output);

    assert_eq!(first, NativeSemanticPathClaim::Claimed(base.clone()));
    assert_eq!(
        second,
        NativeSemanticPathClaim::Duplicate { existing: base }
    );
}

#[test]
fn semantic_export_path_registry_keeps_distinct_cross_bundle_payloads() {
    let registry = NativeSemanticExportPathRegistry::default();
    let mut first_state = NativeSemanticExportPathState::with_registry(registry.clone());
    let mut second_state = NativeSemanticExportPathState::with_registry(registry);
    let asset = UnityAssetInfo {
        index: 0,
        name: Some("shared".to_string()),
        container: Some("assets/shared.prefab".to_string()),
        asset_type: Some("MonoBehaviour".to_string()),
        type_id: 114,
        path_id: 1,
        unique_id: None,
        size: 1,
        source_file: None,
    };
    let mut read_output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
            success: true,
            asset: Some(asset.clone()),
            payload_kind: Some("typetree_json".to_string()),
            payload_len: 1,
            suggested_extension: Some(".json".to_string()),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: b"1".to_vec().into(),
    };
    let base = PathBuf::from("/tmp/out/shared.json");

    let first = first_state.claim_payload(base.clone(), &asset, &read_output);
    read_output.payload = b"2".to_vec().into();
    let second = second_state.claim_payload(base.clone(), &asset, &read_output);

    assert_eq!(first, NativeSemanticPathClaim::Claimed(base));
    assert_eq!(
        second,
        NativeSemanticPathClaim::Claimed(PathBuf::from("/tmp/out/shared__dup2.json"))
    );
}
