//! Normalize the game's optional FixtureBundleMeta into the public JSON contract.
use std::collections::HashMap;

use super::payload::write_native_object_payload;
use super::selectors::{asset_studio_export_type_list, assetstudio_type_selector_matches};
use super::types::{
    NativeObjectExportOptions, NativeSemanticExportPathState, UnityAssetInfo,
    UnityObjectReadOutput, UnityObjectReadResponse,
};
use crate::ExportPipelineError;

const DEFAULT_META: &[u8] = br#"{"m_Name":"fixture_metadata","stackEnables":{"array":[]},"stackHeight":0,"motionArea":{"array":[]},"cutsceneArea":{"array":[]},"AddUsingGrid":{"array":[]},"_harukiSource":"fixture_bundle_default"}"#;

pub(super) fn write_fixture_default(
    options: &NativeObjectExportOptions<'_>,
    state: &mut NativeSemanticExportPathState,
    assets: &[UnityAssetInfo],
) -> Result<(), ExportPipelineError> {
    if !asset_studio_export_type_list(options.region)
        .iter()
        .any(|selector| assetstudio_type_selector_matches(selector, "MonoBehaviour"))
    {
        return Ok(());
    }
    let Some(asset) = default_fixture_asset(options.export_path, assets) else {
        return Ok(());
    };
    let output = UnityObjectReadOutput {
        response: UnityObjectReadResponse {
            success: true,
            asset: None,
            payload_kind: Some("typetree_json".into()),
            payload_len: DEFAULT_META.len() as i64,
            suggested_extension: Some(".json".into()),
            warnings: vec![],
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: None,
        },
        payload: DEFAULT_META.to_vec().into(),
    };
    write_native_object_payload(options, state, &asset, &output)
}

fn default_fixture_asset(export_path: &str, assets: &[UnityAssetInfo]) -> Option<UnityAssetInfo> {
    let name = export_path.strip_prefix("mysekai/fixture/")?;
    if name.is_empty() || name.contains(['/', '\\']) || name == "." || name == ".." {
        return None;
    }
    // Inspect ALL objects before export filtering. A present but unreadable or
    // excluded metadata object must never be replaced with an empty footprint.
    if assets.iter().any(|a| {
        a.name.as_deref() == Some("fixture_metadata")
            || a.container
                .as_deref()
                .is_some_and(|p| p.ends_with("/fixture_metadata.asset"))
    }) {
        return None;
    }
    let prefix = "assets/sekai/assetbundle/resources/ondemand/mysekai/fixture/";
    let prefab = assets.iter().find(|a| {
        a.type_id == 1
            && a.container
                .as_deref()
                .is_some_and(|p| p.starts_with(prefix) && p.ends_with(".prefab"))
    })?;
    let mut asset = prefab.clone();
    asset.name = Some("fixture_metadata".into());
    asset.container = Some(format!(
        "assets/sekai/assetbundle/resources/ondemand/{export_path}/fixture_metadata.asset"
    ));
    asset.asset_type = Some("MonoBehaviour".into());
    asset.type_id = 114;
    Some(asset)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn prefab() -> UnityAssetInfo {
        UnityAssetInfo {
            index: 0,
            name: Some("furniture".into()),
            container: Some(
                "assets/sekai/assetbundle/resources/ondemand/mysekai/fixture/chair/chair.prefab"
                    .into(),
            ),
            asset_type: Some("GameObject".into()),
            type_id: 1,
            path_id: 1,
            unique_id: None,
            size: 0,
            source_file: None,
        }
    }
    #[test]
    fn missing_metadata_has_the_official_empty_grids() {
        let asset = default_fixture_asset("mysekai/fixture/chair", &[prefab()]).unwrap();
        assert_eq!(asset.name.as_deref(), Some("fixture_metadata"));
        let data: sonic_rs::Value = sonic_rs::from_slice(DEFAULT_META).unwrap();
        for field in ["stackEnables", "motionArea", "cutsceneArea", "AddUsingGrid"] {
            assert_eq!(sonic_rs::to_string(&data[field]["array"]).unwrap(), "[]");
        }
        assert_eq!(sonic_rs::to_string(&data["stackHeight"]).unwrap(), "0");
    }
    #[test]
    fn present_metadata_is_never_defaulted_even_if_unreadable() {
        let mut meta = prefab();
        meta.type_id = 114;
        meta.name = Some("fixture_metadata".into());
        assert!(
            default_fixture_asset("mysekai/fixture/chair", &[prefab(), meta.clone()]).is_none()
        );
        meta.name = None;
        meta.container = Some("any/fixture_metadata.asset".into());
        assert!(default_fixture_asset("mysekai/fixture/chair", &[prefab(), meta]).is_none());
    }
    #[test]
    fn only_loaded_fixture_prefabs_receive_defaults() {
        for path in [
            "other/chair",
            "mysekai/fixture_timeline/chair",
            "mysekai/fixture/",
            "mysekai/fixture/..",
            "mysekai/fixture/a/b",
        ] {
            assert!(default_fixture_asset(path, &[prefab()]).is_none());
        }
        assert!(default_fixture_asset("mysekai/fixture/chair", &[]).is_none());
        let mut mesh = prefab();
        mesh.type_id = 43;
        assert!(default_fixture_asset("mysekai/fixture/chair", &[mesh]).is_none());
    }
    #[test]
    fn writer_tracks_default_and_respects_export_filter() {
        use super::super::types::NativeImageEncodeSettings;
        use crate::test_support::processing_pipeline_options;
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let mut region = processing_pipeline_options().region;
        let read_kinds = BTreeMap::new();
        for (types, expected) in [(vec!["all".into()], 1), (vec!["Texture2D".into()], 0)] {
            region.export.asset_studio_types = types;
            let options = NativeObjectExportOptions {
                output_dir: dir.path(),
                export_path: "mysekai/fixture/chair",
                strip_path_prefix: "assets/sekai/assetbundle/resources",
                region: &region,
                read_kinds: &read_kinds,
                image_format: "raw_rgba",
                read_batch_size: 16,
                image_encode: &NativeImageEncodeSettings::default(),
            };
            let mut state = NativeSemanticExportPathState::default();
            write_fixture_default(&options, &mut state, &[prefab()]).unwrap();
            assert_eq!(state.written_files.len(), expected);
            if expected == 1 {
                let path = dir
                    .path()
                    .join("mysekai/fixture/chair/fixture_metadata.json");
                assert_eq!(state.written_files, vec![path.clone()]);
                assert_eq!(std::fs::read(path).unwrap(), DEFAULT_META);
            }
            let mut absent = NativeSemanticExportPathState::default();
            write_fixture_default(&options, &mut absent, &[]).unwrap();
            assert!(absent.written_files.is_empty());
        }
    }
}
