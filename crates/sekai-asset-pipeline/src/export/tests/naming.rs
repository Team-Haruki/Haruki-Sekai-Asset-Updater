//! Output paths, export groups and type selectors.

use std::path::{Path, PathBuf};

use super::super::get_export_group;
use super::super::paths::{
    assetstudio_fix_file_name, assetstudio_semantic_file_stem, default_extension_for_asset,
    native_object_output_extension, native_object_output_path, normalize_semantic_path_component,
    safe_payload_bundle_path, semantic_assetstudio_object_output_path,
    static_known_payload_extension, strip_container_prefix,
};
use super::super::selectors::{
    assetstudio_export_type_selector, assetstudio_type_selector_matches,
};
use super::super::types::{UnityAssetInfo, ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS};

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
fn native_object_output_extension_prefers_payload_kind() {
    let asset = UnityAssetInfo {
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
fn mono_behaviour_primary_asset_uses_container_json_path() {
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
fn live2d_build_motion_data_uses_motion_container_json_path() {
    let asset = UnityAssetInfo {
            index: 0,
            name: Some("BuildMotionData".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/live2d/model/v1/main/01_ichika/01ichika_cloth001/motions/buildmotiondata.asset"
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
        "live2d/model/v1/main/01_ichika/01ichika_cloth001",
        "assets/sekai/assetbundle/resources",
        true,
        &asset,
        Some("typetree_json"),
        Some(".json"),
    );

    assert_eq!(
        target,
        PathBuf::from(
            "/tmp/out/startapp/live2d/model/v1/main/01_ichika/01ichika_cloth001/motions/buildmotiondata.json"
        )
    );
}

#[test]
fn mono_script_stays_in_container_subasset_path() {
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
fn non_character_sprite_objects_route_under_container_sprite_directory() {
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
    let asset = UnityAssetInfo {
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
fn path_helpers_cover_every_semantic_directory_and_extension_family() {
    let base = PathBuf::from("/tmp/container.bin");
    for (asset_type, expected_dir) in [
        ("Sprite", "sprite"),
        ("Mesh", "mesh"),
        ("Animator", "animator"),
        ("MonoBehaviour", "monobehaviour"),
        ("Texture2DArray", "texture2d_array"),
        ("MonoScript", "monoscript"),
        ("GameObject", "gameobject"),
        ("Material", "material"),
        ("Transform", "transform"),
        ("RectTransform", "recttransform"),
        ("ParticleSystem", "particle_system"),
        ("ParticleSystemRenderer", "particle_system_renderer"),
        ("SpriteRenderer", "sprite_renderer"),
        ("SpriteMask", "sprite_mask"),
        ("MeshFilter", "mesh_filter"),
        ("MeshRenderer", "mesh_renderer"),
        ("SkinnedMeshRenderer", "skinned_mesh_renderer"),
        ("PlayableDirector", "playable_director"),
        ("Canvas", "canvas"),
        ("CanvasRenderer", "canvas_renderer"),
        ("Camera", "camera"),
        ("Avatar", "avatar"),
        ("AudioListener", "audio_listener"),
        ("Animation", "animation"),
        ("AnimationClip", "animation_clip"),
        ("TextMesh", "text_mesh"),
        ("SortingGroup", "sorting_group"),
        ("Cubemap", "cubemap"),
        ("Texture3D", "texture3d"),
        ("Shader", "shader"),
    ] {
        let asset = UnityAssetInfo {
            index: 0,
            name: Some("named".to_string()),
            container: None,
            asset_type: Some(asset_type.to_string()),
            type_id: 0,
            path_id: 1,
            unique_id: None,
            size: 1,
            source_file: None,
        };
        let path = semantic_assetstudio_object_output_path(base.clone(), &asset);
        assert!(
            path.to_string_lossy().contains(expected_dir),
            "{asset_type}: {path:?}"
        );
    }

    let mut asset = UnityAssetInfo {
        index: 0,
        name: None,
        container: None,
        asset_type: None,
        type_id: 0,
        path_id: 1,
        unique_id: Some("unique".to_string()),
        size: 1,
        source_file: None,
    };
    assert_eq!(assetstudio_semantic_file_stem(&asset), "unique");
    asset.unique_id = None;
    assert_eq!(assetstudio_semantic_file_stem(&asset), "asset");
    assert_eq!(default_extension_for_asset(&asset), "dat");
    assert_eq!(normalize_semantic_path_component(" A-b_c "), "abc");
    assert_eq!(
        strip_container_prefix("../../safe/file", ""),
        PathBuf::from("safe/file")
    );

    for extension in [
        ".bytes", "DAT", "json", "lua", "txt", "bmp", "png", "tga", "jpeg", "webp", "wav", "mp3",
        "flac", "ogg", "ttf", "otf", "shader", "obj", "fbx", "bin",
    ] {
        assert!(
            static_known_payload_extension(extension).is_some(),
            "{extension}"
        );
    }
    assert!(static_known_payload_extension("unknown").is_none());

    for payload_kind in [
        "raw",
        "typetree_json",
        "text_bytes",
        "image_bmp",
        "image_raw_rgba",
        "image_png",
        "image_tga",
        "image_jpeg",
        "image_webp",
        "image_array_bundle_raw_rgba",
        "audio_raw",
        "video_raw",
        "movie_ogv",
        "font",
        "shader_text",
        "mesh_obj",
    ] {
        let _ = native_object_output_extension(&asset, Some(payload_kind), Some(".txt"));
    }
}
