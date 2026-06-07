use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use haruki_sekai_asset_updater::core::config::{
    AppConfig, ChartHashConfig, GitSyncConfig, RegionConfig, RegionExportConfig, RegionPathsConfig,
    RegionProviderConfig, RegionRuntimeConfig, RegionUploadConfig, RetryConfig, StorageConfig,
};
use haruki_sekai_asset_updater::core::export_pipeline::extract_unity_asset_bundle;
use haruki_sekai_asset_updater::{
    AssetStudioFfiClient, AssetStudioInspectOptions, AssetStudioObjectReadOptions,
};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

fn required_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    required_env(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

#[test]
fn real_assetstudio_ffi_exports_expected_file_when_configured() {
    let Some(asset_studio_ffi_library_path) = required_env("ASSET_STUDIO_FFI_LIBRARY_PATH") else {
        return;
    };
    run_real_assetstudio_export(asset_studio_ffi_library_path);
}

#[test]
fn real_assetstudio_ffi_client_reads_object_when_configured() {
    let Some(asset_studio_ffi_library_path) = required_env("ASSET_STUDIO_FFI_LIBRARY_PATH") else {
        return;
    };
    let Some(bundle_path) = required_env("ASSET_STUDIO_BUNDLE_PATH") else {
        return;
    };

    let unity_version =
        required_env("ASSET_STUDIO_UNITY_VERSION").unwrap_or_else(|| "2022.3.21f1".to_string());
    let asset_types = required_env("ASSET_STUDIO_ASSET_TYPES")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec!["tex2d".to_string()]);
    let filter_by_path_ids: Vec<i64> = required_env("ASSET_STUDIO_FILTER_BY_PATH_IDS")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::parse::<i64>)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        })
        .unwrap_or_default();

    let mut options = AssetStudioInspectOptions::new(&bundle_path)
        .asset_types(asset_types)
        .unity_version(unity_version);
    if !filter_by_path_ids.is_empty() {
        options = options.filter_by_path_ids(filter_by_path_ids);
    }
    let mut context = AssetStudioFfiClient::new(asset_studio_ffi_library_path)
        .open_context(&options)
        .unwrap();
    assert!(
        !context.open_response().assets.is_empty(),
        "FFI client context_open returned no assets"
    );
    let path_id = required_env("ASSET_STUDIO_READ_PATH_ID")
        .map(|value| value.parse::<i64>().unwrap())
        .unwrap_or_else(|| context.open_response().assets[0].path_id);
    let read = context
        .read_object(&AssetStudioObjectReadOptions::new(path_id))
        .unwrap();
    assert!(read.response.success);
    assert!(
        read.response.payload_len >= 0,
        "FFI client object read returned an invalid payload length"
    );
    context.close().unwrap();
}

#[test]
fn fixture_bundles_are_available_for_assetstudio_regressions() {
    for (name, expected_len, expected_sha256) in [
        (
            "unityasset_long",
            2_039_487usize,
            "de2b955b34e8fb3a45330e4517951d17d046c6df051082bdf58047e5836a1e61",
        ),
        (
            "jacket_s_712",
            53_269usize,
            "183e8db956e615bbdcfad9e563aa2c58cb87fa0169530754972ff0ccda6127fb",
        ),
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("files")
            .join(name);
        let data = fs::read(&path).unwrap_or_else(|err| {
            panic!("failed to read fixture {}: {err}", path.display());
        });
        assert_eq!(data.len(), expected_len, "fixture size changed: {name}");
        let digest = Sha256::digest(&data)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(digest, expected_sha256, "fixture hash changed: {name}");
    }
}

fn run_real_assetstudio_export(asset_studio_ffi_library_path: String) {
    let Some(bundle_path) = required_env("ASSET_STUDIO_BUNDLE_PATH") else {
        return;
    };
    let Some(expected_relative_file) = required_env("ASSET_STUDIO_EXPECTED_RELATIVE_FILE") else {
        return;
    };

    let unity_version =
        required_env("ASSET_STUDIO_UNITY_VERSION").unwrap_or_else(|| "2022.3.21f1".to_string());
    let export_path = required_env("ASSET_STUDIO_EXPORT_PATH")
        .unwrap_or_else(|| "assetstudio/fixture".to_string());
    let by_category = parse_bool_env("ASSET_STUDIO_BY_CATEGORY", false);
    let category = required_env("ASSET_STUDIO_CATEGORY").unwrap_or_else(|| "StartApp".to_string());

    let output_dir = tempdir().unwrap();
    let mut region = RegionConfig {
        enabled: true,
        provider: RegionProviderConfig::default(),
        runtime: RegionRuntimeConfig { unity_version },
        paths: RegionPathsConfig {
            asset_save_dir: Some(output_dir.path().to_string_lossy().into_owned()),
            downloaded_asset_record_file: Some(
                output_dir
                    .path()
                    .join("downloaded_assets.json")
                    .to_string_lossy()
                    .into_owned(),
            ),
        },
        export: RegionExportConfig {
            by_category,
            video: haruki_sekai_asset_updater::core::config::VideoExportConfig {
                formats: vec![haruki_sekai_asset_updater::core::config::VideoOutputFormat::M2v],
                direct_mp4: false,
            },
            audio: haruki_sekai_asset_updater::core::config::AudioExportConfig {
                formats: vec![haruki_sekai_asset_updater::core::config::AudioOutputFormat::Wav],
            },
            images: haruki_sekai_asset_updater::core::config::ImageExportConfig {
                formats: vec![haruki_sekai_asset_updater::core::config::ImageOutputFormat::Png],
            },
            ..RegionExportConfig::default()
        },
        upload: RegionUploadConfig {
            enabled: false,
            providers: Vec::new(),
            public_read: haruki_sekai_asset_updater::core::config::UploadPublicReadConfig::default(
            ),
            remove_local_after_upload: false,
        },
        ..RegionConfig::default()
    };
    region.export.usm.export = true;
    region.export.usm.decode = true;
    region.export.acb.export = true;
    region.export.acb.decode = true;
    region.export.hca.decode = true;

    let config = AppConfig {
        backends: haruki_sekai_asset_updater::core::config::BackendsConfig {
            media: haruki_sekai_asset_updater::core::config::MediaBackendConfig {
                ffmpeg_path: "ffmpeg".to_string(),
                ..haruki_sekai_asset_updater::core::config::MediaBackendConfig::default()
            },
            asset_studio: haruki_sekai_asset_updater::core::config::AssetStudioBackendConfig {
                library_path: Some(asset_studio_ffi_library_path),
                ..haruki_sekai_asset_updater::core::config::AssetStudioBackendConfig::default()
            },
            ..haruki_sekai_asset_updater::core::config::BackendsConfig::default()
        },
        storage: StorageConfig {
            providers: Vec::new(),
        },
        git_sync: GitSyncConfig {
            chart_hashes: ChartHashConfig::default(),
        },
        execution: haruki_sekai_asset_updater::core::config::ExecutionConfig {
            retry: RetryConfig {
                attempts: 2,
                initial_backoff_ms: 100,
                max_backoff_ms: 200,
            },
            ..haruki_sekai_asset_updater::core::config::ExecutionConfig::default()
        },
        ..AppConfig::default()
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let summary = runtime
        .block_on(extract_unity_asset_bundle(
            &config,
            "jp",
            &region,
            Path::new(&bundle_path),
            &export_path,
            output_dir.path(),
            &category,
        ))
        .unwrap();

    let export_root = if by_category {
        output_dir
            .path()
            .join(category.to_lowercase())
            .join(&export_path)
    } else {
        output_dir.path().join(&export_path)
    };
    let expected_path = export_root.join(PathBuf::from(expected_relative_file));
    assert!(
        expected_path.exists(),
        "expected AssetStudio output missing: {}",
        expected_path.display()
    );
    assert!(
        summary.export_root.exists(),
        "export root missing: {}",
        summary.export_root.display()
    );

    let exported_count = walk_files(&export_root).len();
    assert!(exported_count > 0, "no files exported by AssetStudio");
}

fn walk_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(walk_files(&path));
            } else {
                files.push(path);
            }
        }
    }
    files
}
