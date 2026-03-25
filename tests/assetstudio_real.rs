use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use haruki_sekai_asset_updater::core::config::{
    AppConfig, ChartHashConfig, GitSyncConfig, RegionConfig, RegionExportConfig, RegionPathsConfig,
    RegionProviderConfig, RegionRuntimeConfig, RegionUploadConfig, RetryConfig, StorageConfig,
};
use haruki_sekai_asset_updater::core::export_pipeline::extract_unity_asset_bundle;
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
fn real_assetstudio_cli_exports_expected_file_when_configured() {
    let Some(asset_studio_cli_path) = required_env("ASSET_STUDIO_CLI_PATH") else {
        return;
    };
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
                convert_to_mp4: false,
                direct_usm_to_mp4_with_ffmpeg: false,
                remove_m2v: false,
            },
            audio: haruki_sekai_asset_updater::core::config::AudioExportConfig {
                convert_to_mp3: false,
                convert_to_flac: false,
                remove_wav: false,
            },
            images: haruki_sekai_asset_updater::core::config::ImageExportConfig {
                convert_to_webp: false,
                remove_png: false,
            },
            ..RegionExportConfig::default()
        },
        upload: RegionUploadConfig {
            enabled: false,
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
        tools: haruki_sekai_asset_updater::core::config::ToolsConfig {
            ffmpeg_path: "ffmpeg".to_string(),
            asset_studio_cli_path: Some(asset_studio_cli_path),
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
