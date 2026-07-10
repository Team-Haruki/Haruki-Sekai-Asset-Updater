use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use haruki_assetstudio_ffi::{configured_worker_path, AssetStudioWorkerPool};
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
fn real_assetstudio_ffi_exports_expected_file_when_configured() {
    let Some(asset_studio_ffi_library_path) = required_env("ASSET_STUDIO_FFI_LIBRARY_PATH") else {
        return;
    };
    run_real_assetstudio_export(asset_studio_ffi_library_path);
}

fn run_real_assetstudio_export(asset_studio_ffi_library_path: String) {
    let Some(bundle_path) = required_env("ASSET_STUDIO_BUNDLE_PATH") else {
        return;
    };
    let expected_relative_file = required_env("ASSET_STUDIO_EXPECTED_RELATIVE_FILE");

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
                worker_path: required_env("ASSET_STUDIO_FFI_WORKER_PATH")
                    .or_else(|| required_env("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH")),
                worker_idle_timeout_seconds: required_env(
                    "ASSET_STUDIO_WORKER_IDLE_TIMEOUT_SECONDS",
                )
                .and_then(|value| value.parse().ok())
                .unwrap_or(60),
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
    if let Some(expected_relative_file) = expected_relative_file {
        let expected_path = export_root.join(PathBuf::from(expected_relative_file));
        assert!(
            expected_path.exists(),
            "expected AssetStudio output missing: {}",
            expected_path.display()
        );
    }
    assert!(
        summary.export_root.exists(),
        "export root missing: {}",
        summary.export_root.display()
    );

    let exported_count = walk_files(&export_root).len();
    assert!(exported_count > 0, "no files exported by AssetStudio");

    if parse_bool_env("ASSET_STUDIO_ASSERT_IDLE_REAP", false) {
        let idle_timeout =
            Duration::from_secs(config.backends.asset_studio.worker_idle_timeout_seconds as u64);
        let worker_path =
            configured_worker_path(config.backends.asset_studio.worker_path.as_deref()).unwrap();
        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            config
                .backends
                .asset_studio
                .library_path
                .as_deref()
                .unwrap(),
            config.effective_asset_studio_ffi_process_concurrency(),
            config.backends.asset_studio.worker_max_calls,
            idle_timeout,
        );
        assert!(
            runtime.block_on(pool.idle_worker_count()) > 0
                || pool.maintenance_stats_snapshot().idle_reaped > 0
        );
        runtime
            .block_on(async {
                tokio::time::timeout(idle_timeout + Duration::from_secs(5), async {
                    loop {
                        let stats = pool.maintenance_stats_snapshot();
                        let allocator_trimmed = !cfg!(all(target_os = "linux", target_env = "gnu"))
                            || stats.allocator_trim_attempts > 0;
                        if pool.idle_worker_count().await == 0
                            && stats.idle_reaped > 0
                            && allocator_trimmed
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
            })
            .expect("AssetStudio worker pool did not become idle");
        assert!(pool.maintenance_stats_snapshot().idle_reaped > 0);
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        assert!(pool.maintenance_stats_snapshot().allocator_trim_attempts > 0);
    }
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
