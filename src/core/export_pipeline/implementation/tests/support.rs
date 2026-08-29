//! Fixtures the export test modules share.
//!
//! One copy, because several of these build configs and payload bytes that
//! the assertions compare against; two drifting copies would not fail, they
//! would quietly test different things.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::core::config::{
    AppConfig, ChartHashConfig, GitSyncConfig, RegionConfig, RegionExportConfig, RegionPathsConfig,
    RegionProviderConfig, RegionRuntimeConfig, RegionUploadConfig, StorageConfig,
};

pub(super) fn sample_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("HARUKI_CODEC_SAMPLE_DIR")
        .map(PathBuf::from)
        .map(|dir| dir.join(name))
}

pub(super) fn processing_config() -> (AppConfig, RegionConfig) {
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

pub(super) fn processing_pipeline_options() -> sekai_asset_pipeline::PipelineOptions {
    let (app_config, region) = processing_config();
    super::super::options::pipeline_options(&app_config, &region)
}

pub(super) fn make_native_rgba_ir_payload(width: u32, height: u32, pixels: &[u8]) -> Vec<u8> {
    let stride = width * 4;
    let mut payload = Vec::new();
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_RGBA_IR_MAGIC);
    payload.extend_from_slice(&width.to_le_bytes());
    payload.extend_from_slice(&height.to_le_bytes());
    payload.extend_from_slice(&stride.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(pixels);
    payload
}
