use crate::core::codec::CODEC_BACKEND;
use crate::core::config::AppConfig;
use crate::core::errors::PlanningError;
use crate::core::models::{AssetUpdateRequest, ExecutionPlan};
use crate::core::regions::{build_url_preview, select_region};
use crate::core::storage::plan_storage_targets;

pub fn build_execution_plan(
    config: &AppConfig,
    request: &AssetUpdateRequest,
) -> Result<ExecutionPlan, PlanningError> {
    let region = select_region(config, &request.region)?;
    let url_preview = build_url_preview(region, request);
    let download_record_file = region
        .paths
        .downloaded_asset_record_file
        .clone()
        .ok_or_else(|| PlanningError::MissingDownloadRecordPath {
            region: request.region.clone(),
        })?;

    let upload_targets = if region.upload.enabled {
        plan_storage_targets(&config.storage, &request.region)?
    } else {
        Vec::new()
    };

    let chart_hash_sync = if config.git_sync.chart_hashes.enabled {
        let repository_dir = config
            .git_sync
            .chart_hashes
            .repository_dir
            .clone()
            .unwrap_or_else(|| "./sekai-chart-hash".to_string());
        Some(crate::core::models::ChartHashSyncPlan {
            output_file: format!("{repository_dir}/{}_chart_hashes.json", request.region),
            repository_dir,
            branch_hint: None,
        })
    } else {
        None
    };

    let mut pending_steps = vec![
        "dry-run responses stop after planning; live bundle discovery and execution happen only for non-dry-run jobs".to_string(),
    ];

    if region.upload.enabled {
        pending_steps.push(
            "cloud upload is configured and implemented, but it is not called until export outputs exist".to_string(),
        );
    }
    if chart_hash_sync.is_some() {
        pending_steps.push(
            "chart-hash Git sync is configured and implemented, but it is not called until downloaded assets are available".to_string(),
        );
    }

    Ok(ExecutionPlan {
        region: request.region.clone(),
        dry_run: request.dry_run,
        codec_backend: CODEC_BACKEND.to_string(),
        url_preview,
        download_record_file,
        upload_targets,
        chart_hash_sync,
        pending_steps,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, RegionConfig, RegionPathsConfig,
        RegionProviderConfig, RegionUploadConfig, StorageConfig, StorageProviderConfig,
    };
    use crate::core::models::AssetUpdateRequest;

    use super::build_execution_plan;

    #[test]
    fn execution_plan_includes_storage_and_git_sync_when_enabled() {
        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());

        let mut regions = BTreeMap::new();
        regions.insert(
            "jp".to_string(),
            RegionConfig {
                enabled: true,
                provider: RegionProviderConfig::ColorfulPalette {
                    asset_info_url_template:
                        "https://info/{env}/{hash}/{asset_version}/{asset_hash}".to_string(),
                    asset_bundle_url_template: "https://bundle/{bundle_path}".to_string(),
                    profile: "production".to_string(),
                    profile_hashes,
                    required_cookies: false,
                    cookie_bootstrap_url: None,
                },
                paths: RegionPathsConfig {
                    asset_save_dir: Some("./Data/jp-assets".to_string()),
                    downloaded_asset_record_file: Some(
                        "./Data/jp-assets/downloaded_assets.json".to_string(),
                    ),
                },
                upload: RegionUploadConfig {
                    enabled: true,
                    remove_local_after_upload: false,
                },
                ..RegionConfig::default()
            },
        );

        let config = AppConfig {
            storage: StorageConfig {
                providers: vec![StorageProviderConfig {
                    endpoint: "assets.example.com".to_string(),
                    bucket: "sekai-{server}-assets".to_string(),
                    ..StorageProviderConfig::default()
                }],
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig {
                    enabled: true,
                    repository_dir: Some("./sekai-chart-hash".to_string()),
                    ..ChartHashConfig::default()
                },
            },
            regions,
            ..AppConfig::default()
        };

        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: true,
        };

        let plan = build_execution_plan(&config, &request).unwrap();
        assert_eq!(
            plan.download_record_file,
            "./Data/jp-assets/downloaded_assets.json"
        );
        assert_eq!(plan.upload_targets.len(), 1);
        assert!(plan.chart_hash_sync.is_some());
        assert!(!plan.pending_steps.is_empty());
    }
}
