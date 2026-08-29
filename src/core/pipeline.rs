use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::PlanningError;
use crate::core::models::{AssetUpdateRequest, ExecutionPlan};
use crate::core::regions::{build_url_preview, select_region};
use crate::core::storage::plan_storage_targets;
use sekai_asset_pipeline::CODEC_BACKEND;

/// Describes what a request would do, for the dry-run response and the job
/// snapshot.
///
/// This is a preview, not an execution input: nothing consumes the returned
/// plan to run the job. `AssetExecutionContext::new` re-derives everything from
/// the same config and request, which is only safe while the two derivations
/// agree. The storage half is covered by
/// `planned_upload_targets_match_what_the_upload_would_resolve`; the gap that
/// remains is that planning describes providers without opening them, so a plan
/// can name a target a live run cannot build.
/// Everything a run needs that comes from resolving the request against the
/// config, derived once.
///
/// The preview and the execution used to work this out separately, which is how
/// the same missing `downloaded_asset_record_file` produced
/// `PlanningError::MissingDownloadRecordPath` in one place and
/// `AssetExecutionError::MissingAssetSaveDir` -- naming the wrong setting -- in
/// the other.
#[derive(Debug, Clone)]
pub struct PreparedAssetRun {
    pub region_name: String,
    pub region: RegionConfig,
    pub download_record_file: String,
}

pub fn prepare_asset_run(
    config: &AppConfig,
    request: &AssetUpdateRequest,
) -> Result<PreparedAssetRun, PlanningError> {
    let region = select_region(config, &request.region)?;
    let download_record_file = region
        .paths
        .downloaded_asset_record_file
        .clone()
        .ok_or_else(|| PlanningError::MissingDownloadRecordPath {
            region: request.region.clone(),
        })?;
    Ok(PreparedAssetRun {
        region_name: request.region.clone(),
        region: region.clone(),
        download_record_file,
    })
}

pub fn build_execution_plan(
    config: &AppConfig,
    request: &AssetUpdateRequest,
) -> Result<ExecutionPlan, PlanningError> {
    let prepared = prepare_asset_run(config, request)?;
    let region = &prepared.region;
    let url_preview = build_url_preview(region, request);
    let download_record_file = prepared.download_record_file.clone();

    let upload_targets = if region.upload.enabled {
        plan_storage_targets(&config.storage, &request.region, &region.upload.providers)?
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

    use crate::core::errors::PlanningError;

    use super::{build_execution_plan, prepare_asset_run};

    fn planning_fixture() -> (AppConfig, AssetUpdateRequest) {
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
                    providers: Vec::new(),
                    public_read: crate::core::config::UploadPublicReadConfig::default(),
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
                    // Planning never opens a provider, but the upload does, and
                    // S3 refuses to build without a region. See
                    // `planning_does_not_prove_a_provider_can_be_opened`.
                    region: Some("auto".to_string()),
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
            mode: Default::default(),
        };

        (config, request)
    }

    #[test]
    fn execution_plan_includes_storage_and_git_sync_when_enabled() {
        let (config, request) = planning_fixture();

        let plan = build_execution_plan(&config, &request).unwrap();
        assert_eq!(
            plan.download_record_file,
            "./Data/jp-assets/downloaded_assets.json"
        );
        assert_eq!(plan.upload_targets.len(), 1);
        assert!(plan.chart_hash_sync.is_some());
        assert!(!plan.pending_steps.is_empty());
    }

    /// The plan is a preview: execution does not consume it, it re-derives from
    /// the same config and request. That is only safe while both derivations
    /// agree, and storage targets are where they could drift -- the preview
    /// calls `plan_storage_targets`, the upload calls
    /// `build_storage_operator_targets`. They share `selected_provider_configs`
    /// and `resolve_storage_provider` today; this fails if someone gives either
    /// side its own resolution.
    #[test]
    fn planned_upload_targets_match_what_the_upload_would_resolve() {
        let (config, request) = planning_fixture();

        let plan = build_execution_plan(&config, &request).unwrap();
        let region = crate::core::regions::select_region(&config, &request.region).unwrap();
        let actual = crate::core::storage::build_storage_operator_targets(
            &config.storage,
            &request.region,
            &region.upload.providers,
        )
        .unwrap();

        // The operator target keeps its bucket and root inside the OpenDAL
        // operator, so what is comparable here is which providers were selected,
        // in what order, resolved to which scheme.
        assert_eq!(plan.upload_targets.len(), actual.len());
        assert!(!actual.is_empty(), "the fixture must select a provider");
        for (planned, resolved) in plan.upload_targets.iter().zip(actual.iter()) {
            assert_eq!(planned.provider, resolved.provider);
            assert_eq!(planned.provider_kind, resolved.scheme);
        }
    }

    /// Planning describes upload targets; it does not open them. The upload
    /// path builds an OpenDAL operator, which validates far more -- an S3
    /// provider with no region resolves fine in a plan and fails to build at
    /// upload time. A dry run can therefore report a target that a live run
    /// cannot use.
    ///
    /// Pinned rather than fixed: making planning open every provider would put
    /// network-dependent validation on the dry-run path, which is a design
    /// decision, not a test.
    #[test]
    fn planning_does_not_prove_a_provider_can_be_opened() {
        let (mut config, request) = planning_fixture();
        config.storage.providers[0].region = None;

        let plan = build_execution_plan(&config, &request).unwrap();
        assert_eq!(plan.upload_targets.len(), 1, "the plan accepts it");

        let region = crate::core::regions::select_region(&config, &request.region).unwrap();
        let opened = crate::core::storage::build_storage_operator_targets(
            &config.storage,
            &request.region,
            &region.upload.providers,
        );
        assert!(opened.is_err(), "the upload path rejects the same provider");
    }

    /// One derivation, two consumers. The plan the caller is shown and the
    /// executor that runs the job now come from the same `prepare_asset_run`,
    /// so the region and the record path cannot differ between what was
    /// promised and what runs.
    #[test]
    fn the_plan_and_the_prepared_run_agree() {
        let (config, request) = planning_fixture();

        let prepared = prepare_asset_run(&config, &request).unwrap();
        let plan = build_execution_plan(&config, &request).unwrap();

        assert_eq!(plan.region, prepared.region_name);
        assert_eq!(plan.download_record_file, prepared.download_record_file);
    }

    /// A region with no `downloaded_asset_record_file` is rejected once, by
    /// name. Execution used to re-derive this and report it as
    /// `MissingAssetSaveDir`, naming a setting that was not the problem.
    #[test]
    fn a_missing_record_path_is_rejected_when_the_run_is_prepared() {
        let (mut config, request) = planning_fixture();
        config
            .regions
            .get_mut("jp")
            .unwrap()
            .paths
            .downloaded_asset_record_file = None;

        let error = prepare_asset_run(&config, &request).unwrap_err();

        assert!(
            matches!(error, PlanningError::MissingDownloadRecordPath { .. }),
            "{error}"
        );
    }
}
