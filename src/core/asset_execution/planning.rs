//! Turning a region's filters and an asset-info document into download tasks.

use std::path::{Component, Path, PathBuf};

use super::model::{
    AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext, DownloadTask,
};
use crate::core::config::{RegionConfig, RegionProviderConfig};
use crate::core::download_records::DownloadRecord;
use crate::core::errors::AssetExecutionError;
use crate::core::regions::{compile_patterns, first_match_index, matches_any};

pub(super) fn download_path_for_region(
    provider: &RegionProviderConfig,
    bundle_name: &str,
    detail: &AssetBundleDetail,
) -> String {
    match provider {
        RegionProviderConfig::ColorfulPalette { .. } => bundle_name.to_string(),
        RegionProviderConfig::Nuverse { .. } => detail
            .download_path
            .as_ref()
            .map(|prefix| format!("{prefix}/{bundle_name}"))
            .unwrap_or_else(|| bundle_name.to_string()),
    }
}

pub fn should_download_bundle(
    region: &RegionConfig,
    bundle_name: &str,
    category: &AssetCategory,
) -> bool {
    let compiled = match category {
        AssetCategory::StartApp => compile_patterns(&region.filters.start_app),
        AssetCategory::OnDemand | AssetCategory::LivePv => {
            compile_patterns(&region.filters.on_demand)
        }
        AssetCategory::Other(_) => return false,
    };
    if compiled.is_empty() {
        return false;
    }
    matches_any(&compiled, bundle_name)
}

pub(super) fn raw_bundle_output_path(
    root: &Path,
    bundle_path: &str,
) -> Result<PathBuf, AssetExecutionError> {
    let relative = validate_relative_bundle_path(bundle_path)?;

    let mut path = root.to_path_buf();
    for component in relative.components() {
        if let Component::Normal(value) = component {
            path.push(value);
        }
    }

    if path.extension().and_then(|ext| ext.to_str()) != Some("bundle") {
        path.set_extension("bundle");
    }
    Ok(path)
}

/// Validate an untrusted, server-provided bundle path: it must be a relative path made only of
/// normal components (no empty / `.` / `..` / absolute / root / prefix). Returns it as a relative
/// `Path` so callers can safely `join` it onto a trusted root without escaping it.
pub(super) fn validate_relative_bundle_path(
    bundle_path: &str,
) -> Result<&Path, AssetExecutionError> {
    let invalid = |reason: &str| AssetExecutionError::InvalidRawBundlePath {
        bundle: bundle_path.to_string(),
        reason: reason.to_string(),
    };
    if bundle_path.is_empty() {
        return Err(invalid("path is empty"));
    }
    if bundle_path
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(invalid(
            "empty, current-directory, or parent-directory components are not allowed",
        ));
    }

    let relative = Path::new(bundle_path);
    if relative.is_absolute() {
        return Err(invalid("absolute paths are not allowed"));
    }

    for component in relative.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {
                return Err(invalid("current-directory components are not allowed"))
            }
            Component::ParentDir => {
                return Err(invalid("parent-directory components are not allowed"))
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(invalid("root or prefix components are not allowed"))
            }
        }
    }

    Ok(relative)
}

impl AssetExecutionContext {
    pub(super) fn build_download_tasks(
        &self,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
        haruki_3d_downloaded_assets: &DownloadRecord,
        can_reuse_haruki_3d_download_record: bool,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        self.append_haruki_3d_download_tasks(
            self.build_standard_download_tasks(info, downloaded_assets),
            info,
            haruki_3d_downloaded_assets,
            can_reuse_haruki_3d_download_record,
        )
    }

    pub(super) fn build_standard_download_tasks(
        &self,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
    ) -> Vec<DownloadTask> {
        let skip_patterns = compile_patterns(&self.region.filters.skip);
        let priority_patterns = compile_patterns(&self.region.filters.priority);
        let start_app_patterns = compile_patterns(&self.region.filters.start_app);
        let on_demand_patterns = compile_patterns(&self.region.filters.on_demand);
        let mut tasks = Vec::new();

        for (bundle_name, detail) in &info.bundles {
            if matches_any(&skip_patterns, bundle_name) {
                continue;
            }
            let category_patterns = match &detail.category {
                AssetCategory::StartApp => &start_app_patterns,
                AssetCategory::OnDemand | AssetCategory::LivePv => &on_demand_patterns,
                AssetCategory::Other(_) => continue,
            };
            if category_patterns.is_empty() || !matches_any(category_patterns, bundle_name) {
                continue;
            }

            let bundle_hash = match self.region.provider {
                RegionProviderConfig::Nuverse { .. } => detail.crc.to_string(),
                RegionProviderConfig::ColorfulPalette { .. } => detail.hash.clone(),
            };

            if downloaded_assets
                .get(bundle_name)
                .is_some_and(|existing| existing == &bundle_hash)
            {
                continue;
            }

            let priority = first_match_index(&priority_patterns, bundle_name).unwrap_or(usize::MAX);
            tasks.push(DownloadTask {
                download_path: download_path_for_region(&self.region.provider, bundle_name, detail),
                bundle_path: bundle_name.clone(),
                bundle_hash,
                category: detail.category.clone(),
                file_size: detail.file_size,
                priority,
                export_payloads: true,
                stage_haruki_3d: false,
            });
        }

        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.bundle_path.cmp(&b.bundle_path))
        });
        tasks
    }

    pub(super) fn matches_raw_bundle_filters(&self, bundle_path: &str) -> bool {
        let Some(raw_bundles) = self.region.export.raw_bundles.as_ref() else {
            return false;
        };
        let include_patterns = compile_patterns(&raw_bundles.include);
        let exclude_patterns = compile_patterns(&raw_bundles.exclude);
        (include_patterns.is_empty() || matches_any(&include_patterns, bundle_path))
            && !matches_any(&exclude_patterns, bundle_path)
    }

    pub(super) fn raw_bundle_output_path(
        &self,
        asset_save_dir: &str,
        bundle_path: &str,
    ) -> Result<PathBuf, AssetExecutionError> {
        let root = self
            .region
            .export
            .raw_bundles
            .as_ref()
            .and_then(|raw_bundles| raw_bundles.output_dir.as_deref())
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(asset_save_dir).join("AssetBundles"));
        raw_bundle_output_path(&root, bundle_path)
    }

    pub(super) fn build_raw_bundle_filter_tasks(
        &self,
        info: &AssetBundleInfo,
    ) -> Vec<DownloadTask> {
        let mut tasks = Vec::new();
        for (bundle_name, detail) in &info.bundles {
            if !self.matches_raw_bundle_filters(bundle_name) {
                continue;
            }
            let bundle_hash = match self.region.provider {
                RegionProviderConfig::Nuverse { .. } => detail.crc.to_string(),
                RegionProviderConfig::ColorfulPalette { .. } => detail.hash.clone(),
            };
            tasks.push(DownloadTask {
                download_path: download_path_for_region(&self.region.provider, bundle_name, detail),
                bundle_path: bundle_name.clone(),
                bundle_hash,
                category: detail.category.clone(),
                file_size: detail.file_size,
                priority: usize::MAX,
                export_payloads: false,
                stage_haruki_3d: false,
            });
        }
        tasks.sort_by(|a, b| a.bundle_path.cmp(&b.bundle_path));
        tasks
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;

    use tempfile::tempdir;

    use crate::core::config::{AppConfig, RawBundleExportConfig, RegionProviderConfig};
    use crate::core::download_records::DownloadRecord;
    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::super::model::{
        AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext,
    };
    use super::super::planning::{raw_bundle_output_path, should_download_bundle};

    use super::super::test_support::test_region;

    #[test]
    fn raw_bundle_filters_are_independent_of_haruki_3d() {
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.raw_bundles = Some(RawBundleExportConfig {
            output_dir: None,
            include: vec!["^live_pv/model/characterv2/body/".to_string()],
            exclude: Vec::new(),
        });
        region.filters.on_demand.clear();
        region.filters.skip = vec![".*".to_string()];
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let config = AppConfig::default();

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        assert!(
            executor.matches_raw_bundle_filters("live_pv/model/characterv2/body/01"),
            "raw bundle retention must remain independent while 3D is disabled"
        );
        assert!(!executor.matches_raw_bundle_filters("live_pv/model/characterv2/face/01"));

        let detail = |bundle_name: &str| AssetBundleDetail {
            bundle_name: bundle_name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: format!("{bundle_name}-hash"),
            category: AssetCategory::OnDemand,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "live_pv/model/characterv2/body/01".to_string(),
                    detail("live_pv/model/characterv2/body/01"),
                ),
                (
                    "live_pv/model/characterv2/face/01".to_string(),
                    detail("live_pv/model/characterv2/face/01"),
                ),
            ]),
        };
        let tasks = executor.build_raw_bundle_filter_tasks(&info);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].bundle_path, "live_pv/model/characterv2/body/01");
    }

    #[test]
    fn build_download_tasks_skips_unchanged_and_queues_changed() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let config = AppConfig::default();
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let ctx = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();

        let detail = |hash: &str| AssetBundleDetail {
            bundle_name: String::new(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: hash.to_string(),
            category: AssetCategory::StartApp,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                ("start/a".to_string(), detail("h1")),
                ("start/aa".to_string(), detail("h2")),
            ]),
        };

        // Recorded hash matches -> skipped; bundle absent from record -> queued.
        let record = DownloadRecord::from([("start/a".to_string(), "h1".to_string())]);
        let tasks = ctx
            .build_download_tasks(&info, &record, &DownloadRecord::new(), false)
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert!(
            !paths.contains(&"start/a"),
            "unchanged bundle must be skipped"
        );
        assert!(paths.contains(&"start/aa"), "new bundle must be queued");

        // Recorded hash differs -> re-queued.
        let stale = DownloadRecord::from([("start/a".to_string(), "OLD".to_string())]);
        let tasks = ctx
            .build_download_tasks(&info, &stale, &DownloadRecord::new(), false)
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert!(
            paths.contains(&"start/a"),
            "changed bundle must be re-queued"
        );
    }

    #[tokio::test]
    async fn build_download_tasks_routes_3d_only_matches_to_staging() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.filters.on_demand.clear();
        region.export.haruki_3d = crate::core::config::Haruki3dExportConfig {
            enabled: true,
            work_dir: temp.path().join("3d-work").to_string_lossy().into_owned(),
            manifest_file: temp
                .path()
                .join("runtime/haruki-3d-export-manifest.json")
                .to_string_lossy()
                .into_owned(),
            include: vec!["^(start/a|live_pv/model/characterv2/body/)".to_string()],
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let config = AppConfig::default();
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let detail = |bundle_name: &str, category| AssetBundleDetail {
            bundle_name: bundle_name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: format!("{bundle_name}-hash"),
            category,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "start/a".to_string(),
                    detail("start/a", AssetCategory::StartApp),
                ),
                (
                    "live_pv/model/characterv2/body/01".to_string(),
                    detail("live_pv/model/characterv2/body/01", AssetCategory::OnDemand),
                ),
            ]),
        };

        let tasks = executor
            .build_download_tasks(&info, &DownloadRecord::new(), &DownloadRecord::new(), false)
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "3D matches with missing staging must be merged once after ordinary download filtering"
        );
        assert!(
            tasks[0].export_payloads && tasks[0].stage_haruki_3d,
            "ordinary tasks must export payloads"
        );
        assert!(
            !tasks[1].export_payloads && tasks[1].stage_haruki_3d,
            "3D-only tasks must only stage raw bundles"
        );

        let haruki_3d_record = DownloadRecord::from([(
            "live_pv/model/characterv2/body/01".to_string(),
            "live_pv/model/characterv2/body/01-hash".to_string(),
        )]);
        let tasks = executor
            .build_download_tasks(
                &info,
                &DownloadRecord::new(),
                &haruki_3d_record,
                executor.can_reuse_haruki_3d_download_record().await,
            )
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "the independent 3D record must not skip bundles when the runtime manifest is missing"
        );

        let manifest = Path::new(&region.export.haruki_3d.manifest_file);
        std::fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        std::fs::write(manifest, b"{broken").unwrap();
        let tasks = executor
            .build_download_tasks(
                &info,
                &DownloadRecord::new(),
                &haruki_3d_record,
                executor.can_reuse_haruki_3d_download_record().await,
            )
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "a malformed 3D runtime manifest must not make the download record reusable"
        );

        std::fs::write(manifest, br#"{"parts/example":{"bundleLength":1}}"#).unwrap();
        let tasks = executor
            .build_download_tasks(
                &info,
                &DownloadRecord::new(),
                &haruki_3d_record,
                executor.can_reuse_haruki_3d_download_record().await,
            )
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a"],
            "the independent 3D record must skip an unchanged bundle even after staging cleanup"
        );
    }

    #[test]
    fn download_filters_match_go_logic() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "".to_string(),
            asset_bundle_url_template: "".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });

        assert!(should_download_bundle(
            &region,
            "start/a",
            &AssetCategory::StartApp
        ));
        assert!(should_download_bundle(
            &region,
            "ond/a",
            &AssetCategory::OnDemand
        ));
        assert!(should_download_bundle(
            &region,
            "live_pv/model/characterv2/body/99/0018/ladies_s",
            &AssetCategory::LivePv
        ));
        assert!(!should_download_bundle(
            &region,
            "other/a",
            &AssetCategory::OnDemand
        ));
        assert!(!should_download_bundle(
            &region,
            "character/member/001",
            &AssetCategory::LivePv
        ));
    }

    #[test]
    fn raw_bundle_output_path_appends_bundle_extension_and_rejects_unsafe_paths() {
        let root = std::path::Path::new("/tmp/raw-root");
        assert_eq!(
            raw_bundle_output_path(root, "live_pv/model/character/body/foo").unwrap(),
            root.join("live_pv/model/character/body/foo.bundle")
        );
        assert_eq!(
            raw_bundle_output_path(root, "character/motion/costume_setting/01_00.bundle").unwrap(),
            root.join("character/motion/costume_setting/01_00.bundle")
        );
        assert!(raw_bundle_output_path(root, "").is_err());
        assert!(raw_bundle_output_path(root, "/absolute/path").is_err());
        assert!(raw_bundle_output_path(root, "../escape").is_err());
        assert!(raw_bundle_output_path(root, "safe/../escape").is_err());
        assert!(raw_bundle_output_path(root, "safe/./escape").is_err());
    }
}
