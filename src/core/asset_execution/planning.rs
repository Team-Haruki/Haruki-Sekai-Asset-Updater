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
