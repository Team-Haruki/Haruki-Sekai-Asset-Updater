//! Deciding which bundles the 3D export needs.

//! The 3D export pipeline: staging, dependency indexes, exporter invocation.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::super::model::{AssetBundleInfo, AssetExecutionContext, DownloadTask};
use super::super::planning::download_path_for_region;
use crate::core::config::RegionProviderConfig;
use crate::core::download_records::DownloadRecord;
use crate::core::errors::AssetExecutionError;
use crate::core::regions::{compile_patterns, matches_any};

pub(super) fn bundle_dependency_closure(info: &AssetBundleInfo, bundle_name: &str) -> Vec<String> {
    let mut closure = HashSet::new();
    let mut pending = vec![bundle_name.to_string()];
    while let Some(current) = pending.pop() {
        let Some(detail) = info.bundles.get(&current) else {
            continue;
        };
        for dependency in &detail.dependencies {
            if closure.insert(dependency.clone()) {
                pending.push(dependency.clone());
            }
        }
    }
    closure.remove(bundle_name);
    let mut result: Vec<_> = closure.into_iter().collect();
    result.sort();
    result
}

impl AssetExecutionContext {
    pub(crate) fn append_haruki_3d_download_tasks(
        &self,
        mut tasks: Vec<DownloadTask>,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
        can_reuse_download_record: bool,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        tasks.extend(self.build_haruki_3d_download_tasks(
            info,
            downloaded_assets,
            can_reuse_download_record,
        )?);
        tasks.sort_by(|a, b| {
            a.bundle_path
                .cmp(&b.bundle_path)
                .then_with(|| a.priority.cmp(&b.priority))
        });
        tasks.dedup_by(|a, b| {
            if a.bundle_path != b.bundle_path {
                return false;
            }
            b.export_payloads |= a.export_payloads;
            b.stage_haruki_3d |= a.stage_haruki_3d;
            true
        });
        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.bundle_path.cmp(&b.bundle_path))
        });
        Ok(tasks)
    }

    pub(crate) fn matches_haruki_3d_filters(&self, bundle_path: &str) -> bool {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled || haruki_3d.include.is_empty() {
            return false;
        }
        let include_patterns = compile_patterns(&haruki_3d.include);
        let exclude_patterns = compile_patterns(&haruki_3d.exclude);
        matches_any(&include_patterns, bundle_path) && !matches_any(&exclude_patterns, bundle_path)
    }

    pub(super) fn build_haruki_3d_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        self.build_haruki_3d_filter_tasks(info)
    }

    pub(super) fn build_haruki_3d_download_tasks(
        &self,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
        can_reuse_download_record: bool,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        if self.haruki_3d_work_asset_root().is_none() {
            return Ok(Vec::new());
        }
        let mut tasks = Vec::new();
        for task in self.build_haruki_3d_filter_tasks(info) {
            let has_current_record = can_reuse_download_record
                && downloaded_assets
                    .get(&task.bundle_path)
                    .is_some_and(|existing| existing == &task.bundle_hash);
            if !has_current_record {
                tasks.push(DownloadTask {
                    stage_haruki_3d: true,
                    ..task
                });
            }
        }
        Ok(tasks)
    }

    pub(crate) async fn can_reuse_haruki_3d_download_record(&self) -> bool {
        let manifest_file = self.region.export.haruki_3d.manifest_file.clone();
        tokio::task::spawn_blocking(move || {
            let Ok(bytes) = std::fs::read(manifest_file) else {
                return false;
            };
            sonic_rs::from_slice::<HashMap<String, sonic_rs::Value>>(&bytes)
                .is_ok_and(|entries| !entries.is_empty())
        })
        .await
        .unwrap_or(false)
    }

    pub(super) fn build_haruki_3d_filter_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        let mut selected: HashSet<String> = info
            .bundles
            .keys()
            .filter(|bundle_name| self.matches_haruki_3d_filters(bundle_name))
            .cloned()
            .collect();
        let mut pending: Vec<String> = selected.iter().cloned().collect();
        while let Some(bundle_name) = pending.pop() {
            let Some(detail) = info.bundles.get(&bundle_name) else {
                continue;
            };
            for dependency in &detail.dependencies {
                if info.bundles.contains_key(dependency) && selected.insert(dependency.clone()) {
                    pending.push(dependency.clone());
                }
            }
        }
        let mut tasks = Vec::new();
        for (bundle_name, detail) in &info.bundles {
            if !selected.contains(bundle_name) {
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
                stage_haruki_3d: true,
            });
        }
        tasks.sort_by(|a, b| a.bundle_path.cmp(&b.bundle_path));
        tasks
    }

    pub(super) fn save_haruki_3d_dependency_index(
        path: &Path,
        info: &AssetBundleInfo,
        tasks: &[DownloadTask],
    ) -> Result<(), AssetExecutionError> {
        let index: HashMap<String, Vec<String>> = tasks
            .iter()
            .map(|task| {
                (
                    task.bundle_path.clone(),
                    bundle_dependency_closure(info, &task.bundle_path),
                )
            })
            .collect();
        let bytes = sonic_rs::to_vec_pretty(&index)
            .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?;
        Self::write_haruki_3d_work_bundle(path, &bytes)
    }
}
