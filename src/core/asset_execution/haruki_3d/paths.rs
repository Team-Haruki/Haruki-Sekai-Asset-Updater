//! Where the 3D pipeline keeps its work tree and its state files.

//! The 3D export pipeline: staging, dependency indexes, exporter invocation.

use std::path::{Path, PathBuf};

use super::super::model::AssetExecutionContext;
use crate::core::errors::AssetExecutionError;

impl AssetExecutionContext {
    pub(crate) fn haruki_3d_work_asset_root(&self) -> Option<PathBuf> {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled {
            return None;
        }
        let run_id = self
            .resolved_asset_version
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("current")
            .replace(['/', '\\', ':'], "_");
        Some(
            self.haruki_3d_state_root()?
                .join(run_id)
                .join("AssetBundles"),
        )
    }

    pub(super) fn haruki_3d_state_root(&self) -> Option<PathBuf> {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled {
            return None;
        }
        Some(Path::new(&Self::haruki_3d_work_dir(haruki_3d)).join(&self.region_name))
    }

    pub(crate) fn haruki_3d_download_record_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("downloaded_assets.json"))
    }

    pub(crate) fn haruki_3d_bundle_hash_index_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("bundle_sha256.json"))
    }

    pub(super) fn haruki_3d_bundle_dependency_index_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("bundle_dependencies.json"))
    }

    pub(crate) fn haruki_3d_work_dir(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
    ) -> String {
        if !haruki_3d.work_dir.trim().is_empty() {
            haruki_3d.work_dir.clone()
        } else {
            haruki_3d.staging_dir.clone()
        }
    }

    pub(super) fn required_haruki_3d_dependency_index_path(
        &self,
    ) -> Result<PathBuf, AssetExecutionError> {
        self.haruki_3d_bundle_dependency_index_path()
            .ok_or_else(|| {
                AssetExecutionError::BlockingTask(
                    "3D bundle dependency index path is unavailable".to_string(),
                )
            })
    }

    pub(super) fn required_haruki_3d_download_record_path(
        &self,
    ) -> Result<PathBuf, AssetExecutionError> {
        self.haruki_3d_download_record_path().ok_or_else(|| {
            AssetExecutionError::BlockingTask("3D download record path is unavailable".to_string())
        })
    }
}
