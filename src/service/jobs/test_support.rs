//! Fixtures shared by the job modules' test suites.

use std::sync::Arc;

use crate::core::config::AppConfig;
use crate::core::models::{AssetUpdateMode, AssetUpdateRequest, JobSnapshot};

use super::manager::JobManager;

pub(super) fn request(region: &str) -> AssetUpdateRequest {
    AssetUpdateRequest {
        region: region.to_string(),
        asset_version: None,
        asset_hash: None,
        dry_run: false,
        mode: AssetUpdateMode::Update,
    }
}

pub(super) fn queued_job(region: &str) -> JobSnapshot {
    JobSnapshot::new(&request(region))
}

pub(super) fn manager() -> JobManager {
    JobManager::new(Arc::new(AppConfig::default()))
}
