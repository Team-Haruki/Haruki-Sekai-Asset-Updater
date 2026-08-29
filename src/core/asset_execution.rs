//! Running an asset update: planning, downloading, exporting, recording.
//!
//! This file is the facade; the submodules below hold the implementation.
//! Every external path -- `crate::core::asset_execution::Thing` -- is
//! unchanged.

mod cache;
mod crypto;
mod download;
mod haruki_3d;
mod model;
mod planning;
mod progress;
mod provider;
mod runner;
#[cfg(test)]
mod test_support;

pub use crypto::{decrypt_asset_bundle_info, deobfuscate};
pub use model::{
    AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext, Haruki3dExportSummary,
};
pub use planning::should_download_bundle;
pub use progress::ExecutionProgressUpdate;
pub use provider::fetch_live_asset_bundle_info;
