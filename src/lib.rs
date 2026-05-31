#![allow(clippy::result_large_err)]

pub mod core;
pub mod service;

pub use core::assetstudio_native::{
    AssetStudioAssetInfo, AssetStudioExportOptions, AssetStudioExportResponse,
    AssetStudioInspectOptions, AssetStudioInspectResponse, AssetStudioNativeClient,
    AssetStudioNativeVersion,
};
pub use core::config::AppConfig;
pub use service::http::{build_router, AppState};
pub use service::jobs::JobManager;
