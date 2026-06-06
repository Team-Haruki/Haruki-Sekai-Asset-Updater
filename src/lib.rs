#![allow(clippy::result_large_err)]

pub mod core;
pub mod service;

pub use core::assetstudio_ffi::{
    AssetStudioAssetInfo, AssetStudioFfiClient, AssetStudioFfiVersion, AssetStudioInspectOptions,
    AssetStudioInspectResponse, AssetStudioObjectPayload, AssetStudioObjectReadBatchOutput,
    AssetStudioObjectReadOptions, AssetStudioObjectReadOutput, AssetStudioReadKind,
};
pub use core::config::AppConfig;
pub use service::http::{build_router, AppState};
pub use service::jobs::JobManager;
