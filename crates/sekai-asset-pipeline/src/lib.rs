//! Reusable primitives for resolving and processing individual Sekai bundles.
//!
//! Application concerns such as HTTP routing, job state, batch scheduling,
//! download records, publishing, and Git synchronization deliberately stay in
//! the consuming application.

mod crypto;
mod error;
mod manifest;
mod path;

pub use crypto::{decrypt_asset_bundle_info, deobfuscate, deobfuscate_owned};
pub use error::PipelineError;
pub use manifest::{asset_category_name, AssetBundleDetail, AssetBundleInfo, AssetCategory};
pub use path::{raw_bundle_output_path, validate_relative_bundle_path};
