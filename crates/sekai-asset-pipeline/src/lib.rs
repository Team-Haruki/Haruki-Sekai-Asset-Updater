//! Reusable primitives for resolving and processing individual Sekai bundles.
//!
//! Application concerns such as HTTP routing, job state, batch scheduling,
//! download records, publishing, and Git synchronization deliberately stay in
//! the consuming application.

mod bundle;
mod crypto;
mod error;
mod manifest;
mod options;
mod path;
mod provider;

pub use bundle::ResolvedBundle;
pub use crypto::{decrypt_asset_bundle_info, deobfuscate, deobfuscate_owned};
pub use error::PipelineError;
pub use manifest::{asset_category_name, AssetBundleDetail, AssetBundleInfo, AssetCategory};
pub use options::{
    AcbExportOptions, AssetStudioOptions, AudioExportOptions, AudioFormat, BackendsOptions,
    ConcurrencyOptions, CpuResourceOptions, CpuThrottleOptions, ExecutionOptions, HcaExportOptions,
    ImageEncodingOptions, ImageExportOptions, ImageFormat, MediaBackend, MediaOptions,
    MemoryResourceOptions, PipelineOptions, PipelineRegionOptions, PngCompression,
    RegionExportOptions, RegionRuntimeOptions, ResourceOptions, RetryOptions, UsmExportOptions,
    VideoExportOptions, VideoFormat,
};
pub use path::{raw_bundle_output_path, validate_relative_bundle_path};
pub use provider::{ProviderEndpoint, ProviderKind, ResolvedRelease};
