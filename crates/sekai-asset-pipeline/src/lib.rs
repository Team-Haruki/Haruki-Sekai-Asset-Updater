//! Reusable primitives for resolving and processing individual Sekai bundles.
//!
//! Application concerns such as HTTP routing, job state, batch scheduling,
//! download records, publishing, and Git synchronization deliberately stay in
//! the consuming application.

mod bundle;
mod cleanup;
mod codec;
mod crypto;
mod error;
mod manifest;
mod options;
mod path;
mod provider;
mod retry;

pub use bundle::{Artifact, ArtifactManifest, BundleRequest, BundleResult, ResolvedBundle};
pub use cleanup::{remove_file_if_exists, remove_file_with_retries};
pub use codec::{
    codec_summary, decode_hca_bytes_to_wav, decode_hca_bytes_to_wav_bytes, decode_hca_to_wav,
    export_acb, export_acb_unique_to_memory, export_usm, export_usm_reader_to_memory,
    export_usm_to_memory, file_has_usm_magic, has_usm_magic, read_usm_metadata, CodecSummary,
    CODEC_BACKEND,
};
pub use crypto::{decrypt_asset_bundle_info, deobfuscate, deobfuscate_owned};
pub use error::{CodecError, PipelineError};
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
pub use retry::{retry_async, retry_sync, RetryPolicy};
