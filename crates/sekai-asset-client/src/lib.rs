//! Provider-aware HTTP transport for Project Sekai assets.
//!
//! The client resolves and downloads one immutable release at a time. Batch
//! scheduling, persistent caches, queue acknowledgement, and publishing stay
//! with the consuming application.

mod client;
mod download;
mod error;
mod options;
mod provider;

pub use client::{ManifestCrypto, RequestedRelease, SekaiAssetClient};
pub use download::DownloadedBundle;
pub use error::{ClientError, ClientErrorCategory};
pub use options::{ClientConfig, ClientLimits, HttpVersion, RetryOptions};
pub use provider::ProviderEndpoint;

pub use sekai_asset_pipeline::{ProviderKind, ResolvedRelease};
