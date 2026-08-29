use std::time::Duration;

use crate::ProviderEndpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpVersion {
    #[default]
    Auto,
    Http1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryOptions {
    pub attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            attempts: 4,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(4),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientLimits {
    pub max_manifest_bytes: u64,
    pub max_bundle_bytes: u64,
}

impl Default for ClientLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 64 * 1024 * 1024,
            max_bundle_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoint: ProviderEndpoint,
    pub unity_version: String,
    pub proxy: Option<String>,
    pub http_version: HttpVersion,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub retry: RetryOptions,
    pub limits: ClientLimits,
    /// Force downloaded file contents to stable storage before the atomic
    /// rename. Disabled by default for ephemeral worker and Haruki inputs.
    pub durable_downloads: bool,
}

impl ClientConfig {
    pub fn new(endpoint: ProviderEndpoint, unity_version: impl Into<String>) -> Self {
        Self {
            endpoint,
            unity_version: unity_version.into(),
            proxy: None,
            http_version: HttpVersion::Auto,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(180),
            retry: RetryOptions::default(),
            limits: ClientLimits::default(),
            durable_downloads: false,
        }
    }
}
