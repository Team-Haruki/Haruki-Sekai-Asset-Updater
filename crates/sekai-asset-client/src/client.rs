use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, COOKIE, SET_COOKIE,
    USER_AGENT,
};
use sekai_asset_pipeline::{
    decrypt_asset_bundle_info, AssetBundleInfo, ProviderKind, ResolvedRelease,
};

use crate::options::HttpVersion;
use crate::provider::cache_buster_jst;
use crate::{ClientConfig, ClientError, ProviderEndpoint, RetryOptions};

const DEFAULT_COOKIE_BOOTSTRAP_URL: &str = "https://issue.sekai.colorfulpalette.org/api/signature";

#[derive(Debug, Clone, Default)]
pub struct RequestedRelease {
    pub asset_version: Option<String>,
    pub asset_hash: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct ManifestCrypto<'a> {
    pub aes_key_hex: &'a str,
    pub aes_iv_hex: &'a str,
}

#[derive(Clone)]
pub struct SekaiAssetClient {
    pub(crate) http: reqwest::Client,
    pub(crate) endpoint: ProviderEndpoint,
    pub(crate) retry: RetryOptions,
    pub(crate) max_manifest_bytes: u64,
    pub(crate) max_bundle_bytes: u64,
    pub(crate) cookie: Option<HeaderValue>,
}

impl std::fmt::Debug for SekaiAssetClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SekaiAssetClient")
            .field("endpoint", &self.endpoint)
            .field("retry", &self.retry)
            .field("max_manifest_bytes", &self.max_manifest_bytes)
            .field("max_bundle_bytes", &self.max_bundle_bytes)
            .field("has_runtime_cookie", &self.cookie.is_some())
            .finish_non_exhaustive()
    }
}

impl SekaiAssetClient {
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("ProductName/134 CFNetwork/1408.0.4 Darwin/22.5.0"),
        );
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        headers.insert(
            ACCEPT_LANGUAGE,
            HeaderValue::from_static("zh-CN,zh-Hans;q=0.9"),
        );
        headers.insert(
            "X-Unity-Version",
            HeaderValue::from_str(&config.unity_version)
                .map_err(|error| ClientError::BuildClient(error.to_string()))?,
        );

        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(100)
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .tcp_keepalive(Duration::from_secs(30));
        if config.http_version == HttpVersion::Http1 {
            builder = builder.http1_only();
        }
        if let Some(proxy) = config.proxy.as_deref().filter(|value| !value.is_empty()) {
            builder = builder.proxy(
                reqwest::Proxy::all(proxy)
                    .map_err(|error| ClientError::BuildClient(error.to_string()))?,
            );
        }

        Ok(Self {
            http: builder
                .build()
                .map_err(|error| ClientError::BuildClient(error.to_string()))?,
            endpoint: config.endpoint,
            retry: config.retry,
            max_manifest_bytes: config.limits.max_manifest_bytes,
            max_bundle_bytes: config.limits.max_bundle_bytes,
            cookie: None,
        })
    }

    pub const fn provider_kind(&self) -> ProviderKind {
        self.endpoint.kind()
    }

    pub async fn bootstrap_cookie(&mut self, url: Option<&str>) -> Result<bool, ClientError> {
        let url = url.unwrap_or(DEFAULT_COOKIE_BOOTSTRAP_URL).to_string();
        let cookie = self
            .retry("cookie bootstrap", || async {
                let response =
                    self.http
                        .post(&url)
                        .send()
                        .await
                        .map_err(|source| ClientError::Network {
                            url: url.clone(),
                            source,
                        })?;
                ensure_success(&url, response.status().as_u16())?;
                Ok(response.headers().get(SET_COOKIE).cloned())
            })
            .await?;
        self.cookie = cookie;
        Ok(self.cookie.is_some())
    }

    pub async fn resolve_release(
        &self,
        requested: &RequestedRelease,
    ) -> Result<ResolvedRelease, ClientError> {
        match &self.endpoint {
            ProviderEndpoint::ColorfulPalette { .. } => {
                let asset_version = requested
                    .asset_version
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(ClientError::MissingReleaseInput)?;
                let asset_hash = requested
                    .asset_hash
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(ClientError::MissingReleaseInput)?;
                Ok(ResolvedRelease {
                    asset_version: asset_version.to_string(),
                    asset_hash: asset_hash.to_string(),
                })
            }
            ProviderEndpoint::Nuverse { .. } => {
                let url = self
                    .endpoint
                    .render_release_url()
                    .expect("Nuverse endpoints always have a release URL");
                let body = self.get_bytes(&url, self.max_manifest_bytes).await?;
                let asset_version = std::str::from_utf8(&body)
                    .ok()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| ClientError::InvalidReleaseResponse { url: url.clone() })?;
                Ok(ResolvedRelease {
                    asset_version: asset_version.to_string(),
                    asset_hash: String::new(),
                })
            }
        }
    }

    pub async fn fetch_manifest(
        &self,
        release: &ResolvedRelease,
        crypto: ManifestCrypto<'_>,
    ) -> Result<AssetBundleInfo, ClientError> {
        let url = self
            .endpoint
            .render_asset_info_url(release, &cache_buster_jst());
        let body = self.get_bytes(&url, self.max_manifest_bytes).await?;
        decrypt_asset_bundle_info(crypto.aes_key_hex, crypto.aes_iv_hex, &body)
            .map_err(ClientError::from)
    }

    pub(crate) async fn send_get(&self, url: &str) -> Result<reqwest::Response, ClientError> {
        let mut request = self.http.get(url);
        if let Some(cookie) = &self.cookie {
            request = request.header(COOKIE, cookie);
        }
        let response = request
            .send()
            .await
            .map_err(|source| ClientError::Network {
                url: url.to_string(),
                source,
            })?;
        ensure_success(url, response.status().as_u16())?;
        Ok(response)
    }

    async fn get_bytes(&self, url: &str, limit: u64) -> Result<Vec<u8>, ClientError> {
        self.retry("asset HTTP GET", || async {
            let mut response = self.send_get(url).await?;
            let declared = response.content_length();
            reject_declared_size(url, limit, declared)?;
            let capacity = declared
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            let mut body = Vec::with_capacity(capacity);
            let mut observed = 0_u64;
            while let Some(chunk) =
                response
                    .chunk()
                    .await
                    .map_err(|source| ClientError::Network {
                        url: url.to_string(),
                        source,
                    })?
            {
                observed = observed.saturating_add(chunk.len() as u64);
                if observed > limit {
                    return Err(ClientError::ResponseTooLarge {
                        url: url.to_string(),
                        limit,
                        declared,
                        observed,
                    });
                }
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        })
        .await
    }

    pub(crate) async fn retry<T, Operation, FutureResult>(
        &self,
        operation_name: &str,
        mut operation: Operation,
    ) -> Result<T, ClientError>
    where
        Operation: FnMut() -> FutureResult,
        FutureResult: Future<Output = Result<T, ClientError>>,
    {
        let attempts = self.retry.attempts.max(1);
        for attempt in 1..=attempts {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < attempts && error.is_retryable() => {
                    let delay = retry_delay(&self.retry, attempt);
                    tracing::warn!(
                        operation = operation_name,
                        attempt,
                        max_attempts = attempts,
                        delay_ms = delay.as_millis(),
                        error = %error,
                        "asset client operation failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("asset client retry loop always returns")
    }
}

pub(crate) fn reject_declared_size(
    url: &str,
    limit: u64,
    declared: Option<u64>,
) -> Result<(), ClientError> {
    if declared.is_some_and(|length| length > limit) {
        return Err(ClientError::ResponseTooLarge {
            url: url.to_string(),
            limit,
            declared,
            observed: 0,
        });
    }
    Ok(())
}

fn ensure_success(url: &str, status: u16) -> Result<(), ClientError> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(ClientError::HttpStatus {
            url: url.to_string(),
            status,
        })
    }
}

fn retry_delay(options: &RetryOptions, attempt: usize) -> Duration {
    let multiplier = 1_u32
        .checked_shl(attempt.saturating_sub(1) as u32)
        .unwrap_or(u32::MAX);
    options
        .initial_backoff
        .saturating_mul(multiplier)
        .min(options.max_backoff.max(options.initial_backoff))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::header::{COOKIE, SET_COOKIE};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Router;
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
    use sekai_asset_pipeline::{AssetBundleDetail, AssetBundleInfo, AssetCategory};

    use super::{ManifestCrypto, RequestedRelease, SekaiAssetClient};
    use crate::{ClientConfig, ClientErrorCategory, ClientLimits, ProviderEndpoint, RetryOptions};

    const KEY_HEX: &str = "00112233445566778899aabbccddeeff";
    const IV_HEX: &str = "0102030405060708090a0b0c0d0e0f10";

    fn encrypt_manifest(info: &AssetBundleInfo) -> Vec<u8> {
        let key = hex::decode(KEY_HEX).unwrap();
        let iv = hex::decode(IV_HEX).unwrap();
        let payload = rmp_serde::to_vec_named(info).unwrap();
        let mut padded = payload.clone();
        let original_len = padded.len();
        let padding = 16 - (original_len % 16);
        padded.resize(original_len + padding, 0);
        cbc::Encryptor::<aes::Aes128>::new_from_slices(&key, &iv)
            .unwrap()
            .encrypt_padded::<Pkcs7>(&mut padded, original_len)
            .unwrap()
            .to_vec()
    }

    fn manifest() -> AssetBundleInfo {
        AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
                    category: AssetCategory::StartApp,
                    crc: 123,
                    file_size: 10,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: Some("startapp".to_string()),
                },
            )]),
        }
    }

    #[tokio::test]
    async fn cookie_release_and_encrypted_manifest_form_one_client_flow() {
        let expected = manifest();
        let encrypted = encrypt_manifest(&expected);
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route(
                "/signature",
                post(|| async { ([(SET_COOKIE, "session=abc; Path=/")], "ok") }),
            )
            .route("/version/5.2.0", get(|| async { "20250321" }))
            .route(
                "/info/5.2.0/20250321",
                get({
                    let encrypted = encrypted.clone();
                    let cookie_seen = cookie_seen.clone();
                    move |headers: HeaderMap| {
                        let encrypted = encrypted.clone();
                        let cookie_seen = cookie_seen.clone();
                        async move {
                            if headers
                                .get(COOKIE)
                                .and_then(|value| value.to_str().ok())
                                .is_some_and(|value| value.contains("session=abc"))
                            {
                                cookie_seen.store(true, Ordering::SeqCst);
                            }
                            Body::from(encrypted)
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut config = ClientConfig::new(
            ProviderEndpoint::Nuverse {
                asset_version_url_template: format!("http://{address}/version/{{app_version}}"),
                asset_info_url_template: format!(
                    "http://{address}/info/{{app_version}}/{{asset_version}}"
                ),
                asset_bundle_url_template: format!("http://{address}/bundle/{{bundle_path}}"),
                app_version: "5.2.0".to_string(),
            },
            "2022.3.21f1",
        );
        config.retry = RetryOptions {
            attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        let mut client = SekaiAssetClient::new(config).unwrap();

        assert!(client
            .bootstrap_cookie(Some(&format!("http://{address}/signature")))
            .await
            .unwrap());
        let release = client
            .resolve_release(&RequestedRelease::default())
            .await
            .unwrap();
        assert_eq!(release.asset_version, "20250321");
        assert!(release.asset_hash.is_empty());
        let actual = client
            .fetch_manifest(
                &release,
                ManifestCrypto {
                    aes_key_hex: KEY_HEX,
                    aes_iv_hex: IV_HEX,
                },
            )
            .await
            .unwrap();

        assert_eq!(actual.bundles.len(), expected.bundles.len());
        assert!(actual.bundles.contains_key("start/a"));
        assert!(cookie_seen.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn manifest_content_length_obeys_its_own_hard_limit() {
        let encrypted = encrypt_manifest(&manifest());
        let app = Router::new().route(
            "/manifest/1/hash",
            get(move || {
                let encrypted = encrypted.clone();
                async move { Body::from(encrypted) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = ClientConfig::new(
            ProviderEndpoint::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{address}/manifest/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: String::new(),
                profile: "production".to_string(),
                profile_hash: "profile".to_string(),
            },
            "2022.3.21f1",
        );
        config.limits = ClientLimits {
            max_manifest_bytes: 8,
            max_bundle_bytes: 64,
        };
        let client = SekaiAssetClient::new(config).unwrap();
        let release = client
            .resolve_release(&RequestedRelease {
                asset_version: Some("1".to_string()),
                asset_hash: Some("hash".to_string()),
            })
            .await
            .unwrap();

        let error = client
            .fetch_manifest(
                &release,
                ManifestCrypto {
                    aes_key_hex: KEY_HEX,
                    aes_iv_hex: IV_HEX,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.category(), ClientErrorCategory::SizeExceeded);
    }
}
