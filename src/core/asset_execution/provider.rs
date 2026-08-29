//! Talking to a region's asset provider: cookies, asset info, bundle URLs.

use chrono::FixedOffset;
use reqwest::header::{COOKIE, SET_COOKIE};

use super::crypto::decrypt_asset_bundle_info;
use super::model::{AssetBundleInfo, AssetExecutionContext, DownloadTask};
use crate::core::config::{AppConfig, RegionConfig, RegionProviderConfig};
use crate::core::errors::{format_reqwest_error_chain, AssetExecutionError};
use crate::core::models::AssetUpdateRequest;
use crate::core::retry::retry_async;

pub(super) fn time_arg_jst() -> String {
    let tz = FixedOffset::east_opt(9 * 3600).unwrap();
    format!(
        "?t={}",
        chrono::Utc::now().with_timezone(&tz).format("%Y%m%d%H%M%S")
    )
}

pub(super) fn is_retryable_http_error(err: &AssetExecutionError) -> bool {
    match err {
        AssetExecutionError::Http(_) => true,
        // 5xx are transient; 429 (Too Many Requests) and 408 (Request Timeout) are the canonical
        // "back off and retry" signals that Project Sekai CDNs/rate limiters emit under load.
        AssetExecutionError::HttpStatus { status, .. } => {
            *status >= 500 || *status == 429 || *status == 408
        }
        _ => false,
    }
}

pub async fn fetch_live_asset_bundle_info(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    request: &AssetUpdateRequest,
) -> Result<AssetBundleInfo, AssetExecutionError> {
    let mut context = AssetExecutionContext::new(app_config, region_name, region, request)?;
    if context.requires_cookies() {
        context.fetch_runtime_cookies().await?;
    }
    context.fetch_asset_bundle_info().await
}

impl AssetExecutionContext {
    pub(super) fn requires_cookies(&self) -> bool {
        match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                required_cookies, ..
            } => *required_cookies,
            RegionProviderConfig::Nuverse {
                required_cookies, ..
            } => *required_cookies,
        }
    }

    pub(super) async fn fetch_runtime_cookies(&mut self) -> Result<(), AssetExecutionError> {
        let url = match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                cookie_bootstrap_url,
                ..
            }
            | RegionProviderConfig::Nuverse {
                cookie_bootstrap_url,
                ..
            } => cookie_bootstrap_url.clone().unwrap_or_else(|| {
                "https://issue.sekai.colorfulpalette.org/api/signature".to_string()
            }),
        };
        self.runtime_cookie = retry_async(
            &self.retry,
            "cookie bootstrap",
            |_| async {
                let response = self.client.post(&url).send().await.map_err(|err| {
                    tracing::warn!(
                        url,
                        error = %format_reqwest_error_chain(&err),
                        "HTTP request failed"
                    );
                    AssetExecutionError::Http(err)
                })?;
                if response.status().is_success() {
                    Ok(response
                        .headers()
                        .get(SET_COOKIE)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string))
                } else {
                    Err(AssetExecutionError::HttpStatus {
                        url: url.clone(),
                        status: response.status().as_u16(),
                    })
                }
            },
            is_retryable_http_error,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn fetch_asset_bundle_info(
        &mut self,
    ) -> Result<AssetBundleInfo, AssetExecutionError> {
        let url = self.render_asset_info_url().await?;
        let body = self.get_with_retry(&url).await?;
        decrypt_asset_bundle_info(
            self.region.crypto.aes_key_hex.as_deref().ok_or_else(|| {
                AssetExecutionError::MissingCryptoConfig {
                    region: self.region_name.clone(),
                }
            })?,
            self.region.crypto.aes_iv_hex.as_deref().ok_or_else(|| {
                AssetExecutionError::MissingCryptoConfig {
                    region: self.region_name.clone(),
                }
            })?,
            &body,
        )
    }

    pub(super) async fn render_asset_info_url(&mut self) -> Result<String, AssetExecutionError> {
        match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                asset_info_url_template,
                profile,
                profile_hashes,
                ..
            } => {
                let asset_version = self.request.asset_version.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let asset_hash = self.request.asset_hash.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let profile_hash = profile_hashes.get(profile).ok_or_else(|| {
                    AssetExecutionError::MissingProfileHash {
                        region: self.region_name.clone(),
                        profile: profile.clone(),
                    }
                })?;
                Ok(asset_info_url_template
                    .replace("{env}", profile)
                    .replace("{hash}", profile_hash)
                    .replace("{asset_version}", asset_version)
                    .replace("{asset_hash}", asset_hash)
                    + &time_arg_jst())
            }
            RegionProviderConfig::Nuverse {
                asset_version_url,
                app_version,
                asset_info_url_template,
                ..
            } => {
                // For nuverse, always fetch the version from asset_version_url.
                // The incoming request.asset_version is intentionally ignored here
                // to match Go reference behavior.
                let version_url = asset_version_url.replace("{app_version}", app_version);
                let resolved_version =
                    String::from_utf8_lossy(&self.get_with_retry(&version_url).await?)
                        .trim()
                        .to_string();
                self.resolved_asset_version = Some(resolved_version.clone());
                Ok(asset_info_url_template
                    .replace("{app_version}", app_version)
                    .replace("{asset_version}", &resolved_version)
                    + &time_arg_jst())
            }
        }
    }

    pub(super) fn render_bundle_url(
        &self,
        task: &DownloadTask,
    ) -> Result<String, AssetExecutionError> {
        match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                asset_bundle_url_template,
                profile,
                profile_hashes,
                ..
            } => {
                let asset_version = self.request.asset_version.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let asset_hash = self.request.asset_hash.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let profile_hash = profile_hashes.get(profile).ok_or_else(|| {
                    AssetExecutionError::MissingProfileHash {
                        region: self.region_name.clone(),
                        profile: profile.clone(),
                    }
                })?;

                Ok(asset_bundle_url_template
                    .replace("{bundle_path}", &task.download_path)
                    .replace("{asset_version}", asset_version)
                    .replace("{asset_hash}", asset_hash)
                    .replace("{env}", profile)
                    .replace("{hash}", profile_hash)
                    + &time_arg_jst())
            }
            RegionProviderConfig::Nuverse {
                asset_bundle_url_template,
                app_version,
                ..
            } => {
                let asset_version = self
                    .resolved_asset_version
                    .as_deref()
                    .unwrap_or("<resolved-asset-version>");
                Ok(asset_bundle_url_template
                    .replace("{bundle_path}", &task.download_path)
                    .replace("{app_version}", app_version)
                    .replace("{asset_version}", asset_version)
                    + &time_arg_jst())
            }
        }
    }

    pub(super) async fn get_with_retry(&self, url: &str) -> Result<Vec<u8>, AssetExecutionError> {
        retry_async(
            &self.retry,
            "http get",
            |_| async {
                let mut request = self.client.get(url);
                if let Some(cookie) = &self.runtime_cookie {
                    request = request.header(COOKIE, cookie);
                }
                match request.send().await {
                    Ok(mut response) if response.status().is_success() => {
                        // Build the final owned buffer directly from response chunks. Calling
                        // `bytes().to_vec()` first aggregates into `Bytes` and then copies the
                        // entire response a second time.
                        const MAX_HTTP_PREALLOC_BYTES: u64 = 64 * 1024 * 1024;
                        let capacity = response
                            .content_length()
                            .map(|length| length.min(MAX_HTTP_PREALLOC_BYTES))
                            .and_then(|length| usize::try_from(length).ok())
                            .unwrap_or_default();
                        let mut body = Vec::with_capacity(capacity);
                        while let Some(chunk) = response.chunk().await? {
                            body.extend_from_slice(&chunk);
                        }
                        Ok(body)
                    }
                    Ok(response) => Err(AssetExecutionError::HttpStatus {
                        url: url.to_string(),
                        status: response.status().as_u16(),
                    }),
                    Err(err) => {
                        tracing::warn!(
                            url,
                            error = %format_reqwest_error_chain(&err),
                            "HTTP request failed"
                        );
                        Err(AssetExecutionError::Http(err))
                    }
                }
            },
            is_retryable_http_error,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::header::{COOKIE, SET_COOKIE};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Router;
    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, RawBundleExportConfig, RegionConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    };

    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::super::model::{
        AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext,
    };

    use super::super::test_support::{encrypt_asset_info, TEST_AES_IV_HEX, TEST_AES_KEY_HEX};

    #[tokio::test]
    async fn required_cookies_are_forwarded_and_nuverse_uses_resolved_version() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "ond/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "ond/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
                    category: AssetCategory::OnDemand,
                    crc: 888,
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: Some("download-root".to_string()),
                },
            )]),
        };
        let encrypted = encrypt_asset_info(&info);
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let version_hits = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/version/5.2.0",
                get({
                    let version_hits = version_hits.clone();
                    move || {
                        let version_hits = version_hits.clone();
                        async move {
                            version_hits.fetch_add(1, Ordering::SeqCst);
                            "20250321"
                        }
                    }
                }),
            )
            .route(
                "/info/5.2.0/20250321",
                get({
                    let encrypted = encrypted.clone();
                    move || async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            encrypted.clone(),
                        )
                    }
                }),
            )
            .route(
                "/bundle/download-root/ond/a",
                get({
                    let cookie_seen = cookie_seen.clone();
                    move |headers: HeaderMap| {
                        let cookie_seen = cookie_seen.clone();
                        async move {
                            if headers
                                .get(COOKIE)
                                .and_then(|value| value.to_str().ok())
                                .is_some_and(|value| value.contains("session=abc"))
                            {
                                cookie_seen.store(true, Ordering::SeqCst);
                            }
                            (
                                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                                Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                            )
                        }
                    }
                }),
            )
            .route(
                "/signature",
                post(|| async move { ([(SET_COOKIE.as_str(), "session=abc; Path=/")], "ok") }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::Nuverse {
                asset_version_url: format!("http://{addr}/version/{{app_version}}"),
                app_version: "5.2.0".to_string(),
                asset_info_url_template: format!(
                    "http://{addr}/info/{{app_version}}/{{asset_version}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                required_cookies: true,
                cookie_bootstrap_url: Some(format!("http://{addr}/signature")),
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: Vec::new(),
                on_demand: vec!["^ond/".to_string()],
                skip: Vec::new(),
                priority: vec!["^ond/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^ond/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("cn".to_string(), region.clone());
        let config = AppConfig {
            regions,
            backends: crate::core::config::BackendsConfig {
                media: crate::core::config::MediaBackendConfig {
                    ffmpeg_path: "ffmpeg".to_string(),
                    ..crate::core::config::MediaBackendConfig::default()
                },
                ..crate::core::config::BackendsConfig::default()
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            concurrency: crate::core::config::ConcurrencyConfig {
                download: 2,
                ..crate::core::config::ConcurrencyConfig::default()
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "cn", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();
        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(version_hits.load(Ordering::SeqCst), 1);
        assert!(cookie_seen.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn http_fetch_retries_on_503_then_succeeds() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
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
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: None,
                },
            )]),
        };
        let encrypted = encrypt_asset_info(&info);
        let info_hits = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/info/production/abc/1/hash",
                get({
                    let encrypted = encrypted.clone();
                    let info_hits = info_hits.clone();
                    move || {
                        let encrypted = encrypted.clone();
                        let info_hits = info_hits.clone();
                        async move {
                            let attempt = info_hits.fetch_add(1, Ordering::SeqCst);
                            if attempt < 2 {
                                (
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                    Body::from("retry"),
                                )
                            } else {
                                (axum::http::StatusCode::OK, Body::from(encrypted.clone()))
                            }
                        }
                    }
                }),
            )
            .route(
                "/bundle/start/a",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                        Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                    )
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{addr}/info/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^start/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            execution: crate::core::config::ExecutionConfig {
                retry: crate::core::config::RetryConfig {
                    attempts: 3,
                    initial_backoff_ms: 1,
                    max_backoff_ms: 1,
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();

        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(info_hits.load(Ordering::SeqCst), 3);
    }
}
