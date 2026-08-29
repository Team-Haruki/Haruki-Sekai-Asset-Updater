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
