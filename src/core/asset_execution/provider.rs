//! Adapting Haruki region configuration to the shared asset client.

use super::model::{AssetExecutionContext, DownloadTask};
use crate::core::config::{AppConfig, RegionProviderConfig};
use crate::core::errors::AssetExecutionError;
use crate::core::models::AssetUpdateRequest;
use crate::core::pipeline::prepare_asset_run;
use sekai_asset_client::{ManifestCrypto, RequestedRelease};
use sekai_asset_pipeline::{AssetBundleInfo, BundleRequest};

pub async fn fetch_live_asset_bundle_info(
    app_config: &AppConfig,
    request: &AssetUpdateRequest,
) -> Result<AssetBundleInfo, AssetExecutionError> {
    let prepared = prepare_asset_run(app_config, request)?;
    let mut context = AssetExecutionContext::new(app_config, &prepared, request)?;
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
            }
            | RegionProviderConfig::Nuverse {
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
            } => cookie_bootstrap_url.as_deref(),
        };
        self.client.bootstrap_cookie(url).await?;
        Ok(())
    }

    pub(super) async fn fetch_asset_bundle_info(
        &mut self,
    ) -> Result<AssetBundleInfo, AssetExecutionError> {
        if let RegionProviderConfig::ColorfulPalette {
            profile,
            profile_hashes,
            ..
        } = &self.region.provider
        {
            if !profile_hashes.contains_key(profile) {
                return Err(AssetExecutionError::MissingProfileHash {
                    region: self.region_name.clone(),
                    profile: profile.clone(),
                });
            }
        }
        if matches!(
            self.region.provider,
            RegionProviderConfig::ColorfulPalette { .. }
        ) && (self
            .request
            .asset_version
            .as_deref()
            .is_none_or(str::is_empty)
            || self.request.asset_hash.as_deref().is_none_or(str::is_empty))
        {
            return Err(AssetExecutionError::MissingAssetVersionOrHash {
                region: self.region_name.clone(),
            });
        }
        let release = self
            .client
            .resolve_release(&RequestedRelease {
                asset_version: self.request.asset_version.clone(),
                asset_hash: self.request.asset_hash.clone(),
            })
            .await?;
        let aes_key_hex = self.region.crypto.aes_key_hex.as_deref().ok_or_else(|| {
            AssetExecutionError::MissingCryptoConfig {
                region: self.region_name.clone(),
            }
        })?;
        let aes_iv_hex = self.region.crypto.aes_iv_hex.as_deref().ok_or_else(|| {
            AssetExecutionError::MissingCryptoConfig {
                region: self.region_name.clone(),
            }
        })?;
        let manifest = self
            .client
            .fetch_manifest(
                &release,
                ManifestCrypto {
                    aes_key_hex,
                    aes_iv_hex,
                },
            )
            .await?;
        self.resolved_release = Some(release);
        Ok(manifest)
    }

    pub(super) fn bundle_request(
        &self,
        task: &DownloadTask,
    ) -> Result<BundleRequest, AssetExecutionError> {
        let release = self.resolved_release.clone().ok_or_else(|| {
            AssetExecutionError::BlockingTask(
                "asset release must be resolved before downloading bundles".to_string(),
            )
        })?;
        Ok(BundleRequest {
            region: self.region_name.clone(),
            provider: self.client.provider_kind(),
            release,
            bundle: task.bundle.clone(),
        })
    }
}
