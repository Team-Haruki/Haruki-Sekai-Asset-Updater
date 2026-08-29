//! Compatibility facade for reusable bundle cryptography.

use super::model::AssetBundleInfo;
use crate::core::errors::AssetExecutionError;

pub(super) use sekai_asset_pipeline::deobfuscate_owned;

pub fn deobfuscate(data: &[u8]) -> Vec<u8> {
    sekai_asset_pipeline::deobfuscate(data)
}

pub fn decrypt_asset_bundle_info(
    aes_key_hex: &str,
    aes_iv_hex: &str,
    content: &[u8],
) -> Result<AssetBundleInfo, AssetExecutionError> {
    sekai_asset_pipeline::decrypt_asset_bundle_info(aes_key_hex, aes_iv_hex, content)
        .map_err(AssetExecutionError::from)
}
