//! Fixtures and constants shared by the asset-execution test modules.
//!
//! These lived in the single test module the split dissolved. Keeping one
//! copy matters for the AES fixtures: two modules each with their own key
//! would drift apart silently.

use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};

use crate::core::config::{
    RegionConfig, RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
};

use super::model::AssetBundleInfo;

pub(super) type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
pub(super) const TEST_AES_KEY_HEX: &str = "00112233445566778899aabbccddeeff";
pub(super) const TEST_AES_IV_HEX: &str = "0102030405060708090a0b0c0d0e0f10";

pub(super) fn test_region(provider: RegionProviderConfig) -> RegionConfig {
    RegionConfig {
        enabled: true,
        provider,
        crypto: crate::core::config::CryptoConfig {
            aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
            aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
        },
        runtime: RegionRuntimeConfig {
            unity_version: "2022.3.21f1".to_string(),
        },
        paths: RegionPathsConfig {
            asset_save_dir: Some("./Data/jp-assets".to_string()),
            downloaded_asset_record_file: Some(
                "./Data/jp-assets/downloaded_assets.json".to_string(),
            ),
        },
        filters: crate::core::config::RegionFiltersConfig {
            start_app: vec!["^start/".to_string()],
            on_demand: vec!["^ond/".to_string(), "^live_pv/model/".to_string()],
            skip: vec!["^skip/".to_string()],
            priority: vec!["^start/a".to_string(), "^ond/".to_string()],
        },
        ..RegionConfig::default()
    }
}

// Needed by more than one group of tests: the provider tests serve an
// encrypted asset-info document, and the download tests fetch one.
pub(super) fn encrypt_asset_info(info: &AssetBundleInfo) -> Vec<u8> {
    let key = hex::decode(TEST_AES_KEY_HEX).unwrap();
    let iv = hex::decode(TEST_AES_IV_HEX).unwrap();
    let payload = rmp_serde::to_vec_named(info).unwrap();
    let mut padded = payload.clone();
    let original_len = padded.len();
    let padding = 16 - (original_len % 16);
    padded.resize(original_len + padding, 0);
    let encrypted = Aes128CbcEnc::new_from_slices(&key, &iv)
        .unwrap()
        .encrypt_padded::<Pkcs7>(&mut padded, original_len)
        .unwrap()
        .to_vec();
    encrypted
}
