//! Fixtures and constants shared by the asset-execution test modules.
//!
//! These lived in the single test module the split dissolved. Keeping one
//! copy matters for the AES fixtures: two modules each with their own key
//! would drift apart silently.

use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};

use crate::core::config::{
    RegionConfig, RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
};

use sekai_asset_pipeline::AssetBundleInfo;

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

pub(super) fn text_asset_unity_fs_bundle(name: &str, payload: &[u8]) -> Vec<u8> {
    let mut object = Vec::new();
    push_aligned_bytes(&mut object, name.as_bytes());
    push_aligned_bytes(&mut object, payload);

    let mut metadata = Vec::new();
    metadata.extend_from_slice(b"2022.3.62f1\0");
    metadata.extend_from_slice(&13_i32.to_le_bytes());
    metadata.push(0);
    metadata.extend_from_slice(&1_i32.to_le_bytes());
    metadata.extend_from_slice(&49_i32.to_le_bytes());
    metadata.push(0);
    metadata.extend_from_slice(&(-1_i16).to_le_bytes());
    metadata.extend_from_slice(&[0; 16]);
    metadata.extend_from_slice(&1_i32.to_le_bytes());
    while !(48 + metadata.len()).is_multiple_of(4) {
        metadata.push(0);
    }
    metadata.extend_from_slice(&7_i64.to_le_bytes());
    metadata.extend_from_slice(&0_i64.to_le_bytes());
    metadata.extend_from_slice(&u32::try_from(object.len()).unwrap().to_le_bytes());
    metadata.extend_from_slice(&0_i32.to_le_bytes());
    for _ in 0..3 {
        metadata.extend_from_slice(&0_i32.to_le_bytes());
    }
    metadata.push(0);
    let data_offset = (48 + metadata.len()).next_multiple_of(16);
    let mut serialized = vec![0_u8; 48];
    serialized[8..12].copy_from_slice(&22_u32.to_be_bytes());
    serialized[20..24].copy_from_slice(&u32::try_from(metadata.len()).unwrap().to_be_bytes());
    serialized[24..32].copy_from_slice(
        &i64::try_from(data_offset + object.len())
            .unwrap()
            .to_be_bytes(),
    );
    serialized[32..40].copy_from_slice(&i64::try_from(data_offset).unwrap().to_be_bytes());
    serialized.extend_from_slice(&metadata);
    serialized.resize(data_offset, 0);
    serialized.extend_from_slice(&object);

    let mut blocks_info = vec![0_u8; 16];
    blocks_info.extend_from_slice(&1_i32.to_be_bytes());
    blocks_info.extend_from_slice(&u32::try_from(serialized.len()).unwrap().to_be_bytes());
    blocks_info.extend_from_slice(&u32::try_from(serialized.len()).unwrap().to_be_bytes());
    blocks_info.extend_from_slice(&0_u16.to_be_bytes());
    blocks_info.extend_from_slice(&1_i32.to_be_bytes());
    blocks_info.extend_from_slice(&0_i64.to_be_bytes());
    blocks_info.extend_from_slice(&i64::try_from(serialized.len()).unwrap().to_be_bytes());
    blocks_info.extend_from_slice(&4_u32.to_be_bytes());
    blocks_info.extend_from_slice(b"asset.assets\0");

    let mut output = Vec::new();
    output.extend_from_slice(b"UnityFS\0");
    output.extend_from_slice(&6_u32.to_be_bytes());
    output.extend_from_slice(b"5.x.x\0");
    output.extend_from_slice(b"2022.3.62f1\0");
    let size_offset = output.len();
    output.extend_from_slice(&0_i64.to_be_bytes());
    output.extend_from_slice(&u32::try_from(blocks_info.len()).unwrap().to_be_bytes());
    output.extend_from_slice(&u32::try_from(blocks_info.len()).unwrap().to_be_bytes());
    output.extend_from_slice(&0x40_u32.to_be_bytes());
    output.extend_from_slice(&blocks_info);
    output.extend_from_slice(&serialized);
    let output_size = i64::try_from(output.len()).unwrap();
    output[size_offset..size_offset + 8].copy_from_slice(&output_size.to_be_bytes());
    output
}

fn push_aligned_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_le_bytes());
    output.extend_from_slice(bytes);
    while !output.len().is_multiple_of(4) {
        output.push(0);
    }
}
