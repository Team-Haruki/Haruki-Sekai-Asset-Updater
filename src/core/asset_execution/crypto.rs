//! Bundle deobfuscation.

use cbc::cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};

use super::model::AssetBundleInfo;
use crate::core::errors::AssetExecutionError;

pub(super) fn deobfuscate_owned(mut data: Vec<u8>) -> Vec<u8> {
    const SIMPLE: [u8; 4] = [0x20, 0x00, 0x00, 0x00];
    const XOR_HEADER: [u8; 4] = [0x10, 0x00, 0x00, 0x00];

    if data.starts_with(&SIMPLE) {
        data.copy_within(4.., 0);
        data.truncate(data.len() - 4);
        return data;
    }

    if data.starts_with(&XOR_HEADER) {
        data.copy_within(4.., 0);
        data.truncate(data.len() - 4);
        if data.len() < 128 {
            return data;
        }
        let pattern = [0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00];
        for idx in 0..128 {
            data[idx] ^= pattern[idx % pattern.len()];
        }
        return data;
    }

    data
}

pub fn deobfuscate(data: &[u8]) -> Vec<u8> {
    deobfuscate_owned(data.to_vec())
}

pub fn decrypt_asset_bundle_info(
    aes_key_hex: &str,
    aes_iv_hex: &str,
    content: &[u8],
) -> Result<AssetBundleInfo, AssetExecutionError> {
    if content.is_empty() {
        return Err(AssetExecutionError::EmptyEncryptedContent);
    }
    if !content.len().is_multiple_of(16) {
        return Err(AssetExecutionError::InvalidEncryptedBlockSize);
    }

    let key = hex::decode(aes_key_hex)
        .map_err(|err| AssetExecutionError::InvalidAesKeyHex(err.to_string()))?;
    let iv = hex::decode(aes_iv_hex)
        .map_err(|err| AssetExecutionError::InvalidAesIvHex(err.to_string()))?;
    if iv.len() != 16 {
        return Err(AssetExecutionError::InvalidAesIvLength { got: iv.len() });
    }

    let mut buf = content.to_vec();
    let decrypted = match key.len() {
        16 => Aes128CbcDec::new_from_slices(&key, &iv)
            .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?
            .decrypt_padded::<Pkcs7>(&mut buf)
            .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?,
        24 => Aes192CbcDec::new_from_slices(&key, &iv)
            .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?
            .decrypt_padded::<Pkcs7>(&mut buf)
            .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?,
        32 => Aes256CbcDec::new_from_slices(&key, &iv)
            .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?
            .decrypt_padded::<Pkcs7>(&mut buf)
            .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?,
        _ => {
            return Err(AssetExecutionError::AssetInfoDecode(format!(
                "unsupported AES key length {}",
                key.len()
            )))
        }
    };

    rmp_serde::from_slice::<AssetBundleInfo>(decrypted)
        .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))
}

pub(super) type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

pub(super) type Aes192CbcDec = cbc::Decryptor<aes::Aes192>;

pub(super) type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::crypto::{decrypt_asset_bundle_info, deobfuscate, deobfuscate_owned};

    use super::super::model::{AssetBundleDetail, AssetBundleInfo, AssetCategory};

    use super::super::test_support::{encrypt_asset_info, TEST_AES_IV_HEX, TEST_AES_KEY_HEX};

    #[test]
    fn decrypt_asset_info_round_trips_msgpack_payload() {
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash".to_string(),
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
        let decrypted =
            decrypt_asset_bundle_info(TEST_AES_KEY_HEX, TEST_AES_IV_HEX, &encrypted).unwrap();
        assert_eq!(decrypted.version.as_deref(), Some("1"));
        assert!(decrypted.bundles.contains_key("start/a"));
    }

    #[test]
    fn deobfuscate_matches_go_headers() {
        assert_eq!(
            deobfuscate(&[0x20, 0x00, 0x00, 0x00, 1, 2, 3]),
            vec![1, 2, 3]
        );
        assert_eq!(deobfuscate(&[9, 8, 7]), vec![9, 8, 7]);
    }

    #[test]
    fn deobfuscate_owned_reuses_the_input_allocation() {
        let simple = vec![0x20, 0x00, 0x00, 0x00, 1, 2, 3];
        let simple_pointer = simple.as_ptr();
        let simple_capacity = simple.capacity();
        let simple = deobfuscate_owned(simple);
        assert_eq!(simple, vec![1, 2, 3]);
        assert_eq!(simple.as_ptr(), simple_pointer);
        assert_eq!(simple.capacity(), simple_capacity);

        let pattern = [0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00];
        let decoded = (0..132).map(|index| index as u8).collect::<Vec<_>>();
        let mut xor = vec![0x10, 0x00, 0x00, 0x00];
        xor.extend(decoded.iter().enumerate().map(|(index, byte)| {
            if index < 128 {
                byte ^ pattern[index % pattern.len()]
            } else {
                *byte
            }
        }));
        let xor_pointer = xor.as_ptr();
        let xor = deobfuscate_owned(xor);
        assert_eq!(xor, decoded);
        assert_eq!(xor.as_ptr(), xor_pointer);
    }
}
