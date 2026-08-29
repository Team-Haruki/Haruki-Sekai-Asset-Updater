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
