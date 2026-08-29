use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBundleInfo {
    #[serde(rename = "version")]
    pub version: Option<String>,
    #[serde(rename = "os")]
    pub os: Option<String>,
    #[serde(rename = "bundles")]
    pub bundles: HashMap<String, AssetBundleDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBundleDetail {
    #[serde(rename = "bundleName", deserialize_with = "de_null_as_empty_string")]
    pub bundle_name: String,
    #[serde(rename = "cacheFileName", deserialize_with = "de_null_as_empty_string")]
    pub cache_file_name: String,
    #[serde(
        rename = "cacheDirectoryName",
        deserialize_with = "de_null_as_empty_string"
    )]
    pub cache_directory_name: String,
    // Nuverse regions use `crc` instead of `hash`; the server may send nil here.
    #[serde(rename = "hash", deserialize_with = "de_null_as_empty_string")]
    pub hash: String,
    #[serde(rename = "category")]
    pub category: AssetCategory,
    #[serde(rename = "crc")]
    pub crc: i64,
    #[serde(rename = "fileSize")]
    pub file_size: i64,
    #[serde(rename = "dependencies")]
    pub dependencies: Vec<String>,
    #[serde(rename = "paths", default)]
    pub paths: Vec<String>,
    #[serde(rename = "isBuiltin")]
    pub is_builtin: bool,
    #[serde(rename = "isRelocate")]
    pub is_relocate: Option<bool>,
    #[serde(rename = "md5Hash")]
    pub md5_hash: Option<String>,
    #[serde(rename = "downloadPath")]
    pub download_path: Option<String>,
}

/// Deserializes a MessagePack/JSON null or missing value as an empty string.
/// This matches the zero-value coercion used by the original Go service.
fn de_null_as_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum AssetCategory {
    StartApp,
    OnDemand,
    LivePv,
    Other(String),
}

impl<'de> Deserialize<'de> for AssetCategory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
        Ok(match raw.as_str() {
            "StartApp" | "startApp" => Self::StartApp,
            "OnDemand" | "onDemand" => Self::OnDemand,
            "Live_pv" | "live_pv" | "LivePv" | "livePv" => Self::LivePv,
            other => Self::Other(other.to_string()),
        })
    }
}

pub fn asset_category_name(category: &AssetCategory) -> &'static str {
    match category {
        AssetCategory::StartApp => "StartApp",
        AssetCategory::OnDemand | AssetCategory::LivePv | AssetCategory::Other(_) => "OnDemand",
    }
}
