use serde::{Deserialize, Serialize};

/// The two provider families used by Sekai regions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    ColorfulPalette,
    Nuverse,
}

impl ProviderKind {
    /// Resolves the provider-specific relative path stored in a worker message.
    pub fn download_path(self, bundle_path: &str, manifest_prefix: Option<&str>) -> String {
        match self {
            Self::ColorfulPalette => bundle_path.to_string(),
            Self::Nuverse => manifest_prefix
                .map(|prefix| format!("{prefix}/{bundle_path}"))
                .unwrap_or_else(|| bundle_path.to_string()),
        }
    }
}

/// The immutable game release to which a manifest and its bundle messages are pinned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedRelease {
    pub asset_version: String,
    /// Empty for providers whose URLs do not use an asset hash.
    pub asset_hash: String,
}

#[cfg(test)]
mod tests {
    use super::ProviderKind;

    #[test]
    fn provider_kind_resolves_manifest_download_prefixes() {
        let path = ProviderKind::Nuverse.download_path("music/a", Some("startapp"));

        assert_eq!(path, "startapp/music/a");
        assert_eq!(
            ProviderKind::ColorfulPalette.download_path("music/a", Some("ignored")),
            "music/a"
        );
    }
}
