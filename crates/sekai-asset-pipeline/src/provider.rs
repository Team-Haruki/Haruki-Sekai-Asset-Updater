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

/// Trusted provider templates after application configuration has been resolved.
///
/// Cookies and credentials deliberately do not live here. A planner or worker
/// obtains those at runtime and keeps them out of serialized bundle messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEndpoint {
    ColorfulPalette {
        asset_info_url_template: String,
        asset_bundle_url_template: String,
        profile: String,
        profile_hash: String,
    },
    Nuverse {
        asset_info_url_template: String,
        asset_bundle_url_template: String,
        app_version: String,
    },
}

impl ProviderEndpoint {
    pub const fn kind(&self) -> ProviderKind {
        match self {
            Self::ColorfulPalette { .. } => ProviderKind::ColorfulPalette,
            Self::Nuverse { .. } => ProviderKind::Nuverse,
        }
    }

    pub fn render_asset_info_url(&self, release: &ResolvedRelease, cache_buster: &str) -> String {
        let rendered = match self {
            Self::ColorfulPalette {
                asset_info_url_template,
                profile,
                profile_hash,
                ..
            } => asset_info_url_template
                .replace("{env}", profile)
                .replace("{hash}", profile_hash)
                .replace("{asset_version}", &release.asset_version)
                .replace("{asset_hash}", &release.asset_hash),
            Self::Nuverse {
                asset_info_url_template,
                app_version,
                ..
            } => asset_info_url_template
                .replace("{app_version}", app_version)
                .replace("{asset_version}", &release.asset_version),
        };
        rendered + cache_buster
    }

    pub fn render_bundle_url(
        &self,
        release: &ResolvedRelease,
        download_path: &str,
        cache_buster: &str,
    ) -> String {
        let rendered = match self {
            Self::ColorfulPalette {
                asset_bundle_url_template,
                profile,
                profile_hash,
                ..
            } => asset_bundle_url_template
                .replace("{bundle_path}", download_path)
                .replace("{asset_version}", &release.asset_version)
                .replace("{asset_hash}", &release.asset_hash)
                .replace("{env}", profile)
                .replace("{hash}", profile_hash),
            Self::Nuverse {
                asset_bundle_url_template,
                app_version,
                ..
            } => asset_bundle_url_template
                .replace("{bundle_path}", download_path)
                .replace("{app_version}", app_version)
                .replace("{asset_version}", &release.asset_version),
        };
        rendered + cache_buster
    }
}

#[cfg(test)]
mod tests {
    use super::{ProviderEndpoint, ProviderKind, ResolvedRelease};

    #[test]
    fn colorful_palette_urls_use_the_pinned_release_and_profile() {
        let endpoint = ProviderEndpoint::ColorfulPalette {
            asset_info_url_template: "https://info/{env}/{hash}/{asset_version}/{asset_hash}"
                .to_string(),
            asset_bundle_url_template:
                "https://bundle/{env}/{hash}/{asset_version}/{asset_hash}/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hash: "profile-hash".to_string(),
        };
        let release = ResolvedRelease {
            asset_version: "42".to_string(),
            asset_hash: "asset-hash".to_string(),
        };

        assert_eq!(
            endpoint.render_asset_info_url(&release, "?t=1"),
            "https://info/production/profile-hash/42/asset-hash?t=1"
        );
        assert_eq!(
            endpoint.render_bundle_url(&release, "music/a", "?t=2"),
            "https://bundle/production/profile-hash/42/asset-hash/music/a?t=2"
        );
    }

    #[test]
    fn nuverse_urls_and_download_paths_use_manifest_metadata() {
        let endpoint = ProviderEndpoint::Nuverse {
            asset_info_url_template: "https://info/{app_version}/{asset_version}".to_string(),
            asset_bundle_url_template: "https://bundle/{app_version}/{asset_version}/{bundle_path}"
                .to_string(),
            app_version: "3.9.0".to_string(),
        };
        let release = ResolvedRelease {
            asset_version: "39".to_string(),
            asset_hash: String::new(),
        };
        let path = ProviderKind::Nuverse.download_path("music/a", Some("startapp"));

        assert_eq!(path, "startapp/music/a");
        assert_eq!(
            endpoint.render_bundle_url(&release, &path, "?t=3"),
            "https://bundle/3.9.0/39/startapp/music/a?t=3"
        );
        assert_eq!(
            ProviderKind::ColorfulPalette.download_path("music/a", Some("ignored")),
            "music/a"
        );
    }
}
