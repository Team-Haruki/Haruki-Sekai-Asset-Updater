use chrono::FixedOffset;
use sekai_asset_pipeline::{ProviderKind, ResolvedRelease};

/// Trusted URL templates after application configuration has been resolved.
///
/// Cookies and credentials deliberately do not live here. They are runtime
/// client state and must not leak into serialized worker messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEndpoint {
    ColorfulPalette {
        asset_info_url_template: String,
        asset_bundle_url_template: String,
        profile: String,
        profile_hash: String,
    },
    Nuverse {
        asset_version_url_template: String,
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

    pub fn render_release_url(&self) -> Option<String> {
        match self {
            Self::ColorfulPalette { .. } => None,
            Self::Nuverse {
                asset_version_url_template,
                app_version,
                ..
            } => Some(asset_version_url_template.replace("{app_version}", app_version)),
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

pub(crate) fn cache_buster_jst() -> String {
    let timezone = FixedOffset::east_opt(9 * 3600).expect("JST is a valid fixed offset");
    format!(
        "?t={}",
        chrono::Utc::now()
            .with_timezone(&timezone)
            .format("%Y%m%d%H%M%S")
    )
}

#[cfg(test)]
mod tests {
    use super::ProviderEndpoint;
    use sekai_asset_pipeline::{ProviderKind, ResolvedRelease};

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
            asset_version_url_template: "https://version/{app_version}".to_string(),
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

        assert_eq!(
            endpoint.render_release_url().as_deref(),
            Some("https://version/3.9.0")
        );
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
