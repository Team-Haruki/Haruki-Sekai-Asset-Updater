use regex::Regex;

use crate::core::config::{AppConfig, RegionConfig, RegionFiltersConfig, RegionProviderConfig};
use crate::core::errors::RegionError;
use crate::core::models::{AssetUpdateRequest, UrlPreview};
use sekai_asset_client::ProviderEndpoint;
use sekai_asset_pipeline::ResolvedRelease;

pub fn select_region<'a>(
    config: &'a AppConfig,
    name: &str,
) -> Result<&'a RegionConfig, RegionError> {
    // Configured region keys are forced lowercase by AppConfig::validate(); normalize the
    // requested name so callers can send "JP"/"Jp" without hitting a spurious NotFound.
    let region = config
        .regions
        .get(&name.to_ascii_lowercase())
        .ok_or_else(|| RegionError::NotFound(name.to_string()))?;
    if !region.enabled {
        return Err(RegionError::Disabled(name.to_string()));
    }
    Ok(region)
}

pub fn build_url_preview(region: &RegionConfig, request: &AssetUpdateRequest) -> UrlPreview {
    match &region.provider {
        RegionProviderConfig::ColorfulPalette {
            asset_info_url_template,
            asset_bundle_url_template,
            profile,
            profile_hashes,
            required_cookies,
            ..
        } => {
            let mut notes = Vec::new();
            if *required_cookies {
                notes
                    .push("region requires runtime cookies before downloads can start".to_string());
            }

            let profile_hash = profile_hashes.get(profile).cloned();
            if profile_hash.is_none() {
                notes.push(format!("profile hash for `{profile}` is missing"));
            }

            let preview_release = ResolvedRelease {
                asset_version: request
                    .asset_version
                    .clone()
                    .unwrap_or_else(|| "<asset-version>".to_string()),
                asset_hash: request
                    .asset_hash
                    .clone()
                    .unwrap_or_else(|| "<asset-hash>".to_string()),
            };
            let endpoint = ProviderEndpoint::ColorfulPalette {
                asset_info_url_template: asset_info_url_template.clone(),
                asset_bundle_url_template: asset_bundle_url_template.clone(),
                profile: profile.clone(),
                profile_hash: profile_hash
                    .clone()
                    .unwrap_or_else(|| "<profile-hash>".to_string()),
            };
            let asset_info_url = match (&request.asset_version, &request.asset_hash, &profile_hash)
            {
                (Some(_), Some(_), Some(_)) => {
                    Some(endpoint.render_asset_info_url(&preview_release, ""))
                }
                _ => {
                    notes.push("asset info URL preview is incomplete until asset_version, asset_hash, and profile hash are known".to_string());
                    None
                }
            };
            let asset_bundle_url_template =
                endpoint.render_bundle_url(&preview_release, "{bundle_path}", "");

            UrlPreview {
                provider_kind: "colorful_palette".to_string(),
                asset_info_url,
                asset_version_lookup_url: None,
                asset_bundle_url_template,
                notes,
            }
        }
        RegionProviderConfig::Nuverse {
            asset_version_url,
            app_version,
            asset_info_url_template,
            asset_bundle_url_template,
            required_cookies,
            ..
        } => {
            let mut notes = Vec::new();
            if *required_cookies {
                notes
                    .push("region requires runtime cookies before downloads can start".to_string());
            }
            // For nuverse, asset_version is ALWAYS fetched from asset_version_url at runtime;
            // any asset_version provided in the request is ignored.
            notes.push(
                "asset_version is always resolved at runtime from the provider lookup URL"
                    .to_string(),
            );

            let endpoint = ProviderEndpoint::Nuverse {
                asset_version_url_template: asset_version_url.clone(),
                asset_info_url_template: asset_info_url_template.clone(),
                asset_bundle_url_template: asset_bundle_url_template.clone(),
                app_version: app_version.clone(),
            };
            let preview_release = ResolvedRelease {
                asset_version: "<resolved-at-runtime>".to_string(),
                asset_hash: String::new(),
            };
            UrlPreview {
                provider_kind: "nuverse".to_string(),
                asset_info_url: Some(endpoint.render_asset_info_url(&preview_release, "")),
                asset_version_lookup_url: endpoint.render_release_url(),
                asset_bundle_url_template: endpoint.render_bundle_url(
                    &preview_release,
                    "{bundle_path}",
                    "",
                ),
                notes,
            }
        }
    }
}

pub fn should_skip_bundle(filters: &RegionFiltersConfig, bundle_name: &str) -> bool {
    let skip_patterns = compile_patterns(&filters.skip);
    should_skip_bundle_compiled(&skip_patterns, bundle_name)
}

pub fn download_priority(filters: &RegionFiltersConfig, bundle_name: &str) -> Option<usize> {
    let priority_patterns = compile_patterns(&filters.priority);
    download_priority_compiled(&priority_patterns, bundle_name)
}

pub(crate) fn compile_patterns(patterns: &[String]) -> Vec<Regex> {
    patterns
        .iter()
        .filter_map(|pattern| Regex::new(pattern).ok())
        .collect()
}

pub(crate) fn matches_any(patterns: &[Regex], bundle_name: &str) -> bool {
    patterns.iter().any(|regex| regex.is_match(bundle_name))
}

pub(crate) fn first_match_index(patterns: &[Regex], bundle_name: &str) -> Option<usize> {
    patterns
        .iter()
        .enumerate()
        .find_map(|(idx, regex)| regex.is_match(bundle_name).then_some(idx))
}

pub fn should_skip_bundle_compiled(skip_patterns: &[Regex], bundle_name: &str) -> bool {
    matches_any(skip_patterns, bundle_name)
}

pub fn download_priority_compiled(priority_patterns: &[Regex], bundle_name: &str) -> Option<usize> {
    first_match_index(priority_patterns, bundle_name)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::core::config::{AppConfig, RegionConfig, RegionFiltersConfig, RegionProviderConfig};
    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::{build_url_preview, download_priority, select_region, should_skip_bundle};

    fn empty_request() -> AssetUpdateRequest {
        AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: true,
            mode: AssetUpdateMode::Update,
        }
    }

    #[test]
    fn colorful_palette_preview_uses_profile_hash_and_request_values() {
        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc123".to_string());

        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: "https://info/{env}/{hash}/{asset_version}/{asset_hash}"
                    .to_string(),
                asset_bundle_url_template:
                    "https://bundle/{env}/{hash}/{asset_version}/{asset_hash}/{bundle_path}"
                        .to_string(),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: true,
                cookie_bootstrap_url: None,
            },
            ..RegionConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("6.0.0".to_string()),
            asset_hash: Some("deadbeef".to_string()),
            dry_run: true,
            mode: Default::default(),
        };

        let preview = build_url_preview(&region, &request);
        assert_eq!(
            preview.asset_info_url.as_deref(),
            Some("https://info/production/abc123/6.0.0/deadbeef")
        );
        assert!(preview
            .asset_bundle_url_template
            .contains("https://bundle/production/abc123/6.0.0/deadbeef/{bundle_path}"));
        assert!(!preview.notes.is_empty());
    }

    #[test]
    fn priority_and_skip_filters_follow_first_regex_match() {
        let filters = RegionFiltersConfig {
            skip: vec![r"^ignore/.*".to_string()],
            priority: vec![r"^music/.*".to_string(), r"^event/.*".to_string()],
            ..RegionFiltersConfig::default()
        };

        assert!(should_skip_bundle(&filters, "ignore/file"));
        assert_eq!(download_priority(&filters, "music/long/test"), Some(0));
        assert_eq!(download_priority(&filters, "event/live"), Some(1));
        assert_eq!(download_priority(&filters, "other/path"), None);
    }

    #[test]
    fn region_selection_and_incomplete_colorful_preview_cover_error_paths() {
        let mut config = AppConfig::default();
        config.regions.insert(
            "jp".to_string(),
            RegionConfig {
                enabled: true,
                ..RegionConfig::default()
            },
        );
        assert!(select_region(&config, "JP").is_ok());
        config.regions.get_mut("jp").unwrap().enabled = false;
        assert!(select_region(&config, "jp")
            .unwrap_err()
            .to_string()
            .contains("disabled"));
        assert!(select_region(&config, "en")
            .unwrap_err()
            .to_string()
            .contains("not found"));

        let region = RegionConfig {
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: "https://info/{hash}/{asset_version}/{asset_hash}".into(),
                asset_bundle_url_template: "https://bundle/{bundle_path}".into(),
                profile: "missing".into(),
                profile_hashes: BTreeMap::new(),
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            ..RegionConfig::default()
        };
        let preview = build_url_preview(&region, &empty_request());
        assert!(preview.asset_info_url.is_none());
        assert_eq!(preview.notes.len(), 2);
    }

    #[test]
    fn nuverse_preview_describes_runtime_release_resolution() {
        let region = RegionConfig {
            provider: RegionProviderConfig::Nuverse {
                asset_version_url: "https://version/{app_version}".into(),
                app_version: "4.2.0".into(),
                asset_info_url_template: "https://info/{asset_version}".into(),
                asset_bundle_url_template: "https://bundle/{asset_version}/{bundle_path}".into(),
                required_cookies: true,
                cookie_bootstrap_url: None,
            },
            ..RegionConfig::default()
        };
        let preview = build_url_preview(&region, &empty_request());
        assert_eq!(preview.provider_kind, "nuverse");
        assert_eq!(
            preview.asset_version_lookup_url.as_deref(),
            Some("https://version/4.2.0")
        );
        assert!(preview
            .asset_info_url
            .unwrap()
            .contains("resolved-at-runtime"));
        assert_eq!(preview.notes.len(), 2);
    }
}
