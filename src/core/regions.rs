use regex::Regex;

use crate::core::config::{AppConfig, RegionConfig, RegionFiltersConfig, RegionProviderConfig};
use crate::core::errors::RegionError;
use crate::core::models::{AssetUpdateRequest, UrlPreview};

pub fn select_region<'a>(
    config: &'a AppConfig,
    name: &str,
) -> Result<&'a RegionConfig, RegionError> {
    let region = config
        .regions
        .get(name)
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

            let asset_info_url = match (&request.asset_version, &request.asset_hash, &profile_hash)
            {
                (Some(asset_version), Some(asset_hash), Some(profile_hash)) => Some(
                    asset_info_url_template
                        .replace("{env}", profile)
                        .replace("{hash}", profile_hash)
                        .replace("{asset_version}", asset_version)
                        .replace("{asset_hash}", asset_hash),
                ),
                _ => {
                    notes.push("asset info URL preview is incomplete until asset_version, asset_hash, and profile hash are known".to_string());
                    None
                }
            };

            let asset_bundle_url_template = asset_bundle_url_template
                .replace("{env}", profile)
                .replace(
                    "{hash}",
                    profile_hash.as_deref().unwrap_or("<profile-hash>"),
                )
                .replace(
                    "{asset_version}",
                    request
                        .asset_version
                        .as_deref()
                        .unwrap_or("<asset-version>"),
                )
                .replace(
                    "{asset_hash}",
                    request.asset_hash.as_deref().unwrap_or("<asset-hash>"),
                );

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

            if request.asset_version.is_none() {
                notes.push(
                    "asset_version is resolved at runtime from the provider lookup URL".to_string(),
                );
            }

            UrlPreview {
                provider_kind: "nuverse".to_string(),
                asset_info_url: Some(
                    asset_info_url_template
                        .replace("{app_version}", app_version)
                        .replace(
                            "{asset_version}",
                            request
                                .asset_version
                                .as_deref()
                                .unwrap_or("<resolved-asset-version>"),
                        ),
                ),
                asset_version_lookup_url: Some(
                    asset_version_url.replace("{app_version}", app_version),
                ),
                asset_bundle_url_template: asset_bundle_url_template
                    .replace("{app_version}", app_version)
                    .replace(
                        "{asset_version}",
                        request
                            .asset_version
                            .as_deref()
                            .unwrap_or("<resolved-asset-version>"),
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

    use crate::core::config::{RegionConfig, RegionFiltersConfig, RegionProviderConfig};
    use crate::core::models::AssetUpdateRequest;

    use super::{build_url_preview, download_priority, should_skip_bundle};

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
}
