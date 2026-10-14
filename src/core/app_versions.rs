//! Runtime app and Unity versions per region.
//!
//! The YAML config gives every Nuverse region its starting
//! `provider.app_version` and every region its `runtime.unity_version`. This
//! module keeps runtime overrides of the two on top of it: set by the registry
//! poller, the HTTP API or an update request, persisted to
//! `app_version_sync.state_file`, and applied to a job's own copy of the
//! config when the job is created, so a running job never sees a change.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use regex::Regex;
use sekai_asset_client::{
    ClientConfig, HttpVersion, ProviderEndpoint, RequestedRelease, RetryOptions, SekaiAssetClient,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::core::config::{AppConfig, AssetHttpVersion, RegionConfig, RegionProviderConfig};

const STATE_FILE_VERSION: u32 = 1;
const MAX_UNITY_VERSION_LEN: usize = 32;

static UNITY_VERSION_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d+\.\d+\.\d+[a-z]+\d+[0-9a-z]*$").expect("unity version pattern compiles")
});

/// Who set an override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSource {
    /// The registry poller.
    Registry,
    /// `PUT /v2/regions/{region}/app-version`.
    Manual,
    /// The `app_version` of a `POST /v2/assets/update` request.
    UpdateRequest,
}

/// One overridden value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionOverride {
    pub value: String,
    pub source: VersionSource,
    pub updated_at: DateTime<Utc>,
    /// Set with `force`: skipped the CDN probe and the downgrade check, and
    /// survives a restart even when the YAML value is newer.
    #[serde(default)]
    pub forced: bool,
}

impl VersionOverride {
    pub fn new(value: &str, source: VersionSource, forced: bool) -> Self {
        Self {
            value: value.to_string(),
            source,
            updated_at: Utc::now(),
            forced,
        }
    }
}

/// The overrides of one region. A missing field means "use the YAML value".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionVersionOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_version: Option<VersionOverride>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unity_version: Option<VersionOverride>,
}

impl RegionVersionOverrides {
    pub fn is_empty(&self) -> bool {
        self.app_version.is_none() && self.unity_version.is_none()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StateFile {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    regions: BTreeMap<String, RegionVersionOverrides>,
}

#[derive(Debug, Error)]
pub enum RuntimeVersionError {
    #[error("failed to persist runtime versions to {path}: {message}")]
    Persist { path: String, message: String },
}

/// The YAML app version of a region; `None` for non-Nuverse providers, which
/// have no app version in their URLs.
pub fn configured_app_version(region: &RegionConfig) -> Option<&str> {
    match &region.provider {
        RegionProviderConfig::Nuverse { app_version, .. } => Some(app_version.as_str()),
        RegionProviderConfig::ColorfulPalette { .. } => None,
    }
}

/// The app version a job created now would use.
pub fn effective_app_version(
    region: &RegionConfig,
    overrides: &RegionVersionOverrides,
) -> Option<String> {
    let configured = configured_app_version(region)?;
    Some(
        overrides
            .app_version
            .as_ref()
            .map_or_else(|| configured.to_string(), |entry| entry.value.clone()),
    )
}

/// The Unity version a job created now would use.
pub fn effective_unity_version(
    region: &RegionConfig,
    overrides: &RegionVersionOverrides,
) -> String {
    overrides.unity_version.as_ref().map_or_else(
        || region.runtime.unity_version.clone(),
        |entry| entry.value.clone(),
    )
}

/// Parses `x.y.z` or `x.y.z.w` (numeric components only).
pub fn parse_app_version(value: &str) -> Option<[u64; 4]> {
    let parts: Vec<&str> = value.trim().split('.').collect();
    if !(3..=4).contains(&parts.len()) {
        return None;
    }
    let mut parsed = [0_u64; 4];
    for (slot, part) in parsed.iter_mut().zip(&parts) {
        if part.is_empty() || part.len() > 9 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        *slot = part.parse().ok()?;
    }
    Some(parsed)
}

/// Compares two app versions; a missing fourth component counts as `0`.
/// `None` when either side does not parse.
pub fn compare_app_versions(left: &str, right: &str) -> Option<Ordering> {
    Some(parse_app_version(left)?.cmp(&parse_app_version(right)?))
}

/// Whether `value` looks like a Unity version (`2022.3.21f1`, `2020.3.48f1c1`).
pub fn is_valid_unity_version(value: &str) -> bool {
    value.len() <= MAX_UNITY_VERSION_LEN && UNITY_VERSION_PATTERN.is_match(value)
}

/// Runtime overrides, shared by the HTTP API, the poller and the job manager.
#[derive(Clone, Default)]
pub struct RuntimeVersions {
    path: Option<PathBuf>,
    regions: Arc<RwLock<BTreeMap<String, RegionVersionOverrides>>>,
}

impl RuntimeVersions {
    /// Overrides kept in memory only (tests and tools).
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Loads `app_version_sync.state_file`. A missing file is an empty state;
    /// an unreadable one is logged and also treated as empty, so a damaged
    /// file can never keep the service from starting.
    pub fn load(config: &AppConfig) -> Self {
        let path = PathBuf::from(&config.app_version_sync.state_file);
        let mut regions = read_state_file(&path);
        prune_stale_overrides(config, &mut regions);
        Self {
            path: Some(path),
            regions: Arc::new(RwLock::new(regions)),
        }
    }

    pub async fn overrides(&self, region: &str) -> RegionVersionOverrides {
        self.regions
            .read()
            .await
            .get(&region.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    /// The config a job for `region` runs with: `config` itself when the
    /// region has no override, otherwise a copy with the region's app and
    /// Unity versions replaced.
    pub async fn apply(&self, config: &Arc<AppConfig>, region: &str) -> Arc<AppConfig> {
        let key = region.to_ascii_lowercase();
        let overrides = self.overrides(&key).await;
        if overrides.is_empty() || !config.regions.contains_key(&key) {
            return config.clone();
        }
        let mut effective = AppConfig::clone(config);
        if let Some(region_config) = effective.regions.get_mut(&key) {
            apply_overrides(region_config, &overrides);
        }
        Arc::new(effective)
    }

    /// Runs `change` on the region's overrides and persists the result when it
    /// changed anything. The file is written before the in-memory state is
    /// replaced, so a failed write leaves both untouched.
    pub async fn modify<R>(
        &self,
        region: &str,
        change: impl FnOnce(&mut RegionVersionOverrides) -> R,
    ) -> Result<R, RuntimeVersionError> {
        let key = region.to_ascii_lowercase();
        let mut regions = self.regions.write().await;
        let before = regions.get(&key).cloned().unwrap_or_default();
        let mut after = before.clone();
        let result = change(&mut after);
        if after == before {
            return Ok(result);
        }
        let mut next = regions.clone();
        if after.is_empty() {
            next.remove(&key);
        } else {
            next.insert(key, after);
        }
        if let Some(path) = &self.path {
            write_state_file(path, &next).await?;
        }
        *regions = next;
        Ok(result)
    }
}

fn apply_overrides(region: &mut RegionConfig, overrides: &RegionVersionOverrides) {
    if let (Some(entry), RegionProviderConfig::Nuverse { app_version, .. }) =
        (&overrides.app_version, &mut region.provider)
    {
        *app_version = entry.value.clone();
    }
    if let Some(entry) = &overrides.unity_version {
        region.runtime.unity_version = entry.value.clone();
    }
}

fn read_state_file(path: &Path) -> BTreeMap<String, RegionVersionOverrides> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return BTreeMap::new(),
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot read runtime versions; starting empty");
            return BTreeMap::new();
        }
    };
    match sonic_rs::from_slice::<StateFile>(&bytes) {
        Ok(state) => state
            .regions
            .into_iter()
            .map(|(name, overrides)| (name.to_ascii_lowercase(), overrides))
            .collect(),
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot parse runtime versions; starting empty");
            BTreeMap::new()
        }
    }
}

/// Drops app-version overrides the YAML has caught up with (an operator
/// raised `provider.app_version` and restarted) unless they were forced, and
/// app-version overrides of regions that are not Nuverse.
fn prune_stale_overrides(
    config: &AppConfig,
    regions: &mut BTreeMap<String, RegionVersionOverrides>,
) {
    for (name, overrides) in regions.iter_mut() {
        let Some(entry) = &overrides.app_version else {
            continue;
        };
        let configured = config.regions.get(name).and_then(configured_app_version);
        let stale = match configured {
            None => config.regions.contains_key(name),
            Some(configured) => {
                !entry.forced
                    && compare_app_versions(configured, &entry.value)
                        .is_some_and(|order| order != Ordering::Less)
            }
        };
        if stale {
            info!(
                region = %name,
                app_version = %entry.value,
                configured = ?configured,
                "dropping runtime app_version override superseded by the config"
            );
            overrides.app_version = None;
        }
    }
    regions.retain(|_, overrides| !overrides.is_empty());
}

async fn write_state_file(
    path: &Path,
    regions: &BTreeMap<String, RegionVersionOverrides>,
) -> Result<(), RuntimeVersionError> {
    let persist_error = |message: String| RuntimeVersionError::Persist {
        path: path.display().to_string(),
        message,
    };
    let state = StateFile {
        version: STATE_FILE_VERSION,
        regions: regions.clone(),
    };
    let body = sonic_rs::to_vec_pretty(&state).map_err(|err| persist_error(err.to_string()))?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| persist_error(err.to_string()))?;
    }
    let mut temp = path.as_os_str().to_owned();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    tokio::fs::write(&temp, body)
        .await
        .map_err(|err| persist_error(err.to_string()))?;
    tokio::fs::rename(&temp, path)
        .await
        .map_err(|err| persist_error(err.to_string()))
}

/// Asks the region's Nuverse version URL whether `app_version` exists: the
/// endpoint must answer 2xx with a numeric asset version. Returns that asset
/// version. One attempt, no retries; the caller retries on its own schedule.
pub async fn probe_app_version(
    config: &AppConfig,
    region: &RegionConfig,
    app_version: &str,
    unity_version: &str,
) -> Result<String, String> {
    let RegionProviderConfig::Nuverse {
        asset_version_url,
        asset_info_url_template,
        asset_bundle_url_template,
        required_cookies,
        cookie_bootstrap_url,
        ..
    } = &region.provider
    else {
        return Err("only nuverse regions have an app version".to_string());
    };
    let endpoint = ProviderEndpoint::Nuverse {
        asset_version_url_template: asset_version_url.clone(),
        asset_info_url_template: asset_info_url_template.clone(),
        asset_bundle_url_template: asset_bundle_url_template.clone(),
        app_version: app_version.to_string(),
    };
    let timeout = Duration::from_secs(config.app_version_sync.request_timeout_secs.max(1));
    let mut client_config = ClientConfig::new(endpoint, unity_version);
    client_config.proxy = config.server.proxy.clone();
    client_config.http_version = match config.server.asset_http_version {
        AssetHttpVersion::Auto => HttpVersion::Auto,
        AssetHttpVersion::Http1 => HttpVersion::Http1,
    };
    client_config.connect_timeout = timeout;
    client_config.request_timeout = timeout;
    client_config.retry = RetryOptions {
        attempts: 1,
        ..RetryOptions::default()
    };
    client_config.limits.max_manifest_bytes = 4096;
    let mut client = SekaiAssetClient::new(client_config).map_err(|err| err.to_string())?;
    if *required_cookies {
        client
            .bootstrap_cookie(cookie_bootstrap_url.as_deref())
            .await
            .map_err(|err| err.to_string())?;
    }
    let release = client
        .resolve_release(&RequestedRelease::default())
        .await
        .map_err(|err| err.to_string())?;
    let numeric = !release.asset_version.is_empty()
        && release.asset_version.bytes().all(|b| b.is_ascii_digit());
    if numeric {
        Ok(release.asset_version)
    } else {
        Err(format!(
            "version endpoint for app version {app_version} returned a non-numeric body"
        ))
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::http::{StatusCode, Uri};
    use axum::Router;

    use crate::core::config::{
        AppConfig, RegionConfig, RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    };

    /// A local HTTP server answering each configured path with its status
    /// and body, 404 for everything else. Answers can change while it runs.
    #[derive(Clone)]
    pub(crate) struct MockHttp {
        answers: Arc<Mutex<BTreeMap<String, (StatusCode, String)>>>,
        pub(crate) base_url: String,
    }

    impl MockHttp {
        pub(crate) async fn start() -> Self {
            let answers: Arc<Mutex<BTreeMap<String, (StatusCode, String)>>> = Arc::default();
            let lookup = answers.clone();
            let app = Router::new().fallback(move |uri: Uri| {
                let answer = lookup.lock().unwrap().get(uri.path()).cloned();
                async move { answer.unwrap_or((StatusCode::NOT_FOUND, String::new())) }
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                answers,
                base_url: format!("http://{address}"),
            }
        }

        pub(crate) fn set(&self, path: &str, status: StatusCode, body: &str) {
            self.answers
                .lock()
                .unwrap()
                .insert(path.to_string(), (status, body.to_string()));
        }
    }

    /// A version CDN answering `/version/{app_version}` with the given
    /// status and body per app version, 404 otherwise.
    pub(crate) async fn version_cdn(
        answers: Vec<(&'static str, StatusCode, &'static str)>,
    ) -> String {
        let server = MockHttp::start().await;
        for (version, status, body) in answers {
            server.set(&format!("/version/{version}"), status, body);
        }
        server.base_url
    }

    /// A Nuverse region whose URLs point at `base_url` and whose paths live in
    /// the system temp directory (jobs in tests never write there before
    /// failing on the missing manifest).
    pub(crate) fn nuverse_region(base_url: &str, app_version: &str) -> RegionConfig {
        let scratch = std::env::temp_dir().join("haruki-app-version-tests");
        RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::Nuverse {
                asset_version_url: format!("{base_url}/version/{{app_version}}"),
                app_version: app_version.to_string(),
                asset_info_url_template: format!(
                    "{base_url}/info/{{app_version}}/{{asset_version}}"
                ),
                asset_bundle_url_template: format!("{base_url}/bundle/{{bundle_path}}"),
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(scratch.join("assets").display().to_string()),
                downloaded_asset_record_file: Some(
                    scratch.join("downloaded_assets.json").display().to_string(),
                ),
            },
            ..RegionConfig::default()
        }
    }

    pub(crate) fn config_with(regions: Vec<(&str, RegionConfig)>, state_file: &str) -> AppConfig {
        let mut config = AppConfig::default();
        config.app_version_sync.state_file = state_file.to_string();
        config.app_version_sync.request_timeout_secs = 5;
        config.regions = regions
            .into_iter()
            .map(|(name, region)| (name.to_string(), region))
            .collect();
        config
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{config_with, nuverse_region, version_cdn};
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn app_versions_parse_and_compare_numerically() {
        assert_eq!(parse_app_version("6.4.0"), Some([6, 4, 0, 0]));
        assert_eq!(parse_app_version(" 6.4.0.12 "), Some([6, 4, 0, 12]));
        for invalid in [
            "",
            "6.4",
            "6.4.0.1.2",
            "6.x.0",
            "6..0",
            "6.4.0-beta",
            "1234567890.0.0",
        ] {
            assert_eq!(parse_app_version(invalid), None, "{invalid}");
        }
        assert_eq!(
            compare_app_versions("6.10.0", "6.9.9"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare_app_versions("6.4.0", "6.4.0.0"),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare_app_versions("6.4.0", "6.4.0.1"),
            Some(Ordering::Less)
        );
        assert_eq!(compare_app_versions("6.4.0", "latest"), None);
    }

    #[test]
    fn unity_versions_are_validated() {
        for valid in ["2022.3.21f1", "2020.3.48f1c1", "6000.0.1b2"] {
            assert!(is_valid_unity_version(valid), "{valid}");
        }
        for invalid in ["", "2022.3", "2022.3.21", "2022.3.21f1\n", "x2022.3.21f1"] {
            assert!(!is_valid_unity_version(invalid), "{invalid:?}");
        }
        assert!(!is_valid_unity_version(&format!(
            "2022.3.21f1{}",
            "a".repeat(40)
        )));
    }

    #[test]
    fn effective_values_prefer_overrides_and_ignore_app_version_on_colorful_palette() {
        let region = nuverse_region("http://cdn", "6.4.0");
        let mut overrides = RegionVersionOverrides::default();
        assert_eq!(
            effective_app_version(&region, &overrides).as_deref(),
            Some("6.4.0")
        );
        assert_eq!(effective_unity_version(&region, &overrides), "2022.3.21f1");
        overrides.app_version = Some(VersionOverride::new("6.5.0", VersionSource::Manual, false));
        overrides.unity_version = Some(VersionOverride::new(
            "2022.3.62f1",
            VersionSource::Manual,
            false,
        ));
        assert_eq!(
            effective_app_version(&region, &overrides).as_deref(),
            Some("6.5.0")
        );
        assert_eq!(effective_unity_version(&region, &overrides), "2022.3.62f1");
        assert_eq!(
            effective_app_version(&RegionConfig::default(), &overrides),
            None
        );
    }

    #[tokio::test]
    async fn state_round_trips_through_the_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = dir.path().join("nested/runtime-versions.json");
        let config = config_with(
            vec![("cn", nuverse_region("http://cdn", "6.4.0"))],
            state_file.to_str().unwrap(),
        );
        let versions = RuntimeVersions::load(&config);
        assert!(versions.overrides("cn").await.is_empty());

        versions
            .modify("CN", |overrides| {
                overrides.app_version = Some(VersionOverride::new(
                    "6.5.0",
                    VersionSource::Registry,
                    false,
                ));
                overrides.unity_version = Some(VersionOverride::new(
                    "2022.3.62f1",
                    VersionSource::Manual,
                    false,
                ));
            })
            .await
            .unwrap();
        assert!(state_file.exists());
        assert!(!state_file.with_extension("json.tmp").exists());

        let reloaded = RuntimeVersions::load(&config);
        let overrides = reloaded.overrides("cn").await;
        assert_eq!(overrides.app_version.as_ref().unwrap().value, "6.5.0");
        assert_eq!(
            overrides.app_version.as_ref().unwrap().source,
            VersionSource::Registry
        );
        assert_eq!(
            overrides.unity_version.as_ref().unwrap().value,
            "2022.3.62f1"
        );

        reloaded
            .modify("cn", |overrides| {
                *overrides = RegionVersionOverrides::default()
            })
            .await
            .unwrap();
        assert!(RuntimeVersions::load(&config)
            .overrides("cn")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn loading_drops_overrides_the_config_caught_up_with_unless_forced() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = dir.path().join("runtime-versions.json");
        std::fs::write(
            &state_file,
            r#"{"version":1,"regions":{
                "cn":{"app_version":{"value":"6.4.0","source":"registry","updated_at":"2026-10-01T00:00:00Z"}},
                "tw":{"app_version":{"value":"6.0.0","source":"manual","updated_at":"2026-10-01T00:00:00Z","forced":true}},
                "kr":{"app_version":{"value":"6.9.0","source":"manual","updated_at":"2026-10-01T00:00:00Z"}},
                "jp":{"app_version":{"value":"6.9.0","source":"manual","updated_at":"2026-10-01T00:00:00Z"},
                      "unity_version":{"value":"2022.3.62f1","source":"manual","updated_at":"2026-10-01T00:00:00Z"}},
                "gone":{"app_version":{"value":"1.0.0","source":"manual","updated_at":"2026-10-01T00:00:00Z"}}
            }}"#,
        )
        .unwrap();
        let config = config_with(
            vec![
                ("cn", nuverse_region("http://cdn", "6.5.0")),
                ("tw", nuverse_region("http://cdn", "6.5.0")),
                ("kr", nuverse_region("http://cdn", "6.5.0")),
                ("jp", RegionConfig::default()),
            ],
            state_file.to_str().unwrap(),
        );

        let versions = RuntimeVersions::load(&config);

        assert!(versions.overrides("cn").await.is_empty(), "YAML caught up");
        assert_eq!(
            versions.overrides("tw").await.app_version.unwrap().value,
            "6.0.0"
        );
        assert_eq!(
            versions.overrides("kr").await.app_version.unwrap().value,
            "6.9.0"
        );
        let jp = versions.overrides("jp").await;
        assert!(
            jp.app_version.is_none(),
            "colorful palette has no app version"
        );
        assert_eq!(jp.unity_version.unwrap().value, "2022.3.62f1");
        assert!(
            versions.overrides("gone").await.app_version.is_some(),
            "an unconfigured region keeps its state"
        );
    }

    #[tokio::test]
    async fn damaged_or_unreadable_state_files_start_empty() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = dir.path().join("runtime-versions.json");
        std::fs::write(&state_file, "not json").unwrap();
        let config = config_with(vec![], state_file.to_str().unwrap());
        assert!(RuntimeVersions::load(&config)
            .overrides("cn")
            .await
            .is_empty());

        let config = config_with(vec![], dir.path().to_str().unwrap());
        assert!(RuntimeVersions::load(&config)
            .overrides("cn")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn a_failed_write_leaves_the_state_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("file");
        std::fs::write(&blocker, "").unwrap();
        let config = config_with(vec![], blocker.join("state.json").to_str().unwrap());
        let versions = RuntimeVersions::load(&config);

        let err = versions
            .modify("cn", |overrides| {
                overrides.unity_version = Some(VersionOverride::new(
                    "2022.3.62f1",
                    VersionSource::Manual,
                    false,
                ));
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("failed to persist"), "{err}");
        assert!(versions.overrides("cn").await.is_empty());
    }

    #[tokio::test]
    async fn apply_patches_only_the_overridden_region() {
        let versions = RuntimeVersions::in_memory();
        let config = Arc::new(config_with(
            vec![
                ("cn", nuverse_region("http://cdn", "6.4.0")),
                ("tw", nuverse_region("http://cdn", "6.3.0")),
            ],
            "unused",
        ));
        assert!(Arc::ptr_eq(&versions.apply(&config, "cn").await, &config));

        versions
            .modify("cn", |overrides| {
                overrides.app_version =
                    Some(VersionOverride::new("6.5.0", VersionSource::Manual, false));
                overrides.unity_version = Some(VersionOverride::new(
                    "2022.3.62f1",
                    VersionSource::Manual,
                    false,
                ));
            })
            .await
            .unwrap();
        let effective = versions.apply(&config, "CN").await;
        assert_eq!(
            configured_app_version(&effective.regions["cn"]),
            Some("6.5.0")
        );
        assert_eq!(effective.regions["cn"].runtime.unity_version, "2022.3.62f1");
        assert_eq!(
            configured_app_version(&effective.regions["tw"]),
            Some("6.3.0")
        );
        assert_eq!(configured_app_version(&config.regions["cn"]), Some("6.4.0"));
        assert!(Arc::ptr_eq(&versions.apply(&config, "tw").await, &config));
        assert!(Arc::ptr_eq(
            &versions.apply(&config, "unknown").await,
            &config
        ));
    }

    #[tokio::test]
    async fn probe_accepts_numeric_bodies_and_rejects_missing_or_odd_versions() {
        let base = version_cdn(vec![
            ("6.4.0", StatusCode::OK, "1234\n"),
            ("6.5.0", StatusCode::OK, "<html>"),
            ("6.6.0", StatusCode::OK, " \n"),
        ])
        .await;
        let config = config_with(vec![], "unused");
        let region = nuverse_region(&base, "6.3.0");

        assert_eq!(
            probe_app_version(&config, &region, "6.4.0", "2022.3.21f1")
                .await
                .unwrap(),
            "1234"
        );
        let not_found = probe_app_version(&config, &region, "6.4.1", "2022.3.21f1")
            .await
            .unwrap_err();
        assert!(not_found.contains("404"), "{not_found}");
        let odd = probe_app_version(&config, &region, "6.5.0", "2022.3.21f1")
            .await
            .unwrap_err();
        assert!(odd.contains("non-numeric"), "{odd}");
        assert!(probe_app_version(&config, &region, "6.6.0", "2022.3.21f1")
            .await
            .is_err());
        let bad_header = probe_app_version(&config, &region, "6.4.0", "bad\nheader")
            .await
            .unwrap_err();
        assert!(!bad_header.is_empty());
        let colorful = probe_app_version(&config, &RegionConfig::default(), "6.4.0", "x")
            .await
            .unwrap_err();
        assert!(colorful.contains("nuverse"), "{colorful}");
    }

    #[tokio::test]
    async fn probe_bootstraps_cookies_when_the_region_requires_them() {
        let base = version_cdn(vec![("6.4.0", StatusCode::OK, "1")]).await;
        let config = config_with(vec![], "unused");
        let mut region = nuverse_region(&base, "6.3.0");
        if let RegionProviderConfig::Nuverse {
            required_cookies,
            cookie_bootstrap_url,
            ..
        } = &mut region.provider
        {
            *required_cookies = true;
            *cookie_bootstrap_url = Some(format!("{base}/no-such-cookie-endpoint"));
        }
        let err = probe_app_version(&config, &region, "6.4.0", "2022.3.21f1")
            .await
            .unwrap_err();
        assert!(err.contains("no-such-cookie-endpoint"), "{err}");
    }
}
