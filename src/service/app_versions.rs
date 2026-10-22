//! Adopting new app/Unity versions at runtime, and the registry poller.
//!
//! Every app-version change goes through [`AppVersionManager::offer_app_version`]:
//! a candidate must parse, must be newer than the region's current value
//! (no downgrades) and must pass a probe of the Nuverse version URL before it
//! replaces the current value. `force` skips the last two checks. The rule is
//! the same for the poller, the HTTP API and update requests, so the current
//! value only moves forward unless an operator forces it or reverts it.
//!
//! A forced value is an operator's pin (for example a rollback away from a
//! broken build): the poller and update requests leave it alone until a
//! manual `PUT` replaces it or `DELETE` reverts it.
//!
//! Nuverse keys its CDN by the bundle version, which stays `x.y.0` when the
//! client is republished as `x.y.z` without new assets. So when the version
//! URL answers 404 for `x.y.z`, the probe is repeated for `x.y.0`: equal to
//! the current value it is up to date, newer it is adopted (as `x.y.0`, the
//! CDN key, never the store version). The candidate itself still only ever
//! comes from the registry, an operator or an update request.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sonic_rs::JsonValueTrait;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::core::app_versions::{
    compare_app_versions, configured_app_version, effective_app_version, effective_unity_version,
    is_valid_unity_version, parse_app_version, patch_zeroed_app_version, probe_app_version,
    RegionVersionOverrides, RuntimeVersionError, RuntimeVersions, VersionOverride, VersionSource,
};
use crate::core::config::{AppConfig, RegionConfig, RegionProviderConfig};
use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};
use crate::service::jobs::JobManager;

/// Which overridden value to revert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionField {
    AppVersion,
    UnityVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptOutcome {
    /// `app_version` replaced `previous`. It is the candidate, or the
    /// candidate with its patch zeroed when only that exists on the CDN.
    /// `asset_version` is what the version URL answered (`None` when the
    /// probe was skipped with `force`).
    Adopted {
        app_version: String,
        previous: String,
        asset_version: Option<String>,
    },
    /// The candidate already is the current value.
    Unchanged,
}

#[derive(Debug, thiserror::Error)]
pub enum AdoptError {
    #[error("region `{0}` not found")]
    UnknownRegion(String),
    #[error("region `{0}` is not a nuverse region; it has no app version")]
    NotNuverse(String),
    #[error("{0}")]
    Invalid(String),
    #[error("app version is pinned to {current} by a forced override; {candidate} not adopted")]
    Pinned { current: String, candidate: String },
    #[error("refusing to downgrade app version from {current} to {candidate}")]
    Downgrade { current: String, candidate: String },
    #[error("app version {candidate} failed the CDN probe: {message}")]
    ProbeFailed { candidate: String, message: String },
    #[error(transparent)]
    Persist(#[from] RuntimeVersionError),
}

/// What the last poll of a region found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncResult {
    UpToDate,
    Adopted,
    RegistryOlder,
    /// A forced override pins the region; the registry value is ignored.
    Pinned,
    RegistryFailed,
    ProbeFailed,
    PersistFailed,
}

/// Poller bookkeeping for one region (in memory; reset by a restart).
#[derive(Debug, Clone, Default, Serialize)]
pub struct RegionSyncStatus {
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_result: Option<SyncResult>,
    pub last_error: Option<String>,
    pub registry_app_version: Option<String>,
    pub last_probe_at: Option<DateTime<Utc>>,
    pub last_probe_ok: Option<bool>,
    pub last_probe_asset_version: Option<String>,
    pub consecutive_probe_failures: u32,
    pub consecutive_registry_failures: u32,
    pub last_adopted_at: Option<DateTime<Utc>>,
    /// A newer version was adopted and its update job is still to be
    /// submitted (waits while the region has an active job).
    pub pending_submit: bool,
    pub last_submitted_job_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VersionView {
    /// What a job created now uses.
    pub effective: String,
    /// The YAML value.
    pub configured: String,
    #[serde(rename = "override", skip_serializing_if = "Option::is_none")]
    pub override_: Option<VersionOverride>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegionVersionView {
    pub region: String,
    pub enabled: bool,
    pub provider_kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_version: Option<VersionView>,
    pub unity_version: VersionView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync: Option<RegionSyncStatus>,
}

/// The `/healthz` summary of the poller, for alerting.
#[derive(Debug, Clone, Serialize)]
pub struct SyncHealth {
    pub max_consecutive_probe_failures: u32,
    pub max_consecutive_registry_failures: u32,
    pub regions: BTreeMap<String, RegionSyncHealth>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegionSyncHealth {
    pub app_version: Option<String>,
    pub last_result: Option<SyncResult>,
    pub consecutive_probe_failures: u32,
    pub consecutive_registry_failures: u32,
}

#[derive(Clone)]
pub struct AppVersionManager {
    config: Arc<AppConfig>,
    versions: RuntimeVersions,
    jobs: JobManager,
    status: Arc<RwLock<BTreeMap<String, RegionSyncStatus>>>,
    registry: reqwest::Client,
}

fn provider_kind(region: &RegionConfig) -> &'static str {
    match region.provider {
        RegionProviderConfig::ColorfulPalette { .. } => "colorful_palette",
        RegionProviderConfig::Nuverse { .. } => "nuverse",
    }
}

fn is_nuverse(region: &RegionConfig) -> bool {
    configured_app_version(region).is_some()
}

impl AppVersionManager {
    pub fn new(config: Arc<AppConfig>, versions: RuntimeVersions, jobs: JobManager) -> Self {
        let timeout = Duration::from_secs(config.app_version_sync.request_timeout_secs.max(1));
        let registry = reqwest::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .unwrap_or_default();
        Self {
            config,
            versions,
            jobs,
            status: Arc::new(RwLock::new(BTreeMap::new())),
            registry,
        }
    }

    fn region(&self, name: &str) -> Result<(String, &RegionConfig), AdoptError> {
        let key = name.to_ascii_lowercase();
        let region = self
            .config
            .regions
            .get(&key)
            .ok_or_else(|| AdoptError::UnknownRegion(name.to_string()))?;
        Ok((key, region))
    }

    /// Offers `candidate` as the region's app version (see the module docs).
    pub async fn offer_app_version(
        &self,
        region: &str,
        candidate: &str,
        source: VersionSource,
        force: bool,
        probe_unity_version: Option<&str>,
    ) -> Result<AdoptOutcome, AdoptError> {
        let (key, region_config) = self.region(region)?;
        if !is_nuverse(region_config) {
            return Err(AdoptError::NotNuverse(key));
        }
        let candidate = candidate.trim();
        if parse_app_version(candidate).is_none() {
            return Err(AdoptError::Invalid(format!(
                "invalid app version `{candidate}`; expected x.y.z or x.y.z.w"
            )));
        }
        let overrides = self.versions.overrides(&key).await;
        check_pin(&overrides, source, candidate)?;
        if !force && !is_newer(region_config, &overrides, candidate)? {
            return Ok(AdoptOutcome::Unchanged);
        }
        let (app_version, asset_version) = if force {
            (candidate.to_string(), None)
        } else {
            let unity_version = probe_unity_version
                .map(str::to_string)
                .unwrap_or_else(|| effective_unity_version(region_config, &overrides));
            match self
                .probe_candidate(&key, region_config, &overrides, candidate, &unity_version)
                .await?
            {
                Some((app_version, asset_version)) => (app_version, Some(asset_version)),
                None => return Ok(AdoptOutcome::Unchanged),
            }
        };
        // The probe ran without the lock: check again against whatever is
        // current now, so a concurrent adoption of a newer value wins.
        let outcome = self
            .versions
            .modify(&key, |overrides| {
                let previous = effective_app_version(region_config, overrides).unwrap_or_default();
                check_pin(overrides, source, &app_version)?;
                if !force && !is_newer(region_config, overrides, &app_version)? {
                    return Ok(AdoptOutcome::Unchanged);
                }
                overrides.app_version = Some(VersionOverride::new(&app_version, source, force));
                Ok(AdoptOutcome::Adopted {
                    app_version: app_version.clone(),
                    previous,
                    asset_version: asset_version.clone(),
                })
            })
            .await?;
        if let Ok(AdoptOutcome::Adopted { previous, .. }) = &outcome {
            info!(
                region = %key,
                previous = %previous,
                app_version = %app_version,
                candidate = %candidate,
                asset_version = ?asset_version,
                source = ?source,
                force,
                "adopted new app version"
            );
        }
        outcome
    }

    /// Probes `candidate`, and on a 404 its patch-zeroed version (see the
    /// module docs). Returns the version to adopt with its asset version, or
    /// `None` when the patch-zeroed version is the current one.
    async fn probe_candidate(
        &self,
        region: &str,
        region_config: &RegionConfig,
        overrides: &RegionVersionOverrides,
        candidate: &str,
        unity_version: &str,
    ) -> Result<Option<(String, String)>, AdoptError> {
        let err =
            match probe_app_version(&self.config, region_config, candidate, unity_version).await {
                Ok(asset_version) => return Ok(Some((candidate.to_string(), asset_version))),
                Err(err) => err,
            };
        let failed = |message: String| AdoptError::ProbeFailed {
            candidate: candidate.to_string(),
            message,
        };
        let zeroed = match patch_zeroed_app_version(candidate) {
            Some(zeroed) if err.not_found => zeroed,
            _ => return Err(failed(err.message)),
        };
        match is_newer(region_config, overrides, &zeroed) {
            Ok(true) => {}
            Ok(false) => {
                debug!(
                    region,
                    candidate, cdn_app_version = %zeroed,
                    "app version not on the CDN; its patch-zeroed version is current"
                );
                return Ok(None);
            }
            // Older than the current value: nothing to fall back to.
            Err(_) => return Err(failed(err.message)),
        }
        match probe_app_version(&self.config, region_config, &zeroed, unity_version).await {
            Ok(asset_version) => Ok(Some((zeroed, asset_version))),
            Err(zeroed_err) => Err(failed(format!(
                "{}; patch-zeroed {zeroed} also failed: {}",
                err.message, zeroed_err.message
            ))),
        }
    }

    /// Sets the region's Unity version (any provider kind). Returns whether
    /// it changed.
    pub async fn set_unity_version(
        &self,
        region: &str,
        unity_version: &str,
        source: VersionSource,
    ) -> Result<bool, AdoptError> {
        let (key, region_config) = self.region(region)?;
        let unity_version = unity_version.trim();
        if !is_valid_unity_version(unity_version) {
            return Err(AdoptError::Invalid(format!(
                "invalid unity version `{unity_version}`; expected e.g. 2022.3.21f1"
            )));
        }
        let changed = self
            .versions
            .modify(&key, |overrides| {
                if effective_unity_version(region_config, overrides) == unity_version {
                    return false;
                }
                overrides.unity_version = Some(VersionOverride::new(unity_version, source, false));
                true
            })
            .await?;
        if changed {
            info!(region = %key, unity_version = %unity_version, source = ?source, "unity version changed");
        }
        Ok(changed)
    }

    /// Reverts the given fields (both when `None`) to the YAML values.
    pub async fn revert(
        &self,
        region: &str,
        field: Option<VersionField>,
    ) -> Result<(), AdoptError> {
        let (key, _) = self.region(region)?;
        self.versions
            .modify(&key, |overrides| {
                if field != Some(VersionField::UnityVersion) {
                    overrides.app_version = None;
                }
                if field != Some(VersionField::AppVersion) {
                    overrides.unity_version = None;
                }
            })
            .await?;
        info!(region = %key, field = ?field, "runtime version override reverted");
        Ok(())
    }

    pub async fn region_view(&self, region: &str) -> Result<RegionVersionView, AdoptError> {
        let (key, region_config) = self.region(region)?;
        let overrides = self.versions.overrides(&key).await;
        let sync = if self.config.app_version_sync.enabled && is_nuverse(region_config) {
            Some(
                self.status
                    .read()
                    .await
                    .get(&key)
                    .cloned()
                    .unwrap_or_default(),
            )
        } else {
            None
        };
        Ok(RegionVersionView {
            app_version: configured_app_version(region_config).map(|configured| VersionView {
                effective: effective_app_version(region_config, &overrides).unwrap_or_default(),
                configured: configured.to_string(),
                override_: overrides.app_version.clone(),
            }),
            unity_version: VersionView {
                effective: effective_unity_version(region_config, &overrides),
                configured: region_config.runtime.unity_version.clone(),
                override_: overrides.unity_version.clone(),
            },
            region: key,
            enabled: region_config.enabled,
            provider_kind: provider_kind(region_config),
            sync,
        })
    }

    pub async fn region_views(&self) -> Vec<RegionVersionView> {
        let mut views = Vec::with_capacity(self.config.regions.len());
        for name in self.config.regions.keys() {
            if let Ok(view) = self.region_view(name).await {
                views.push(view);
            }
        }
        views
    }

    /// The poller summary for `/healthz`; `None` while the poller is off.
    pub async fn health(&self) -> Option<SyncHealth> {
        if !self.config.app_version_sync.enabled {
            return None;
        }
        let status = self.status.read().await;
        let mut regions = BTreeMap::new();
        for name in self.polled_regions() {
            let entry = status.get(&name).cloned().unwrap_or_default();
            let overrides = self.versions.overrides(&name).await;
            regions.insert(
                name.clone(),
                RegionSyncHealth {
                    app_version: effective_app_version(&self.config.regions[&name], &overrides),
                    last_result: entry.last_result,
                    consecutive_probe_failures: entry.consecutive_probe_failures,
                    consecutive_registry_failures: entry.consecutive_registry_failures,
                },
            );
        }
        Some(SyncHealth {
            max_consecutive_probe_failures: regions
                .values()
                .map(|region| region.consecutive_probe_failures)
                .max()
                .unwrap_or(0),
            max_consecutive_registry_failures: regions
                .values()
                .map(|region| region.consecutive_registry_failures)
                .max()
                .unwrap_or(0),
            regions,
        })
    }

    /// Enabled Nuverse regions, the ones the poller follows.
    fn polled_regions(&self) -> Vec<String> {
        self.config
            .regions
            .iter()
            .filter(|(_, region)| region.enabled && is_nuverse(region))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Starts the registry poller when `app_version_sync.enabled`.
    pub fn spawn_poller(&self) -> Option<JoinHandle<()>> {
        if !self.config.app_version_sync.enabled {
            return None;
        }
        let manager = self.clone();
        let interval = Duration::from_secs(self.config.app_version_sync.interval_secs.max(1));
        info!(
            registry_url = %self.config.app_version_sync.registry_url,
            interval_secs = interval.as_secs(),
            regions = ?self.polled_regions(),
            "app version sync enabled"
        );
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                manager.poll_once().await;
            }
        }))
    }

    /// Polls every followed region once, concurrently; one region's failure
    /// never affects another.
    pub async fn poll_once(&self) {
        let regions = self.polled_regions();
        futures_util::future::join_all(regions.iter().map(|region| self.poll_region(region))).await;
    }

    async fn poll_region(&self, region: &str) {
        let now = Utc::now();
        let fetched = self.fetch_registry_app_version(region).await;
        let mut adopted = false;
        {
            let outcome = match &fetched {
                Ok(candidate) => Some(
                    self.offer_app_version(region, candidate, VersionSource::Registry, false, None)
                        .await,
                ),
                Err(_) => None,
            };
            let mut status = self.status.write().await;
            let entry = status.entry(region.to_string()).or_default();
            entry.last_checked_at = Some(now);
            match (&fetched, outcome) {
                (Err(message), _) => {
                    warn!(region, error = %message, "app version sync: registry request failed");
                    entry.consecutive_registry_failures += 1;
                    entry.last_result = Some(SyncResult::RegistryFailed);
                    entry.last_error = Some(message.clone());
                }
                (Ok(candidate), Some(outcome)) => {
                    entry.consecutive_registry_failures = 0;
                    entry.registry_app_version = Some(candidate.clone());
                    adopted = record_outcome(entry, region, outcome, now);
                    if adopted && self.config.app_version_sync.auto_submit {
                        entry.pending_submit = true;
                    }
                }
                (Ok(_), None) => unreachable!("an outcome exists for every fetched version"),
            }
        }
        if adopted
            || self
                .status
                .read()
                .await
                .get(region)
                .is_some_and(|s| s.pending_submit)
        {
            self.submit_pending(region).await;
        }
    }

    async fn submit_pending(&self, region: &str) {
        if !self
            .status
            .read()
            .await
            .get(region)
            .is_some_and(|status| status.pending_submit)
        {
            return;
        }
        if self.jobs.has_active_job(region).await {
            debug!(region, "app version sync: region busy; update job deferred");
            return;
        }
        let request = AssetUpdateRequest {
            region: region.to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let submitted = self.jobs.submit(request).await;
        let mut status = self.status.write().await;
        let entry = status.entry(region.to_string()).or_default();
        entry.pending_submit = false;
        match submitted {
            Ok(job) => {
                info!(region, job_id = %job.id, app_version = ?job.app_version, "app version sync: update job submitted");
                entry.last_submitted_job_id = Some(job.id);
            }
            Err(err) => warn!(region, error = %err, "app version sync: update job rejected"),
        }
    }

    async fn fetch_registry_app_version(&self, region: &str) -> Result<String, String> {
        let url = format!(
            "{}/v1/master/{region}/current",
            self.config
                .app_version_sync
                .registry_url
                .trim_end_matches('/')
        );
        let response = self
            .registry
            .get(&url)
            .send()
            .await
            .map_err(|err| format!("GET {url}: {err}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("GET {url} returned status {}", status.as_u16()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| format!("GET {url}: {err}"))?;
        let manifest: sonic_rs::Value =
            sonic_rs::from_slice(&body).map_err(|err| format!("GET {url}: invalid JSON: {err}"))?;
        let app_version = manifest
            .get("appVersion")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .unwrap_or_default();
        if parse_app_version(app_version).is_none() {
            return Err(format!(
                "GET {url}: manifest appVersion `{app_version}` is not x.y.z or x.y.z.w"
            ));
        }
        Ok(app_version.to_string())
    }
}

/// Only an operator (`PUT`) may replace a forced override.
fn check_pin(
    overrides: &RegionVersionOverrides,
    source: VersionSource,
    candidate: &str,
) -> Result<(), AdoptError> {
    match &overrides.app_version {
        Some(pin) if pin.forced && source != VersionSource::Manual && pin.value != candidate => {
            Err(AdoptError::Pinned {
                current: pin.value.clone(),
                candidate: candidate.to_string(),
            })
        }
        _ => Ok(()),
    }
}

/// Whether `candidate` is newer than the current value (`false` when equal);
/// an error when it is older. A current value that does not parse never
/// blocks a valid candidate.
fn is_newer(
    region: &RegionConfig,
    overrides: &RegionVersionOverrides,
    candidate: &str,
) -> Result<bool, AdoptError> {
    let current = effective_app_version(region, overrides).unwrap_or_default();
    match compare_app_versions(candidate, &current) {
        Some(Ordering::Greater) | None => Ok(true),
        Some(Ordering::Equal) => Ok(false),
        Some(Ordering::Less) => Err(AdoptError::Downgrade {
            current,
            candidate: candidate.to_string(),
        }),
    }
}

fn record_outcome(
    entry: &mut RegionSyncStatus,
    region: &str,
    outcome: Result<AdoptOutcome, AdoptError>,
    now: DateTime<Utc>,
) -> bool {
    match outcome {
        Ok(AdoptOutcome::Unchanged) => {
            entry.consecutive_probe_failures = 0;
            entry.last_result = Some(SyncResult::UpToDate);
            entry.last_error = None;
            false
        }
        Ok(AdoptOutcome::Adopted { asset_version, .. }) => {
            entry.consecutive_probe_failures = 0;
            entry.last_result = Some(SyncResult::Adopted);
            entry.last_error = None;
            entry.last_probe_at = Some(now);
            entry.last_probe_ok = Some(true);
            entry.last_probe_asset_version = asset_version;
            entry.last_adopted_at = Some(now);
            true
        }
        Err(AdoptError::Downgrade { .. }) => {
            entry.consecutive_probe_failures = 0;
            entry.last_result = Some(SyncResult::RegistryOlder);
            entry.last_error = None;
            false
        }
        Err(AdoptError::Pinned { .. }) => {
            entry.consecutive_probe_failures = 0;
            entry.last_result = Some(SyncResult::Pinned);
            entry.last_error = None;
            false
        }
        Err(err @ AdoptError::ProbeFailed { .. }) => {
            warn!(region, error = %err, "app version sync: probe failed; keeping the current version");
            entry.consecutive_probe_failures += 1;
            entry.last_result = Some(SyncResult::ProbeFailed);
            entry.last_error = Some(err.to_string());
            entry.last_probe_at = Some(now);
            entry.last_probe_ok = Some(false);
            false
        }
        Err(err) => {
            error!(region, error = %err, "app version sync: cannot adopt the registry version");
            entry.last_result = Some(SyncResult::PersistFailed);
            entry.last_error = Some(err.to_string());
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::core::app_versions::test_support::{config_with, nuverse_region, MockHttp};
    use crate::core::config::RegionConfig;
    use crate::core::models::JobStatus;

    struct Fixture {
        manager: AppVersionManager,
        registry: MockHttp,
        cdn: MockHttp,
        _dir: tempfile::TempDir,
    }

    /// `cn` and `tw` are Nuverse regions at 6.4.0, `kr` a disabled Nuverse
    /// region, `jp` a colorful-palette region.
    async fn fixture(enabled: bool, auto_submit: bool) -> Fixture {
        let registry = MockHttp::start().await;
        let cdn = MockHttp::start().await;
        let dir = tempfile::tempdir().unwrap();
        let state_file = dir.path().join("runtime-versions.json");
        let mut disabled = nuverse_region(&cdn.base_url, "6.4.0");
        disabled.enabled = false;
        let mut config = config_with(
            vec![
                ("cn", nuverse_region(&cdn.base_url, "6.4.0")),
                ("tw", nuverse_region(&cdn.base_url, "6.4.0")),
                ("kr", disabled),
                (
                    "jp",
                    RegionConfig {
                        enabled: true,
                        ..RegionConfig::default()
                    },
                ),
            ],
            state_file.to_str().unwrap(),
        );
        config.app_version_sync.enabled = enabled;
        config.app_version_sync.auto_submit = auto_submit;
        config.app_version_sync.registry_url = format!("{}/", registry.base_url);
        let config = Arc::new(config);
        let versions = RuntimeVersions::load(&config);
        let jobs = JobManager::with_versions(config.clone(), versions.clone());
        Fixture {
            manager: AppVersionManager::new(config, versions, jobs),
            registry,
            cdn,
            _dir: dir,
        }
    }

    impl Fixture {
        fn registry_says(&self, region: &str, app_version: &str) {
            self.registry.set(
                &format!("/v1/master/{region}/current"),
                StatusCode::OK,
                &format!(r#"{{"appVersion":"{app_version}","dataVersion":"1"}}"#),
            );
        }

        fn cdn_has(&self, app_version: &str) {
            self.cdn
                .set(&format!("/version/{app_version}"), StatusCode::OK, "100");
        }

        async fn status(&self, region: &str) -> RegionSyncStatus {
            self.manager.status.read().await[region].clone()
        }

        async fn effective(&self, region: &str) -> String {
            self.manager
                .region_view(region)
                .await
                .unwrap()
                .app_version
                .unwrap()
                .effective
        }
    }

    #[tokio::test]
    async fn a_failing_region_never_blocks_the_others() {
        let fx = fixture(true, false).await;
        fx.registry_says("cn", "6.5.0");
        fx.registry_says("tw", "6.6.1");
        fx.cdn_has("6.5.0");

        fx.manager.poll_once().await;
        fx.manager.poll_once().await;

        let cn = fx.status("cn").await;
        assert_eq!(fx.effective("cn").await, "6.5.0");
        assert_eq!(cn.last_result, Some(SyncResult::UpToDate));
        assert_eq!(cn.last_probe_asset_version.as_deref(), Some("100"));
        assert!(cn.last_adopted_at.is_some());
        assert!(!cn.pending_submit, "auto_submit is off");

        let tw = fx.status("tw").await;
        assert_eq!(
            fx.effective("tw").await,
            "6.4.0",
            "404 keeps the current value"
        );
        assert_eq!(tw.last_result, Some(SyncResult::ProbeFailed));
        assert_eq!(tw.consecutive_probe_failures, 2);
        assert_eq!(tw.last_probe_ok, Some(false));
        assert!(tw.last_error.as_deref().unwrap().contains("404"));

        assert!(
            !fx.manager.status.read().await.contains_key("kr"),
            "disabled regions are not polled"
        );
        let health = fx.manager.health().await.unwrap();
        assert_eq!(health.max_consecutive_probe_failures, 2);
        assert_eq!(health.max_consecutive_registry_failures, 0);
        assert_eq!(health.regions["cn"].app_version.as_deref(), Some("6.5.0"));
        assert_eq!(health.regions.len(), 2);

        // The CDN catches up: the next poll adopts and resets the counter.
        fx.cdn_has("6.6.1");
        fx.manager.poll_once().await;
        let tw = fx.status("tw").await;
        assert_eq!(tw.last_result, Some(SyncResult::Adopted));
        assert_eq!(tw.consecutive_probe_failures, 0);
        assert_eq!(fx.effective("tw").await, "6.6.1");
    }

    #[tokio::test]
    async fn a_patch_release_whose_zeroed_version_is_current_is_up_to_date() {
        let fx = fixture(true, true).await;
        // The store republished 6.4.0 as 6.4.1; the CDN only knows 6.4.0.
        fx.registry_says("cn", "6.4.1");
        fx.registry_says("tw", "6.4.0");
        fx.manager
            .status
            .write()
            .await
            .entry("cn".to_string())
            .or_default()
            .consecutive_probe_failures = 7;

        fx.manager.poll_once().await;

        let cn = fx.status("cn").await;
        assert_eq!(cn.last_result, Some(SyncResult::UpToDate));
        assert_eq!(cn.consecutive_probe_failures, 0, "the counter resets");
        assert_eq!(cn.last_error, None);
        assert_eq!(cn.registry_app_version.as_deref(), Some("6.4.1"));
        assert!(!cn.pending_submit);
        assert!(cn.last_submitted_job_id.is_none(), "no job for 6.4.0 again");
        assert_eq!(fx.effective("cn").await, "6.4.0");
        let view = fx.manager.region_view("cn").await.unwrap();
        assert!(view.app_version.unwrap().override_.is_none());
        let health = fx.manager.health().await.unwrap();
        assert_eq!(health.max_consecutive_probe_failures, 0);
    }

    #[tokio::test]
    async fn a_patch_release_adopts_its_newer_zeroed_cdn_version() {
        let fx = fixture(true, false).await;
        fx.registry_says("cn", "6.5.1");
        fx.registry_says("tw", "6.4.0");
        fx.cdn_has("6.5.0");

        fx.manager.poll_once().await;

        let cn = fx.status("cn").await;
        assert_eq!(cn.last_result, Some(SyncResult::Adopted));
        assert_eq!(cn.consecutive_probe_failures, 0);
        assert_eq!(cn.last_probe_ok, Some(true));
        assert_eq!(cn.last_probe_asset_version.as_deref(), Some("100"));
        assert_eq!(cn.registry_app_version.as_deref(), Some("6.5.1"));
        assert_eq!(fx.effective("cn").await, "6.5.0", "the CDN key is adopted");
        let view = fx.manager.region_view("cn").await.unwrap();
        let entry = view.app_version.unwrap().override_.unwrap();
        assert_eq!(entry.value, "6.5.0");
        assert_eq!(entry.source, VersionSource::Registry);
        assert!(!entry.forced);

        // The next poll sees 6.5.1 again: 404, 6.5.0 is current now.
        fx.manager.poll_once().await;
        assert_eq!(
            fx.status("cn").await.last_result,
            Some(SyncResult::UpToDate)
        );

        let outcome = fx
            .manager
            .offer_app_version("tw", "6.5.3", VersionSource::Manual, false, None)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            AdoptOutcome::Adopted {
                app_version: "6.5.0".to_string(),
                previous: "6.4.0".to_string(),
                asset_version: Some("100".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn a_patch_release_fails_when_its_zeroed_version_is_missing_too() {
        let fx = fixture(true, false).await;
        fx.registry_says("cn", "6.5.1");
        fx.registry_says("tw", "6.4.0");

        fx.manager.poll_once().await;
        fx.manager.poll_once().await;

        let cn = fx.status("cn").await;
        assert_eq!(cn.last_result, Some(SyncResult::ProbeFailed));
        assert_eq!(cn.consecutive_probe_failures, 2);
        assert_eq!(cn.last_probe_ok, Some(false));
        let message = cn.last_error.unwrap();
        assert!(message.contains("6.5.1 failed the CDN probe"), "{message}");
        assert!(
            message.contains("patch-zeroed 6.5.0 also failed"),
            "{message}"
        );
        assert_eq!(fx.effective("cn").await, "6.4.0");
    }

    #[tokio::test]
    async fn only_a_404_falls_back_and_never_below_the_current_version() {
        let fx = fixture(false, false).await;
        // Not a 404: no fallback, although 6.4.0 is current.
        fx.cdn.set("/version/6.4.1", StatusCode::OK, "<html>");
        let err = fx
            .manager
            .offer_app_version("cn", "6.4.1", VersionSource::Registry, false, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AdoptError::ProbeFailed { .. }), "{err}");
        assert!(!err.to_string().contains("patch-zeroed"), "{err}");

        // A candidate already at x.y.0 has nothing to fall back to.
        let err = fx
            .manager
            .offer_app_version("cn", "6.6.0", VersionSource::Registry, false, None)
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("patch-zeroed"), "{err}");

        // Current 6.4.2: 6.4.3 is missing and 6.4.0 would be a downgrade.
        fx.cdn_has("6.4.2");
        fx.cdn_has("6.4.0");
        fx.manager
            .offer_app_version("tw", "6.4.2", VersionSource::Manual, false, None)
            .await
            .unwrap();
        let err = fx
            .manager
            .offer_app_version("tw", "6.4.3", VersionSource::Registry, false, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AdoptError::ProbeFailed { .. }), "{err}");
        assert_eq!(fx.effective("tw").await, "6.4.2");
    }

    #[tokio::test]
    async fn colorful_palette_regions_are_untouched_by_the_fallback() {
        let fx = fixture(true, false).await;
        fx.registry_says("cn", "6.4.0");
        fx.registry_says("tw", "6.4.0");
        fx.registry_says("jp", "7.1.1");
        fx.cdn_has("7.1.0");

        let err = fx
            .manager
            .offer_app_version("jp", "7.1.1", VersionSource::Registry, false, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AdoptError::NotNuverse(_)), "{err}");
        fx.manager.poll_once().await;
        assert!(!fx.manager.status.read().await.contains_key("jp"));
        let health = fx.manager.health().await.unwrap();
        assert!(!health.regions.contains_key("jp"));
        let jp = fx.manager.region_view("jp").await.unwrap();
        assert!(jp.app_version.is_none());
        assert!(jp.sync.is_none());
    }

    #[tokio::test]
    async fn registry_failures_are_counted_per_region() {
        let fx = fixture(true, false).await;
        fx.registry.set(
            "/v1/master/tw/current",
            StatusCode::OK,
            r#"{"appVersion":"latest"}"#,
        );
        fx.manager.poll_once().await;
        fx.registry
            .set("/v1/master/tw/current", StatusCode::OK, "not json");
        fx.manager.poll_once().await;

        let cn = fx.status("cn").await;
        assert_eq!(cn.last_result, Some(SyncResult::RegistryFailed));
        assert_eq!(cn.consecutive_registry_failures, 2);
        assert!(cn.last_error.as_deref().unwrap().contains("404"));
        let tw = fx.status("tw").await;
        assert_eq!(tw.consecutive_registry_failures, 2);
        assert!(tw.last_error.as_deref().unwrap().contains("invalid JSON"));

        fx.registry_says("cn", "6.4.0");
        fx.manager.poll_once().await;
        let cn = fx.status("cn").await;
        assert_eq!(cn.last_result, Some(SyncResult::UpToDate));
        assert_eq!(cn.consecutive_registry_failures, 0);
        assert_eq!(cn.registry_app_version.as_deref(), Some("6.4.0"));
    }

    #[tokio::test]
    async fn an_unreachable_registry_is_a_registry_failure() {
        let mut fx = fixture(true, false).await;
        let mut config = AppConfig::clone(&fx.manager.config);
        config.app_version_sync.registry_url = "http://127.0.0.1:1".to_string();
        fx.manager.config = Arc::new(config);
        fx.manager.poll_once().await;
        assert_eq!(
            fx.status("cn").await.last_result,
            Some(SyncResult::RegistryFailed)
        );
    }

    #[tokio::test]
    async fn manual_overrides_win_until_the_registry_is_newer() {
        let fx = fixture(true, false).await;
        fx.cdn_has("6.6.0");
        fx.cdn_has("6.7.0");
        let outcome = fx
            .manager
            .offer_app_version("CN", "6.6.0", VersionSource::Manual, false, None)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            AdoptOutcome::Adopted {
                app_version: "6.6.0".to_string(),
                previous: "6.4.0".to_string(),
                asset_version: Some("100".to_string()),
            }
        );

        fx.registry_says("cn", "6.5.0");
        fx.manager.poll_once().await;
        assert_eq!(fx.effective("cn").await, "6.6.0");
        assert_eq!(
            fx.status("cn").await.last_result,
            Some(SyncResult::RegistryOlder)
        );

        fx.registry_says("cn", "6.7.0");
        fx.manager.poll_once().await;
        assert_eq!(fx.effective("cn").await, "6.7.0");
        let view = fx.manager.region_view("cn").await.unwrap();
        assert_eq!(
            view.app_version.unwrap().override_.unwrap().source,
            VersionSource::Registry
        );
        assert!(view.sync.is_some());
    }

    #[tokio::test]
    async fn downgrades_need_force_and_force_skips_the_probe() {
        let fx = fixture(false, false).await;
        fx.cdn_has("6.5.0");
        fx.manager
            .offer_app_version(
                "cn",
                "6.5.0",
                VersionSource::Manual,
                false,
                Some("2022.3.62f1"),
            )
            .await
            .unwrap();

        let err = fx
            .manager
            .offer_app_version("cn", "6.4.9", VersionSource::Manual, false, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AdoptError::Downgrade { .. }), "{err}");
        assert_eq!(
            fx.manager
                .offer_app_version("cn", "6.5.0.0", VersionSource::Manual, false, None)
                .await
                .unwrap(),
            AdoptOutcome::Unchanged
        );

        let forced = fx
            .manager
            .offer_app_version("cn", "6.3.0", VersionSource::Manual, true, None)
            .await
            .unwrap();
        assert_eq!(
            forced,
            AdoptOutcome::Adopted {
                app_version: "6.3.0".to_string(),
                previous: "6.5.0".to_string(),
                asset_version: None,
            }
        );
        let view = fx.manager.region_view("cn").await.unwrap();
        assert!(
            view.app_version
                .as_ref()
                .unwrap()
                .override_
                .as_ref()
                .unwrap()
                .forced
        );
        assert!(
            view.sync.is_none(),
            "no sync status while the poller is off"
        );
        assert!(fx.manager.health().await.is_none());
        assert!(fx.manager.spawn_poller().is_none());
    }

    #[tokio::test]
    async fn a_forced_pin_holds_until_an_operator_replaces_it() {
        let fx = fixture(true, false).await;
        fx.cdn_has("6.5.0");
        fx.cdn_has("6.6.0");
        fx.registry_says("cn", "6.5.0");
        fx.manager
            .offer_app_version("cn", "6.3.0", VersionSource::Manual, true, None)
            .await
            .unwrap();

        fx.manager.poll_once().await;
        assert_eq!(fx.effective("cn").await, "6.3.0");
        assert_eq!(fx.status("cn").await.last_result, Some(SyncResult::Pinned));
        let err = fx
            .manager
            .offer_app_version("cn", "6.5.0", VersionSource::UpdateRequest, false, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AdoptError::Pinned { .. }), "{err}");
        assert_eq!(
            fx.manager
                .offer_app_version("cn", "6.3.0", VersionSource::Registry, false, None)
                .await
                .unwrap(),
            AdoptOutcome::Unchanged
        );

        fx.manager
            .offer_app_version("cn", "6.6.0", VersionSource::Manual, false, None)
            .await
            .unwrap();
        fx.registry_says("cn", "6.7.0");
        fx.cdn_has("6.7.0");
        fx.manager.poll_once().await;
        assert_eq!(fx.effective("cn").await, "6.7.0", "the pin is gone");
    }

    #[tokio::test]
    async fn invalid_requests_are_rejected_before_anything_changes() {
        let fx = fixture(false, false).await;
        let cases = [
            ("xx", "6.5.0", "not found"),
            ("jp", "6.5.0", "not a nuverse region"),
            ("cn", "6.5", "invalid app version"),
        ];
        for (region, candidate, expected) in cases {
            let err = fx
                .manager
                .offer_app_version(region, candidate, VersionSource::Manual, false, None)
                .await
                .unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
        }
        let probe = fx
            .manager
            .offer_app_version("cn", "6.5.0", VersionSource::Manual, false, None)
            .await
            .unwrap_err();
        assert!(matches!(probe, AdoptError::ProbeFailed { .. }), "{probe}");
        let unity = fx
            .manager
            .set_unity_version("jp", "2022.3", VersionSource::Manual)
            .await
            .unwrap_err();
        assert!(unity.to_string().contains("invalid unity version"));
        assert!(fx.manager.revert("xx", None).await.is_err());
        assert!(fx.manager.region_view("xx").await.is_err());
    }

    #[tokio::test]
    async fn unity_versions_apply_to_every_provider_and_revert_per_field() {
        let fx = fixture(false, false).await;
        fx.cdn_has("6.5.0");
        assert!(fx
            .manager
            .set_unity_version("jp", "2022.3.62f1", VersionSource::Manual)
            .await
            .unwrap());
        assert!(!fx
            .manager
            .set_unity_version("jp", "2022.3.62f1", VersionSource::Manual)
            .await
            .unwrap());
        assert!(
            !fx.manager
                .set_unity_version("cn", "2022.3.21f1", VersionSource::Manual)
                .await
                .unwrap(),
            "equal to the configured value"
        );
        fx.manager
            .set_unity_version("cn", "2022.3.62f1", VersionSource::Manual)
            .await
            .unwrap();
        fx.manager
            .offer_app_version("cn", "6.5.0", VersionSource::Manual, false, None)
            .await
            .unwrap();

        let jp = fx.manager.region_view("jp").await.unwrap();
        assert!(jp.app_version.is_none());
        assert_eq!(jp.provider_kind, "colorful_palette");
        assert_eq!(jp.unity_version.effective, "2022.3.62f1");

        fx.manager
            .revert("cn", Some(VersionField::AppVersion))
            .await
            .unwrap();
        let cn = fx.manager.region_view("cn").await.unwrap();
        assert_eq!(cn.app_version.unwrap().effective, "6.4.0");
        assert_eq!(cn.unity_version.effective, "2022.3.62f1");
        fx.manager
            .revert("cn", Some(VersionField::UnityVersion))
            .await
            .unwrap();
        assert_eq!(
            fx.manager
                .region_view("cn")
                .await
                .unwrap()
                .unity_version
                .effective,
            "2022.3.21f1"
        );
        fx.manager.revert("jp", None).await.unwrap();
        assert!(fx
            .manager
            .region_view("jp")
            .await
            .unwrap()
            .unity_version
            .override_
            .is_none());
        assert_eq!(fx.manager.region_views().await.len(), 4);
    }

    #[tokio::test]
    async fn jobs_keep_the_version_current_at_their_creation() {
        let fx = fixture(false, false).await;
        fx.cdn_has("6.5.0");
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: true,
            mode: AssetUpdateMode::Update,
        };
        let before = fx.manager.jobs.submit(request.clone()).await.unwrap();
        fx.manager
            .offer_app_version("cn", "6.5.0", VersionSource::Registry, false, None)
            .await
            .unwrap();
        let after = fx.manager.jobs.submit(request).await.unwrap();

        assert_eq!(before.app_version.as_deref(), Some("6.4.0"));
        assert_eq!(after.app_version.as_deref(), Some("6.5.0"));
        for _ in 0..100 {
            let job = fx.manager.jobs.get(after.id).await.unwrap();
            if let Some(plan) = job.plan {
                let lookup = plan.url_preview.asset_version_lookup_url.unwrap();
                assert!(lookup.ends_with("/version/6.5.0"), "{lookup}");
                let first = fx.manager.jobs.get(before.id).await.unwrap();
                if let Some(plan) = first.plan {
                    let lookup = plan.url_preview.asset_version_lookup_url.unwrap();
                    assert!(lookup.ends_with("/version/6.4.0"), "{lookup}");
                }
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the dry-run job never planned");
    }

    #[tokio::test]
    async fn adopted_versions_submit_a_job_once_the_region_is_idle() {
        let fx = fixture(true, true).await;
        fx.registry_says("cn", "6.5.0");
        fx.registry_says("tw", "6.4.0");
        fx.cdn_has("6.5.0");
        let mut busy = crate::core::models::JobSnapshot::new(&AssetUpdateRequest {
            region: "CN".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
            mode: AssetUpdateMode::Update,
        });
        busy.status = JobStatus::Running;
        fx.manager.jobs.insert_snapshot(busy.clone()).await;

        fx.manager.poll_once().await;
        let cn = fx.status("cn").await;
        assert!(cn.pending_submit, "busy region defers the job");
        assert!(cn.last_submitted_job_id.is_none());

        fx.manager
            .jobs
            .force_status(busy.id, JobStatus::Completed)
            .await;
        fx.manager.poll_once().await;
        let cn = fx.status("cn").await;
        assert!(!cn.pending_submit);
        let job_id = cn.last_submitted_job_id.expect("job submitted");
        let job = fx.manager.jobs.get(job_id).await.unwrap();
        assert_eq!(job.app_version.as_deref(), Some("6.5.0"));
        assert!(!job.dry_run);
        assert!(fx.status("tw").await.last_submitted_job_id.is_none());
    }

    #[tokio::test]
    async fn a_rejected_submit_clears_the_pending_flag() {
        let fx = fixture(true, true).await;
        fx.manager
            .status
            .write()
            .await
            .entry("kr".to_string())
            .or_default()
            .pending_submit = true;
        fx.manager.submit_pending("kr").await;
        let kr = fx.status("kr").await;
        assert!(!kr.pending_submit);
        assert!(kr.last_submitted_job_id.is_none());
        fx.manager.submit_pending("cn").await;
        assert!(!fx.manager.status.read().await.contains_key("cn"));
    }

    #[tokio::test]
    async fn persist_failures_are_reported_and_change_nothing() {
        let fx = fixture(true, false).await;
        let blocker = fx._dir.path().join("blocker");
        std::fs::write(&blocker, "").unwrap();
        let mut config = AppConfig::clone(&fx.manager.config);
        config.app_version_sync.state_file = blocker.join("state.json").display().to_string();
        let config = Arc::new(config);
        let versions = RuntimeVersions::load(&config);
        let jobs = JobManager::with_versions(config.clone(), versions.clone());
        let manager = AppVersionManager::new(config, versions, jobs);
        fx.registry_says("cn", "6.5.0");
        fx.cdn_has("6.5.0");

        manager.poll_once().await;

        let cn = manager.status.read().await["cn"].clone();
        assert_eq!(cn.last_result, Some(SyncResult::PersistFailed));
        assert!(cn.last_error.unwrap().contains("failed to persist"));
        let view = manager.region_view("cn").await.unwrap();
        assert_eq!(view.app_version.unwrap().effective, "6.4.0");
        assert!(manager
            .set_unity_version("cn", "2022.3.62f1", VersionSource::Manual)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn the_poller_runs_on_its_interval() {
        let fx = fixture(true, false).await;
        fx.registry_says("cn", "6.4.0");
        let handle = fx.manager.spawn_poller().expect("enabled");
        for _ in 0..200 {
            if fx.manager.status.read().await.contains_key("cn") {
                handle.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        panic!("the poller never polled");
    }
}
