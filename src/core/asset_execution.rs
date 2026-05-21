use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cbc::cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};
use chrono::FixedOffset;
use opendal::Operator;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, CONNECTION, COOKIE,
    SET_COOKIE, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::core::config::{AppConfig, RegionConfig, RegionProviderConfig};
use crate::core::download_records::{
    load_download_record, load_download_record_from_storage, save_download_record,
    save_download_record_to_storage, DownloadRecord,
};
use crate::core::errors::AssetExecutionError;
use crate::core::export_pipeline::extract_unity_asset_bundle;
use crate::core::git_sync::sync_chart_hashes;
use crate::core::models::{AssetUpdateRequest, ExecutionSummary, JobPhase};
use crate::core::regions::{compile_patterns, first_match_index, matches_any};
use crate::core::retry::retry_async;
use crate::core::storage::{build_storage_operator_target, resolve_storage_template};

type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
type Aes192CbcDec = cbc::Decryptor<aes::Aes192>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum AssetCategory {
    StartApp,
    OnDemand,
    Other(String),
}

impl<'de> Deserialize<'de> for AssetCategory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Treat nil/null as Other("") — matches Go's zero-value coercion.
        let raw = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
        Ok(match raw.as_str() {
            "StartApp" | "startApp" => Self::StartApp,
            "OnDemand" | "onDemand" => Self::OnDemand,
            other => Self::Other(other.to_string()),
        })
    }
}

/// Deserializes a msgpack/JSON null or missing value as an empty String.
/// Go silently coerces nil → zero value for non-pointer types; this helper
/// mirrors that behavior for String fields.
fn de_null_as_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
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
    // nuverse regions use `crc` instead of `hash`; the server may send nil here.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBundleInfo {
    #[serde(rename = "version")]
    pub version: Option<String>,
    #[serde(rename = "os")]
    pub os: Option<String>,
    #[serde(rename = "bundles")]
    pub bundles: HashMap<String, AssetBundleDetail>,
}

#[derive(Debug, Clone)]
struct DownloadTask {
    download_path: String,
    bundle_path: String,
    bundle_hash: String,
    category: AssetCategory,
    priority: usize,
}

#[derive(Clone)]
enum DownloadRecordStore {
    Local {
        path: String,
    },
    Storage {
        provider: String,
        path: String,
        operator: Operator,
    },
}

impl DownloadRecordStore {
    async fn load(&self) -> Result<DownloadRecord, crate::core::errors::DownloadRecordError> {
        match self {
            Self::Local { path } => load_download_record(path),
            Self::Storage {
                provider,
                path,
                operator,
            } => load_download_record_from_storage(provider, operator, path).await,
        }
    }

    async fn save(
        &self,
        record: &DownloadRecord,
    ) -> Result<(), crate::core::errors::DownloadRecordError> {
        match self {
            Self::Local { path } => save_download_record(path, record),
            Self::Storage {
                provider,
                path,
                operator,
            } => save_download_record_to_storage(provider, operator, path, record).await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AssetExecutionContext {
    client: reqwest::Client,
    region_name: String,
    region: RegionConfig,
    request: AssetUpdateRequest,
    retry: crate::core::config::RetryConfig,
    runtime_cookie: Option<String>,
    resolved_asset_version: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ExecutionProgressUpdate {
    Phase { phase: JobPhase, message: String },
    DownloadsPlanned { total: usize },
    BundleStarted { bundle: String },
    BundleCompleted { bundle: String },
    BundleFailed { bundle: String, error: String },
    RecordSaved { entries: usize },
    ChartHashSyncFinished { performed: bool },
}

impl AssetExecutionContext {
    pub fn new(
        app_config: &AppConfig,
        region_name: &str,
        region: &RegionConfig,
        request: &AssetUpdateRequest,
    ) -> Result<Self, AssetExecutionError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("ProductName/134 CFNetwork/1408.0.4 Darwin/22.5.0"),
        );
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(
            ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br"),
        );
        headers.insert(
            ACCEPT_LANGUAGE,
            HeaderValue::from_static("zh-CN,zh-Hans;q=0.9"),
        );
        headers.insert(
            "X-Unity-Version",
            HeaderValue::from_str(&region.runtime.unity_version)
                .map_err(|err| AssetExecutionError::HttpClient(err.to_string()))?,
        );

        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(180))
            .pool_max_idle_per_host(100)
            .tcp_keepalive(Duration::from_secs(30));

        if let Some(proxy) = &app_config.server.proxy {
            if !proxy.is_empty() {
                builder = builder.proxy(
                    reqwest::Proxy::all(proxy)
                        .map_err(|err| AssetExecutionError::HttpClient(err.to_string()))?,
                );
            }
        }

        Ok(Self {
            client: builder
                .build()
                .map_err(|err| AssetExecutionError::HttpClient(err.to_string()))?,
            region_name: region_name.to_string(),
            region: region.clone(),
            request: request.clone(),
            retry: app_config.execution.retry.clone(),
            runtime_cookie: None,
            resolved_asset_version: request.asset_version.clone(),
        })
    }

    pub async fn execute(
        mut self,
        app_config: &AppConfig,
        progress: Option<UnboundedSender<ExecutionProgressUpdate>>,
        cancel_flag: Option<Arc<AtomicBool>>,
        workspace_id: Option<String>,
    ) -> Result<ExecutionSummary, AssetExecutionError> {
        self.ensure_not_cancelled(&cancel_flag)?;
        let record_store = self.download_record_store(app_config)?;
        let mut downloaded_assets = record_store.load().await?;

        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::FetchingAssetInfo,
                message: "fetching asset bundle info".to_string(),
            },
        );

        if self.requires_cookies() {
            self.fetch_runtime_cookies().await?;
        }

        self.ensure_not_cancelled(&cancel_flag)?;
        let info = self.fetch_asset_bundle_info().await?;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::PlanningDownloads,
                message: "building download task list".to_string(),
            },
        );
        let tasks = self.build_download_tasks(&info, &downloaded_assets);
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::DownloadsPlanned { total: tasks.len() },
        );

        if tasks.is_empty() {
            return Ok(ExecutionSummary {
                discovered_bundles: info.bundles.len(),
                queued_downloads: 0,
                completed_downloads: 0,
                failed_downloads: 0,
                updated_record_entries: 0,
                chart_hash_sync_performed: false,
            });
        }

        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut pending_save_count = 0usize;
        let batch_save_size = app_config.execution.batch_save_size;
        let semaphore = std::sync::Arc::new(Semaphore::new(app_config.concurrency.download.max(1)));
        let mut joins = JoinSet::new();
        let app_config_cloned = app_config.clone();
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::DownloadingBundles,
                message: format!("downloading {} bundle(s)", tasks.len()),
            },
        );

        for task in tasks.clone() {
            let ctx = self.clone();
            let semaphore = semaphore.clone();
            let app_config = app_config_cloned.clone();
            let progress = progress.clone();
            let cancel_flag = cancel_flag.clone();
            let workspace_id = workspace_id.clone();
            joins.spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                if cancel_flag
                    .as_ref()
                    .is_some_and(|flag| flag.load(Ordering::SeqCst))
                {
                    return (
                        task.bundle_path.clone(),
                        task.bundle_hash.clone(),
                        Err(AssetExecutionError::Cancelled),
                    );
                }
                Self::send_progress(
                    &progress,
                    ExecutionProgressUpdate::BundleStarted {
                        bundle: task.bundle_path.clone(),
                    },
                );
                let bundle_path = task.bundle_path.clone();
                let bundle_hash = task.bundle_hash.clone();
                let result = ctx
                    .download_and_export_bundle(&app_config, &task, workspace_id.as_deref())
                    .await;
                (bundle_path, bundle_hash, result)
            });
        }

        while let Some(result) = joins.join_next().await {
            let (bundle_path, bundle_hash, result) = result.expect("bundle task panicked");
            match result {
                Ok(()) => {
                    completed += 1;
                    downloaded_assets.insert(bundle_path.clone(), bundle_hash);
                    pending_save_count += 1;
                    Self::send_progress(
                        &progress,
                        ExecutionProgressUpdate::BundleCompleted {
                            bundle: bundle_path,
                        },
                    );
                    if batch_save_size > 0 && pending_save_count >= batch_save_size {
                        tracing::info!(
                            region = %self.region_name,
                            batch = pending_save_count,
                            "batch-flushing download record"
                        );
                        match record_store.save(&downloaded_assets).await {
                            Ok(()) => Self::send_progress(
                                &progress,
                                ExecutionProgressUpdate::RecordSaved {
                                    entries: downloaded_assets.len(),
                                },
                            ),
                            Err(err) => tracing::warn!(
                                region = %self.region_name,
                                error = %err,
                                "mid-run batch save of download record failed; will retry at end"
                            ),
                        }
                        pending_save_count = 0;
                    }
                }
                Err(AssetExecutionError::Cancelled) => {
                    return Err(AssetExecutionError::Cancelled);
                }
                Err(err) => {
                    failed += 1;
                    Self::send_progress(
                        &progress,
                        ExecutionProgressUpdate::BundleFailed {
                            bundle: bundle_path.clone(),
                            error: err.to_string(),
                        },
                    );
                    tracing::warn!(
                        region = %self.region_name,
                        bundle = %bundle_path,
                        error = %err,
                        "bundle processing failed"
                    );
                }
            }
        }

        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::PersistingState,
                message: "saving downloaded asset record".to_string(),
            },
        );
        self.ensure_not_cancelled(&cancel_flag)?;
        record_store.save(&downloaded_assets).await?;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::RecordSaved {
                entries: downloaded_assets.len(),
            },
        );
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::SyncingChartHashes,
                message: "syncing chart hashes".to_string(),
            },
        );
        let chart_hash_sync_performed = sync_chart_hashes(
            &app_config.git_sync.chart_hashes,
            &self.region_name,
            &downloaded_assets,
            app_config.server.proxy.as_deref(),
            &app_config.execution.retry,
            false,
        )?
        .is_some();
        self.cleanup_bundle_workspace(app_config, workspace_id.as_deref(), failed == 0)
            .await;
        self.cleanup_staged_exports(app_config, workspace_id.as_deref(), failed == 0)
            .await;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::ChartHashSyncFinished {
                performed: chart_hash_sync_performed,
            },
        );

        Ok(ExecutionSummary {
            discovered_bundles: info.bundles.len(),
            queued_downloads: tasks.len(),
            completed_downloads: completed,
            failed_downloads: failed,
            updated_record_entries: downloaded_assets.len(),
            chart_hash_sync_performed,
        })
    }

    fn ensure_not_cancelled(
        &self,
        cancel_flag: &Option<Arc<AtomicBool>>,
    ) -> Result<(), AssetExecutionError> {
        if cancel_flag
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
        {
            Err(AssetExecutionError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn download_record_store(
        &self,
        app_config: &AppConfig,
    ) -> Result<DownloadRecordStore, AssetExecutionError> {
        if let Some(storage) = &self.region.paths.downloaded_asset_record_storage {
            let provider = storage.provider.trim();
            if provider.is_empty() {
                return Err(AssetExecutionError::InvalidDownloadRecordStorage {
                    region: self.region_name.clone(),
                    message: "provider is empty".to_string(),
                });
            }

            let path = normalize_download_record_storage_path(&storage.path, &self.region_name)
                .ok_or_else(|| AssetExecutionError::InvalidDownloadRecordStorage {
                    region: self.region_name.clone(),
                    message: "path is empty".to_string(),
                })?;
            let target =
                build_storage_operator_target(&app_config.storage, provider, &self.region_name)?;

            return Ok(DownloadRecordStore::Storage {
                provider: target.provider,
                path,
                operator: target.operator,
            });
        }

        self.region
            .paths
            .downloaded_asset_record_file
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(|path| DownloadRecordStore::Local {
                path: path.to_string(),
            })
            .ok_or_else(|| AssetExecutionError::MissingDownloadRecordPath {
                region: self.region_name.clone(),
            })
    }

    fn send_progress(
        sender: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        update: ExecutionProgressUpdate,
    ) {
        if let Some(sender) = sender {
            let _ = sender.send(update);
        }
    }

    fn requires_cookies(&self) -> bool {
        match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                required_cookies, ..
            } => *required_cookies,
            RegionProviderConfig::Nuverse {
                required_cookies, ..
            } => *required_cookies,
        }
    }

    async fn fetch_runtime_cookies(&mut self) -> Result<(), AssetExecutionError> {
        let url = match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                cookie_bootstrap_url,
                ..
            }
            | RegionProviderConfig::Nuverse {
                cookie_bootstrap_url,
                ..
            } => cookie_bootstrap_url.clone().unwrap_or_else(|| {
                "https://issue.sekai.colorfulpalette.org/api/signature".to_string()
            }),
        };
        self.runtime_cookie = retry_async(
            &self.retry,
            "cookie bootstrap",
            |_| async {
                let response = self.client.post(&url).send().await?;
                if response.status().is_success() {
                    Ok(response
                        .headers()
                        .get(SET_COOKIE)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string))
                } else {
                    Err(AssetExecutionError::HttpStatus {
                        url: url.clone(),
                        status: response.status().as_u16(),
                    })
                }
            },
            is_retryable_http_error,
        )
        .await?;
        Ok(())
    }

    async fn fetch_asset_bundle_info(&mut self) -> Result<AssetBundleInfo, AssetExecutionError> {
        let url = self.render_asset_info_url().await?;
        let body = self.get_with_retry(&url).await?;
        decrypt_asset_bundle_info(
            self.region.crypto.aes_key_hex.as_deref().ok_or_else(|| {
                AssetExecutionError::MissingCryptoConfig {
                    region: self.region_name.clone(),
                }
            })?,
            self.region.crypto.aes_iv_hex.as_deref().ok_or_else(|| {
                AssetExecutionError::MissingCryptoConfig {
                    region: self.region_name.clone(),
                }
            })?,
            &body,
        )
    }

    async fn render_asset_info_url(&mut self) -> Result<String, AssetExecutionError> {
        match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                asset_info_url_template,
                profile,
                profile_hashes,
                ..
            } => {
                let asset_version = self.request.asset_version.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let asset_hash = self.request.asset_hash.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let profile_hash = profile_hashes.get(profile).ok_or_else(|| {
                    AssetExecutionError::MissingProfileHash {
                        region: self.region_name.clone(),
                        profile: profile.clone(),
                    }
                })?;
                Ok(asset_info_url_template
                    .replace("{env}", profile)
                    .replace("{hash}", profile_hash)
                    .replace("{asset_version}", asset_version)
                    .replace("{asset_hash}", asset_hash)
                    + &time_arg_jst())
            }
            RegionProviderConfig::Nuverse {
                asset_version_url,
                app_version,
                asset_info_url_template,
                ..
            } => {
                // For nuverse, always fetch the version from asset_version_url.
                // The incoming request.asset_version is intentionally ignored here
                // to match Go reference behavior.
                let version_url = asset_version_url.replace("{app_version}", app_version);
                let resolved_version =
                    String::from_utf8_lossy(&self.get_with_retry(&version_url).await?)
                        .trim()
                        .to_string();
                self.resolved_asset_version = Some(resolved_version.clone());
                Ok(asset_info_url_template
                    .replace("{app_version}", app_version)
                    .replace("{asset_version}", &resolved_version)
                    + &time_arg_jst())
            }
        }
    }

    fn render_bundle_url(&self, task: &DownloadTask) -> Result<String, AssetExecutionError> {
        match &self.region.provider {
            RegionProviderConfig::ColorfulPalette {
                asset_bundle_url_template,
                profile,
                profile_hashes,
                ..
            } => {
                let asset_version = self.request.asset_version.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let asset_hash = self.request.asset_hash.as_deref().ok_or_else(|| {
                    AssetExecutionError::MissingAssetVersionOrHash {
                        region: self.region_name.clone(),
                    }
                })?;
                let profile_hash = profile_hashes.get(profile).ok_or_else(|| {
                    AssetExecutionError::MissingProfileHash {
                        region: self.region_name.clone(),
                        profile: profile.clone(),
                    }
                })?;

                Ok(asset_bundle_url_template
                    .replace("{bundle_path}", &task.download_path)
                    .replace("{asset_version}", asset_version)
                    .replace("{asset_hash}", asset_hash)
                    .replace("{env}", profile)
                    .replace("{hash}", profile_hash)
                    + &time_arg_jst())
            }
            RegionProviderConfig::Nuverse {
                asset_bundle_url_template,
                app_version,
                ..
            } => {
                let asset_version = self
                    .resolved_asset_version
                    .as_deref()
                    .unwrap_or("<resolved-asset-version>");
                Ok(asset_bundle_url_template
                    .replace("{bundle_path}", &task.download_path)
                    .replace("{app_version}", app_version)
                    .replace("{asset_version}", asset_version)
                    + &time_arg_jst())
            }
        }
    }

    async fn get_with_retry(&self, url: &str) -> Result<Vec<u8>, AssetExecutionError> {
        retry_async(
            &self.retry,
            "http get",
            |_| async {
                let mut request = self.client.get(url);
                if let Some(cookie) = &self.runtime_cookie {
                    request = request.header(COOKIE, cookie);
                }
                match request.send().await {
                    Ok(response) if response.status().is_success() => {
                        Ok(response.bytes().await?.to_vec())
                    }
                    Ok(response) => Err(AssetExecutionError::HttpStatus {
                        url: url.to_string(),
                        status: response.status().as_u16(),
                    }),
                    Err(err) => Err(AssetExecutionError::Http(err)),
                }
            },
            is_retryable_http_error,
        )
        .await
    }

    fn build_download_tasks(
        &self,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
    ) -> Vec<DownloadTask> {
        let skip_patterns = compile_patterns(&self.region.filters.skip);
        let priority_patterns = compile_patterns(&self.region.filters.priority);
        let start_app_patterns = compile_patterns(&self.region.filters.start_app);
        let on_demand_patterns = compile_patterns(&self.region.filters.on_demand);
        let mut tasks = Vec::new();

        for (bundle_name, detail) in &info.bundles {
            if matches_any(&skip_patterns, bundle_name) {
                continue;
            }
            let category_patterns = match &detail.category {
                AssetCategory::StartApp => &start_app_patterns,
                AssetCategory::OnDemand => &on_demand_patterns,
                AssetCategory::Other(_) => continue,
            };
            if category_patterns.is_empty() || !matches_any(category_patterns, bundle_name) {
                continue;
            }

            let bundle_hash = match self.region.provider {
                RegionProviderConfig::Nuverse { .. } => detail.crc.to_string(),
                RegionProviderConfig::ColorfulPalette { .. } => detail.hash.clone(),
            };

            if downloaded_assets
                .get(bundle_name)
                .is_some_and(|existing| existing == &bundle_hash)
            {
                continue;
            }

            let priority = first_match_index(&priority_patterns, bundle_name).unwrap_or(usize::MAX);
            tasks.push(DownloadTask {
                download_path: download_path_for_region(&self.region.provider, bundle_name, detail),
                bundle_path: bundle_name.clone(),
                bundle_hash,
                category: detail.category.clone(),
                priority,
            });
        }

        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.bundle_path.cmp(&b.bundle_path))
        });
        tasks
    }

    async fn download_and_export_bundle(
        &self,
        app_config: &AppConfig,
        task: &DownloadTask,
        workspace_id: Option<&str>,
    ) -> Result<(), AssetExecutionError> {
        let bundle_url = self.render_bundle_url(task)?;
        let body = self.get_with_retry(&bundle_url).await?;
        let deobfuscated = deobfuscate(&body);

        let temp_file = bundle_temp_file_path(
            app_config,
            &self.region_name,
            workspace_id,
            &task.bundle_path,
        );
        if let Some(parent) = temp_file.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                AssetExecutionError::CreateTempDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        std::fs::write(&temp_file, deobfuscated).map_err(|source| {
            AssetExecutionError::WriteTempFile {
                path: temp_file.clone(),
                source,
            }
        })?;

        let category = match task.category {
            AssetCategory::StartApp => "StartApp",
            AssetCategory::OnDemand => "OnDemand",
            AssetCategory::Other(_) => "OnDemand",
        };
        let export_output_dir = bundle_export_output_dir(
            app_config,
            &self.region_name,
            workspace_id,
            self.region.paths.asset_save_dir.as_deref().map(Path::new),
        )
        .ok_or_else(|| AssetExecutionError::MissingAssetSaveDir {
            region: self.region_name.clone(),
        })?;
        let export_result = extract_unity_asset_bundle(
            app_config,
            &self.region_name,
            &self.region,
            &temp_file,
            &task.bundle_path,
            &export_output_dir,
            category,
        )
        .await;
        if export_result.is_ok() && app_config.execution.workspace.cleanup_on_success {
            let _ = std::fs::remove_file(&temp_file);
        }
        export_result.map(|_| ()).map_err(Into::into)
    }

    async fn cleanup_staged_exports(
        &self,
        app_config: &AppConfig,
        workspace_id: Option<&str>,
        completed_without_bundle_failures: bool,
    ) {
        if !completed_without_bundle_failures
            || !self.region.upload.enabled
            || !app_config.execution.workspace.cleanup_exports_on_success
        {
            return;
        }

        let Some(root) = staged_export_root(app_config, &self.region_name, workspace_id) else {
            return;
        };
        match tokio::fs::remove_dir_all(&root).await {
            Ok(()) => tracing::info!(
                region = %self.region_name,
                path = %root.display(),
                "cleaned staged export workspace"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                region = %self.region_name,
                path = %root.display(),
                error = %err,
                "failed to clean staged export workspace"
            ),
        }
    }

    async fn cleanup_bundle_workspace(
        &self,
        app_config: &AppConfig,
        workspace_id: Option<&str>,
        completed_without_bundle_failures: bool,
    ) {
        if !completed_without_bundle_failures || !app_config.execution.workspace.cleanup_on_success
        {
            return;
        }

        let Some(root) = bundle_workspace_root(app_config, workspace_id) else {
            return;
        };
        match tokio::fs::remove_dir_all(&root).await {
            Ok(()) => tracing::info!(
                region = %self.region_name,
                path = %root.display(),
                "cleaned bundle workspace"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                region = %self.region_name,
                path = %root.display(),
                error = %err,
                "failed to clean bundle workspace"
            ),
        }
    }
}

fn normalize_download_record_storage_path(raw_path: &str, region_name: &str) -> Option<String> {
    let resolved = resolve_storage_template(raw_path, region_name);
    let normalized = resolved.trim().trim_matches('/').replace('\\', "/");
    (!normalized.is_empty()).then_some(normalized)
}

fn bundle_temp_file_path(
    app_config: &AppConfig,
    region_name: &str,
    workspace_id: Option<&str>,
    bundle_path: &str,
) -> PathBuf {
    if let Some(root) = bundle_workspace_root(app_config, workspace_id) {
        root.join(region_name).join(bundle_path)
    } else {
        workspace_root(app_config)
            .join(region_name)
            .join(bundle_path)
    }
}

fn workspace_root(app_config: &AppConfig) -> PathBuf {
    app_config
        .execution
        .workspace
        .work_dir
        .as_deref()
        .map(str::trim)
        .filter(|work_dir| !work_dir.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn bundle_workspace_root(app_config: &AppConfig, workspace_id: Option<&str>) -> Option<PathBuf> {
    let workspace_id = workspace_id
        .map(str::trim)
        .filter(|workspace_id| !workspace_id.is_empty())?;
    Some(workspace_root(app_config).join("jobs").join(workspace_id))
}

fn bundle_export_output_dir(
    app_config: &AppConfig,
    region_name: &str,
    workspace_id: Option<&str>,
    asset_save_dir: Option<&Path>,
) -> Option<PathBuf> {
    staged_export_root(app_config, region_name, workspace_id)
        .or_else(|| asset_save_dir.map(Path::to_path_buf))
}

fn staged_export_root(
    app_config: &AppConfig,
    region_name: &str,
    workspace_id: Option<&str>,
) -> Option<PathBuf> {
    let workspace_id = workspace_id
        .map(str::trim)
        .filter(|workspace_id| !workspace_id.is_empty())?;
    let template = app_config
        .execution
        .workspace
        .export_dir
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())?;
    let resolved = resolve_storage_template(template, region_name);

    if resolved.contains("{job_id}") {
        Some(PathBuf::from(resolved.replace("{job_id}", workspace_id)))
    } else {
        Some(PathBuf::from(resolved).join(workspace_id))
    }
}

pub async fn fetch_live_asset_bundle_info(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    request: &AssetUpdateRequest,
) -> Result<AssetBundleInfo, AssetExecutionError> {
    let mut context = AssetExecutionContext::new(app_config, region_name, region, request)?;
    if context.requires_cookies() {
        context.fetch_runtime_cookies().await?;
    }
    context.fetch_asset_bundle_info().await
}

fn is_retryable_http_error(err: &AssetExecutionError) -> bool {
    match err {
        AssetExecutionError::Http(_) => true,
        AssetExecutionError::HttpStatus { status, .. } => *status >= 500,
        _ => false,
    }
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

pub fn deobfuscate(data: &[u8]) -> Vec<u8> {
    const SIMPLE: [u8; 4] = [0x20, 0x00, 0x00, 0x00];
    const XOR_HEADER: [u8; 4] = [0x10, 0x00, 0x00, 0x00];

    if data.starts_with(&SIMPLE) {
        return data[4..].to_vec();
    }

    if data.starts_with(&XOR_HEADER) {
        let body = &data[4..];
        if body.len() < 128 {
            return body.to_vec();
        }

        let mut header = vec![0u8; 128];
        let pattern = [0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00];
        for idx in 0..128 {
            header[idx] = body[idx] ^ pattern[idx % pattern.len()];
        }
        let mut output = header;
        output.extend_from_slice(&body[128..]);
        return output;
    }

    data.to_vec()
}

pub fn should_download_bundle(
    region: &RegionConfig,
    bundle_name: &str,
    category: &AssetCategory,
) -> bool {
    let compiled = match category {
        AssetCategory::StartApp => compile_patterns(&region.filters.start_app),
        AssetCategory::OnDemand => compile_patterns(&region.filters.on_demand),
        AssetCategory::Other(_) => return false,
    };
    if compiled.is_empty() {
        return false;
    }
    matches_any(&compiled, bundle_name)
}

fn download_path_for_region(
    provider: &RegionProviderConfig,
    bundle_name: &str,
    detail: &AssetBundleDetail,
) -> String {
    match provider {
        RegionProviderConfig::ColorfulPalette { .. } => bundle_name.to_string(),
        RegionProviderConfig::Nuverse { .. } => detail
            .download_path
            .as_ref()
            .map(|prefix| format!("{prefix}/{bundle_name}"))
            .unwrap_or_else(|| bundle_name.to_string()),
    }
}

fn time_arg_jst() -> String {
    let tz = FixedOffset::east_opt(9 * 3600).unwrap();
    format!(
        "?t={}",
        chrono::Utc::now().with_timezone(&tz).format("%Y%m%d%H%M%S")
    )
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::header::{COOKIE, SET_COOKIE};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Router;
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, DownloadRecordStorageConfig, GitSyncConfig, RegionConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig, RuntimeWorkspaceConfig,
        StorageConfig, StorageProviderConfig,
    };
    use crate::core::download_records::load_download_record;
    use crate::core::models::AssetUpdateRequest;

    use super::{
        bundle_export_output_dir, bundle_temp_file_path, decrypt_asset_bundle_info, deobfuscate,
        should_download_bundle, AssetBundleDetail, AssetBundleInfo, AssetCategory,
        AssetExecutionContext,
    };

    type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
    const TEST_AES_KEY_HEX: &str = "00112233445566778899aabbccddeeff";
    const TEST_AES_IV_HEX: &str = "0102030405060708090a0b0c0d0e0f10";

    fn test_region(provider: RegionProviderConfig) -> RegionConfig {
        RegionConfig {
            enabled: true,
            provider,
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some("./Data/jp-assets".to_string()),
                downloaded_asset_record_file: Some(
                    "./Data/jp-assets/downloaded_assets.json".to_string(),
                ),
                downloaded_asset_record_storage: None,
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: vec!["^ond/".to_string()],
                skip: vec!["^skip/".to_string()],
                priority: vec!["^start/a".to_string(), "^ond/".to_string()],
            },
            ..RegionConfig::default()
        }
    }

    fn encrypt_asset_info(info: &AssetBundleInfo) -> Vec<u8> {
        let key = hex::decode(TEST_AES_KEY_HEX).unwrap();
        let iv = hex::decode(TEST_AES_IV_HEX).unwrap();
        let payload = rmp_serde::to_vec_named(info).unwrap();
        let mut padded = payload.clone();
        let original_len = padded.len();
        let padding = 16 - (original_len % 16);
        padded.resize(original_len + padding, 0);
        let encrypted = Aes128CbcEnc::new_from_slices(&key, &iv)
            .unwrap()
            .encrypt_padded::<Pkcs7>(&mut padded, original_len)
            .unwrap()
            .to_vec();
        encrypted
    }

    #[tokio::test]
    async fn download_record_store_uses_opendal_provider_when_configured() {
        let temp = tempdir().unwrap();
        let state_root = temp.path().join("state");
        std::fs::create_dir_all(&state_root).unwrap();
        let mut region = test_region(RegionProviderConfig::default());
        region.paths.downloaded_asset_record_file = None;
        region.paths.downloaded_asset_record_storage = Some(DownloadRecordStorageConfig {
            provider: "state".to_string(),
            path: "records/{region}/downloaded_assets.json".to_string(),
        });

        let config = AppConfig {
            storage: StorageConfig {
                providers: vec![StorageProviderConfig {
                    name: Some("state".to_string()),
                    scheme: "fs".to_string(),
                    root: Some(state_root.to_string_lossy().into_owned()),
                    ..StorageProviderConfig::default()
                }],
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let store = executor.download_record_store(&config).unwrap();
        let mut record = BTreeMap::new();
        record.insert("start/a".to_string(), "hash-a".to_string());

        store.save(&record).await.unwrap();
        let loaded = store.load().await.unwrap();

        assert_eq!(loaded, record);
        assert!(state_root
            .join("records")
            .join("jp")
            .join("downloaded_assets.json")
            .exists());
    }

    #[test]
    fn bundle_temp_file_path_uses_configured_workspace_and_ignores_blank() {
        let config = AppConfig {
            execution: crate::core::config::ExecutionConfig {
                workspace: RuntimeWorkspaceConfig {
                    work_dir: Some("/tmp/haruki-work".to_string()),
                    cleanup_on_success: false,
                    export_dir: None,
                    cleanup_exports_on_success: true,
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            ..AppConfig::default()
        };

        assert_eq!(
            bundle_temp_file_path(&config, "jp", None, "music/a"),
            std::path::Path::new("/tmp/haruki-work")
                .join("jp")
                .join("music/a")
        );
        assert_eq!(
            bundle_temp_file_path(&config, "jp", Some("job-1"), "music/a"),
            std::path::Path::new("/tmp/haruki-work")
                .join("jobs")
                .join("job-1")
                .join("jp")
                .join("music/a")
        );

        let blank_config = AppConfig {
            execution: crate::core::config::ExecutionConfig {
                workspace: RuntimeWorkspaceConfig {
                    work_dir: Some(" ".to_string()),
                    cleanup_on_success: true,
                    export_dir: None,
                    cleanup_exports_on_success: true,
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            ..AppConfig::default()
        };

        assert!(bundle_temp_file_path(&blank_config, "jp", None, "music/a")
            .starts_with(std::env::temp_dir()));
    }

    #[test]
    fn bundle_export_output_dir_supports_job_scoped_staging() {
        let config = AppConfig {
            execution: crate::core::config::ExecutionConfig {
                workspace: RuntimeWorkspaceConfig {
                    export_dir: Some("/tmp/haruki-exports/{region}/{job_id}".to_string()),
                    ..RuntimeWorkspaceConfig::default()
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            ..AppConfig::default()
        };

        assert_eq!(
            bundle_export_output_dir(
                &config,
                "jp",
                Some("job-1"),
                Some(std::path::Path::new("/persistent/jp"))
            ),
            Some(std::path::Path::new("/tmp/haruki-exports/jp/job-1").to_path_buf())
        );
        assert_eq!(
            bundle_export_output_dir(&config, "jp", Some("job-1"), None),
            Some(std::path::Path::new("/tmp/haruki-exports/jp/job-1").to_path_buf())
        );
        assert_eq!(
            bundle_export_output_dir(
                &config,
                "jp",
                None,
                Some(std::path::Path::new("/persistent/jp"))
            ),
            Some(std::path::Path::new("/persistent/jp").to_path_buf())
        );
        assert_eq!(bundle_export_output_dir(&config, "jp", None, None), None);
    }

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
    fn download_filters_match_go_logic() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "".to_string(),
            asset_bundle_url_template: "".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });

        assert!(should_download_bundle(
            &region,
            "start/a",
            &AssetCategory::StartApp
        ));
        assert!(should_download_bundle(
            &region,
            "ond/a",
            &AssetCategory::OnDemand
        ));
        assert!(!should_download_bundle(
            &region,
            "other/a",
            &AssetCategory::OnDemand
        ));
    }

    #[tokio::test]
    async fn non_dry_run_can_fetch_asset_info_and_update_download_record() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
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

        let app = Router::new()
            .route(
                "/info/production/abc/1/hash",
                get({
                    let encrypted = encrypted.clone();
                    move || async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            encrypted.clone(),
                        )
                    }
                }),
            )
            .route(
                "/bundle/start/a",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                        Body::from(vec![
                            0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N', b'D', b'L', b'E',
                        ]),
                    )
                }),
            )
            .route("/signature", post(|| async { "ok" }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{addr}/info/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
                downloaded_asset_record_storage: None,
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            tools: crate::core::config::ToolsConfig {
                ffmpeg_path: "ffmpeg".to_string(),
                asset_studio_cli_path: None,
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor.execute(&config, None, None, None).await.unwrap();
        assert_eq!(summary.completed_downloads, 1);

        let record = load_download_record(&record_file).unwrap();
        assert_eq!(record.get("start/a").map(String::as_str), Some("hash-a"));
    }

    #[tokio::test]
    async fn non_dry_run_can_use_staged_export_without_asset_save_dir() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let work_dir = temp.path().join("work");
        let export_dir = temp
            .path()
            .join("exports")
            .join("{region}")
            .join("{job_id}");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
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

        let app = Router::new()
            .route(
                "/info/production/abc/1/hash",
                get({
                    let encrypted = encrypted.clone();
                    move || async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            encrypted.clone(),
                        )
                    }
                }),
            )
            .route(
                "/bundle/start/a",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                        Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                    )
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{addr}/info/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: None,
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
                downloaded_asset_record_storage: None,
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            execution: crate::core::config::ExecutionConfig {
                workspace: RuntimeWorkspaceConfig {
                    work_dir: Some(work_dir.to_string_lossy().into_owned()),
                    cleanup_on_success: true,
                    export_dir: Some(export_dir.to_string_lossy().into_owned()),
                    cleanup_exports_on_success: true,
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            tools: crate::core::config::ToolsConfig {
                ffmpeg_path: "ffmpeg".to_string(),
                asset_studio_cli_path: None,
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor
            .execute(&config, None, None, Some("job-1".to_string()))
            .await
            .unwrap();
        assert_eq!(summary.completed_downloads, 1);

        let record = load_download_record(&record_file).unwrap();
        assert_eq!(record.get("start/a").map(String::as_str), Some("hash-a"));
    }

    #[tokio::test]
    async fn required_cookies_are_forwarded_and_nuverse_uses_resolved_version() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "ond/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "ond/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
                    category: AssetCategory::OnDemand,
                    crc: 888,
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: Some("download-root".to_string()),
                },
            )]),
        };
        let encrypted = encrypt_asset_info(&info);
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let version_hits = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/version/5.2.0",
                get({
                    let version_hits = version_hits.clone();
                    move || {
                        let version_hits = version_hits.clone();
                        async move {
                            version_hits.fetch_add(1, Ordering::SeqCst);
                            "20250321"
                        }
                    }
                }),
            )
            .route(
                "/info/5.2.0/20250321",
                get({
                    let encrypted = encrypted.clone();
                    move || async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            encrypted.clone(),
                        )
                    }
                }),
            )
            .route(
                "/bundle/download-root/ond/a",
                get({
                    let cookie_seen = cookie_seen.clone();
                    move |headers: HeaderMap| {
                        let cookie_seen = cookie_seen.clone();
                        async move {
                            if headers
                                .get(COOKIE)
                                .and_then(|value| value.to_str().ok())
                                .is_some_and(|value| value.contains("session=abc"))
                            {
                                cookie_seen.store(true, Ordering::SeqCst);
                            }
                            (
                                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                                Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                            )
                        }
                    }
                }),
            )
            .route(
                "/signature",
                post(|| async move { ([(SET_COOKIE.as_str(), "session=abc; Path=/")], "ok") }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::Nuverse {
                asset_version_url: format!("http://{addr}/version/{{app_version}}"),
                app_version: "5.2.0".to_string(),
                asset_info_url_template: format!(
                    "http://{addr}/info/{{app_version}}/{{asset_version}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                required_cookies: true,
                cookie_bootstrap_url: Some(format!("http://{addr}/signature")),
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
                downloaded_asset_record_storage: None,
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: Vec::new(),
                on_demand: vec!["^ond/".to_string()],
                skip: Vec::new(),
                priority: vec!["^ond/".to_string()],
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("cn".to_string(), region.clone());
        let config = AppConfig {
            regions,
            tools: crate::core::config::ToolsConfig {
                ffmpeg_path: "ffmpeg".to_string(),
                asset_studio_cli_path: None,
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            concurrency: crate::core::config::ConcurrencyConfig {
                download: 2,
                ..crate::core::config::ConcurrencyConfig::default()
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
        };

        let executor = AssetExecutionContext::new(&config, "cn", &region, &request).unwrap();
        let summary = executor.execute(&config, None, None, None).await.unwrap();
        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(version_hits.load(Ordering::SeqCst), 1);
        assert!(cookie_seen.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn http_fetch_retries_on_503_then_succeeds() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
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
        let info_hits = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/info/production/abc/1/hash",
                get({
                    let encrypted = encrypted.clone();
                    let info_hits = info_hits.clone();
                    move || {
                        let encrypted = encrypted.clone();
                        let info_hits = info_hits.clone();
                        async move {
                            let attempt = info_hits.fetch_add(1, Ordering::SeqCst);
                            if attempt < 2 {
                                (
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                    Body::from("retry"),
                                )
                            } else {
                                (axum::http::StatusCode::OK, Body::from(encrypted.clone()))
                            }
                        }
                    }
                }),
            )
            .route(
                "/bundle/start/a",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                        Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                    )
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{addr}/info/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
                downloaded_asset_record_storage: None,
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            execution: crate::core::config::ExecutionConfig {
                retry: crate::core::config::RetryConfig {
                    attempts: 3,
                    initial_backoff_ms: 1,
                    max_backoff_ms: 1,
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor.execute(&config, None, None, None).await.unwrap();

        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(info_hits.load(Ordering::SeqCst), 3);
    }
}
