use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cbc::cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};
use chrono::FixedOffset;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, COOKIE, SET_COOKIE,
    USER_AGENT,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::core::cleanup::remove_file_if_exists;
use crate::core::config::{AppConfig, AssetHttpVersion, RegionConfig, RegionProviderConfig};
use crate::core::download_records::{load_download_record, save_download_record, DownloadRecord};
use crate::core::errors::{format_reqwest_error_chain, AssetExecutionError};
use crate::core::export_pipeline::{
    export_unity_asset_bundle_payloads, flush_pending_native_image_writes,
    post_process_exported_files, NativeObjectReadPlanStats, UnityAssetBundlePayloadExport,
};
use crate::core::git_sync::sync_chart_hashes;
use crate::core::models::{AssetUpdateRequest, ExecutionSummary, JobPhase};
use crate::core::regions::{compile_patterns, first_match_index, matches_any};
use crate::core::retry::retry_async;

type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
type Aes192CbcDec = cbc::Decryptor<aes::Aes192>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

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
        // Treat nil/null as Other("") — matches Go's zero-value coercion.
        let raw = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
        Ok(match raw.as_str() {
            "StartApp" | "startApp" => Self::StartApp,
            "OnDemand" | "onDemand" => Self::OnDemand,
            "Live_pv" | "live_pv" | "LivePv" | "livePv" => Self::LivePv,
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
    file_size: i64,
    priority: usize,
    export_payloads: bool,
    stage_haruki_3d: bool,
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

#[derive(Debug, Clone, Default)]
pub struct Haruki3dExportSummary {
    pub matched_bundles: usize,
    pub downloaded_bundles: usize,
}

#[derive(Debug, Clone)]
pub enum ExecutionProgressUpdate {
    Phase {
        phase: JobPhase,
        message: String,
    },
    DownloadsPlanned {
        total: usize,
    },
    BundleStarted {
        bundle: String,
    },
    BundleDownloaded {
        bundle: String,
        bytes: usize,
        elapsed_ms: u128,
    },
    BundleFetchDetails {
        bundle: String,
        source: String,
        cache_read_ms: Option<u128>,
        network_download_ms: Option<u128>,
        cache_write_ms: Option<u128>,
    },
    BundleDeobfuscated {
        bundle: String,
        elapsed_ms: u128,
    },
    BundleTempWritten {
        bundle: String,
        elapsed_ms: u128,
    },
    BundleExported {
        bundle: String,
        elapsed_ms: u128,
    },
    BundleFfiExportPhases {
        bundle: String,
        phase_ms: HashMap<String, u64>,
    },
    BundleFfiSkippedObjectReads {
        bundle: String,
        count: usize,
    },
    BundleFfiObjectReadPlan {
        bundle: String,
        plan: NativeObjectReadPlanStats,
    },
    SchedulerTelemetry {
        bundle: Option<String>,
        phase_ms: HashMap<String, u64>,
    },
    BundleCompleted {
        bundle: String,
    },
    BundleFailed {
        bundle: String,
        error: String,
    },
    RecordSaved {
        entries: usize,
    },
    ChartHashSyncFinished {
        performed: bool,
    },
}

struct NativeBundlePostProcessJob {
    bundle_path: String,
    bundle_hash: String,
    export_started: Instant,
    payload_export: UnityAssetBundlePayloadExport,
    backlog_wait_ms: u128,
    _backlog_permit: Option<OwnedSemaphorePermit>,
    _memory_permit: Option<OwnedSemaphorePermit>,
}

enum BundleWorkOutput {
    Completed,
    NativePostProcess(Box<NativeBundlePostProcessJob>),
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
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
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
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(180))
            .pool_max_idle_per_host(100)
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .tcp_keepalive(Duration::from_secs(30));
        if app_config.server.asset_http_version == AssetHttpVersion::Http1 {
            builder = builder.http1_only();
        }

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
    ) -> Result<ExecutionSummary, AssetExecutionError> {
        self.ensure_not_cancelled(&cancel_flag)?;
        let record_path = self
            .region
            .paths
            .downloaded_asset_record_file
            .clone()
            .ok_or_else(|| AssetExecutionError::MissingAssetSaveDir {
                region: self.region_name.clone(),
            })?;
        let mut downloaded_assets = load_download_record(&record_path)?;
        let haruki_3d_downloaded_assets = self
            .haruki_3d_download_record_path()
            .map(load_download_record)
            .transpose()?
            .unwrap_or_default();

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
        let tasks =
            self.build_download_tasks(&info, &downloaded_assets, &haruki_3d_downloaded_assets)?;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::DownloadsPlanned { total: tasks.len() },
        );

        if tasks.is_empty() {
            tracing::info!(
                region = %self.region_name,
                discovered = info.bundles.len(),
                "no new assets to download"
            );
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
        let mut completed_standard = 0usize;
        let mut failed = 0usize;
        let mut pending_save_count = 0usize;
        let batch_save_size = app_config.execution.batch_save_size;
        let concurrency = app_config.effective_concurrency();
        let download_concurrency = concurrency.download.max(1);
        let media_encode_concurrency = concurrency
            .audio_encode
            .max(concurrency.video_encode)
            .max(concurrency.media_encode)
            .max(1);
        let post_process_concurrency = if concurrency.post_process == 0 {
            media_encode_concurrency
        } else {
            concurrency.post_process
        }
        .max(1);
        let semaphore = std::sync::Arc::new(Semaphore::new(download_concurrency));
        let memory_limiter = BundleMemoryLimiter::from_config(app_config);
        let post_process_semaphore = std::sync::Arc::new(Semaphore::new(post_process_concurrency));
        let post_process_backlog_capacity =
            post_process_backlog_capacity(download_concurrency, post_process_concurrency);
        let post_process_backlog_semaphore =
            std::sync::Arc::new(Semaphore::new(post_process_backlog_capacity));
        let post_process_queued = std::sync::Arc::new(AtomicUsize::new(0));
        let post_process_active = std::sync::Arc::new(AtomicUsize::new(0));
        let mut joins = JoinSet::new();
        let mut post_process_joins = JoinSet::new();
        let app_config_cloned = app_config.clone();
        let haruki_3d_work_root = self.haruki_3d_work_asset_root();
        let bundle_hash_index_path = self.haruki_3d_bundle_hash_index_path();
        let bundle_hash_index = bundle_hash_index_path
            .as_ref()
            .map(load_download_record)
            .transpose()?
            .map(|record| Arc::new(std::sync::Mutex::new(record)));
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::DownloadingBundles,
                message: format!("downloading {} bundle(s)", tasks.len()),
            },
        );
        tracing::info!(
            region = %self.region_name,
            queued = tasks.len(),
            download_concurrency,
            audio_encode_concurrency = concurrency.audio_encode,
            video_encode_concurrency = concurrency.video_encode,
            post_process_concurrency,
            memory_limit_bytes = memory_limiter.limit_bytes(),
            "starting asset bundle processing"
        );

        for task in tasks.clone() {
            let ctx = self.clone();
            let semaphore = semaphore.clone();
            let memory_limiter = memory_limiter.clone();
            let post_process_backlog_semaphore = post_process_backlog_semaphore.clone();
            let app_config = app_config_cloned.clone();
            let progress = progress.clone();
            let cancel_flag = cancel_flag.clone();
            let haruki_3d_work_root = haruki_3d_work_root.clone();
            let bundle_hash_index = bundle_hash_index.clone();
            joins.spawn(async move {
                let download_slot_wait_started = Instant::now();
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                let download_slot_wait_ms = download_slot_wait_started.elapsed().as_millis();
                if cancel_flag
                    .as_ref()
                    .is_some_and(|flag| flag.load(Ordering::SeqCst))
                {
                    return (
                        task.bundle_path.clone(),
                        task.bundle_hash.clone(),
                        task.export_payloads,
                        Err(AssetExecutionError::Cancelled),
                    );
                }
                let memory_wait_started = Instant::now();
                let mut memory_permit =
                    memory_limiter.acquire(task.file_size.max(0) as usize).await;
                let memory_wait_ms = memory_wait_started.elapsed().as_millis();
                Self::send_progress(
                    &progress,
                    ExecutionProgressUpdate::BundleStarted {
                        bundle: task.bundle_path.clone(),
                    },
                );
                let bundle_path = task.bundle_path.clone();
                let bundle_hash = task.bundle_hash.clone();
                let record_standard = task.export_payloads;
                let result = match ctx
                    .download_and_export_bundle_payloads(
                        &app_config,
                        &task,
                        &progress,
                        haruki_3d_work_root.as_deref(),
                        bundle_hash_index.as_ref(),
                    )
                    .await
                {
                    Ok(Some(mut job)) => {
                        let backlog_wait_started = Instant::now();
                        let backlog_permit = post_process_backlog_semaphore
                            .acquire_owned()
                            .await
                            .expect("post-process backlog semaphore closed");
                        let backlog_wait_ms = backlog_wait_started.elapsed().as_millis();
                        job.backlog_wait_ms = backlog_wait_ms;
                        job._backlog_permit = Some(backlog_permit);
                        job._memory_permit = memory_permit.take();
                        Ok(BundleWorkOutput::NativePostProcess(Box::new(job)))
                    }
                    Ok(None) => Ok(BundleWorkOutput::Completed),
                    Err(error) => Err(error),
                };
                let mut phase_ms = HashMap::new();
                phase_ms.insert(
                    "scheduler.download_slot_wait".to_string(),
                    download_slot_wait_ms.min(u128::from(u64::MAX)) as u64,
                );
                phase_ms.insert(
                    "scheduler.memory_wait".to_string(),
                    memory_wait_ms.min(u128::from(u64::MAX)) as u64,
                );
                Self::send_progress(
                    &progress,
                    ExecutionProgressUpdate::SchedulerTelemetry {
                        bundle: Some(task.bundle_path.clone()),
                        phase_ms,
                    },
                );
                (bundle_path, bundle_hash, record_standard, result)
            });
        }

        while !joins.is_empty() || !post_process_joins.is_empty() {
            // If cancellation was requested, stop scheduling/awaiting more (expensive) post-process
            // work and fall through to persist the record before returning Cancelled, rather than
            // draining every already-queued bundle first.
            if self.ensure_not_cancelled(&cancel_flag).is_err() {
                break;
            }
            tokio::select! {
                Some(result) = joins.join_next(), if !joins.is_empty() => {
                    let (bundle_path, bundle_hash, record_standard, result) = match result {
                        Ok(tuple) => tuple,
                        Err(join_err) => {
                            // A download/export sub-task panicked or was aborted. Count it as a
                            // failed bundle instead of unwinding the orchestrator (which would
                            // leave the owning job wedged in Running forever).
                            failed += 1;
                            tracing::error!(
                                region = %self.region_name,
                                error = %join_err,
                                "bundle download/export task panicked or was aborted; counting as failed"
                            );
                            continue;
                        }
                    };
                    match result {
                        Ok(BundleWorkOutput::Completed) => {
                            Self::record_completed_bundle(
                                &progress,
                                &record_path,
                                &mut downloaded_assets,
                                &mut completed,
                                &mut completed_standard,
                                &mut pending_save_count,
                                batch_save_size,
                                &self.region_name,
                                bundle_hash_index_path.as_ref(),
                                bundle_hash_index.as_ref(),
                                bundle_path,
                                bundle_hash,
                                record_standard,
                            )
                            .await;
                        }
                        Ok(BundleWorkOutput::NativePostProcess(job)) => {
                            let app_config = app_config_cloned.clone();
                            let region = self.region.clone();
                            let region_name = self.region_name.clone();
                            let progress = progress.clone();
                            let semaphore = post_process_semaphore.clone();
                            let post_process_queued = post_process_queued.clone();
                            let post_process_active = post_process_active.clone();
                            let queued = post_process_queued.fetch_add(1, Ordering::Relaxed) + 1;
                            Self::send_progress(
                                &progress,
                                ExecutionProgressUpdate::SchedulerTelemetry {
                                    bundle: Some(job.bundle_path.clone()),
                                    phase_ms: HashMap::from([
                                        (
                                            "scheduler.post_process_queued".to_string(),
                                            queued as u64,
                                        ),
                                        (
                                            "scheduler.post_process_backlog_capacity".to_string(),
                                            post_process_backlog_capacity as u64,
                                        ),
                                        (
                                            "scheduler.post_process_concurrency".to_string(),
                                            post_process_concurrency as u64,
                                        ),
                                    ]),
                                },
                            );
                            post_process_joins.spawn(async move {
                                let queue_started = Instant::now();
                                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                                let queue_wait_ms = queue_started.elapsed().as_millis();
                                post_process_queued.fetch_sub(1, Ordering::Relaxed);
                                let active = post_process_active.fetch_add(1, Ordering::Relaxed) + 1;
                                let bundle_path = job.bundle_path.clone();
                                let bundle_hash = job.bundle_hash.clone();
                                let result = Self::finish_native_bundle_post_process(
                                    &app_config,
                                    &region_name,
                                    &region,
                                    &progress,
                                    *job,
                                    queue_wait_ms,
                                )
                                .await;
                                post_process_active.fetch_sub(1, Ordering::Relaxed);
                                Self::send_progress(
                                    &progress,
                                    ExecutionProgressUpdate::SchedulerTelemetry {
                                        bundle: Some(bundle_path.clone()),
                                        phase_ms: HashMap::from([
                                            (
                                                "scheduler.post_process_active_peak".to_string(),
                                                active as u64,
                                            ),
                                            (
                                                "scheduler.post_process_queue_wait".to_string(),
                                                queue_wait_ms.min(u128::from(u64::MAX)) as u64,
                                            ),
                                        ]),
                                    },
                                );
                                (bundle_path, bundle_hash, true, result)
                            });
                        }
                        Err(AssetExecutionError::Cancelled) => {
                            // Stop scheduling further work but fall through to persist the record so
                            // already-completed bundles aren't re-downloaded on the next run.
                            break;
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
                Some(result) = post_process_joins.join_next(), if !post_process_joins.is_empty() => {
                    let (bundle_path, bundle_hash, record_standard, result) = match result {
                        Ok(tuple) => tuple,
                        Err(join_err) => {
                            // Post-process sub-task panicked or was aborted: count as failed
                            // rather than re-panicking the orchestrator.
                            failed += 1;
                            tracing::error!(
                                region = %self.region_name,
                                error = %join_err,
                                "bundle post-process task panicked or was aborted; counting as failed"
                            );
                            continue;
                        }
                    };
                    match result {
                        Ok(()) => {
                            Self::record_completed_bundle(
                                &progress,
                                &record_path,
                                &mut downloaded_assets,
                                &mut completed,
                                &mut completed_standard,
                                &mut pending_save_count,
                                batch_save_size,
                                &self.region_name,
                                bundle_hash_index_path.as_ref(),
                                bundle_hash_index.as_ref(),
                                bundle_path,
                                bundle_hash,
                                record_standard,
                            )
                            .await;
                        }
                        Err(AssetExecutionError::Cancelled) => {
                            // Stop scheduling further work but fall through to persist the record so
                            // already-completed bundles aren't re-downloaded on the next run.
                            break;
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
                                "bundle post-process failed"
                            );
                        }
                    }
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
        // Persist the record BEFORE honoring cancellation: every bundle that finished already ran
        // its export/upload side effects, so dropping them here would force a redundant re-run.
        Self::save_bundle_hash_index_checkpoint(
            bundle_hash_index_path.as_ref(),
            bundle_hash_index.as_ref(),
        )
        .await?;
        Self::save_download_record_on_blocking_thread(
            record_path.clone(),
            downloaded_assets.clone(),
        )
        .await?;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::RecordSaved {
                entries: downloaded_assets.len(),
            },
        );
        // Honor cancellation now that the record is durable (this skips the heavier chart sync).
        self.ensure_not_cancelled(&cancel_flag)?;
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
            // Number of record entries actually added/updated this run (not the whole record size),
            // keeping the semantics consistent with the empty-task early-return path above.
            updated_record_entries: completed_standard,
            chart_hash_sync_performed,
        })
    }

    pub async fn prefetch_asset_bundles(
        mut self,
        app_config: &AppConfig,
        progress: Option<UnboundedSender<ExecutionProgressUpdate>>,
        cancel_flag: Option<Arc<AtomicBool>>,
    ) -> Result<ExecutionSummary, AssetExecutionError> {
        self.ensure_not_cancelled(&cancel_flag)?;
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
                message: "building prefetch task list".to_string(),
            },
        );
        let tasks = self.build_raw_bundle_filter_tasks(&info);
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::DownloadsPlanned { total: tasks.len() },
        );

        if tasks.is_empty() {
            tracing::info!(
                region = %self.region_name,
                discovered = info.bundles.len(),
                "no assets matched prefetch filters"
            );
            return Ok(ExecutionSummary {
                discovered_bundles: info.bundles.len(),
                queued_downloads: 0,
                completed_downloads: 0,
                failed_downloads: 0,
                updated_record_entries: 0,
                chart_hash_sync_performed: false,
            });
        }

        let semaphore = Arc::new(Semaphore::new(
            app_config.effective_concurrency().download.max(1),
        ));
        let memory_limiter = BundleMemoryLimiter::from_config(app_config);
        let mut joins = JoinSet::new();
        let app_config_cloned = app_config.clone();
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::DownloadingBundles,
                message: format!("prefetching {} bundle(s)", tasks.len()),
            },
        );
        tracing::info!(
            region = %self.region_name,
            queued = tasks.len(),
            memory_limit_bytes = memory_limiter.limit_bytes(),
            "starting asset bundle prefetch"
        );

        for task in tasks.clone() {
            let ctx = self.clone();
            let semaphore = semaphore.clone();
            let memory_limiter = memory_limiter.clone();
            let app_config = app_config_cloned.clone();
            let progress = progress.clone();
            let cancel_flag = cancel_flag.clone();
            joins.spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                if cancel_flag
                    .as_ref()
                    .is_some_and(|flag| flag.load(Ordering::SeqCst))
                {
                    return (
                        task.bundle_path.clone(),
                        Err(AssetExecutionError::Cancelled),
                    );
                }
                let _memory_permit = memory_limiter.acquire(task.file_size.max(0) as usize).await;
                Self::send_progress(
                    &progress,
                    ExecutionProgressUpdate::BundleStarted {
                        bundle: task.bundle_path.clone(),
                    },
                );
                let bundle_path = task.bundle_path.clone();
                let result = ctx.prefetch_bundle(&app_config, &task, &progress).await;
                (bundle_path, result)
            });
        }

        let mut completed = 0usize;
        let mut failed = 0usize;
        while let Some(result) = joins.join_next().await {
            let (bundle_path, result) = match result {
                Ok(tuple) => tuple,
                Err(join_err) => {
                    // Prefetch sub-task panicked or was aborted: count as failed instead of
                    // unwinding the run.
                    failed += 1;
                    tracing::error!(
                        region = %self.region_name,
                        error = %join_err,
                        "bundle prefetch task panicked or was aborted; counting as failed"
                    );
                    continue;
                }
            };
            match result {
                Ok(()) => {
                    completed += 1;
                    Self::send_progress(
                        &progress,
                        ExecutionProgressUpdate::BundleCompleted {
                            bundle: bundle_path,
                        },
                    );
                }
                Err(AssetExecutionError::Cancelled) => return Err(AssetExecutionError::Cancelled),
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
                        "bundle prefetch failed"
                    );
                }
            }
        }

        Ok(ExecutionSummary {
            discovered_bundles: info.bundles.len(),
            queued_downloads: tasks.len(),
            completed_downloads: completed,
            failed_downloads: failed,
            updated_record_entries: 0,
            chart_hash_sync_performed: false,
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

    fn send_progress(
        sender: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        update: ExecutionProgressUpdate,
    ) {
        if let Some(sender) = sender {
            let _ = sender.send(update);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_completed_bundle(
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        record_path: &str,
        downloaded_assets: &mut DownloadRecord,
        completed: &mut usize,
        completed_standard: &mut usize,
        pending_save_count: &mut usize,
        batch_save_size: usize,
        region_name: &str,
        bundle_hash_index_path: Option<&PathBuf>,
        bundle_hash_index: Option<&Arc<std::sync::Mutex<DownloadRecord>>>,
        bundle_path: String,
        bundle_hash: String,
        record_standard: bool,
    ) {
        *completed += 1;
        if record_standard {
            *completed_standard += 1;
            downloaded_assets.insert(bundle_path.clone(), bundle_hash);
        }
        *pending_save_count += 1;
        if progress.is_none() {
            tracing::info!(
                region = %region_name,
                bundle = %bundle_path,
                completed = *completed,
                "bundle completed"
            );
        }
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleCompleted {
                bundle: bundle_path,
            },
        );
        if batch_save_size > 0 && *pending_save_count >= batch_save_size {
            tracing::info!(
                region = %region_name,
                batch = *pending_save_count,
                "batch-flushing download record"
            );
            let save_result = match Self::save_bundle_hash_index_checkpoint(
                bundle_hash_index_path,
                bundle_hash_index,
            )
            .await
            {
                Ok(()) => {
                    Self::save_download_record_on_blocking_thread(
                        record_path.to_string(),
                        downloaded_assets.clone(),
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            match save_result {
                Ok(()) => Self::send_progress(
                    progress,
                    ExecutionProgressUpdate::RecordSaved {
                        entries: downloaded_assets.len(),
                    },
                ),
                Err(err) => tracing::warn!(
                    region = %region_name,
                    error = %err,
                    "mid-run batch save of download record failed; will retry at end"
                ),
            }
            *pending_save_count = 0;
        }
    }

    async fn save_download_record_on_blocking_thread(
        record_path: String,
        downloaded_assets: DownloadRecord,
    ) -> Result<(), AssetExecutionError> {
        tokio::task::spawn_blocking(move || save_download_record(record_path, &downloaded_assets))
            .await
            .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?
            .map_err(AssetExecutionError::from)
    }

    async fn save_bundle_hash_index_checkpoint(
        path: Option<&PathBuf>,
        index: Option<&Arc<std::sync::Mutex<DownloadRecord>>>,
    ) -> Result<(), AssetExecutionError> {
        let (Some(path), Some(index)) = (path, index) else {
            return Ok(());
        };
        let record = index
            .lock()
            .map_err(|_| {
                AssetExecutionError::BlockingTask("bundle hash index lock poisoned".to_string())
            })?
            .clone();
        Self::save_download_record_on_blocking_thread(path.to_string_lossy().into_owned(), record)
            .await
    }

    async fn finish_native_bundle_post_process(
        app_config: &AppConfig,
        region_name: &str,
        region: &RegionConfig,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        mut job: NativeBundlePostProcessJob,
        queue_wait_ms: u128,
    ) -> Result<(), AssetExecutionError> {
        let image_app_config = app_config.clone();
        let pending_image_writes = std::mem::take(&mut job.payload_export.pending_image_writes);
        let image_phase_ms = tokio::task::spawn_blocking(move || {
            flush_pending_native_image_writes(&image_app_config, pending_image_writes)
        })
        .await
        .map_err(
            |source| crate::core::errors::ExportPipelineError::WorkerPanic {
                worker: "native image flush".to_string(),
                message: source.to_string(),
            },
        )??;
        let post_process_summary = post_process_exported_files(
            app_config,
            region_name,
            region,
            &job.payload_export.export_path,
            &job.payload_export.export_root,
            job.payload_export.native_scoped_post_process,
            &job.payload_export.native_written_files,
            job.payload_export.native_acb_sources,
        )
        .await?;

        let mut phase_ms = job.payload_export.ffi_export_phase_ms;
        phase_ms.extend(image_phase_ms);
        phase_ms.extend(post_process_summary.post_process_phase_ms);
        phase_ms.insert(
            "post_process.queue_wait".to_string(),
            queue_wait_ms.min(u128::from(u64::MAX)) as u64,
        );
        phase_ms.insert(
            "scheduler.post_process_backlog_wait".to_string(),
            job.backlog_wait_ms.min(u128::from(u64::MAX)) as u64,
        );
        phase_ms.insert(
            "scheduler.bundle_active_before_post_process".to_string(),
            job.export_started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        );
        if !phase_ms.is_empty() {
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleFfiExportPhases {
                    bundle: job.bundle_path.clone(),
                    phase_ms,
                },
            );
        }
        if !job.payload_export.ffi_skipped_object_reads.is_empty() {
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleFfiSkippedObjectReads {
                    bundle: job.bundle_path.clone(),
                    count: job.payload_export.ffi_skipped_object_reads.len(),
                },
            );
        }
        if !job.payload_export.ffi_object_read_plan.is_empty() {
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleFfiObjectReadPlan {
                    bundle: job.bundle_path.clone(),
                    plan: job.payload_export.ffi_object_read_plan,
                },
            );
        }
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleExported {
                bundle: job.bundle_path,
                elapsed_ms: job.export_started.elapsed().as_millis(),
            },
        );
        Ok(())
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
                let response = self.client.post(&url).send().await.map_err(|err| {
                    tracing::warn!(
                        url,
                        error = %format_reqwest_error_chain(&err),
                        "HTTP request failed"
                    );
                    AssetExecutionError::Http(err)
                })?;
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
                    Err(err) => {
                        tracing::warn!(
                            url,
                            error = %format_reqwest_error_chain(&err),
                            "HTTP request failed"
                        );
                        Err(AssetExecutionError::Http(err))
                    }
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
        haruki_3d_downloaded_assets: &DownloadRecord,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        self.append_haruki_3d_download_tasks(
            self.build_standard_download_tasks(info, downloaded_assets),
            info,
            haruki_3d_downloaded_assets,
        )
    }

    fn build_standard_download_tasks(
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
                AssetCategory::OnDemand | AssetCategory::LivePv => &on_demand_patterns,
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
                file_size: detail.file_size,
                priority,
                export_payloads: true,
                stage_haruki_3d: false,
            });
        }

        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.bundle_path.cmp(&b.bundle_path))
        });
        tasks
    }

    fn append_haruki_3d_download_tasks(
        &self,
        mut tasks: Vec<DownloadTask>,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        tasks.extend(self.build_haruki_3d_download_tasks(info, downloaded_assets)?);
        tasks.sort_by(|a, b| {
            a.bundle_path
                .cmp(&b.bundle_path)
                .then_with(|| a.priority.cmp(&b.priority))
        });
        tasks.dedup_by(|a, b| {
            if a.bundle_path != b.bundle_path {
                return false;
            }
            b.export_payloads |= a.export_payloads;
            b.stage_haruki_3d |= a.stage_haruki_3d;
            true
        });
        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.bundle_path.cmp(&b.bundle_path))
        });
        Ok(tasks)
    }

    async fn download_and_export_bundle_payloads(
        &self,
        app_config: &AppConfig,
        task: &DownloadTask,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        haruki_3d_work_root: Option<&Path>,
        bundle_hash_index: Option<&Arc<std::sync::Mutex<DownloadRecord>>>,
    ) -> Result<Option<NativeBundlePostProcessJob>, AssetExecutionError> {
        let asset_save_dir = self.region.paths.asset_save_dir.clone().ok_or_else(|| {
            AssetExecutionError::MissingAssetSaveDir {
                region: self.region_name.clone(),
            }
        })?;
        let bundle_url = self.render_bundle_url(task)?;
        let download_started = Instant::now();
        let network_started = Instant::now();
        let body = self.get_with_retry(&bundle_url).await?;
        let network_download_ms = Some(network_started.elapsed().as_millis());
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: task.bundle_path.clone(),
                bytes: body.len(),
                elapsed_ms: download_started.elapsed().as_millis(),
            },
        );
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleFetchDetails {
                bundle: task.bundle_path.clone(),
                source: "network".to_string(),
                cache_read_ms: None,
                network_download_ms,
                cache_write_ms: None,
            },
        );

        // The bundle path originates from the (untrusted) asset-info server. Validate it before it
        // is used to build any filesystem path, so a name like "../../etc/foo" can't escape the
        // temp/export directories.
        let safe_bundle_path = validate_relative_bundle_path(&task.bundle_path)?.to_path_buf();
        let raw_bundle_target = if self.matches_raw_bundle_filters(&task.bundle_path) {
            Some(self.raw_bundle_output_path(&asset_save_dir, &task.bundle_path)?)
        } else {
            None
        };
        let haruki_3d_work_target = if task.stage_haruki_3d {
            match haruki_3d_work_root {
                Some(work_root) => Some(raw_bundle_output_path(work_root, &task.bundle_path)?),
                None => None,
            }
        } else {
            None
        };
        let temp_file = std::env::temp_dir()
            .join(&self.region_name)
            .join(&safe_bundle_path);

        // Deobfuscation (a full-buffer transform), retention/staging, and temp-file writes are
        // CPU/blocking work. Run them on a blocking thread so the async runtime workers (which also
        // serve HTTP and other jobs) aren't stalled while many bundles process concurrently.
        let blocking_started = Instant::now();
        let temp_file_for_blocking = task.export_payloads.then(|| temp_file.clone());
        let bundle_hash_index = bundle_hash_index.cloned();
        let bundle_hash_index_key = bundle_hash_index_key(&task.bundle_path)?;
        tokio::task::spawn_blocking(move || -> Result<(), AssetExecutionError> {
            let deobfuscated = deobfuscate(&body);
            if let Some(raw_path) = raw_bundle_target {
                Self::write_raw_bundle(&raw_path, &deobfuscated)?;
            }
            if let Some(work_path) = haruki_3d_work_target {
                Self::write_haruki_3d_work_bundle(&work_path, &deobfuscated)?;
                if let Some(index) = bundle_hash_index {
                    let digest = hex::encode(Sha256::digest(&deobfuscated));
                    index
                        .lock()
                        .map_err(|_| {
                            AssetExecutionError::BlockingTask(
                                "bundle hash index lock poisoned".to_string(),
                            )
                        })?
                        .insert(bundle_hash_index_key, digest);
                }
            }
            if let Some(temp_file) = temp_file_for_blocking {
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
                        path: temp_file,
                        source,
                    }
                })?;
            }
            Ok(())
        })
        .await
        .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))??;
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDeobfuscated {
                bundle: task.bundle_path.clone(),
                elapsed_ms: blocking_started.elapsed().as_millis(),
            },
        );
        if task.export_payloads {
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleTempWritten {
                    bundle: task.bundle_path.clone(),
                    elapsed_ms: blocking_started.elapsed().as_millis(),
                },
            );
        }

        if !task.export_payloads {
            return Ok(None);
        }

        let category = match task.category {
            AssetCategory::StartApp => "StartApp",
            AssetCategory::OnDemand | AssetCategory::LivePv => "OnDemand",
            AssetCategory::Other(_) => "OnDemand",
        };
        let export_started = Instant::now();
        let payload_export = export_unity_asset_bundle_payloads(
            app_config,
            &self.region,
            &temp_file,
            &task.bundle_path,
            Path::new(&asset_save_dir),
            category,
        )
        .await;
        let _ = remove_file_if_exists(&temp_file);

        Ok(Some(NativeBundlePostProcessJob {
            bundle_path: task.bundle_path.clone(),
            bundle_hash: task.bundle_hash.clone(),
            export_started,
            payload_export: payload_export?,
            backlog_wait_ms: 0,
            _backlog_permit: None,
            _memory_permit: None,
        }))
    }

    async fn prefetch_bundle(
        &self,
        app_config: &AppConfig,
        task: &DownloadTask,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
    ) -> Result<(), AssetExecutionError> {
        let asset_save_dir = self.region.paths.asset_save_dir.clone().ok_or_else(|| {
            AssetExecutionError::MissingAssetSaveDir {
                region: self.region_name.clone(),
            }
        })?;
        let bundle_url = self.render_bundle_url(task)?;
        let download_started = Instant::now();
        let network_started = Instant::now();
        let body = self.get_with_retry(&bundle_url).await?;
        let network_download_ms = Some(network_started.elapsed().as_millis());
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: task.bundle_path.clone(),
                bytes: body.len(),
                elapsed_ms: download_started.elapsed().as_millis(),
            },
        );
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleFetchDetails {
                bundle: task.bundle_path.clone(),
                source: "network".to_string(),
                cache_read_ms: None,
                network_download_ms,
                cache_write_ms: None,
            },
        );

        let deobfuscate_started = Instant::now();
        let deobfuscated = deobfuscate(&body);
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDeobfuscated {
                bundle: task.bundle_path.clone(),
                elapsed_ms: deobfuscate_started.elapsed().as_millis(),
            },
        );

        let raw_path = self.raw_bundle_output_path(&asset_save_dir, &task.bundle_path)?;
        Self::write_raw_bundle(&raw_path, &deobfuscated)?;
        tracing::debug!(
            region = %self.region_name,
            bundle = %task.bundle_path,
            output = %raw_path.display(),
            http_version = ?app_config.server.asset_http_version,
            "prefetched raw asset bundle"
        );
        Ok(())
    }

    fn matches_raw_bundle_filters(&self, bundle_path: &str) -> bool {
        let Some(raw_bundles) = self.region.export.raw_bundles.as_ref() else {
            return false;
        };
        let include_patterns = compile_patterns(&raw_bundles.include);
        let exclude_patterns = compile_patterns(&raw_bundles.exclude);
        (include_patterns.is_empty() || matches_any(&include_patterns, bundle_path))
            && !matches_any(&exclude_patterns, bundle_path)
    }

    fn matches_haruki_3d_filters(&self, bundle_path: &str) -> bool {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled || haruki_3d.include.is_empty() {
            return false;
        }
        let include_patterns = compile_patterns(&haruki_3d.include);
        let exclude_patterns = compile_patterns(&haruki_3d.exclude);
        matches_any(&include_patterns, bundle_path) && !matches_any(&exclude_patterns, bundle_path)
    }

    fn haruki_3d_work_asset_root(&self) -> Option<PathBuf> {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled {
            return None;
        }
        let run_id = self
            .resolved_asset_version
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("current")
            .replace(['/', '\\', ':'], "_");
        Some(
            self.haruki_3d_state_root()?
                .join(run_id)
                .join("AssetBundles"),
        )
    }

    fn haruki_3d_state_root(&self) -> Option<PathBuf> {
        let haruki_3d = &self.region.export.haruki_3d;
        if !haruki_3d.enabled {
            return None;
        }
        Some(Path::new(&Self::haruki_3d_work_dir(haruki_3d)).join(&self.region_name))
    }

    fn haruki_3d_download_record_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("downloaded_assets.json"))
    }

    fn haruki_3d_bundle_hash_index_path(&self) -> Option<PathBuf> {
        self.haruki_3d_state_root()
            .map(|root| root.join("bundle_sha256.json"))
    }

    fn haruki_3d_work_dir(haruki_3d: &crate::core::config::Haruki3dExportConfig) -> String {
        if !haruki_3d.work_dir.trim().is_empty() {
            haruki_3d.work_dir.clone()
        } else {
            haruki_3d.staging_dir.clone()
        }
    }

    fn raw_bundle_output_path(
        &self,
        asset_save_dir: &str,
        bundle_path: &str,
    ) -> Result<PathBuf, AssetExecutionError> {
        let root = self
            .region
            .export
            .raw_bundles
            .as_ref()
            .and_then(|raw_bundles| raw_bundles.output_dir.as_deref())
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(asset_save_dir).join("AssetBundles"));
        raw_bundle_output_path(&root, bundle_path)
    }

    fn write_raw_bundle(path: &Path, deobfuscated: &[u8]) -> Result<(), AssetExecutionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                AssetExecutionError::CreateRawBundleDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        std::fs::write(path, deobfuscated).map_err(|source| AssetExecutionError::WriteRawBundle {
            path: path.to_path_buf(),
            source,
        })
    }

    fn write_haruki_3d_work_bundle(
        path: &Path,
        deobfuscated: &[u8],
    ) -> Result<(), AssetExecutionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                AssetExecutionError::CreateHaruki3dStagingDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        std::fs::write(path, deobfuscated).map_err(|source| {
            AssetExecutionError::WriteHaruki3dStagingBundle {
                path: path.to_path_buf(),
                source,
            }
        })
    }

    pub async fn run_haruki_3d_background_export(
        mut self,
        _app_config: &AppConfig,
        progress: Option<UnboundedSender<ExecutionProgressUpdate>>,
        cancel_flag: Option<Arc<AtomicBool>>,
    ) -> Result<Haruki3dExportSummary, AssetExecutionError> {
        let haruki_3d = self.region.export.haruki_3d.clone();
        if !haruki_3d.enabled {
            return Ok(Haruki3dExportSummary::default());
        }
        self.ensure_not_cancelled(&cancel_flag)?;
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::FetchingAssetInfo,
                message: "fetching asset bundle info for Haruki 3D export".to_string(),
            },
        );
        if self.requires_cookies() {
            self.fetch_runtime_cookies().await?;
        }
        let info = self.fetch_asset_bundle_info().await?;
        let tasks = self.build_haruki_3d_tasks(&info);
        let record_path = self.haruki_3d_download_record_path().ok_or_else(|| {
            AssetExecutionError::BlockingTask("3D download record path is unavailable".to_string())
        })?;
        let downloaded_assets = load_download_record(&record_path)?;
        let pending_tasks: Vec<_> = tasks
            .iter()
            .filter(|task| {
                downloaded_assets
                    .get(&task.bundle_path)
                    .is_none_or(|hash| hash != &task.bundle_hash)
            })
            .collect();
        let pending_paths: HashSet<_> = pending_tasks
            .iter()
            .map(|task| task.bundle_path.as_str())
            .collect();
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::DownloadsPlanned {
                total: pending_tasks.len(),
            },
        );
        let asset_root = self.haruki_3d_work_asset_root();
        let Some(asset_root) = asset_root else {
            return Ok(Haruki3dExportSummary::default());
        };
        let work_run_dir = asset_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| asset_root.clone());

        if pending_tasks.is_empty() && Path::new(&haruki_3d.manifest_file).is_file() {
            let catalog_args = Self::build_haruki_3d_runtime_catalog_command(&haruki_3d);
            if let Err(error) = self
                .run_haruki_3d_exporter_stage(&haruki_3d, &catalog_args, &progress)
                .await
            {
                if haruki_3d.cleanup_work_dir_after_failure {
                    Self::remove_haruki_3d_work_dir(&work_run_dir)?;
                }
                return Err(error);
            }
            if haruki_3d.cleanup_work_dir_after_success {
                Self::remove_haruki_3d_work_dir(&work_run_dir)?;
            }
            return Ok(Haruki3dExportSummary {
                matched_bundles: tasks.len(),
                downloaded_bundles: 0,
            });
        }

        for task in &pending_tasks {
            self.ensure_not_cancelled(&cancel_flag)?;
            Self::send_progress(
                &progress,
                ExecutionProgressUpdate::BundleStarted {
                    bundle: task.bundle_path.clone(),
                },
            );
            let output_path = raw_bundle_output_path(&asset_root, &task.bundle_path)?;
            if output_path.exists() {
                Self::send_progress(
                    &progress,
                    ExecutionProgressUpdate::BundleCompleted {
                        bundle: task.bundle_path.clone(),
                    },
                );
                continue;
            }
            return Err(AssetExecutionError::MissingHaruki3dStagingBundle { path: output_path });
        }

        for task in &tasks {
            if pending_paths.contains(task.bundle_path.as_str()) {
                continue;
            }
            let output_path = raw_bundle_output_path(&asset_root, &task.bundle_path)?;
            if !output_path.exists() {
                Self::write_haruki_3d_work_bundle(&output_path, &[])?;
            }
        }
        let sparse_input_marker = asset_root.join(".haruki-sparse-input");
        if pending_tasks.len() < tasks.len() {
            Self::write_haruki_3d_work_bundle(&sparse_input_marker, &[])?;
        } else if sparse_input_marker.exists() {
            std::fs::remove_file(&sparse_input_marker).map_err(|source| {
                AssetExecutionError::RemoveHaruki3dStagingDir {
                    path: sparse_input_marker.clone(),
                    source,
                }
            })?;
        }

        let bundle_hash_index_path = self.haruki_3d_bundle_hash_index_path().ok_or_else(|| {
            AssetExecutionError::BlockingTask(
                "3D bundle hash index path is unavailable".to_string(),
            )
        })?;
        let exporter_commands = Self::build_haruki_3d_exporter_commands(
            &haruki_3d,
            &asset_root,
            &bundle_hash_index_path,
        );

        for args in exporter_commands {
            if let Err(error) = self
                .run_haruki_3d_exporter_stage(&haruki_3d, &args, &progress)
                .await
            {
                if haruki_3d.cleanup_work_dir_after_failure {
                    Self::remove_haruki_3d_work_dir(&work_run_dir)?;
                }
                return Err(error);
            }
        }

        let catalog_args = Self::build_haruki_3d_runtime_catalog_command(&haruki_3d);
        if let Err(error) = self
            .run_haruki_3d_exporter_stage(&haruki_3d, &catalog_args, &progress)
            .await
        {
            if haruki_3d.cleanup_work_dir_after_failure {
                Self::remove_haruki_3d_work_dir(&work_run_dir)?;
            }
            return Err(error);
        }

        let completed_record = tasks
            .iter()
            .map(|task| (task.bundle_path.clone(), task.bundle_hash.clone()))
            .collect();
        save_download_record(&record_path, &completed_record)?;

        if haruki_3d.cleanup_work_dir_after_success {
            Self::remove_haruki_3d_work_dir(&work_run_dir)?;
        }
        Ok(Haruki3dExportSummary {
            matched_bundles: tasks.len(),
            downloaded_bundles: pending_tasks.len(),
        })
    }

    async fn run_haruki_3d_exporter_stage(
        &self,
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
        args: &[String],
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
    ) -> Result<(), AssetExecutionError> {
        let stage = args.first().map(String::as_str).unwrap_or("unknown");
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::Exporting3dRuntime,
                message: format!("running Haruki 3D exporter: {stage}"),
            },
        );
        let exporter_started = Instant::now();
        let output = tokio::process::Command::new(&haruki_3d.exporter_path)
            .args(args)
            .output()
            .await
            .map_err(|source| AssetExecutionError::Haruki3dExporterSpawn {
                program: haruki_3d.exporter_path.clone(),
                source,
            })?;
        if !output.status.success() {
            return Err(AssetExecutionError::Haruki3dExporterFailed {
                program: haruki_3d.exporter_path.clone(),
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        tracing::info!(
            region = %self.region_name,
            %stage,
            elapsed_ms = exporter_started.elapsed().as_millis(),
            "Haruki 3D exporter stage completed"
        );
        let metrics = exporter_metric_lines(&output.stdout);
        if !metrics.is_empty() {
            tracing::info!(region = %self.region_name, %metrics, "Haruki 3D exporter metrics");
        }
        tracing::debug!(
            region = %self.region_name,
            stdout = %String::from_utf8_lossy(&output.stdout).trim(),
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "Haruki 3D exporter stage output"
        );
        Ok(())
    }

    fn build_haruki_3d_runtime_catalog_command(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
    ) -> Vec<String> {
        vec![
            "--emit-runtime-role-catalog".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
        ]
    }

    fn build_haruki_3d_exporter_commands(
        haruki_3d: &crate::core::config::Haruki3dExportConfig,
        asset_root: &Path,
        bundle_hash_index: &Path,
    ) -> Vec<Vec<String>> {
        let asset_root_arg = asset_root.to_string_lossy().to_string();
        let model_texture_args = || {
            vec![
                "--convert-model-textures".to_string(),
                haruki_3d.convert_model_textures.to_string(),
            ]
        };
        let registry_args = [
            "--emit-costume-registries".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--asset-root".to_string(),
            asset_root_arg.clone(),
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
        ]
        .into_iter()
        .chain(model_texture_args())
        .collect();
        let mut part_args: Vec<String> = [
            "--emit-part-packages".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--asset-root".to_string(),
            asset_root_arg.clone(),
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
            "--manifest".to_string(),
            haruki_3d.manifest_file.clone(),
            "--part-package-process-concurrency".to_string(),
            haruki_3d.process_concurrency.to_string(),
        ]
        .into_iter()
        .chain(model_texture_args())
        .collect();
        if !haruki_3d.shared_content_store.trim().is_empty() {
            part_args.push("--shared-content-store".to_string());
            part_args.push(haruki_3d.shared_content_store.clone());
        }
        if !haruki_3d.compiled_content_store.trim().is_empty() {
            part_args.push("--compiled-content-store".to_string());
            part_args.push(haruki_3d.compiled_content_store.clone());
        }
        part_args.push("--bundle-hash-index".to_string());
        part_args.push(bundle_hash_index.to_string_lossy().into_owned());
        let mut exporter_commands = vec![registry_args, part_args];
        let mut role_args = vec![
            "--emit-role-runtimes".to_string(),
            "--master".to_string(),
            haruki_3d.master_dir.clone(),
            "--asset-root".to_string(),
            asset_root_arg,
            "--out".to_string(),
            haruki_3d.output_dir.clone(),
        ];
        role_args.push("--part-package-process-concurrency".to_string());
        role_args.push(haruki_3d.process_concurrency.to_string());
        for id in &haruki_3d.role_character3d_ids {
            role_args.push("--role-character3d-id".to_string());
            role_args.push(id.to_string());
        }
        role_args.extend(model_texture_args());
        exporter_commands.push(role_args);
        exporter_commands
    }

    fn build_haruki_3d_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        self.build_haruki_3d_filter_tasks(info)
    }

    fn build_raw_bundle_filter_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        let mut tasks = Vec::new();
        for (bundle_name, detail) in &info.bundles {
            if !self.matches_raw_bundle_filters(bundle_name) {
                continue;
            }
            let bundle_hash = match self.region.provider {
                RegionProviderConfig::Nuverse { .. } => detail.crc.to_string(),
                RegionProviderConfig::ColorfulPalette { .. } => detail.hash.clone(),
            };
            tasks.push(DownloadTask {
                download_path: download_path_for_region(&self.region.provider, bundle_name, detail),
                bundle_path: bundle_name.clone(),
                bundle_hash,
                category: detail.category.clone(),
                file_size: detail.file_size,
                priority: usize::MAX,
                export_payloads: false,
                stage_haruki_3d: false,
            });
        }
        tasks.sort_by(|a, b| a.bundle_path.cmp(&b.bundle_path));
        tasks
    }

    fn build_haruki_3d_download_tasks(
        &self,
        info: &AssetBundleInfo,
        downloaded_assets: &DownloadRecord,
    ) -> Result<Vec<DownloadTask>, AssetExecutionError> {
        if self.haruki_3d_work_asset_root().is_none() {
            return Ok(Vec::new());
        }
        let mut tasks = Vec::new();
        for task in self.build_haruki_3d_filter_tasks(info) {
            let has_current_record = downloaded_assets
                .get(&task.bundle_path)
                .is_some_and(|existing| existing == &task.bundle_hash);
            if !has_current_record {
                tasks.push(DownloadTask {
                    stage_haruki_3d: true,
                    ..task
                });
            }
        }
        Ok(tasks)
    }

    fn build_haruki_3d_filter_tasks(&self, info: &AssetBundleInfo) -> Vec<DownloadTask> {
        let mut tasks = Vec::new();
        for (bundle_name, detail) in &info.bundles {
            if !self.matches_haruki_3d_filters(bundle_name) {
                continue;
            }
            let bundle_hash = match self.region.provider {
                RegionProviderConfig::Nuverse { .. } => detail.crc.to_string(),
                RegionProviderConfig::ColorfulPalette { .. } => detail.hash.clone(),
            };
            tasks.push(DownloadTask {
                download_path: download_path_for_region(&self.region.provider, bundle_name, detail),
                bundle_path: bundle_name.clone(),
                bundle_hash,
                category: detail.category.clone(),
                file_size: detail.file_size,
                priority: usize::MAX,
                export_payloads: false,
                stage_haruki_3d: true,
            });
        }
        tasks.sort_by(|a, b| a.bundle_path.cmp(&b.bundle_path));
        tasks
    }

    fn remove_haruki_3d_work_dir(work_run_dir: &Path) -> Result<(), AssetExecutionError> {
        if !work_run_dir.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(work_run_dir).map_err(|source| {
            AssetExecutionError::RemoveHaruki3dStagingDir {
                path: work_run_dir.to_path_buf(),
                source,
            }
        })
    }
}

/// Validate an untrusted, server-provided bundle path: it must be a relative path made only of
/// normal components (no empty / `.` / `..` / absolute / root / prefix). Returns it as a relative
/// `Path` so callers can safely `join` it onto a trusted root without escaping it.
fn validate_relative_bundle_path(bundle_path: &str) -> Result<&Path, AssetExecutionError> {
    let invalid = |reason: &str| AssetExecutionError::InvalidRawBundlePath {
        bundle: bundle_path.to_string(),
        reason: reason.to_string(),
    };
    if bundle_path.is_empty() {
        return Err(invalid("path is empty"));
    }
    if bundle_path
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(invalid(
            "empty, current-directory, or parent-directory components are not allowed",
        ));
    }

    let relative = Path::new(bundle_path);
    if relative.is_absolute() {
        return Err(invalid("absolute paths are not allowed"));
    }

    for component in relative.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {
                return Err(invalid("current-directory components are not allowed"))
            }
            Component::ParentDir => {
                return Err(invalid("parent-directory components are not allowed"))
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(invalid("root or prefix components are not allowed"))
            }
        }
    }

    Ok(relative)
}

fn raw_bundle_output_path(root: &Path, bundle_path: &str) -> Result<PathBuf, AssetExecutionError> {
    let relative = validate_relative_bundle_path(bundle_path)?;

    let mut path = root.to_path_buf();
    for component in relative.components() {
        if let Component::Normal(value) = component {
            path.push(value);
        }
    }

    if path.extension().and_then(|ext| ext.to_str()) != Some("bundle") {
        path.set_extension("bundle");
    }
    Ok(path)
}

fn bundle_hash_index_key(bundle_path: &str) -> Result<String, AssetExecutionError> {
    Ok(raw_bundle_output_path(Path::new(""), bundle_path)?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn exporter_metric_lines(stdout: &[u8]) -> String {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter(|line| line.contains(" metrics:") || line.starts_with("Planned "))
        .collect::<Vec<_>>()
        .join(" | ")
}

#[derive(Clone)]
struct BundleMemoryLimiter {
    semaphore: Option<Arc<Semaphore>>,
    limit_bytes: usize,
    limit_units: u32,
}

impl BundleMemoryLimiter {
    const UNIT_BYTES: usize = 1024 * 1024;

    fn from_config(app_config: &AppConfig) -> Self {
        let limit_bytes = app_config.resources.memory.max_in_flight_bundle_bytes;
        if limit_bytes == 0 {
            return Self {
                semaphore: None,
                limit_bytes,
                limit_units: 0,
            };
        }
        let limit_units = bytes_to_units(limit_bytes).min(u32::MAX as usize).max(1) as u32;
        Self {
            semaphore: Some(Arc::new(Semaphore::new(limit_units as usize))),
            limit_bytes,
            limit_units,
        }
    }

    fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    async fn acquire(&self, estimated_bytes: usize) -> Option<OwnedSemaphorePermit> {
        let semaphore = self.semaphore.as_ref()?;
        let units = bytes_to_units(estimated_bytes)
            .min(self.limit_units as usize)
            .max(1) as u32;
        semaphore.clone().acquire_many_owned(units).await.ok()
    }
}

fn bytes_to_units(bytes: usize) -> usize {
    bytes.div_ceil(BundleMemoryLimiter::UNIT_BYTES).max(1)
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
        // 5xx are transient; 429 (Too Many Requests) and 408 (Request Timeout) are the canonical
        // "back off and retry" signals that Project Sekai CDNs/rate limiters emit under load.
        AssetExecutionError::HttpStatus { status, .. } => {
            *status >= 500 || *status == 429 || *status == 408
        }
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
        AssetCategory::OnDemand | AssetCategory::LivePv => {
            compile_patterns(&region.filters.on_demand)
        }
        AssetCategory::Other(_) => return false,
    };
    if compiled.is_empty() {
        return false;
    }
    matches_any(&compiled, bundle_name)
}

fn post_process_backlog_capacity(
    download_concurrency: usize,
    post_process_concurrency: usize,
) -> usize {
    let _ = download_concurrency;
    post_process_concurrency.saturating_mul(2).max(1)
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
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, RawBundleExportConfig, RegionConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    };
    use crate::core::download_records::DownloadRecord;
    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::{
        bundle_hash_index_key, decrypt_asset_bundle_info, deobfuscate, exporter_metric_lines,
        post_process_backlog_capacity, raw_bundle_output_path, should_download_bundle,
        AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext,
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
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: vec!["^ond/".to_string(), "^live_pv/model/".to_string()],
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

    #[test]
    fn haruki_3d_work_root_is_disabled_by_default() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();

        assert!(executor.haruki_3d_work_asset_root().is_none());
    }

    #[test]
    fn raw_bundle_filters_are_independent_of_haruki_3d() {
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.raw_bundles = Some(RawBundleExportConfig {
            output_dir: None,
            include: vec!["^live_pv/model/characterv2/body/".to_string()],
            exclude: Vec::new(),
        });
        region.filters.on_demand.clear();
        region.filters.skip = vec![".*".to_string()];
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let config = AppConfig::default();

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        assert!(
            executor.matches_raw_bundle_filters("live_pv/model/characterv2/body/01"),
            "raw bundle retention must remain independent while 3D is disabled"
        );
        assert!(!executor.matches_raw_bundle_filters("live_pv/model/characterv2/face/01"));

        let detail = |bundle_name: &str| AssetBundleDetail {
            bundle_name: bundle_name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: format!("{bundle_name}-hash"),
            category: AssetCategory::OnDemand,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "live_pv/model/characterv2/body/01".to_string(),
                    detail("live_pv/model/characterv2/body/01"),
                ),
                (
                    "live_pv/model/characterv2/face/01".to_string(),
                    detail("live_pv/model/characterv2/face/01"),
                ),
            ]),
        };
        let tasks = executor.build_raw_bundle_filter_tasks(&info);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].bundle_path, "live_pv/model/characterv2/body/01");
    }

    #[test]
    fn haruki_3d_export_tasks_include_unrecorded_candidates() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.haruki_3d = crate::core::config::Haruki3dExportConfig {
            enabled: true,
            exporter_path: "/bin/true".to_string(),
            master_dir: "/data/master".to_string(),
            work_dir: temp.path().join("3d-work").to_string_lossy().into_owned(),
            manifest_file: temp
                .path()
                .join("manifest.json")
                .to_string_lossy()
                .into_owned(),
            output_dir: temp.path().join("out").to_string_lossy().into_owned(),
            include: vec!["^live_pv/model/characterv2/".to_string()],
            exclude: Vec::new(),
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("6.0.9".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let matched = "live_pv/model/characterv2/body/01_0001.bundle".to_string();
        let missing_from_record = "live_pv/model/characterv2/body/02_0001.bundle".to_string();
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    matched.clone(),
                    AssetBundleDetail {
                        bundle_name: matched.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "new-hash".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: Vec::new(),
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
                (
                    missing_from_record.clone(),
                    AssetBundleDetail {
                        bundle_name: missing_from_record.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "missing-from-record".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: Vec::new(),
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
            ]),
        };
        let tasks = executor.build_haruki_3d_tasks(&info);

        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().any(|task| task.bundle_path == matched));
        assert!(tasks
            .iter()
            .any(|task| task.bundle_path == missing_from_record));
        assert_eq!(
            executor.haruki_3d_work_asset_root().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("6.0.9")
                .join("AssetBundles")
        );
        assert_eq!(
            executor.haruki_3d_download_record_path().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("downloaded_assets.json")
        );
        assert_eq!(
            executor.haruki_3d_bundle_hash_index_path().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("bundle_sha256.json")
        );
    }

    #[test]
    fn haruki_3d_background_export_runs_registry_parts_and_role_runtimes() {
        let config = crate::core::config::Haruki3dExportConfig {
            master_dir: "/master".to_string(),
            output_dir: "/runtime".to_string(),
            manifest_file: "/runtime/manifest.json".to_string(),
            shared_content_store: "/runtime-cas".to_string(),
            compiled_content_store: "/runtime-compiled".to_string(),
            process_concurrency: 16,
            role_character3d_ids: vec![5, 7],
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let commands = AssetExecutionContext::build_haruki_3d_exporter_commands(
            &config,
            Path::new("/work/AssetBundles"),
            Path::new("/work/bundle_sha256.json"),
        );
        assert_eq!(
            AssetExecutionContext::build_haruki_3d_runtime_catalog_command(&config),
            vec![
                "--emit-runtime-role-catalog",
                "--master",
                "/master",
                "--out",
                "/runtime",
            ]
        );

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0][0], "--emit-costume-registries");
        assert_eq!(commands[1][0], "--emit-part-packages");
        for command in &commands {
            assert!(
                !command.iter().any(|arg| arg == "--runtime-json-output"),
                "Haruki 3D exporter command should use the exporter's fixed msgpack-br runtime format: {command:?}"
            );
            assert!(
                command
                    .windows(2)
                    .any(|pair| pair == ["--convert-model-textures", "false"]),
                "Haruki 3D exporter command should disable redundant model texture conversion: {command:?}"
            );
        }
        assert!(
            commands[1]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "16"]),
            "part package command should pass haruki_3d.process_concurrency"
        );
        assert!(commands[1]
            .windows(2)
            .any(|pair| pair == ["--shared-content-store", "/runtime-cas"]));
        assert!(commands[1]
            .windows(2)
            .any(|pair| pair == ["--compiled-content-store", "/runtime-compiled"]));
        assert!(commands[1]
            .windows(2)
            .any(|pair| pair == ["--bundle-hash-index", "/work/bundle_sha256.json"]));
        assert_eq!(commands[2][0], "--emit-role-runtimes");
        assert!(
            commands[2]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "16"]),
            "role runtime command should pass haruki_3d.process_concurrency"
        );
        assert_eq!(
            commands[2]
                .iter()
                .filter(|value| value.as_str() == "--role-character3d-id")
                .count(),
            2
        );
        assert!(commands[2].contains(&"5".to_string()));
        assert!(commands[2].contains(&"7".to_string()));
    }

    #[test]
    fn haruki_3d_background_export_runs_role_runtimes_without_role_id_filter() {
        let config = crate::core::config::Haruki3dExportConfig {
            master_dir: "/master".to_string(),
            output_dir: "/runtime".to_string(),
            manifest_file: "/runtime/manifest.json".to_string(),
            process_concurrency: 48,
            role_character3d_ids: Vec::new(),
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let commands = AssetExecutionContext::build_haruki_3d_exporter_commands(
            &config,
            Path::new("/work/AssetBundles"),
            Path::new("/work/bundle_sha256.json"),
        );

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[2][0], "--emit-role-runtimes");
        assert!(
            commands[2]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "48"]),
            "role runtime command should still pass haruki_3d.process_concurrency"
        );
        assert_eq!(
            commands[2]
                .iter()
                .filter(|value| value.as_str() == "--role-character3d-id")
                .count(),
            0,
            "empty role_character3d_ids should let the exporter choose its default role set"
        );
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
    fn build_download_tasks_skips_unchanged_and_queues_changed() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let config = AppConfig::default();
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let ctx = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();

        let detail = |hash: &str| AssetBundleDetail {
            bundle_name: String::new(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: hash.to_string(),
            category: AssetCategory::StartApp,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                ("start/a".to_string(), detail("h1")),
                ("start/aa".to_string(), detail("h2")),
            ]),
        };

        // Recorded hash matches -> skipped; bundle absent from record -> queued.
        let record = DownloadRecord::from([("start/a".to_string(), "h1".to_string())]);
        let tasks = ctx
            .build_download_tasks(&info, &record, &DownloadRecord::new())
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert!(
            !paths.contains(&"start/a"),
            "unchanged bundle must be skipped"
        );
        assert!(paths.contains(&"start/aa"), "new bundle must be queued");

        // Recorded hash differs -> re-queued.
        let stale = DownloadRecord::from([("start/a".to_string(), "OLD".to_string())]);
        let tasks = ctx
            .build_download_tasks(&info, &stale, &DownloadRecord::new())
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert!(
            paths.contains(&"start/a"),
            "changed bundle must be re-queued"
        );
    }

    #[test]
    fn build_download_tasks_routes_3d_only_matches_to_staging() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.filters.on_demand.clear();
        region.export.haruki_3d = crate::core::config::Haruki3dExportConfig {
            enabled: true,
            work_dir: temp.path().join("3d-work").to_string_lossy().into_owned(),
            include: vec!["^(start/a|live_pv/model/characterv2/body/)".to_string()],
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let config = AppConfig::default();
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let detail = |bundle_name: &str, category| AssetBundleDetail {
            bundle_name: bundle_name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: format!("{bundle_name}-hash"),
            category,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "start/a".to_string(),
                    detail("start/a", AssetCategory::StartApp),
                ),
                (
                    "live_pv/model/characterv2/body/01".to_string(),
                    detail("live_pv/model/characterv2/body/01", AssetCategory::OnDemand),
                ),
            ]),
        };

        let tasks = executor
            .build_download_tasks(&info, &DownloadRecord::new(), &DownloadRecord::new())
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "3D matches with missing staging must be merged once after ordinary download filtering"
        );
        assert!(
            tasks[0].export_payloads && tasks[0].stage_haruki_3d,
            "ordinary tasks must export payloads"
        );
        assert!(
            !tasks[1].export_payloads && tasks[1].stage_haruki_3d,
            "3D-only tasks must only stage raw bundles"
        );

        let haruki_3d_record = DownloadRecord::from([(
            "live_pv/model/characterv2/body/01".to_string(),
            "live_pv/model/characterv2/body/01-hash".to_string(),
        )]);
        let tasks = executor
            .build_download_tasks(&info, &DownloadRecord::new(), &haruki_3d_record)
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a"],
            "the independent 3D record must skip an unchanged bundle even after staging cleanup"
        );
    }

    #[tokio::test]
    async fn three_d_only_completion_does_not_pollute_standard_download_record() {
        let mut record = DownloadRecord::new();
        let mut completed = 0;
        let mut completed_standard = 0;
        let mut pending_save_count = 0;

        AssetExecutionContext::record_completed_bundle(
            &None,
            "/unused/downloaded_assets.json",
            &mut record,
            &mut completed,
            &mut completed_standard,
            &mut pending_save_count,
            0,
            "jp",
            None,
            None,
            "live_pv/model/characterv2/body/01".to_string(),
            "3d-hash".to_string(),
            false,
        )
        .await;

        assert_eq!(completed, 1);
        assert_eq!(completed_standard, 0);
        assert!(record.is_empty());
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
        assert!(should_download_bundle(
            &region,
            "live_pv/model/characterv2/body/99/0018/ladies_s",
            &AssetCategory::LivePv
        ));
        assert!(!should_download_bundle(
            &region,
            "other/a",
            &AssetCategory::OnDemand
        ));
        assert!(!should_download_bundle(
            &region,
            "character/member/001",
            &AssetCategory::LivePv
        ));
    }

    #[test]
    fn post_process_backlog_capacity_tracks_post_process_pressure() {
        assert_eq!(post_process_backlog_capacity(0, 0), 1);
        assert_eq!(post_process_backlog_capacity(8, 2), 4);
        assert_eq!(post_process_backlog_capacity(4, 12), 24);
    }

    #[test]
    fn raw_bundle_output_path_appends_bundle_extension_and_rejects_unsafe_paths() {
        let root = std::path::Path::new("/tmp/raw-root");
        assert_eq!(
            raw_bundle_output_path(root, "live_pv/model/character/body/foo").unwrap(),
            root.join("live_pv/model/character/body/foo.bundle")
        );
        assert_eq!(
            raw_bundle_output_path(root, "character/motion/costume_setting/01_00.bundle").unwrap(),
            root.join("character/motion/costume_setting/01_00.bundle")
        );
        assert!(raw_bundle_output_path(root, "").is_err());
        assert!(raw_bundle_output_path(root, "/absolute/path").is_err());
        assert!(raw_bundle_output_path(root, "../escape").is_err());
        assert!(raw_bundle_output_path(root, "safe/../escape").is_err());
        assert!(raw_bundle_output_path(root, "safe/./escape").is_err());
    }

    #[test]
    fn bundle_hash_index_uses_exporter_relative_bundle_path() {
        assert_eq!(
            bundle_hash_index_key("live_pv/model/characterv2/body/01/0001").unwrap(),
            "live_pv/model/characterv2/body/01/0001.bundle"
        );
        assert_eq!(
            bundle_hash_index_key("character/motion/01.bundle").unwrap(),
            "character/motion/01.bundle"
        );
    }

    #[test]
    fn exporter_metrics_keep_summary_lines_only() {
        let stdout = b"Started worker\nPart export metrics: built=3, restored=7\nnoise\nPart export parent metrics: totalMs=42\n";
        assert_eq!(
            exporter_metric_lines(stdout),
            "Part export metrics: built=3, restored=7 | Part export parent metrics: totalMs=42"
        );
    }

    #[tokio::test]
    async fn bundle_hash_index_checkpoint_is_durable() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("bundle-hashes.json");
        let index = Arc::new(std::sync::Mutex::new(DownloadRecord::from([(
            "live_pv/model/body.bundle".to_string(),
            "ab".repeat(32),
        )])));

        AssetExecutionContext::save_bundle_hash_index_checkpoint(Some(&path), Some(&index))
            .await
            .unwrap();

        assert_eq!(
            crate::core::download_records::load_download_record(&path).unwrap(),
            index.lock().unwrap().clone()
        );
    }

    #[tokio::test]
    async fn prefetch_can_fetch_asset_info_and_download_bundle() {
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
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^start/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            backends: crate::core::config::BackendsConfig {
                media: crate::core::config::MediaBackendConfig {
                    ffmpeg_path: "ffmpeg".to_string(),
                    ..crate::core::config::MediaBackendConfig::default()
                },
                ..crate::core::config::BackendsConfig::default()
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
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();
        assert_eq!(summary.completed_downloads, 1);

        assert_eq!(summary.failed_downloads, 0);
        assert_eq!(
            std::fs::read(save_dir.join("AssetBundles/start/a.bundle")).unwrap(),
            b"BUNDLE"
        );
        assert!(!record_file.exists());
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
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: Vec::new(),
                on_demand: vec!["^ond/".to_string()],
                skip: Vec::new(),
                priority: vec!["^ond/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^ond/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("cn".to_string(), region.clone());
        let config = AppConfig {
            regions,
            backends: crate::core::config::BackendsConfig {
                media: crate::core::config::MediaBackendConfig {
                    ffmpeg_path: "ffmpeg".to_string(),
                    ..crate::core::config::MediaBackendConfig::default()
                },
                ..crate::core::config::BackendsConfig::default()
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
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "cn", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();
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
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^start/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
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
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();

        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(info_hits.load(Ordering::SeqCst), 3);
    }
}
