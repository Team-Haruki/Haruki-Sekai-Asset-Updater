//! The run loop: scheduling bundles, recording what completed, finishing jobs.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, USER_AGENT,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use super::model::{
    AssetExecutionContext, BundleWorkOutput, DownloadTask, NativeBundlePostProcessJob,
};
use super::progress::ExecutionProgressUpdate;
use crate::core::config::{AppConfig, AssetHttpVersion, RegionConfig};
use crate::core::download_records::{load_download_record, save_download_record, DownloadRecord};
use crate::core::errors::AssetExecutionError;
use crate::core::export_pipeline::{post_process_exported_files, NativeSemanticExportPathRegistry};
use crate::core::git_sync::sync_chart_hashes;
use crate::core::models::{AssetUpdateRequest, ExecutionSummary, JobPhase};

pub(super) fn post_process_backlog_capacity(
    download_concurrency: usize,
    post_process_concurrency: usize,
) -> usize {
    let _ = download_concurrency;
    post_process_concurrency.saturating_mul(2).max(1)
}

pub(super) fn blocking_panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "blocking task panicked".to_string()
    }
}

pub(super) fn bytes_to_units(bytes: usize) -> usize {
    bytes.div_ceil(BundleMemoryLimiter::UNIT_BYTES).max(1)
}

impl BundleMemoryLimiter {
    const UNIT_BYTES: usize = 1024 * 1024;

    pub(super) fn from_config(app_config: &AppConfig) -> Self {
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

    pub(super) fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub(super) async fn acquire(&self, estimated_bytes: usize) -> Option<OwnedSemaphorePermit> {
        let semaphore = self.semaphore.as_ref()?;
        let units = bytes_to_units(estimated_bytes)
            .min(self.limit_units as usize)
            .max(1) as u32;
        semaphore.clone().acquire_many_owned(units).await.ok()
    }
}

#[derive(Clone)]
pub(super) struct BundleMemoryLimiter {
    pub(super) semaphore: Option<Arc<Semaphore>>,
    pub(super) limit_bytes: usize,
    pub(super) limit_units: u32,
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
        let can_reuse_haruki_3d_download_record = self.can_reuse_haruki_3d_download_record().await;

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
        let tasks = self.build_download_tasks(
            &info,
            &downloaded_assets,
            &haruki_3d_downloaded_assets,
            can_reuse_haruki_3d_download_record,
        )?;
        let discovered_bundles = info.bundles.len();
        let queued_downloads = tasks.len();
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::DownloadsPlanned {
                total: queued_downloads,
            },
        );

        if tasks.is_empty() {
            tracing::info!(
                region = %self.region_name,
                discovered = discovered_bundles,
                "no new assets to download"
            );
            return Ok(ExecutionSummary {
                discovered_bundles,
                queued_downloads: 0,
                completed_downloads: 0,
                failed_downloads: 0,
                updated_record_entries: 0,
                chart_hash_sync_performed: false,
            });
        }
        // `AssetBundleInfo` can contain tens of thousands of entries. The execution phase only
        // needs the compact task list and the discovered count, so release the larger manifest
        // before any bundle payloads enter memory.
        drop(info);

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
        let export_path_registry = NativeSemanticExportPathRegistry::default();
        // Spawned tasks require an owned `'static` config. Share one deep clone rather than cloning
        // every region/filter/storage string once per bundle.
        let app_config_cloned = Arc::new(app_config.clone());
        let execution_context = Arc::new(self.clone());
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
                message: format!("downloading {queued_downloads} bundle(s)"),
            },
        );
        tracing::info!(
            region = %self.region_name,
            queued = queued_downloads,
            download_concurrency,
            audio_encode_concurrency = concurrency.audio_encode,
            video_encode_concurrency = concurrency.video_encode,
            post_process_concurrency,
            memory_limit_bytes = memory_limiter.limit_bytes(),
            "starting asset bundle processing"
        );

        let spawn_bundle_task = |joins: &mut JoinSet<_>, task: DownloadTask| {
            let ctx = execution_context.clone();
            let semaphore = semaphore.clone();
            let memory_limiter = memory_limiter.clone();
            let post_process_backlog_semaphore = post_process_backlog_semaphore.clone();
            let app_config = app_config_cloned.clone();
            let progress = progress.clone();
            let cancel_flag = cancel_flag.clone();
            let haruki_3d_work_root = haruki_3d_work_root.clone();
            let export_path_registry = export_path_registry.clone();
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
                        &export_path_registry,
                        bundle_hash_index.as_ref(),
                    )
                    .await
                {
                    Ok(Some(mut job)) if crate::core::export_pipeline::flat_pipeline_enabled() => {
                        // Flat mode: this worker finishes its own bundle instead of
                        // handing it to a second pool, so there is no queue between
                        // the stages and no backlog to bound. Parallelism is exactly
                        // the number of workers, which is what the Python front-end
                        // this is measured against does.
                        job._memory_permit = memory_permit.take();
                        match Self::finish_native_bundle_post_process(
                            &app_config,
                            &ctx.region_name,
                            &ctx.region,
                            &progress,
                            *Box::new(job),
                            0,
                        )
                        .await
                        {
                            Ok(()) => Ok(BundleWorkOutput::Completed),
                            Err(error) => Err(error),
                        }
                    }
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
        };

        // Maintain a fixed refill window instead of creating one Tokio task per bundle up front.
        // The semaphore remains as a defensive invariant, while pending task metadata stays O(D)
        // where D is download concurrency rather than O(all discovered bundles).
        let mut remaining_tasks = tasks.into_iter();
        for task in remaining_tasks.by_ref().take(download_concurrency) {
            spawn_bundle_task(&mut joins, task);
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
                    let completed_task = match result {
                        Ok(tuple) => Some(tuple),
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
                            None
                        }
                    };
                    if let Some((bundle_path, bundle_hash, record_standard, result)) = completed_task {
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
                    if let Some(task) = remaining_tasks.next() {
                        spawn_bundle_task(&mut joins, task);
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
        Self::save_download_record_on_blocking_thread(record_path.clone(), &mut downloaded_assets)
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
            discovered_bundles,
            queued_downloads,
            completed_downloads: completed,
            failed_downloads: failed,
            // Number of record entries actually added/updated this run (not the whole record size),
            // keeping the semantics consistent with the empty-task early-return path above.
            updated_record_entries: completed_standard,
            chart_hash_sync_performed,
        })
    }

    pub(super) fn ensure_not_cancelled(
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

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn record_completed_bundle(
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
                        downloaded_assets,
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

    pub(super) async fn save_download_record_on_blocking_thread(
        record_path: String,
        downloaded_assets: &mut DownloadRecord,
    ) -> Result<(), AssetExecutionError> {
        let record = std::mem::take(downloaded_assets);
        let (record, result) = tokio::task::spawn_blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                save_download_record(record_path, &record)
            }));
            (record, result)
        })
        .await
        .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?;
        *downloaded_assets = record;
        result
            .map_err(|panic| AssetExecutionError::BlockingTask(blocking_panic_message(panic)))?
            .map_err(AssetExecutionError::from)
    }

    pub(super) async fn save_owned_download_record_on_blocking_thread(
        record_path: String,
        downloaded_assets: DownloadRecord,
    ) -> Result<(), AssetExecutionError> {
        tokio::task::spawn_blocking(move || save_download_record(record_path, &downloaded_assets))
            .await
            .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?
            .map_err(AssetExecutionError::from)
    }

    pub(super) async fn save_bundle_hash_index_checkpoint(
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
        Self::save_owned_download_record_on_blocking_thread(
            path.to_string_lossy().into_owned(),
            record,
        )
        .await
    }

    pub(super) async fn finish_native_bundle_post_process(
        app_config: &AppConfig,
        region_name: &str,
        region: &RegionConfig,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        job: NativeBundlePostProcessJob,
        queue_wait_ms: u128,
    ) -> Result<(), AssetExecutionError> {
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

        let mut phase_ms = job.payload_export.unity_rs_export_phase_ms;
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
                ExecutionProgressUpdate::BundleUnityRsExportPhases {
                    bundle: job.bundle_path.clone(),
                    phase_ms,
                },
            );
        }
        if !job.payload_export.unity_rs_skipped_object_reads.is_empty() {
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleUnityRsSkippedObjectReads {
                    bundle: job.bundle_path.clone(),
                    count: job.payload_export.unity_rs_skipped_object_reads.len(),
                },
            );
        }
        if !job.payload_export.unity_rs_object_read_plan.is_empty() {
            Self::send_progress(
                progress,
                ExecutionProgressUpdate::BundleUnityRsObjectReadPlan {
                    bundle: job.bundle_path.clone(),
                    plan: job.payload_export.unity_rs_object_read_plan,
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
}
