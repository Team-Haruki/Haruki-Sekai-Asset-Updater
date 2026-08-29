//! Fetching a bundle and getting its payloads onto disk.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::cache::{bundle_hash_index_key, configured_asset_bundle_cache_dir};
use super::crypto::deobfuscate_owned;
use super::model::{
    asset_category_name, AssetExecutionContext, BundleFetch, BundleFetchSource, BundleWritePlan,
    DownloadTask, NativeBundlePostProcessJob,
};
use super::planning::{raw_bundle_output_path, validate_relative_bundle_path};
use super::progress::ExecutionProgressUpdate;
use super::runner::BundleMemoryLimiter;
use crate::core::cleanup::remove_file_if_exists;
use crate::core::config::AppConfig;
use crate::core::download_records::DownloadRecord;
use crate::core::errors::AssetExecutionError;
use crate::core::export_pipeline::{
    export_unity_asset_bundle_payloads_with_registry, NativeSemanticExportPathRegistry,
};
use crate::core::models::{ExecutionSummary, JobPhase};

impl AssetExecutionContext {
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
        let tasks = if configured_asset_bundle_cache_dir(app_config).is_some() {
            self.build_standard_download_tasks(&info, &DownloadRecord::new())
        } else {
            self.build_raw_bundle_filter_tasks(&info)
        };
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
                "no assets matched prefetch filters"
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
        drop(info);

        let download_concurrency = app_config.effective_concurrency().download.max(1);
        let semaphore = Arc::new(Semaphore::new(download_concurrency));
        let memory_limiter = BundleMemoryLimiter::from_config(app_config);
        let mut joins = JoinSet::new();
        let app_config_cloned = Arc::new(app_config.clone());
        let execution_context = Arc::new(self.clone());
        Self::send_progress(
            &progress,
            ExecutionProgressUpdate::Phase {
                phase: JobPhase::DownloadingBundles,
                message: format!("prefetching {queued_downloads} bundle(s)"),
            },
        );
        tracing::info!(
            region = %self.region_name,
            queued = queued_downloads,
            download_concurrency,
            memory_limit_bytes = memory_limiter.limit_bytes(),
            "starting asset bundle prefetch"
        );

        let spawn_prefetch_task = |joins: &mut JoinSet<_>, task: DownloadTask| {
            let ctx = execution_context.clone();
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
        };
        let mut remaining_tasks = tasks.into_iter();
        for task in remaining_tasks.by_ref().take(download_concurrency) {
            spawn_prefetch_task(&mut joins, task);
        }

        let mut completed = 0usize;
        let mut failed = 0usize;
        while let Some(result) = joins.join_next().await {
            let completed_task = match result {
                Ok(tuple) => Some(tuple),
                Err(join_err) => {
                    // Prefetch sub-task panicked or was aborted: count as failed instead of
                    // unwinding the run.
                    failed += 1;
                    tracing::error!(
                        region = %self.region_name,
                        error = %join_err,
                        "bundle prefetch task panicked or was aborted; counting as failed"
                    );
                    None
                }
            };
            if let Some((bundle_path, result)) = completed_task {
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
                            "bundle prefetch failed"
                        );
                    }
                }
            }
            if let Some(task) = remaining_tasks.next() {
                spawn_prefetch_task(&mut joins, task);
            }
        }

        Ok(ExecutionSummary {
            discovered_bundles,
            queued_downloads,
            completed_downloads: completed,
            failed_downloads: failed,
            updated_record_entries: 0,
            chart_hash_sync_performed: false,
        })
    }

    pub(super) async fn download_and_export_bundle_payloads(
        &self,
        app_config: &AppConfig,
        task: &DownloadTask,
        progress: &Option<UnboundedSender<ExecutionProgressUpdate>>,
        haruki_3d_work_root: Option<&Path>,
        export_path_registry: &NativeSemanticExportPathRegistry,
        bundle_hash_index: Option<&Arc<std::sync::Mutex<DownloadRecord>>>,
    ) -> Result<Option<NativeBundlePostProcessJob>, AssetExecutionError> {
        let asset_save_dir = self.region.paths.asset_save_dir.clone().ok_or_else(|| {
            AssetExecutionError::MissingAssetSaveDir {
                region: self.region_name.clone(),
            }
        })?;
        let bundle_url = self.render_bundle_url(task)?;
        let download_started = Instant::now();
        let fetch = self
            .fetch_deobfuscated_bundle(app_config, &bundle_url, task)
            .await?;
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: task.bundle_path.clone(),
                bytes: fetch.body.len(),
                elapsed_ms: download_started.elapsed().as_millis(),
            },
        );

        let temp_file = self.bundle_temp_file(task)?;
        let write_plan = self.bundle_write_plan(
            task,
            &asset_save_dir,
            haruki_3d_work_root,
            &temp_file,
            bundle_hash_index,
        )?;
        let blocking_started = Instant::now();
        Self::persist_bundle_payload(fetch.body, write_plan).await?;
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

        self.export_bundle_payloads(
            app_config,
            task,
            &asset_save_dir,
            &temp_file,
            export_path_registry,
        )
        .await
    }

    pub(super) fn bundle_temp_file(
        &self,
        task: &DownloadTask,
    ) -> Result<PathBuf, AssetExecutionError> {
        // Asset-info is untrusted, so validate before using the bundle name in any path.
        let safe_path = validate_relative_bundle_path(&task.bundle_path)?;
        Ok(std::env::temp_dir().join(&self.region_name).join(safe_path))
    }

    pub(super) fn bundle_write_plan(
        &self,
        task: &DownloadTask,
        asset_save_dir: &str,
        haruki_3d_work_root: Option<&Path>,
        temp_file: &Path,
        bundle_hash_index: Option<&Arc<std::sync::Mutex<DownloadRecord>>>,
    ) -> Result<BundleWritePlan, AssetExecutionError> {
        let raw_target = self
            .matches_raw_bundle_filters(&task.bundle_path)
            .then(|| self.raw_bundle_output_path(asset_save_dir, &task.bundle_path))
            .transpose()?;
        let haruki_3d_target = task
            .stage_haruki_3d
            .then_some(haruki_3d_work_root)
            .flatten()
            .map(|root| raw_bundle_output_path(root, &task.bundle_path))
            .transpose()?;
        Ok(BundleWritePlan {
            raw_target,
            haruki_3d_target,
            temp_target: task.export_payloads.then(|| temp_file.to_path_buf()),
            bundle_hash_index: bundle_hash_index.cloned(),
            bundle_hash_index_key: bundle_hash_index_key(&task.bundle_path)?,
        })
    }

    pub(super) async fn persist_bundle_payload(
        payload: Vec<u8>,
        plan: BundleWritePlan,
    ) -> Result<(), AssetExecutionError> {
        tokio::task::spawn_blocking(move || Self::write_bundle_payload(&payload, plan))
            .await
            .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?
    }

    pub(super) fn write_bundle_payload(
        payload: &[u8],
        plan: BundleWritePlan,
    ) -> Result<(), AssetExecutionError> {
        if let Some(path) = plan.raw_target {
            Self::write_raw_bundle(&path, payload)?;
        }
        if let Some(path) = plan.haruki_3d_target {
            Self::write_haruki_3d_work_bundle(&path, payload)?;
            Self::record_bundle_payload_hash(
                plan.bundle_hash_index,
                plan.bundle_hash_index_key,
                payload,
            )?;
        }
        if let Some(path) = plan.temp_target {
            Self::write_temp_bundle(&path, payload)?;
        }
        Ok(())
    }

    pub(super) fn write_temp_bundle(
        path: &Path,
        payload: &[u8],
    ) -> Result<(), AssetExecutionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                AssetExecutionError::CreateTempDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        std::fs::write(path, payload).map_err(|source| AssetExecutionError::WriteTempFile {
            path: path.to_path_buf(),
            source,
        })
    }

    pub(super) async fn export_bundle_payloads(
        &self,
        app_config: &AppConfig,
        task: &DownloadTask,
        asset_save_dir: &str,
        temp_file: &Path,
        export_path_registry: &NativeSemanticExportPathRegistry,
    ) -> Result<Option<NativeBundlePostProcessJob>, AssetExecutionError> {
        let export_started = Instant::now();
        let payload_export = export_unity_asset_bundle_payloads_with_registry(
            app_config,
            &self.region,
            temp_file,
            &task.bundle_path,
            Path::new(asset_save_dir),
            asset_category_name(&task.category),
            export_path_registry,
        )
        .await;
        let _ = remove_file_if_exists(temp_file);
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

    pub(super) async fn prefetch_bundle(
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
        let fetch = self
            .fetch_deobfuscated_bundle(app_config, &bundle_url, task)
            .await?;
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: task.bundle_path.clone(),
                bytes: fetch.body.len(),
                elapsed_ms: download_started.elapsed().as_millis(),
            },
        );
        if configured_asset_bundle_cache_dir(app_config).is_none()
            || self.matches_raw_bundle_filters(&task.bundle_path)
        {
            let raw_path = self.raw_bundle_output_path(&asset_save_dir, &task.bundle_path)?;
            Self::write_raw_bundle(&raw_path, &fetch.body)?;
            tracing::debug!(
                region = %self.region_name,
                bundle = %task.bundle_path,
                output = %raw_path.display(),
                http_version = ?app_config.server.asset_http_version,
                "prefetched raw asset bundle"
            );
        }
        Ok(())
    }

    pub(super) async fn fetch_deobfuscated_bundle(
        &self,
        app_config: &AppConfig,
        bundle_url: &str,
        task: &DownloadTask,
    ) -> Result<BundleFetch, AssetExecutionError> {
        let Some(cache_dir) = configured_asset_bundle_cache_dir(app_config) else {
            let body = self.get_with_retry(bundle_url).await?;
            return Ok(BundleFetch {
                body: deobfuscate_owned(body),
                source: BundleFetchSource::Network,
            });
        };

        self.get_bundle_with_cache(bundle_url, task, &cache_dir)
            .await
    }

    pub(super) fn write_raw_bundle(
        path: &Path,
        deobfuscated: &[u8],
    ) -> Result<(), AssetExecutionError> {
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
}
