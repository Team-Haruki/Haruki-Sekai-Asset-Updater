//! Fetching a bundle and getting its payloads onto disk.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::cache::{bundle_hash_index_key, configured_asset_bundle_cache_dir};
use super::model::{
    AssetExecutionContext, BundleFetch, BundleFetchSource, BundleWritePlan, DownloadTask,
    NativeBundlePostProcessJob,
};
use super::progress::ExecutionProgressUpdate;
use super::runner::BundleMemoryLimiter;
use crate::core::config::{pipeline_options, AppConfig};
use crate::core::download_records::DownloadRecord;
use crate::core::errors::AssetExecutionError;
use crate::core::models::{ExecutionSummary, JobPhase};
use sekai_asset_pipeline::{
    asset_category_name, export_unity_asset_bundle_payloads_with_registry, raw_bundle_output_path,
    NativeSemanticExportPathRegistry,
};

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
        let download_started = Instant::now();
        let fetch = self.fetch_deobfuscated_bundle(app_config, task).await?;
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: task.bundle_path.clone(),
                bytes: usize::try_from(fetch.decoded_bytes).unwrap_or(usize::MAX),
                elapsed_ms: download_started.elapsed().as_millis(),
            },
        );

        let write_plan = self.bundle_write_plan(
            task,
            &asset_save_dir,
            haruki_3d_work_root,
            bundle_hash_index,
        )?;
        if write_plan.raw_target.is_some() || write_plan.haruki_3d_target.is_some() {
            Self::persist_bundle_payload(fetch.path.clone(), write_plan).await?;
        }

        if !task.export_payloads {
            return Ok(None);
        }

        self.export_bundle_payloads(
            app_config,
            task,
            &asset_save_dir,
            &fetch.path,
            export_path_registry,
        )
        .await
    }

    pub(super) fn bundle_write_plan(
        &self,
        task: &DownloadTask,
        asset_save_dir: &str,
        haruki_3d_work_root: Option<&Path>,
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
            bundle_hash_index: bundle_hash_index.cloned(),
            bundle_hash_index_key: bundle_hash_index_key(&task.bundle_path)?,
        })
    }

    pub(super) async fn persist_bundle_payload(
        payload: PathBuf,
        plan: BundleWritePlan,
    ) -> Result<(), AssetExecutionError> {
        tokio::task::spawn_blocking(move || Self::copy_bundle_payload(&payload, plan))
            .await
            .map_err(|source| AssetExecutionError::BlockingTask(source.to_string()))?
    }

    pub(super) fn copy_bundle_payload(
        payload: &Path,
        plan: BundleWritePlan,
    ) -> Result<(), AssetExecutionError> {
        if let Some(path) = plan.raw_target {
            Self::copy_raw_bundle(payload, &path)?;
        }
        if let Some(path) = plan.haruki_3d_target {
            Self::copy_haruki_3d_work_bundle(payload, &path)?;
            Self::record_bundle_payload_hash(
                plan.bundle_hash_index,
                plan.bundle_hash_index_key,
                &path,
            )?;
        }
        Ok(())
    }

    pub(super) async fn export_bundle_payloads(
        &self,
        app_config: &AppConfig,
        task: &DownloadTask,
        asset_save_dir: &str,
        bundle_file: &Path,
        export_path_registry: &NativeSemanticExportPathRegistry,
    ) -> Result<Option<NativeBundlePostProcessJob>, AssetExecutionError> {
        let export_started = Instant::now();
        let options = pipeline_options(app_config, &self.region);
        let payload_export = export_unity_asset_bundle_payloads_with_registry(
            &options,
            bundle_file,
            &task.bundle_path,
            Path::new(asset_save_dir),
            asset_category_name(&task.category),
            export_path_registry,
        )
        .await;
        Ok(Some(NativeBundlePostProcessJob {
            bundle_path: task.bundle_path.clone(),
            bundle_hash: task.revision.clone(),
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
        let download_started = Instant::now();
        let fetch = self.fetch_deobfuscated_bundle(app_config, task).await?;
        Self::send_progress(
            progress,
            ExecutionProgressUpdate::BundleDownloaded {
                bundle: task.bundle_path.clone(),
                bytes: usize::try_from(fetch.decoded_bytes).unwrap_or(usize::MAX),
                elapsed_ms: download_started.elapsed().as_millis(),
            },
        );
        if configured_asset_bundle_cache_dir(app_config).is_none()
            || self.matches_raw_bundle_filters(&task.bundle_path)
        {
            let raw_path = self.raw_bundle_output_path(&asset_save_dir, &task.bundle_path)?;
            Self::copy_raw_bundle(&fetch.path, &raw_path)?;
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
        task: &DownloadTask,
    ) -> Result<BundleFetch, AssetExecutionError> {
        let Some(cache_dir) = configured_asset_bundle_cache_dir(app_config) else {
            let temporary_directory =
                tempfile::tempdir().map_err(|source| AssetExecutionError::CreateTempDir {
                    path: std::env::temp_dir(),
                    source,
                })?;
            let destination = temporary_directory.path().join("bundle");
            let request = self.bundle_request(task)?;
            let downloaded = self
                .client
                .download_bundle_to_file(&request, &destination)
                .await?;
            return Ok(BundleFetch {
                path: downloaded.path,
                decoded_bytes: downloaded.decoded_bytes,
                source: BundleFetchSource::Network,
                _temporary_directory: Some(temporary_directory),
            });
        };

        self.get_bundle_with_cache(task, &cache_dir).await
    }

    pub(super) fn copy_raw_bundle(source: &Path, path: &Path) -> Result<(), AssetExecutionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                AssetExecutionError::CreateRawBundleDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        std::fs::copy(source, path).map_err(|source| AssetExecutionError::WriteRawBundle {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::pipeline::prepare_asset_run;
    use std::collections::{BTreeMap, HashMap};

    use axum::body::Body;

    use axum::routing::{get, post};
    use axum::Router;
    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, RawBundleExportConfig, RegionConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    };

    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::super::model::AssetExecutionContext;

    use super::super::test_support::{encrypt_asset_info, TEST_AES_IV_HEX, TEST_AES_KEY_HEX};
    use sekai_asset_pipeline::{AssetBundleDetail, AssetBundleInfo, AssetCategory};

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
                    file_size: 10,
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

        let executor = AssetExecutionContext::new(
            &config,
            &prepare_asset_run(&config, &request).unwrap(),
            &request,
        )
        .unwrap();
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
}
