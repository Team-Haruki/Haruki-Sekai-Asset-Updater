//! The persistent bundle cache: reading it, validating it, writing into it.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::model::{
    AssetExecutionContext, BundleCacheEntryStatus, BundleFetch, BundleFetchSource, DownloadTask,
};
use crate::core::config::AppConfig;
use crate::core::download_records::DownloadRecord;
use crate::core::errors::AssetExecutionError;
use sekai_asset_pipeline::{raw_bundle_output_path, validate_relative_bundle_path};

pub(super) fn bundle_hash_index_key(bundle_path: &str) -> Result<String, AssetExecutionError> {
    Ok(raw_bundle_output_path(Path::new(""), bundle_path)?
        .to_string_lossy()
        .replace('\\', "/"))
}

pub(super) fn bundle_cache_metadata_path(cache_file: &Path) -> PathBuf {
    let mut file_name = std::ffi::OsString::from(".");
    file_name.push(cache_file.file_name().unwrap_or_default());
    file_name.push(".haruki-cache-hash");
    cache_file.with_file_name(file_name)
}

pub(super) fn configured_asset_bundle_cache_dir(app_config: &AppConfig) -> Option<PathBuf> {
    app_config
        .execution
        .asset_bundle_cache_dir
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

pub(super) static BUNDLE_CACHE_WRITE_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

impl AssetExecutionContext {
    pub(super) fn record_bundle_payload_hash(
        index: Option<Arc<std::sync::Mutex<DownloadRecord>>>,
        key: String,
        payload: &Path,
    ) -> Result<(), AssetExecutionError> {
        let Some(index) = index else {
            return Ok(());
        };
        let mut file =
            std::fs::File::open(payload).map_err(|source| AssetExecutionError::ReadTempFile {
                path: payload.to_path_buf(),
                source,
            })?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read =
                file.read(&mut buffer)
                    .map_err(|source| AssetExecutionError::ReadTempFile {
                        path: payload.to_path_buf(),
                        source,
                    })?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        let digest = hex::encode(digest.finalize());
        index
            .lock()
            .map_err(|_| {
                AssetExecutionError::BlockingTask("bundle hash index lock poisoned".to_string())
            })?
            .insert(key, digest);
        Ok(())
    }

    pub(super) async fn get_bundle_with_cache(
        &self,
        task: &DownloadTask,
        cache_dir: &Path,
    ) -> Result<BundleFetch, AssetExecutionError> {
        let safe_bundle_path = validate_relative_bundle_path(&task.bundle_path)?;
        let cache_file = cache_dir.join(&self.region_name).join(safe_bundle_path);
        match Self::bundle_cache_entry_status(&cache_file, task).await {
            BundleCacheEntryStatus::Current => match tokio::fs::metadata(&cache_file).await {
                Ok(metadata) => {
                    return Ok(BundleFetch {
                        path: cache_file,
                        decoded_bytes: metadata.len(),
                        source: BundleFetchSource::CacheHit,
                        _temporary_directory: None,
                    });
                }
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                    // The cache may have been concurrently replaced between metadata validation
                    // and the read. Fall back to the network rather than failing the whole job.
                }
                Err(source) => {
                    return Err(AssetExecutionError::ReadTempFile {
                        path: cache_file,
                        source,
                    });
                }
            },
            BundleCacheEntryStatus::Stale => {
                tracing::warn!(
                    region = %self.region_name,
                    bundle = %task.bundle_path,
                    cache = %cache_file.display(),
                    "asset bundle cache entry is stale or incomplete; refreshing it"
                );
            }
            BundleCacheEntryStatus::Missing => {}
        }

        let request = self.bundle_request(task)?;
        let downloaded = self
            .client
            .download_bundle_to_file(&request, &cache_file)
            .await?;
        let metadata_path = bundle_cache_metadata_path(&cache_file);
        Self::atomic_write_bundle_cache_file(&metadata_path, task.revision.as_bytes()).await?;
        Ok(BundleFetch {
            path: downloaded.path,
            decoded_bytes: downloaded.decoded_bytes,
            source: BundleFetchSource::CacheMiss,
            _temporary_directory: None,
        })
    }

    pub(super) async fn bundle_cache_entry_status(
        cache_file: &Path,
        task: &DownloadTask,
    ) -> BundleCacheEntryStatus {
        let metadata_path = bundle_cache_metadata_path(cache_file);
        match tokio::fs::read_to_string(&metadata_path).await {
            Ok(cached_hash) => {
                if cached_hash.trim() != task.revision {
                    return if tokio::fs::metadata(cache_file).await.is_ok() {
                        BundleCacheEntryStatus::Stale
                    } else {
                        BundleCacheEntryStatus::Missing
                    };
                }
                match tokio::fs::metadata(cache_file).await {
                    Ok(_) => BundleCacheEntryStatus::Current,
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                        BundleCacheEntryStatus::Missing
                    }
                    Err(_) => BundleCacheEntryStatus::Stale,
                }
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                Self::legacy_bundle_cache_entry_status(cache_file, task.file_size).await
            }
            Err(_) => BundleCacheEntryStatus::Stale,
        }
    }

    async fn legacy_bundle_cache_entry_status(
        cache_file: &Path,
        expected_size: i64,
    ) -> BundleCacheEntryStatus {
        // Compatibility with caches produced before hash sidecars were
        // introduced. Network payloads carry a four-byte obfuscation marker
        // that the persistent cache omits.
        match tokio::fs::metadata(cache_file).await {
            Ok(metadata) if Self::legacy_cache_size_matches(metadata.len(), expected_size) => {
                BundleCacheEntryStatus::Current
            }
            Ok(_) => BundleCacheEntryStatus::Stale,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                BundleCacheEntryStatus::Missing
            }
            Err(_) => BundleCacheEntryStatus::Stale,
        }
    }

    fn legacy_cache_size_matches(body_len: u64, expected_size: i64) -> bool {
        let Some(expected) = u64::try_from(expected_size).ok().filter(|size| *size > 0) else {
            return true;
        };
        body_len == expected || body_len.checked_add(4) == Some(expected)
    }

    pub(super) async fn atomic_write_bundle_cache_file(
        path: &Path,
        body: &[u8],
    ) -> Result<(), AssetExecutionError> {
        let sequence = BUNDLE_CACHE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut temp_name = std::ffi::OsString::from(".");
        temp_name.push(path.file_name().unwrap_or_default());
        temp_name.push(format!(".tmp-{}-{sequence}", std::process::id()));
        let temp_path = path.with_file_name(temp_name);
        tokio::fs::write(&temp_path, body).await.map_err(|source| {
            AssetExecutionError::WriteTempFile {
                path: temp_path.clone(),
                source,
            }
        })?;
        if let Err(source) = tokio::fs::rename(&temp_path, path).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(AssetExecutionError::WriteTempFile {
                path: path.to_path_buf(),
                source,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::pipeline::prepare_asset_run;
    use std::collections::BTreeMap;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::body::Body;

    use axum::routing::get;
    use axum::Router;
    use tempfile::tempdir;

    use crate::core::config::{AppConfig, RegionProviderConfig};
    use crate::core::download_records::DownloadRecord;
    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::super::cache::{bundle_cache_metadata_path, bundle_hash_index_key};

    use super::super::model::{AssetExecutionContext, BundleCacheEntryStatus, DownloadTask};
    use sekai_asset_pipeline::{AssetCategory, ResolvedBundle};

    use super::super::test_support::test_region;

    #[tokio::test]
    async fn bundle_cache_status_validates_sidecar_before_loading_body() {
        let temp = tempdir().unwrap();
        let cache_file = temp.path().join("bundle-cache/cn/start/a");
        tokio::fs::create_dir_all(cache_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&cache_file, b"UnityFS cache body")
            .await
            .unwrap();
        let task = DownloadTask {
            bundle: ResolvedBundle {
                download_path: "start/a".to_string(),
                bundle_path: "start/a".to_string(),
                revision: "expected-hash".to_string(),
                category: AssetCategory::StartApp,
                file_size: 22,
            },
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };

        tokio::fs::write(bundle_cache_metadata_path(&cache_file), "stale-hash")
            .await
            .unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Stale
        );

        tokio::fs::write(bundle_cache_metadata_path(&cache_file), &task.revision)
            .await
            .unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Current
        );
    }

    #[tokio::test]
    async fn bundle_cache_downloads_once_then_avoids_network() {
        let temp = tempdir().unwrap();
        let cache_root = temp.path().join("bundle-cache");
        let request_count = Arc::new(AtomicUsize::new(0));
        let network_body = [
            &[0x20, 0x00, 0x00, 0x00],
            b"UnityFS cached test bundle".as_slice(),
        ]
        .concat();
        let app = Router::new().route(
            "/bundle/ond/a",
            get({
                let request_count = request_count.clone();
                let network_body = network_body.clone();
                move || {
                    let request_count = request_count.clone();
                    let network_body = network_body.clone();
                    async move {
                        request_count.fetch_add(1, Ordering::SeqCst);
                        Body::from(network_body)
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut config = AppConfig::default();
        config.execution.asset_bundle_cache_dir = Some(cache_root.to_string_lossy().into_owned());
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        config.regions.insert("cn".to_string(), region.clone());
        let context = AssetExecutionContext::new(
            &config,
            &prepare_asset_run(&config, &request).unwrap(),
            &request,
        )
        .unwrap();
        let task = DownloadTask {
            bundle: ResolvedBundle {
                download_path: "ond/a".to_string(),
                bundle_path: "ond/a".to_string(),
                revision: "hash-a".to_string(),
                category: AssetCategory::OnDemand,
                file_size: network_body.len() as i64,
            },
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };
        let first = context
            .fetch_deobfuscated_bundle(&config, &task)
            .await
            .unwrap();
        let second = context
            .fetch_deobfuscated_bundle(&config, &task)
            .await
            .unwrap();

        assert_eq!(first.source.as_str(), "cache_miss");
        assert_eq!(second.source.as_str(), "cache_hit");
        assert_eq!(
            tokio::fs::read(&first.path).await.unwrap(),
            b"UnityFS cached test bundle"
        );
        assert_eq!(
            tokio::fs::read(&second.path).await.unwrap(),
            tokio::fs::read(&first.path).await.unwrap()
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        let cache_file = cache_root.join("cn/ond/a");
        assert_eq!(
            tokio::fs::read(&cache_file).await.unwrap(),
            b"UnityFS cached test bundle"
        );
        assert_eq!(
            tokio::fs::read_to_string(bundle_cache_metadata_path(&cache_file))
                .await
                .unwrap(),
            "hash-a"
        );
    }

    #[tokio::test]
    async fn legacy_deobfuscated_bundle_cache_is_reused_without_network() {
        let temp = tempdir().unwrap();
        let cache_root = temp.path().join("bundle-cache");
        let cache_file = cache_root.join("cn/start/a");
        let cached_body = b"UnityFS legacy cached bundle";
        tokio::fs::create_dir_all(cache_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&cache_file, cached_body).await.unwrap();

        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: "http://127.0.0.1:1/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut config = AppConfig::default();
        config.execution.asset_bundle_cache_dir = Some(cache_root.to_string_lossy().into_owned());
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        config.regions.insert("cn".to_string(), region.clone());
        let context = AssetExecutionContext::new(
            &config,
            &prepare_asset_run(&config, &request).unwrap(),
            &request,
        )
        .unwrap();
        let task = DownloadTask {
            bundle: ResolvedBundle {
                download_path: "start/a".to_string(),
                bundle_path: "start/a".to_string(),
                revision: "hash-a".to_string(),
                category: AssetCategory::StartApp,
                file_size: (cached_body.len() + 4) as i64,
            },
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };

        let fetch = context
            .fetch_deobfuscated_bundle(&config, &task)
            .await
            .unwrap();

        assert_eq!(fetch.source.as_str(), "cache_hit");
        assert_eq!(tokio::fs::read(&fetch.path).await.unwrap(), cached_body);
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
    async fn cache_helpers_cover_configuration_hashing_and_missing_stale_entries() {
        let mut config = AppConfig::default();
        assert!(super::configured_asset_bundle_cache_dir(&config).is_none());
        config.execution.asset_bundle_cache_dir = Some("  /tmp/cache  ".to_string());
        assert_eq!(
            super::configured_asset_bundle_cache_dir(&config).unwrap(),
            std::path::PathBuf::from("/tmp/cache")
        );

        let dir = tempdir().unwrap();
        let payload = dir.path().join("payload");
        std::fs::write(&payload, b"abc").unwrap();
        AssetExecutionContext::record_bundle_payload_hash(None, "ignored".to_string(), &payload)
            .unwrap();
        let index = Arc::new(std::sync::Mutex::new(DownloadRecord::new()));
        AssetExecutionContext::record_bundle_payload_hash(
            Some(index.clone()),
            "bundle".to_string(),
            &payload,
        )
        .unwrap();
        assert_eq!(
            index.lock().unwrap()["bundle"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(AssetExecutionContext::record_bundle_payload_hash(
            Some(index),
            "missing".to_string(),
            &dir.path().join("missing"),
        )
        .is_err());

        let task = DownloadTask {
            bundle: ResolvedBundle {
                download_path: "start/a".to_string(),
                bundle_path: "start/a".to_string(),
                revision: "expected".to_string(),
                category: AssetCategory::StartApp,
                file_size: 8,
            },
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };
        let cache_file = dir.path().join("cache");
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Missing
        );
        tokio::fs::write(&cache_file, b"1234").await.unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Current
        );
        tokio::fs::write(bundle_cache_metadata_path(&cache_file), "wrong")
            .await
            .unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Stale
        );
        tokio::fs::remove_file(&cache_file).await.unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Missing
        );

        let atomic = dir.path().join("atomic");
        AssetExecutionContext::atomic_write_bundle_cache_file(&atomic, b"body")
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&atomic).await.unwrap(), b"body");
        assert!(
            AssetExecutionContext::atomic_write_bundle_cache_file(dir.path(), b"bad")
                .await
                .is_err()
        );
    }
}
