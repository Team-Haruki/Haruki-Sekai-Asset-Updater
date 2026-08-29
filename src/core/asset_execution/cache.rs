//! The persistent bundle cache: reading it, validating it, writing into it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::crypto::deobfuscate_owned;
use super::model::{
    AssetExecutionContext, BundleCacheEntryStatus, BundleFetch, BundleFetchSource, DownloadTask,
};
use super::planning::{raw_bundle_output_path, validate_relative_bundle_path};
use crate::core::config::AppConfig;
use crate::core::download_records::DownloadRecord;
use crate::core::errors::AssetExecutionError;

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
        payload: &[u8],
    ) -> Result<(), AssetExecutionError> {
        let Some(index) = index else {
            return Ok(());
        };
        let digest = hex::encode(Sha256::digest(payload));
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
        bundle_url: &str,
        task: &DownloadTask,
        cache_dir: &Path,
    ) -> Result<BundleFetch, AssetExecutionError> {
        let safe_bundle_path = validate_relative_bundle_path(&task.bundle_path)?;
        let cache_file = cache_dir.join(&self.region_name).join(safe_bundle_path);
        match Self::bundle_cache_entry_status(&cache_file, task).await {
            BundleCacheEntryStatus::Current => match tokio::fs::read(&cache_file).await {
                Ok(body) => {
                    // Persistent cache entries are already deobfuscated. Returning the owned file
                    // buffer directly avoids a same-size `data.to_vec()` on every cache hit.
                    return Ok(BundleFetch {
                        body,
                        source: BundleFetchSource::CacheHit,
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

        let network_body = self.get_with_retry(bundle_url).await?;
        let body = deobfuscate_owned(network_body);
        Self::write_bundle_cache_entry(&cache_file, &task.bundle_hash, &body).await?;
        Ok(BundleFetch {
            body,
            source: BundleFetchSource::CacheMiss,
        })
    }

    pub(super) async fn bundle_cache_entry_status(
        cache_file: &Path,
        task: &DownloadTask,
    ) -> BundleCacheEntryStatus {
        let metadata_path = bundle_cache_metadata_path(cache_file);
        match tokio::fs::read_to_string(&metadata_path).await {
            Ok(cached_hash) => {
                if cached_hash.trim() != task.bundle_hash {
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
                // Compatibility with caches produced before hash sidecars were
                // introduced. Network payloads carry a four-byte obfuscation
                // marker that the persistent cache omits.
                let expected = usize::try_from(task.file_size)
                    .ok()
                    .filter(|size| *size > 0);
                match tokio::fs::metadata(cache_file).await {
                    Ok(metadata) => {
                        let body_len = usize::try_from(metadata.len()).ok();
                        if body_len.is_some_and(|body_len| {
                            expected.is_none_or(|expected| {
                                body_len == expected || body_len.checked_add(4) == Some(expected)
                            })
                        }) {
                            BundleCacheEntryStatus::Current
                        } else {
                            BundleCacheEntryStatus::Stale
                        }
                    }
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                        BundleCacheEntryStatus::Missing
                    }
                    Err(_) => BundleCacheEntryStatus::Stale,
                }
            }
            Err(_) => BundleCacheEntryStatus::Stale,
        }
    }

    pub(super) async fn write_bundle_cache_entry(
        cache_file: &Path,
        bundle_hash: &str,
        body: &[u8],
    ) -> Result<(), AssetExecutionError> {
        if let Some(parent) = cache_file.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|source| {
                AssetExecutionError::CreateTempDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        Self::atomic_write_bundle_cache_file(cache_file, body).await?;
        let metadata_path = bundle_cache_metadata_path(cache_file);
        Self::atomic_write_bundle_cache_file(&metadata_path, bundle_hash.as_bytes()).await
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
