use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use aws_sdk_s3::Client;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::core::config::{RetryConfig, StorageConfig, StorageProviderConfig};
use crate::core::errors::StorageError;
use crate::core::models::StorageTargetPlan;
use crate::core::retry::retry_async;

pub fn plan_storage_targets(
    storage: &StorageConfig,
    region_name: &str,
) -> Result<Vec<StorageTargetPlan>, StorageError> {
    storage
        .providers
        .iter()
        .map(|provider| {
            if provider.endpoint.trim().is_empty() {
                return Err(StorageError::MissingEndpoint {
                    provider: provider.kind.clone(),
                });
            }
            if provider.bucket.trim().is_empty() {
                return Err(StorageError::MissingBucket {
                    provider: provider.kind.clone(),
                });
            }

            Ok(StorageTargetPlan {
                provider_kind: provider.kind.clone(),
                endpoint: provider.endpoint.clone(),
                bucket: resolve_bucket_template(&provider.bucket, region_name),
                prefix: normalize_prefix(provider.prefix.as_deref()),
                base_url: construct_endpoint_url(&provider.endpoint, provider.tls),
                public_read: provider.public_read,
                path_style: provider.path_style,
            })
        })
        .collect()
}

pub async fn upload_to_all_storages(
    storage: &StorageConfig,
    region_name: &str,
    extracted_save_path: &Path,
    files: &[PathBuf],
    remove_local: bool,
    concurrency: usize,
    retry: &RetryConfig,
) -> Result<(), StorageError> {
    if storage.providers.is_empty() || files.is_empty() {
        return Ok(());
    }

    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut tasks = JoinSet::new();

    for provider in storage.providers.clone() {
        for file in files {
            let file = file.clone();
            let extracted_save_path = extracted_save_path.to_path_buf();
            let semaphore = semaphore.clone();
            let region_name = region_name.to_string();
            let provider = provider.clone();
            let retry = retry.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                upload_single_file(&provider, &region_name, &extracted_save_path, &file, &retry)
                    .await
            });
        }
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    if remove_local {
        for file in files {
            tokio::fs::remove_file(file)
                .await
                .map_err(|source| StorageError::Io {
                    path: file.clone(),
                    source,
                })?;
        }
    }

    Ok(())
}

pub fn resolve_bucket_template(bucket: &str, region_name: &str) -> String {
    bucket
        .replace("{server}", region_name)
        .replace("{region}", region_name)
}

pub fn construct_endpoint_url(endpoint: &str, tls: bool) -> String {
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{endpoint}")
}

pub fn construct_remote_path(
    extracted_save_path: &Path,
    file_path: &Path,
) -> Result<String, StorageError> {
    let relative = file_path
        .strip_prefix(extracted_save_path)
        .map_err(|_| StorageError::InvalidRelativePath(file_path.display().to_string()))?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

pub fn normalize_prefix(prefix: Option<&str>) -> Option<String> {
    prefix
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
        .map(|prefix| prefix.trim_matches('/').replace('\\', "/"))
        .filter(|prefix| !prefix.is_empty())
}

pub fn construct_storage_key(
    prefix: Option<&str>,
    extracted_save_path: &Path,
    file_path: &Path,
) -> Result<String, StorageError> {
    let remote_path = construct_remote_path(extracted_save_path, file_path)?;
    if let Some(prefix) = normalize_prefix(prefix) {
        Ok(format!("{prefix}/{remote_path}"))
    } else {
        Ok(remote_path)
    }
}

async fn upload_single_file(
    provider: &StorageProviderConfig,
    region_name: &str,
    extracted_save_path: &Path,
    file_path: &Path,
    retry: &RetryConfig,
) -> Result<(), StorageError> {
    if provider.endpoint.trim().is_empty() {
        return Err(StorageError::MissingEndpoint {
            provider: provider.kind.clone(),
        });
    }
    if provider.bucket.trim().is_empty() {
        return Err(StorageError::MissingBucket {
            provider: provider.kind.clone(),
        });
    }

    let region = provider
        .region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_string());
    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;

    let mut builder = Builder::from(&shared_config)
        .endpoint_url(construct_endpoint_url(&provider.endpoint, provider.tls))
        .force_path_style(provider.path_style);

    if let (Some(access_key), Some(secret_key)) =
        (provider.access_key.clone(), provider.secret_key.clone())
    {
        let credentials =
            aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "haruki");
        builder = builder.credentials_provider(credentials);
    }

    let client = Client::from_conf(builder.build());
    let remote_path =
        construct_storage_key(provider.prefix.as_deref(), extracted_save_path, file_path)?;
    let bucket = resolve_bucket_template(&provider.bucket, region_name);
    let content_type = mime_guess::from_path(file_path)
        .first_or_octet_stream()
        .to_string();
    retry_async(
        retry,
        "s3 upload",
        |_| {
            let client = client.clone();
            let bucket = bucket.clone();
            let remote_path = remote_path.clone();
            let content_type = content_type.clone();
            let file_path = file_path.to_path_buf();
            let provider_kind = provider.kind.clone();
            let public_read = provider.public_read;
            async move {
                let body = ByteStream::from_path(file_path.clone())
                    .await
                    .map_err(|source| StorageError::Io {
                        path: file_path.clone(),
                        source: source.into(),
                    })?;

                let mut request = client
                    .put_object()
                    .bucket(bucket.clone())
                    .key(remote_path.clone())
                    .body(body)
                    .content_type(content_type.clone());

                if public_read {
                    request = request.acl(ObjectCannedAcl::PublicRead);
                }

                request
                    .send()
                    .await
                    .map_err(|source| StorageError::Upload {
                        provider: provider_kind,
                        path: file_path,
                        source: source.into(),
                    })?;

                Ok(())
            }
        },
        is_retryable_storage_error,
    )
    .await?;

    Ok(())
}

fn is_retryable_storage_error(err: &StorageError) -> bool {
    matches!(err, StorageError::Upload { .. })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::core::config::{StorageConfig, StorageProviderConfig};

    use super::{
        construct_endpoint_url, construct_remote_path, construct_storage_key, normalize_prefix,
        plan_storage_targets, resolve_bucket_template,
    };

    #[test]
    fn bucket_template_supports_server_and_region_aliases() {
        assert_eq!(
            resolve_bucket_template("sekai-{server}-assets", "jp"),
            "sekai-jp-assets"
        );
        assert_eq!(
            resolve_bucket_template("sekai-{region}-assets", "cn"),
            "sekai-cn-assets"
        );
    }

    #[test]
    fn remote_path_is_relative_to_extract_root() {
        let root = Path::new("/tmp/data");
        let file = Path::new("/tmp/data/music/file.txt");
        assert_eq!(
            construct_remote_path(root, file).unwrap(),
            "music/file.txt".to_string()
        );
    }

    #[test]
    fn storage_key_includes_normalized_prefix_when_present() {
        let root = Path::new("/tmp/data");
        let file = Path::new("/tmp/data/music/file.txt");
        assert_eq!(
            construct_storage_key(Some("/smoke/test//"), root, file).unwrap(),
            "smoke/test/music/file.txt".to_string()
        );
    }

    #[test]
    fn normalize_prefix_discards_blank_values() {
        assert_eq!(normalize_prefix(Some("   ")), None);
        assert_eq!(
            normalize_prefix(Some("/upload/test/")),
            Some("upload/test".to_string())
        );
    }

    #[test]
    fn storage_targets_include_resolved_bucket_and_url() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                endpoint: "assets.example.com".to_string(),
                bucket: "sekai-{server}-assets".to_string(),
                prefix: Some("upload/smoke".to_string()),
                ..StorageProviderConfig::default()
            }],
        };

        let targets = plan_storage_targets(&storage, "jp").unwrap();
        assert_eq!(targets[0].bucket, "sekai-jp-assets");
        assert_eq!(targets[0].prefix.as_deref(), Some("upload/smoke"));
        assert_eq!(
            targets[0].base_url,
            construct_endpoint_url("assets.example.com", true)
        );
    }
}
