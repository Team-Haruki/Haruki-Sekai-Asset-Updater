use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use opendal::Operator;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::core::config::{RetryConfig, StorageConfig, StorageProviderConfig};
use crate::core::errors::StorageError;
use crate::core::models::StorageTargetPlan;
use crate::core::retry::retry_async;

#[derive(Debug, Clone)]
struct ResolvedStorageProvider {
    provider: String,
    scheme: String,
    endpoint: String,
    bucket: String,
    root: Option<String>,
    base_url: String,
    public_read: bool,
    path_style: bool,
    options: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct StorageOperatorTarget {
    pub provider: String,
    pub operator: Operator,
}

pub struct StorageUploadOptions<'a> {
    pub selected_providers: &'a [String],
    pub remove_local: bool,
    pub concurrency: usize,
    pub retry: &'a RetryConfig,
}

pub fn build_storage_operator_targets(
    storage: &StorageConfig,
    region_name: &str,
    selected_providers: &[String],
) -> Result<Vec<StorageOperatorTarget>, StorageError> {
    selected_provider_configs(storage, selected_providers)?
        .into_iter()
        .map(|provider| build_operator_target(provider, region_name))
        .collect()
}

pub fn plan_storage_targets(
    storage: &StorageConfig,
    region_name: &str,
    selected_providers: &[String],
) -> Result<Vec<StorageTargetPlan>, StorageError> {
    selected_provider_configs(storage, selected_providers)?
        .into_iter()
        .map(|provider| {
            let resolved = resolve_storage_provider(provider, region_name)?;

            Ok(StorageTargetPlan {
                provider: resolved.provider,
                provider_kind: resolved.scheme,
                endpoint: resolved.endpoint,
                bucket: resolved.bucket,
                prefix: resolved.root,
                base_url: resolved.base_url,
                public_read: resolved.public_read,
                path_style: resolved.path_style,
            })
        })
        .collect()
}

pub async fn upload_to_all_storages(
    storage: &StorageConfig,
    region_name: &str,
    extracted_save_path: &Path,
    files: &[PathBuf],
    options: StorageUploadOptions<'_>,
) -> Result<(), StorageError> {
    if files.is_empty() || (storage.providers.is_empty() && options.selected_providers.is_empty()) {
        return Ok(());
    }

    let targets = build_storage_operator_targets(storage, region_name, options.selected_providers)?;

    let semaphore = Arc::new(Semaphore::new(options.concurrency.max(1)));
    let mut tasks = JoinSet::new();

    for target in targets {
        for file in files {
            let file = file.clone();
            let extracted_save_path = extracted_save_path.to_path_buf();
            let semaphore = semaphore.clone();
            let target = target.clone();
            let retry = options.retry.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                upload_single_file(&target, &extracted_save_path, &file, &retry).await
            });
        }
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    if options.remove_local {
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
    resolve_storage_template(bucket, region_name)
}

pub fn resolve_storage_template(value: &str, region_name: &str) -> String {
    value
        .replace("{server}", region_name)
        .replace("{region}", region_name)
}

pub fn construct_endpoint_url(endpoint: &str, tls: bool) -> String {
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{endpoint}")
}

fn construct_provider_endpoint(endpoint: &str, tls: bool) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        construct_endpoint_url(endpoint, tls)
    }
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
    target: &StorageOperatorTarget,
    extracted_save_path: &Path,
    file_path: &Path,
    retry: &RetryConfig,
) -> Result<(), StorageError> {
    let remote_path = construct_remote_path(extracted_save_path, file_path)?;
    let content_type = mime_guess::from_path(file_path)
        .first_or_octet_stream()
        .to_string();
    let content = tokio::fs::read(file_path)
        .await
        .map_err(|source| StorageError::Io {
            path: file_path.to_path_buf(),
            source,
        })?;

    retry_async(
        retry,
        "storage upload",
        |_| {
            let operator = target.operator.clone();
            let remote_path = remote_path.clone();
            let content_type = content_type.clone();
            let content = content.clone();
            let file_path = file_path.to_path_buf();
            let provider = target.provider.clone();
            async move {
                operator
                    .write_with(&remote_path, content)
                    .content_type(&content_type)
                    .await
                    .map_err(|source| StorageError::Upload {
                        provider,
                        path: file_path,
                        source,
                    })?;

                Ok(())
            }
        },
        is_retryable_storage_error,
    )
    .await?;

    Ok(())
}

fn build_operator_target(
    provider: &StorageProviderConfig,
    region_name: &str,
) -> Result<StorageOperatorTarget, StorageError> {
    opendal::init_default_registry();
    let resolved = resolve_storage_provider(provider, region_name)?;
    let operator = Operator::via_iter(&resolved.scheme, resolved.options).map_err(|source| {
        StorageError::Provider {
            provider: resolved.provider.clone(),
            source,
        }
    })?;

    Ok(StorageOperatorTarget {
        provider: resolved.provider,
        operator,
    })
}

pub fn build_storage_operator_target(
    storage: &StorageConfig,
    provider_name: &str,
    region_name: &str,
) -> Result<StorageOperatorTarget, StorageError> {
    let provider = find_storage_provider_config(storage, provider_name)?;
    build_operator_target(provider, region_name)
}

fn selected_provider_configs<'a>(
    storage: &'a StorageConfig,
    selected_providers: &[String],
) -> Result<Vec<&'a StorageProviderConfig>, StorageError> {
    if selected_providers.is_empty() {
        return Ok(storage.providers.iter().collect());
    }

    selected_providers
        .iter()
        .map(|provider_name| find_storage_provider_config(storage, provider_name))
        .collect()
}

fn find_storage_provider_config<'a>(
    storage: &'a StorageConfig,
    provider_name: &str,
) -> Result<&'a StorageProviderConfig, StorageError> {
    let provider_name = provider_name.trim();
    if provider_name.is_empty() {
        return Err(StorageError::InvalidProviderConfig {
            provider: "<selection>".to_string(),
            message: "selected provider name is empty".to_string(),
        });
    }

    let explicit_matches = storage
        .providers
        .iter()
        .filter(|provider| {
            provider
                .name
                .as_deref()
                .is_some_and(|name| name.trim() == provider_name)
        })
        .collect::<Vec<_>>();

    let matches = if explicit_matches.is_empty() {
        storage
            .providers
            .iter()
            .filter(|provider| {
                provider
                    .name
                    .as_deref()
                    .is_none_or(|name| name.trim().is_empty())
                    && provider.scheme.trim().eq_ignore_ascii_case(provider_name)
            })
            .collect::<Vec<_>>()
    } else {
        explicit_matches
    };

    match matches.as_slice() {
        [provider] => Ok(*provider),
        [] => Err(StorageError::InvalidProviderConfig {
            provider: provider_name.to_string(),
            message: "selected provider was not found".to_string(),
        }),
        _ => Err(StorageError::InvalidProviderConfig {
            provider: provider_name.to_string(),
            message: "selected provider matched multiple storage providers; set unique provider.name values".to_string(),
        }),
    }
}

fn resolve_storage_provider(
    provider: &StorageProviderConfig,
    region_name: &str,
) -> Result<ResolvedStorageProvider, StorageError> {
    let scheme = provider.scheme.trim().to_ascii_lowercase();
    let provider_label = provider
        .name
        .clone()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| scheme.clone());

    if scheme.is_empty() {
        return Err(StorageError::InvalidProviderConfig {
            provider: provider_label,
            message: "scheme is empty".to_string(),
        });
    }

    let mut options = provider
        .options
        .iter()
        .map(|(key, value)| (key.clone(), resolve_storage_template(value, region_name)))
        .collect::<BTreeMap<_, _>>();

    if let Some(root) = provider
        .root
        .as_deref()
        .map(str::trim)
        .filter(|root| !root.is_empty())
    {
        options
            .entry("root".to_string())
            .or_insert_with(|| resolve_storage_template(root, region_name));
    } else if let Some(prefix) = normalize_prefix(provider.prefix.as_deref()) {
        options
            .entry("root".to_string())
            .or_insert_with(|| resolve_storage_template(&prefix, region_name));
    }

    if !provider.bucket.trim().is_empty() {
        options
            .entry("bucket".to_string())
            .or_insert_with(|| resolve_bucket_template(&provider.bucket, region_name));
    }

    if !provider.endpoint.trim().is_empty() {
        options
            .entry("endpoint".to_string())
            .or_insert_with(|| construct_provider_endpoint(&provider.endpoint, provider.tls));
    }

    if let Some(region) = provider
        .region
        .as_deref()
        .filter(|region| !region.is_empty())
    {
        options
            .entry("region".to_string())
            .or_insert_with(|| region.to_string());
    }

    if let Some(access_key) = provider
        .access_key
        .as_deref()
        .filter(|access_key| !access_key.is_empty())
    {
        options
            .entry("access_key_id".to_string())
            .or_insert_with(|| access_key.to_string());
    }

    if let Some(secret_key) = provider
        .secret_key
        .as_deref()
        .filter(|secret_key| !secret_key.is_empty())
    {
        options
            .entry("secret_access_key".to_string())
            .or_insert_with(|| secret_key.to_string());
    }

    if provider.public_read {
        options
            .entry("default_acl".to_string())
            .or_insert_with(|| "public-read".to_string());
    }

    if !provider.path_style {
        options
            .entry("enable_virtual_host_style".to_string())
            .or_insert_with(|| "true".to_string());
    }

    let bucket = options.get("bucket").cloned().unwrap_or_default();
    if scheme == "s3" && bucket.trim().is_empty() {
        return Err(StorageError::MissingBucket {
            provider: provider_label,
        });
    }

    let endpoint = options.get("endpoint").cloned().unwrap_or_default();
    let root = options
        .get("root")
        .map(|root| root.trim().trim_matches('/').replace('\\', "/"))
        .filter(|root| !root.is_empty());
    let public_read = options
        .get("default_acl")
        .map(|acl| acl.eq_ignore_ascii_case("public-read"))
        .unwrap_or(false);
    let path_style = !options
        .get("enable_virtual_host_style")
        .is_some_and(|value| matches_bool(value, true));
    let base_url = provider
        .public_base_url
        .as_deref()
        .filter(|url| !url.trim().is_empty())
        .map(|url| resolve_storage_template(url.trim().trim_end_matches('/'), region_name))
        .unwrap_or_else(|| endpoint.clone());

    Ok(ResolvedStorageProvider {
        provider: provider_label,
        scheme,
        endpoint,
        bucket,
        root,
        base_url,
        public_read,
        path_style,
        options,
    })
}

fn matches_bool(value: &str, expected: bool) -> bool {
    matches!(
        (value.trim().to_ascii_lowercase().as_str(), expected),
        ("true" | "1" | "yes" | "on", true) | ("false" | "0" | "no" | "off", false)
    )
}

fn is_retryable_storage_error(err: &StorageError) -> bool {
    matches!(err, StorageError::Upload { .. })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;

    use tempfile::tempdir;

    use crate::core::config::{RetryConfig, StorageConfig, StorageProviderConfig};

    use super::{
        build_storage_operator_target, construct_endpoint_url, construct_remote_path,
        construct_storage_key, normalize_prefix, plan_storage_targets, resolve_bucket_template,
        upload_to_all_storages, StorageUploadOptions,
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

        let targets = plan_storage_targets(&storage, "jp", &[]).unwrap();
        assert_eq!(targets[0].provider, "s3");
        assert_eq!(targets[0].bucket, "sekai-jp-assets");
        assert_eq!(targets[0].prefix.as_deref(), Some("upload/smoke"));
        assert_eq!(
            targets[0].base_url,
            construct_endpoint_url("assets.example.com", true)
        );
    }

    #[test]
    fn storage_targets_support_opendal_options_schema() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("assets".to_string()),
                scheme: "s3".to_string(),
                root: Some("assets/{region}".to_string()),
                public_base_url: Some("https://cdn.example.com/assets/{region}".to_string()),
                options: BTreeMap::from([
                    ("bucket".to_string(), "sekai-{region}-assets".to_string()),
                    ("endpoint".to_string(), "https://s3.example.com".to_string()),
                    ("default_acl".to_string(), "public-read".to_string()),
                    ("enable_virtual_host_style".to_string(), "true".to_string()),
                ]),
                ..StorageProviderConfig::default()
            }],
        };

        let targets = plan_storage_targets(&storage, "jp", &[]).unwrap();
        assert_eq!(targets[0].provider, "assets");
        assert_eq!(targets[0].provider_kind, "s3");
        assert_eq!(targets[0].bucket, "sekai-jp-assets");
        assert_eq!(targets[0].prefix.as_deref(), Some("assets/jp"));
        assert_eq!(
            targets[0].base_url,
            "https://cdn.example.com/assets/jp".to_string()
        );
        assert!(targets[0].public_read);
        assert!(!targets[0].path_style);
    }

    #[test]
    fn storage_targets_can_filter_selected_named_providers() {
        let storage = StorageConfig {
            providers: vec![
                StorageProviderConfig {
                    name: Some("primary".to_string()),
                    scheme: "fs".to_string(),
                    root: Some("tmp/primary".to_string()),
                    ..StorageProviderConfig::default()
                },
                StorageProviderConfig {
                    name: Some("backup".to_string()),
                    scheme: "fs".to_string(),
                    root: Some("tmp/backup".to_string()),
                    ..StorageProviderConfig::default()
                },
            ],
        };

        let targets = plan_storage_targets(&storage, "jp", &["backup".to_string()]).unwrap();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].provider, "backup");
        assert_eq!(targets[0].prefix.as_deref(), Some("tmp/backup"));
    }

    #[test]
    fn selected_storage_provider_reports_missing_provider() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("primary".to_string()),
                scheme: "fs".to_string(),
                root: Some("tmp/primary".to_string()),
                ..StorageProviderConfig::default()
            }],
        };

        let err = match build_storage_operator_target(&storage, "backup", "jp") {
            Ok(_) => panic!("missing provider unexpectedly resolved"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("selected provider was not found"));
    }

    #[tokio::test]
    async fn upload_to_fs_storage_writes_relative_files() {
        let source_root = tempdir().unwrap();
        let target_root = tempdir().unwrap();
        let nested = source_root.path().join("music").join("file.txt");
        fs::create_dir_all(nested.parent().unwrap()).unwrap();
        fs::write(&nested, b"hello").unwrap();

        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                scheme: "fs".to_string(),
                root: Some(target_root.path().to_string_lossy().into_owned()),
                ..StorageProviderConfig::default()
            }],
        };

        upload_to_all_storages(
            &storage,
            "jp",
            source_root.path(),
            std::slice::from_ref(&nested),
            StorageUploadOptions {
                selected_providers: &[],
                remove_local: false,
                concurrency: 1,
                retry: &RetryConfig {
                    attempts: 1,
                    initial_backoff_ms: 1,
                    max_backoff_ms: 1,
                },
            },
        )
        .await
        .unwrap();

        assert_eq!(
            fs::read(target_root.path().join("music").join("file.txt")).unwrap(),
            b"hello"
        );
    }

    #[tokio::test]
    async fn upload_to_fs_storage_respects_selected_providers() {
        let source_root = tempdir().unwrap();
        let primary_root = tempdir().unwrap();
        let backup_root = tempdir().unwrap();
        let nested = source_root.path().join("music").join("file.txt");
        fs::create_dir_all(nested.parent().unwrap()).unwrap();
        fs::write(&nested, b"hello").unwrap();

        let storage = StorageConfig {
            providers: vec![
                StorageProviderConfig {
                    name: Some("primary".to_string()),
                    scheme: "fs".to_string(),
                    root: Some(primary_root.path().to_string_lossy().into_owned()),
                    ..StorageProviderConfig::default()
                },
                StorageProviderConfig {
                    name: Some("backup".to_string()),
                    scheme: "fs".to_string(),
                    root: Some(backup_root.path().to_string_lossy().into_owned()),
                    ..StorageProviderConfig::default()
                },
            ],
        };

        upload_to_all_storages(
            &storage,
            "jp",
            source_root.path(),
            std::slice::from_ref(&nested),
            StorageUploadOptions {
                selected_providers: &["backup".to_string()],
                remove_local: false,
                concurrency: 1,
                retry: &RetryConfig {
                    attempts: 1,
                    initial_backoff_ms: 1,
                    max_backoff_ms: 1,
                },
            },
        )
        .await
        .unwrap();

        assert!(!primary_root.path().join("music").join("file.txt").exists());
        assert_eq!(
            fs::read(backup_root.path().join("music").join("file.txt")).unwrap(),
            b"hello"
        );
    }

    #[tokio::test]
    async fn upload_reports_missing_selected_provider() {
        let source_root = tempdir().unwrap();
        let file = source_root.path().join("file.txt");
        fs::write(&file, b"hello").unwrap();
        let storage = StorageConfig {
            providers: Vec::new(),
        };

        let err = upload_to_all_storages(
            &storage,
            "jp",
            source_root.path(),
            std::slice::from_ref(&file),
            StorageUploadOptions {
                selected_providers: &["backup".to_string()],
                remove_local: false,
                concurrency: 1,
                retry: &RetryConfig {
                    attempts: 1,
                    initial_backoff_ms: 1,
                    max_backoff_ms: 1,
                },
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("selected provider was not found"));
    }
}
