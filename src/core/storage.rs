use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use opendal::Operator;
use regex::Regex;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::core::config::{RetryConfig, StorageConfig, StorageProviderConfig};
use crate::core::errors::StorageError;
use crate::core::models::StorageTargetPlan;
use sekai_asset_pipeline::retry_async;

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
    pub scheme: String,
    pub operator: Operator,
    pub public_operator: Option<Operator>,
}

pub struct StorageUploadOptions<'a> {
    pub selected_providers: &'a [String],
    pub public_read_include: &'a [String],
    pub public_read_exclude: &'a [String],
    pub remove_local: bool,
    pub concurrency: usize,
    pub retry: &'a RetryConfig,
}

#[derive(Debug)]
struct PublicReadMatcher {
    include: Vec<Regex>,
    exclude: Vec<Regex>,
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

pub fn build_storage_operator_target(
    storage: &StorageConfig,
    provider_name: &str,
    region_name: &str,
) -> Result<StorageOperatorTarget, StorageError> {
    let provider = find_storage_provider_config(storage, provider_name)?;
    build_operator_target(provider, region_name)
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
    let public_read_matcher =
        PublicReadMatcher::new(options.public_read_include, options.public_read_exclude)?;

    let concurrency = options.concurrency.max(1);
    let semaphore = global_upload_semaphore(concurrency);
    let mut tasks = JoinSet::new();
    let extracted_save_path = Arc::new(extracted_save_path.to_path_buf());
    let retry = Arc::new(options.retry.clone());
    let mut target_index = 0usize;
    let mut file_index = 0usize;
    let mut next_upload = || -> Result<_, StorageError> {
        if target_index >= targets.len() {
            return Ok(None);
        }
        let target = targets[target_index].clone();
        let file = files[file_index].clone();
        let public_read =
            public_read_matcher.matches_file(extracted_save_path.as_ref(), file.as_path())?;
        file_index += 1;
        if file_index == files.len() {
            file_index = 0;
            target_index += 1;
        }
        Ok(Some((target, file, public_read)))
    };
    let spawn_upload =
        |tasks: &mut JoinSet<_>,
         (target, file, public_read): (StorageOperatorTarget, PathBuf, bool)| {
            let extracted_save_path = extracted_save_path.clone();
            let semaphore = semaphore.clone();
            let retry = retry.clone();
            tasks.spawn(async move {
                // This semaphore is shared by every bundle and Job in the process. Waiting tasks hold
                // only paths/operators; they do not read a file until a global slot is available.
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                upload_single_file(
                    &target,
                    extracted_save_path.as_ref(),
                    &file,
                    public_read,
                    retry.as_ref(),
                )
                .await
            });
        };

    // Keep each caller's future set bounded as well as the process-wide active upload count.
    for _ in 0..concurrency {
        let Some(upload) = next_upload()? else {
            break;
        };
        spawn_upload(&mut tasks, upload);
    }

    while let Some(result) = tasks.join_next().await {
        result??;
        if let Some(upload) = next_upload()? {
            spawn_upload(&mut tasks, upload);
        }
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

fn global_upload_semaphore(concurrency: usize) -> Arc<Semaphore> {
    // AppConfig is immutable after service startup, so the first upload establishes the process
    // limit used by all regions and Jobs. Unit-test processes also get one deterministic limiter.
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER
        .get_or_init(|| Arc::new(Semaphore::new(concurrency.max(1))))
        .clone()
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
    public_read: bool,
    retry: &RetryConfig,
) -> Result<(), StorageError> {
    let remote_path = construct_remote_path(extracted_save_path, file_path)?;
    let content_type = mime_guess::from_path(file_path)
        .first_or_octet_stream()
        .to_string();
    let metadata = tokio::fs::metadata(file_path)
        .await
        .map_err(|source| StorageError::Io {
            path: file_path.to_path_buf(),
            source,
        })?;

    const WHOLE_FILE_UPLOAD_THRESHOLD_BYTES: u64 = 1024 * 1024;
    if metadata.len() > WHOLE_FILE_UPLOAD_THRESHOLD_BYTES {
        return retry_async(
            retry,
            "streaming storage upload",
            |_| {
                upload_streaming_file_once(
                    target,
                    file_path,
                    &remote_path,
                    &content_type,
                    public_read,
                    metadata.len(),
                )
            },
            is_retryable_storage_error,
        )
        .await;
    }

    // Small-file fast path: hold the body as an OpenDAL `Buffer` so retry clones are cheap pointer
    // bumps. Larger files use the bounded streaming path above.
    let content = opendal::Buffer::from(tokio::fs::read(file_path).await.map_err(|source| {
        StorageError::Io {
            path: file_path.to_path_buf(),
            source,
        }
    })?);

    retry_async(
        retry,
        "storage upload",
        |_| {
            let operator = target.operator_for_file(public_read).clone();
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

async fn upload_streaming_file_once(
    target: &StorageOperatorTarget,
    file_path: &Path,
    remote_path: &str,
    content_type: &str,
    public_read: bool,
    file_len: u64,
) -> Result<(), StorageError> {
    const UPLOAD_STREAM_CHUNK_SIZE: usize = 8 * 1024 * 1024;

    let mut input = tokio::fs::File::open(file_path)
        .await
        .map_err(|source| StorageError::Io {
            path: file_path.to_path_buf(),
            source,
        })?;
    let operator = target.operator_for_file(public_read).clone();
    let mut writer = operator
        .writer_with(remote_path)
        .content_type(content_type)
        .chunk(UPLOAD_STREAM_CHUNK_SIZE)
        // OpenDAL can run multipart chunks concurrently, but keeping this at one makes the memory
        // bound explicit. Parallelism comes from the process-wide file upload limiter instead.
        .concurrent(1)
        .await
        .map_err(|source| StorageError::Upload {
            provider: target.provider.clone(),
            path: file_path.to_path_buf(),
            source,
        })?;
    let read_chunk_size = usize::try_from(file_len)
        .unwrap_or(UPLOAD_STREAM_CHUNK_SIZE)
        .clamp(1, UPLOAD_STREAM_CHUNK_SIZE);

    loop {
        let mut chunk = vec![0u8; read_chunk_size];
        let read = match input.read(&mut chunk).await {
            Ok(read) => read,
            Err(source) => {
                let _ = writer.abort().await;
                return Err(StorageError::Io {
                    path: file_path.to_path_buf(),
                    source,
                });
            }
        };
        if read == 0 {
            break;
        }
        chunk.truncate(read);
        if let Err(source) = writer.write(chunk).await {
            let _ = writer.abort().await;
            return Err(StorageError::Upload {
                provider: target.provider.clone(),
                path: file_path.to_path_buf(),
                source,
            });
        }
    }

    if let Err(source) = writer.close().await {
        let _ = writer.abort().await;
        return Err(StorageError::Upload {
            provider: target.provider.clone(),
            path: file_path.to_path_buf(),
            source,
        });
    }
    Ok(())
}

fn build_operator_target(
    provider: &StorageProviderConfig,
    region_name: &str,
) -> Result<StorageOperatorTarget, StorageError> {
    opendal::init_default_registry();
    let resolved = resolve_storage_provider(provider, region_name)?;
    let operator =
        Operator::via_iter(&resolved.scheme, resolved.options.clone()).map_err(|source| {
            StorageError::Provider {
                provider: resolved.provider.clone(),
                source,
            }
        })?;
    let public_operator = if resolved.scheme == "s3" && !resolved.public_read {
        let mut public_options = resolved.options.clone();
        public_options.insert("default_acl".to_string(), "public-read".to_string());
        Some(
            Operator::via_iter(&resolved.scheme, public_options).map_err(|source| {
                StorageError::Provider {
                    provider: resolved.provider.clone(),
                    source,
                }
            })?,
        )
    } else {
        None
    };

    Ok(StorageOperatorTarget {
        provider: resolved.provider,
        scheme: resolved.scheme,
        operator,
        public_operator,
    })
}

impl StorageOperatorTarget {
    fn operator_for_file(&self, public_read: bool) -> &Operator {
        if public_read && self.scheme == "s3" {
            self.public_operator.as_ref().unwrap_or(&self.operator)
        } else {
            &self.operator
        }
    }
}

impl PublicReadMatcher {
    fn new(include: &[String], exclude: &[String]) -> Result<Self, StorageError> {
        Ok(Self {
            include: compile_public_read_rules(include)?,
            exclude: compile_public_read_rules(exclude)?,
        })
    }

    fn matches_file(
        &self,
        extracted_save_path: &Path,
        file_path: &Path,
    ) -> Result<bool, StorageError> {
        let remote_path = construct_remote_path(extracted_save_path, file_path)?;
        Ok(self.matches_remote_path(&remote_path))
    }

    fn matches_remote_path(&self, remote_path: &str) -> bool {
        if self.include.is_empty() {
            return false;
        }
        self.include.iter().any(|rule| rule.is_match(remote_path))
            && !self.exclude.iter().any(|rule| rule.is_match(remote_path))
    }
}

fn compile_public_read_rules(patterns: &[String]) -> Result<Vec<Regex>, StorageError> {
    patterns
        .iter()
        .filter(|pattern| !pattern.trim().is_empty())
        .map(|pattern| {
            Regex::new(pattern).map_err(|source| StorageError::InvalidPublicReadRule {
                pattern: pattern.clone(),
                source,
            })
        })
        .collect()
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
                    && provider
                        .effective_scheme()
                        .trim()
                        .eq_ignore_ascii_case(provider_name)
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

trait StorageProviderConfigExt {
    fn effective_scheme(&self) -> &str;
}

impl StorageProviderConfigExt for StorageProviderConfig {
    fn effective_scheme(&self) -> &str {
        &self.scheme
    }
}

fn resolve_storage_provider(
    provider: &StorageProviderConfig,
    region_name: &str,
) -> Result<ResolvedStorageProvider, StorageError> {
    let scheme = provider.effective_scheme().trim().to_ascii_lowercase();
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

    if scheme == "s3" && provider.public_read {
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
    use crate::core::errors::StorageError;

    use super::{
        build_storage_operator_target, construct_endpoint_url, construct_remote_path,
        construct_storage_key, normalize_prefix, plan_storage_targets, resolve_bucket_template,
        upload_single_file, upload_to_all_storages, PublicReadMatcher, StorageUploadOptions,
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
    fn s3_public_read_field_sets_default_acl() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("assets".to_string()),
                scheme: "s3".to_string(),
                endpoint: "s3.example.com".to_string(),
                bucket: "sekai-{region}-assets".to_string(),
                public_read: true,
                ..StorageProviderConfig::default()
            }],
        };

        let targets = plan_storage_targets(&storage, "jp", &[]).unwrap();

        assert_eq!(targets[0].provider, "assets");
        assert!(targets[0].public_read);
    }

    #[test]
    fn public_read_field_is_ignored_for_non_s3_providers() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("local".to_string()),
                scheme: "fs".to_string(),
                root: Some("tmp/local".to_string()),
                public_read: true,
                ..StorageProviderConfig::default()
            }],
        };

        let targets = plan_storage_targets(&storage, "jp", &[]).unwrap();

        assert_eq!(targets[0].provider, "local");
        assert!(!targets[0].public_read);
    }

    #[test]
    fn file_public_read_rules_match_relative_upload_paths() {
        let matcher =
            PublicReadMatcher::new(&["\\.(png|mp3)$".to_string()], &["^private/".to_string()])
                .unwrap();

        assert!(matcher.matches_remote_path("startapp/icon.png"));
        assert!(matcher.matches_remote_path("ondemand/music/song.mp3"));
        assert!(!matcher.matches_remote_path("startapp/data.json"));
        assert!(!matcher.matches_remote_path("private/icon.png"));
    }

    #[test]
    fn invalid_file_public_read_regex_is_reported() {
        let err = PublicReadMatcher::new(&["[".to_string()], &[]).unwrap_err();
        assert!(matches!(
            err,
            StorageError::InvalidPublicReadRule { ref pattern, .. } if pattern == "["
        ));
    }

    #[test]
    fn s3_target_builds_public_operator_for_file_level_rules() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("assets".to_string()),
                scheme: "s3".to_string(),
                endpoint: "s3.example.com".to_string(),
                region: Some("us-east-1".to_string()),
                bucket: "sekai-{region}-assets".to_string(),
                ..StorageProviderConfig::default()
            }],
        };

        let target = build_storage_operator_target(&storage, "assets", "jp").unwrap();

        assert_eq!(target.scheme, "s3");
        assert!(target.public_operator.is_some());
    }

    #[test]
    fn provider_level_public_read_reuses_default_operator() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("assets".to_string()),
                scheme: "s3".to_string(),
                endpoint: "s3.example.com".to_string(),
                region: Some("us-east-1".to_string()),
                bucket: "sekai-{region}-assets".to_string(),
                public_read: true,
                ..StorageProviderConfig::default()
            }],
        };

        let target = build_storage_operator_target(&storage, "assets", "jp").unwrap();

        assert_eq!(target.scheme, "s3");
        assert!(target.public_operator.is_none());
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
                public_read_include: &[],
                public_read_exclude: &[],
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
    async fn upload_to_fs_storage_streams_large_files() {
        let source_root = tempdir().unwrap();
        let target_root = tempdir().unwrap();
        let nested = source_root.path().join("movie").join("large.bin");
        fs::create_dir_all(nested.parent().unwrap()).unwrap();
        let content = (0..(3 * 1024 * 1024 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&nested, &content).unwrap();

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
                public_read_include: &[],
                public_read_exclude: &[],
                remove_local: false,
                concurrency: 2,
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
            fs::read(target_root.path().join("movie").join("large.bin")).unwrap(),
            content
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
                public_read_include: &[],
                public_read_exclude: &[],
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
                public_read_include: &[],
                public_read_exclude: &[],
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

    #[test]
    fn provider_validation_covers_empty_duplicate_and_missing_bucket_cases() {
        let empty = StorageConfig {
            providers: vec![StorageProviderConfig::default()],
        };
        assert!(plan_storage_targets(&empty, "jp", &[]).is_err());
        assert!(plan_storage_targets(&empty, "jp", &[" ".to_string()]).is_err());

        let duplicate = StorageConfig {
            providers: vec![
                StorageProviderConfig {
                    scheme: "fs".to_string(),
                    ..StorageProviderConfig::default()
                },
                StorageProviderConfig {
                    scheme: "FS".to_string(),
                    ..StorageProviderConfig::default()
                },
            ],
        };
        assert!(plan_storage_targets(&duplicate, "jp", &["fs".to_string()]).is_err());

        let missing_bucket = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("s3".to_string()),
                scheme: "s3".to_string(),
                ..StorageProviderConfig::default()
            }],
        };
        assert!(matches!(
            plan_storage_targets(&missing_bucket, "jp", &[]),
            Err(StorageError::MissingBucket { .. })
        ));
    }

    #[test]
    fn provider_resolution_maps_legacy_fields_and_url_options() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("primary".to_string()),
                scheme: "s3".to_string(),
                endpoint: "example.com/".to_string(),
                bucket: "assets-{region}".to_string(),
                prefix: Some("/prefix/{server}/".to_string()),
                region: Some("us-east-1".to_string()),
                access_key: Some("access".to_string()),
                secret_key: Some("secret".to_string()),
                public_base_url: Some("https://cdn.example/{region}/".to_string()),
                public_read: true,
                path_style: false,
                tls: true,
                ..StorageProviderConfig::default()
            }],
        };
        let target = plan_storage_targets(&storage, "jp", &[]).unwrap().remove(0);
        assert_eq!(target.endpoint, "https://example.com");
        assert_eq!(target.bucket, "assets-jp");
        assert_eq!(target.prefix.as_deref(), Some("prefix/jp"));
        assert_eq!(target.base_url, "https://cdn.example/jp");
        assert!(target.public_read);
        assert!(!target.path_style);
        assert_eq!(construct_endpoint_url("host", false), "http://host");
    }

    #[tokio::test]
    async fn upload_empty_inputs_and_remove_local_paths_are_explicit() {
        let root = tempdir().unwrap();
        let target = tempdir().unwrap();
        let file = root.path().join("remove.txt");
        fs::write(&file, b"remove me").unwrap();
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                scheme: "fs".to_string(),
                root: Some(target.path().to_string_lossy().into_owned()),
                ..StorageProviderConfig::default()
            }],
        };
        let retry = RetryConfig {
            attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        };
        upload_to_all_storages(
            &storage,
            "jp",
            root.path(),
            &[],
            StorageUploadOptions {
                selected_providers: &[],
                public_read_include: &[],
                public_read_exclude: &[],
                remove_local: false,
                concurrency: 0,
                retry: &retry,
            },
        )
        .await
        .unwrap();
        upload_to_all_storages(
            &storage,
            "jp",
            root.path(),
            std::slice::from_ref(&file),
            StorageUploadOptions {
                selected_providers: &[],
                public_read_include: &[],
                public_read_exclude: &[],
                remove_local: true,
                concurrency: 0,
                retry: &retry,
            },
        )
        .await
        .unwrap();
        assert!(!file.exists());
        assert_eq!(
            fs::read(target.path().join("remove.txt")).unwrap(),
            b"remove me"
        );
    }

    #[tokio::test]
    async fn upload_errors_cover_missing_sources_and_duplicate_cleanup() {
        let source_root = tempdir().unwrap();
        let target_root = tempdir().unwrap();
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                scheme: "fs".to_string(),
                root: Some(target_root.path().to_string_lossy().into_owned()),
                ..StorageProviderConfig::default()
            }],
        };
        let target = build_storage_operator_target(&storage, "fs", "jp").unwrap();
        let retry = RetryConfig {
            attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        };
        let missing = source_root.path().join("missing.txt");
        assert!(matches!(
            upload_single_file(&target, source_root.path(), &missing, false, &retry).await,
            Err(StorageError::Io { .. })
        ));

        let file = source_root.path().join("duplicate.txt");
        fs::write(&file, b"payload").unwrap();
        assert!(matches!(
            upload_to_all_storages(
                &storage,
                "jp",
                source_root.path(),
                &[file.clone(), file.clone()],
                StorageUploadOptions {
                    selected_providers: &[],
                    public_read_include: &[],
                    public_read_exclude: &[],
                    remove_local: true,
                    concurrency: 1,
                    retry: &retry,
                },
            )
            .await,
            Err(StorageError::Io { .. })
        ));
        assert_eq!(
            fs::read(target_root.path().join("duplicate.txt")).unwrap(),
            b"payload"
        );
    }

    #[test]
    fn public_operator_selection_and_matcher_file_paths_are_explicit() {
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("assets".to_string()),
                scheme: "s3".to_string(),
                endpoint: "s3.example.com".to_string(),
                region: Some("us-east-1".to_string()),
                bucket: "assets".to_string(),
                ..StorageProviderConfig::default()
            }],
        };
        let target = build_storage_operator_target(&storage, "assets", "jp").unwrap();
        assert!(std::ptr::eq(
            target.operator_for_file(true),
            target.public_operator.as_ref().unwrap()
        ));
        assert!(std::ptr::eq(
            target.operator_for_file(false),
            &target.operator
        ));

        let matcher = PublicReadMatcher::new(&["\\.png$".to_string()], &[]).unwrap();
        assert!(matcher
            .matches_file(Path::new("root"), Path::new("root/icons/a.png"))
            .unwrap());
        assert!(matcher
            .matches_file(Path::new("root"), Path::new("outside/a.png"))
            .is_err());
    }

    #[tokio::test]
    async fn seaweedfs_s3_smoke_when_configured() {
        let endpoint = match std::env::var("HARUKI_TEST_S3_ENDPOINT") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return,
        };
        let bucket = std::env::var("HARUKI_TEST_S3_BUCKET")
            .unwrap_or_else(|_| "haruki-opendal-smoke".to_string());
        let access_key =
            std::env::var("HARUKI_TEST_S3_ACCESS_KEY").unwrap_or_else(|_| "any".to_string());
        let secret_key =
            std::env::var("HARUKI_TEST_S3_SECRET_KEY").unwrap_or_else(|_| "any".to_string());
        let source_root = tempdir().unwrap();
        let nested = source_root.path().join("music").join("file.txt");
        fs::create_dir_all(nested.parent().unwrap()).unwrap();
        fs::write(&nested, b"hello seaweedfs").unwrap();

        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                name: Some("seaweed".to_string()),
                scheme: "s3".to_string(),
                root: Some("smoke/{region}".to_string()),
                options: BTreeMap::from([
                    ("bucket".to_string(), bucket),
                    ("endpoint".to_string(), endpoint),
                    ("region".to_string(), "us-east-1".to_string()),
                    ("access_key_id".to_string(), access_key),
                    ("secret_access_key".to_string(), secret_key),
                ]),
                ..StorageProviderConfig::default()
            }],
        };

        upload_to_all_storages(
            &storage,
            "cn",
            source_root.path(),
            std::slice::from_ref(&nested),
            StorageUploadOptions {
                selected_providers: &["seaweed".to_string()],
                public_read_include: &[],
                public_read_exclude: &[],
                remove_local: false,
                concurrency: 1,
                retry: &RetryConfig {
                    attempts: 2,
                    initial_backoff_ms: 50,
                    max_backoff_ms: 100,
                },
            },
        )
        .await
        .unwrap();

        let target = build_storage_operator_target(&storage, "seaweed", "cn").unwrap();
        let content = target
            .operator
            .read("music/file.txt")
            .await
            .unwrap()
            .to_vec();
        assert_eq!(content, b"hello seaweedfs");
    }
}
