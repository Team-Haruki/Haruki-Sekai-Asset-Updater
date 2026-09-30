//! Complete inventory publication. Immutable shards are uploaded first; the
//! small current pointer is written only after a second inventory agrees.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use opendal::Operator;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::AssetExecutionError;
use crate::core::storage::build_asset_index_targets;

const ROOT: &str = "indexes/assets/v1/";
const MAX_OBJECTS: usize = 500_000;
const MAX_BLOB_BYTES: u64 = 64 << 20;

#[derive(Debug, Error)]
pub enum AssetIndexError {
    #[error("asset index publication cancelled")]
    Cancelled,
    #[error("invalid asset index: {0}")]
    Invalid(String),
    #[error(transparent)]
    Storage(#[from] opendal::Error),
    #[error(transparent)]
    Json(#[from] sonic_rs::Error),
    #[error("asset index publication failed at {stage} for provider {provider}: {source}")]
    Stage {
        stage: &'static str,
        provider: String,
        #[source]
        source: Box<AssetIndexError>,
    },
    #[error("asset index object {key}: {source}")]
    Object {
        key: String,
        #[source]
        source: Box<AssetIndexError>,
    },
}

impl AssetIndexError {
    fn at_stage(self, stage: &'static str, provider: &str) -> Self {
        if matches!(self, Self::Cancelled) {
            return self;
        }
        Self::Stage {
            stage,
            provider: provider.to_string(),
            source: Box::new(self),
        }
    }

    pub(crate) fn at_object(self, key: &str) -> Self {
        if matches!(self, Self::Cancelled) {
            return self;
        }
        Self::Object {
            key: key.to_string(),
            source: Box::new(self),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AssetObject {
    pub key: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AssetBlob {
    pub key: String,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AssetShardRef {
    pub prefix: String,
    #[serde(flatten)]
    pub blob: AssetBlob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AssetManifest {
    pub version: u32,
    pub region: String,
    pub revision: String,
    pub complete: bool,
    pub published_at: DateTime<Utc>,
    pub shards: Vec<AssetShardRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bpm: Option<AssetBlob>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AssetShard {
    pub version: u32,
    pub region: String,
    pub prefix: String,
    pub objects: Vec<AssetObject>,
}

#[derive(Debug)]
pub(crate) struct PreparedAssetIndex {
    pub manifest: AssetManifest,
    source_revision: String,
}

pub(crate) fn digest(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub(crate) fn check_cancelled(flag: &Option<Arc<AtomicBool>>) -> Result<(), AssetIndexError> {
    if flag
        .as_ref()
        .is_some_and(|flag| flag.load(Ordering::SeqCst))
    {
        Err(AssetIndexError::Cancelled)
    } else {
        Ok(())
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 1024
        && !key.contains('\\')
        && !key.chars().any(|c| c < ' ')
        && key
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn validate_blob(blob: &AssetBlob, prefix: &str) -> Result<(), AssetIndexError> {
    if !valid_key(&blob.key) || !blob.key.starts_with(prefix) || !valid_digest(&blob.sha256) {
        return Err(AssetIndexError::Invalid(
            "invalid blob reference".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn read_bounded(
    op: &Operator,
    key: &str,
    limit: u64,
) -> Result<Vec<u8>, AssetIndexError> {
    let size = op.stat(key).await?.content_length();
    if size > limit {
        return Err(AssetIndexError::Invalid(
            "object exceeds size limit".to_string(),
        ));
    }
    if size == 0 {
        return Ok(Vec::new());
    }
    // OpenDAL enforces exact ranges: requesting limit+1 for a small file is
    // an error. Read the bounded size observed by Stat; a concurrent growth
    // cannot increase this allocation, and immutable blobs verify the digest.
    Ok(op.read_with(key).range(0..size).await?.to_vec())
}

pub(crate) async fn read_blob(op: &Operator, blob: &AssetBlob) -> Result<Vec<u8>, AssetIndexError> {
    let data = read_bounded(op, &blob.key, MAX_BLOB_BYTES).await?;
    if digest(&data) != blob.sha256 {
        return Err(AssetIndexError::Invalid("blob digest mismatch".to_string()));
    }
    Ok(data)
}

async fn inventory(
    op: &Operator,
    region: &str,
    flag: &Option<Arc<AtomicBool>>,
) -> Result<(Vec<AssetObject>, String), AssetIndexError> {
    if !matches!(region, "jp" | "en" | "tw" | "kr" | "cn") {
        return Err(AssetIndexError::Invalid("unknown region".to_string()));
    }
    check_cancelled(flag)?;
    let prefix = format!("{region}-assets/");
    let mut lister = op.lister_with(&prefix).recursive(true).await?;
    let mut objects = Vec::new();
    while let Some(entry) = lister.try_next().await? {
        check_cancelled(flag)?;
        let metadata = entry.metadata();
        if metadata.is_dir() {
            continue;
        }
        if !metadata.is_file() || !entry.path().starts_with(&prefix) || !valid_key(entry.path()) {
            return Err(AssetIndexError::Invalid(
                "object outside inventory namespace".to_string(),
            ));
        }
        if objects.len() >= MAX_OBJECTS {
            return Err(AssetIndexError::Invalid(
                "object limit exceeded".to_string(),
            ));
        }
        objects.push(AssetObject {
            key: entry.path().to_string(),
            size: metadata.content_length(),
            etag: metadata
                .etag()
                .map(|etag| etag.trim_matches('"').to_string()),
            modified: metadata
                .last_modified()
                .map(|modified| modified.to_string()),
        });
    }
    objects.sort_by(|a, b| a.key.cmp(&b.key));
    if objects.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(AssetIndexError::Invalid(
            "duplicate inventory key".to_string(),
        ));
    }
    let revision = digest(&sonic_rs::to_vec(&objects)?);
    Ok((objects, revision))
}

fn object_group(key: &str) -> String {
    let parts: Vec<_> = key.split('/').collect();
    if parts.len() >= 4 {
        format!("{}/", parts[..3].join("/"))
    } else {
        format!("{}/", parts[0])
    }
}

fn manifest_revision(shards: &[AssetShardRef]) -> String {
    let mut refs: Vec<_> = shards.iter().collect();
    refs.sort_by(|a, b| a.prefix.cmp(&b.prefix));
    let mut bytes = Vec::new();
    for shard in refs {
        bytes.extend_from_slice(shard.prefix.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(shard.blob.sha256.as_bytes());
        bytes.push(b'\n');
    }
    digest(&bytes)
}

pub(crate) async fn prepare(
    op: &Operator,
    region: &str,
    flag: &Option<Arc<AtomicBool>>,
) -> Result<PreparedAssetIndex, AssetIndexError> {
    let (objects, source_revision) = inventory(op, region, flag).await?;
    if objects.is_empty() {
        return Err(AssetIndexError::Invalid(
            "refusing empty complete inventory".to_string(),
        ));
    }
    let mut groups: BTreeMap<String, Vec<AssetObject>> = BTreeMap::new();
    for object in objects {
        groups
            .entry(object_group(&object.key))
            .or_default()
            .push(object);
    }
    if groups.len() > 4096 {
        return Err(AssetIndexError::Invalid("shard count exceeded".to_string()));
    }
    let mut manifest = AssetManifest {
        version: 1,
        region: region.to_string(),
        revision: String::new(),
        complete: true,
        published_at: Utc::now(),
        shards: Vec::new(),
        bpm: None,
    };
    for (prefix, objects) in groups {
        check_cancelled(flag)?;
        let data = sonic_rs::to_vec(&AssetShard {
            version: 1,
            region: region.to_string(),
            prefix: prefix.clone(),
            objects,
        })?;
        if data.len() as u64 > MAX_BLOB_BYTES {
            return Err(AssetIndexError::Invalid(
                "shard exceeds size limit".to_string(),
            ));
        }
        let sha256 = digest(&data);
        let key = format!("{ROOT}{region}/shards/{sha256}.json");
        op.write_with(&key, data)
            .content_type("application/json")
            .await?;
        manifest.shards.push(AssetShardRef {
            prefix,
            blob: AssetBlob {
                key,
                sha256,
                revision: None,
            },
        });
    }
    manifest.revision = manifest_revision(&manifest.shards);
    // Repeated jobs can reuse a verified BPM index for identical resources.
    let pointer = format!("{ROOT}{region}/current.json");
    match read_bounded(op, &pointer, 1 << 20).await {
        Ok(bytes) => {
            if bytes.len() <= 1 << 20 {
                if let Ok(old) = sonic_rs::from_slice::<AssetManifest>(&bytes) {
                    if old.version == 1
                        && old.complete
                        && old.region == region
                        && old.revision == manifest.revision
                    {
                        if let Some(blob) = old.bpm {
                            if blob.revision.as_deref() == Some(manifest.revision.as_str())
                                && validate_blob(&blob, &format!("indexes/bpm/v1/{region}/"))
                                    .is_ok()
                                && read_blob(op, &blob).await.is_ok()
                            {
                                manifest.bpm = Some(blob);
                            }
                        }
                    }
                }
            }
        }
        Err(AssetIndexError::Storage(err)) if err.kind() == opendal::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    Ok(PreparedAssetIndex {
        manifest,
        source_revision,
    })
}

async fn validate_prepared(
    op: &Operator,
    prepared: &PreparedAssetIndex,
    flag: &Option<Arc<AtomicBool>>,
) -> Result<(), AssetIndexError> {
    let manifest = &prepared.manifest;
    if manifest.version != 1
        || !manifest.complete
        || manifest.shards.is_empty()
        || manifest.revision != manifest_revision(&manifest.shards)
    {
        return Err(AssetIndexError::Invalid(
            "invalid prepared manifest".to_string(),
        ));
    }
    let mut keys = HashSet::new();
    let mut prefixes = HashSet::new();
    let mut loaded_objects = Vec::new();
    for reference in &manifest.shards {
        check_cancelled(flag)?;
        if !reference
            .prefix
            .starts_with(&format!("{}-assets/", manifest.region))
            || !reference.prefix.ends_with('/')
            || !prefixes.insert(reference.prefix.clone())
        {
            return Err(AssetIndexError::Invalid(
                "invalid or duplicate shard prefix".to_string(),
            ));
        }
        validate_blob(
            &reference.blob,
            &format!("{ROOT}{}/shards/", manifest.region),
        )?;
        let bytes = read_blob(op, &reference.blob).await?;
        let shard: AssetShard = sonic_rs::from_slice(&bytes)?;
        if shard.version != 1 || shard.region != manifest.region || shard.prefix != reference.prefix
        {
            return Err(AssetIndexError::Invalid(
                "shard metadata mismatch".to_string(),
            ));
        }
        for object in shard.objects {
            if !object.key.starts_with(&reference.prefix)
                || !valid_key(&object.key)
                || !keys.insert(object.key.clone())
            {
                return Err(AssetIndexError::Invalid(
                    "duplicate or invalid shard object".to_string(),
                ));
            }
            loaded_objects.push(object);
        }
    }
    if let Some(blob) = &manifest.bpm {
        validate_blob(blob, &format!("indexes/bpm/v1/{}/", manifest.region))?;
        if blob.revision.as_deref() != Some(manifest.revision.as_str()) {
            return Err(AssetIndexError::Invalid(
                "BPM revision mismatch".to_string(),
            ));
        }
        read_blob(op, blob).await?;
    }
    loaded_objects.sort_by(|a, b| a.key.cmp(&b.key));
    if digest(&sonic_rs::to_vec(&loaded_objects)?) != prepared.source_revision {
        return Err(AssetIndexError::Invalid(
            "shards do not cover the complete inventory".to_string(),
        ));
    }
    let (_, revision) = inventory(op, &manifest.region, flag).await?;
    if revision != prepared.source_revision {
        return Err(AssetIndexError::Invalid(
            "assets changed during publication".to_string(),
        ));
    }
    check_cancelled(flag)
}

async fn write_pointer(
    op: &Operator,
    prepared: PreparedAssetIndex,
    flag: &Option<Arc<AtomicBool>>,
) -> Result<(), AssetIndexError> {
    let pointer = format!("{ROOT}{}/current.json", prepared.manifest.region);
    let data = sonic_rs::to_vec(&prepared.manifest)?;
    check_cancelled(flag)?;
    op.write_with(&pointer, data)
        .content_type("application/json")
        .await?;
    Ok(())
}

/// Called while JobManager still owns the region lock, after every requested
/// upload has completed successfully. All destinations are prepared before any
/// current pointer moves. Cross-provider pointer writes are not a transaction.
pub(crate) async fn publish_after_update(
    config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    failed: usize,
    flag: &Option<Arc<AtomicBool>>,
) -> Result<(), AssetExecutionError> {
    if !region.upload.enabled || !region.upload.publish_asset_index || failed != 0 {
        return Ok(());
    }
    let targets =
        build_asset_index_targets(&config.storage, region_name, &region.upload.providers)?;
    if targets.is_empty() {
        return Err(
            AssetIndexError::Invalid("no storage destinations configured".to_string()).into(),
        );
    }
    let started = std::time::Instant::now();
    let publication = async {
        let mut publications = Vec::new();
        for target in targets {
            let mut prepared = prepare(&target.operator, region_name, flag)
                .await
                .map_err(|err| err.at_stage("inventory_prepare", &target.provider))?;
            crate::core::asset_bpm::publish_bpm_index(&target.operator, &mut prepared.manifest, flag)
                .await
                .map_err(|err| err.at_stage("bpm_index", &target.provider))?;
            validate_prepared(&target.operator, &prepared, flag)
                .await
                .map_err(|err| err.at_stage("inventory_verify", &target.provider))?;
            publications.push((target, prepared));
        }
        for (target, prepared) in publications {
            let revision = prepared.manifest.revision.clone();
            let shard_count = prepared.manifest.shards.len();
            write_pointer(&target.operator, prepared, flag)
                .await
                .map_err(|err| err.at_stage("pointer_publish", &target.provider))?;
            tracing::info!(region = region_name, provider = %target.provider, revision, shard_count, elapsed_ms = started.elapsed().as_millis(), "complete asset index published");
        }
        Ok::<(), AssetIndexError>(())
    }.await;
    match publication {
        Err(AssetIndexError::Cancelled) => Err(AssetExecutionError::Cancelled),
        Err(err) => Err(err.into()),
        Ok(()) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::{StorageConfig, StorageProviderConfig};

    fn filesystem() -> (tempfile::TempDir, Operator) {
        let dir = tempfile::tempdir().unwrap();
        opendal::install_default();
        let op = Operator::via_iter(
            "fs",
            [("root".to_string(), dir.path().to_str().unwrap().to_string())],
        )
        .unwrap();
        (dir, op)
    }

    async fn seed(op: &Operator) {
        for key in [
            "jp-assets/ondemand/event/one/banner.png",
            "jp-assets/startapp/music/jacket/one.png",
            "jp-assets/root.txt",
        ] {
            op.write(key, "asset").await.unwrap();
        }
        op.write("kr-assets/ondemand/event/two/banner.png", "other region")
            .await
            .unwrap();
    }

    async fn publish_fixture(op: &Operator) -> AssetManifest {
        let prepared = prepare(op, "jp", &None).await.unwrap();
        let manifest = prepared.manifest.clone();
        validate_prepared(op, &prepared, &None).await.unwrap();
        write_pointer(op, prepared, &None).await.unwrap();
        manifest
    }

    #[tokio::test]
    async fn complete_inventory_is_deterministic_and_region_scoped() {
        let (_dir, op) = filesystem();
        seed(&op).await;
        let first = prepare(&op, "jp", &None).await.unwrap();
        assert!(op.stat("indexes/assets/v1/jp/current.json").await.is_err());
        assert_eq!(first.manifest.shards.len(), 3);
        let second = prepare(&op, "jp", &None).await.unwrap();
        assert_eq!(first.manifest.revision, second.manifest.revision);
        let mut keys = Vec::new();
        for reference in &first.manifest.shards {
            let shard: AssetShard =
                sonic_rs::from_slice(&read_blob(&op, &reference.blob).await.unwrap()).unwrap();
            keys.extend(shard.objects.into_iter().map(|object| object.key));
        }
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|key| key.starts_with("jp-assets/")));
        validate_prepared(&op, &first, &None).await.unwrap();
        write_pointer(&op, first, &None).await.unwrap();
        let pointer = op
            .read("indexes/assets/v1/jp/current.json")
            .await
            .unwrap()
            .to_vec();
        let manifest: AssetManifest = sonic_rs::from_slice(&pointer).unwrap();
        assert_eq!(manifest.revision, second.manifest.revision);
        assert!(manifest.complete);
    }

    #[tokio::test]
    async fn changed_inventory_corrupt_shard_and_cancel_do_not_advance_pointer() {
        let (_dir, op) = filesystem();
        seed(&op).await;
        publish_fixture(&op).await;
        let original = op
            .read("indexes/assets/v1/jp/current.json")
            .await
            .unwrap()
            .to_vec();
        let changed = prepare(&op, "jp", &None).await.unwrap();
        op.write("jp-assets/new.png", "new").await.unwrap();
        assert!(validate_prepared(&op, &changed, &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("changed"));
        let corrupt = prepare(&op, "jp", &None).await.unwrap();
        op.write(&corrupt.manifest.shards[0].blob.key, "corrupt")
            .await
            .unwrap();
        assert!(validate_prepared(&op, &corrupt, &None).await.is_err());
        let cancelled = Some(Arc::new(AtomicBool::new(true)));
        assert!(matches!(
            prepare(&op, "jp", &cancelled).await,
            Err(AssetIndexError::Cancelled)
        ));
        assert_eq!(
            original,
            op.read("indexes/assets/v1/jp/current.json")
                .await
                .unwrap()
                .to_vec()
        );
    }

    #[tokio::test]
    async fn missing_shard_cannot_be_published_as_complete() {
        let (_dir, op) = filesystem();
        seed(&op).await;
        let mut prepared = prepare(&op, "jp", &None).await.unwrap();
        prepared.manifest.shards.remove(0);
        prepared.manifest.revision = manifest_revision(&prepared.manifest.shards);
        assert!(validate_prepared(&op, &prepared, &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("complete inventory"));
        assert!(op.stat("indexes/assets/v1/jp/current.json").await.is_err());
    }

    #[tokio::test]
    async fn empty_and_invalid_regions_cannot_publish() {
        let (_dir, op) = filesystem();
        assert!(prepare(&op, "jp", &None).await.is_err());
        assert!(prepare(&op, "../jp", &None).await.is_err());
        op.write("jp-assets/a.png", "asset").await.unwrap();
        let mut prepared = prepare(&op, "jp", &None).await.unwrap();
        prepared.manifest.version = 9;
        assert!(validate_prepared(&op, &prepared, &None).await.is_err());
    }

    #[tokio::test]
    async fn identical_inventory_preserves_verified_bpm_reference() {
        let (_dir, op) = filesystem();
        seed(&op).await;
        let mut prepared = prepare(&op, "jp", &None).await.unwrap();
        let data = b"{}";
        let sha256 = digest(data);
        let key = format!("indexes/bpm/v1/jp/{sha256}.json");
        op.write(&key, data.as_slice()).await.unwrap();
        prepared.manifest.bpm = Some(AssetBlob {
            key: key.clone(),
            sha256,
            revision: Some(prepared.manifest.revision.clone()),
        });
        validate_prepared(&op, &prepared, &None).await.unwrap();
        write_pointer(&op, prepared, &None).await.unwrap();
        let unchanged = prepare(&op, "jp", &None).await.unwrap();
        assert_eq!(unchanged.manifest.bpm.unwrap().key, key);
        op.write("jp-assets/changed.png", "new").await.unwrap();
        assert!(prepare(&op, "jp", &None)
            .await
            .unwrap()
            .manifest
            .bpm
            .is_none());
    }

    #[tokio::test]
    async fn corrupted_prior_bpm_is_not_reused() {
        let (_dir, op) = filesystem();
        seed(&op).await;
        let mut manifest = publish_fixture(&op).await;
        manifest.bpm = Some(AssetBlob {
            key: "indexes/bpm/v1/jp/missing.json".to_string(),
            sha256: digest(b"missing"),
            revision: Some(manifest.revision.clone()),
        });
        op.write(
            "indexes/assets/v1/jp/current.json",
            sonic_rs::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();
        assert!(prepare(&op, "jp", &None)
            .await
            .unwrap()
            .manifest
            .bpm
            .is_none());
    }

    #[tokio::test]
    async fn failed_or_disabled_updates_do_not_touch_storage() {
        let config = AppConfig::default();
        let mut region = RegionConfig::default();
        publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .unwrap();
        region.upload.enabled = true;
        region.upload.publish_asset_index = true;
        region.upload.providers = vec!["missing-provider".to_string()];
        publish_after_update(&config, "jp", &region, 1, &None)
            .await
            .unwrap();
        assert!(publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn publication_target_uses_parent_namespace_and_keeps_asset_keys() {
        let (dir, op) = filesystem();
        seed(&op).await;
        let config = AppConfig {
            storage: StorageConfig {
                providers: vec![StorageProviderConfig {
                    name: Some("assets".to_string()),
                    scheme: "fs".to_string(),
                    root: Some(dir.path().join("jp-assets").to_str().unwrap().to_string()),
                    ..StorageProviderConfig::default()
                }],
            },
            ..AppConfig::default()
        };
        let targets = build_asset_index_targets(&config.storage, "jp", &[]).unwrap();
        assert_eq!(
            inventory(&targets[0].operator, "jp", &None)
                .await
                .unwrap()
                .0
                .len(),
            3
        );
        assert_eq!(
            inventory(&targets[0].operator, "kr", &None)
                .await
                .unwrap()
                .0
                .len(),
            1
        );
        let mut region = RegionConfig::default();
        region.upload.enabled = true;
        region.upload.publish_asset_index = true;
        let cancelled = Some(Arc::new(AtomicBool::new(true)));
        assert!(matches!(
            publish_after_update(&config, "jp", &region, 0, &cancelled).await,
            Err(AssetExecutionError::Cancelled)
        ));
    }

    #[test]
    fn inventory_paths_and_wire_revision_are_canonical() {
        for invalid in ["", "a//b", "/a", "a/../b", "a/./b", "a\\b", "a\nb", "a\0b"] {
            assert!(!valid_key(invalid), "accepted {invalid:?}");
        }
        assert!(!valid_key(&"x".repeat(1025)));
        assert!(valid_key("jp-assets/日本語.png"));
        assert!(!valid_digest("123"));
        let refs = vec![
            AssetShardRef {
                prefix: "b/".to_string(),
                blob: AssetBlob {
                    key: "b.json".to_string(),
                    sha256: "b".repeat(64),
                    revision: None,
                },
            },
            AssetShardRef {
                prefix: "a/".to_string(),
                blob: AssetBlob {
                    key: "a.json".to_string(),
                    sha256: "a".repeat(64),
                    revision: None,
                },
            },
        ];
        let bytes = format!("a/\0{}\nb/\0{}\n", "a".repeat(64), "b".repeat(64));
        assert_eq!(manifest_revision(&refs), digest(bytes.as_bytes()));
        assert!(validate_blob(&refs[0].blob, "other/").is_err());
    }
    #[tokio::test]
    async fn successful_update_publishes_and_later_destination_failure_keeps_pointers() {
        let (dir, op) = filesystem();
        seed(&op).await;
        let provider = StorageProviderConfig {
            name: Some("first".to_string()),
            scheme: "fs".to_string(),
            root: Some(dir.path().join("jp-assets").to_str().unwrap().to_string()),
            ..StorageProviderConfig::default()
        };
        let mut config = AppConfig {
            storage: StorageConfig {
                providers: vec![provider],
            },
            ..AppConfig::default()
        };
        let mut region = RegionConfig::default();
        region.upload.enabled = true;
        region.upload.publish_asset_index = true;
        publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .unwrap();
        let pointer = op
            .read("indexes/assets/v1/jp/current.json")
            .await
            .unwrap()
            .to_vec();
        let manifest: AssetManifest = sonic_rs::from_slice(&pointer).unwrap();
        assert!(manifest.bpm.is_some());
        op.write("jp-assets/next.png", "next").await.unwrap();
        let (second, second_op) = filesystem();
        config.storage.providers.push(StorageProviderConfig {
            name: Some("second".to_string()),
            scheme: "fs".to_string(),
            root: Some(
                second
                    .path()
                    .join("jp-assets")
                    .to_str()
                    .unwrap()
                    .to_string(),
            ),
            ..StorageProviderConfig::default()
        });
        assert!(publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .is_err());
        assert_eq!(
            pointer,
            op.read("indexes/assets/v1/jp/current.json")
                .await
                .unwrap()
                .to_vec()
        );
        assert!(second_op
            .stat("indexes/assets/v1/jp/current.json")
            .await
            .is_err());
        config.storage.providers.clear();
        assert!(publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("no storage destinations"));
    }

    #[tokio::test]
    async fn malformed_chart_reports_stage_and_keeps_previous_pointer() {
        let (dir, op) = filesystem();
        seed(&op).await;
        let config = AppConfig {
            storage: StorageConfig {
                providers: vec![StorageProviderConfig {
                    name: Some("assets".to_string()),
                    scheme: "fs".to_string(),
                    root: Some(dir.path().join("jp-assets").to_str().unwrap().to_string()),
                    ..StorageProviderConfig::default()
                }],
            },
            ..AppConfig::default()
        };
        let mut region = RegionConfig::default();
        region.upload.enabled = true;
        region.upload.publish_asset_index = true;
        publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .unwrap();
        let pointer = "indexes/assets/v1/jp/current.json";
        let previous = op.read(pointer).await.unwrap().to_vec();
        let chart = "jp-assets/ondemand/music/music_score/0001_01/expert.txt";
        op.write(chart, "broken chart").await.unwrap();
        let error = publish_after_update(&config, "jp", &region, 0, &None)
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("bpm_index"), "{message}");
        assert!(message.contains("provider assets"), "{message}");
        assert!(message.contains(chart), "{message}");
        assert_eq!(op.read(pointer).await.unwrap().to_vec(), previous);
        assert!(op.stat(chart).await.is_ok(), "uploads are not rolled back");
        assert!(matches!(
            AssetIndexError::Cancelled.at_object(chart),
            AssetIndexError::Cancelled
        ));
    }

    #[tokio::test]
    async fn bounded_reads_and_invalid_previous_pointer_are_handled() {
        let (_dir, op) = filesystem();
        op.write("empty", Vec::<u8>::new()).await.unwrap();
        assert!(read_bounded(&op, "empty", 0).await.unwrap().is_empty());
        op.write("larger", "12345").await.unwrap();
        assert!(read_bounded(&op, "larger", 4).await.is_err());
        assert_eq!(read_bounded(&op, "larger", 5).await.unwrap(), b"12345");
        seed(&op).await;
        op.write("indexes/assets/v1/jp/current.json", "invalid json")
            .await
            .unwrap();
        assert!(prepare(&op, "jp", &None)
            .await
            .unwrap()
            .manifest
            .bpm
            .is_none());
        op.write("jp-assets/invalid\nname", "asset").await.unwrap();
        assert!(prepare(&op, "jp", &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("namespace"));
    }

    #[tokio::test]
    async fn prepared_manifest_rejects_bad_prefix_metadata_and_bpm() {
        let (_dir, op) = filesystem();
        seed(&op).await;
        let mut prepared = prepare(&op, "jp", &None).await.unwrap();
        prepared.manifest.shards[0].prefix = "kr-assets/".to_string();
        prepared.manifest.revision = manifest_revision(&prepared.manifest.shards);
        assert!(validate_prepared(&op, &prepared, &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("prefix"));
        let mut prepared = prepare(&op, "jp", &None).await.unwrap();
        let reference = &mut prepared.manifest.shards[0];
        let mut shard: AssetShard =
            sonic_rs::from_slice(&read_blob(&op, &reference.blob).await.unwrap()).unwrap();
        shard.region = "kr".to_string();
        let data = sonic_rs::to_vec(&shard).unwrap();
        reference.blob.sha256 = digest(&data);
        op.write(&reference.blob.key, data).await.unwrap();
        prepared.manifest.revision = manifest_revision(&prepared.manifest.shards);
        assert!(validate_prepared(&op, &prepared, &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("metadata"));
        let mut prepared = prepare(&op, "jp", &None).await.unwrap();
        prepared.manifest.bpm = Some(AssetBlob {
            key: "indexes/bpm/v1/jp/file.json".to_string(),
            sha256: digest(b"{}"),
            revision: Some("wrong".to_string()),
        });
        assert!(validate_prepared(&op, &prepared, &None)
            .await
            .unwrap_err()
            .to_string()
            .contains("BPM revision"));
    }

    #[test]
    fn s3_inventory_target_preserves_namespace_and_rejects_wrong_roots() {
        for (root, want) in [
            ("", "/"),
            ("{region}-assets", "/"),
            ("namespace/{region}-assets", "/namespace/"),
        ] {
            let storage = StorageConfig {
                providers: vec![StorageProviderConfig {
                    scheme: "s3".to_string(),
                    root: Some(root.to_string()),
                    bucket: "assets".to_string(),
                    endpoint: "http://127.0.0.1:3900".to_string(),
                    region: Some("garage".to_string()),
                    ..StorageProviderConfig::default()
                }],
            };
            let targets = build_asset_index_targets(&storage, "jp", &[]).unwrap();
            assert_eq!(targets[0].operator.info().root(), want);
        }
        let storage = StorageConfig {
            providers: vec![StorageProviderConfig {
                scheme: "s3".to_string(),
                root: Some("wrong/jp".to_string()),
                bucket: "assets".to_string(),
                endpoint: "http://127.0.0.1:3900".to_string(),
                region: Some("garage".to_string()),
                ..StorageProviderConfig::default()
            }],
        };
        assert!(build_asset_index_targets(&storage, "jp", &[]).is_err());
    }
}
