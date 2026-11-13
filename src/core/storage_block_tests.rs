use super::*;
use std::fs;
use tempfile::tempdir;

fn storage(root: &Path) -> StorageConfig {
    StorageConfig {
        providers: vec![StorageProviderConfig {
            scheme: "fs".into(),
            root: Some(root.to_string_lossy().into_owned()),
            ..StorageProviderConfig::default()
        }],
    }
}

async fn publish(
    config: &StorageConfig,
    root: &Path,
    files: &[PathBuf],
    remove_local: bool,
) -> Result<(), StorageError> {
    upload_to_all_storages(
        config,
        "jp",
        root,
        files,
        StorageUploadOptions {
            selected_providers: &[],
            public_read_include: &[],
            public_read_exclude: &[],
            remove_local,
            concurrency: 4,
            retry: &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        },
    )
    .await
}

#[tokio::test]
async fn failed_wave_keeps_old_descriptor_and_markers_until_retry_succeeds() {
    let source = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let config = storage(remote.path());
    let folder = source.path().join("cue.sequence");
    let target = remote.path().join("cue.sequence");
    fs::create_dir(&folder).unwrap();
    fs::create_dir(&target).unwrap();
    let descriptor = folder.join("sequence.json");
    let wave = folder.join("wave-7.mp3");
    fs::write(&descriptor, b"new descriptor").unwrap();
    fs::write(target.join("sequence.json"), b"old descriptor").unwrap();
    fs::write(target.join("unsupported.json"), b"unsupported").unwrap();
    fs::write(target.join("retired.json"), b"retired").unwrap();
    let files = vec![descriptor.clone(), wave.clone()];
    assert!(publish(&config, source.path(), &files, true).await.is_err());
    assert_eq!(
        fs::read(target.join("sequence.json")).unwrap(),
        b"old descriptor"
    );
    assert!(target.join("unsupported.json").exists());
    assert!(target.join("retired.json").exists());
    assert!(descriptor.exists());
    fs::write(&wave, b"mp3").unwrap();
    publish(&config, source.path(), &files, false)
        .await
        .unwrap();
    assert_eq!(
        fs::read(target.join("sequence.json")).unwrap(),
        b"new descriptor"
    );
    assert_eq!(fs::read(target.join("wave-7.mp3")).unwrap(), b"mp3");
    assert!(!target.join("unsupported.json").exists());
    assert!(!target.join("retired.json").exists());
    // Cleanup is idempotent even when markers were already removed.
    publish(&config, source.path(), &files, true).await.unwrap();
    assert!(!descriptor.exists());
    assert!(!wave.exists());
}

#[tokio::test]
async fn diagnostics_invalidate_remote_descriptors_and_suppress_stale_scanned_files() {
    for (marker_name, opposite) in [
        ("retired.json", "unsupported.json"),
        ("unsupported.json", "retired.json"),
    ] {
        let source = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let folder = source.path().join("cue.sequence");
        let target = remote.path().join("cue.sequence");
        fs::create_dir(&folder).unwrap();
        fs::create_dir(&target).unwrap();
        let marker = folder.join(marker_name);
        let descriptor = folder.join("sequence.json");
        fs::write(&marker, b"diagnostic").unwrap();
        fs::write(&descriptor, b"stale").unwrap();
        fs::write(target.join("sequence.json"), b"previous").unwrap();
        fs::write(target.join(opposite), b"previous marker").unwrap();
        let files = vec![descriptor, marker];
        for _ in 0..2 {
            publish(&storage(remote.path()), source.path(), &files, false)
                .await
                .unwrap();
            assert!(!target.join("sequence.json").exists());
            assert!(!target.join(opposite).exists());
            assert_eq!(fs::read(target.join(marker_name)).unwrap(), b"diagnostic");
        }
    }
}

#[tokio::test]
async fn cleanup_failure_retains_local_inputs_for_retry() {
    let source = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let folder = source.path().join("cue.sequence");
    let target = remote.path().join("cue.sequence");
    fs::create_dir(&folder).unwrap();
    fs::create_dir_all(target.join("unsupported.json")).unwrap();
    fs::write(target.join("unsupported.json/child"), b"block deletion").unwrap();
    let descriptor = folder.join("sequence.json");
    fs::write(&descriptor, b"new").unwrap();
    let config = storage(remote.path());
    let files = vec![descriptor.clone()];
    assert!(publish(&config, source.path(), &files, true).await.is_err());
    assert!(descriptor.exists());
    fs::remove_dir_all(target.join("unsupported.json")).unwrap();
    publish(&config, source.path(), &files, true).await.unwrap();
    assert!(!descriptor.exists());
}

#[tokio::test]
async fn dependency_barrier_covers_every_storage_target() {
    let source = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let blocked = tempdir().unwrap();
    let folder = source.path().join("cue.sequence");
    fs::create_dir(&folder).unwrap();
    let descriptor = folder.join("sequence.json");
    let wave = folder.join("wave-7.mp3");
    fs::write(&descriptor, b"new").unwrap();
    fs::write(&wave, b"audio").unwrap();
    fs::create_dir_all(blocked.path().join("cue.sequence/wave-7.mp3")).unwrap();
    let mut config = storage(remote.path());
    config.providers.extend(storage(blocked.path()).providers);
    assert!(publish(&config, source.path(), &[descriptor, wave], false)
        .await
        .is_err());
    for root in [remote.path(), blocked.path()] {
        assert!(!root.join("cue.sequence/sequence.json").exists());
    }
}

#[tokio::test]
async fn failed_raw_upload_does_not_publish_its_unsupported_marker() {
    let source = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let folder = source.path().join("cue.sequence");
    let target = remote.path().join("cue.sequence");
    fs::create_dir(&folder).unwrap();
    fs::create_dir(&target).unwrap();
    let marker = folder.join("unsupported.json");
    let raw = folder.join("source-0123456789abcdef.acb.raw");
    fs::write(&marker, b"diagnostic").unwrap();
    fs::write(target.join("sequence.json"), b"previous").unwrap();
    // The raw ACB is missing locally, so its upload fails before any marker is sent.
    let config = storage(remote.path());
    let files = vec![marker.clone(), raw.clone()];
    assert!(publish(&config, source.path(), &files, true).await.is_err());
    assert!(!target.join("unsupported.json").exists());
    assert_eq!(fs::read(target.join("sequence.json")).unwrap(), b"previous");
    assert!(marker.exists());
    fs::write(&raw, b"acb").unwrap();
    publish(&config, source.path(), &files, true).await.unwrap();
    assert_eq!(
        fs::read(target.join("unsupported.json")).unwrap(),
        b"diagnostic"
    );
    assert!(!target.join("sequence.json").exists());
}
