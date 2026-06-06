use std::collections::BTreeMap;
use std::path::Path;

use opendal::Operator;

use crate::core::errors::DownloadRecordError;

pub type DownloadRecord = BTreeMap<String, String>;

pub fn load_download_record(path: impl AsRef<Path>) -> Result<DownloadRecord, DownloadRecordError> {
    let path = path.as_ref();
    match std::fs::read(path) {
        Ok(bytes) => parse_download_record(path, &bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(source) => Err(DownloadRecordError::Read {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub fn save_download_record(
    path: impl AsRef<Path>,
    record: &DownloadRecord,
) -> Result<(), DownloadRecordError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| DownloadRecordError::CreateParent {
            path: path.to_path_buf(),
            source,
        })?;
    }
    let data = serialize_download_record(path, record)?;
    std::fs::write(path, data).map_err(|source| DownloadRecordError::Write {
        path: path.to_path_buf(),
        source,
    })
}

pub fn parse_download_record(
    path: impl AsRef<Path>,
    bytes: &[u8],
) -> Result<DownloadRecord, DownloadRecordError> {
    let path = path.as_ref();
    sonic_rs::from_slice(bytes).map_err(|source| DownloadRecordError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn serialize_download_record(
    path: impl AsRef<Path>,
    record: &DownloadRecord,
) -> Result<Vec<u8>, DownloadRecordError> {
    let path = path.as_ref();
    sonic_rs::to_vec_pretty(record).map_err(|source| DownloadRecordError::Serialize {
        path: path.to_path_buf(),
        source,
    })
}

pub async fn load_download_record_from_storage(
    provider: &str,
    operator: &Operator,
    path: &str,
) -> Result<DownloadRecord, DownloadRecordError> {
    match operator.read(path).await {
        Ok(bytes) => parse_download_record(path, &bytes.to_vec()),
        Err(source) if source.kind() == opendal::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(source) => Err(DownloadRecordError::StorageRead {
            provider: provider.to_string(),
            path: path.to_string(),
            source,
        }),
    }
}

pub async fn save_download_record_to_storage(
    provider: &str,
    operator: &Operator,
    path: &str,
    record: &DownloadRecord,
) -> Result<(), DownloadRecordError> {
    let data = serialize_download_record(path, record)?;
    operator
        .write_with(path, data)
        .content_type("application/json")
        .await
        .map_err(|source| DownloadRecordError::StorageWrite {
            provider: provider.to_string(),
            path: path.to_string(),
            source,
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use opendal::Operator;

    use super::{
        load_download_record, load_download_record_from_storage, save_download_record,
        save_download_record_to_storage,
    };

    #[test]
    fn missing_file_returns_empty_record() {
        let dir = tempfile::tempdir().unwrap();
        let record = load_download_record(dir.path().join("missing.json")).unwrap();
        assert!(record.is_empty());
    }

    #[test]
    fn round_trip_persists_json_map() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("downloaded_assets.json");
        let mut record = BTreeMap::new();
        record.insert("music/test".to_string(), "deadbeef".to_string());

        save_download_record(&path, &record).unwrap();
        let loaded = load_download_record(&path).unwrap();

        assert_eq!(loaded, record);
    }

    #[tokio::test]
    async fn storage_round_trip_persists_json_map() {
        opendal::init_default_registry();
        let dir = tempfile::tempdir().unwrap();
        let operator = Operator::via_iter(
            "fs",
            BTreeMap::from([(
                "root".to_string(),
                dir.path().to_string_lossy().into_owned(),
            )]),
        )
        .unwrap();
        let mut record = BTreeMap::new();
        record.insert("music/test".to_string(), "deadbeef".to_string());

        let missing = load_download_record_from_storage("local", &operator, "missing.json")
            .await
            .unwrap();
        assert!(missing.is_empty());

        save_download_record_to_storage(
            "local",
            &operator,
            "state/downloaded_assets.json",
            &record,
        )
        .await
        .unwrap();
        let loaded =
            load_download_record_from_storage("local", &operator, "state/downloaded_assets.json")
                .await
                .unwrap();

        assert_eq!(loaded, record);
    }
}
