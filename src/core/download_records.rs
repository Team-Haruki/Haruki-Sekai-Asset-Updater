use std::collections::BTreeMap;
use std::path::Path;

use crate::core::errors::DownloadRecordError;

pub type DownloadRecord = BTreeMap<String, String>;

pub fn load_download_record(path: impl AsRef<Path>) -> Result<DownloadRecord, DownloadRecordError> {
    let path = path.as_ref();
    match std::fs::read(path) {
        Ok(bytes) => sonic_rs::from_slice(&bytes).map_err(|source| DownloadRecordError::Parse {
            path: path.to_path_buf(),
            source,
        }),
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
    let data =
        sonic_rs::to_vec_pretty(record).map_err(|source| DownloadRecordError::Serialize {
            path: path.to_path_buf(),
            source,
        })?;
    std::fs::write(path, data).map_err(|source| DownloadRecordError::Write {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{load_download_record, save_download_record};

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
}
