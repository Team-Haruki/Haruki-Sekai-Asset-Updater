use serde::{Deserialize, Serialize};

use crate::AssetCategory;

/// A bundle pinned to one resolved asset release.
///
/// This is the stable hand-off between manifest planning and a single-bundle
/// worker. Batch priority, progress, cancellation, and application-specific
/// staging flags belong to the caller rather than this contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedBundle {
    /// Canonical logical name from the asset manifest.
    pub bundle_path: String,
    /// Provider-specific relative path used to download the payload.
    pub download_path: String,
    /// Manifest hash or CRC that identifies the exact payload revision.
    pub revision: String,
    pub category: AssetCategory,
    pub file_size: i64,
}

#[cfg(test)]
mod tests {
    use super::ResolvedBundle;
    use crate::AssetCategory;

    #[test]
    fn resolved_bundle_round_trips_as_a_worker_contract() {
        let bundle = ResolvedBundle {
            bundle_path: "music/short/0001".to_string(),
            download_path: "startapp/music/short/0001".to_string(),
            revision: "12345".to_string(),
            category: AssetCategory::StartApp,
            file_size: 42,
        };

        let encoded = rmp_serde::to_vec_named(&bundle).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<ResolvedBundle>(&encoded).unwrap(),
            bundle
        );
    }
}
