use serde::{Deserialize, Serialize};

use crate::{AssetCategory, ProviderKind, ResolvedRelease};

/// One bundle entry resolved from an asset manifest.
///
/// Release and region identity deliberately live in [`BundleRequest`], so a
/// manifest entry cannot be mistaken for a complete worker message.
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

/// Immutable input for one bundle worker.
///
/// A planner resolves this once and serializes it into its queue message. A
/// worker therefore does not need to refetch the manifest and cannot combine
/// one bundle with a newer game release midway through a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BundleRequest {
    pub region: String,
    pub provider: ProviderKind,
    pub release: ResolvedRelease,
    pub bundle: ResolvedBundle,
}

/// One deterministic file produced by a bundle pipeline.
///
/// Paths are relative to the worker's output root. The digest lets a publisher
/// make idempotent decisions when multiple bundles resolve to the same object
/// key without relying on process-local coordination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Artifact {
    pub relative_path: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactManifest {
    pub artifacts: Vec<Artifact>,
}

impl ArtifactManifest {
    pub fn canonicalize(&mut self) {
        self.artifacts.sort_by(|left, right| {
            left.relative_path
                .cmp(&right.relative_path)
                .then_with(|| left.sha256.cmp(&right.sha256))
                .then_with(|| left.size.cmp(&right.size))
        });
        self.artifacts.dedup();
    }
}

/// Serializable result of processing one bundle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BundleResult {
    pub region: String,
    pub release: ResolvedRelease,
    pub bundle_path: String,
    pub revision: String,
    pub artifacts: ArtifactManifest,
}

#[cfg(test)]
mod tests {
    use super::{Artifact, ArtifactManifest, BundleRequest, BundleResult, ResolvedBundle};
    use crate::{AssetCategory, ProviderKind, ResolvedRelease};

    #[test]
    fn resolved_bundle_round_trips_as_a_manifest_entry() {
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

    #[test]
    fn worker_request_and_result_round_trip_without_runtime_state() {
        let release = ResolvedRelease {
            asset_version: "39".to_string(),
            asset_hash: "release-hash".to_string(),
        };
        let bundle = ResolvedBundle {
            bundle_path: "music/short/0001".to_string(),
            download_path: "startapp/music/short/0001".to_string(),
            revision: "bundle-revision".to_string(),
            category: AssetCategory::StartApp,
            file_size: 42,
        };
        let request = BundleRequest {
            region: "cn".to_string(),
            provider: ProviderKind::Nuverse,
            release: release.clone(),
            bundle: bundle.clone(),
        };
        let mut artifacts = ArtifactManifest {
            artifacts: vec![
                Artifact {
                    relative_path: "music/0001.mp3".to_string(),
                    size: 20,
                    sha256: "b".repeat(64),
                },
                Artifact {
                    relative_path: "music/0001.wav".to_string(),
                    size: 10,
                    sha256: "a".repeat(64),
                },
            ],
        };
        artifacts.canonicalize();
        let result = BundleResult {
            region: request.region.clone(),
            release,
            bundle_path: bundle.bundle_path,
            revision: bundle.revision,
            artifacts,
        };

        let request_bytes = rmp_serde::to_vec_named(&request).unwrap();
        let result_bytes = rmp_serde::to_vec_named(&result).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<BundleRequest>(&request_bytes).unwrap(),
            request
        );
        assert_eq!(
            rmp_serde::from_slice::<BundleResult>(&result_bytes).unwrap(),
            result
        );
        assert_eq!(
            result.artifacts.artifacts[0].relative_path,
            "music/0001.mp3"
        );
    }
}
