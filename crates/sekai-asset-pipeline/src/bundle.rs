use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AssetCategory, ExportPipelineError, ProviderKind, ResolvedRelease};

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

    /// Builds a deterministic manifest for files emitted below `output_root`.
    ///
    /// Files are resolved before hashing so a symlink cannot make a publisher
    /// read outside the trusted output tree. Duplicate file paths are hashed
    /// once, and serialized paths always use `/` separators.
    pub fn from_files(output_root: &Path, files: &[PathBuf]) -> Result<Self, ExportPipelineError> {
        if files.is_empty() {
            return Ok(Self::default());
        }

        let canonical_root = canonicalize(output_root)?;
        let mut canonical_files = files
            .iter()
            .map(|path| {
                let candidate = if path.is_absolute() {
                    path.clone()
                } else {
                    output_root.join(path)
                };
                let canonical_file = canonicalize(&candidate)?;
                if !canonical_file.starts_with(&canonical_root) {
                    return Err(ExportPipelineError::InvalidArtifactPath {
                        path: candidate,
                        reason: "path escapes the output root".to_string(),
                    });
                }
                Ok(canonical_file)
            })
            .collect::<Result<Vec<_>, ExportPipelineError>>()?;
        canonical_files.sort();
        canonical_files.dedup();

        let mut artifacts = Vec::with_capacity(canonical_files.len());
        for path in canonical_files {
            let relative = path.strip_prefix(&canonical_root).map_err(|_| {
                ExportPipelineError::InvalidArtifactPath {
                    path: path.clone(),
                    reason: "path escapes the output root".to_string(),
                }
            })?;
            let relative_path = relative
                .to_str()
                .ok_or_else(|| ExportPipelineError::InvalidArtifactPath {
                    path: path.clone(),
                    reason: "path is not valid UTF-8".to_string(),
                })?
                .replace(std::path::MAIN_SEPARATOR, "/");
            if relative_path.is_empty() {
                return Err(ExportPipelineError::InvalidArtifactPath {
                    path,
                    reason: "artifact must be a file below the output root".to_string(),
                });
            }

            let (size, sha256) = hash_file(&path)?;
            artifacts.push(Artifact {
                relative_path,
                size,
                sha256,
            });
        }

        let mut manifest = Self { artifacts };
        manifest.canonicalize();
        Ok(manifest)
    }
}

fn canonicalize(path: &Path) -> Result<PathBuf, ExportPipelineError> {
    fs::canonicalize(path).map_err(|source| ExportPipelineError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn hash_file(path: &Path) -> Result<(u64, String), ExportPipelineError> {
    let mut file = File::open(path).map_err(|source| ExportPipelineError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ExportPipelineError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        size += read as u64;
    }
    Ok((size, hex::encode(digest.finalize())))
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
    use std::fs;

    use tempfile::tempdir;

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

    #[test]
    fn artifact_manifest_hashes_sorts_and_deduplicates_files() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let first = nested.join("a.txt");
        let second = dir.path().join("z.txt");
        fs::write(&first, b"abc").unwrap();
        fs::write(&second, b"z").unwrap();

        let manifest =
            ArtifactManifest::from_files(dir.path(), &[second, first.clone(), first]).unwrap();

        assert_eq!(manifest.artifacts.len(), 2);
        assert_eq!(manifest.artifacts[0].relative_path, "nested/a.txt");
        assert_eq!(manifest.artifacts[0].size, 3);
        assert_eq!(
            manifest.artifacts[0].sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(manifest.artifacts[1].relative_path, "z.txt");
    }

    #[test]
    fn artifact_manifest_rejects_files_outside_the_output_root() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let file = outside.path().join("outside.txt");
        fs::write(&file, b"outside").unwrap();

        let error = ArtifactManifest::from_files(root.path(), &[file]).unwrap_err();

        assert!(matches!(
            error,
            crate::ExportPipelineError::InvalidArtifactPath { .. }
        ));
    }

    #[test]
    fn artifact_manifest_handles_relative_empty_and_invalid_inputs() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("relative.txt"), b"relative").unwrap();
        let manifest =
            ArtifactManifest::from_files(dir.path(), &[std::path::PathBuf::from("relative.txt")])
                .unwrap();
        assert_eq!(manifest.artifacts[0].relative_path, "relative.txt");
        assert!(ArtifactManifest::from_files(dir.path(), &[])
            .unwrap()
            .artifacts
            .is_empty());
        assert!(ArtifactManifest::from_files(dir.path(), &[dir.path().to_path_buf()]).is_err());
        assert!(ArtifactManifest::from_files(
            &dir.path().join("missing-root"),
            &[std::path::PathBuf::from("missing")],
        )
        .is_err());
        assert!(
            ArtifactManifest::from_files(dir.path(), &[std::path::PathBuf::from("missing")],)
                .is_err()
        );
    }
}
