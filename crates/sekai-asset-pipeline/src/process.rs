//! The transport-neutral execution boundary for one resolved bundle.

use std::path::Path;

use crate::{
    asset_category_name, export_unity_asset_bundle_payloads, post_process_exported_files,
    scoped_upload_files, validate_relative_bundle_path, ArtifactManifest, BundleRequest,
    BundleResult, ExportPipelineError, PipelineOptions,
};

/// Processes one already-downloaded and deobfuscated bundle.
///
/// Downloading, queue acknowledgement, publishing, and job progress remain the
/// caller's responsibility. This boundary is therefore usable by both the
/// long-running Haruki service and a one-message Lambda worker.
pub async fn process_bundle(
    request: &BundleRequest,
    options: &PipelineOptions,
    asset_bundle_file: &Path,
    output_dir: &Path,
) -> Result<BundleResult, ExportPipelineError> {
    validate_relative_bundle_path(&request.bundle.bundle_path)?;

    let payload_export = export_unity_asset_bundle_payloads(
        options,
        asset_bundle_file,
        &request.bundle.bundle_path,
        output_dir,
        asset_category_name(&request.bundle.category),
    )
    .await?;
    let summary = post_process_exported_files(
        options,
        &payload_export.export_path,
        payload_export.native_scoped_post_process,
        &payload_export.native_written_files,
        payload_export.native_acb_sources,
    )
    .await?;
    let files = scoped_upload_files(
        &payload_export.native_written_files,
        &summary.generated_files,
    );
    let artifacts = ArtifactManifest::from_files(&payload_export.export_root, &files)?;

    Ok(BundleResult {
        region: request.region.clone(),
        release: request.release.clone(),
        bundle_path: request.bundle.bundle_path.clone(),
        revision: request.bundle.revision.clone(),
        artifacts,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::process_bundle;
    use crate::test_support::{empty_unity_fs_bundle, processing_pipeline_options};
    use crate::{
        AssetCategory, BundleRequest, ExportPipelineError, PipelineError, ProviderKind,
        ResolvedBundle, ResolvedRelease,
    };

    fn request(bundle_path: &str) -> BundleRequest {
        BundleRequest {
            region: "cn".to_string(),
            provider: ProviderKind::Nuverse,
            release: ResolvedRelease {
                asset_version: "39".to_string(),
                asset_hash: "release-hash".to_string(),
            },
            bundle: ResolvedBundle {
                bundle_path: bundle_path.to_string(),
                download_path: "startapp/event_story/foo".to_string(),
                revision: "bundle-revision".to_string(),
                category: AssetCategory::StartApp,
                file_size: 0,
            },
        }
    }

    #[test]
    fn valid_empty_bundle_preserves_request_identity() {
        let dir = tempdir().unwrap();
        let bundle_file = dir.path().join("empty.bundle");
        fs::write(&bundle_file, empty_unity_fs_bundle()).unwrap();
        let request = request("event_story/foo");

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime
            .block_on(process_bundle(
                &request,
                &processing_pipeline_options(),
                &bundle_file,
                &dir.path().join("output"),
            ))
            .unwrap();

        assert_eq!(result.region, request.region);
        assert_eq!(result.release, request.release);
        assert_eq!(result.bundle_path, request.bundle.bundle_path);
        assert_eq!(result.revision, request.bundle.revision);
        assert!(result.artifacts.artifacts.is_empty());
    }

    #[test]
    fn invalid_bundle_path_is_rejected_before_export() {
        let dir = tempdir().unwrap();
        let missing_bundle_file = dir.path().join("does-not-exist.bundle");

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(process_bundle(
                &request("../escape"),
                &processing_pipeline_options(),
                &missing_bundle_file,
                &dir.path().join("output"),
            ))
            .unwrap_err();

        assert!(matches!(
            error,
            ExportPipelineError::Contract(PipelineError::InvalidBundlePath { .. })
        ));
    }
}
