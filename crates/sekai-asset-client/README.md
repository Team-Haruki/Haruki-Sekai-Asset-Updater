# sekai-asset-client

Provider-aware HTTP transport for Project Sekai assets.

The crate resolves immutable releases, bootstraps runtime cookies, downloads
and decrypts asset manifests, and streams deobfuscated bundles to files with
hard response-size limits. It does not own batch scheduling, persistent cache
policy, publishing, or job state.

Runtime cookies, manifest keys, proxy settings, and request headers remain in
the client process. They are never added to the serializable
`sekai_asset_pipeline::BundleRequest` contract.

## Worker boundary

After a planner has pinned a `BundleRequest`, a worker downloads the exact
bundle to its own temporary directory and passes that file to the offline
pipeline:

```rust,no_run
use sekai_asset_client::SekaiAssetClient;
use sekai_asset_pipeline::{process_bundle, BundleRequest, PipelineOptions};
use std::path::Path;

async fn run(
    client: &SekaiAssetClient,
    request: &BundleRequest,
    options: &PipelineOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let downloaded = client
        .download_bundle_to_file(request, Path::new("/tmp/input.bundle"))
        .await?;
    let result = process_bundle(
        request,
        options,
        &downloaded.path,
        Path::new("/tmp/output"),
    )
    .await?;
    println!("{} artifact(s)", result.artifacts.artifacts.len());
    Ok(())
}
```

`ClientError::category()` separates transient network failures, permanent HTTP
responses, response-size violations, bundle size mismatches, file writes,
configuration errors, and manifest decode failures for queue retry decisions.
