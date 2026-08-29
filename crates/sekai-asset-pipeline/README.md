# Sekai Asset Pipeline

`sekai-asset-pipeline` is the reusable execution kernel shared by Haruki's
long-running updater and queue-based workers. It processes one immutable bundle
request at a time and has no HTTP server, in-memory job manager, publisher, or
batch scheduler.

## Boundary

The crate owns:

- provider and pinned release contracts;
- manifest parsing, AES decryption, and bundle deobfuscation;
- safe bundle/output paths;
- direct `unity-rs-core` export;
- CRI decoding and optional FFmpeg FFI media conversion;
- retry/resource options; and
- deterministic artifact manifests with relative paths, sizes, and SHA-256.

The caller owns:

- downloading and authentication;
- SQS/Lambda acknowledgement or long-running job state;
- batching, progress, and cancellation;
- S3 or filesystem publication;
- download records, Haruki 3D, and Git synchronization.

## Single-bundle API

The planner serializes a `BundleRequest` after resolving the region, release,
and manifest entry. The worker downloads and deobfuscates that exact payload,
then calls:

```rust,no_run
use sekai_asset_pipeline::{process_bundle, BundleRequest, PipelineOptions};
use std::path::Path;

async fn run(
    request: &BundleRequest,
    options: &PipelineOptions,
) -> Result<(), sekai_asset_pipeline::ExportPipelineError> {
    let result = process_bundle(
        request,
        options,
        Path::new("/tmp/input.bundle"),
        Path::new("/tmp/output"),
    )
    .await?;

    for artifact in result.artifacts.artifacts {
        println!("{} {} {}", artifact.relative_path, artifact.size, artifact.sha256);
    }
    Ok(())
}
```

`process_bundle` validates the logical bundle path, exports and post-processes
the input, and returns only files produced by that bundle. It does not upload or
delete them. Empty but structurally valid Unity containers return an empty
manifest; unrecognized or corrupt input returns an error so the caller can
retry or dead-letter the message.

For a separate deployment repository, pin the Haruki revision:

```toml
sekai-asset-pipeline = {
  git = "https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater.git",
  rev = "<commit>"
}
```

Local development can replace it with a path dependency. Enable `media-ffi`
only when the deployment provides the matching FFmpeg system libraries.
