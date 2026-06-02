# AssetStudio NativeAOT FFI

This branch adds a NativeAOT backend for AssetStudio. The production pipeline
now defaults to the NativeAOT object flow plus media FFI; CLI/export-mode paths
remain as legacy compatibility and test entry points.

## Backends

Configure the backend with `tools.asset_studio_backend` or
`HARUKI_ASSET_STUDIO_BACKEND`.

- `cli`: run `AssetStudioModCLI` as a child process.
- `native`: load the NativeAOT shared library with `libloading`.
- `auto`: try `native`, then fall back to `cli` with a warning.

The native backend has three call modes:

- `direct`: load the NativeAOT library in the current Rust process. This is the
  lowest-latency path for single calls and is what `AssetStudioNativeClient`
  uses, but it shares all AssetStudio process state with Rust.
- `process`: spawn the Rust `assetstudio_native_worker` sidecar for each FFI
  call. Each worker loads the same NativeAOT library in an isolated process, so
  Rust can run multiple exports concurrently without sharing AssetStudio's
  static process state.
- `pool`: keep a bounded pool of `assetstudio_native_worker` sidecars alive and
  send FFI calls over a length-prefixed JSON protocol. This keeps the process
  isolation from `process` mode while avoiding per-bundle NativeAOT startup
  cost. Native export requests BMP surrogate output by default, then Rust
  converts it to PNG in parallel. This avoids the NativeAOT + ImageSharp PNG
  encoder bottleneck. Set `tools.asset_studio_native_image_format` or
  `HARUKI_ASSET_STUDIO_NATIVE_IMAGE_FORMAT` only when you need to force another
  first-stage format such as `png`, `tga`, `jpeg`, or `webp`. Pool mode is the
  recommended export mode.

Configure it with `tools.asset_studio_native_call_mode` or
`HARUKI_ASSET_STUDIO_NATIVE_CALL_MODE`. Set
`tools.asset_studio_native_worker_path` or
`HARUKI_ASSET_STUDIO_NATIVE_WORKER_PATH` only when the worker binary is not next
to the current executable. Process and pool mode are limited by
`tools.asset_studio_native_process_concurrency` or
`HARUKI_ASSET_STUDIO_NATIVE_PROCESS_CONCURRENCY`; the default call mode is
`pool`, and the default process concurrency is `3`. This keeps NativeAOT workers
isolated while avoiding the SIGSEGVs seen with unrestricted in-process parallel
exports.

The native backend uses a JSON C ABI:

```c
int haruki_assetstudio_version(char** response_json);
int haruki_assetstudio_inspect(const char* request_json, char** response_json);
int haruki_assetstudio_context_open(const char* request_json, char** response_json);
int haruki_assetstudio_context_list_objects(const char* request_json, char** response_json);
int haruki_assetstudio_context_read_object(
    const char* request_json,
    char** response_json,
    uint8_t** payload_ptr,
    int64_t* payload_len);
int haruki_assetstudio_context_read_objects(
    const char* request_json,
    char** response_json,
    uint8_t** payload_ptr,
    int64_t* payload_len);
int haruki_assetstudio_context_close(const char* request_json, char** response_json);
void haruki_assetstudio_free_string(char* ptr);
void haruki_assetstudio_free_buffer(uint8_t* ptr);
```

Rust still owns request/response parsing with `sonic-rs`. The C# adapter
serializes ordinary operations with a process gate because the AssetStudio export
path uses static process state. Context operations hold that gate from
`context_open` until `context_close`, so they should be used through one worker
process and closed promptly.

The production path now prefers the double-FFI object flow:

```yaml
tools:
  asset_studio_backend: "native"
  asset_studio_native_call_mode: "pool"
  asset_studio_native_unitypy_mode: true
  asset_studio_native_read_batch_size: 32
  asset_studio_native_read_kinds:
    Texture2D: image
    Sprite: image
    TextAsset: text_bytes
    MonoBehaviour: typetree_json
    AudioClip: audio
    Animator: fbx
    all: typetree_json
  media_backend: "ffi"
```

The Rust pipeline keeps control of object selection, output paths,
ACB/HCA/image handling, uploads, and records. CLI paths remain available only as
explicit legacy compatibility and benchmark fallbacks; NativeAOT export-mode
entry points have been removed in favor of object-level context reads.

Build the Rust service with the media FFI feature for the production path:

```bash
cargo build --release --features media-ffi
```

For crash investigation, set `RUST_BACKTRACE=1` and enable C# adapter logging
with:

```bash
export HARUKI_ASSET_STUDIO_NATIVE_TRACE=1
export HARUKI_ASSET_STUDIO_NATIVE_WORKER_TRACE=1
export HARUKI_ASSET_STUDIO_NATIVE_LOG_DIR=/tmp/haruki-native-logs
export HARUKI_ASSET_STUDIO_NATIVE_PROCESS_CONCURRENCY=3
export HARUKI_ASSET_STUDIO_NATIVE_MAX_EXPORT_TASKS=4
```

Each NativeAOT operation records an operation id, CLI-equivalent arguments,
captured console output, duration, and managed exception stack traces.

## Inspect ABI

`haruki_assetstudio_inspect` is the first higher-integration API beyond export.
It lets Rust load a bundle, parse assets, and receive metadata before deciding
what to export.

Request fields:

- `input_path`: bundle or extracted UnityFS path.
- `asset_types`: AssetStudio type filters such as `tex2d`, `textAsset`, or
  `audio`.
- `unity_version`: optional Unity version override.
- `filter_exclude_mode`, `filter_with_regex`, `filter_by_name`,
  `filter_by_container`: optional AssetStudio filters.
- `filter_by_path_ids`: exact asset `path_id` filters, intended for the
  inspect-then-export flow.
- `load_all_assets`: mirrors AssetStudio's load-all option.

Response fields:

- `success`, `error`, `warnings`, `duration_ms`.
- `assets_file_count`: loaded assets file count.
- `exportable_asset_count`: parsed/exportable asset count after filters.
- `unity_version`: version discovered from the first loaded assets file.
- `assets`: array of `index`, `name`, `container`, `type`, `type_id`,
  `path_id`, `size`, and `source_file`.

Rust has a helper CLI for local inspection:

```bash
cargo run --bin assetstudio_inspect -- \
  --native-library /path/to/HarukiAssetStudioNative.dylib \
  --bundle /path/to/bundle.unityfs \
  --unity-version 2022.3.21f1 \
  --asset-types tex2d,textAsset \
  --filter-by-path-id -7162526471603727243
```

The command prints pretty JSON using `sonic-rs`.

Library callers can use the same native adapter through the Rust client API:

```rust
use haruki_sekai_asset_updater::{
    AssetStudioInspectOptions, AssetStudioNativeClient, AssetStudioObjectReadOptions,
    AssetStudioReadKind,
};

let client = AssetStudioNativeClient::new("/path/to/HarukiAssetStudioNative.dylib");
let options = AssetStudioInspectOptions::new("/path/to/bundle.unityfs")
    .asset_types(["tex2d"])
    .unity_version("2022.3.21f1")
    .include_assets(false);
let mut context = client.open_context(&options)?;
let assets = context.list_all_objects()?;
for asset in &assets {
    println!("{:?} {:?}", asset.asset_type, asset.container);
}
if let Some(asset) = assets.first() {
    let read = context.read_object(
        &AssetStudioObjectReadOptions::new(asset.path_id)
            .kind(AssetStudioReadKind::Image)
            .image_format("bmp"),
    )?;
    println!("read {} bytes", read.payload.len());
}
context.close()?;
# Ok::<(), haruki_sekai_asset_updater::core::errors::ExportPipelineError>(())
```

## Context ABI

`haruki_assetstudio_context_open`,
`haruki_assetstudio_context_list_objects`,
`haruki_assetstudio_context_read_object`,
`haruki_assetstudio_context_read_objects`, and
`haruki_assetstudio_context_close` treat NativeAOT as a loaded Unity object
library. `context_open` accepts the same request shape as `inspect`, loads and
parses the bundle once, and returns a `context_id`. It can also return filtered
asset metadata immediately, but the production Rust worker now passes
`include_assets=false` and pages metadata with `context_list_objects`. This
keeps huge player resource files from putting the entire object list in the
open response frame.

`context_list_objects` accepts:

- `context_id`: the open context handle.
- `offset`: zero-based object offset.
- `limit`: maximum objects to return.

The response contains `assets`, `total_count`, `returned_count`, and
`next_offset`; `next_offset=null` means the page stream is complete. The worker
pool uses a page size of `4096`, then records `context_list.pages`,
`context_list.objects`, and max `context_list.duration_ms` in bundle timing
diagnostics.

`context_open` should pass specific `asset_types` whenever possible; requesting
`all` or `*` intentionally retains all metadata and can be very large for
player resource files. `context_read_object` accepts:

- `context_id`: the open context handle.
- `path_id`: the exact Unity object path id.
- `kind`: `auto`, `raw`, `typetree_json`, `image`, `image_archive`,
  `audio`, `video`, `font`, `shader`, `text`, `text_bytes`, `mesh`, `obj`,
  `animator`, or `fbx`. Rust chooses defaults by AssetStudio type and can
  override them with `tools.asset_studio_native_read_kinds`.
- `image_format`: currently `bmp` or `png`; `bmp` is the fast default.

The response JSON contains object metadata, payload kind, payload length,
warnings, timing, and errors. Object bytes are returned through `payload_ptr` and
must be released with `haruki_assetstudio_free_buffer`. In worker-pool mode the
Rust worker returns the response JSON and binary payload as separate length
prefixed frames. `tools.asset_studio_native_read_batch_size` or
`HARUKI_ASSET_STUDIO_NATIVE_READ_BATCH_SIZE` controls how many object reads Rust
packs into one `context_read_objects` request; the default is `32`.
Batch responses include diagnostics used by Rust benchmarks:
`worker_id`, `call_seq`, `object_count`, `payload_bundle_bytes`,
`failed_count`, `read_payload_ms`, and per managed phase `p50_ms` / `p95_ms`
stats. Rust records an object-read plan per bundle with inspected, planned,
successful, failed, skipped, batch count, payload bytes, and Native payload-read
time totals. The benchmark JSONL also receives `read_batch.<phase>.p50` and
`read_batch.<phase>.p95` entries. These are max-merged across batches because
percentiles should not be summed like ordinary elapsed-time counters.

Keep `32` as the global default. Image-heavy workloads such as
`character/member` can be re-benchmarked with
`HARUKI_ASSET_STUDIO_NATIVE_READ_BATCH_SIZE=64` or
`--native-read-batch-size 64`; audio/text-heavy paths such as `music/short`
have recently favored `32`.

Worker-pool logs include worker spawn, recycle, kill, protocol error, request
id, operation, elapsed time, and completed-call counters when `RUST_LOG` enables
debug logs for the updater. Signal/protocol failures still trigger isolated
worker recovery, but the recovery path is now observable instead of silent.

Rust derives output extensions from `payload_kind` first, then from the adapter
suggestion, and finally from the AssetStudio type. Important object-flow
defaults are:

- `raw` -> `.dat`
- `typetree_json` -> `.json`
- `text_bytes` -> `.bytes` unless the adapter identifies a more specific type
- `animator_bundle_fbx` and image array bundles -> directory payload bundles

`context_close` accepts the `context_id` and releases AssetStudio process state.
The current implementation supports one active context per NativeAOT process.
The Rust worker server keeps the process alive across open/read/close calls so
bundle loading is reused while Rust stays responsible for per-object decisions.

## Native Library Layout

Publish NativeAOT with `TargetFrameworks` forced to `net9.0`; otherwise
referenced AssetStudio projects can fall back to their `net472` target during
publish.

```bash
dotnet publish AssetStudioNative/AssetStudioNative.csproj \
  -c Release -f net9.0 -r linux-x64 --self-contained true \
  /p:TargetFrameworks=net9.0 \
  /p:PublishAot=true \
  /p:InvariantGlobalization=false \
  -o /app/assetstudio-native
```

Keep the NativeAOT adapter and the texture decoder native library together:

- Linux: `HarukiAssetStudioNative.so`, `libTexture2DDecoderNative.so`
- macOS: `HarukiAssetStudioNative.dylib`, `libTexture2DDecoderNative.dylib`
- Windows: `HarukiAssetStudioNative.dll`, `Texture2DDecoderNative.dll`

Set `HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH` to the adapter library path. Rust
also forwards that path to the adapter before calling it so the C# P/Invoke
resolver can find the decoder library next to the adapter.

## Benchmark Snapshot

Measured on macOS arm64 with the Rust release test binary and the real
`extract_unity_asset_bundle` path. Each row is one warmup run plus five measured
runs; the table reports the measured mean.

| Sample | CLI mean | Native mean | Result |
| --- | ---: | ---: | --- |
| `unityasset_long` | `0.738s` | `0.480s` | native used about `65%` of CLI time |
| `jacket_s_712` | `0.628s` | `0.332s` | native used about `53%` of CLI time |

The native backend helps most on small bundles where CLI process startup is a
large part of total latency. Larger exports still benefit, but Rust-side
post-processing reduces the visible backend delta.

You can repeat a local native-backend run with the Rust benchmark helper. The
helper defaults to the production NativeAOT FFI backend; pass
`--backend cli,native` only when you explicitly want a legacy CLI comparison.

```bash
cargo run --release --bin assetstudio_bench -- \
  --bundle /path/to/bundle.unityfs \
  --cli-path /path/to/AssetStudioModCLI \
  --native-library /path/to/HarukiAssetStudioNative.dylib \
  --warmup 1 \
  --iterations 5 \
  --expected-file music/jacket/jacket_s_712/jacket_s_712.png
```

The helper prints JSON with per-backend mean, median, min, max, exported file
counts, native phase timings, skipped object-read details, and object-read plan
diagnostics. It also calls `haruki_assetstudio_version` before native
benchmarking so ABI loading failures are separated from export failures.

For full region-rule benchmarks, use `asset_region_bench`. Keep the production
default `tools.asset_studio_native_process_concurrency=3`, but `music/short`
has recently benchmarked best with `native_process=16` and `media_encode=12`;
`native_process=20` showed over-concurrency on the same machine.

```bash
cargo run --release --features media-ffi --bin asset_region_bench -- \
  --config haruki-asset-configs.yaml \
  --region cn \
  --start-app-rule '^music/short' \
  --backend native \
  --media-backend ffi \
  --native-process-concurrency 16 \
  --media-encode-concurrency 12 \
  --jsonl-output /tmp/haruki-music-short-native16-media12.jsonl \
  --progress-every 500
```

When reading benchmark output, separate bundle active time from queueing:
`worker_pool.wait` is time spent waiting for a NativeAOT worker,
`post_process.queue_wait` is time spent waiting for the bundle post-process
slot, `post_process.hca.media_pool_wait` is time spent waiting for media encode
capacity, and `post_process.acb.hca_tracks_wall` is the wall time for the
bundle-level ACB/HCA track queue.
