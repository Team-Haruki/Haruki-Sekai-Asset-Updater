# AssetStudio NativeAOT FFI

This branch adds a NativeAOT backend for AssetStudio. The production pipeline
now defaults to the AssetStudioFFI object flow plus media FFI. Rust controls
object selection, output paths, post-processing, upload, and records; C# keeps
Unity bundle parsing and object payload extraction behind the FFI boundary.

## Call Modes

The native backend has three call modes:

- `direct`: load the NativeAOT library in the current Rust process. This is the
  lowest-latency path for single calls and is what `AssetStudioFfiClient`
  uses, but it shares all AssetStudio process state with Rust.
- `process`: spawn the Rust `assetstudio_ffi_worker` sidecar for each FFI
  call. Each worker loads the same NativeAOT library in an isolated process, so
  Rust can run multiple exports concurrently without sharing AssetStudio's
  static process state.
- `pool`: keep a bounded pool of `assetstudio_ffi_worker` sidecars alive and
  send operation/result frames over a length-prefixed JSON protocol. The worker
  IPC remains JSON, but the worker calls AssetStudioFFI through typed C structs,
  not Native JSON request/response strings. Pool mode is the recommended export
  mode.

Image reads use the raw RGBA IR by default. Rust encodes the final PNG/WebP
outputs, which avoids the NativeAOT + ImageSharp PNG encoder path and keeps
alpha handling consistent. `tools.asset_studio_ffi_image_format` is retained
only as an override surface and currently accepts `raw_rgba`.

Configure it with `tools.asset_studio_ffi_call_mode` or
`HARUKI_ASSET_STUDIO_FFI_CALL_MODE`. Set
`tools.asset_studio_ffi_worker_path` or
`HARUKI_ASSET_STUDIO_FFI_WORKER_PATH` only when the worker binary is not next
to the current executable. Process and pool mode are limited by
`tools.asset_studio_ffi_process_concurrency` or
`HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY`; the default call mode is
`pool`, and the default process concurrency is `0`, meaning auto. Without CPU
throttle, auto uses the shared CPU budget as a hard worker cap. With CPU
throttle enabled, auto intentionally oversubscribes workers up to the CPU count
so the throttle can keep actual process CPU near the same budget. This keeps
NativeAOT workers isolated while avoiding the SIGSEGVs seen with unrestricted
in-process parallel exports.

The CPU performance control is intentionally centered on the shared CPU budget:
`concurrency.cpu_budget_ratio` and `concurrency.cpu_reserved` determine the
effective budget. For shared machines that need this program itself to stay near
the budget, enable `concurrency.cpu_throttle_enabled`; the throttle samples this
process tree, including native workers, and delays new CPU-heavy work when
sampled usage is above `effective_cpu_budget * 100%`.

The AssetStudioFFI boundary uses typed C structs. Rust loads and validates the
native ABI layout before calling:

```c
int haruki_assetstudio_abi_layout_v1(...);
int haruki_assetstudio_limits_v1(...);
int haruki_assetstudio_capabilities_v1(...);
int haruki_assetstudio_context_open_v1(...);
int haruki_assetstudio_context_list_objects_size_v1(...);
int haruki_assetstudio_context_list_objects_into_v1(...);
int haruki_assetstudio_read_objects_direct_retry_v1(...);
int haruki_assetstudio_context_close_v1(...);
void haruki_assetstudio_result_free(...);
```

The exact struct definitions live in Rust and the AssetStudioFFI project. Any
struct size or version mismatch is treated as a load-time failure.

The production path now prefers the double-FFI object flow:

```yaml
tools:
  asset_studio_ffi_call_mode: "pool"
  asset_studio_ffi_read_batch_size: 32
  asset_studio_ffi_read_kinds:
    Texture2D: image
    Sprite: image
    TextAsset: text_bytes
    MonoBehaviour: typetree_json
    AudioClip: audio
    Animator: fbx
    all: typetree_json
  media_backend: "ffi"
```

NativeAOT export-mode entry points have been removed in favor of object-level
context reads.

Build the Rust service with the media FFI feature for the production path:

```bash
cargo build --release --features media-ffi
```

For crash investigation, set `RUST_BACKTRACE=1` and enable C# adapter logging
with:

```bash
export HARUKI_ASSET_STUDIO_FFI_TRACE=1
export HARUKI_ASSET_STUDIO_FFI_WORKER_TRACE=1
export HARUKI_ASSET_STUDIO_FFI_LOG_DIR=/tmp/haruki-native-logs
export HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY=3
```

Each NativeAOT operation records an operation id, CLI-equivalent arguments,
captured console output, duration, and managed exception stack traces.

## Inspect Flow

Rust implements inspection through `context_open`, paged
`context_list_objects`, and `context_close`. There is no separate production
JSON inspect ABI.

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
  --ffi-library /path/to/HarukiAssetStudioFFI.dylib \
  --bundle /path/to/bundle.unityfs \
  --unity-version 2022.3.21f1 \
  --asset-types tex2d,textAsset \
  --filter-by-path-id -7162526471603727243
```

The command prints pretty JSON using `sonic-rs`.

Library callers can use the same native adapter through the Rust client API:

```rust
use haruki_sekai_asset_updater::{
    AssetStudioInspectOptions, AssetStudioFfiClient, AssetStudioObjectReadOptions,
    AssetStudioReadKind,
};

let client = AssetStudioFfiClient::new("/path/to/HarukiAssetStudioFFI.dylib");
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
            .image_format("raw_rgba"),
    )?;
    println!("read {} bytes", read.payload.len());
}
context.close()?;
# Ok::<(), haruki_sekai_asset_updater::core::errors::ExportPipelineError>(())
```

## Context Flow

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
  override them with `tools.asset_studio_ffi_read_kinds`.
- `image_format`: currently `raw_rgba`; Rust encodes final image files.

The typed response contains object metadata, payload kind, payload length,
warnings, timing, and errors. Object bytes are returned through typed payload
buffers and must be released with `haruki_assetstudio_result_free`. In
worker-pool mode the Rust worker returns a JSON operation/result frame and a
binary payload frame over its private process IPC. `tools.asset_studio_ffi_read_batch_size` or
`HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE` controls how many object reads Rust
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
`HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE=64` or
`--ffi-read-batch-size 64`; audio/text-heavy paths such as `music/short`
have recently favored `32`.

Worker-pool logs include worker spawn, recycle, kill, protocol error, request
id, operation, elapsed time, and completed-call counters when `RUST_LOG` enables
debug logs for the updater. Signal/protocol failures still trigger isolated
worker recovery, but the recovery path is observable instead of silent.

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
dotnet publish AssetStudioFFI/AssetStudioFFI.csproj \
  -c Release -f net9.0 -r linux-x64 --self-contained true \
  /p:TargetFrameworks=net9.0 \
  /p:PublishAot=true \
  /p:InvariantGlobalization=false \
  -o /app/assetstudio-ffi
```

Keep the NativeAOT adapter and the texture decoder native library together:

- Linux: `HarukiAssetStudioFFI.so`, `libTexture2DDecoderNative.so`
- macOS: `HarukiAssetStudioFFI.dylib`, `libTexture2DDecoderNative.dylib`
- Windows: `HarukiAssetStudioFFI.dll`, `Texture2DDecoderNative.dll`

Set `HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH` to the adapter library path. Rust
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
helper defaults to the production NativeAOT FFI backend.

```bash
cargo run --release --bin assetstudio_bench -- \
  --bundle /path/to/bundle.unityfs \
  --ffi-library /path/to/HarukiAssetStudioFFI.dylib \
  --warmup 1 \
  --iterations 5 \
  --expected-file music/jacket/jacket_s_712/jacket_s_712.png
```

The helper prints JSON with per-backend mean, median, min, max, exported file
counts, native phase timings, skipped object-read details, and object-read plan
diagnostics. It also queries typed capabilities before native
benchmarking so ABI loading failures are separated from export failures.

For full region-rule benchmarks, use `asset_region_bench`. Keep the production
default `tools.asset_studio_ffi_process_concurrency=0` for shared hosts, but
`music/short` has recently benchmarked best with `native_process=16` and
`media_encode=12`; `native_process=20` showed over-concurrency on the same
machine. The benchmark summary reports effective native process concurrency,
CPU budget, and CPU throttle target percent.

```bash
cargo run --release --features media-ffi --bin asset_region_bench -- \
  --config haruki-asset-configs.yaml \
  --region cn \
  --start-app-rule '^music/short' \
  --backend native \
  --media-backend ffi \
  --ffi-process-concurrency 16 \
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
