# AssetStudioFFI Samples

This repository keeps small Python and Go samples for two ways to call
AssetStudioFFI:

- direct typed FFI: caller process loads `HarukiAssetStudioFFI` and calls the C ABI;
- worker pool bridge: caller process talks to `assetstudio_ffi_worker --server` with framed
  JSON IPC, and the worker calls the typed C ABI.

Use the published AssetStudioFFI directory for real image reads. The `publish`
directory keeps native dependencies such as `libTexture2DDecoderNative` beside
`HarukiAssetStudioFFI`, while the `native` directory may only contain the main
library.

```bash
export HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=/Users/seiun/RiderProjects/AssetStudio/AssetStudioFFI/bin/Release/net9.0/osx-arm64/publish/HarukiAssetStudioFFI.dylib
export HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=/Users/seiun/RustroverProjects/Haruki-Sekai-Asset-Updater/target/release/assetstudio_ffi_worker
export HARUKI_SAMPLE_BUNDLE=/Users/seiun/RustroverProjects/Haruki-Sekai-Asset-Updater/Data/asset-bundle-cache/cn/character/member/res020_no006
```

## Direct FFI

Direct FFI is the shortest path and has the least IPC overhead:

```text
caller process -> typed C ABI -> AssetStudioFFI
```

Python sample:

```bash
python3 tools/ffi/python/assetstudio_ffi.py \
  --ffi-library "$HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH" \
  --bundle "$HARUKI_SAMPLE_BUNDLE" \
  --read-images
```

Go sample:

```bash
cd tools/ffi/go
go run . \
  --ffi-library "$HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH" \
  --bundle "$HARUKI_SAMPLE_BUNDLE" \
  --read-images
```

The Go direct sample uses cgo and `dlopen`/`dlsym`, then validates ABI layout
sizes before opening a context.

## Worker Pool Bridge

The worker pool bridge isolates the NativeAOT call stack in child processes and
is closer to the main Rust service:

```text
caller process -> JSON IPC frame -> assetstudio_ffi_worker -> typed C ABI -> AssetStudioFFI
```

Frame format:

- 8-byte little-endian `u64` frame length;
- UTF-8 JSON request or response body;
- if `payload_len > 0` and `payload_file` is empty, the binary payload follows
  as a second frame;
- if `payload_file` is present, the caller reads that temporary file and deletes
  it.

Python worker-pool sample:

```bash
python3 tools/ffi/python/assetstudio_worker_pool.py \
  --ffi-library "$HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH" \
  --ffi-worker "$HARUKI_ASSET_STUDIO_FFI_WORKER_PATH" \
  --bundle "$HARUKI_SAMPLE_BUNDLE" \
  --pool-size 2 \
  --read-images
```

Go worker-pool sample:

```bash
cd tools/ffi/go-worker
go run . \
  --ffi-library "$HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH" \
  --ffi-worker "$HARUKI_ASSET_STUDIO_FFI_WORKER_PATH" \
  --bundle "$HARUKI_SAMPLE_BUNDLE" \
  --pool-size 2 \
  --read-images
```

The Rust crate `crates/assetstudio-ffi` contains both pieces: `native.rs` is the
direct typed adapter, while `worker_pool.rs` and `assetstudio_ffi_worker` provide
the process bridge used by the main application.

## Tradeoffs

Direct FFI usually wins on latency and avoids JSON IPC, but the caller process is
responsible for ABI loading, stack sizing, and native crash isolation.

Worker pools add process and JSON frame overhead, but make crash isolation,
worker recycling, and stack sizing much easier. This is why the Rust service uses
the worker pool path for production AssetStudio calls.
