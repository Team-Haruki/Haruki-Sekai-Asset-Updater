# AssetStudioFFI language samples

Reference clients for the `HarukiAssetStudioFFI` NativeAOT library, in two
integration styles per language:

| Style | What it talks to | Samples |
|-------|------------------|---------|
| Direct typed C ABI | `HarukiAssetStudioFFI` dynamic library in-process | `go/`, `python/assetstudio_ffi.py`, `node/assetstudio_ffi.mjs` |
| Worker stdio protocol | `assetstudio_ffi_worker` subprocess (length-prefixed JSON frames) | `go-worker/`, `python/assetstudio_worker_pool.py`, `node/assetstudio_worker_pool.mjs` |

The direct samples follow the recommended SDK flow from `README.FFI.md` in the
AssetStudio repository: `capabilities_v1 -> abi_layout_v1 -> limits_v1 ->
context_open_v1 -> list size_v1/into_v1 (paged) -> read_objects_direct_retry_v1
-> result_free -> context_close_v1`, with struct-size verification against
`abi_layout_v1` at startup. The worker samples speak the same protocol the
updater uses in production (`crates/assetstudio-ffi`): u64 little-endian
length-prefixed JSON frames over stdin/stdout, inline payload frames below the
spill threshold and `payload_file` temp-file handoff above it.

## Prerequisites

Publish the NativeAOT library (requires .NET SDK 10):

```bash
cd /path/to/AssetStudio/AssetStudioFFI
dotnet publish -c Release -r osx-arm64 -f net10.0 --self-contained true \
  -p:TargetFrameworks=net10.0 -p:PublishAot=true -p:InvariantGlobalization=true
# output: bin/Release/net10.0/<rid>/publish/HarukiAssetStudioFFI.{dylib,so}
```

Shipped native dependencies (`Texture2DDecoderNative`, `AssetStudioFBXNative`,
`ooz`, `fmod`) are copied next to the library by the publish step and resolved
by the library itself from its own directory — no caller-side preloading is
needed. Set `HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH` (file, directory, or
path-list) only when the dependencies live somewhere else.

The worker samples additionally need the worker binary from this repository:

```bash
cargo build --release --bin assetstudio_ffi_worker
```

## Running

All samples take the same flags and print a JSON summary; `--read-images`
batch-reads every `Texture2D` as raw RGBA.

```bash
FFI=/path/to/publish/HarukiAssetStudioFFI.dylib
BUNDLE=/path/to/bundle-or-assets-file

# Go (direct; adjust the cgo include path in main.go to your AssetStudioFFI checkout)
cd go && go run . --ffi-library "$FFI" --bundle "$BUNDLE" --read-images

# Go (worker pool)
cd go-worker && go run . --ffi-library "$FFI" --ffi-worker ../../../target/release/assetstudio_ffi_worker --bundle "$BUNDLE" --read-images

# Python (direct, ctypes, no dependencies)
python3 python/assetstudio_ffi.py --ffi-library "$FFI" --bundle "$BUNDLE" --read-images

# Python (worker pool, no dependencies)
python3 python/assetstudio_worker_pool.py --ffi-library "$FFI" --ffi-worker target/release/assetstudio_ffi_worker --bundle "$BUNDLE" --read-images

# Node.js (direct, koffi)
cd node && npm install && node assetstudio_ffi.mjs --ffi-library "$FFI" --bundle "$BUNDLE" --read-images

# Node.js (worker pool, no dependencies)
node node/assetstudio_worker_pool.mjs --ffi-library "$FFI" --ffi-worker target/release/assetstudio_ffi_worker --bundle "$BUNDLE" --read-images
```

## Notes

- The typed ABI is versioned; every sample validates `abi_version` /
  `schema_version` / per-feature ABI versions and native struct sizes before
  the hot path, and fails fast on mismatch.
- Batch reads use the direct-retry entry point: pass caller-owned buffers when
  you have them, or null buffers to let the library allocate exact-size
  replacements owned by `result_handle` (release once with
  `haruki_assetstudio_result_free`).
- `image_format` defaults to `raw_rgba` natively; the FFI only returns raw RGBA
  IR — final image encoding is the caller's job.
- The worker `context_read_objects` request accepts an optional
  `payload_capacity_hint` (bytes). Above the worker's spill threshold the
  packed payload is streamed through a sparse temp file instead of memory.
- Reads address objects by `path_id`; when a context spans multiple asset
  files, colliding path ids resolve to the first match (the sample against a
  multi-file `.assets` context reports one such `unsupported_kind` item). Use
  the by-index read entry points keyed by the stable list `index` when that
  matters.
- On macOS the NativeAOT library has a known rare suspension crash; the worker
  samples inherit the production mitigation — each context lives in an
  isolated worker process, and a crashed worker surfaces as a closed-stdout
  error instead of taking the host down.
