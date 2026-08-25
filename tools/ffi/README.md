# unity-rs AssetStudio integration samples

These reference clients cover the production worker protocol and the archived
compatibility C ABI exposed by the pinned Rust `Team-Haruki/unity-rs` engine:

| Style | What it talks to | Samples |
| --- | --- | --- |
| Direct typed C ABI | `assetstudio-ffi` Rust `cdylib` in-process | `python/assetstudio_ffi.py`, `node/assetstudio_ffi.mjs` |
| Worker stdio protocol | This repository's self-contained `assetstudio_ffi_worker` | `go-worker/`, `python/assetstudio_worker_pool.py`, `node/assetstudio_worker_pool.mjs` |

The updater itself does not load a dynamic library. Its service binary and
worker both compile the pinned `unity-rs` revision in through Cargo. The direct
samples are only for third-party callers that deliberately build the archived
compatibility `cdylib`; upstream excludes that artifact from its normal build,
test and release paths.

## Worker samples

Build the self-contained worker from this repository:

```bash
cargo build --release --bin assetstudio_ffi_worker
```

Then point any worker client at a Unity bundle or `.assets` file. No engine
library path or .NET installation is required:

```bash
BUNDLE=/path/to/bundle-or-assets-file

# Go
cd tools/ffi/go-worker
go run . \
  --ffi-worker ../../../target/release/assetstudio_ffi_worker \
  --bundle "$BUNDLE" \
  --read-images

# Python
python3 tools/ffi/python/assetstudio_worker_pool.py \
  --ffi-worker target/release/assetstudio_ffi_worker \
  --bundle "$BUNDLE" \
  --read-images

# Node.js (zero dependencies)
node tools/ffi/node/assetstudio_worker_pool.mjs \
  --ffi-worker target/release/assetstudio_ffi_worker \
  --bundle "$BUNDLE" \
  --read-images
```

The worker protocol uses u64 little-endian length-prefixed JSON frames over
stdin/stdout. Small binary payloads follow the JSON response as a second frame;
large payloads use a `payload_file` handoff that the caller reads and deletes.

## Archived direct ABI samples

Build the `assetstudio-ffi` `cdylib` from the same pinned `unity-rs` revision
recorded in `crates/assetstudio-ffi/Cargo.toml`:

```bash
git clone https://github.com/Team-Haruki/unity-rs.git
cd unity-rs
git checkout 81b02a5
cargo build --release --manifest-path crates/assetstudio-ffi/Cargo.toml
```

The library is named `libassetstudio_ffi.dylib` on macOS,
`libassetstudio_ffi.so` on Linux, and `assetstudio_ffi.dll` on Windows. Pass its
path to the Python or Node client:

```bash
FFI=/path/to/unity-rs/crates/assetstudio-ffi/target/release/libassetstudio_ffi.dylib
BUNDLE=/path/to/bundle-or-assets-file

python3 tools/ffi/python/assetstudio_ffi.py \
  --ffi-library "$FFI" \
  --bundle "$BUNDLE" \
  --read-images

cd tools/ffi/node
npm install
node assetstudio_ffi.mjs \
  --ffi-library "$FFI" \
  --bundle "$BUNDLE" \
  --read-images
```

The direct clients validate capability, schema and native struct-layout
versions before opening a context. Batch reads use the direct-retry entry point
and release engine-owned buffers through `haruki_assetstudio_result_free`.
Images are returned as raw RGBA IR; final encoding remains the caller's job.

## Protocol notes

- Production and worker samples address reads by the stable list `index`, while
  the worker protocol retains `path_id` fallback compatibility for older
  clients. Indexes avoid ambiguity when several assets files reuse a path ID.
- Production uses the worker pool for crash isolation. Direct ABI calls place
  engine failures in the caller process.
