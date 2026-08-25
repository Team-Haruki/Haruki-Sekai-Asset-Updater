# unity-rs compatibility ABI samples

The updater itself does not use these samples. Its production path links the
pinned `unity-rs-core` crate and calls the pure Rust `Studio` API directly.

The Python and Node.js clients in this directory are retained for third-party
callers that deliberately build unity-rs's archived `assetstudio-ffi`
compatibility `cdylib`:

```bash
git clone https://github.com/Team-Haruki/unity-rs.git
cd unity-rs
git checkout 81b02a5
cargo build --release --manifest-path crates/assetstudio-ffi/Cargo.toml
```

The library is named `libassetstudio_ffi.dylib` on macOS,
`libassetstudio_ffi.so` on Linux, and `assetstudio_ffi.dll` on Windows. Pass its
path to either client:

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

These compatibility clients are not a service dependency and must not be used
to reintroduce a worker pool, stdio protocol, or dynamic library into the main
Rust application.
