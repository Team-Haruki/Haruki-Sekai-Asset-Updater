# unity-rs AssetStudio samples

The updater and `assetstudio_ffi_worker` compile the pinned
`Team-Haruki/unity-rs` engine directly into their binaries. No AssetStudio
NativeAOT library or .NET toolchain is part of the runtime or build chain.

Cross-language examples are maintained in
[`tools/ffi/README.md`](../tools/ffi/README.md). It documents:

- Go, Python and Node.js clients for the self-contained worker protocol;
- Python and Node.js clients for the archived compatibility `unity-rs` Rust
  `cdylib` ABI;
- framed response payloads and spill-file ownership.

The production service uses the worker protocol because it provides process
isolation while keeping the engine revision compiled and pinned by Cargo.
