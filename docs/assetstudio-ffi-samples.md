# unity-rs AssetStudio samples

The updater links the pinned `Team-Haruki/unity-rs` `unity-rs-core` crate and
calls its pure Rust `Studio` API directly. No AssetStudio worker, NativeAOT
library, stdio protocol, or .NET toolchain is part of the runtime or build chain.

Cross-language examples are maintained in
[`tools/ffi/README.md`](../tools/ffi/README.md). It documents:

- Python and Node.js clients for the archived compatibility `unity-rs` Rust
  `cdylib` ABI.

Those examples exist only for third-party ABI consumers. They are not part of
the production service path.
