# AssetStudio NativeAOT FFI

This branch adds an experimental NativeAOT backend for AssetStudio while keeping
the CLI backend as the default.

## Backends

Configure the backend with `tools.asset_studio_backend` or
`HARUKI_ASSET_STUDIO_BACKEND`.

- `cli`: run `AssetStudioModCLI` as a child process.
- `native`: load the NativeAOT shared library with `libloading`.
- `auto`: try `native`, then fall back to `cli` with a warning.

The native backend uses a JSON C ABI:

```c
int haruki_assetstudio_version(char** response_json);
int haruki_assetstudio_export(const char* request_json, char** response_json);
void haruki_assetstudio_free_string(char* ptr);
```

Rust still owns request/response parsing with `sonic-rs`. The C# adapter
serializes execution with a global lock because the AssetStudio export path uses
static process state.

## Native Library Layout

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

You can repeat a local comparison with the Rust benchmark helper:

```bash
cargo run --release --bin assetstudio_bench -- \
  --bundle /path/to/bundle.unityfs \
  --cli-path /path/to/AssetStudioModCLI \
  --native-library /path/to/HarukiAssetStudioNative.dylib \
  --backend cli,native \
  --warmup 1 \
  --iterations 5 \
  --expected-file music/jacket/jacket_s_712/jacket_s_712.png
```

The helper prints JSON with per-backend mean, median, min, max, and exported file
counts. It also calls `haruki_assetstudio_version` before native benchmarking so
ABI loading failures are separated from export failures.
