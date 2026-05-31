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
int haruki_assetstudio_inspect(const char* request_json, char** response_json);
int haruki_assetstudio_export(const char* request_json, char** response_json);
void haruki_assetstudio_free_string(char* ptr);
```

Rust still owns request/response parsing with `sonic-rs`. The C# adapter
serializes execution with a global lock because the AssetStudio export path uses
static process state.

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
  --asset-types tex2d,textAsset
```

The command prints pretty JSON using `sonic-rs`.

Library callers can use the same native adapter through the Rust client API:

```rust
use haruki_sekai_asset_updater::{
    AssetStudioExportOptions, AssetStudioInspectOptions, AssetStudioNativeClient,
};

let client = AssetStudioNativeClient::new("/path/to/HarukiAssetStudioNative.dylib");
let options = AssetStudioInspectOptions::new("/path/to/bundle.unityfs")
    .asset_types(["tex2d"])
    .unity_version("2022.3.21f1");
let response = client.inspect(&options)?;
for asset in response.assets {
    println!("{:?} {:?}", asset.asset_type, asset.container);
}

let export_options =
    AssetStudioExportOptions::new("/path/to/bundle.unityfs", "/tmp/export")
        .export_path("music/jacket/jacket_s_712")
        .strip_path_prefix("assets/sekai/assetbundle/resources")
        .asset_types(["tex2d"])
        .unity_version("2022.3.21f1");
let export_response = client.export(&export_options)?;
println!("exported {} files", export_response.exported_files.len());
# Ok::<(), haruki_sekai_asset_updater::core::errors::ExportPipelineError>(())
```

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
