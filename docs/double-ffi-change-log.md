# Double FFI Change Log

This document records the main design and implementation changes made while
moving the updater production path to AssetStudio NativeAOT FFI plus media FFI.

## Direction

The updater now treats Rust as the control plane and keeps Unity object reading
inside AssetStudio NativeAOT workers.

Rust owns:

- asset info fetch and bundle filtering
- download, cache, and deobfuscation
- object selection by configured AssetStudio types
- output path and extension policy
- ACB/HCA/image/USM post-processing
- media encode scheduling
- upload and downloaded record state
- benchmark telemetry and adaptive scheduling

AssetStudio NativeAOT owns:

- opening Unity bundles
- listing parsed objects
- reading object payloads by path id
- converting Unity object payloads into raw bytes, image payloads, text bytes,
  typetree JSON, audio data, shader/font/mesh/FBX-compatible payloads where
  supported

Legacy CLI paths remain available for explicit tests and benchmark comparisons,
but they are no longer the intended production path.

## NativeAOT FFI

The C# adapter was expanded from export-style calls to library-style object
APIs:

- `version`
- `inspect`
- `context_open`
- `context_list_objects`
- `context_read_object`
- `context_read_objects`
- `context_close`

The ABI still uses JSON for the control plane and a binary payload frame for
object data. This kept the interface stable while allowing Rust to avoid
cross-language struct layout problems.

Context open can omit the full asset list. Rust then pages metadata through
`context_list_objects`, which avoids oversized response frames for large player
resource files.

Batch object reads are the production path. `context_read_objects` returns:

- response JSON
- one binary payload bundle
- per-batch diagnostics such as worker id, call sequence, object count, payload
  bytes, failed object count, read payload time, and managed phase p50/p95 data

The old export-style NativeAOT path was removed from the Rust production path
and retained only as historical context in prior commits.

## Worker Pool

Production NativeAOT calls use the Rust `assetstudio_native_worker` sidecar in
`pool` mode. Each worker process loads the NativeAOT shared library once and
serves length-prefixed requests over stdin/stdout.

This gives the pipeline:

- process isolation from AssetStudio static state
- lower overhead than spawning one worker per call
- bounded concurrency
- worker recycling via `asset_studio_native_worker_max_calls`
- per-worker telemetry for call counts, restarts, wait time, and protocol
  errors

Direct in-process loading is still useful for smoke tests and client API usage,
but it is not the production default because unrestricted direct concurrency
previously caused native crashes.

Recent Rust changes also make the server-mode worker reuse a loaded
`LoadedAssetStudioNativeLibrary` instead of loading the shared library for every
request.

## Object Output

The object flow supports configured AssetStudio type selectors, including common
aliases such as `tex2d`, `tex2dArray`, `textAsset`, `monoBehaviour`, `audio`,
`font`, `shader`, `mesh`, and `animator`.

Default read kinds are type aware:

- `Texture2D`, `Sprite`, `Texture2DArrayImage`: `image`
- `TextAsset`: `text_bytes`
- `MonoBehaviour`: `typetree_json`
- `AudioClip`: `audio`
- `Shader`: `shader`
- `Font`: `font`
- `Mesh`: `mesh`
- `Animator`: `fbx`

Rust can override read behavior with
`tools.asset_studio_native_read_kinds`.

Output path behavior:

- `by_category=false`: `save_dir / bundle_path`
- `by_category=true`: `save_dir / container_relative_path`
- container paths strip `assets/sekai/assetbundle/resources` when possible
- CLI parity mode can force AssetStudio-like filename and extension behavior

CLI parity mode is intended for benchmark rigor, not normal production output.
It can be enabled with:

```bash
HARUKI_ASSET_STUDIO_NATIVE_CLI_PARITY_MODE=true
```

or with `asset_region_bench --native-cli-parity`.

## Media FFI

The production media backend uses Rust-side FFI:

- `cridecoder` handles ACB/HCA extraction and decode support
- `rsmpeg` handles FFmpeg-backed encode/remux work
- FFmpeg CLI remains available only when explicitly requested

The Rust pipeline now has a finer media scheduler and fast paths for common
single ACB / single HCA cases. The scheduler records queue wait, active worker
counts, encode wall time, and per-post-process phase timings.

`rsmpeg` is built with FFmpeg 8 bindings. Linux Docker images therefore need
FFmpeg 8 development headers at build time and FFmpeg 8 runtime libraries at
run time. Ubuntu 26.04 provides matching packages such as `libavcodec62`,
`libavformat62`, `libavutil60`, `libswresample6`, and `libswscale9`.

## Benchmark Tooling

`asset_region_bench` now supports:

- repeated `--start-app-rule`
- repeated `--on-demand-rule`
- `--prefetch-only`
- `--bundle-cache-dir`
- `--native-cli-parity`
- native worker pool controls
- media/backend controls
- JSONL output with detailed phase and bundle telemetry

Use `--prefetch-only` first when comparing backends, then run each backend
against the same cache directory.

## Cross-Platform Fixes

`media_ffi` now uses `std::ffi::c_char` for `av_strerror` buffers. This matters
because `c_char` is signed on some targets and unsigned on Linux arm64. The fix
keeps the FFmpeg error path portable across macOS, Linux x64, and Linux arm64.

## Current Caveats

- NativeAOT still uses JSON for control messages; binary payloads are already
  framed separately, but the control ABI is not a full binary struct ABI.
- Direct NativeAOT concurrency is not the production default.
- CLI paths are retained for explicit comparison and rollback testing.
- AssetStudio normal `dotnet build` may still trip platform-specific legacy FBX
  copy targets. The NativeAOT publish path is the validation path for this
  integration.
