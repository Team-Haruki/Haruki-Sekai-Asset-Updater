# Double FFI Operations

This document summarizes how to run, tune, and debug the current double-FFI
pipeline.

## Production Defaults

Recommended production settings:

```yaml
tools:
  asset_studio_native_call_mode: "pool"
  asset_studio_native_unitypy_mode: true
  asset_studio_native_read_batch_size: 32
  media_backend: "ffi"
```

The Rust service must be built with:

```bash
cargo build --release --features media-ffi
```

Required paths:

```bash
export HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH=/app/assetstudio/HarukiAssetStudioNative.so
export HARUKI_ASSET_STUDIO_NATIVE_WORKER_PATH=/app/assetstudio_native_worker
```

## Backend Modes

Native call mode:

- `pool`: production default
- `process`: one worker process per operation, useful for isolation tests
- `direct`: load NativeAOT in the Rust process, useful for smoke tests and
  client API usage

Media backend:

- `ffi`: production default
- `cli`: legacy benchmark path
- `auto`: compatibility value; avoid it for rigorous benchmarks

## Native Read Tuning

Useful environment variables:

```bash
export HARUKI_ASSET_STUDIO_NATIVE_PROCESS_CONCURRENCY=0
export HARUKI_ASSET_STUDIO_NATIVE_WORKER_MAX_CALLS=256
export HARUKI_ASSET_STUDIO_NATIVE_READ_BATCH_SIZE=32
export HARUKI_ASSET_STUDIO_NATIVE_IMAGE_FORMAT=raw_rgba
export HARUKI_CPU_BUDGET_AUTO=true
export HARUKI_CPU_BUDGET_RATIO=0.75
export HARUKI_CPU_RESERVED=1
```

General guidance:

- Keep `asset_studio_native_read_batch_size=32` as the default.
- Try `64` for image-heavy rules such as `character/member`.
- Use `asset_studio_native_process_concurrency=0` for the shared-host default.
  Without CPU throttle it auto-scales to the CPU budget. With CPU throttle
  enabled it can oversubscribe workers up to the CPU count while the throttle
  controls actual process CPU. `cpu_budget_ratio` and `cpu_reserved` remain the
  single CPU budget control.
- Enable `concurrency.cpu_throttle_enabled` when the process tree itself should
  stay near the same CPU budget. This throttle samples the updater and its
  native workers, then delays new CPU-heavy permits while usage is above
  `effective_cpu_budget * 100%`.
  Explicit values are still useful for benchmark-only runs; high-core servers
  have tested well around `56` to `64` for broad CN workloads.
- Do not use unrestricted direct NativeAOT concurrency as the production path.

## Media Scheduler Tuning

Useful environment variables:

```bash
export HARUKI_MEDIA_ENCODE_CONCURRENCY=12
```

The media scheduler records:

- `post_process.hca.convert_mp3`
- `post_process.hca.media_pool_wait`
- `post_process.acb.hca_tracks_wall`
- `media_scheduler.media_encode_wait`
- `media_scheduler.media_encode_active_peak`

If media encode wait grows while CPU is not saturated, increase media encode
concurrency. If CPU is already saturated, raising it further usually only
increases contention.

## Benchmark Workflow

Use a clean cache directory for prefetch, then reuse it for each backend:

```bash
asset_region_bench \
  --prefetch-only \
  --bundle-cache-dir /tmp/haruki-cache \
  --jsonl-output /tmp/haruki-prefetch.jsonl \
  ...

asset_region_bench \
  --backend native \
  --media-backend ffi \
  --bundle-cache-dir /tmp/haruki-cache \
  --jsonl-output /tmp/haruki-dual-ffi.jsonl \
  ...

asset_region_bench \
  --backend cli \
  --media-backend cli \
  --bundle-cache-dir /tmp/haruki-cache \
  --jsonl-output /tmp/haruki-dual-cli.jsonl \
  ...
```

For strict output-shape comparisons:

```bash
asset_region_bench --native-cli-parity ...
```

For production-path benchmarks, leave CLI parity off unless the question is
specifically about matching CLI filenames and extensions.

## Docker Notes

`rsmpeg` is compiled with FFmpeg 8 bindings. The build image and runtime image
must match that choice.

Known-good Linux package family:

- Ubuntu 26.04
- `libavcodec-dev` / `libavcodec62`
- `libavformat-dev` / `libavformat62`
- `libavutil-dev` / `libavutil60`
- `libswresample-dev` / `libswresample6`
- `libswscale-dev` / `libswscale9`
- `libavdevice-dev` / `libavdevice62`

Debian bookworm FFmpeg 5 headers are incompatible with the current
`rsmpeg`/`rusty_ffmpeg` FFmpeg 8 feature set.

Linux arm64 also requires portable C FFI pointer handling. Use `c_char` for C
string buffers instead of assuming `i8`.

## Debugging

Rust:

```bash
export RUST_BACKTRACE=1
export RUST_LOG=info
```

NativeAOT worker and C# adapter:

```bash
export HARUKI_ASSET_STUDIO_NATIVE_TRACE=1
export HARUKI_ASSET_STUDIO_NATIVE_WORKER_TRACE=1
export HARUKI_ASSET_STUDIO_NATIVE_LOG_DIR=/tmp/haruki-native-logs
```

Useful signs in benchmark JSONL:

- `bundle_fetch_sources`: confirms cache hit or miss
- `bundle_worker_wait_ms`: worker pool pressure
- `bundle_native_call_ms`: NativeAOT call cost
- `native_object_read_plan`: object count, batch count, payload bytes, failures
- `native_batch_diagnostics`: payload kinds, asset type counts, payload bytes
- `post_process_phase_ms`: media and image post-process cost

## Cleanup

Temporary benchmark directories are safe to remove when results have been
recorded:

```bash
rm -rf /tmp/haruki-local-docker-bench
rm -rf /tmp/haruki-native-logs
rm -rf /tmp/haruki-region-bench-*
rm -rf /tmp/assetstudio-native-verify
```
