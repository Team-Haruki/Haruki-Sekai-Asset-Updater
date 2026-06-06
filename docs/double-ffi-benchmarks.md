# Double FFI Benchmarks

This document records benchmark runs used to compare the legacy CLI path with
the double-FFI path. The numbers here are snapshots, not hard performance
contracts.

## Common Rule Set

The broad CN benchmark used:

```yaml
region: cn
filters:
  start_app:
    - "^bonds_honor/"
    - "^honor/"
    - "^rank_live/"
    - "^music/"
    - "^mysekai/"
    - "^thumbnail/"
    - "^custom_profile/"
    - "^virtual_live/"
  on_demand:
    - "^virtual_live/"
    - "^mysekai/"
```

The run discovers 74,732 bundles and queues 15,616 bundles for processing.

Always prefetch first:

```bash
asset_region_bench \
  --config /app/haruki-asset-configs.yaml \
  --region cn \
  --prefetch-only \
  --bundle-cache-dir /app/cache \
  --download-concurrency 32 \
  --start-app-rule '^bonds_honor/' \
  --start-app-rule '^honor/' \
  --start-app-rule '^rank_live/' \
  --start-app-rule '^music/' \
  --start-app-rule '^mysekai/' \
  --start-app-rule '^thumbnail/' \
  --start-app-rule '^custom_profile/' \
  --start-app-rule '^virtual_live/' \
  --on-demand-rule '^virtual_live/' \
  --on-demand-rule '^mysekai/'
```

## Remote EPYC 7763

Host: 64-core EPYC 7763 Linux server.

The bundle cache was prewarmed before backend comparisons.

| Backend | Concurrency | Project total | Failed |
| --- | ---: | ---: | ---: |
| AssetStudio CLI + ffmpeg CLI | CLI baseline | 247.937s | 0 |
| NativeAOT FFI + media FFI | ffi_process=32 | 115.564s | 0 |
| NativeAOT FFI + media FFI | ffi_process=56 | 97.181s | 0 |
| NativeAOT FFI + media FFI | ffi_process=64 | 95.393s | 0 |
| NativeAOT FFI + media FFI | ffi_process=80 | 95.310s | 0 |
| NativeAOT FFI + media FFI | ffi_process=96 | 95.340s | 0 |

Observed sweet spot: `ffi_process=56` to `64`. Higher concurrency saturated
CPU but did not improve total time.

The fastest remote broad-rule result was about 2.60x faster than the dual CLI
baseline.

## Local Docker on macOS

Host: macOS with OrbStack Linux arm64 Docker.

Docker allocation during the run:

- 10 CPUs
- 8 GiB memory
- image: `haruki-asset-bench:local-arm64`
- base runtime: Ubuntu 26.04 with FFmpeg 8 runtime libraries

The benchmark used the same broad CN rule set and the same prewarmed cache.

| Backend | Settings | Project total | Outer real time | Failed | Cache |
| --- | --- | ---: | ---: | ---: | --- |
| Prefetch only | download=32 | 115.334s | 116.37s | 0 | miss |
| NativeAOT FFI + media FFI | ffi_process=8, media_encode=8, CLI parity | 328.619s | 337.05s | 0 | hit |
| AssetStudio CLI + ffmpeg CLI | media_encode=8 | 1112.113s | 1116.66s | 0 | hit |

Local Docker speedup: about 3.38x by project total.

Important medians and means:

| Metric | Double FFI | Double CLI |
| --- | ---: | ---: |
| bundle export median | 112ms | 1614ms |
| bundle export mean | 1048.7ms | 2265.3ms |
| native or CLI call median | 13ms | 1346ms |
| native or CLI call mean | 193.6ms | 1565.3ms |
| post-process mean | 796.7ms | 1530.1ms |

Interpretation:

- Both paths can keep CPU busy.
- The CLI path pays a large per-bundle process startup and initialization cost.
- The double-FFI path reduces fixed per-bundle overhead and makes small bundle
  handling much cheaper.
- The local Docker gap is large but smaller than some macOS native observations.
  The likely reason is that macOS-native .NET CLI startup and single-file
  runtime behavior adds extra overhead that is less visible inside Linux Docker.

## Music Short 2x2 Benchmark

Rule:

```yaml
region: cn
filters:
  start_app:
    - "^music/short"
  on_demand: []
```

Bundles: 1,547. All runs had 0 failed bundles.

| AssetStudio backend | Media backend | Total time | Bundle export median | Bundle export mean |
| --- | --- | ---: | ---: | ---: |
| CLI | ffmpeg CLI | 202.873s | 1723ms | 1739.6ms |
| CLI | ffmpeg FFI | 187.338s | 1591ms | 1601.3ms |
| NativeAOT FFI | ffmpeg CLI | 83.296s | 554ms | 597.7ms |
| NativeAOT FFI | ffmpeg FFI | 78.585s | 460ms | 498.9ms |

Conclusion: for this audio-heavy rule, the AssetStudio backend change produced
the dominant speedup. Media FFI still helped, but it was not the main bottleneck.

## Notes

- Benchmarks should be compared only when bundle cache status, selected rules,
  exported types, media backend, native batch size, and concurrency settings are
  aligned.
- `--ffi-cli-parity` is useful when comparing output shape with CLI, but it
  is not the recommended production output mode.
- On high-core servers, increasing `ffi_process` too far can raise CPU usage
  without lowering wall time.
- On constrained Docker allocations, smaller values such as
  `ffi_process=8` and `media_encode=8` can be more representative.
