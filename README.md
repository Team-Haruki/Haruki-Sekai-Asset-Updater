> [!Caution]
> This project was rewritten in Rust.  
> Go edition is not maintained anymore.   
> If you want to use Go edition, please go to [old go branch](https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater/tree/old-go).

# Haruki Sekai Asset Updater
**Haruki Sekai Asset Updater** is a companion project for [HarukiBot](https://github.com/Team-Haruki), it's a high performance game asset extractor and exporter of the game `Project Sekai`.

## Scope

- Loads v3 YAML config
- Exposes `GET /healthz`
- Exposes `POST /v2/assets/update`
- Exposes `GET /v2/jobs/{id}`
- Exposes `POST /v2/jobs/{id}/cancel`
- Uses [`cridecoder`](https://crates.io/crates/cridecoder) as the codec backend
- Supports bundle download, deobfuscation, export post-processing, S3-compatible upload, and Git CLI chart sync
- Uses the Rust image backend for PNG/JPG/WebP output from AssetStudio RGBA payloads
- Uses the double-FFI production path by default: AssetStudio FFI worker
  pool plus FFmpeg/rsmpeg FFI. FFmpeg CLI remains available as a media fallback
  for platforms where FFI is unavailable.
- The native AssetStudioFFI library is built from
  [`Team-Haruki/AssetStudio`](https://github.com/Team-Haruki/AssetStudio)'s
  `sekai-modified` branch, which is the fork's default branch.

## Layout

- `src/`: application code
- `crates/assetstudio-ffi/`: AssetStudio FFI ABI and worker binary
- `tests/`: integration tests
- `docs/migration/v2-api.md`: current HTTP API notes

## Secret Config

- Sensitive config fields support `${env:VAR_NAME}` references instead of checked-in plaintext.
- The main service only accepts the current v3 config shape. Use
  `haruki-asset-configs.example.yaml` as the current config template.
- The loader resolves this syntax for:
  `server.auth.bearer_token`,
  `backends.asset_studio.library_path`,
  `backends.asset_studio.worker_path`,
  `storage.providers[].access_key`,
  `storage.providers[].secret_key`,
  `git_sync.chart_hashes.password`,
  `regions.*.crypto.aes_key_hex`,
  `regions.*.crypto.aes_iv_hex`.
- Tracked config templates expect values such as:
  `HARUKI_MEDIA_BACKEND`,
  `HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_PATH`,
  `HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_IDLE_TIMEOUT_SECONDS`,
  `HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE`,
  `HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT`,
  `HARUKI_ASSET_HTTP_VERSION`,
  `HARUKI_CPU_BUDGET_AUTO`,
  `HARUKI_CPU_BUDGET_RATIO`,
  `HARUKI_CPU_RESERVED`,
  `HARUKI_SHARED_AES_KEY_HEX`,
  `HARUKI_SHARED_AES_IV_HEX`,
  `HARUKI_EN_AES_KEY_HEX`,
  `HARUKI_EN_AES_IV_HEX`.

## Run locally

1. Copy the example config:

```bash
cp haruki-asset-configs.example.yaml haruki-asset-configs.yaml
```

2. Fill the environment values used by your local config:

```bash
cp .env.example .env
export HARUKI_MEDIA_BACKEND=ffi
export HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=/path/to/HarukiAssetStudioFFI.so
export HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=/path/to/assetstudio_ffi_worker
export HARUKI_SHARED_AES_KEY_HEX=...
export HARUKI_SHARED_AES_IV_HEX=...
export HARUKI_EN_AES_KEY_HEX=...
export HARUKI_EN_AES_IV_HEX=...
```

3. Start the service:

```bash
cargo run --features media-ffi
```

Or run it with Docker Compose:

```bash
docker compose up --build
```

4. Check health:

```bash
curl http://127.0.0.1:8080/healthz
```

5. Submit a dry-run job:

```bash
curl -X POST http://127.0.0.1:8080/v2/assets/update \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: HarukiInternal/1.0' \
  -H 'Authorization: Bearer change-me' \
  -d '{"region":"jp","asset_version":"6.0.0","asset_hash":"deadbeef","dry_run":true}'
```

### AssetStudioFFI Runtime

The Rust service talks to AssetStudio through `assetstudio_ffi_worker`, while
the native `HarukiAssetStudioFFI` dynamic library comes from the
[`Team-Haruki/AssetStudio`](https://github.com/Team-Haruki/AssetStudio)
`sekai-modified` branch. Release and Docker builds use that branch by default.

Platform release archives include the matching AssetStudioFFI files under
`assetstudio/`. For local development with a release archive, point the service
at that bundled library and the worker binary:

```bash
export HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=./assetstudio/HarukiAssetStudioFFI.so
export HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=./assetstudio_ffi_worker
```

Use the platform-specific library extension for your host: `.so` on Linux,
`.dylib` on macOS, and `.dll` on Windows; Windows releases use
`./assetstudio/HarukiAssetStudioFFI.dll` and `./assetstudio_ffi_worker.exe`.
You can also download the standalone AssetStudioFFI archive or build the
`AssetStudioFFI` project yourself, then set the same variables to those paths.

## Runtime Tuning

- AssetStudio exports use the `assetstudio_ffi_worker` pool. Set
  `HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH` and, when the worker cannot be inferred
  from the service binary directory, `HARUKI_ASSET_STUDIO_FFI_WORKER_PATH`.
  `HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_IDLE_TIMEOUT_SECONDS`, and
  `HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE` tune worker pool throughput.
- Idle workers exit after `backends.asset_studio.worker_idle_timeout_seconds`
  (60 seconds by default). Set it to `0` only when permanent warm workers are
  intentionally preferred over returning idle memory to the operating system.
- `backends.asset_studio.mode` is always `worker_pool` for production crash
  isolation. The former in-process `direct` mode was removed; configuring it
  fails config loading with a clear error.
- `resources.memory.max_in_flight_bundle_bytes` is a soft memory guard. The default
  `0` disables it. On small Linux hosts, set it to the amount of bundle work the
  process may keep in memory, for example
  `HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES=4294967296`.
- `resources.cpu.budget_auto` and `resources.cpu.budget_ratio` size the
  CPU-heavy worker pools. The default uses the available CPU budget for
  full-throughput export runs; lower it on shared or memory-constrained hosts.
- `resources.cpu.throttle.enabled` is optional and defaults to `false`. Enable
  it only when the process should actively wait based on sampled process-tree
  CPU usage; leave it disabled for full-throughput export runs.
- `backends.image` controls Rust-side image encoding. Keep
  `png_compression: fast` for high-throughput exports unless smaller PNG output
  is more important than CPU time.
- `concurrency.post_process` limits bundle post-processing. Keep it near the
  CPU budget for production full exports, and raise `concurrency.images` for
  image-heavy paths such as `character/member`.
- `concurrency.media_encode` is the legacy aggregate FFmpeg/rsmpeg cap, while
  `concurrency.audio_encode` and `concurrency.video_encode` split audio and
  video encode pressure. Keep video encoding lower on memory-constrained hosts
  because x264 keeps per-encoder frame queues; audio encoding can usually run
  much wider.
- Normal progress logging emits bundle-level start/completion/failure lines.
  Use debug logging for detailed download, native FFI, export, and post-process
  phase traces.

## Benchmark Snapshot

The following local comparison was run on an Apple Mac mini M4 with OrbStack
Linux arm64 containers, using cached CN bundles where noted. It compares the
current Rust FFI pipeline against the old Rust v5.2.2 AssetStudio CLI pipeline
and the retired Go CLI pipeline.

| Rule | Current Rust FFI | Rust v5.2.2 CLI | Old Go CLI |
| --- | ---: | ---: | ---: |
| `^character/member/` images | `71.5s` with local bundle HTTP, `1250/1250` | `272.9s`, `1250/1250` | `298.3s`, `1250/1250` |
| `^music/short` audio MP3 | `57.4s`, `1547/1547` | `113.0s`, `1547/1547` | `120.3s`, `1547/1547` |
| `^movie/gacha` video MP4 | `370.2s`, `448/448` | `401.8s`, `445/448` | `415.0s`, `448/448` |

Notes:

- The image run is the most CPU-bound comparison for the current pipeline; the
  same current image rule through the normal CDN path took `108.6s`.
- The v5.2.2 video result shown uses the best stable direct-FFmpeg rerun
  (`download: 8`, `usm: 4`, `cridecoder@0.2.3`). The original v5 video path only
  completed `154/448` because many USM files failed extraction.
- Output file counts are not fully path-contract comparable across versions:
  current Rust writes final semantic outputs, while older CLI pipelines may keep
  extra exported or intermediate files.

## Verification

- Run the Rust test suite with `cargo test --workspace`.
- Real codec sample baselines are opt-in. Put `0703.usm` and
  `se_0126_01.acb` in an external directory and run with
  `HARUKI_CODEC_SAMPLE_DIR=/path/to/codec-samples`; otherwise those sample
  checks skip while the rest of the suite still runs.
