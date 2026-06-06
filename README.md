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
- Includes `usmexport`, `usmmeta`, `assetinfo_dump`, and `s3ls` helper CLIs
- Supports bundle download, deobfuscation, export post-processing, S3-compatible upload, and Git CLI chart sync
- Uses the Rust image backend for PNG/JPG/WebP output from AssetStudio RGBA payloads
- Uses the double-FFI production path by default: AssetStudio FFI worker
  pool plus FFmpeg/rsmpeg FFI. FFmpeg CLI remains available as a media
  fallback/test path.

## Layout

- `src/`: application code
- `src/bin/`: helper CLIs
- `tests/`: integration tests
- `docs/assetstudio-ffi.md`: NativeAOT FFI API and object-flow details
- `docs/double-ffi-change-log.md`: recorded double-FFI implementation changes
- `docs/double-ffi-benchmarks.md`: benchmark snapshots and interpretation
- `docs/double-ffi-operations.md`: runtime tuning, Docker, and debugging notes
- `docs/migration/`: preserved migration and parity notes

## Secret Config

- Sensitive config fields support `${env:VAR_NAME}` references instead of checked-in plaintext.
- The main service only accepts the current v3 config shape. Migrate older
  config files explicitly with:
  `cargo run --bin config_migrate -- --input old.yaml --output haruki-asset-configs.yaml --check`.
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
  `HARUKI_ASSET_STUDIO_FFI_CALL_MODE`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_PATH`,
  `HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS`,
  `HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE`,
  `HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT`,
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
export HARUKI_ASSET_STUDIO_FFI_CALL_MODE=pool
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

## Helper CLIs

```bash
cargo run --bin usmexport -- --input ./tests/files/0703.usm --output-dir ./exports
cargo run --bin usmmeta -- --input ./tests/files/0703.usm
```

## Runtime Tuning

- `resources.memory.max_in_flight_bundle_bytes` is a soft memory guard. The default
  `0` disables it. On small Linux hosts, set it to the amount of bundle work the
  process may keep in memory, for example
  `HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES=4294967296`.
- `resources.cpu.budget_auto` and `resources.cpu.budget_ratio` size the
  CPU-heavy worker pools when auto tuning is enabled.
- `resources.cpu.throttle.enabled` is optional and defaults to `false`. Enable
  it only when the process should actively wait based on sampled process-tree
  CPU usage; leave it disabled for full-throughput export runs.
- `backends.image` controls Rust-side image encoding. Keep
  `png_compression: fast` for high-throughput exports unless smaller PNG output
  is more important than CPU time.
- `concurrency.post_process` limits bundle post-processing. The default `0`
  follows `concurrency.media_encode`; raise it for image-heavy full exports such
  as `character/member`.
- Normal progress logging emits bundle-level start/completion/failure lines.
  Use debug logging for detailed download, native FFI, export, and post-process
  phase traces.

## Verification

- Run the Rust test suite with `cargo test`.
- Sample output hashes for `tests/files/0703.usm` and `tests/files/se_0126_01.acb` are enforced directly in `tests/codec_smoke.rs`.
