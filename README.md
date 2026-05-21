> [!Caution]
> This project was rewritten in Rust.  
> Go edition is not maintained anymore.   
> If you want to use Go edition, please go to [old go branch](https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater/tree/old-go).

# Haruki Sekai Asset Updater
**Haruki Sekai Asset Updater** is a companion project for [HarukiBot](https://github.com/Team-Haruki), it's a high performance game asset extractor and exporter of the game `Project Sekai`.

## Scope

- Loads v2 YAML config
- Exposes `GET /healthz`
- Exposes `POST /v2/assets/update`
- Exposes `GET /v2/jobs/{id}`
- Exposes `POST /v2/jobs/{id}/cancel`
- Uses the published [`cridecoder`](https://crates.io/crates/cridecoder) crate as the codec backend
- Includes `usmexport`, `usmmeta`, `assetinfo_dump`, and `s3ls` helper CLIs
- Supports bundle download, deobfuscation, export post-processing, OpenDAL-backed upload, and `git2-rs` chart sync
- Uses a pure Rust WebP encoder for PNG to WebP conversion

## Layout

- `src/`: application code
- `src/bin/`: helper CLIs
- `tests/`: integration tests
- `docs/migration/`: preserved migration and parity notes

## Config

- The service can load config from a local file or from OpenDAL-backed storage.
  `HARUKI_CONFIG_URI=opendal://config/haruki-asset-configs.yaml` takes
  precedence over local file discovery. Bootstrap the config storage with
  `HARUKI_CONFIG_OPENDAL_SCHEME`, optional `HARUKI_CONFIG_OPENDAL_ROOT`, and
  `HARUKI_CONFIG_OPENDAL_OPTION_*` environment variables.
- String config fields support `${env:VAR_NAME}` references instead of checked-in plaintext.
- Any config path can be overridden with double-underscore env vars after YAML is loaded.
  Examples:
  `HARUKI__SERVER__PORT=8081`,
  `HARUKI__REGIONS__JP__ENABLED=true`,
  `HARUKI__STORAGE__PROVIDERS__0__OPTIONS__BUCKET=sekai-jp-assets`.
- AssetStudio export types are configurable per region via
  `regions.<region>.export.asset_studio.export_types`.
- Storage providers use OpenDAL-style `scheme`, `root`, and `options` fields. Legacy S3-compatible fields remain accepted for existing v2 configs.
- Tracked config templates expect values such as:
  `HARUKI_ASSET_STUDIO_CLI_PATH`,
  `HARUKI_SHARED_AES_KEY_HEX`,
  `HARUKI_SHARED_AES_IV_HEX`,
  `HARUKI_EN_AES_KEY_HEX`,
  `HARUKI_EN_AES_IV_HEX`,
  `HARUKI_STORAGE_ACCESS_KEY_ID`,
  `HARUKI_STORAGE_SECRET_ACCESS_KEY`.

## Run locally

1. Copy the example config:

```bash
cp haruki-asset-configs.example.yaml haruki-asset-configs.yaml
```

2. Fill the environment values used by your local config:

```bash
cp .env.example .env
export HARUKI_ASSET_STUDIO_CLI_PATH=/path/to/AssetStudioModCLI
export HARUKI_SHARED_AES_KEY_HEX=...
export HARUKI_SHARED_AES_IV_HEX=...
export HARUKI_EN_AES_KEY_HEX=...
export HARUKI_EN_AES_IV_HEX=...
```

3. Start the service:

```bash
cargo run
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

## Verification

- Run the Rust test suite with `cargo test`.
- Sample output hashes for `tests/files/0703.usm` and `tests/files/se_0126_01.acb` are enforced directly in `tests/codec_smoke.rs`.
