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
- Supports bundle download, deobfuscation, export post-processing, S3-compatible upload, and `git2-rs` chart sync
- Uses a pure Rust WebP encoder for PNG to WebP conversion

## Layout

- `src/`: application code
- `src/bin/`: helper CLIs
- `tests/`: integration tests
- `docs/migration/`: preserved migration and parity notes

## Secret Config

- Sensitive config fields support `${env:VAR_NAME}` references instead of checked-in plaintext.
- The loader resolves this syntax for:
  `server.auth.bearer_token`,
  `tools.asset_studio_cli_path`,
  `storage.providers[].access_key`,
  `storage.providers[].secret_key`,
  `git_sync.chart_hashes.password`,
  `regions.*.crypto.aes_key_hex`,
  `regions.*.crypto.aes_iv_hex`.
- Tracked config templates expect values such as:
  `HARUKI_ASSET_STUDIO_CLI_PATH`,
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
