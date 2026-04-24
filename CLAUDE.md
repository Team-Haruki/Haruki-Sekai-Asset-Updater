# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Haruki Sekai Asset Updater is a Rust HTTP service that extracts and exports game assets from Project Sekai. It downloads asset bundles, deobfuscates them (AES-CBC), runs codec/export pipelines, uploads results to S3-compatible storage, and optionally syncs chart hashes via git. This is **not** a Go project -- the Go edition was removed.

## Build & Development Commands

```bash
# Build
cargo build

# Run the service (requires haruki-asset-configs.yaml and env vars)
cargo run

# Run all tests
cargo test

# Run a single test
cargo test <test_name>

# Run a specific integration test file
cargo test --test codec_smoke
cargo test --test api
cargo test --test cli

# Pre-commit checks (must all pass)
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test

# Helper CLIs
cargo run --bin usmexport -- --input ./tests/files/0703.usm --output-dir ./exports
cargo run --bin usmmeta -- --input ./tests/files/0703.usm

# Docker
docker compose up --build
```

## Architecture

**Entry point:** `src/main.rs` -- starts an Axum HTTP server with graceful shutdown.

**Two-layer module structure (flat, no `mod.rs` files):**

- `src/core.rs` / `src/core/` -- business logic:
  - `config.rs` -- YAML config loading with `${env:VAR_NAME}` secret resolution
  - `pipeline.rs` -- builds an `ExecutionPlan` from config + request
  - `asset_execution.rs` -- runs the plan (download, decrypt, export, upload)
  - `export_pipeline.rs` -- post-processing: AssetStudioModCLI invocation, PNG-to-WebP (pure Rust), media conversion
  - `codec.rs` -- wraps the `cridecoder` crate for USM/ACB decoding
  - `media.rs` -- ffmpeg-based conversions (USM/M2V to MP4, WAV to FLAC/MP3)
  - `storage.rs` -- S3-compatible upload via `aws-sdk-s3`
  - `git_sync.rs` -- chart hash sync via `git2-rs`
  - `regions.rs` -- multi-region (JP/EN/TW/KR/CN) config selection
  - `retry.rs` -- generic async retry helper
  - `download_records.rs` -- tracks previously downloaded assets
  - `models.rs` / `errors.rs` -- shared types and error enums

- `src/service.rs` / `src/service/` -- HTTP and infrastructure:
  - `http` -- Axum router, handlers, `AppState`
  - `jobs.rs` -- async job manager with progress tracking and cancellation
  - `logging.rs` -- tracing-subscriber setup with file and JSON output

- `src/bin/` -- standalone CLIs: `usmexport`, `usmmeta`, `assetinfo_dump`, `s3ls`

**Request flow:** `POST /v2/assets/update` -> handler creates a job -> `JobManager` spawns a tokio task -> `build_execution_plan` -> `AssetExecutionContext` runs download/decrypt/export/upload pipeline -> job status queryable via `GET /v2/jobs/{id}`.

## Key Constraints

- **JSON:** use `sonic-rs`, never `serde_json`
- **YAML:** use `yaml_serde`, never `serde_yaml`
- **Codec:** use published `cridecoder` crate from crates.io
- **Image conversion:** pure Rust WebP encoder (`image` crate), no external WebP toolchain
- **External tool deps:** `AssetStudioModCLI` (.NET) and `ffmpeg` are runtime dependencies
- **Config files:** only `haruki-asset-configs.yaml` (active) and `haruki-asset-configs.example.yaml` (template)
- **Sensitive config** uses `${env:VAR_NAME}` syntax, never hardcoded secrets
- **Test samples** live in `tests/files/` (`0703.usm`, `se_0126_01.acb`)

## HTTP Endpoints

- `GET /healthz`
- `POST /v2/assets/update`
- `GET /v2/jobs/{id}`
- `POST /v2/jobs/{id}/cancel`

## Environment Variables

- `HARUKI_CONFIG_PATH` -- override config file path
- `HARUKI_ASSET_STUDIO_CLI_PATH` -- path to AssetStudioModCLI binary
- `HARUKI_SHARED_AES_KEY_HEX` / `HARUKI_SHARED_AES_IV_HEX` -- shared AES keys (JP/TW/KR/CN)
- `HARUKI_EN_AES_KEY_HEX` / `HARUKI_EN_AES_IV_HEX` -- EN-specific AES keys
- `RUST_LOG` -- tracing log level filter
