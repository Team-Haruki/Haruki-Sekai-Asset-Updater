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

## Git commits

All commit subjects must follow:

```text
[Type] Short description starting with capital letter
```

Allowed types:

| Type      | Usage                                                 |
|-----------|-------------------------------------------------------|
| `[Feat]`  | New feature or capability                             |
| `[Fix]`   | Bug fix                                               |
| `[Chore]` | Maintenance, refactoring, dependency or build changes |
| `[Docs]`  | Documentation-only changes                            |

Rules:

- Description starts with a capital letter.
- Use imperative mood: `Add ...`, not `Added ...`.
- No trailing period.
- Keep the subject at or below roughly 70 characters.
- **Agent attribution uses the standard Git `Co-authored-by:` trailer in the commit body, not a free-form `Agent:` line.** This makes GitHub render the co-author avatar on the commit page. The trailer must be on its own line, separated from the subject by a blank line, in the form `Co-authored-by: <Display Name> <email>`. Suggested values per agent:
  - Claude (any 4.x): `Co-authored-by: Claude Opus 4.7 <noreply@anthropic.com>` (substitute the actual model, e.g. `Claude Sonnet 4.6`, `Claude Haiku 4.5`)
  - Codex: `Co-authored-by: Codex <noreply@openai.com>`
  - Copilot: `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`

Examples from this repo's history:

```text
[Feat] Add configurable asset export types
[Fix] Nuverse parse issue
[Chore] Update dependencies
[Feat] Replace git2 with git CLI and add commit signing (#16)
```

## GitHub Actions workflows

Use the standardized workflow layout in `.github/workflows`:

- `ci.yml` runs on `main` pushes, pull requests targeting `main`, and manual dispatch.
- Rust CI order: `cargo fmt --all -- --check`, `cargo check --locked --all-targets`, `cargo clippy --locked --all-targets -- -D warnings`, then `cargo test --locked`.
- `release.yml` is the standard release build entrypoint. It runs on `v*` tags and manual dispatch, builds release artifacts, uploads them with `actions/upload-artifact`, and publishes GitHub Release assets on tag pushes.
- `docker.yml` is the standard Docker entrypoint. It runs on `main` pushes, `v*` tags, PRs that touch Docker/build inputs, and manual dispatch. PRs build only; non-PR runs push GHCR images with lowercase image names and Docker metadata tags.

Workflow maintenance rules:

- Keep workflow filenames and top-level names aligned: `CI`, `Release`, `Docker`, and optional package-specific names.
- Use `actions/checkout@v6`, `actions/setup-go@v6`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `softprops/action-gh-release@v3`, and current Docker actions (`setup-buildx@v4`, `login@v4`, `metadata@v6`, `build-push@v7`).
- Keep `permissions` minimal: `contents: read` for CI/Docker build-only work, `contents: write` for release publishing, and `packages: write` only when pushing container images.
- Use workflow `concurrency` keyed by workflow name and ref, with release jobs using `release-${{ github.ref_name }}` and `cancel-in-progress: false`.
- Do not reintroduce legacy workflow names such as `rust-ci.yml`, `build.yml`, `release-build.yml`, `docker-build.yml`, or `docker-release.yml` unless a package-specific workflow already exists and is intentionally preserved.
