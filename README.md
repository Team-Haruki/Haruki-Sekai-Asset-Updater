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
- Exposes `GET /v2/jobs`
- Exposes `GET /v2/jobs/{id}`
- Exposes `POST /v2/jobs/{id}/cancel`
- Uses [`cridecoder`](https://crates.io/crates/cridecoder) as the codec backend
- Supports bundle download, deobfuscation, export post-processing, S3-compatible upload, and Git CLI chart sync
- Uses the Rust image backend for PNG/JPG/WebP output from AssetStudio RGBA payloads
- Uses the published [`unity-rs-core`](https://crates.io/crates/unity-rs-core)
  library directly. FFmpeg/rsmpeg FFI handles media;
  FFmpeg CLI remains available as a fallback where FFI is unavailable.

## Layout

- `crates/sekai-asset-pipeline/`: reusable, transport-neutral single-bundle pipeline
- `crates/sekai-asset-client/`: provider-aware release, manifest, cookie, and bundle HTTP client
- `src/`: HTTP service, batch scheduling, progress, publishing, and Git synchronization
- `tests/`: integration tests
- `docs/migration/v2-api.md`: current HTTP API notes

The pipeline crate owns the serializable provider/release contracts, manifest
types and crypto primitives, Unity export, CRI decoding, media conversion, path
validation, and the deterministic artifact contract. Its `process_bundle` API
accepts one resolved, already-downloaded and deobfuscated bundle and returns an
`ArtifactManifest` containing relative paths, sizes, and SHA-256 digests.

The client crate owns provider URL templates, release resolution, cookie
bootstrap, bounded manifest requests, and atomic streaming bundle downloads.
It keeps cookies, AES keys, proxy configuration, and request headers out of
`BundleRequest`. Haruki and queue workers can therefore share transport logic
without importing the long-running service.

Application concerns deliberately remain in the root package: Axum, job state,
batch concurrency, cancellation, download records, storage publication,
Haruki 3D, and Git synchronization. A queue worker such as AWS Lambda can depend
on the two shared crates without importing those long-running service concerns. See
[`crates/sekai-asset-client/README.md`](crates/sekai-asset-client/README.md) and
[`crates/sekai-asset-pipeline/README.md`](crates/sekai-asset-pipeline/README.md)
for the boundary and API example.

## Secret Config

- Sensitive config fields support `${env:VAR_NAME}` references instead of checked-in plaintext.
- The main service only accepts the current v3 config shape. Use
  `haruki-asset-configs.example.yaml` as the current config template.
- The loader resolves this syntax for:
  `server.auth.bearer_token`,
  `storage.providers[].access_key`,
  `storage.providers[].secret_key`,
  `git_sync.chart_hashes.password`,
  `regions.*.crypto.aes_key_hex`,
  `regions.*.crypto.aes_iv_hex`.
- Tracked config templates expect values such as:
  `HARUKI_MEDIA_BACKEND`,
  `HARUKI_ASSET_STUDIO_READ_BATCH_SIZE`,
  `HARUKI_ASSET_STUDIO_IMAGE_FORMAT`,
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

### unity-rs Engine

The asset engine is the published [`unity-rs-core`](https://crates.io/crates/unity-rs-core)
crate from [`seiunx-dev/unity-rs`](https://github.com/seiunx-dev/unity-rs), a Rust
implementation of AssetStudio. Cargo resolves it from crates.io and `Cargo.lock`
pins the exact release. The `sekai-asset-pipeline` crate calls
`unity_rs_core::studio::Studio` and `StudioObject` directly from blocking Rust
tasks. The engine is compiled into the `haruki-sekai-asset-updater` binary and
remains the only asset-unpacking path.

Set both `regions.<name>.filters.start_app` and `on_demand` to `[".*"]` to
include every bundle in those categories. `asset_studio_types: [all]` makes the
direct engine attempt every serialized object: known types use their specialized
unity-rs reader, while other types fall back from TypeTree JSON to raw bytes.

### Reusable AssetBundle cache

Set `execution.asset_bundle_cache_dir` (for example
`./Data/asset-bundle-cache`) to keep deobfuscated bundles below
`<cache>/<region>/<bundle path>`. Normal update jobs consult this cache before
the CDN, and cache misses are downloaded and persisted for later runs. Existing
deobfuscated caches without hash sidecars remain readable when their size
matches current asset info.

With a cache directory configured, `mode: "prefetch_raw_bundles"` prefetches
every StartApp/OnDemand bundle selected by the region filters into that cache;
`export.raw_bundles` continues to control the optional second raw-bundle copy in
the asset output tree.

## Runtime Tuning

- AssetStudio exports directly call the linked `unity-rs-core` library.
  `HARUKI_ASSET_STUDIO_READ_BATCH_SIZE` controls object-read chunking; the
  existing CPU budget controls concurrent blocking export work.
- `resources.memory.max_in_flight_bundle_bytes` is a soft memory guard. The default
  `0` disables it. On small Linux hosts, set it to the amount of bundle work the
  process may keep in memory, for example
  `HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES=4294967296`. The same process-wide ceiling
  also bounds estimated image decode/encode scratch memory across concurrent
  regions and jobs; an individual oversized image runs alone.
- `resources.cpu.budget_auto` and `resources.cpu.budget_ratio` size the
  CPU-heavy tasks. The default uses the available CPU budget for
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
- `concurrency.auto_tune` (default `false`) sizes the CPU-bound pools for the
  host instead of taking the configured numbers literally. **Enable it on hosts
  wider than the roughly 10 cores the shipped defaults were tuned for.** With it
  off, `audio_encode: 12` means twelve busy cores whether the machine has ten or
  sixty-four: on a 64-core EPYC 7B13 that held the `music/long` rule to 12 cores
  and 290 s, where widening finished the identical work in 74 s. Auto-tuning
  treats each configured value as a floor, raises it to a core-count-derived
  width (`usm` to `cpus / 2`, `video_encode` to `cpus / 4` since each x264
  instance is already several threads, the rest — `post_process` included — to
  the CPU budget), then applies the existing caps. `download` and `upload` are
  left alone because the remote endpoint bounds them. Note that
  `concurrency.post_process` caps every other CPU pool in practice, since audio
  and image work runs inside a bundle's post-process slot: setting it below the
  budget holds the whole pipeline there no matter how wide the other pools are.
- Leave `auto_tune` off when you need to hand-hold a pool. A floor overrides
  that intent, and on a narrow host enabling it can also *narrow* one pool:
  `video_encode` follows `cpus / 4` in both directions, so an 8-core host
  resolves the shipped `4` down to `2`. Compared with earlier releases of
  auto-tuning the widening is inert at or below 12 cores, but "inert" is
  relative to auto-tuning, not to leaving it off.
- Normal progress logging emits bundle-level start/completion/failure lines.
  Use debug logging for detailed download, unity-rs, export, and post-process
  phase traces.
## Benchmark Snapshot

Four generations of this service on two machines. `main` is measured after the
image pipeline moved its encode to where the texture is decoded; the generations
it is compared against are unchanged.

| Version | Unity engine |
| --- | --- |
| Rust v7 | `unity-rs` compiled into the binary (current `main`) |
| Rust v6.0.5 | AssetStudio FFI via a resident NativeAOT worker |
| Rust v5.2.2 | AssetStudioModCLI, one subprocess per bundle |
| Go v4.0.1-dev | AssetStudioModCLI, one subprocess per bundle |

In both environments `cridecoder` is pinned to 0.3.5 in every Rust version, so
the audio and video rules compare pipelines rather than decoder revisions, and
the corpus is served to every version over one local HTTP server — v5, v6 and
the Go build have no local-cache option, so this is the only way to put all four
on the same footing. Export settings are matched: PNG only, MP3 audio, and video
via USM to m2v to ffmpeg. Wall clock comes from each service's own clock, CPU
from the container cgroup, and peak RSS from sampling the process tree.

### Environment A — 48 cores, JP corpus

`Haruki-JP01-YHM01`, EPYC 7B12 pinned to 48 cores, Debian trixie container,
ffmpeg 7.1, `unity-rs-core` 0.5.1. JP corpus, `asset_version 6.8.0.10`. **Span**
is the instrumented bundle phase — first bundle starting to last one finishing,
excluding asset-info planning and record persistence.

#### `image` — 12 image-bearing trees, 16 844 bundles → 36 GB out

| Version | Wall | CPU | Cores | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| **Rust v7** | **36.6 s** | **1 258 s** | 34.5 | **7.4 GB** |
| Rust v6 | 58.6 s | 2 152 s | 36.7 | 16.9 GB |
| Rust v5 | 421.2 s | 19 876 s | 47.2 | 13.2 GB |
| Go v4 | 422.6 s | 19 619 s | 46.4 | 11.6 GB |

#### `music/short` — 1 779 bundles · `movie/gacha` — 547 bundles

| Version | `music/short` | | | `movie/gacha` | | |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| | Wall | CPU | RSS | Wall | CPU | RSS |
| Rust v7 | 16.9 s | 764 s | 0.9 GB | 98.6 s | 4 658 s | 5.9 GB |
| Rust v6 | 17.1 s | 769 s | 1.6 GB | 99.2 s | 4 682 s | 6.9 GB |
| Rust v5 | 32.4 s | 1 452 s | 3.1 GB | 97.5 s | 4 626 s | 12.5 GB |
| Go v4 | 73.1 s | 1 612 s | 1.3 GB | 642.3 s | 7 592 s | 12.7 GB |

### Environment B — 10 cores, CN corpus

`Haruki-JP02-NRT01`, Ryzen 9 3950X pinned to 10 cores, Debian trixie container,
ffmpeg 7.1, `unity-rs-core` 0.5.1. CN corpus, `asset_version 39`.

| Rule | Version | Wall | CPU | Cores | Peak RSS | Output |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `^character/member/` | **Rust v7** | **22.4 s** | **208 s** | 9.3 | **555 MB** | 6 922 |
| 1250 bundles | Rust v6 | 34.9 s | 309 s | 8.9 | 2 337 MB | 4 479 |
| | Rust v5 | 268.7 s | 2 655 s | 9.9 | 1 743 MB | 4 478 |
| | Go v4 | 273.3 s | 2 657 s | 9.7 | 1 562 MB | 4 478 |
| `^music/short` | Rust v7 | 60.7 s | 594 s | 9.8 | **262 MB** | 6 226 |
| 1555 bundles | Rust v6 | 61.3 s | 598 s | 9.8 | 478 MB | 3 116 |
| | Rust v5 | 119.0 s | 1 065 s | 9.0 | 775 MB | 3 115 |
| | Go v4 | 129.1 s | 1 148 s | 8.9 | 600 MB | 3 115 |
| `movie/gacha` | Rust v7 | 76.3 s | 750 s | 9.8 | 2 128 MB | 400 |
| 100-bundle subset | Rust v6 | 75.9 s | 748 s | 9.9 | 2 363 MB | 202 |
| | **Rust v5** | **75.5 s** | 745 s | 9.9 | 2 686 MB | 200 |
| | Go v4 | 165.2 s | 1 208 s | 7.3 | 10 460 MB | 200 |

The v5 and Go rows here were recorded before this host was reinstalled; neither
version was touched since. Re-running v6 is what makes them usable: it came back
at +0.5%, +0.2% and −0.1% of its pre-reinstall wall clock on the three rules,
and within 1 MB of its peak RSS, so the rebuilt host is the same measuring
instrument. Had it not reproduced, those rows would have had to go too.

### Reading it

- **The AssetStudio subprocess costs less the more of the work FFmpeg does.**
  The two CLI-based generations are 11x behind on images, 2–4x on audio, and
  level on video — where v5 is in fact the fastest row on the narrow host.
  Starting a process per bundle is the whole story when AssetStudio does the
  decoding, and invisible when x264 does.
- **Saturating the cores is not the same as being efficient.** On 48 cores v5's
  image row has the highest utilisation on the board, 47.2 of 48, while taking
  eleven times as long and sixteen times the CPU. Every core is busy starting
  sixteen thousand subprocesses.
- **v7 wins the image rule on both machines** — 1.60x faster than v6 on 42% less
  CPU at 48 cores, 1.56x on 33% less at 10 — while emitting 1.5–2x as many files,
  because unity-rs dumps more MonoBehaviour typetrees.
- **v7's image memory is now a fraction of everyone else's**: 7.4 GB against
  v6's 16.9 on the wide host, 555 MB against 2 337 on the narrow one. Encoding
  where the texture is decoded, rather than queueing the decoded RGBA for a
  later stage, is what changed: an RGBA surface is 2.5–4x its encoded form, and
  the queue it used to sit in was as deep as `download + post_process * 2`
  bundles. Peak RSS fell 67% on the wide host and 71% on the narrow one.
- **On audio and video the engine barely matters** — the Rust generations land
  within 1% on video, because `cridecoder` and FFmpeg do the work and the Unity
  parser only extracts the container.
- **Go's weakness is its own pipeline, not AssetStudio.** It matches v5 on
  images (same CLI, same subprocess-per-bundle cost) but is 6.6x slower on video
  at 11.8 of 48 cores, where v5 drives the identical CLI at 47.5. Whenever the
  real work returns to its own process, it stops scaling.

### What these numbers are not

- **One run per cell**, except v7's image row on each host, which is the mean of
  two runs that agreed within 2%. Wall clock repeats to better than 1%; peak RSS
  is noisier, so treat memory differences under 10% as noise.
- **The two environments are not comparable to each other.** Different corpus,
  different rules, different core count.
- **Outputs match on the assets, not on metadata.** v7 emits 1.5–2x as many
  files as the older generations at identical PNG, MP3 and MP4 counts; the
  difference is JSON. Those configurations have no asset-type selector, so it
  cannot be configured away.
- **v7's `music/short` and `movie/gacha` rows in Environment A** were recorded
  just before the image-path change and have not been re-run at 48 cores. The
  change touches only the image write path, and Environment B measured those two
  rules across it at −0.1% and +0.2%.

Each cell also produced a JSON record -- component versions, start-of-cell
load, CPU accounting method, concurrency width -- alongside a long-form
write-up. Those live in the benchmark workspace, which is kept out of this
repository; the tables above are the published form of them.

## Verification

- Run formatting, lint, and the Rust test suite before submitting changes:

  ```bash
  cargo fmt --all -- --check
  cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
  cargo test --locked --workspace
  ```

- SonarQube CI consumes workspace LCOV and enforces 90% line coverage both
  overall and on pull-request changes. Reproduce the overall gate with:

  ```bash
  cargo llvm-cov --locked --workspace --lcov --output-path lcov.info --fail-under-lines 90
  ```

- Real codec sample baselines are opt-in. Put `0703.usm` and
  `se_0126_01.acb` in an external directory and run with
  `HARUKI_CODEC_SAMPLE_DIR=/path/to/codec-samples`; otherwise those sample
  checks skip while the rest of the suite still runs.
