# Go v1 vs Rust v2 Parity

This document tracks implementation parity between the legacy Go service and the Rust v2 service.

Status legend:

- `implemented`: Rust v2 has a working counterpart and test coverage exists
- `partial`: Rust v2 has a counterpart, but behavior or coverage is still incomplete
- `missing`: Go behavior has no Rust v2 counterpart yet
- `intentional-diff`: Rust v2 differs by design

## HTTP and Job Lifecycle

| Area | Go v1 | Rust v2 | Status | Notes |
| --- | --- | --- | --- | --- |
| Service startup | `main.go` + Fiber | `src/main.rs` + Axum | `implemented` | Rust starts, loads config, and serves HTTP. |
| Health endpoint | none | `GET /healthz` | `intentional-diff` | Rust adds health checks; Go had none despite compose expecting one. |
| Submit update endpoint | `POST /update_asset` | `POST /v2/assets/update` | `intentional-diff` | Rust uses v2 job-oriented API instead of fire-and-forget v1 route. |
| Job status query | none | `GET /v2/jobs/{id}` | `intentional-diff` | Rust adds persisted in-memory job snapshots. |
| Job cancel | none | `POST /v2/jobs/{id}/cancel` | `intentional-diff` | Rust adds soft cancellation. |
| Authorization | User-Agent + bearer token | User-Agent + bearer token | `implemented` | Covered by API tests in both codepaths. |
| TLS server mode | supported | config exists, runtime not wired | `partial` | Rust parses TLS config but still binds plain HTTP only. |
| Job lifecycle states | implicit background goroutine | explicit `queued/planning/running/cancelled/completed/failed` | `intentional-diff` | Rust exposes richer lifecycle. |
| Progress reporting | log-only | `progress.phase/current_step/recent_events` | `intentional-diff` | Rust adds structured progress. |
| Failure reporting | log-only | structured `failure.kind/message/retryable` | `intentional-diff` | Rust adds structured failure metadata. |

## Config and Deployment

| Area | Go v1 | Rust v2 | Status | Notes |
| --- | --- | --- | --- | --- |
| YAML config loader | `haruki-asset-configs.yaml` | `haruki-asset-configs.yaml` | `intentional-diff` | Rust keeps the filename but uses the v2 schema internally. |
| Config schema | flat v1 sections | reorganized v2 sections | `intentional-diff` | Mapping documented in `config-v1-to-v2.md`. |
| Execution controls | none | `execution.timeout_seconds`, `execution.allow_cancel`, `execution.retry.*` | `intentional-diff` | Rust adds timeout, cancel, and retry policy controls. |
| Main log file | supported | supported | `implemented` | Rust writes file-backed main logs. |
| Access log file | supported | supported | `implemented` | Rust middleware writes formatted access logs. |
| Docker image | `Dockerfile` | `Dockerfile` | `implemented` | The repo root Docker image now builds the Rust service. |
| Compose file | `docker-compose.yml` | `docker-compose.yml` | `implemented` | The repo root compose file now targets the Rust service. |

## Asset Info, Downloading, and Records

| Area | Go v1 | Rust v2 | Status | Notes |
| --- | --- | --- | --- | --- |
| Colorful Palette asset info URL rendering | yes | yes | `implemented` | Includes profile hash substitution. |
| Nuverse version lookup | yes | yes | `implemented` | Rust persists resolved version into bundle URL rendering. |
| Cookie bootstrap | JP hardcoded | provider-level `cookie_bootstrap_url` with default | `implemented` | Rust is more flexible and has test coverage. |
| AES-CBC + PKCS7 asset info decode | yes | yes | `implemented` | Rust uses `aes` + `cbc` + `rmp-serde`. |
| Ordered msgpack helpers | generic utility | not ported as a generic utility | `partial` | Rust decodes concrete asset info structs but does not expose the Go ordered-msgpack helper surface. |
| Bundle filtering (`start_app`, `on_demand`, `skip`) | yes | yes | `implemented` | Regex-based selection exists in Rust. |
| Bundle priority sorting | yes | yes | `implemented` | Rust sorts planned downloads by regex priority then path. |
| Download retries | GET retries | shared retry policy | `implemented` | Rust retries HTTP fetches through a shared backoff helper controlled by `execution.retry`. |
| Download concurrency | semaphore-based | semaphore-based | `implemented` | Rust executes bundle downloads concurrently. |
| Download record JSON | yes | yes | `implemented` | Same object shape; Rust saves immediately after execution. |
| Batch-save optimization | yes | no | `partial` | Go batches successful updates; Rust writes once at the end. |
| Persistent job storage | none | none | `implemented` | Both are process-memory only for job state. |

## Export and Post-Processing

| Area | Go v1 | Rust v2 | Status | Notes |
| --- | --- | --- | --- | --- |
| AssetStudioCLI invocation | yes | yes | `implemented` | Rust mirrors the command-building logic and has fake CLI coverage. |
| Export grouping (`container` / `containerFull`) | yes | yes | `implemented` | Tested in both codepaths. |
| Strip-path logic | yes | yes | `implemented` | Rust mirrors category and `mysekai` path handling. |
| USM extraction | internal Go codec | `cridecoder` | `implemented` | Real sample parity tests pass. |
| ACB extraction | internal Go codec | `cridecoder` | `implemented` | Real sample parity tests pass. |
| HCA -> WAV | internal Go codec | `cridecoder` | `implemented` | Real sample decode test passes. |
| USM direct-to-MP4 with ffmpeg | yes | yes | `implemented` | Rust supports metadata-based output naming and direct ffmpeg mode. |
| M2V -> MP4 | yes | yes | `implemented` | Rust carries over frame-rate-aware conversion. |
| WAV -> MP3 / FLAC | yes | yes | `implemented` | Rust invokes ffmpeg for both conversions. |
| PNG -> WebP | nativewebp | pure Rust lossless WebP encoder | `partial` | Rust no longer shells out for image conversion, but encoder output is still not guaranteed to be byte-for-byte identical to Go/nativewebp. |
| Merge multiple USM fragments | yes | yes | `implemented` | Rust mirrors the merge-and-delete behavior. |
| ACB/HCA inner concurrency | semaphore + goroutines | worker-pool concurrency via `concurrency.acb` and `concurrency.hca` | `implemented` | Rust now mirrors Go's bounded concurrency model for ACB fan-out and per-ACB HCA processing. |
| CI-grade real AssetStudio coverage | limited local tests | default test suite + optional real AssetStudio workflow | `implemented` | Rust keeps stable default CI and adds an opt-in GitHub Actions job for a real AssetStudio binary and sample bundle when repository vars/secrets are configured. |

## Upload, Git Sync, and Observability

| Area | Go v1 | Rust v2 | Status | Notes |
| --- | --- | --- | --- | --- |
| S3-compatible upload | yes | yes | `implemented` | Rust uses AWS SDK v2-compatible flow. |
| Remove local after upload | yes | yes | `implemented` | Wired through upload config and post-process path. |
| Chart hash file generation | yes | yes | `implemented` | Both gather `music/music_score*` records. |
| Git push | `go-git` | `git2-rs` | `implemented` | Rust intentionally uses `git2-rs`; push tests exist. |
| Transient retry layer | ad hoc by subsystem | shared retry helper for HTTP/export/storage/git push | `implemented` | Rust now applies one configurable backoff policy across the main transient-failure paths. |
| Proxy handling for Git | yes | yes | `implemented` | Rust push path supports proxy options. |
| Structured progress events | no | yes | `intentional-diff` | Rust exposes richer runtime state. |
| Structured access logs | yes | yes | `implemented` | Rust uses middleware instead of Fiber logger. |

## Codec Surface

| Area | Go v1 | Rust v2 | Status | Notes |
| --- | --- | --- | --- | --- |
| Internal codec implementation location | `utils/cricodecs` | external crate `cridecoder` | `intentional-diff` | Rust intentionally depends on crates.io release `cridecoder@0.1.1`. |
| Sample parity (`0703.usm`) | yes | yes | `implemented` | Hash matches frozen baseline. |
| Sample parity (`se_0126_01.acb`) | yes | yes | `implemented` | Output names and hashes match frozen baseline. |
| Generic codec CLI | Go helper cmds | `usmexport` + `usmmeta` bins in `src/bin` | `implemented` | Rust now exposes repo-level USM helper CLIs backed by `cridecoder`. |

## Highest-Priority Remaining Gaps

1. TLS serving is still not wired in the Rust runtime even though the v2 config models it.
2. Rust does not provide a v1 compatibility route for `/update_asset`; migration requires callers to switch to `/v2/assets/update`.
3. Generic ordered-msgpack utility behavior is not carried over as a reusable Rust module; only concrete asset-info decode is covered.

## Rust-Only Additions

- Structured job progress
- Structured failure classification
- Job cancellation
- Execution timeout
- Health endpoint
- Job status polling API
