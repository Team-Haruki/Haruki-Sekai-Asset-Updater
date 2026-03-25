# Rust v2 API

## `GET /healthz`

Returns service health and enabled region summary.

Example response:

```json
{
  "status": "ok",
  "service": "haruki-sekai-asset-updater",
  "config_version": 2,
  "enabled_regions": ["jp"]
}
```

## `POST /v2/assets/update`

Submits a new asset update job.

Request body:

```json
{
  "region": "jp",
  "asset_version": "6.0.0",
  "asset_hash": "deadbeef",
  "dry_run": true
}
```

Response body:

```json
{
  "message": "job accepted",
  "job": {
    "id": "uuid",
    "region": "jp",
    "status": "queued",
    "message": "job accepted and queued for planning"
  }
}
```

Notes:

- The current service accepts, plans, and executes jobs.
- Every job plan now reports `codec_backend: "crates.io:cridecoder@0.1.1"`.
- Dry-run jobs complete with a concrete execution plan.
- Non-dry-run jobs now execute asset info fetch, bundle download, deobfuscation, and Rust post-processing.

## `GET /v2/jobs/{id}`

Returns the current snapshot for a submitted job.

Current response fields include:

- `status`
- `message`
- `plan`
- `execution`
- `failure`
- `progress`

Current status values:

- `queued`
- `planning`
- `running`
- `cancelled`
- `failed`
- `completed`

## `POST /v2/jobs/{id}/cancel`

Requests cancellation for a queued, planning, or running job.

Response body:

```json
{
  "message": "job cancellation requested",
  "job": {
    "id": "uuid",
    "status": "cancelled",
    "failure": {
      "kind": "cancelled",
      "retryable": false
    }
  }
}
```

Notes:

- Cancellation is enabled only when `execution.allow_cancel` is true.
- The service performs soft cancellation: it stops between major execution steps and before new bundle downloads start.

## Failure Semantics

When a job fails or is cancelled, the response snapshot includes:

```json
{
  "failure": {
    "kind": "network",
    "message": "HTTP request to ... returned status 503",
    "retryable": true,
    "at": "2026-01-01T00:00:00Z"
  }
}
```

Current failure kinds:

- `validation`
- `configuration`
- `network`
- `decode`
- `export`
- `storage`
- `git_sync`
- `timeout`
- `cancelled`
- `internal`
