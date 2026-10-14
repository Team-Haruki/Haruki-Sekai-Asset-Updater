# Rust v2 API

## `GET /healthz`

Returns service health and enabled region summary. While
`app_version_sync.enabled` is true it also carries an `app_version_sync` block:
`max_consecutive_probe_failures`, `max_consecutive_registry_failures` and per
region `app_version`, `last_result` and both counters.

Example response:

```json
{
  "status": "ok",
  "service": "haruki-sekai-asset-updater",
  "config_version": 3,
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
  "mode": "update",
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
- Every job plan reports `codec_backend: "crates.io:cridecoder"` (the crate version
  is pinned by `Cargo.lock`, not reported in the plan).
- Dry-run jobs complete with a concrete execution plan.
- Non-dry-run jobs now execute asset info fetch, bundle download, deobfuscation, and Rust post-processing.
- `mode` defaults to `update`. `prefetch_raw_bundles` downloads selected bundles
  without unity-rs export. When `execution.asset_bundle_cache_dir` is set, this
  mode fills the reusable deobfuscated cache for every StartApp/OnDemand bundle
  matched by the region filters; otherwise it retains the legacy
  `export.raw_bundles`-only behavior.
- Normal `update` jobs also read and populate the configured bundle cache, so a
  subsequent full unity-rs validation does not redownload unchanged bundles.

Optional field `app_version` (string): the app version the caller's game client
logged in with. For a Nuverse region it is adopted as the region's app version
when it is newer than the current value and the CDN version URL answers it
(see `PUT /v2/regions/{region}/app-version`); the job then uses it. Other
regions ignore it. It never fails the request; the response then carries an
`app_version` string describing the decision, for example
`"adopted 6.5.0 (was 6.4.0)"` or `"not adopted: ..."`. Job snapshots of Nuverse
regions report the `app_version` they run with.

## `GET /v2/regions`

Lists every configured region with its runtime versions:

```json
{
  "app_version_sync_enabled": true,
  "regions": [
    {
      "region": "cn",
      "enabled": true,
      "provider_kind": "nuverse",
      "app_version": {
        "effective": "6.5.0",
        "configured": "6.4.0",
        "override": {
          "value": "6.5.0",
          "source": "registry",
          "updated_at": "2026-10-15T00:00:00Z",
          "forced": false
        }
      },
      "unity_version": { "effective": "2022.3.21f1", "configured": "2022.3.21f1" },
      "sync": {
        "last_checked_at": "2026-10-15T00:01:00Z",
        "last_result": "up_to_date",
        "registry_app_version": "6.5.0",
        "last_probe_ok": true,
        "consecutive_probe_failures": 0,
        "consecutive_registry_failures": 0,
        "pending_submit": false
      }
    }
  ]
}
```

`app_version` is absent for colorful-palette regions. `source` is `registry`,
`manual` or `update_request`. `sync` is present for enabled Nuverse regions
while `app_version_sync.enabled` is true; `last_result` is one of `up_to_date`,
`adopted`, `registry_older`, `registry_failed`, `probe_failed` or
`persist_failed`.

## `PUT /v2/regions/{region}/app-version`

Request body (at least one of the two values):

```json
{ "app_version": "6.5.0", "unity_version": "2022.3.21f1", "force": false }
```

- `app_version` (Nuverse regions only): adopted when newer than the current
  value and the version URL answers it with a numeric body. Equal values are a
  no-op. `force: true` skips both checks.
- `unity_version` (every region): replaces the Unity version.

Responses: `200` with `{"message", "region"}` (`message` is `"unchanged"`
when nothing changed), `400` for invalid values or an `app_version` on a
colorful-palette region, `404` for an unknown region, `409` for a downgrade,
`502` when the CDN probe fails, `500` when the state file cannot be written
(nothing changes then).

## `DELETE /v2/regions/{region}/app-version`

Reverts the region to its YAML values. `?field=app_version` or
`?field=unity_version` reverts only that value. Responds like `PUT`.

## `GET /v2/jobs`

Returns a summary of all known jobs, grouped by status.

Current response fields:

- `total`
- `queued` / `running` / `completed` / `failed` / `cancelled` (job id lists;
  `running` also holds `planning` and `waiting_for_pipeline` jobs)
- `jobs` (per-job entries)

## `GET /v2/jobs/{id}`

Returns the current snapshot for a submitted job, wrapped as `{"job": {...}}`.

Current fields of `job` include:

- `status`
- `message`
- `plan`
- `execution`
- `failure`
- `progress`

Current status values:

- `queued`
- `planning`
- `waiting_for_pipeline` (defined in the status enum; the current service
  does not assign it)
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

When a job fails or is cancelled, the job snapshot includes:

```json
{
  "job": {
    "failure": {
      "kind": "network",
      "message": "HTTP request to ... returned status 503",
      "retryable": true,
      "at": "2026-01-01T00:00:00Z"
    }
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
