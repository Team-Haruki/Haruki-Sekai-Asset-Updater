# Cloud Native Config Notes

This project now treats OpenDAL providers as the shared storage boundary for
runtime state and exported assets.

## Config file source

Local files are still supported through `HARUKI_CONFIG_PATH` and the existing
default search path. For cloud-native deployments, set `HARUKI_CONFIG_URI` to
load the main YAML file through OpenDAL before normal config parsing begins:

```bash
HARUKI_CONFIG_URI=opendal://config/haruki-asset-configs.yaml
HARUKI_CONFIG_OPENDAL_SCHEME=s3
HARUKI_CONFIG_OPENDAL_OPTION_BUCKET=sekai-configs
HARUKI_CONFIG_OPENDAL_OPTION_ENDPOINT=https://s3.example.com
HARUKI_CONFIG_OPENDAL_OPTION_REGION=auto
HARUKI_CONFIG_OPENDAL_OPTION_ACCESS_KEY_ID=...
HARUKI_CONFIG_OPENDAL_OPTION_SECRET_ACCESS_KEY=...
```

`HARUKI_CONFIG_URI` has the shape `opendal://<label>/<path>`. The label is used
for diagnostics; the actual OpenDAL backend comes from the bootstrap env vars.
For local smoke tests, the same flow can use the filesystem service:

```bash
HARUKI_CONFIG_URI=opendal://config/haruki-asset-configs.yaml
HARUKI_CONFIG_OPENDAL_SCHEME=fs
HARUKI_CONFIG_OPENDAL_ROOT=/etc/haruki
```

When `HARUKI_CONFIG_URI` is set, it takes precedence over `HARUKI_CONFIG_PATH`.
After the YAML is read, `${env:...}` expansion and `HARUKI__...` overrides are
applied exactly as they are for local files.

## Environment overrides

Any YAML value can be overridden with `HARUKI__` plus a double-underscore path.
Segments are lowercased and numeric segments index arrays.

```bash
HARUKI__SERVER__PORT=8081
HARUKI__EXECUTION__WORKSPACE__WORK_DIR=/var/run/haruki/work
HARUKI__REGIONS__JP__UPLOAD__PROVIDERS__0=assets
HARUKI__STORAGE__PROVIDERS__0__OPTIONS__BUCKET=sekai-jp-assets
```

Secret values should still be referenced from YAML with `${env:VAR_NAME}`:

```yaml
storage:
  providers:
    - name: assets
      scheme: s3
      root: "assets/{region}"
      options:
        bucket: "sekai-{region}-assets"
        endpoint: "https://s3.example.com"
        access_key_id: "${env:HARUKI_STORAGE_ACCESS_KEY_ID}"
        secret_access_key: "${env:HARUKI_STORAGE_SECRET_ACCESS_KEY}"
```

## OpenDAL providers

`storage.providers` uses OpenDAL service names and option keys. The legacy S3
fields still work, but new configs should prefer `scheme`, `root`,
`public_base_url`, and `options`.

```yaml
storage:
  providers:
    - name: assets
      scheme: s3
      root: "assets/{region}"
      public_base_url: "https://cdn.example.com/assets/{region}"
      options:
        bucket: "sekai-{region}-assets"
        endpoint: "https://s3.example.com"
        region: "auto"
    - name: state
      scheme: fs
      root: "/var/lib/haruki/state"
      options: {}
```

`{region}` and `{server}` are both replaced with the active region key.

## Download Record State

Local record files remain supported:

```yaml
paths:
  downloaded_asset_record_file: "./Data/jp-assets/downloaded_assets.json"
```

Cloud-native deployments can store the same JSON through any configured OpenDAL
provider:

```yaml
paths:
  downloaded_asset_record_storage:
    provider: state
    path: "records/{region}/downloaded_assets.json"
```

When `downloaded_asset_record_storage` is present, it takes precedence over the
local file path. A missing object is treated as an empty record, matching the old
local-file behavior.

## Runtime Workspace

Temporary downloaded bundles can be moved out of the system temp directory:

```yaml
execution:
  workspace:
    work_dir: "/var/run/haruki/work"
    cleanup_on_success: true
```

`cleanup_on_success` defaults to `true`. Failed bundle files are kept for
inspection; successful files are removed when cleanup is enabled.

## Upload Provider Selection

By default, uploads fan out to every configured storage provider. A region can
limit upload to named providers:

```yaml
regions:
  jp:
    upload:
      enabled: true
      remove_local_after_upload: false
      providers:
        - assets
```

Provider names should be unique. If a selected provider name is missing or
ambiguous, planning/execution fails instead of silently uploading to the wrong
target.
