# Cloud Native Config Notes

This project now uses OpenDAL as the storage abstraction for exported asset
uploads and optional remote config loading.

## Config Source

Local files still use `HARUKI_CONFIG_PATH` and the default
`haruki-asset-configs.yaml` search path. For cloud-native deployments,
`HARUKI_CONFIG_URI` can load the main YAML through OpenDAL before normal config
parsing begins:

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
for diagnostics; the actual backend comes from the bootstrap environment
variables. A local filesystem smoke test can use:

```bash
HARUKI_CONFIG_URI=opendal://config/haruki-asset-configs.yaml
HARUKI_CONFIG_OPENDAL_SCHEME=fs
HARUKI_CONFIG_OPENDAL_ROOT=/etc/haruki
```

When `HARUKI_CONFIG_URI` is set, it takes precedence over `HARUKI_CONFIG_PATH`.
After the YAML is read, `${env:...}` expansion and `HARUKI__...` overrides are
applied.

## Environment Overrides

Any YAML value can be overridden with `HARUKI__` plus a double-underscore path.
Segments are lowercased and numeric segments index arrays.

```bash
HARUKI__SERVER__PORT=8081
HARUKI__REGIONS__JP__UPLOAD__PROVIDERS__0=assets
HARUKI__STORAGE__PROVIDERS__0__OPTIONS__BUCKET=sekai-jp-assets
```

Secret values should still be referenced from YAML with `${env:VAR_NAME}`.

## OpenDAL Providers

`storage.providers` accepts OpenDAL service names and option keys. Existing
S3-compatible fields still parse, but new configs should prefer `scheme`,
`root`, `public_base_url`, and `options`.

```yaml
storage:
  providers:
    - name: assets
      scheme: s3
      root: "assets/{region}"
      public_base_url: "https://cdn.example.com/assets/{region}"
      public_read: false
      options:
        bucket: "sekai-{region}-assets"
        endpoint: "https://s3.example.com"
        region: "auto"
    - name: local
      scheme: fs
      root: "./Data/upload-smoke"
      options: {}
```

`{region}` and `{server}` are both replaced with the active region key.
For S3-compatible providers, `public_read: true` maps to OpenDAL
`default_acl=public-read` during upload. Leave it `false` for private buckets,
CDN origin access, or providers that do not support S3 ACLs.

Provider-level `public_read` makes every uploaded object public. For file-level
control, keep the provider private and configure upload rules on the region.
Rules match the exported path relative to the upload root; `exclude` wins over
`include`.

```yaml
regions:
  jp:
    upload:
      enabled: true
      public_read:
        include:
          - "\\.(png|webp|mp3|mp4)$"
        exclude:
          - "^private/"
```

OpenDAL exposes S3 ACL as an operator-level `default_acl`, so the uploader
internally keeps a private S3 operator and a public-read S3 operator and chooses
between them per file.

By default, uploads fan out to every configured provider. A region can limit
upload to named providers:

```yaml
regions:
  jp:
    upload:
      enabled: true
      providers:
        - assets
      remove_local_after_upload: false
```

Provider names should be unique. Missing or ambiguous selected providers fail
planning/execution instead of silently uploading to the wrong target.

## Download Records

Local record files remain the production path:

```yaml
paths:
  downloaded_asset_record_file: "./Data/jp-assets/downloaded_assets.json"
```

The core download record serializer now also has OpenDAL read/write helpers, so
record state can be moved to object storage in a later step without changing
the JSON contract.
