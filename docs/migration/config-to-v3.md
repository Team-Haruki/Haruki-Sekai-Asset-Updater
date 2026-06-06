# Config Migration: legacy configs to v3

The original Rust rewrite used config v2 for the Go-to-Rust migration. The
current service schema is config v3, which keeps YAML but reorganizes the file
around responsibilities instead of mirroring legacy package structs.

## Top-level mapping

| legacy key | v3 key | Notes |
| --- | --- | --- |
| `proxy` | `server.proxy` | Moves under server/network concerns. |
| `concurrents` | `concurrency` + `resources.cpu` | Worker counts stay under `concurrency`; CPU budget/throttle lives under `resources.cpu`. |
| `sekai_music_chart_hash_collection` | `git_sync.chart_hashes` | Grouped with Git-related behavior. |
| `backend` | `server` + `logging` | Network binding and logging split apart. |
| `tool` | `backends` | External backend settings now live under media and AssetStudio FFI backend groups. |
| `profiles` | `regions.<name>.provider.profile_hashes` | Provider-specific hashes live with the region that uses them. |
| `servers` | `regions` | Region definitions become the main config body. |
| `remote_storages` | `storage.providers` | Storage grouped as a dedicated section. |

## Backend and logging

| legacy key | v3 key | Default / change |
| --- | --- | --- |
| `backend.host` | `server.host` | Default stays `0.0.0.0`. |
| `backend.port` | `server.port` | Default changes to `8080` for the Rust service. |
| `backend.ssl` | `server.tls.enabled` | TLS fields grouped together. |
| `backend.ssl_cert` | `server.tls.cert_file` | Pure move. |
| `backend.ssl_key` | `server.tls.key_file` | Pure move. |
| `backend.enable_authorization` | `server.auth.enabled` | Pure move. |
| `backend.accept_user_agent_prefix` | `server.auth.user_agent_prefix` | Pure move. |
| `backend.accept_authorization_token` | `server.auth.bearer_token` | Pure rename. |
| `backend.log_level` | `logging.level` | Pure move. |
| `backend.main_log_file` | `logging.file` | Pure move. |
| `backend.access_log` | `logging.access.format` | Pure move. |
| `backend.access_log_path` | `logging.access.file` | Pure move. |

## Backends, storage, and Git sync

| legacy key | v3 key | Notes |
| --- | --- | --- |
| `tool.ffmpeg_path` | `backends.media.ffmpeg_path` | Pure move. |
| `tool.media_backend` | `backends.media.backend` | `ffi` is the default; `cli`/`auto` remain available for platform fallback and tests. |
| `tool.asset_studio_cli_path` | removed | Rust production export uses AssetStudio FFI, not the old CLI path. |
| `tools.asset_studio_ffi_library_path` | `backends.asset_studio.library_path` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_call_mode` | `backends.asset_studio.call_mode` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_worker_path` | `backends.asset_studio.worker_path` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_process_concurrency` | `backends.asset_studio.process_concurrency` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_worker_max_calls` | `backends.asset_studio.worker_max_calls` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_read_batch_size` | `backends.asset_studio.read_batch_size` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_image_format` | `backends.asset_studio.image_format` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_read_kinds` | `backends.asset_studio.read_kinds` | Supported by `config_migrate`; not accepted by the main service loader. |
| `tools.asset_studio_ffi_cli_parity_mode` | `backends.asset_studio.cli_parity_mode` | Supported by `config_migrate`; not accepted by the main service loader. |
| `remote_storages[].type` | `storage.providers[].kind` | Rename to match Rust enum naming. |
| `remote_storages[].endpoint` | `storage.providers[].endpoint` | Same behavior. |
| `remote_storages[].ssl` | `storage.providers[].tls` | Renamed for clarity. |
| `remote_storages[].path_style` | `storage.providers[].path_style` | Same behavior. |
| `remote_storages[].bucket` | `storage.providers[].bucket` | Same behavior. |
| `remote_storages[].region` | `storage.providers[].region` | Same behavior. |
| `remote_storages[].acl_public` | `storage.providers[].public_read` | Renamed to explicit access intent. |
| `remote_storages[].access_key` | `storage.providers[].access_key` | Same behavior. |
| `remote_storages[].secret_key` | `storage.providers[].secret_key` | Same behavior. |
| `sekai_music_chart_hash_collection.enabled` | `git_sync.chart_hashes.enabled` | Same behavior. |
| `sekai_music_chart_hash_collection.repository_dir` | `git_sync.chart_hashes.repository_dir` | Same behavior. |
| `sekai_music_chart_hash_collection.username` | `git_sync.chart_hashes.username` | Same behavior. |
| `sekai_music_chart_hash_collection.email` | `git_sync.chart_hashes.email` | Same behavior. |
| `sekai_music_chart_hash_collection.password` | `git_sync.chart_hashes.password` | Same behavior. |

## Concurrency

| legacy key | v3 key |
| --- | --- |
| `concurrents.concurrent_download` | `concurrency.download` |
| `concurrents.concurrent_upload` | `concurrency.upload` |
| `concurrents.concurrent_acb` | `concurrency.acb` |
| `concurrents.concurrent_usm` | `concurrency.usm` |
| `concurrents.concurrent_hca` | `concurrency.hca` |
| `concurrency.cpu_budget_auto` | `resources.cpu.budget_auto` |
| `concurrency.cpu_budget_ratio` | `resources.cpu.budget_ratio` |
| `concurrency.cpu_reserved` | `resources.cpu.reserved` |
| `concurrency.cpu_throttle_enabled` | `resources.cpu.throttle.enabled` |
| `concurrency.cpu_throttle_sample_ms` | `resources.cpu.throttle.sample_ms` |
| `execution.max_in_flight_bundle_bytes` | `resources.memory.max_in_flight_bundle_bytes` |

## Region layout

Every former `servers.<region>` entry becomes `regions.<region>` with nested groups:

- `provider`
- `crypto`
- `paths`
- `filters`
- `export`
- `upload`

### Provider and crypto

| legacy key | v3 key | Notes |
| --- | --- | --- |
| `enabled` | `enabled` | Same behavior. |
| `asset_info_url_template` | `provider.asset_info_url_template` | Same behavior. |
| `asset_url_template` | `provider.asset_bundle_url_template` | Renamed for clarity. |
| `cp_asset_profile` | `provider.profile` | Only used by `colorful_palette` provider. |
| `nuverse_asset_version_url` | `provider.asset_version_url` | Only used by `nuverse` provider. |
| `nuverse_override_app_version` | `provider.app_version` | Renamed to match runtime intent. |
| `required_cookies` | `provider.required_cookies` | Same behavior. |
| `aes_key_hex` | `crypto.aes_key_hex` | Same behavior. |
| `aes_iv_hex` | `crypto.aes_iv_hex` | Same behavior. |
| `unity_version` | `runtime.unity_version` | Moves under runtime metadata. |

### Paths and filters

| legacy key | v3 key |
| --- | --- |
| `asset_save_dir` | `paths.asset_save_dir` |
| `downloaded_asset_record_file` | `paths.downloaded_asset_record_file` |
| `start_app_regexes` | `filters.start_app` |
| `ondemand_regexes` | `filters.on_demand` |
| `skip_regexes` | `filters.skip` |
| `download_priority_list` | `filters.priority` |

### Export and upload

| legacy key | v3 key |
| --- | --- |
| `export_by_category` | `export.by_category` |
| `export_usm_files` | `export.usm.export` |
| `decode_usm_files` | `export.usm.decode` |
| `export_acb_files` | `export.acb.export` |
| `decode_acb_files` | `export.acb.decode` |
| `decode_hca_files` | `export.hca.decode` |
| `convert_photo_to_webp` | `export.images.convert_to_webp` |
| `remove_png` | `export.images.remove_png` |
| `convert_video_to_mp4` | `export.video.convert_to_mp4` |
| `direct_usm_to_mp4_with_ffmpeg` | `export.video.direct_usm_to_mp4_with_ffmpeg` |
| `remove_m2v` | `export.video.remove_m2v` |
| `convert_audio_to_mp3` | `export.audio.convert_to_mp3` |
| `convert_wav_to_flac` | `export.audio.convert_to_flac` |
| `remove_wav` | `export.audio.remove_wav` |
| `upload_to_cloud` | `upload.enabled` |
| `remove_local_after_upload` | `upload.remove_local_after_upload` |

## Explicit defaults in v3

- `config_version` is required and must be `3`.
- `server.port` defaults to `8080`.
- `logging.level` defaults to `INFO`.
- `execution.retry.attempts` defaults to `4`.
- `execution.retry.initial_backoff_ms` defaults to `1000`.
- `execution.retry.max_backoff_ms` defaults to `4000`.
- `concurrency.download` defaults to `4`.
- `concurrency.upload` defaults to `4`.
- `concurrency.acb` defaults to `8`.
- `concurrency.usm` defaults to `4`.
- `concurrency.hca` defaults to `16`.
- `backends.media.backend` defaults to `ffi`.
- `backends.asset_studio.call_mode` defaults to `pool`.
- `resources.cpu.budget_auto` defaults to `true`.
- `resources.cpu.budget_ratio` defaults to `0.75`.
- `resources.cpu.throttle.enabled` defaults to `false`.

## Migrating older Rust config drafts

The main service loader only accepts the current v3 schema. Run the standalone
migration CLI for older Rust draft configs that still use `tools.*`,
`asset_studio_native_*`, `asset_studio_ffi_*`, or CPU fields embedded under
`concurrency`:

```bash
cargo run --bin config_migrate -- --input old.yaml --output haruki-asset-configs.yaml --check
```

## Removed or intentionally deferred in v3

- Direct reuse of the old `/update_asset` endpoint is removed in favor of `/v2/assets/update`.
- Go-embedded codec implementations are not part of the Rust service plan.
- Any temporary Go-to-Rust codec bridge is intentionally out of scope.
- `execution.retry.*` was added during the Rust migration; there is no direct
  legacy Go field mapping.
