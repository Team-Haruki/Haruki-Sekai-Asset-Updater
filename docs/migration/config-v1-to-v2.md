# Config Migration: v1 to v2

The Rust v2 service keeps YAML but reorganizes the file around responsibilities instead of mirroring Go package structs.

## Top-level mapping

| v1 key | v2 key | Notes |
| --- | --- | --- |
| `proxy` | `server.proxy` | Moves under server/network concerns. |
| `concurrents` | `concurrency` | Field names become shorter and consistent. |
| `sekai_music_chart_hash_collection` | `git_sync.chart_hashes` | Grouped with Git-related behavior. |
| `backend` | `server` + `logging` | Network binding and logging split apart. |
| `tool` | `tools` | Pure rename to pluralized section. |
| `profiles` | `regions.<name>.provider.profile_hashes` | Provider-specific hashes live with the region that uses them. |
| `servers` | `regions` | Region definitions become the main config body. |
| `remote_storages` | `storage.providers` | Storage grouped as a dedicated section. |

## Backend and logging

| v1 key | v2 key | Default / change |
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

## Tools, storage, and Git sync

| v1 key | v2 key | Notes |
| --- | --- | --- |
| `tool.ffmpeg_path` | `tools.ffmpeg_path` | Pure move. |
| `tool.asset_studio_cli_path` | `tools.asset_studio_cli_path` | Pure move. |
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

| v1 key | v2 key |
| --- | --- |
| `concurrents.concurrent_download` | `concurrency.download` |
| `concurrents.concurrent_upload` | `concurrency.upload` |
| `concurrents.concurrent_acb` | `concurrency.acb` |
| `concurrents.concurrent_usm` | `concurrency.usm` |
| `concurrents.concurrent_hca` | `concurrency.hca` |

## Region layout

Every former `servers.<region>` entry becomes `regions.<region>` with nested groups:

- `provider`
- `crypto`
- `paths`
- `filters`
- `export`
- `upload`

### Provider and crypto

| v1 key | v2 key | Notes |
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

| v1 key | v2 key |
| --- | --- |
| `asset_save_dir` | `paths.asset_save_dir` |
| `downloaded_asset_record_file` | `paths.downloaded_asset_record_file` |
| `start_app_regexes` | `filters.start_app` |
| `ondemand_regexes` | `filters.on_demand` |
| `skip_regexes` | `filters.skip` |
| `download_priority_list` | `filters.priority` |

### Export and upload

| v1 key | v2 key |
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

## Explicit defaults in v2

- `config_version` is required and must be `2`.
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

## Removed or intentionally deferred in v2

- Direct reuse of the old `/update_asset` endpoint is removed in favor of `/v2/assets/update`.
- Go-embedded codec implementations are not part of the Rust service plan.
- Any temporary Go-to-Rust codec bridge is intentionally out of scope.
- `execution.retry.*` is a Rust v2-only addition; there is no direct v1 field mapping.
