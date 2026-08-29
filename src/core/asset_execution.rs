//! Running an asset update: planning, downloading, exporting, recording.
//!
//! This file is the facade; the submodules below hold the implementation.
//! Every external path -- `crate::core::asset_execution::Thing` -- is
//! unchanged.

mod cache;
mod crypto;
mod download;
mod haruki_3d;
mod model;
mod planning;
mod progress;
mod provider;
mod runner;

pub use crypto::{decrypt_asset_bundle_info, deobfuscate};
pub use model::{
    AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext, Haruki3dExportSummary,
};
pub use planning::should_download_bundle;
pub use progress::ExecutionProgressUpdate;
pub use provider::fetch_live_asset_bundle_info;

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::header::{COOKIE, SET_COOKIE};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Router;
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, RawBundleExportConfig, RegionConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    };
    use crate::core::download_records::DownloadRecord;
    use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};

    use super::cache::{bundle_cache_metadata_path, bundle_hash_index_key};
    use super::crypto::{decrypt_asset_bundle_info, deobfuscate, deobfuscate_owned};
    use super::haruki_3d::{
        bundle_dependency_closure, exporter_metric_lines, missing_haruki_3d_bundle_paths,
    };
    use super::model::{
        AssetBundleDetail, AssetBundleInfo, AssetCategory, AssetExecutionContext,
        BundleCacheEntryStatus, DownloadTask,
    };
    use super::planning::{raw_bundle_output_path, should_download_bundle};
    use super::runner::post_process_backlog_capacity;

    type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
    const TEST_AES_KEY_HEX: &str = "00112233445566778899aabbccddeeff";
    const TEST_AES_IV_HEX: &str = "0102030405060708090a0b0c0d0e0f10";

    fn test_region(provider: RegionProviderConfig) -> RegionConfig {
        RegionConfig {
            enabled: true,
            provider,
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some("./Data/jp-assets".to_string()),
                downloaded_asset_record_file: Some(
                    "./Data/jp-assets/downloaded_assets.json".to_string(),
                ),
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: vec!["^ond/".to_string(), "^live_pv/model/".to_string()],
                skip: vec!["^skip/".to_string()],
                priority: vec!["^start/a".to_string(), "^ond/".to_string()],
            },
            ..RegionConfig::default()
        }
    }

    #[tokio::test]
    async fn blocking_record_save_returns_the_original_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("downloaded_assets.json");
        let mut record = DownloadRecord::from([("bundle".to_string(), "hash".to_string())]);

        AssetExecutionContext::save_download_record_on_blocking_thread(
            path.to_string_lossy().into_owned(),
            &mut record,
        )
        .await
        .unwrap();

        assert_eq!(record.get("bundle").map(String::as_str), Some("hash"));
        assert_eq!(
            crate::core::download_records::load_download_record(&path).unwrap(),
            record
        );
    }

    fn encrypt_asset_info(info: &AssetBundleInfo) -> Vec<u8> {
        let key = hex::decode(TEST_AES_KEY_HEX).unwrap();
        let iv = hex::decode(TEST_AES_IV_HEX).unwrap();
        let payload = rmp_serde::to_vec_named(info).unwrap();
        let mut padded = payload.clone();
        let original_len = padded.len();
        let padding = 16 - (original_len % 16);
        padded.resize(original_len + padding, 0);
        let encrypted = Aes128CbcEnc::new_from_slices(&key, &iv)
            .unwrap()
            .encrypt_padded::<Pkcs7>(&mut padded, original_len)
            .unwrap()
            .to_vec();
        encrypted
    }

    #[test]
    fn haruki_3d_work_root_is_disabled_by_default() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();

        assert!(executor.haruki_3d_work_asset_root().is_none());
    }

    #[test]
    fn raw_bundle_filters_are_independent_of_haruki_3d() {
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.raw_bundles = Some(RawBundleExportConfig {
            output_dir: None,
            include: vec!["^live_pv/model/characterv2/body/".to_string()],
            exclude: Vec::new(),
        });
        region.filters.on_demand.clear();
        region.filters.skip = vec![".*".to_string()];
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let config = AppConfig::default();

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        assert!(
            executor.matches_raw_bundle_filters("live_pv/model/characterv2/body/01"),
            "raw bundle retention must remain independent while 3D is disabled"
        );
        assert!(!executor.matches_raw_bundle_filters("live_pv/model/characterv2/face/01"));

        let detail = |bundle_name: &str| AssetBundleDetail {
            bundle_name: bundle_name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: format!("{bundle_name}-hash"),
            category: AssetCategory::OnDemand,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "live_pv/model/characterv2/body/01".to_string(),
                    detail("live_pv/model/characterv2/body/01"),
                ),
                (
                    "live_pv/model/characterv2/face/01".to_string(),
                    detail("live_pv/model/characterv2/face/01"),
                ),
            ]),
        };
        let tasks = executor.build_raw_bundle_filter_tasks(&info);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].bundle_path, "live_pv/model/characterv2/body/01");
    }

    #[test]
    fn haruki_3d_export_tasks_include_unrecorded_candidates() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "https://example.com/info".to_string(),
            asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::new(),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.export.haruki_3d = crate::core::config::Haruki3dExportConfig {
            enabled: true,
            exporter_path: "/bin/true".to_string(),
            master_dir: "/data/master".to_string(),
            work_dir: temp.path().join("3d-work").to_string_lossy().into_owned(),
            manifest_file: temp
                .path()
                .join("manifest.json")
                .to_string_lossy()
                .into_owned(),
            output_dir: temp.path().join("out").to_string_lossy().into_owned(),
            include: vec!["^live_pv/model/characterv2/".to_string()],
            exclude: Vec::new(),
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("6.0.9".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let matched = "live_pv/model/characterv2/body/01_0001.bundle".to_string();
        let missing_from_record = "live_pv/model/characterv2/body/02_0001.bundle".to_string();
        let dependency = "common/materials/character.bundle".to_string();
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    matched.clone(),
                    AssetBundleDetail {
                        bundle_name: matched.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "new-hash".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: vec![dependency.clone()],
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
                (
                    missing_from_record.clone(),
                    AssetBundleDetail {
                        bundle_name: missing_from_record.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "missing-from-record".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: Vec::new(),
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
                (
                    dependency.clone(),
                    AssetBundleDetail {
                        bundle_name: dependency.clone(),
                        cache_file_name: String::new(),
                        cache_directory_name: String::new(),
                        hash: "dependency-hash".to_string(),
                        category: AssetCategory::OnDemand,
                        crc: 0,
                        file_size: 1,
                        dependencies: Vec::new(),
                        paths: Vec::new(),
                        is_builtin: false,
                        is_relocate: None,
                        md5_hash: None,
                        download_path: None,
                    },
                ),
            ]),
        };
        let tasks = executor.build_haruki_3d_tasks(&info);

        assert_eq!(tasks.len(), 3);
        assert!(tasks.iter().any(|task| task.bundle_path == matched));
        assert!(tasks
            .iter()
            .any(|task| task.bundle_path == missing_from_record));
        assert!(tasks.iter().any(|task| task.bundle_path == dependency));
        assert_eq!(
            executor.haruki_3d_work_asset_root().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("6.0.9")
                .join("AssetBundles")
        );
        assert_eq!(
            executor.haruki_3d_download_record_path().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("downloaded_assets.json")
        );
        assert_eq!(
            executor.haruki_3d_bundle_hash_index_path().unwrap(),
            temp.path()
                .join("3d-work")
                .join("jp")
                .join("bundle_sha256.json")
        );
    }

    #[test]
    fn haruki_3d_background_export_publishes_registry_after_runtime_packages() {
        let config = crate::core::config::Haruki3dExportConfig {
            master_dir: "/master".to_string(),
            output_dir: "/runtime".to_string(),
            manifest_file: "/runtime/manifest.json".to_string(),
            shared_content_store: "/runtime-cas".to_string(),
            compiled_content_store: "/runtime-compiled".to_string(),
            process_concurrency: 16,
            role_character3d_ids: vec![5, 7],
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let commands = AssetExecutionContext::build_haruki_3d_exporter_commands(
            &config,
            Path::new("/work/AssetBundles"),
            Path::new("/work/bundle_sha256.json"),
            Path::new("/work/bundle_dependencies.json"),
        );
        assert_eq!(
            AssetExecutionContext::build_haruki_3d_runtime_catalog_command(&config),
            vec![
                "--emit-runtime-role-catalog",
                "--master",
                "/master",
                "--out",
                "/runtime",
            ]
        );

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0][0], "--emit-part-packages");
        assert_eq!(commands[1][0], "--emit-role-runtimes");
        assert_eq!(commands[2][0], "--emit-costume-registries");
        for command in &commands {
            assert!(
                !command.iter().any(|arg| arg == "--runtime-json-output"),
                "Haruki 3D exporter command should use the exporter's fixed msgpack-br runtime format: {command:?}"
            );
            assert!(
                command
                    .windows(2)
                    .any(|pair| pair == ["--convert-model-textures", "false"]),
                "Haruki 3D exporter command should disable redundant model texture conversion: {command:?}"
            );
        }
        assert!(
            commands[0]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "16"]),
            "part package command should pass haruki_3d.process_concurrency"
        );
        assert!(commands[0]
            .windows(2)
            .any(|pair| pair == ["--shared-content-store", "/runtime-cas"]));
        assert!(commands[0]
            .windows(2)
            .any(|pair| pair == ["--compiled-content-store", "/runtime-compiled"]));
        assert!(commands[0]
            .windows(2)
            .any(|pair| pair == ["--bundle-hash-index", "/work/bundle_sha256.json"]));
        assert!(commands[0].windows(2).any(|pair| pair
            == [
                "--bundle-dependency-index",
                "/work/bundle_dependencies.json"
            ]));
        assert!(
            commands[1]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "16"]),
            "role runtime command should pass haruki_3d.process_concurrency"
        );
        assert_eq!(
            commands[1]
                .iter()
                .filter(|value| value.as_str() == "--role-character3d-id")
                .count(),
            2
        );
        assert!(commands[1].contains(&"5".to_string()));
        assert!(commands[1].contains(&"7".to_string()));
    }

    #[test]
    fn haruki_3d_background_export_runs_role_runtimes_without_role_id_filter() {
        let config = crate::core::config::Haruki3dExportConfig {
            master_dir: "/master".to_string(),
            output_dir: "/runtime".to_string(),
            manifest_file: "/runtime/manifest.json".to_string(),
            process_concurrency: 48,
            role_character3d_ids: Vec::new(),
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let commands = AssetExecutionContext::build_haruki_3d_exporter_commands(
            &config,
            Path::new("/work/AssetBundles"),
            Path::new("/work/bundle_sha256.json"),
            Path::new("/work/bundle_dependencies.json"),
        );

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[1][0], "--emit-role-runtimes");
        assert!(
            commands[1]
                .windows(2)
                .any(|pair| pair == ["--part-package-process-concurrency", "48"]),
            "role runtime command should still pass haruki_3d.process_concurrency"
        );
        assert_eq!(
            commands[1]
                .iter()
                .filter(|value| value.as_str() == "--role-character3d-id")
                .count(),
            0,
            "empty role_character3d_ids should let the exporter choose its default role set"
        );
    }

    #[test]
    fn decrypt_asset_info_round_trips_msgpack_payload() {
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash".to_string(),
                    category: AssetCategory::StartApp,
                    crc: 123,
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: None,
                },
            )]),
        };

        let encrypted = encrypt_asset_info(&info);
        let decrypted =
            decrypt_asset_bundle_info(TEST_AES_KEY_HEX, TEST_AES_IV_HEX, &encrypted).unwrap();
        assert_eq!(decrypted.version.as_deref(), Some("1"));
        assert!(decrypted.bundles.contains_key("start/a"));
    }

    #[test]
    fn build_download_tasks_skips_unchanged_and_queues_changed() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let config = AppConfig::default();
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let ctx = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();

        let detail = |hash: &str| AssetBundleDetail {
            bundle_name: String::new(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: hash.to_string(),
            category: AssetCategory::StartApp,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                ("start/a".to_string(), detail("h1")),
                ("start/aa".to_string(), detail("h2")),
            ]),
        };

        // Recorded hash matches -> skipped; bundle absent from record -> queued.
        let record = DownloadRecord::from([("start/a".to_string(), "h1".to_string())]);
        let tasks = ctx
            .build_download_tasks(&info, &record, &DownloadRecord::new(), false)
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert!(
            !paths.contains(&"start/a"),
            "unchanged bundle must be skipped"
        );
        assert!(paths.contains(&"start/aa"), "new bundle must be queued");

        // Recorded hash differs -> re-queued.
        let stale = DownloadRecord::from([("start/a".to_string(), "OLD".to_string())]);
        let tasks = ctx
            .build_download_tasks(&info, &stale, &DownloadRecord::new(), false)
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert!(
            paths.contains(&"start/a"),
            "changed bundle must be re-queued"
        );
    }

    #[tokio::test]
    async fn build_download_tasks_routes_3d_only_matches_to_staging() {
        let temp = tempdir().unwrap();
        let mut region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: String::new(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        region.filters.on_demand.clear();
        region.export.haruki_3d = crate::core::config::Haruki3dExportConfig {
            enabled: true,
            work_dir: temp.path().join("3d-work").to_string_lossy().into_owned(),
            manifest_file: temp
                .path()
                .join("runtime/haruki-3d-export-manifest.json")
                .to_string_lossy()
                .into_owned(),
            include: vec!["^(start/a|live_pv/model/characterv2/body/)".to_string()],
            ..crate::core::config::Haruki3dExportConfig::default()
        };
        let config = AppConfig::default();
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let detail = |bundle_name: &str, category| AssetBundleDetail {
            bundle_name: bundle_name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: format!("{bundle_name}-hash"),
            category,
            crc: 0,
            file_size: 1,
            dependencies: Vec::new(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([
                (
                    "start/a".to_string(),
                    detail("start/a", AssetCategory::StartApp),
                ),
                (
                    "live_pv/model/characterv2/body/01".to_string(),
                    detail("live_pv/model/characterv2/body/01", AssetCategory::OnDemand),
                ),
            ]),
        };

        let tasks = executor
            .build_download_tasks(&info, &DownloadRecord::new(), &DownloadRecord::new(), false)
            .unwrap();
        let paths: Vec<&str> = tasks.iter().map(|task| task.bundle_path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "3D matches with missing staging must be merged once after ordinary download filtering"
        );
        assert!(
            tasks[0].export_payloads && tasks[0].stage_haruki_3d,
            "ordinary tasks must export payloads"
        );
        assert!(
            !tasks[1].export_payloads && tasks[1].stage_haruki_3d,
            "3D-only tasks must only stage raw bundles"
        );

        let haruki_3d_record = DownloadRecord::from([(
            "live_pv/model/characterv2/body/01".to_string(),
            "live_pv/model/characterv2/body/01-hash".to_string(),
        )]);
        let tasks = executor
            .build_download_tasks(
                &info,
                &DownloadRecord::new(),
                &haruki_3d_record,
                executor.can_reuse_haruki_3d_download_record().await,
            )
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "the independent 3D record must not skip bundles when the runtime manifest is missing"
        );

        let manifest = Path::new(&region.export.haruki_3d.manifest_file);
        std::fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        std::fs::write(manifest, b"{broken").unwrap();
        let tasks = executor
            .build_download_tasks(
                &info,
                &DownloadRecord::new(),
                &haruki_3d_record,
                executor.can_reuse_haruki_3d_download_record().await,
            )
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a", "live_pv/model/characterv2/body/01"],
            "a malformed 3D runtime manifest must not make the download record reusable"
        );

        std::fs::write(manifest, br#"{"parts/example":{"bundleLength":1}}"#).unwrap();
        let tasks = executor
            .build_download_tasks(
                &info,
                &DownloadRecord::new(),
                &haruki_3d_record,
                executor.can_reuse_haruki_3d_download_record().await,
            )
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.bundle_path.as_str())
                .collect::<Vec<_>>(),
            vec!["start/a"],
            "the independent 3D record must skip an unchanged bundle even after staging cleanup"
        );
    }

    #[tokio::test]
    async fn three_d_only_completion_does_not_pollute_standard_download_record() {
        let mut record = DownloadRecord::new();
        let mut completed = 0;
        let mut completed_standard = 0;
        let mut pending_save_count = 0;

        AssetExecutionContext::record_completed_bundle(
            &None,
            "/unused/downloaded_assets.json",
            &mut record,
            &mut completed,
            &mut completed_standard,
            &mut pending_save_count,
            0,
            "jp",
            None,
            None,
            "live_pv/model/characterv2/body/01".to_string(),
            "3d-hash".to_string(),
            false,
        )
        .await;

        assert_eq!(completed, 1);
        assert_eq!(completed_standard, 0);
        assert!(record.is_empty());
    }

    #[test]
    fn deobfuscate_matches_go_headers() {
        assert_eq!(
            deobfuscate(&[0x20, 0x00, 0x00, 0x00, 1, 2, 3]),
            vec![1, 2, 3]
        );
        assert_eq!(deobfuscate(&[9, 8, 7]), vec![9, 8, 7]);
    }

    #[test]
    fn deobfuscate_owned_reuses_the_input_allocation() {
        let simple = vec![0x20, 0x00, 0x00, 0x00, 1, 2, 3];
        let simple_pointer = simple.as_ptr();
        let simple_capacity = simple.capacity();
        let simple = deobfuscate_owned(simple);
        assert_eq!(simple, vec![1, 2, 3]);
        assert_eq!(simple.as_ptr(), simple_pointer);
        assert_eq!(simple.capacity(), simple_capacity);

        let pattern = [0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00];
        let decoded = (0..132).map(|index| index as u8).collect::<Vec<_>>();
        let mut xor = vec![0x10, 0x00, 0x00, 0x00];
        xor.extend(decoded.iter().enumerate().map(|(index, byte)| {
            if index < 128 {
                byte ^ pattern[index % pattern.len()]
            } else {
                *byte
            }
        }));
        let xor_pointer = xor.as_ptr();
        let xor = deobfuscate_owned(xor);
        assert_eq!(xor, decoded);
        assert_eq!(xor.as_ptr(), xor_pointer);
    }

    #[tokio::test]
    async fn bundle_cache_status_validates_sidecar_before_loading_body() {
        let temp = tempdir().unwrap();
        let cache_file = temp.path().join("bundle-cache/cn/start/a");
        tokio::fs::create_dir_all(cache_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&cache_file, b"UnityFS cache body")
            .await
            .unwrap();
        let task = DownloadTask {
            download_path: "start/a".to_string(),
            bundle_path: "start/a".to_string(),
            bundle_hash: "expected-hash".to_string(),
            category: AssetCategory::StartApp,
            file_size: 22,
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };

        tokio::fs::write(bundle_cache_metadata_path(&cache_file), "stale-hash")
            .await
            .unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Stale
        );

        tokio::fs::write(bundle_cache_metadata_path(&cache_file), &task.bundle_hash)
            .await
            .unwrap();
        assert_eq!(
            AssetExecutionContext::bundle_cache_entry_status(&cache_file, &task).await,
            BundleCacheEntryStatus::Current
        );
    }

    #[tokio::test]
    async fn bundle_cache_downloads_once_then_avoids_network() {
        let temp = tempdir().unwrap();
        let cache_root = temp.path().join("bundle-cache");
        let request_count = Arc::new(AtomicUsize::new(0));
        let network_body = [
            &[0x20, 0x00, 0x00, 0x00],
            b"UnityFS cached test bundle".as_slice(),
        ]
        .concat();
        let app = Router::new().route(
            "/bundle/ond/a",
            get({
                let request_count = request_count.clone();
                let network_body = network_body.clone();
                move || {
                    let request_count = request_count.clone();
                    let network_body = network_body.clone();
                    async move {
                        request_count.fetch_add(1, Ordering::SeqCst);
                        Body::from(network_body)
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut config = AppConfig::default();
        config.execution.asset_bundle_cache_dir = Some(cache_root.to_string_lossy().into_owned());
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let context = AssetExecutionContext::new(&config, "cn", &region, &request).unwrap();
        let task = DownloadTask {
            download_path: "ond/a".to_string(),
            bundle_path: "ond/a".to_string(),
            bundle_hash: "hash-a".to_string(),
            category: AssetCategory::OnDemand,
            file_size: network_body.len() as i64,
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };
        let url = format!("http://{addr}/bundle/ond/a");

        let first = context
            .fetch_deobfuscated_bundle(&config, &url, &task)
            .await
            .unwrap();
        let second = context
            .fetch_deobfuscated_bundle(&config, &url, &task)
            .await
            .unwrap();

        assert_eq!(first.source.as_str(), "cache_miss");
        assert_eq!(second.source.as_str(), "cache_hit");
        assert_eq!(first.body, b"UnityFS cached test bundle");
        assert_eq!(second.body, first.body);
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        let cache_file = cache_root.join("cn/ond/a");
        assert_eq!(tokio::fs::read(&cache_file).await.unwrap(), first.body);
        assert_eq!(
            tokio::fs::read_to_string(bundle_cache_metadata_path(&cache_file))
                .await
                .unwrap(),
            "hash-a"
        );
    }

    #[tokio::test]
    async fn legacy_deobfuscated_bundle_cache_is_reused_without_network() {
        let temp = tempdir().unwrap();
        let cache_root = temp.path().join("bundle-cache");
        let cache_file = cache_root.join("cn/start/a");
        let cached_body = b"UnityFS legacy cached bundle";
        tokio::fs::create_dir_all(cache_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&cache_file, cached_body).await.unwrap();

        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: String::new(),
            asset_bundle_url_template: "http://127.0.0.1:1/{bundle_path}".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });
        let mut config = AppConfig::default();
        config.execution.asset_bundle_cache_dir = Some(cache_root.to_string_lossy().into_owned());
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::Update,
        };
        let context = AssetExecutionContext::new(&config, "cn", &region, &request).unwrap();
        let task = DownloadTask {
            download_path: "start/a".to_string(),
            bundle_path: "start/a".to_string(),
            bundle_hash: "hash-a".to_string(),
            category: AssetCategory::StartApp,
            file_size: (cached_body.len() + 4) as i64,
            priority: 0,
            export_payloads: true,
            stage_haruki_3d: false,
        };

        let fetch = context
            .fetch_deobfuscated_bundle(&config, "http://127.0.0.1:1/never", &task)
            .await
            .unwrap();

        assert_eq!(fetch.source.as_str(), "cache_hit");
        assert_eq!(fetch.body, cached_body);
    }

    #[test]
    fn download_filters_match_go_logic() {
        let region = test_region(RegionProviderConfig::ColorfulPalette {
            asset_info_url_template: "".to_string(),
            asset_bundle_url_template: "".to_string(),
            profile: "production".to_string(),
            profile_hashes: BTreeMap::from([("production".to_string(), "abc".to_string())]),
            required_cookies: false,
            cookie_bootstrap_url: None,
        });

        assert!(should_download_bundle(
            &region,
            "start/a",
            &AssetCategory::StartApp
        ));
        assert!(should_download_bundle(
            &region,
            "ond/a",
            &AssetCategory::OnDemand
        ));
        assert!(should_download_bundle(
            &region,
            "live_pv/model/characterv2/body/99/0018/ladies_s",
            &AssetCategory::LivePv
        ));
        assert!(!should_download_bundle(
            &region,
            "other/a",
            &AssetCategory::OnDemand
        ));
        assert!(!should_download_bundle(
            &region,
            "character/member/001",
            &AssetCategory::LivePv
        ));
    }

    #[test]
    fn post_process_backlog_capacity_tracks_post_process_pressure() {
        assert_eq!(post_process_backlog_capacity(0, 0), 1);
        assert_eq!(post_process_backlog_capacity(8, 2), 4);
        assert_eq!(post_process_backlog_capacity(4, 12), 24);
    }

    #[test]
    fn raw_bundle_output_path_appends_bundle_extension_and_rejects_unsafe_paths() {
        let root = std::path::Path::new("/tmp/raw-root");
        assert_eq!(
            raw_bundle_output_path(root, "live_pv/model/character/body/foo").unwrap(),
            root.join("live_pv/model/character/body/foo.bundle")
        );
        assert_eq!(
            raw_bundle_output_path(root, "character/motion/costume_setting/01_00.bundle").unwrap(),
            root.join("character/motion/costume_setting/01_00.bundle")
        );
        assert!(raw_bundle_output_path(root, "").is_err());
        assert!(raw_bundle_output_path(root, "/absolute/path").is_err());
        assert!(raw_bundle_output_path(root, "../escape").is_err());
        assert!(raw_bundle_output_path(root, "safe/../escape").is_err());
        assert!(raw_bundle_output_path(root, "safe/./escape").is_err());
    }

    #[test]
    fn bundle_hash_index_uses_exporter_relative_bundle_path() {
        assert_eq!(
            bundle_hash_index_key("live_pv/model/characterv2/body/01/0001").unwrap(),
            "live_pv/model/characterv2/body/01/0001.bundle"
        );
        assert_eq!(
            bundle_hash_index_key("character/motion/01.bundle").unwrap(),
            "character/motion/01.bundle"
        );
    }

    #[test]
    fn bundle_dependency_closure_is_recursive_and_cycle_safe() {
        let detail = |name: &str, dependencies: &[&str]| AssetBundleDetail {
            bundle_name: name.to_string(),
            cache_file_name: String::new(),
            cache_directory_name: String::new(),
            hash: String::new(),
            category: AssetCategory::OnDemand,
            crc: 0,
            file_size: 0,
            dependencies: dependencies.iter().map(|value| value.to_string()).collect(),
            paths: Vec::new(),
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        };
        let info = AssetBundleInfo {
            version: None,
            os: None,
            bundles: HashMap::from([
                ("body".to_string(), detail("body", &["material", "shared"])),
                ("material".to_string(), detail("material", &["texture"])),
                ("texture".to_string(), detail("texture", &["body"])),
                ("shared".to_string(), detail("shared", &[])),
            ]),
        };

        assert_eq!(
            bundle_dependency_closure(&info, "body"),
            vec![
                "material".to_string(),
                "shared".to_string(),
                "texture".to_string()
            ]
        );
    }

    #[test]
    fn exporter_metrics_keep_summary_lines_only() {
        let stdout = b"Started worker\nPart export metrics: built=3, restored=7\nnoise\nPart export parent metrics: totalMs=42\n";
        assert_eq!(
            exporter_metric_lines(stdout),
            "Part export metrics: built=3, restored=7 | Part export parent metrics: totalMs=42"
        );
    }

    #[test]
    fn sparse_recovery_parses_only_safe_missing_bundle_markers() {
        let stderr = "failure\nHARUKI_3D_MISSING_BUNDLE=live_pv/model/body/0001\n\
HARUKI_3D_MISSING_BUNDLE=../escape\nHARUKI_3D_MISSING_BUNDLE=live_pv/model/body/0001\n";
        assert_eq!(
            missing_haruki_3d_bundle_paths(stderr),
            vec!["live_pv/model/body/0001".to_string()]
        );
    }

    #[tokio::test]
    async fn bundle_hash_index_checkpoint_is_durable() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("bundle-hashes.json");
        let index = Arc::new(std::sync::Mutex::new(DownloadRecord::from([(
            "live_pv/model/body.bundle".to_string(),
            "ab".repeat(32),
        )])));

        AssetExecutionContext::save_bundle_hash_index_checkpoint(Some(&path), Some(&index))
            .await
            .unwrap();

        assert_eq!(
            crate::core::download_records::load_download_record(&path).unwrap(),
            index.lock().unwrap().clone()
        );
    }

    #[tokio::test]
    async fn prefetch_can_fetch_asset_info_and_download_bundle() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
                    category: AssetCategory::StartApp,
                    crc: 123,
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: None,
                },
            )]),
        };
        let encrypted = encrypt_asset_info(&info);

        let app = Router::new()
            .route(
                "/info/production/abc/1/hash",
                get({
                    let encrypted = encrypted.clone();
                    move || async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            encrypted.clone(),
                        )
                    }
                }),
            )
            .route(
                "/bundle/start/a",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                        Body::from(vec![
                            0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N', b'D', b'L', b'E',
                        ]),
                    )
                }),
            )
            .route("/signature", post(|| async { "ok" }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{addr}/info/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^start/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            backends: crate::core::config::BackendsConfig {
                media: crate::core::config::MediaBackendConfig {
                    ffmpeg_path: "ffmpeg".to_string(),
                    ..crate::core::config::MediaBackendConfig::default()
                },
                ..crate::core::config::BackendsConfig::default()
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();
        assert_eq!(summary.completed_downloads, 1);

        assert_eq!(summary.failed_downloads, 0);
        assert_eq!(
            std::fs::read(save_dir.join("AssetBundles/start/a.bundle")).unwrap(),
            b"BUNDLE"
        );
        assert!(!record_file.exists());
    }

    #[tokio::test]
    async fn required_cookies_are_forwarded_and_nuverse_uses_resolved_version() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "ond/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "ond/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
                    category: AssetCategory::OnDemand,
                    crc: 888,
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: Some("download-root".to_string()),
                },
            )]),
        };
        let encrypted = encrypt_asset_info(&info);
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let version_hits = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/version/5.2.0",
                get({
                    let version_hits = version_hits.clone();
                    move || {
                        let version_hits = version_hits.clone();
                        async move {
                            version_hits.fetch_add(1, Ordering::SeqCst);
                            "20250321"
                        }
                    }
                }),
            )
            .route(
                "/info/5.2.0/20250321",
                get({
                    let encrypted = encrypted.clone();
                    move || async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            encrypted.clone(),
                        )
                    }
                }),
            )
            .route(
                "/bundle/download-root/ond/a",
                get({
                    let cookie_seen = cookie_seen.clone();
                    move |headers: HeaderMap| {
                        let cookie_seen = cookie_seen.clone();
                        async move {
                            if headers
                                .get(COOKIE)
                                .and_then(|value| value.to_str().ok())
                                .is_some_and(|value| value.contains("session=abc"))
                            {
                                cookie_seen.store(true, Ordering::SeqCst);
                            }
                            (
                                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                                Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                            )
                        }
                    }
                }),
            )
            .route(
                "/signature",
                post(|| async move { ([(SET_COOKIE.as_str(), "session=abc; Path=/")], "ok") }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::Nuverse {
                asset_version_url: format!("http://{addr}/version/{{app_version}}"),
                app_version: "5.2.0".to_string(),
                asset_info_url_template: format!(
                    "http://{addr}/info/{{app_version}}/{{asset_version}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                required_cookies: true,
                cookie_bootstrap_url: Some(format!("http://{addr}/signature")),
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: Vec::new(),
                on_demand: vec!["^ond/".to_string()],
                skip: Vec::new(),
                priority: vec!["^ond/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^ond/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("cn".to_string(), region.clone());
        let config = AppConfig {
            regions,
            backends: crate::core::config::BackendsConfig {
                media: crate::core::config::MediaBackendConfig {
                    ffmpeg_path: "ffmpeg".to_string(),
                    ..crate::core::config::MediaBackendConfig::default()
                },
                ..crate::core::config::BackendsConfig::default()
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            concurrency: crate::core::config::ConcurrencyConfig {
                download: 2,
                ..crate::core::config::ConcurrencyConfig::default()
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "cn".to_string(),
            asset_version: None,
            asset_hash: None,
            dry_run: false,
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "cn", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();
        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(version_hits.load(Ordering::SeqCst), 1);
        assert!(cookie_seen.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn http_fetch_retries_on_503_then_succeeds() {
        let temp = tempdir().unwrap();
        let record_file = temp.path().join("downloaded_assets.json");
        let save_dir = temp.path().join("exports");

        let info = AssetBundleInfo {
            version: Some("1".to_string()),
            os: Some("ios".to_string()),
            bundles: HashMap::from([(
                "start/a".to_string(),
                AssetBundleDetail {
                    bundle_name: "start/a".to_string(),
                    cache_file_name: "a".to_string(),
                    cache_directory_name: "d".to_string(),
                    hash: "hash-a".to_string(),
                    category: AssetCategory::StartApp,
                    crc: 123,
                    file_size: 1,
                    dependencies: Vec::new(),
                    paths: Vec::new(),
                    is_builtin: false,
                    is_relocate: None,
                    md5_hash: None,
                    download_path: None,
                },
            )]),
        };
        let encrypted = encrypt_asset_info(&info);
        let info_hits = Arc::new(AtomicUsize::new(0));

        let app = Router::new()
            .route(
                "/info/production/abc/1/hash",
                get({
                    let encrypted = encrypted.clone();
                    let info_hits = info_hits.clone();
                    move || {
                        let encrypted = encrypted.clone();
                        let info_hits = info_hits.clone();
                        async move {
                            let attempt = info_hits.fetch_add(1, Ordering::SeqCst);
                            if attempt < 2 {
                                (
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                    Body::from("retry"),
                                )
                            } else {
                                (axum::http::StatusCode::OK, Body::from(encrypted.clone()))
                            }
                        }
                    }
                }),
            )
            .route(
                "/bundle/start/a",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                        Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                    )
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());
        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: format!(
                    "http://{addr}/info/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
                ),
                asset_bundle_url_template: format!("http://{addr}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            crypto: crate::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
            },
            filters: crate::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            export: crate::core::config::RegionExportConfig {
                raw_bundles: Some(RawBundleExportConfig {
                    output_dir: None,
                    include: vec!["^start/".to_string()],
                    exclude: Vec::new(),
                }),
                haruki_3d: crate::core::config::Haruki3dExportConfig {
                    enabled: true,
                    ..crate::core::config::Haruki3dExportConfig::default()
                },
                ..crate::core::config::RegionExportConfig::default()
            },
            ..RegionConfig::default()
        };

        let mut regions = BTreeMap::new();
        regions.insert("jp".to_string(), region.clone());
        let config = AppConfig {
            regions,
            execution: crate::core::config::ExecutionConfig {
                retry: crate::core::config::RetryConfig {
                    attempts: 3,
                    initial_backoff_ms: 1,
                    max_backoff_ms: 1,
                },
                ..crate::core::config::ExecutionConfig::default()
            },
            ..AppConfig::default()
        };
        let request = AssetUpdateRequest {
            region: "jp".to_string(),
            asset_version: Some("1".to_string()),
            asset_hash: Some("hash".to_string()),
            dry_run: false,
            mode: AssetUpdateMode::PrefetchRawBundles,
        };

        let executor = AssetExecutionContext::new(&config, "jp", &region, &request).unwrap();
        let summary = executor
            .prefetch_asset_bundles(&config, None, None)
            .await
            .unwrap();

        assert_eq!(summary.completed_downloads, 1);
        assert_eq!(info_hits.load(Ordering::SeqCst), 3);
    }
}
