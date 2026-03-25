use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::{Method, Request, StatusCode};
use haruki_sekai_asset_updater::core::config::{
    AppConfig, AuthConfig, ChartHashConfig, GitSyncConfig, RegionConfig, RegionPathsConfig,
    RegionProviderConfig, ServerConfig,
};
use haruki_sekai_asset_updater::service::http::{build_router, AppState};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use tower::ServiceExt;

const TEST_AES_KEY_HEX: &str = "00112233445566778899aabbccddeeff";
const TEST_AES_IV_HEX: &str = "0102030405060708090a0b0c0d0e0f10";

fn test_config() -> AppConfig {
    let mut profile_hashes = BTreeMap::new();
    profile_hashes.insert("production".to_string(), "abc123".to_string());

    let mut regions = BTreeMap::new();
    regions.insert(
        "jp".to_string(),
        RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template: "https://info/{env}/{hash}/{asset_version}/{asset_hash}"
                    .to_string(),
                asset_bundle_url_template: "https://bundle/{bundle_path}".to_string(),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some("./Data/jp-assets".to_string()),
                downloaded_asset_record_file: Some(
                    "./Data/jp-assets/downloaded_assets.json".to_string(),
                ),
            },
            ..RegionConfig::default()
        },
    );
    regions.insert(
        "en".to_string(),
        RegionConfig {
            enabled: false,
            ..RegionConfig::default()
        },
    );

    AppConfig {
        server: ServerConfig {
            auth: AuthConfig {
                enabled: true,
                user_agent_prefix: Some("HarukiTest/".to_string()),
                bearer_token: Some("secret-token".to_string()),
            },
            ..ServerConfig::default()
        },
        regions,
        ..AppConfig::default()
    }
}

#[tokio::test]
async fn healthz_reports_enabled_regions() {
    let state = AppState::new(Arc::new(test_config()));
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn submit_update_requires_auth() {
    let state = AppState::new(Arc::new(test_config()));
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v2/assets/update")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"region":"jp","asset_version":"1","asset_hash":"h"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn submit_update_rejects_disabled_region() {
    let state = AppState::new(Arc::new(test_config()));
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v2/assets/update")
                .header("content-type", "application/json")
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::from(r#"{"region":"en"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn submit_update_accepts_and_job_can_be_queried() {
    let state = AppState::new(Arc::new(test_config()));
    let app = build_router(state.clone());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v2/assets/update")
                .header("content-type", "application/json")
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::from(
                    r#"{"region":"jp","asset_version":"6.0.0","asset_hash":"deadbeef","dry_run":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    let job_id = payload["job"]["id"].as_str().unwrap().to_string();

    tokio::time::sleep(Duration::from_millis(25)).await;

    let job_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v2/jobs/{job_id}"))
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(job_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(job_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(payload["job"]["status"].as_str(), Some("completed"));
    assert_eq!(
        payload["job"]["message"].as_str(),
        Some("dry-run plan completed")
    );
    assert_eq!(
        payload["job"]["progress"]["phase"].as_str(),
        Some("completed")
    );
    assert!(payload["job"]["failure"].is_null());
    assert!(payload["job"]["plan"]["download_record_file"].is_str());
    assert!(payload["job"]["plan"]["url_preview"]["asset_info_url"].is_str());
}

#[tokio::test]
async fn submit_update_non_dry_run_executes_pipeline() {
    use aes::cipher::block_padding::Pkcs7;
    use aes::cipher::{BlockEncryptMut, KeyIvInit};
    use axum::routing::get;
    use axum::Router;
    use haruki_sekai_asset_updater::core::asset_execution::{
        AssetBundleDetail, AssetBundleInfo, AssetCategory,
    };
    use std::collections::HashMap;
    use tempfile::tempdir;

    type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;

    fn encrypt_asset_info(info: &AssetBundleInfo) -> Vec<u8> {
        let key = hex::decode(TEST_AES_KEY_HEX).unwrap();
        let iv = hex::decode(TEST_AES_IV_HEX).unwrap();
        let payload = rmp_serde::to_vec_named(info).unwrap();
        let mut padded = payload.clone();
        let original_len = padded.len();
        let padding = 16 - (original_len % 16);
        padded.resize(original_len + padding, 0);
        Aes128CbcEnc::new_from_slices(&key, &iv)
            .unwrap()
            .encrypt_padded_mut::<Pkcs7>(&mut padded, original_len)
            .unwrap()
            .to_vec()
    }

    let temp = tempdir().unwrap();
    let record_file = temp.path().join("downloaded_assets.json");
    let save_dir = temp.path().join("exports");
    let asset_info = AssetBundleInfo {
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
    let encrypted = encrypt_asset_info(&asset_info);
    let bundle_hits = Arc::new(AtomicUsize::new(0));

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
            get({
                let bundle_hits = bundle_hits.clone();
                move |_headers: HeaderMap| {
                    let bundle_hits = bundle_hits.clone();
                    async move {
                        bundle_hits.fetch_add(1, Ordering::SeqCst);
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                            Body::from(vec![0x20, 0x00, 0x00, 0x00, b'B', b'U', b'N']),
                        )
                    }
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut profile_hashes = BTreeMap::new();
    profile_hashes.insert("production".to_string(), "abc".to_string());
    let mut regions = BTreeMap::new();
    regions.insert(
        "jp".to_string(),
        RegionConfig {
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
            crypto: haruki_sekai_asset_updater::core::config::CryptoConfig {
                aes_key_hex: Some(TEST_AES_KEY_HEX.to_string()),
                aes_iv_hex: Some(TEST_AES_IV_HEX.to_string()),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(save_dir.to_string_lossy().into_owned()),
                downloaded_asset_record_file: Some(record_file.to_string_lossy().into_owned()),
            },
            filters: haruki_sekai_asset_updater::core::config::RegionFiltersConfig {
                start_app: vec!["^start/".to_string()],
                on_demand: Vec::new(),
                skip: Vec::new(),
                priority: vec!["^start/".to_string()],
            },
            ..RegionConfig::default()
        },
    );

    let config = AppConfig {
        server: ServerConfig {
            auth: AuthConfig {
                enabled: true,
                user_agent_prefix: Some("HarukiTest/".to_string()),
                bearer_token: Some("secret-token".to_string()),
            },
            ..ServerConfig::default()
        },
        regions,
        git_sync: GitSyncConfig {
            chart_hashes: ChartHashConfig::default(),
        },
        tools: haruki_sekai_asset_updater::core::config::ToolsConfig {
            ffmpeg_path: "ffmpeg".to_string(),
            asset_studio_cli_path: None,
        },
        ..AppConfig::default()
    };

    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v2/assets/update")
                .header("content-type", "application/json")
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::from(
                    r#"{"region":"jp","asset_version":"1","asset_hash":"hash","dry_run":false}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    let job_id = payload["job"]["id"].as_str().unwrap().to_string();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let job_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v2/jobs/{job_id}"))
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(job_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(job_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(payload["job"]["status"].as_str(), Some("completed"));
    assert_eq!(
        payload["job"]["execution"]["completed_downloads"].as_u64(),
        Some(1)
    );
    assert_eq!(
        payload["job"]["progress"]["completed_downloads"].as_u64(),
        Some(1)
    );
    assert_eq!(
        payload["job"]["progress"]["phase"].as_str(),
        Some("completed")
    );
    assert!(payload["job"]["progress"]["recent_events"].is_array());
    assert!(payload["job"]["progress"]["recent_events"]
        .as_array()
        .is_some_and(|events| !events.is_empty()));
    assert!(payload["job"]["failure"].is_null());
    assert_eq!(bundle_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_job_marks_snapshot_cancelled() {
    let state = AppState::new(Arc::new(test_config()));
    let app = build_router(state.clone());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v2/assets/update")
                .header("content-type", "application/json")
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::from(r#"{"region":"jp","dry_run":false}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    let job_id = payload["job"]["id"].as_str().unwrap().to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/v2/jobs/{job_id}/cancel"))
                .header("user-agent", "HarukiTest/1.0")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(payload["job"]["status"].as_str(), Some("cancelled"));
    assert_eq!(
        payload["job"]["progress"]["phase"].as_str(),
        Some("cancelled")
    );
    assert_eq!(
        payload["job"]["failure"]["kind"].as_str(),
        Some("cancelled")
    );
}
