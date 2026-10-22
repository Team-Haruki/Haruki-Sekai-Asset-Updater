//! HTTP surface of the runtime app/Unity versions.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use haruki_sekai_asset_updater::core::config::{
    AppConfig, AuthConfig, RegionConfig, RegionPathsConfig, RegionProviderConfig, ServerConfig,
};
use haruki_sekai_asset_updater::service::http::{build_router, AppState};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use tower::ServiceExt;

/// A CDN whose version URL answers `100` for 6.5.0 and 6.6.0 only.
async fn version_cdn() -> String {
    let app = Router::new().route(
        "/version/{app_version}",
        get(
            |axum::extract::Path(app_version): axum::extract::Path<String>| async move {
                if app_version == "6.5.0" || app_version == "6.6.0" {
                    (StatusCode::OK, "100")
                } else {
                    (StatusCode::NOT_FOUND, "")
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{address}")
}

fn config(cdn: &str, state_file: &std::path::Path, sync_enabled: bool) -> AppConfig {
    let scratch = state_file.parent().unwrap();
    let mut regions = BTreeMap::new();
    regions.insert(
        "cn".to_string(),
        RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::Nuverse {
                asset_version_url: format!("{cdn}/version/{{app_version}}"),
                app_version: "6.4.0".to_string(),
                asset_info_url_template: format!("{cdn}/info/{{app_version}}/{{asset_version}}"),
                asset_bundle_url_template: format!("{cdn}/bundle/{{bundle_path}}"),
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some(scratch.join("cn").display().to_string()),
                downloaded_asset_record_file: Some(
                    scratch.join("cn/downloaded.json").display().to_string(),
                ),
            },
            ..RegionConfig::default()
        },
    );
    let mut profile_hashes = BTreeMap::new();
    profile_hashes.insert("production".to_string(), "abc".to_string());
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
                asset_save_dir: Some(scratch.join("jp").display().to_string()),
                downloaded_asset_record_file: Some(
                    scratch.join("jp/downloaded.json").display().to_string(),
                ),
            },
            ..RegionConfig::default()
        },
    );
    let mut config = AppConfig {
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
    };
    config.app_version_sync.state_file = state_file.display().to_string();
    config.app_version_sync.enabled = sync_enabled;
    config.app_version_sync.registry_url = "http://127.0.0.1:1".to_string();
    config.app_version_sync.request_timeout_secs = 5;
    config
}

async fn call(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, sonic_rs::Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("user-agent", "HarukiTest/1.0")
        .header("authorization", "Bearer secret-token")
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = sonic_rs::from_slice(&bytes).unwrap_or_default();
    (status, value)
}

async fn app(sync_enabled: bool) -> (Router, AppState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cdn = version_cdn().await;
    let state = AppState::new(Arc::new(config(
        &cdn,
        &dir.path().join("runtime-versions.json"),
        sync_enabled,
    )));
    (build_router(state.clone()), state, dir)
}

fn region<'a>(payload: &'a sonic_rs::Value, name: &str) -> &'a sonic_rs::Value {
    payload["regions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|region| region["region"].as_str() == Some(name))
        .unwrap()
}

#[tokio::test]
async fn regions_require_auth_and_list_current_values() {
    let (app, _, _dir) = app(false).await;
    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/regions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let (status, payload) = call(&app, Method::GET, "/v2/regions", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["app_version_sync_enabled"].as_bool(), Some(false));
    let cn = region(&payload, "cn");
    assert_eq!(cn["provider_kind"].as_str(), Some("nuverse"));
    assert_eq!(cn["app_version"]["effective"].as_str(), Some("6.4.0"));
    assert_eq!(cn["app_version"]["configured"].as_str(), Some("6.4.0"));
    assert_eq!(
        cn["unity_version"]["effective"].as_str(),
        Some("2022.3.21f1")
    );
    let jp = region(&payload, "jp");
    assert!(jp.get("app_version").is_none());
}

#[tokio::test]
async fn put_adopts_probes_and_refuses_downgrades_and_delete_reverts() {
    let (app, _, dir) = app(false).await;
    let uri = "/v2/regions/cn/app-version";

    let cases = [
        ("/v2/regions/cn/app-version", "{}", StatusCode::BAD_REQUEST),
        (
            "/v2/regions/cn/app-version",
            r#"{"unity_version":"2022"}"#,
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v2/regions/xx/app-version",
            r#"{"unity_version":"2022.3.62f1"}"#,
            StatusCode::NOT_FOUND,
        ),
        (
            "/v2/regions/jp/app-version",
            r#"{"app_version":"6.5.0"}"#,
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v2/regions/cn/app-version",
            r#"{"app_version":"6.7.0"}"#,
            StatusCode::BAD_GATEWAY,
        ),
    ];
    for (path, body, expected) in cases {
        let (status, payload) = call(&app, Method::PUT, path, Some(body)).await;
        assert_eq!(status, expected, "{path} {body}: {payload:?}");
    }

    // 6.4.1 is not on the CDN, but its patch-zeroed 6.4.0 is the current value.
    let (status, payload) = call(&app, Method::PUT, uri, Some(r#"{"app_version":"6.4.1"}"#)).await;
    assert_eq!(status, StatusCode::OK, "{payload:?}");
    assert_eq!(payload["message"].as_str(), Some("unchanged"));
    assert!(payload["region"]["app_version"].get("override").is_none());

    let (status, payload) = call(
        &app,
        Method::PUT,
        uri,
        Some(r#"{"app_version":"6.5.0","unity_version":"2022.3.62f1"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{payload:?}");
    assert_eq!(
        payload["message"].as_str(),
        Some("app_version 6.4.0 -> 6.5.0; unity_version -> 2022.3.62f1")
    );
    assert_eq!(
        payload["region"]["app_version"]["override"]["source"].as_str(),
        Some("manual")
    );
    assert!(dir.path().join("runtime-versions.json").exists());

    let (status, payload) = call(&app, Method::PUT, uri, Some(r#"{"app_version":"6.5.0"}"#)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["message"].as_str(), Some("unchanged"));

    let (status, _) = call(&app, Method::PUT, uri, Some(r#"{"app_version":"6.3.0"}"#)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, payload) = call(
        &app,
        Method::PUT,
        uri,
        Some(r#"{"app_version":"6.3.0","force":true}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        payload["region"]["app_version"]["effective"].as_str(),
        Some("6.3.0")
    );

    let (status, _) = call(&app, Method::DELETE, &format!("{uri}?field=bogus"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, payload) = call(
        &app,
        Method::DELETE,
        &format!("{uri}?field=app_version"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        payload["region"]["app_version"]["effective"].as_str(),
        Some("6.4.0")
    );
    assert_eq!(
        payload["region"]["unity_version"]["effective"].as_str(),
        Some("2022.3.62f1")
    );
    let (_, payload) = call(
        &app,
        Method::DELETE,
        &format!("{uri}?field=unity_version"),
        None,
    )
    .await;
    assert_eq!(
        payload["region"]["unity_version"]["effective"].as_str(),
        Some("2022.3.21f1")
    );
    let (status, _) = call(&app, Method::DELETE, uri, None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, Method::DELETE, "/v2/regions/xx/app-version", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unity_version_only_puts_work_for_colorful_palette_regions() {
    let (app, _, _dir) = app(false).await;
    let (status, payload) = call(
        &app,
        Method::PUT,
        "/v2/regions/JP/app-version",
        Some(r#"{"unity_version":"2022.3.62f1"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{payload:?}");
    assert_eq!(
        payload["region"]["unity_version"]["effective"].as_str(),
        Some("2022.3.62f1")
    );
}

#[tokio::test]
async fn update_requests_offer_their_app_version() {
    let (app, _, _dir) = app(false).await;
    let cases = [
        (
            r#"{"region":"cn","dry_run":true,"app_version":"6.7.0"}"#,
            "not adopted",
            "6.4.0",
        ),
        (
            r#"{"region":"cn","dry_run":true,"app_version":"6.4.1"}"#,
            "6.4.1 is already current",
            "6.4.0",
        ),
        (
            r#"{"region":"cn","dry_run":true,"app_version":"6.5.2"}"#,
            "adopted 6.5.0 (was 6.4.0)",
            "6.5.0",
        ),
        (
            r#"{"region":"cn","dry_run":true,"app_version":"6.5.0"}"#,
            "6.5.0 is already current",
            "6.5.0",
        ),
        (
            r#"{"region":"jp","dry_run":true,"asset_version":"1","asset_hash":"h","app_version":"6.5.0"}"#,
            "ignored; not a nuverse region",
            "",
        ),
    ];
    for (body, note, job_app_version) in cases {
        let (status, payload) = call(&app, Method::POST, "/v2/assets/update", Some(body)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{payload:?}");
        assert!(
            payload["app_version"].as_str().unwrap().starts_with(note),
            "{payload:?}"
        );
        assert_eq!(
            payload["job"]["app_version"].as_str().unwrap_or_default(),
            job_app_version
        );
    }

    let (status, payload) = call(
        &app,
        Method::POST,
        "/v2/assets/update",
        Some(r#"{"region":"cn","dry_run":true,"app_version":" "}"#),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(payload.get("app_version").is_none());

    let (status, _) = call(
        &app,
        Method::POST,
        "/v2/assets/update",
        Some(r#"{"region":"xx","app_version":"6.6.0"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, payload) = call(&app, Method::GET, "/v2/regions", None).await;
    assert_eq!(
        region(&payload, "cn")["app_version"]["override"]["source"].as_str(),
        Some("update_request")
    );
}

#[tokio::test]
async fn healthz_reports_sync_failures_only_when_enabled() {
    let (app, state, _dir) = app(true).await;
    state.app_versions().poll_once().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&bytes).unwrap();
    let sync = &payload["app_version_sync"];
    assert_eq!(sync["max_consecutive_registry_failures"].as_u64(), Some(1));
    assert_eq!(sync["max_consecutive_probe_failures"].as_u64(), Some(0));
    assert_eq!(
        sync["regions"]["cn"]["last_result"].as_str(),
        Some("registry_failed")
    );
    let (_, regions) = call(&app, Method::GET, "/v2/regions", None).await;
    assert_eq!(
        region(&regions, "cn")["sync"]["consecutive_registry_failures"].as_u64(),
        Some(1)
    );

    let (app, _, _dir) = self::app(false).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: sonic_rs::Value = sonic_rs::from_slice(&bytes).unwrap();
    assert!(payload.get("app_version_sync").is_none());
}
