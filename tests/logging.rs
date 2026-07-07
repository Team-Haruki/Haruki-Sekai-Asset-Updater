use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tempfile::tempdir;
use tokio::process::Command;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn binary_path() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_haruki-sekai-asset-updater") {
        return PathBuf::from(path);
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = std::process::Command::new("cargo")
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_dir.join("Cargo.toml"))
        .arg("--bin")
        .arg("haruki-sekai-asset-updater")
        .status()
        .expect("cargo build can be spawned");
    assert!(status.success(), "cargo build for binary succeeded");

    manifest_dir
        .join("target")
        .join("debug")
        .join("haruki-sekai-asset-updater")
}

fn write_config(path: &Path, port: u16, main_log: &Path, access_log: &Path) {
    let yaml = format!(
        r#"
config_version: 3
server:
  host: "127.0.0.1"
  port: {port}
  auth:
    enabled: false
logging:
  level: "INFO"
  format: "pretty"
  file: "{main_log}"
  access:
    enabled: true
    format: "[${{time}}] ${{status}} ${{method}} ${{path}} ${{latency}}"
    file: "{access_log}"
regions:
  jp:
    enabled: true
    provider:
      kind: colorful_palette
      asset_info_url_template: "https://example.com/{{env}}/{{hash}}/{{asset_version}}/{{asset_hash}}"
      asset_bundle_url_template: "https://example.com/{{bundle_path}}"
      profile: "production"
      profile_hashes:
        production: abc
      required_cookies: false
    paths:
      asset_save_dir: "./Data/jp-assets"
      downloaded_asset_record_file: "./Data/jp-assets/downloaded_assets.json"
"#,
        port = port,
        main_log = main_log.display(),
        access_log = access_log.display(),
    );

    fs::write(path, yaml).unwrap();
}

async fn wait_for_health(port: u16) {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/healthz");
    for _ in 0..50 {
        if let Ok(response) = client.get(&url).send().await {
            if response.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become healthy");
}

#[tokio::test]
async fn binary_writes_main_and_access_logs_to_files() {
    let temp = tempdir().unwrap();
    let port = free_port();
    let config_path = temp.path().join("config.yaml");
    let main_log = temp.path().join("main.log");
    let access_log = temp.path().join("access.log");
    write_config(&config_path, port, &main_log, &access_log);

    let binary = binary_path();

    let mut child = Command::new(binary)
        .env("HARUKI_CONFIG_PATH", &config_path)
        .env_remove("RUST_LOG")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_health(port).await;

    let client = reqwest::Client::new();
    let _ = client
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .unwrap();
    let _ = client
        .post(format!("http://127.0.0.1:{port}/v2/assets/update"))
        .header("content-type", "application/json")
        .body(r#"{"region":"jp","asset_version":"1","asset_hash":"hash","dry_run":true}"#)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Close client connections before signaling so the server's graceful shutdown isn't blocked
    // waiting on idle keep-alive connections held open by the pooled reqwest client.
    drop(client);

    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    #[cfg(not(unix))]
    let _ = child.start_kill();

    // Give graceful shutdown a short window, then force-kill so the test can't hang regardless of
    // shutdown timing. The log assertions below are written during startup/request handling, so
    // they don't depend on a graceful exit.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if std::time::Instant::now() >= deadline => {
                let _ = child.start_kill();
                break;
            }
            Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(_) => break,
        }
    }
    let output = child.wait_with_output().await.unwrap();

    let main_contents = fs::read_to_string(&main_log).unwrap();
    let access_contents = fs::read_to_string(&access_log).unwrap();
    let stdout_contents = String::from_utf8_lossy(&output.stdout);

    assert!(
        main_contents.contains("starting haruki-sekai-asset-updater"),
        "unexpected main log contents: {main_contents}"
    );
    assert!(
        stdout_contents.contains("starting haruki-sekai-asset-updater"),
        "expected startup log on stdout when main log file is enabled, got: {stdout_contents}"
    );
    assert!(access_contents.contains("/healthz"));
    assert!(access_contents.contains("/v2/assets/update"));
}
