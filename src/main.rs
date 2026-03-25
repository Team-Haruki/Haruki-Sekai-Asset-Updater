use std::sync::Arc;

use haruki_sekai_asset_updater::core::config::AppConfig;
use haruki_sekai_asset_updater::service::http::{build_router, AppState};
use haruki_sekai_asset_updater::service::logging::init_logging;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load_default()?);
    let _logging_guards = init_logging(&config)?;

    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    write_startup_hint(&config, &bind_addr)?;
    let state = AppState::new(config.clone());
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    info!(
        bind_addr = %bind_addr,
        enabled_regions = ?config.enabled_regions(),
        "starting haruki-sekai-asset-updater"
    );

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

fn write_startup_hint(config: &AppConfig, bind_addr: &str) -> Result<(), std::io::Error> {
    let file_path = config
        .logging
        .file
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty());

    if let Some(file_path) = file_path {
        let path = std::path::Path::new(file_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        use std::io::Write;
        writeln!(
            file,
            "starting haruki-sekai-asset-updater bind_addr={bind_addr}"
        )?;
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to install ctrl-c handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                let _ = signal.recv().await;
            }
            Err(err) => warn!(error = %err, "failed to install terminate handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
