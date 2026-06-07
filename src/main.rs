use std::sync::Arc;

use haruki_sekai_asset_updater::core::config::AppConfig;
use haruki_sekai_asset_updater::service::http::{build_router, AppState};
use haruki_sekai_asset_updater::service::logging::init_logging;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load_default().await?);
    let _logging_guards = init_logging(&config)?;
    info!(
        "========================= Haruki Sekai Asset Updater v{} =========================",
        env!("CARGO_PKG_VERSION")
    );
    info!("Powered by Haruki Dev Team");

    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    info!(
        bind_addr = %bind_addr,
        config_version = config.config_version,
        enabled_regions = ?config.enabled_regions(),
        "starting haruki-sekai-asset-updater"
    );

    let state = AppState::new(config.clone());
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let local_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| bind_addr.clone());

    info!(addr = %local_addr, "listening at http://{local_addr}");

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("haruki-sekai-asset-updater shutdown complete");
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

    info!("shutdown signal received");
}
