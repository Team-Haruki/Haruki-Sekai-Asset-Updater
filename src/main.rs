use std::sync::Arc;
use std::time::Duration;

use haruki_sekai_asset_updater::core::config::AppConfig;
use haruki_sekai_asset_updater::service::http::{build_router, AppState};
use haruki_sekai_asset_updater::service::logging::init_logging;
use tracing::{info, warn};

/// After a shutdown signal, force the process to exit if the graceful drain hasn't finished within
/// this window, so a lingering (e.g. idle keep-alive) connection can't keep the process alive
/// indefinitely on SIGTERM. Sized to stay under Docker's default 10s stop grace period.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);

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

    // Safety net: graceful drain waits for in-flight connections to close, which can stall on a
    // lingering idle keep-alive connection. Arm a hard deadline so SIGTERM always terminates the
    // process within a bounded time instead of hanging. If the drain finishes first, `main`
    // returns and this detached timer is dropped before it fires.
    tokio::spawn(async {
        tokio::time::sleep(GRACEFUL_SHUTDOWN_TIMEOUT).await;
        warn!(
            timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT.as_secs(),
            "graceful shutdown did not complete in time; forcing exit"
        );
        std::process::exit(0);
    });
}
