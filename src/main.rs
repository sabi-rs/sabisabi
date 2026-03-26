use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let settings = sabisabi::Settings::from_env();
    let bind_address = settings.socket_address();
    let state = Arc::new(sabisabi::AppState::from_settings(settings)?);
    let app = sabisabi::build_router(state);

    let listener = TcpListener::bind(&bind_address).await?;
    info!(address = %bind_address, "starting sabisabi");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
