use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use my_k8s::apiserver::{handlers::AppState, routes::router, storage::PodStore};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "apiserver", version)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    #[arg(long, default_value = "/var/lib/my-k8s/etcd-like")]
    db: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(?args, "apiserver starting");

    if let Some(parent) = args.db.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir {parent:?} for sled DB"))?;
    }

    let store =
        PodStore::open(&args.db).with_context(|| format!("opening sled DB at {:?}", args.db))?;
    let state = AppState {
        store: Arc::new(store),
    };
    let app = router(state);

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!("apiserver listening on {}", args.listen);

    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal())
        .await
        .context("axum::serve")?;

    info!("apiserver shutdown complete");

    Ok(())
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {info!("received SIGTERM")}
        _ = sigint.recv() => {info!("received SIGINT")}
    }
}
