//! my-k8s controller-manager — runs the ReplicaSet controller against the
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use my_k8s::{
    client::Client,
    controller::{endpoints_manager, manager},
};

#[derive(Debug, Parser)]
#[command(name = "controller-manager", version)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    api_server_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(?args, "controller-manager starting");

    let client = Arc::new(Client::new(args.api_server_url));
    let cancel = CancellationToken::new();
    let run = tokio::spawn(manager::run(client.clone(), cancel.clone()));
    let ep_run = tokio::spawn(endpoints_manager::run(client.clone(), cancel.clone()));

    wait_for_shutdown_signal().await;
    info!("shutdown signal received; cancelling");
    cancel.cancel();
    let _ = run.await;
    let _ = ep_run.await;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM"),
        _ = sigint.recv() => info!("received SIGINT"),
    }
}
