use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use my_k8s::{
    apiserver::{
        handlers::AppState,
        routes::router,
        storage::{PodStore, ResourceStore, open_db},
    },
    replicaset::ReplicaSet,
};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// clap's derive API: `#[derive(Parser)]` turns this struct into a CLI parser,
/// each field an `--flag`. `default_value` is parsed via the field's `FromStr`,
/// so `--listen` validates as a real `SocketAddr` at parse time, not later.
#[derive(Debug, Parser)]
#[command(name = "apiserver", version)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// sled DB dir — the persistent state. Runs NON-root, so this path under
    /// root-owned /var/lib/my-k8s must be pre-created + chowned to the run user.
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

    let db = open_db(&args.db).with_context(|| format!("opening sled DB at {:?}", args.db))?;

    let state = AppState {
        store: Arc::new(PodStore::from_db(db.clone())?),
        rs_store: Arc::new(ResourceStore::<ReplicaSet>::from_db(db)?),
    };
    let app = router(state);

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!("apiserver listening on {}", args.listen);

    // `with_graceful_shutdown(future)`: stop accepting new connections and drain
    // in-flight ones once the future resolves. KNOWN LIMITATION: a watch is an
    // infinite response that never drains, so this hangs while a kubelet is
    // watching — needs SIGKILL. Real K8s sends a stream-close frame; we don't.
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
