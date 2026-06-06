use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use my_k8s::{client::Client, reconciler::Reconciler, runtime::youki::YoukiRuntime};

/// my-k8s kubelet — syncs Pods from the apiserver and runs them on this node.
#[derive(Debug, Parser)]
#[command(name = "kubelet", version)]
struct Args {
    /// Base URL of the apiserver (the source of desired state).
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    api_server_url: String,

    #[arg(long, env = "NODE_NAME", default_value = "node-0")]
    node_name: String,

    /// Where libcontainer keeps per-container state (analogous to runc's --root).
    /// Created on startup if missing.
    #[arg(long, default_value = "/var/lib/my-k8s/state")]
    state_dir: PathBuf,

    /// Read-only base rootfs shared by every container.                                                    
    /// MUST exist; prepared once via `scripts/prepare-rootfs.sh`.
    #[arg(long, default_value = "/var/lib/my-k8s/rootfs-base")]
    rootfs_base: PathBuf,
}

impl Args {
    fn validate_and_prepare(&self) -> Result<()> {
        ensure_dir(&self.state_dir).context("preparing state dir")?;
        ensure_dir(&self.pods_dir()).context("preparing pods dir")?;
        if !self.rootfs_base.is_dir() {
            bail!(
                "rootfs-base {:?} does not exist or is not a directory.\n\
                   Hint: run `sudo bash scripts/prepare-rootfs.sh` to create it.",
                self.rootfs_base,
            )
        }
        Ok(())
    }

    fn pods_dir(&self) -> PathBuf {
        self.state_dir.join("pods")
    }
}

fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("creating directory {path:?}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    args.validate_and_prepare()?;
    info!(?args, "kubelet starting");

    let runtime = YoukiRuntime::new(args.state_dir.clone());
    // `Arc<Client>` so the reconciler can share one client across its watch
    // stream + status pushes. Since Phase 2 the kubelet is a pure apiserver
    // client — no local manifests dir anymore.
    let client = Arc::new(Client::new(args.api_server_url.clone()));
    let reconciler = Reconciler::new(
        client,
        args.node_name.clone(),
        args.pods_dir(),
        args.rootfs_base.clone(),
        runtime,
        Some(args.state_dir.join("debug.json")),
    );

    // Cooperative shutdown: a shared token both halves observe. `select!` races
    // "signal arrived" against "reconciler exited on its own". `&mut task` polls
    // the JoinHandle WITHOUT consuming it, so we can still await it again below.
    let cancel = CancellationToken::new();
    let mut reconciler_task = tokio::spawn(reconciler.run(cancel.clone()));

    let received_signal = tokio::select! {
        _ = wait_for_shutdown_signal() => true,
        res = &mut reconciler_task => {
            log_reconciler_exit("reconciler exited unexpectedly", res);
            false
        }
    };

    // On signal: flip the token, then AWAIT the task so its sandbox teardown
    // fully completes before main returns (graceful, not abrupt).
    if received_signal {
        info!("shutdown signal received; draining reconciler");
        cancel.cancel();
        let res = (&mut reconciler_task).await;
        log_reconciler_exit("reconciler", res);
    }

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

fn log_reconciler_exit(
    context: &str,
    res: std::result::Result<Result<()>, tokio::task::JoinError>,
) {
    match res {
        Ok(Ok(())) => info!("{context}: clean"),
        Ok(Err(e)) => warn!(error = ?e, "{context}: error"),
        Err(e) => warn!(error = ?e, "{context}: task panicked or was cancelled"),
    }
}
