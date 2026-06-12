//! 3-node Raft demo: each process runs one RaftShell over HTTP transport.
//! The leader proposes a numbered command every 2s; every node prints what it
//! APPLIES. Kill the leader → watch a survivor take over with no entry lost.
//!
//! raft-demo --id 1 --listen 127.0.0.1:7001 \
//!     --peers "2=http://127.0.0.1:7002,3=http://127.0.0.1:7003" \
//!     --db /tmp/raft-demo-1

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use clap::Parser;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use my_k8s::raft::{
    log::NodeId, message::Message, node::RaftShell, storage::RaftStorage,
    transport::HttpTransport,
};

#[derive(Debug, Parser)]
#[command(name = "raft-demo", version)]
struct Args {
    #[arg(long)]
    id: NodeId,
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: std::net::SocketAddr,
    /// Comma-separated peer map: "2=http://127.0.0.1:7002,3=http://..."
    #[arg(long, default_value = "")]
    peers: String,
    #[arg(long)]
    db: std::path::PathBuf,
}

fn parse_peers(s: &str) -> Result<HashMap<NodeId, String>> {
    let mut map = HashMap::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (id, url) = part
            .split_once('=')
            .with_context(|| format!("bad peer entry {part:?} (want id=url)"))?;
        map.insert(id.parse()?, url.to_string());
    }
    Ok(map)
}

#[derive(Clone)]
struct AppState {
    inbox_tx: mpsc::Sender<(NodeId, Message)>,
    leader_rx: watch::Receiver<Option<NodeId>>,
    applied: Arc<AtomicU64>,
    id: NodeId,
}

#[derive(Deserialize)]
struct FromParam {
    from: NodeId,
}

/// Peers POST Raft messages here; we shove them into the shell's inbox.
/// try_send: a full inbox drops the message — Raft retries anyway.
async fn raft_message(
    State(state): State<AppState>,
    Query(q): Query<FromParam>,
    Json(msg): Json<Message>,
) -> StatusCode {
    let _ = state.inbox_tx.try_send((q.from, msg));
    StatusCode::OK
}

/// For the e2e: who do you think leads, and how much have you applied?
async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "id": state.id,
        "leader": *state.leader_rx.borrow(),
        "applied": state.applied.load(Ordering::Relaxed),
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let peers = parse_peers(&args.peers)?;
    let peer_ids: Vec<NodeId> = peers.keys().copied().collect();
    info!(?args, "raft-demo starting");

    let db = sled::open(&args.db).context("open raft db")?;
    let storage = RaftStorage::open(&db)?;

    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    let (prop_tx, prop_rx) = mpsc::channel(64);
    let (apply_tx, mut apply_rx) = mpsc::channel(1024);
    let (leader_tx, leader_rx) = watch::channel(None);

    // Seed the shell's timeout RNG from wall clock (| 1: xorshift hates 0).
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .subsec_nanos() as u64
        | 1;
    let shell = RaftShell::new(
        args.id,
        peer_ids,
        storage,
        HttpTransport::new(args.id, peers),
        inbox_rx,
        prop_rx,
        apply_tx,
        leader_tx,
        seed,
    )?;
    let cancel = CancellationToken::new();
    tokio::spawn(shell.run(cancel.clone()));

    // Apply consumer: the "state machine" — here, a print + a counter.
    let applied = Arc::new(AtomicU64::new(0));
    let applied_clone = applied.clone();
    tokio::spawn(async move {
        while let Some(entry) = apply_rx.recv().await {
            applied_clone.fetch_add(1, Ordering::Relaxed);
            println!(
                "APPLIED index={} term={} cmd={}",
                entry.index,
                entry.term,
                String::from_utf8_lossy(&entry.command),
            );
        }
    });

    // Proposer: runs on EVERY node, fires only while WE lead — so leadership
    // moving automatically moves the proposal stream, no orchestration.
    let my_id = args.id;
    let leader_rx_prop = leader_rx.clone();
    tokio::spawn(async move {
        let mut n = 0u64;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tick.tick().await;
            if *leader_rx_prop.borrow() == Some(my_id) {
                n += 1;
                let _ = prop_tx
                    .send(format!("from-{my_id}-#{n}").into_bytes())
                    .await;
            }
        }
    });

    let app = Router::new()
        .route("/raft/message", post(raft_message))
        .route("/status", get(status))
        .with_state(AppState {
            inbox_tx,
            leader_rx,
            applied,
            id: args.id,
        });
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    info!("raft-demo node {} listening on {}", args.id, args.listen);
    axum::serve(listener, app).await?;
    Ok(())
}
