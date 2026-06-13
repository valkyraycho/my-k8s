use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::post,
};
use clap::Parser;
use my_k8s::{
    apiserver::{
        applier::Applier,
        handlers::{AppState, WritePath},
        raft_glue::{RAFT_MESSAGE_PATH, RaftProposer, spawn_raft},
        routes::router,
        storage::{PodStore, ResourceStore, open_db},
    },
    endpoints::Endpoints,
    node::Node,
    raft::{log::NodeId, message::Message, storage::RaftStorage},
    replicaset::ReplicaSet,
    service::Service,
};
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "apiserver", version)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// sled DB dir — the persistent state (resources + rv + the raft log).
    #[arg(long, default_value = "/var/lib/my-k8s/etcd-like")]
    db: PathBuf,

    /// This replica's raft node id. Presence of `--raft-id` switches on Raft
    /// mode; omitting it keeps the single-node standalone behavior (default).
    #[arg(long)]
    raft_id: Option<NodeId>,

    /// Other replicas: "2=http://127.0.0.1:8081,3=http://127.0.0.1:8082".
    /// Each URL is both the raft transport target and the write-redirect target.
    #[arg(long, default_value = "")]
    peers: String,
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

    // The five resource stores (shared by reads always, and by Direct writes).
    let store = Arc::new(PodStore::from_db(db.clone())?);
    let rs_store = Arc::new(ResourceStore::<ReplicaSet>::from_db(db.clone())?);
    let node_store = Arc::new(ResourceStore::<Node>::from_db(db.clone())?);
    let svc_store = Arc::new(ResourceStore::<Service>::from_db(db.clone())?);
    let ep_store = Arc::new(ResourceStore::<Endpoints>::from_db(db.clone())?);

    let cancel = CancellationToken::new();

    // Raft mode iff --raft-id was given. Build the proposer + apply loop over
    // the SAME stores the read handlers serve from.
    let (write, raft_proposer) = match args.raft_id {
        Some(id) => {
            let peer_apis = parse_peers(&args.peers)?;
            let peers: Vec<NodeId> = peer_apis.keys().copied().collect();
            let raft_storage = RaftStorage::open(&db)?;
            let applier = Applier {
                pods: store.clone(),
                replicasets: rs_store.clone(),
                nodes: node_store.clone(),
                services: svc_store.clone(),
                endpoints: ep_store.clone(),
            };
            // Seed mixes id + wall clock so replicas pick different timeouts.
            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .subsec_nanos() as u64
                | 1;
            let proposer = spawn_raft(
                id, peers, peer_apis, raft_storage, applier, seed, cancel.clone(),
            )?;
            info!(raft_id = id, "raft mode enabled");
            (WritePath::Raft(proposer.clone()), Some(proposer))
        }
        None => (WritePath::Direct, None),
    };

    let state = AppState {
        store,
        rs_store,
        node_store,
        svc_store,
        ep_store,
        write,
    };

    // Base API router (AppState). In raft mode, merge a tiny second router that
    // serves /raft/message with its OWN state (the proposer) — `merge` composes
    // routers whose states are already applied.
    let mut app = router(state);
    if let Some(proposer) = raft_proposer {
        let raft_router = Router::new()
            .route(RAFT_MESSAGE_PATH, post(raft_message))
            .with_state(proposer);
        // The redirect layer appends the request path to a bare-base Location
        // (a follower's 307 carries just the leader's host; the client must hit
        // the SAME path there).
        app = app.merge(raft_router).layer(axum::middleware::from_fn(
            my_k8s::apiserver::raft_glue::append_path_to_redirect,
        ));
    }

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

#[derive(Deserialize)]
struct FromParam {
    from: NodeId,
}

/// Peers POST raft messages here; hand them to the shell's inbox.
async fn raft_message(
    State(proposer): State<RaftProposer>,
    Query(q): Query<FromParam>,
    Json(msg): Json<Message>,
) -> StatusCode {
    proposer.deliver(q.from, msg);
    StatusCode::OK
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => {info!("received SIGTERM")}
        _ = sigint.recv() => {info!("received SIGINT")}
    }
}
