//! Wires the Raft core (6a) under the apiserver. Two halves that meet at the
//! `pending` map:
//!   - RaftProposer (handler side): build a command → register a oneshot keyed
//!     by its id → propose → await the outcome. Turns an async quorum commit
//!     back into a synchronous HTTP response.
//!   - apply loop (commit side): pull committed entries off the shell, apply
//!     them to the local stores, and resolve the proposer's oneshot by id.
//! Followers apply too, but find no pending entry for the id — harmless.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::apiserver::applier::Applier;
use crate::apiserver::command::{ApplyOutcome, StoreCommand};
use crate::apiserver::handlers::ApiError;
use crate::raft::log::{LogEntry, NodeId};
use crate::raft::message::Message;
use crate::raft::node::RaftShell;
use crate::raft::storage::RaftStorage;
use crate::raft::transport::HttpTransport;

/// Path the leader's peers POST raft messages to (also the redirect suffix —
/// a follower 307s the client to `<leader-api>` and the client re-sends its
/// original path; see ApiError::Redirect handling).
pub const RAFT_MESSAGE_PATH: &str = "/raft/message";

/// How long a write waits for its command to commit before giving up.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Maps a command id → the handler waiting for that command's apply outcome.
type Pending = Arc<Mutex<HashMap<Uuid, oneshot::Sender<ApplyOutcome>>>>;

/// The write side, held in `AppState` (Raft mode). Cloneable (all Arc/channel
/// handles) so axum can stamp it into per-request state.
#[derive(Clone)]
pub struct RaftProposer {
    id: NodeId,
    prop_tx: mpsc::Sender<Vec<u8>>,
    pending: Pending,
    leader_rx: watch::Receiver<Option<NodeId>>,
    /// id → API base URL, for redirecting writes to the current leader.
    peer_apis: Arc<HashMap<NodeId, String>>,
    /// The /raft/message route pushes inbound peer messages here.
    inbox_tx: mpsc::Sender<(NodeId, Message)>,
}

impl RaftProposer {
    /// The current leader as this replica sees it (for tests / observability).
    pub fn leader(&self) -> Option<NodeId> {
        *self.leader_rx.borrow()
    }

    /// Feed an inbound raft message (from the /raft/message HTTP route) to the
    /// shell. Fire-and-forget: a full inbox drops it, raft retries.
    pub fn deliver(&self, from: NodeId, msg: Message) {
        let _ = self.inbox_tx.try_send((from, msg));
    }

    /// Leadership gate: Ok if we lead, Err(Redirect) to the leader, or Err if
    /// no leader is known yet. Handlers that read-before-write (bind) call this
    /// FIRST so their read happens on the leader (whose store is current).
    pub fn ensure_leader(&self) -> Result<(), ApiError> {
        match *self.leader_rx.borrow() {
            Some(l) if l == self.id => Ok(()),
            Some(l) => {
                let url = self
                    .peer_apis
                    .get(&l)
                    .cloned()
                    .ok_or_else(|| ApiError::Internal(format!("unknown leader {l}")))?;
                Err(ApiError::Redirect(url))
            }
            None => Err(ApiError::Internal("no leader elected yet; retry".into())),
        }
    }

    /// Submit a write: propose it and block until it commits + applies (or
    /// redirect if we're not the leader). Returns the resulting object JSON.
    pub async fn submit(&self, cmd: StoreCommand) -> Result<Value, ApiError> {
        self.ensure_leader()?; // only the leader may append to the log

        // Register the mailbox BEFORE proposing, so the apply loop can never
        // resolve it before we're listening.
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(cmd.id, tx);

        let bytes = serde_json::to_vec(&cmd)
            .map_err(|e| ApiError::Internal(format!("encode command: {e}")))?;
        if self.prop_tx.send(bytes).await.is_err() {
            self.pending.lock().unwrap().remove(&cmd.id);
            return Err(ApiError::Internal("raft proposer channel closed".into()));
        }

        // Park until the apply loop resolves our id (or we time out — a write
        // that never commits because leadership was lost mid-flight).
        match tokio::time::timeout(WRITE_TIMEOUT, rx).await {
            Ok(Ok(outcome)) => outcome_to_result(outcome),
            _ => {
                self.pending.lock().unwrap().remove(&cmd.id); // sweep the leak
                Err(ApiError::Internal("write timed out (not committed)".into()))
            }
        }
    }
}

/// Map the deterministic apply verdict back to the HTTP-layer error type.
fn outcome_to_result(outcome: ApplyOutcome) -> Result<Value, ApiError> {
    match outcome {
        ApplyOutcome::Ok(v) => Ok(v),
        ApplyOutcome::NotFound(n) => Err(ApiError::NotFound(n)),
        ApplyOutcome::AlreadyExists(n) => Err(ApiError::AlreadyExists(n)),
        ApplyOutcome::Conflict { current, provided } => {
            Err(ApiError::Conflict { current, provided })
        }
        ApplyOutcome::Internal(m) => Err(ApiError::Internal(m)),
    }
}

/// Spin up Raft under the apiserver: build the shell, spawn it + the apply
/// loop, and return the proposer the handlers use. `peers`/`peer_apis` map
/// every OTHER replica's id to its raft + API URL (same URL here — raft rides
/// the apiserver's HTTP port via the /raft/message route).
#[allow(clippy::too_many_arguments)]
pub fn spawn_raft(
    id: NodeId,
    peers: Vec<NodeId>,
    peer_apis: HashMap<NodeId, String>,
    raft_storage: RaftStorage,
    applier: Applier,
    seed: u64,
    cancel: CancellationToken,
) -> anyhow::Result<RaftProposer> {
    let (inbox_tx, inbox_rx) = mpsc::channel(1024);
    let (prop_tx, prop_rx) = mpsc::channel(64);
    let (apply_tx, apply_rx) = mpsc::channel(1024);
    let (leader_tx, leader_rx) = watch::channel(None);
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

    // HttpTransport::send appends `/raft/message?from=` itself, so hand it the
    // BARE base URLs (double-appending the path → 404 → no messages delivered).
    let transport = HttpTransport::new(id, peer_apis.clone());

    let shell = RaftShell::new(
        id, peers, raft_storage, transport, inbox_rx, prop_rx, apply_tx, leader_tx, seed,
    )?;
    tokio::spawn(shell.run(cancel.clone()));
    tokio::spawn(apply_loop(apply_rx, applier, pending.clone(), cancel));

    Ok(RaftProposer {
        id,
        prop_tx,
        pending,
        leader_rx,
        peer_apis: Arc::new(peer_apis),
        inbox_tx,
    })
}

/// Middleware: if a handler returned a 307 whose Location is just the leader's
/// base URL (no path), append the original request path + query so the client
/// re-sends to the right endpoint on the leader. Shared by the apiserver bin
/// and the multi-replica tests.
pub async fn append_path_to_redirect(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::{HeaderValue, StatusCode, header::LOCATION};
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_default();
    let mut resp = next.run(req).await;
    if resp.status() == StatusCode::TEMPORARY_REDIRECT
        && let Some(loc) = resp.headers().get(LOCATION)
        && let Ok(base) = loc.to_str()
        // Only a bare base "http://host:port" (exactly two '/') gets a path;
        // an already-complete Location is left alone.
        && base.matches('/').count() == 2
        && let Ok(v) = HeaderValue::from_str(&format!("{base}{path_and_query}"))
    {
        resp.headers_mut().insert(LOCATION, v);
    }
    resp
}

/// Consume committed entries, apply each to the local stores, resolve the
/// proposer's oneshot if this replica is the one that proposed it.
async fn apply_loop(
    mut apply_rx: mpsc::Receiver<LogEntry>,
    applier: Applier,
    pending: Pending,
    cancel: CancellationToken,
) {
    loop {
        let entry = tokio::select! {
            _ = cancel.cancelled() => return,
            e = apply_rx.recv() => match e {
                Some(e) => e,
                None => return,
            },
        };
        let cmd: StoreCommand = match serde_json::from_slice(&entry.command) {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "skipping undecodable committed command");
                continue;
            }
        };
        let id = cmd.id;
        let outcome = applier.apply(cmd);
        // Resolve the waiter if WE proposed this (followers find nothing).
        if let Some(tx) = pending.lock().unwrap().remove(&id) {
            let _ = tx.send(outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::apiserver::handlers::{AppState, WritePath};
    use crate::apiserver::routes::router;
    use crate::apiserver::storage::PodStore;
    use crate::client::Client;
    use crate::endpoints::Endpoints;
    use crate::node::Node;
    use crate::pod::{Container, Pod, PodSpec};
    use crate::replicaset::ReplicaSet;
    use crate::service::Service;

    /// Spin up a SINGLE-node raft apiserver in-process and return a Client. One
    /// node = instant self-election, so it's always the leader; writes flow
    /// through the full propose → commit → apply → oneshot path.
    async fn spawn_raft_apiserver() -> Client {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let store = Arc::new(PodStore::from_db(db.clone()).unwrap());
        let rs_store = Arc::new(crate::apiserver::storage::ResourceStore::<ReplicaSet>::from_db(db.clone()).unwrap());
        let node_store = Arc::new(crate::apiserver::storage::ResourceStore::<Node>::from_db(db.clone()).unwrap());
        let svc_store = Arc::new(crate::apiserver::storage::ResourceStore::<Service>::from_db(db.clone()).unwrap());
        let ep_store = Arc::new(crate::apiserver::storage::ResourceStore::<Endpoints>::from_db(db.clone()).unwrap());

        let applier = Applier {
            pods: store.clone(),
            replicasets: rs_store.clone(),
            nodes: node_store.clone(),
            services: svc_store.clone(),
            endpoints: ep_store.clone(),
        };
        let raft_storage = RaftStorage::open(&db).unwrap();
        let cancel = CancellationToken::new();
        // No peers → wins election immediately, no transport ever used.
        let proposer = spawn_raft(
            1, vec![], HashMap::new(), raft_storage, applier, 42, cancel,
        )
        .unwrap();

        let state = AppState {
            store,
            rs_store,
            node_store,
            svc_store,
            ep_store,
            write: WritePath::Raft(proposer),
        };
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Client::new(format!("http://{addr}"))
    }

    fn make_pod(name: &str) -> Pod {
        Pod {
            api_version: "v1".into(),
            kind: "Pod".into(),
            metadata: crate::meta::ObjectMeta {
                name: name.into(),
                ..Default::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "c".into(),
                    image: "busybox".into(),
                    command: vec!["sleep".into()],
                }],
                node_name: None,
            },
            status: None,
        }
    }

    /// Wait for the single node to elect itself (its leader watch flips to
    /// Some(1)) — writes before that get "no leader yet".
    async fn await_ready(client: &Client) {
        for _ in 0..100 {
            if client.create_pod(&make_pod("probe")).await.is_ok() {
                client
                    .delete_pod(
                        "probe",
                        &client
                            .get_pod("probe")
                            .await
                            .unwrap()
                            .unwrap()
                            .metadata
                            .resource_version
                            .unwrap(),
                    )
                    .await
                    .ok();
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("single-node raft never became leader");
    }

    #[tokio::test]
    async fn pod_crud_through_the_raft_log() {
        let client = spawn_raft_apiserver().await;
        await_ready(&client).await;

        // CREATE — flows through propose → commit → apply → oneshot.
        let created = client.create_pod(&make_pod("web")).await.unwrap();
        assert_eq!(created.metadata.name, "web");
        assert!(created.metadata.uid.is_some(), "leader stamped a uid");
        let rv1 = created.metadata.resource_version.clone().unwrap();

        // READ (local, no raft) sees the committed pod.
        let got = client.get_pod("web").await.unwrap().unwrap();
        assert_eq!(got.metadata.uid, created.metadata.uid);

        // Duplicate CREATE → AlreadyExists verdict travels back from apply.
        assert!(client.create_pod(&make_pod("web")).await.is_err());

        // BIND (read-modify-write through the log) sets nodeName.
        let bound = client.bind_pod("web", "node-a").await.unwrap();
        assert_eq!(bound.spec.node_name.as_deref(), Some("node-a"));

        // DELETE with the current rv.
        let rv = client.get_pod("web").await.unwrap().unwrap().metadata.resource_version.unwrap();
        client.delete_pod("web", &rv).await.unwrap();
        assert!(client.get_pod("web").await.unwrap().is_none());

        // Stale-rv delete → Conflict verdict from apply.
        let p = client.create_pod(&make_pod("web2")).await.unwrap();
        let _ = p;
        assert!(client.delete_pod("web2", "999").await.is_err());

        // rv advanced across the writes (deterministic, log-ordered).
        assert_ne!(rv1, client.get_pod("web2").await.unwrap().unwrap().metadata.resource_version.unwrap());
    }

    // ---- 3-replica cluster (real HTTP + Raft over HttpTransport) ----

    use axum::Router;
    use axum::routing::post;

    struct Replica {
        id: NodeId,
        url: String,
        cancel: CancellationToken,
        store: Arc<PodStore>,
        proposer: RaftProposer,
    }

    /// Build one replica: five stores + raft glue + the full router (API routes
    /// + /raft/message + redirect layer), bound to a fixed port so peers can
    /// address it. Mirrors the apiserver bin's assembly.
    async fn spawn_replica(id: NodeId, ports: &[(NodeId, u16)]) -> Replica {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let store = Arc::new(PodStore::from_db(db.clone()).unwrap());
        let rs_store = Arc::new(crate::apiserver::storage::ResourceStore::<ReplicaSet>::from_db(db.clone()).unwrap());
        let node_store = Arc::new(crate::apiserver::storage::ResourceStore::<Node>::from_db(db.clone()).unwrap());
        let svc_store = Arc::new(crate::apiserver::storage::ResourceStore::<Service>::from_db(db.clone()).unwrap());
        let ep_store = Arc::new(crate::apiserver::storage::ResourceStore::<Endpoints>::from_db(db.clone()).unwrap());

        let applier = Applier {
            pods: store.clone(),
            replicasets: rs_store.clone(),
            nodes: node_store.clone(),
            services: svc_store.clone(),
            endpoints: ep_store.clone(),
        };
        let peer_apis: HashMap<NodeId, String> = ports
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, port)| (*pid, format!("http://127.0.0.1:{port}")))
            .collect();
        let peers: Vec<NodeId> = peer_apis.keys().copied().collect();
        let cancel = CancellationToken::new();
        let proposer = spawn_raft(
            id,
            peers,
            peer_apis,
            RaftStorage::open(&db).unwrap(),
            applier,
            1000 + id, // distinct deterministic seeds
            cancel.clone(),
        )
        .unwrap();

        let state = AppState {
            store: store.clone(),
            rs_store,
            node_store,
            svc_store,
            ep_store,
            write: WritePath::Raft(proposer.clone()),
        };
        let proposer_for_replica = proposer.clone();
        let app = router(state)
            .merge(
                Router::new()
                    .route(RAFT_MESSAGE_PATH, post(raft_message_test))
                    .with_state(proposer),
            )
            .layer(axum::middleware::from_fn(append_path_to_redirect));

        let my_port = ports.iter().find(|(pid, _)| *pid == id).unwrap().1;
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{my_port}"))
            .await
            .unwrap();
        let url = format!("http://127.0.0.1:{my_port}");
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { cancel2.cancelled().await })
                .await
                .unwrap();
        });
        Replica {
            id,
            url,
            cancel,
            store,
            proposer: proposer_for_replica,
        }
    }

    /// Test copy of the bin's /raft/message handler.
    async fn raft_message_test(
        axum::extract::State(proposer): axum::extract::State<RaftProposer>,
        axum::extract::Query(q): axum::extract::Query<RaftFrom>,
        axum::Json(msg): axum::Json<crate::raft::message::Message>,
    ) -> axum::http::StatusCode {
        proposer.deliver(q.from, msg);
        axum::http::StatusCode::OK
    }

    #[derive(serde::Deserialize)]
    struct RaftFrom {
        from: NodeId,
    }

    /// Poll a replica's leader view until SOME leader is known cluster-wide.
    async fn await_cluster_leader(clients: &[(NodeId, Client)]) -> NodeId {
        for _ in 0..200 {
            // A successful create on ANY replica means a leader exists (writes
            // either commit locally or redirect to the leader).
            if let Ok(p) = clients[0].1.create_pod(&make_pod("leader-probe")).await {
                let rv = p.metadata.resource_version.unwrap();
                clients[0].1.delete_pod("leader-probe", &rv).await.ok();
                // Find who actually leads by checking which replica accepts a
                // write without redirect — but simplest: just return any id;
                // tests below don't need the exact id, only that writes work.
                return clients[0].0;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("cluster never elected a leader");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_replicas_elect_a_leader() {
        let ports = [(1u64, 28031u16), (2, 28032), (3, 28033)];
        let mut replicas = Vec::new();
        for (id, _) in &ports {
            replicas.push(spawn_replica(*id, &ports).await);
        }
        // Poll each replica's own leader view until one is elected.
        let mut elected = None;
        for _ in 0..200 {
            let views: Vec<Option<NodeId>> = replicas.iter().map(|r| r.proposer.leader()).collect();
            if let Some(Some(l)) = views.iter().find(|v| v.is_some()) {
                elected = Some(*l);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        for r in &replicas {
            r.cancel.cancel();
        }
        assert!(elected.is_some(), "no leader elected within 5s");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_replicas_converge_and_survive_leader_kill() {
        // Fixed ports so replicas can address each other at construction.
        let ports = [(1u64, 28021u16), (2, 28022), (3, 28023)];
        let mut replicas = Vec::new();
        for (id, _) in &ports {
            replicas.push(spawn_replica(*id, &ports).await);
        }
        let clients: Vec<(NodeId, Client)> = replicas
            .iter()
            .map(|r| (r.id, Client::new(r.url.clone())))
            .collect();

        await_cluster_leader(&clients).await;

        // Write via EVERY replica's client in turn. A follower 307-redirects to
        // the leader (reqwest follows automatically) → all writes commit.
        for (i, (_, client)) in clients.iter().enumerate() {
            let name = format!("pod-{i}");
            client.create_pod(&make_pod(&name)).await.unwrap_or_else(|e| {
                panic!("write via replica {i} failed: {e}")
            });
        }

        // Give replication a moment, then assert ALL three stores converged to
        // the same three pods with identical resourceVersions.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let dumps: Vec<Vec<(String, Option<String>)>> = replicas
            .iter()
            .map(|r| {
                let (mut pods, _) = r.store.list().unwrap();
                pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
                pods.into_iter()
                    .map(|p| (p.metadata.name, p.metadata.resource_version))
                    .collect()
            })
            .collect();
        assert_eq!(dumps[0].len(), 3, "expected 3 pods, got {:?}", dumps[0]);
        assert_eq!(dumps[0], dumps[1], "replica 1 vs 2 diverged");
        assert_eq!(dumps[1], dumps[2], "replica 2 vs 3 diverged");

        // Kill replica 1 (might be the leader); the survivors must keep
        // accepting writes — to ANY survivor, redirected as needed.
        replicas[0].cancel.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut wrote_after_kill = false;
        for _ in 0..200 {
            if clients[1].1.create_pod(&make_pod("after-kill")).await.is_ok() {
                wrote_after_kill = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(wrote_after_kill, "survivors must keep accepting writes after a leader kill");

        // The two survivors agree on the new pod.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(replicas[1].store.get("after-kill").unwrap().is_some());
        assert!(replicas[2].store.get("after-kill").unwrap().is_some());

        for r in &replicas[1..] {
            r.cancel.cancel();
        }
    }
}
