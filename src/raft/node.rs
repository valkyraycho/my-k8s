//! The shell: drives the pure core with real time, real disk, real network.
//! All policy lives in core::step(); this file only executes effects — and
//! the ONE rule it must honor is effect ORDER (persist before send).
//!

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::raft::{
    core::{Effect, Event, RaftNode, Role},
    log::{LogEntry, NodeId, RaftLog},
    message::Message,
    storage::{HardState, RaftStorage},
    transport::Transport,
};
const TICK_INTERVAL: Duration = Duration::from_millis(50);
const ELECTION_TICK_MIN: u32 = 10;
const ELECTION_TICK_JITTER: u64 = 10;
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0.max(1); // xorshift must never be 0
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

pub struct RaftShell<T: Transport> {
    node: RaftNode,
    storage: RaftStorage,
    transport: T,
    inbox: mpsc::Receiver<(NodeId, Message)>,
    proposals: mpsc::Receiver<Vec<u8>>,
    apply_tx: mpsc::Sender<LogEntry>,
    leader_watch: watch::Sender<Option<NodeId>>,
    rng: XorShift,
}

impl<T: Transport> RaftShell<T> {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: RaftStorage,
        transport: T,
        inbox: mpsc::Receiver<(NodeId, Message)>,
        proposals: mpsc::Receiver<Vec<u8>>,
        apply_tx: mpsc::Sender<LogEntry>,
        leader_watch: watch::Sender<Option<NodeId>>,
        seed: u64,
    ) -> anyhow::Result<Self> {
        // Mix id with a golden-ratio multiplier, NOT XOR: `seed ^ id` can
        // CANCEL (e.g. seed=1000+id ^ id collapses to 1000 for every id),
        // giving every node the same election timeout → permanent split vote.
        let mut rng = XorShift(seed.wrapping_add(id.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        let timeout = ELECTION_TICK_MIN + (rng.next() % ELECTION_TICK_JITTER) as u32;
        let mut node = RaftNode::new(id, peers, timeout);

        let hs = storage.load_hard_state()?;
        node.current_term = hs.current_term;
        node.voted_for = hs.voted_for;
        node.log = RaftLog::from_entries(storage.load_log()?);
        info!(
            id,
            term = node.current_term,
            last = node.log.last_index(),
            "raft node recovered"
        );

        Ok(Self {
            node,
            storage,
            transport,
            inbox,
            proposals,
            apply_tx,
            leader_watch,
            rng,
        })
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(TICK_INTERVAL);
        loop {
            let event = tokio::select! {
               biased;
               _ = cancel.cancelled() => break,
               _ = tick.tick() => Event::Tick,
               Some((from, msg)) = self.inbox.recv() => Event::Message(from, msg),
               Some(proposal) = self.proposals.recv() => Event::Propose(proposal),
            };

            let was_candidate = matches!(self.node.role, Role::Candidate { .. });
            let effects = self.node.step(event);

            if !was_candidate && matches!(self.node.role, Role::Candidate { .. }) {
                let t = ELECTION_TICK_MIN + (self.rng.next() % ELECTION_TICK_JITTER) as u32;
                self.node.set_election_timeout(t);
            }
            self.execute(effects).await;

            let lead = self.node.leader_hint;
            self.leader_watch.send_if_modified(|cur| {
                if *cur != lead {
                    *cur = lead;
                    true
                } else {
                    false
                }
            });
        }
    }

    async fn execute(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Persist => {
                    let hs = HardState {
                        current_term: self.node.current_term,
                        voted_for: self.node.voted_for,
                    };
                    if let Err(e) = self.storage.save_hard_state(&hs) {
                        warn!(error = ?e, "persist hard state failed")
                    }
                }
                Effect::PersistEntries(entries) => {
                    if let Err(e) = self.storage.append_entries(&entries) {
                        warn!(error = ?e, "persist entries failed");
                    }
                }
                Effect::PersistTruncate(from) => {
                    if let Err(e) = self.storage.truncate_from(from) {
                        warn!(error = ?e, "truncate failed");
                    }
                }
                Effect::Send(to, msg) => self.transport.send(to, msg),
                Effect::Apply(entry) => {
                    if self.apply_tx.send(entry).await.is_err() {
                        warn!("apply channel closed; dropping committed entry");
                    }
                }
                Effect::ProposeRejected { leader_hint } => {
                    warn!(?leader_hint, "proposal rejected (not leader)");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, timeout};

    /// In-memory transport: routes messages straight into peers' inbox
    /// channels. `try_send` = lossy-on-full, faithful to the trait's contract.
    #[derive(Clone)]
    struct InMemTransport {
        self_id: NodeId,
        routes: Arc<Mutex<HashMap<NodeId, mpsc::Sender<(NodeId, Message)>>>>,
    }

    impl Transport for InMemTransport {
        fn send(&self, to: NodeId, msg: Message) {
            if let Some(tx) = self.routes.lock().unwrap().get(&to) {
                let _ = tx.try_send((self.self_id, msg));
            }
        }
    }

    struct TestNode {
        proposals: mpsc::Sender<Vec<u8>>,
        apply_rx: mpsc::Receiver<LogEntry>,
        leader_rx: watch::Receiver<Option<NodeId>>,
        cancel: CancellationToken,
    }

    /// Spawn an n-node cluster of real shells over the in-memory transport.
    fn spawn_cluster(n: u64) -> HashMap<NodeId, TestNode> {
        let routes = Arc::new(Mutex::new(HashMap::new()));
        let ids: Vec<NodeId> = (1..=n).collect();
        let mut handles = HashMap::new();

        for &id in &ids {
            let (inbox_tx, inbox_rx) = mpsc::channel(1024);
            let (prop_tx, prop_rx) = mpsc::channel(64);
            let (apply_tx, apply_rx) = mpsc::channel(1024);
            let (leader_tx, leader_rx) = watch::channel(None);
            routes.lock().unwrap().insert(id, inbox_tx);

            let db = sled::Config::default().temporary(true).open().unwrap();
            let storage = RaftStorage::open(&db).unwrap();
            // Keep the db handle alive for the test's duration.
            Box::leak(Box::new(db));

            let transport = InMemTransport {
                self_id: id,
                routes: routes.clone(),
            };
            let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
            let shell = RaftShell::new(
                id, peers, storage, transport, inbox_rx, prop_rx, apply_tx, leader_tx,
                0xC0FFEE + id, // distinct deterministic seeds
            )
            .unwrap();
            let cancel = CancellationToken::new();
            tokio::spawn(shell.run(cancel.clone()));

            handles.insert(
                id,
                TestNode {
                    proposals: prop_tx,
                    apply_rx,
                    leader_rx,
                    cancel,
                },
            );
        }
        handles
    }

    /// Poll the watch channels until every live node agrees on Some(leader).
    async fn await_leader(
        nodes: &mut HashMap<NodeId, TestNode>,
        exclude: &[NodeId],
    ) -> NodeId {
        timeout(Duration::from_secs(10), async {
            loop {
                let views: Vec<Option<NodeId>> = nodes
                    .iter()
                    .filter(|(id, _)| !exclude.contains(id))
                    .map(|(_, tn)| *tn.leader_rx.borrow())
                    .collect();
                if let Some(Some(lead)) = views.first()
                    && views.iter().all(|v| *v == Some(*lead))
                    && !exclude.contains(lead)
                {
                    return *lead;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no leader emerged in time")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn three_shells_elect_and_replicate_a_command() {
        let mut nodes = spawn_cluster(3);
        let leader = await_leader(&mut nodes, &[]).await;

        // Propose through the leader; the entry must apply on ALL THREE.
        nodes[&leader]
            .proposals
            .send(b"hello-raft".to_vec())
            .await
            .unwrap();

        for (id, tn) in nodes.iter_mut() {
            let entry = timeout(Duration::from_secs(5), tn.apply_rx.recv())
                .await
                .unwrap_or_else(|_| panic!("node {id} never applied"))
                .expect("apply channel open");
            assert_eq!(entry.command, b"hello-raft", "node {id}");
            assert_eq!(entry.index, 1, "node {id}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leader_crash_triggers_reelection_and_cluster_still_commits() {
        let mut nodes = spawn_cluster(3);
        let old = await_leader(&mut nodes, &[]).await;

        // Kill the leader's shell outright.
        nodes[&old].cancel.cancel();

        // The two survivors elect a NEW leader (majority of 3 still alive).
        let new = await_leader(&mut nodes, &[old]).await;
        assert_ne!(new, old, "a survivor must take over");

        // And the cluster still accepts + commits proposals.
        nodes[&new].proposals.send(b"after".to_vec()).await.unwrap();
        for (id, tn) in nodes.iter_mut().filter(|(id, _)| **id != old) {
            let entry = timeout(Duration::from_secs(5), tn.apply_rx.recv())
                .await
                .unwrap_or_else(|_| panic!("survivor {id} never applied"))
                .expect("apply channel open");
            assert_eq!(entry.command, b"after", "node {id}");
        }
    }
}
