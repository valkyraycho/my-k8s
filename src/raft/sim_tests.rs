//! Deterministic multi-node simulation of the Raft core. Real RaftNodes +
//! real RaftStorages, FAKE everything else: messages travel through an
//! in-memory queue with controllable partitions, crashes, and restarts.
//! The paper's safety invariants are checked after EVERY step:
//!   - Election Safety: at most one leader per term, ever
//!   - Log Matching: same (index, term) on two nodes → same command
//!   - State Machine Safety: all apply streams are prefixes of each other

use std::collections::{BTreeMap, HashSet, VecDeque};

use crate::raft::core::{Effect, Event, RaftNode, Role};
use crate::raft::log::{LogEntry, NodeId, RaftLog, Term};
use crate::raft::message::Message;
use crate::raft::storage::{HardState, RaftStorage};

struct Cluster {
    nodes: BTreeMap<NodeId, RaftNode>,
    /// Each node's persistence — survives crash/restart within a test.
    storages: BTreeMap<NodeId, (sled::Db, RaftStorage)>,
    /// In-flight messages, FIFO: (from, to, msg).
    inbox: VecDeque<(NodeId, NodeId, Message)>,
    /// Directed blocked links (partitions).
    blocked: HashSet<(NodeId, NodeId)>,
    crashed: HashSet<NodeId>,
    /// Per-node apply stream (what its state machine has consumed, in order).
    applied: BTreeMap<NodeId, Vec<LogEntry>>,
    /// Every (term → leader) ever observed — the Election Safety record.
    leaders_by_term: BTreeMap<Term, NodeId>,
}

impl Cluster {
    fn new(timeouts: &[(NodeId, u32)]) -> Self {
        let ids: Vec<NodeId> = timeouts.iter().map(|(id, _)| *id).collect();
        let mut nodes = BTreeMap::new();
        let mut storages = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for &(id, timeout) in timeouts {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
            nodes.insert(id, RaftNode::new(id, peers, timeout));
            let db = sled::Config::default().temporary(true).open().unwrap();
            let storage = RaftStorage::open(&db).unwrap();
            storages.insert(id, (db, storage));
            applied.insert(id, Vec::new());
        }
        Self {
            nodes,
            storages,
            inbox: VecDeque::new(),
            blocked: HashSet::new(),
            crashed: HashSet::new(),
            applied,
            leaders_by_term: BTreeMap::new(),
        }
    }

    /// Drive one event into a node, execute its effects (persist → storage,
    /// send → inbox, apply → stream), then check all invariants.
    fn step(&mut self, id: NodeId, event: Event) {
        if self.crashed.contains(&id) {
            return;
        }
        let effects = self.nodes.get_mut(&id).unwrap().step(event);
        for effect in effects {
            match effect {
                Effect::Send(to, msg) => self.inbox.push_back((id, to, msg)),
                Effect::Persist => {
                    let n = &self.nodes[&id];
                    let hs = HardState {
                        current_term: n.current_term,
                        voted_for: n.voted_for,
                    };
                    self.storages[&id].1.save_hard_state(&hs).unwrap();
                }
                Effect::PersistEntries(entries) => {
                    self.storages[&id].1.append_entries(&entries).unwrap()
                }
                Effect::PersistTruncate(from) => {
                    self.storages[&id].1.truncate_from(from).unwrap()
                }
                Effect::Apply(entry) => self.applied.get_mut(&id).unwrap().push(entry),
                Effect::ProposeRejected { .. } => {}
            }
        }
        self.check_invariants();
    }

    fn tick_all(&mut self, rounds: u32) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for _ in 0..rounds {
            for &id in &ids {
                self.step(id, Event::Tick);
            }
        }
    }

    /// Deliver until the network is quiet. Messages to crashed nodes or over
    /// blocked links are DROPPED at delivery time (like a dead NIC).
    fn deliver_all(&mut self) {
        let mut budget = 10_000; // backstop against accidental ping-pong loops
        while let Some((from, to, msg)) = self.inbox.pop_front() {
            budget -= 1;
            assert!(budget > 0, "message storm: delivery never quiesced");
            if self.crashed.contains(&to) || self.blocked.contains(&(from, to)) {
                continue;
            }
            self.step(to, Event::Message(from, msg));
        }
    }

    fn propose(&mut self, id: NodeId, cmd: &[u8]) {
        self.step(id, Event::Propose(cmd.to_vec()));
    }

    /// Block all links between group A and group B, both directions.
    fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        for &x in a {
            for &y in b {
                self.blocked.insert((x, y));
                self.blocked.insert((y, x));
            }
        }
    }

    fn heal(&mut self) {
        self.blocked.clear();
    }

    fn crash(&mut self, id: NodeId) {
        self.crashed.insert(id);
    }

    /// Restart from persistence — the recovery path: hard state + log reload
    /// from the node's OWN storage; volatile state (commit_index, role) starts
    /// fresh. The apply stream resets too: a rebuilt state machine re-applies
    /// from scratch (and the prefix invariant must still hold).
    fn restart(&mut self, id: NodeId, timeout: u32) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
        let mut node = RaftNode::new(id, peers, timeout);
        let hs = self.storages[&id].1.load_hard_state().unwrap();
        node.current_term = hs.current_term;
        node.voted_for = hs.voted_for;
        node.log = RaftLog::from_entries(self.storages[&id].1.load_log().unwrap());
        self.nodes.insert(id, node);
        self.crashed.remove(&id);
        self.applied.get_mut(&id).unwrap().clear();
    }

    /// The unique live leader of the highest term, if any.
    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|(id, n)| {
                !self.crashed.contains(id) && matches!(n.role, Role::Leader { .. })
            })
            .max_by_key(|(_, n)| n.current_term)
            .map(|(id, _)| *id)
    }

    fn node(&self, id: NodeId) -> &RaftNode {
        &self.nodes[&id]
    }

    fn node_mut(&mut self, id: NodeId) -> &mut RaftNode {
        self.nodes.get_mut(&id).unwrap()
    }

    fn check_invariants(&mut self) {
        // Election Safety: ≤1 leader per term, across all of history.
        for (id, n) in &self.nodes {
            if matches!(n.role, Role::Leader { .. }) {
                if let Some(prev) = self.leaders_by_term.insert(n.current_term, *id) {
                    assert_eq!(
                        prev, *id,
                        "ELECTION SAFETY VIOLATED: term {} has leaders {} and {}",
                        n.current_term, prev, id
                    );
                }
            }
        }
        // Log Matching: same (index, term) → same command, pairwise.
        let nodes: Vec<&RaftNode> = self.nodes.values().collect();
        for (i, a) in nodes.iter().enumerate() {
            for b in nodes.iter().skip(i + 1) {
                let min = a.log.last_index().min(b.log.last_index());
                for idx in 1..=min {
                    if a.log.term_at(idx) == b.log.term_at(idx) {
                        assert_eq!(
                            a.log.get(idx).unwrap().command,
                            b.log.get(idx).unwrap().command,
                            "LOG MATCHING VIOLATED at index {idx}"
                        );
                    }
                }
            }
        }
        // State Machine Safety: apply streams are mutual prefixes.
        let streams: Vec<&Vec<LogEntry>> = self.applied.values().collect();
        for (i, a) in streams.iter().enumerate() {
            for b in streams.iter().skip(i + 1) {
                let min = a.len().min(b.len());
                assert_eq!(
                    &a[..min],
                    &b[..min],
                    "STATE MACHINE SAFETY VIOLATED: divergent apply streams"
                );
            }
        }
    }

    /// Convenience: run `rounds` of (tick everyone once, deliver everything).
    fn settle(&mut self, rounds: u32) {
        for _ in 0..rounds {
            self.tick_all(1);
            self.deliver_all();
        }
    }
}

// ---- scenarios ----

/// Scenario 1+2: the node with the shortest timeout wins the election, and
/// heartbeats keep the cluster stable forever after (no spurious terms).
#[test]
fn elects_unique_leader_and_heartbeats_keep_it_stable() {
    let mut c = Cluster::new(&[(1, 10), (2, 15), (3, 20)]);
    c.tick_all(10); // node 1 fires candidacy on its 10th tick
    c.deliver_all();
    assert_eq!(c.leader(), Some(1));
    assert_eq!(c.node(1).current_term, 1);

    // 50 more tick+deliver rounds: same leader, same term, all logs equal.
    c.settle(50);
    assert_eq!(c.leader(), Some(1));
    assert_eq!(c.node(1).current_term, 1, "heartbeats must suppress elections");
}

/// Scenario 3: simultaneous candidates split the vote; re-armed (different)
/// timeouts resolve it in a later term.
#[test]
fn split_vote_resolves_after_rearm() {
    let mut c = Cluster::new(&[(1, 10), (2, 10)]); // 2-node, SAME timeout
    c.tick_all(10); // both become candidates of term 1 before any delivery
    c.deliver_all(); // each already voted for itself → both denied
    assert_eq!(c.leader(), None, "split vote: no winner in term 1");

    // Re-arm with different timeouts (the shell's randomization, made manual).
    c.node_mut(1).set_election_timeout(5);
    c.node_mut(2).set_election_timeout(20);
    c.tick_all(5); // node 1 fires first, term 2
    c.deliver_all(); // node 2 hasn't voted in term 2 → grants
    assert_eq!(c.leader(), Some(1));
    assert_eq!(c.node(1).current_term, 2);
}

/// Scenario 5/7 (Leader Completeness): a committed entry survives a leader
/// crash, because the lagging node CANNOT win election — voters with the
/// entry refuse it, and majority overlap guarantees such a voter exists.
#[test]
fn committed_entry_survives_leader_crash() {
    // Node 3 has the SHORTEST post-crash timeout — it will try first and fail.
    let mut c = Cluster::new(&[(1, 10), (2, 30), (3, 20)]);
    c.tick_all(10);
    c.deliver_all();
    assert_eq!(c.leader(), Some(1));

    // Partition node 3 away; commit an entry on {1, 2} (majority of 3).
    c.partition(&[3], &[1, 2]);
    c.propose(1, b"precious");
    c.deliver_all();
    assert_eq!(c.node(1).commit_index, 1, "me + node 2 = majority");

    // Leader dies; partition heals. Node 3 (no entry) times out FIRST.
    c.crash(1);
    c.heal();
    c.tick_all(20); // node 3 fires candidacy (term 2)
    c.deliver_all();
    // Node 2 refused (3's log is behind) — 3 cannot reach majority.
    assert_eq!(c.leader(), None);

    // With FIXED timeouts node 3 would disrupt forever: it re-fires every 20
    // ticks and each new term resets node 2's counter before its 30 ever
    // elapse — the starvation randomized timeouts exist to break. Play the
    // shell's role and re-arm node 2 shorter for the next round.
    c.node_mut(2).set_election_timeout(5);
    c.tick_all(5);
    c.deliver_all();
    let leader = c.leader().expect("node 2 should win");
    assert_eq!(leader, 2);
    // The committed entry lives on the new leader.
    assert_eq!(c.node(2).log.get(1).unwrap().command, b"precious");

    // And the new leader repairs node 3 (commit needs a CURRENT-term entry —
    // Figure 8 — so propose one, then everything applies everywhere).
    c.propose(2, b"new-era");
    c.settle(5);
    assert_eq!(c.node(3).log.get(1).unwrap().command, b"precious");
    assert!(c.node(3).commit_index >= 1, "old entry committed transitively");
}

/// Scenario 10: a partitioned stale leader can't commit; on heal it steps
/// down and its uncommitted garbage is truncated away.
#[test]
fn stale_leader_steps_down_and_uncommitted_entries_vanish() {
    let mut c = Cluster::new(&[(1, 10), (2, 15), (3, 1000)]);
    c.tick_all(10);
    c.deliver_all();
    assert_eq!(c.leader(), Some(1));
    c.propose(1, b"committed");
    c.settle(3); // replicate + commit news everywhere
    assert_eq!(c.node(3).commit_index, 1);

    // Cut the leader off; it keeps accepting proposals it can never commit.
    c.partition(&[1], &[2, 3]);
    c.propose(1, b"doomed-1");
    c.propose(1, b"doomed-2");
    c.deliver_all(); // all its sends are dropped
    assert_eq!(c.node(1).commit_index, 1, "no majority → no commit");
    assert_eq!(c.node(1).log.last_index(), 3);

    // The majority side elects node 2 (term 2) and commits new entries.
    c.tick_all(15);
    c.deliver_all();
    assert_eq!(c.leader(), Some(2));
    c.propose(2, b"the-future");
    c.deliver_all();
    assert_eq!(c.node(2).commit_index, 2);

    // Heal: old leader hears term 2, steps down, gets repaired — its doomed
    // entries are truncated and replaced by the real index-2 entry.
    c.heal();
    c.settle(10);
    assert!(matches!(c.node(1).role, Role::Follower));
    assert_eq!(c.node(1).current_term, 2);
    assert_eq!(c.node(1).log.last_index(), 2);
    assert_eq!(c.node(1).log.get(2).unwrap().command, b"the-future");
    assert_eq!(c.node(1).commit_index, 2);
}

/// Scenario 11: crash + restart-from-disk — hard state and log survive; the
/// rebuilt node re-applies from scratch and catches up.
#[test]
fn restart_recovers_persistent_state_and_catches_up() {
    let mut c = Cluster::new(&[(1, 10), (2, 100), (3, 1000)]);
    c.tick_all(10);
    c.deliver_all();
    c.propose(1, b"a");
    c.settle(3);
    assert_eq!(c.node(2).commit_index, 1);

    // Node 2 dies; the cluster commits another entry without it.
    c.crash(2);
    c.propose(1, b"b");
    c.deliver_all(); // node 3 acks → 2 of 3 = majority
    assert_eq!(c.node(1).commit_index, 2);

    // Restart node 2 from ITS OWN sled: term + log are back (commit is
    // volatile and re-learned from the leader's heartbeats).
    c.restart(2, 100);
    assert_eq!(c.node(2).current_term, 1, "hard state survived");
    assert_eq!(c.node(2).log.last_index(), 1, "log survived");

    c.settle(5); // heartbeats deliver entry b + commit news
    assert_eq!(c.node(2).log.last_index(), 2);
    assert_eq!(c.node(2).commit_index, 2);
    let applied: Vec<&[u8]> = c.applied[&2].iter().map(|e| e.command.as_slice()).collect();
    assert_eq!(applied, vec![b"a".as_slice(), b"b"], "re-applied in order");
}

/// Scenario 12: a 5-node cluster commits with 2 nodes down, but with 3 down
/// it is SAFE but not LIVE — proposals replicate nowhere near majority.
#[test]
fn five_nodes_survive_two_crashes_but_halt_at_three() {
    let mut c = Cluster::new(&[(1, 10), (2, 50), (3, 60), (4, 70), (5, 80)]);
    c.tick_all(10);
    c.deliver_all();
    assert_eq!(c.leader(), Some(1));

    c.crash(4);
    c.crash(5);
    c.propose(1, b"with-three");
    c.deliver_all(); // acks from 2 and 3 → 3 of 5 = majority
    assert_eq!(c.node(1).commit_index, 1, "3 of 5 still commits");

    c.crash(3);
    c.propose(1, b"with-two");
    c.deliver_all(); // only node 2 can ack → 2 of 5 — never a majority
    assert_eq!(c.node(1).commit_index, 1, "NO commit below majority — safety over liveness");
}

/// Duplicate delivery: re-injecting every in-flight message twice must change
/// nothing (the core is idempotent; invariants run after every step).
#[test]
fn duplicated_messages_are_harmless() {
    let mut c = Cluster::new(&[(1, 10), (2, 15), (3, 20)]);
    c.tick_all(10);
    c.deliver_all();
    c.propose(1, b"once");

    // Duplicate the entire in-flight queue.
    let doubled: Vec<(NodeId, NodeId, Message)> = c.inbox.iter().cloned().collect();
    c.inbox.extend(doubled);
    c.deliver_all();
    c.settle(3);

    // Exactly one entry everywhere, committed everywhere.
    for id in [1, 2, 3] {
        assert_eq!(c.node(id).log.last_index(), 1, "node {id}");
        assert_eq!(c.node(id).commit_index, 1, "node {id}");
    }
    assert_eq!(c.applied[&2].len(), 1, "applied exactly once");
}
