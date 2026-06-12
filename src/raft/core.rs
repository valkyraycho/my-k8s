//! The Raft state machine — Figure 2 as a PURE function. No I/O, no clocks,
//! no randomness: events in, effects out. The shell (node.rs) owns reality.

use std::collections::{HashMap, HashSet};

use crate::raft::{
    log::{LogEntry, LogIndex, NodeId, RaftLog, Term},
    message::{AppendEntriesReq, AppendEntriesResp, Message, RequestVoteReq, RequestVoteResp},
};

/// Leader heartbeats every N ticks (shell ticks ~50ms → ~150ms heartbeats).
pub const HEARTBEAT_TICKS: u32 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Follower,
    Candidate {
        votes: HashSet<NodeId>,
    },
    // Leader bookkeeping lives IN the variant: it can't exist unless we ARE
    // leader, and `role = Follower` destroys it on step-down for free.
    Leader {
        next_index: HashMap<NodeId, LogIndex>,
        match_index: HashMap<NodeId, LogIndex>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Tick,
    Message(NodeId, Message),
    Propose(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    Send(NodeId, Message),
    /// Hard state changed — shell MUST fsync before any Send in the same
    /// batch. Vec ORDER is the contract.
    Persist,
    PersistTruncate(LogIndex),
    PersistEntries(Vec<LogEntry>),
    Apply(LogEntry),
    ProposeRejected {
        leader_hint: Option<NodeId>,
    },
}

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: Role,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: RaftLog,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    ticks_since_reset: u32,
    election_timeout_ticks: u32,
    pub leader_hint: Option<NodeId>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, election_timeout_ticks: u32) -> Self {
        Self {
            id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            ticks_since_reset: 0,
            election_timeout_ticks,
            leader_hint: None,
        }
    }

    pub fn set_election_timeout(&mut self, ticks: u32) {
        self.election_timeout_ticks = ticks;
    }

    // Majority of the FULL cluster (peers + me): div_ceil(p,2)+1 ==
    // floor((p+1)/2)+1. 3 nodes → 2, 5 → 3, single node → 1.
    fn majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    pub fn step(&mut self, event: Event) -> Vec<Effect> {
        match event {
            Event::Tick => self.on_tick(),
            Event::Message(from, Message::RequestVote(req)) => self.on_request_vote(from, req),
            Event::Message(from, Message::RequestVoteResp(resp)) => {
                self.on_request_vote_resp(from, resp)
            }
            Event::Message(from, Message::AppendEntries(req)) => self.on_append_entries(from, req),
            Event::Message(from, Message::AppendEntriesResp(resp)) => {
                self.on_append_entries_resp(from, resp)
            }
            Event::Propose(cmd) => self.on_propose(cmd),
        }
    }

    fn on_tick(&mut self) -> Vec<Effect> {
        self.ticks_since_reset += 1;
        match &self.role {
            Role::Leader { .. } => {
                if self.ticks_since_reset >= HEARTBEAT_TICKS {
                    self.ticks_since_reset = 0;
                    return self.broadcast_append();
                }
                vec![]
            }
            Role::Follower | Role::Candidate { .. } => {
                if self.ticks_since_reset >= self.election_timeout_ticks {
                    return self.start_election();
                }
                vec![]
            }
        }
    }
    /// Become Candidate: term+1, vote for self, ask everyone. Persist comes
    /// FIRST in the effects (term+vote must hit disk before any vote request
    /// leaves — else a crash could let us vote differently in this term).
    fn start_election(&mut self) -> Vec<Effect> {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate {
            votes: HashSet::from([self.id]),
        };
        self.ticks_since_reset = 0;
        self.leader_hint = None;

        let mut effects = vec![Effect::Persist];
        let req = RequestVoteReq {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        for &peer in &self.peers {
            effects.push(Effect::Send(peer, Message::RequestVote(req.clone())));
        }
        effects.extend(self.try_win());
        effects
    }

    /// The all-RPC rule (Figure 2 bottom): ANY message bearing a higher term
    /// makes us a Follower of that term, vote forgotten.
    fn maybe_step_down(&mut self, msg_term: Term) -> Option<Effect> {
        if msg_term <= self.current_term {
            return None;
        }

        self.current_term = msg_term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.ticks_since_reset = 0;
        Some(Effect::Persist)
    }

    fn on_request_vote(&mut self, from: NodeId, req: RequestVoteReq) -> Vec<Effect> {
        let mut effects = vec![];
        effects.extend(self.maybe_step_down(req.term));

        // §5.4.1 up-to-date check: lexicographic (last_term, last_index) —
        // higher term wins outright, same term → longer log wins.
        let log_ok = (req.last_log_term, req.last_log_index)
            >= (self.log.last_term(), self.log.last_index());
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
        let grant = req.term == self.current_term && log_ok && can_vote;

        if grant {
            self.voted_for = Some(req.candidate_id);
            self.ticks_since_reset = 0; // granting defers our own candidacy
            effects.push(Effect::Persist);
        }
        effects.push(Effect::Send(
            from,
            Message::RequestVoteResp(RequestVoteResp {
                term: self.current_term,
                vote_granted: grant,
            }),
        ));

        effects
    }

    fn on_request_vote_resp(&mut self, from: NodeId, resp: RequestVoteResp) -> Vec<Effect> {
        let mut effects = vec![];
        effects.extend(self.maybe_step_down(resp.term));

        if let Role::Candidate { votes } = &mut self.role
            && resp.term == self.current_term
            && resp.vote_granted
        {
            votes.insert(from);
            effects.extend(self.try_win());
        }
        effects
    }

    /// Candidate with a majority → Leader: init next/match, heartbeat at once
    /// (announce + suppress further elections). Also wins single-node clusters
    /// instantly (self vote IS the majority).
    fn try_win(&mut self) -> Vec<Effect> {
        let Role::Candidate { votes } = &self.role else {
            return vec![];
        };
        if votes.len() < self.majority() {
            return vec![];
        }

        let next = self.log.last_index() + 1;
        self.role = Role::Leader {
            next_index: self.peers.iter().map(|&p| (p, next)).collect(),
            match_index: self.peers.iter().map(|&p| (p, 0)).collect(),
        };
        self.leader_hint = Some(self.id);
        self.ticks_since_reset = 0;
        self.broadcast_append()
    }

    /// AppendEntries for one peer from its next_index — carries everything the
    /// peer hasn't acked (empty when caught up = pure heartbeat). One mechanism
    /// = heartbeat + replication + retransmission.
    fn append_for(&self, peer: NodeId) -> Effect {
        let next = match &self.role {
            Role::Leader { next_index, .. } => next_index
                .get(&peer)
                .copied()
                .unwrap_or(self.log.last_index() + 1),
            _ => self.log.last_index() + 1,
        };

        let prev_index = next - 1;
        Effect::Send(
            peer,
            Message::AppendEntries(AppendEntriesReq {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_index,
                prev_log_term: self.log.term_at(prev_index).unwrap_or(0),
                entries: self.log.entries_from(next),
                leader_commit: self.commit_index,
            }),
        )
    }

    fn broadcast_append(&self) -> Vec<Effect> {
        self.peers.iter().map(|&p| self.append_for(p)).collect()
    }

    fn on_append_entries(&mut self, from: NodeId, req: AppendEntriesReq) -> Vec<Effect> {
        let mut effects = vec![];
        effects.extend(self.maybe_step_down(req.term));

        if req.term < self.current_term {
            effects.push(Effect::Send(
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                }),
            ));
            return effects;
        }

        if matches!(self.role, Role::Candidate { .. }) {
            self.role = Role::Follower;
        }
        self.ticks_since_reset = 0;
        self.leader_hint = Some(req.leader_id);

        if self.log.term_at(req.prev_log_index) != Some(req.prev_log_term) {
            effects.push(Effect::Send(
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                }),
            ));
            return effects;
        }

        let new_match = req.prev_log_index + req.entries.len() as u64;

        let mut to_append = vec![];
        for entry in req.entries {
            match self.log.term_at(entry.index) {
                Some(t) if t == entry.term => continue,
                Some(_) => {
                    self.log.truncate_from(entry.index);
                    effects.push(Effect::PersistTruncate(entry.index));
                    to_append.push(entry);
                }
                None => to_append.push(entry),
            }
        }

        if !to_append.is_empty() {
            effects.push(Effect::PersistEntries(to_append.clone()));
            self.log.append_entries(to_append);
        }

        // Commit news rides every AppendEntries (incl. heartbeats), capped at
        // what we actually hold.
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
            effects.extend(self.advance_applied());
        }

        effects.push(Effect::Send(
            from,
            Message::AppendEntriesResp(AppendEntriesResp {
                term: self.current_term,
                success: true,
                match_index: new_match,
            }),
        ));

        effects
    }

    fn on_propose(&mut self, cmd: Vec<u8>) -> Vec<Effect> {
        if !matches!(self.role, Role::Leader { .. }) {
            return vec![Effect::ProposeRejected {
                leader_hint: self.leader_hint,
            }];
        }
        let index = self.log.append(self.current_term, cmd);
        let entry = self.log.get(index).expect("just appended").clone();
        // Durable on the leader BEFORE broadcast — we count ourselves in the
        // majority, so our copy must survive a crash.
        let mut effects = vec![Effect::PersistEntries(vec![entry])];
        effects.extend(self.broadcast_append());
        effects.extend(self.try_commit()); // single-node cluster commits NOW
        effects
    }

    /// Emit Apply for everything committed-but-not-yet-applied, in order.
    /// Shared with the leader's commit path (step 5).
    fn advance_applied(&mut self) -> Vec<Effect> {
        let mut effects = vec![];
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied) {
                effects.push(Effect::Apply(entry.clone()));
            }
        }
        effects
    }

    fn on_append_entries_resp(&mut self, from: NodeId, resp: AppendEntriesResp) -> Vec<Effect> {
        let mut effects = vec![];
        effects.extend(self.maybe_step_down(resp.term));

        if resp.term < self.current_term {
            return effects;
        }

        let succeeded = {
            let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
            else {
                return effects;
            };
            if resp.success {
                // max(): a reordered stale ack can stand still, never regress.
                let m = match_index.entry(from).or_insert(0);
                *m = (*m).max(resp.match_index);
                next_index.insert(from, *m + 1);
                true
            } else {
                // Consistency reject: walk back one and re-probe (floor 1 —
                // prev becomes the (0,0) sentinel that always matches).
                let ni = next_index.entry(from).or_insert(2);
                *ni = ni.saturating_sub(1).max(1);
                false
            }
        };
        if succeeded {
            effects.extend(self.try_commit());
        } else {
            effects.push(self.append_for(from));
        }
        effects
    }

    /// Advance commit_index to the highest majority-held entry — counting ONLY
    /// entries of our OWN term (the Figure 8 rule). Older entries commit
    /// transitively once a current-term entry above them does.
    fn try_commit(&mut self) -> Vec<Effect> {
        let new_commit = {
            let Role::Leader { match_index, .. } = &self.role else {
                return vec![];
            };
            let mut found = self.commit_index;
            for n in (self.commit_index + 1..=self.log.last_index()).rev() {
                if self.log.term_at(n) != Some(self.current_term) {
                    continue; // Figure 8 guard: never count old-term entries
                }

                let holders = 1 + self
                    .peers
                    .iter()
                    .filter(|p| match_index.get(p).copied().unwrap_or(0) >= n)
                    .count();
                if holders >= self.majority() {
                    found = n;
                    break;
                }
            }
            found
        };

        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.advance_applied()
        } else {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId, peers: Vec<NodeId>, timeout: u32) -> RaftNode {
        RaftNode::new(id, peers, timeout)
    }

    /// Drive `n` ticks, returning only the LAST tick's effects.
    fn tick_n(node: &mut RaftNode, n: u32) -> Vec<Effect> {
        let mut last = Vec::new();
        for _ in 0..n {
            last = node.step(Event::Tick);
        }
        last
    }

    fn vote_req(term: Term, candidate: NodeId, last_idx: LogIndex, last_term: Term) -> Event {
        Event::Message(
            candidate,
            Message::RequestVote(RequestVoteReq {
                term,
                candidate_id: candidate,
                last_log_index: last_idx,
                last_log_term: last_term,
            }),
        )
    }

    fn grant_from(voter: NodeId, term: Term) -> Event {
        Event::Message(
            voter,
            Message::RequestVoteResp(RequestVoteResp {
                term,
                vote_granted: true,
            }),
        )
    }

    /// Pull the granted flag out of the reply this node sent.
    fn replied_grant(effects: &[Effect]) -> bool {
        effects
            .iter()
            .find_map(|e| match e {
                Effect::Send(_, Message::RequestVoteResp(r)) => Some(r.vote_granted),
                _ => None,
            })
            .expect("no RequestVoteResp in effects")
    }

    /// 3-node leader fixture: node 1 elected with node 2's vote.
    fn make_leader() -> RaftNode {
        let mut n = node(1, vec![2, 3], 1);
        n.step(Event::Tick); // timeout=1 → instant candidacy
        n.step(grant_from(2, 1)); // majority → leader
        assert!(matches!(n.role, Role::Leader { .. }));
        n
    }

    #[test]
    fn election_fires_exactly_at_timeout() {
        let mut n = node(1, vec![2, 3], 10);
        assert!(tick_n(&mut n, 9).is_empty(), "no election before timeout");
        assert_eq!(n.current_term, 0);

        let effects = n.step(Event::Tick); // tick #10
        assert_eq!(n.current_term, 1);
        assert_eq!(n.voted_for, Some(1));
        assert!(matches!(&n.role, Role::Candidate { votes } if votes.contains(&1)));

        // Persist FIRST (term+vote to disk before any request leaves), then
        // a RequestVote to each peer.
        assert_eq!(effects[0], Effect::Persist);
        let sends = effects
            .iter()
            .filter(|e| matches!(e, Effect::Send(_, Message::RequestVote(_))))
            .count();
        assert_eq!(sends, 2);
    }

    #[test]
    fn single_node_cluster_wins_instantly() {
        let mut n = node(1, vec![], 3);
        tick_n(&mut n, 3);
        // Self vote IS the majority (1 of 1) — leader with no messages sent.
        assert!(matches!(n.role, Role::Leader { .. }));
        assert_eq!(n.leader_hint, Some(1));
    }

    #[test]
    fn wins_on_majority_and_initializes_leader_state() {
        let mut n = node(1, vec![2, 3], 1);
        n.log.append(1, b"old".to_vec()); // pretend prior history: last_index=1
        n.step(Event::Tick); // term 2 candidacy (term bumped from... 0→1)

        let effects = n.step(grant_from(2, 1));
        match &n.role {
            Role::Leader {
                next_index,
                match_index,
            } => {
                // next = last_index+1 for every peer; match starts at 0.
                assert_eq!(next_index[&2], 2);
                assert_eq!(next_index[&3], 2);
                assert_eq!(match_index[&2], 0);
            }
            other => panic!("expected leader, got {other:?}"),
        }
        // Immediate heartbeats to BOTH peers announce the win.
        let hb = effects
            .iter()
            .filter(|e| matches!(e, Effect::Send(_, Message::AppendEntries(_))))
            .count();
        assert_eq!(hb, 2);
    }

    #[test]
    fn five_node_cluster_needs_three_votes() {
        let mut n = node(1, vec![2, 3, 4, 5], 1);
        n.step(Event::Tick);
        n.step(grant_from(2, 1)); // votes {1,2} < 3
        assert!(matches!(n.role, Role::Candidate { .. }));
        n.step(grant_from(3, 1)); // votes {1,2,3} = 3 → majority
        assert!(matches!(n.role, Role::Leader { .. }));
    }

    #[test]
    fn duplicate_grants_from_same_voter_count_once() {
        let mut n = node(1, vec![2, 3, 4, 5], 1);
        n.step(Event::Tick);
        n.step(grant_from(2, 1));
        n.step(grant_from(2, 1)); // same voter again — HashSet dedups
        assert!(
            matches!(n.role, Role::Candidate { .. }),
            "two grants from one voter must not win"
        );
    }

    #[test]
    fn stale_term_vote_response_is_ignored() {
        let mut n = node(1, vec![2, 3], 1);
        n.step(Event::Tick); // term 1 candidacy
        n.step(Event::Tick); // (still candidate; timeout=1 → term 2 candidacy)
        assert_eq!(n.current_term, 2);
        n.step(grant_from(2, 1)); // grant for the OLD term 1
        assert!(
            matches!(n.role, Role::Candidate { .. }),
            "stale-term grant must not count toward term 2"
        );
    }

    // ---- the vote decision table ----

    #[test]
    fn vote_denied_for_stale_term() {
        let mut n = node(2, vec![1, 3], 100);
        n.current_term = 5;
        let effects = n.step(vote_req(3, 1, 0, 0)); // candidate stuck at term 3
        assert!(!replied_grant(&effects));
        assert_eq!(n.voted_for, None);
    }

    #[test]
    fn vote_granted_once_then_denied_for_other_candidate() {
        let mut n = node(2, vec![1, 3], 100);
        // First candidate of term 1 → grant (+Persist before the reply Send).
        let effects = n.step(vote_req(1, 1, 0, 0));
        assert!(replied_grant(&effects));
        assert_eq!(n.voted_for, Some(1));
        let persist_pos = effects.iter().position(|e| *e == Effect::Persist).unwrap();
        let send_pos = effects
            .iter()
            .position(|e| matches!(e, Effect::Send(..)))
            .unwrap();
        assert!(
            persist_pos < send_pos,
            "vote must hit disk before the reply"
        );

        // Second candidate, same term → deny (already voted for 1).
        let effects = n.step(vote_req(1, 3, 0, 0));
        assert!(!replied_grant(&effects));
        // But re-asking by the SAME candidate is granted again (idempotent).
        let effects = n.step(vote_req(1, 1, 0, 0));
        assert!(replied_grant(&effects));
    }

    #[test]
    fn vote_denied_when_candidate_log_is_behind() {
        let mut n = node(2, vec![1, 3], 100);
        n.log.append(1, b"a".to_vec());
        n.log.append(1, b"b".to_vec()); // my log: last=(term 1, index 2)

        // Same last term, SHORTER log → deny.
        let effects = n.step(vote_req(2, 1, 1, 1));
        assert!(!replied_grant(&effects));
        // Lower last term (even if longer) → deny.
        let effects = n.step(vote_req(2, 3, 9, 0));
        assert!(!replied_grant(&effects));
        // NOTE: term 2 was still adopted (all-RPC rule) even though votes
        // were denied — the candidate's term wins, its log doesn't.
        assert_eq!(n.current_term, 2);
        assert_eq!(n.voted_for, None);

        // Higher last term → grant regardless of length.
        let effects = n.step(vote_req(2, 1, 1, 2));
        assert!(replied_grant(&effects));
    }

    #[test]
    fn granting_a_vote_resets_own_election_timer() {
        let mut n = node(2, vec![1, 3], 10);
        tick_n(&mut n, 9); // one tick from starting our own election
        n.step(vote_req(1, 1, 0, 0)); // grant → timer reset
        let effects = n.step(Event::Tick); // would have been tick #10
        assert!(effects.is_empty(), "vote grant must defer our candidacy");
        assert!(matches!(n.role, Role::Follower));
    }

    // ---- step-down + leader behavior ----

    #[test]
    fn higher_term_message_dethrones_leader() {
        let mut n = make_leader(); // leader of term 1
        let effects = n.step(vote_req(2, 3, 0, 0));
        // Stepped down, adopted term 2; leader bookkeeping died with the variant.
        assert!(matches!(n.role, Role::Follower));
        assert_eq!(n.current_term, 2);
        assert!(effects.contains(&Effect::Persist));
    }

    #[test]
    fn leader_heartbeats_on_schedule() {
        let mut n = make_leader();
        // The first HEARTBEAT_TICKS-1 ticks are quiet, then a broadcast.
        assert!(tick_n(&mut n, HEARTBEAT_TICKS - 1).is_empty());
        let effects = n.step(Event::Tick);
        let hb = effects
            .iter()
            .filter(|e| matches!(e, Effect::Send(_, Message::AppendEntries(_))))
            .count();
        assert_eq!(hb, 2, "heartbeat to every peer");
    }

    #[test]
    fn propose_on_non_leader_is_rejected_with_hint() {
        let mut n = node(2, vec![1, 3], 100);
        let effects = n.step(Event::Propose(b"x".to_vec()));
        assert_eq!(effects, vec![Effect::ProposeRejected { leader_hint: None }]);
    }

    // ---- AppendEntries follower side (step 4) ----

    fn entry(index: LogIndex, term: Term) -> LogEntry {
        LogEntry {
            term,
            index,
            command: format!("cmd-{index}").into_bytes(),
        }
    }

    fn ae(
        term: Term,
        leader: NodeId,
        prev: (LogIndex, Term),
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> Event {
        Event::Message(
            leader,
            Message::AppendEntries(AppendEntriesReq {
                term,
                leader_id: leader,
                prev_log_index: prev.0,
                prev_log_term: prev.1,
                entries,
                leader_commit,
            }),
        )
    }

    /// Pull the AppendEntriesResp out of the effects.
    fn ae_resp(effects: &[Effect]) -> AppendEntriesResp {
        effects
            .iter()
            .find_map(|e| match e {
                Effect::Send(_, Message::AppendEntriesResp(r)) => Some(r.clone()),
                _ => None,
            })
            .expect("no AppendEntriesResp in effects")
    }

    #[test]
    fn append_entries_rejects_stale_leader() {
        let mut n = node(2, vec![1, 3], 100);
        n.current_term = 5;
        let effects = n.step(ae(3, 1, (0, 0), vec![], 0));
        let resp = ae_resp(&effects);
        assert!(!resp.success);
        assert_eq!(resp.term, 5, "teach the stale leader our term");
        // A stale leader must NOT reset our timer or claim leader_hint.
        assert_eq!(n.leader_hint, None);
    }

    #[test]
    fn heartbeat_resets_election_timer_and_suppresses_election() {
        let mut n = node(2, vec![1, 3], 10);
        tick_n(&mut n, 9); // one tick from candidacy
        n.step(ae(1, 1, (0, 0), vec![], 0)); // live leader's heartbeat
        let effects = n.step(Event::Tick); // would have been tick #10
        assert!(effects.is_empty(), "heartbeat must defer the election");
        assert_eq!(n.leader_hint, Some(1));
        assert_eq!(n.current_term, 1, "adopted the leader's term");
    }

    #[test]
    fn append_entries_rejects_on_consistency_gap() {
        let mut n = node(2, vec![1, 3], 100);
        // Leader thinks we have (1, t1); our log is empty → reject.
        let effects = n.step(ae(1, 1, (1, 1), vec![entry(2, 1)], 0));
        assert!(!ae_resp(&effects).success);
        assert_eq!(n.log.last_index(), 0, "nothing appended past a gap");
        // But the timer/leader_hint DID reset — a live leader exists and is
        // about to back off next_index to repair us.
        assert_eq!(n.leader_hint, Some(1));
    }

    #[test]
    fn appends_new_entries_acks_match_and_persists_before_reply() {
        let mut n = node(2, vec![1, 3], 100);
        let effects = n.step(ae(1, 1, (0, 0), vec![entry(1, 1), entry(2, 1)], 0));

        let resp = ae_resp(&effects);
        assert!(resp.success);
        assert_eq!(resp.match_index, 2);
        assert_eq!(n.log.last_index(), 2);

        // PersistEntries must precede the reply Send: "I have it" must be
        // durable before we say it.
        let persist_pos = effects
            .iter()
            .position(|e| matches!(e, Effect::PersistEntries(_)))
            .expect("entries persisted");
        let send_pos = effects
            .iter()
            .position(|e| matches!(e, Effect::Send(_, Message::AppendEntriesResp(_))))
            .unwrap();
        assert!(persist_pos < send_pos);
    }

    #[test]
    fn duplicate_delivery_is_idempotent() {
        let mut n = node(2, vec![1, 3], 100);
        let msg = ae(1, 1, (0, 0), vec![entry(1, 1), entry(2, 1)], 0);
        n.step(msg.clone());
        let effects = n.step(msg); // exact retransmit

        // Same ack, but NO new persistence and NO log change.
        let resp = ae_resp(&effects);
        assert!(resp.success);
        assert_eq!(resp.match_index, 2);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::PersistEntries(_) | Effect::PersistTruncate(_))),
            "retransmit must not re-persist or truncate"
        );
        assert_eq!(n.log.last_index(), 2);
    }

    /// The worked example from the walkthrough: follower diverged at 3-4 with
    /// term-2 entries; term-3 leader replaces them.
    #[test]
    fn conflicting_suffix_is_truncated_and_replaced() {
        let mut n = node(2, vec![1, 3], 100);
        n.current_term = 3;
        n.step(ae(3, 1, (0, 0), vec![entry(1, 1), entry(2, 1)], 0));
        // Inject the divergent suffix directly (as if from a deposed leader).
        n.log.append_entries(vec![entry(3, 2), entry(4, 2)]);

        let effects = n.step(ae(3, 1, (2, 1), vec![entry(3, 3), entry(4, 3)], 0));

        assert!(ae_resp(&effects).success);
        assert!(effects.contains(&Effect::PersistTruncate(3)));
        // Final log: [t1, t1, t3, t3] — the leader's version won.
        let terms: Vec<Term> = (1..=4).map(|i| n.log.term_at(i).unwrap()).collect();
        assert_eq!(terms, vec![1, 1, 3, 3]);
        assert_eq!(n.log.last_index(), 4);
    }

    #[test]
    fn commit_advances_applies_in_order_and_caps_at_log_end() {
        let mut n = node(2, vec![1, 3], 100);
        n.step(ae(1, 1, (0, 0), vec![entry(1, 1), entry(2, 1)], 0));

        // Leader says commit=5 but we only hold 2 → cap at 2, apply 1 then 2.
        let effects = n.step(ae(1, 1, (2, 1), vec![], 5));
        let applied: Vec<LogIndex> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::Apply(entry) => Some(entry.index),
                _ => None,
            })
            .collect();
        assert_eq!(applied, vec![1, 2], "apply IN ORDER, capped at our log end");
        assert_eq!(n.commit_index, 2);
        assert_eq!(n.last_applied, 2);

        // Same commit news again → nothing new to apply.
        let effects = n.step(ae(1, 1, (2, 1), vec![], 5));
        assert!(!effects.iter().any(|e| matches!(e, Effect::Apply(_))));
    }

    #[test]
    fn candidate_yields_to_leader_of_same_term() {
        let mut n = node(2, vec![1, 3], 1);
        n.step(Event::Tick); // candidate, term 1
        assert!(matches!(n.role, Role::Candidate { .. }));

        // A leader of OUR term announces itself → we lost the race; yield.
        n.step(ae(1, 1, (0, 0), vec![], 0));
        assert!(matches!(n.role, Role::Follower));
        assert_eq!(n.leader_hint, Some(1));
        assert_eq!(n.current_term, 1, "same term — no bump");
    }

    // ---- leader replication + commit (step 5) ----

    fn ack(from: NodeId, term: Term, match_index: LogIndex) -> Event {
        Event::Message(
            from,
            Message::AppendEntriesResp(AppendEntriesResp {
                term,
                success: true,
                match_index,
            }),
        )
    }

    fn nack(from: NodeId, term: Term) -> Event {
        Event::Message(
            from,
            Message::AppendEntriesResp(AppendEntriesResp {
                term,
                success: false,
                match_index: 0,
            }),
        )
    }

    /// All AppendEntries sends in the effects, as (peer, request) pairs.
    fn ae_sends(effects: &[Effect]) -> Vec<(NodeId, AppendEntriesReq)> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Send(to, Message::AppendEntries(req)) => Some((*to, req.clone())),
                _ => None,
            })
            .collect()
    }

    fn applied(effects: &[Effect]) -> Vec<LogIndex> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Apply(entry) => Some(entry.index),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn propose_persists_replicates_then_commits_on_majority_ack() {
        let mut n = make_leader(); // 3-node leader, term 1
        let effects = n.step(Event::Propose(b"x=5".to_vec()));

        // Persist FIRST, then a broadcast carrying the entry; nothing applied
        // yet — the entry is replicated, not committed.
        assert!(matches!(effects[0], Effect::PersistEntries(_)));
        let sends = ae_sends(&effects);
        assert_eq!(sends.len(), 2);
        assert!(sends.iter().all(|(_, req)| req.entries.len() == 1));
        assert!(applied(&effects).is_empty(), "not committed yet");

        // One ack = me + peer 2 = majority of 3 → commit + apply.
        let effects = n.step(ack(2, 1, 1));
        assert_eq!(n.commit_index, 1);
        assert_eq!(applied(&effects), vec![1]);

        // Bookkeeping advanced for the acking peer.
        let Role::Leader {
            next_index,
            match_index,
        } = &n.role
        else {
            panic!("still leader");
        };
        assert_eq!(match_index[&2], 1);
        assert_eq!(next_index[&2], 2);
    }

    #[test]
    fn five_node_commit_waits_for_two_acks() {
        let mut n = node(1, vec![2, 3, 4, 5], 1);
        n.step(Event::Tick);
        n.step(grant_from(2, 1));
        n.step(grant_from(3, 1)); // leader of 5
        n.step(Event::Propose(b"x".to_vec()));

        let effects = n.step(ack(2, 1, 1)); // me+2 = 2 of 5 — NOT majority
        assert_eq!(n.commit_index, 0);
        assert!(applied(&effects).is_empty());

        let effects = n.step(ack(3, 1, 1)); // me+2+3 = 3 of 5 → commit
        assert_eq!(n.commit_index, 1);
        assert_eq!(applied(&effects), vec![1]);
    }

    #[test]
    fn single_node_proposes_and_commits_in_one_step() {
        let mut n = node(1, vec![], 1);
        n.step(Event::Tick); // instant leader
        let effects = n.step(Event::Propose(b"solo".to_vec()));
        // No peers to wait for: persisted AND applied in the same batch.
        assert_eq!(n.commit_index, 1);
        assert_eq!(applied(&effects), vec![1]);
    }

    #[test]
    fn rejection_walks_next_index_back_and_reprobes_with_more_history() {
        let mut n = make_leader();
        for cmd in [b"a".as_slice(), b"b", b"c"] {
            n.step(Event::Propose(cmd.to_vec()));
        }
        n.step(ack(2, 1, 3)); // peer 2 caught up → next_index[2] = 4

        // Now a (contrived) consistency reject from 2 → walk back to 3 and
        // immediately re-probe carrying entries from 3.
        let effects = n.step(nack(2, 1));
        let sends = ae_sends(&effects);
        assert_eq!(sends.len(), 1);
        let (to, req) = &sends[0];
        assert_eq!(*to, 2);
        assert_eq!(req.prev_log_index, 2);
        assert_eq!(req.entries.len(), 1, "carries everything from new next_index");
        assert_eq!(req.entries[0].index, 3);

        // Repeated rejects floor at next_index = 1 (prev = the (0,0) sentinel).
        n.step(nack(2, 1));
        let effects = n.step(nack(2, 1));
        let (_, req) = &ae_sends(&effects)[0];
        assert_eq!(req.prev_log_index, 0);
        assert_eq!(req.entries.len(), 3, "full history from index 1");
    }

    #[test]
    fn stale_term_ack_is_ignored() {
        let mut n = make_leader(); // term 1
        n.step(Event::Propose(b"x".to_vec()));
        // A reply from some ancient term-0 exchange claims match=5: ignore it.
        let effects = n.step(ack(2, 0, 5));
        assert!(effects.is_empty());
        assert_eq!(n.commit_index, 0, "stale ack must not commit anything");
    }

    /// THE Figure 8 test: an old-term entry on a majority must NOT commit by
    /// counting — only transitively, once a current-term entry commits.
    #[test]
    fn figure8_old_term_entry_commits_only_transitively() {
        // Node 1 in a 5-node cluster holds idx1 from old term 2, and now wins
        // an election for term 4.
        let mut n = node(1, vec![2, 3, 4, 5], 1);
        n.log.append(2, b"old".to_vec());
        n.current_term = 3;
        n.step(Event::Tick); // candidacy bumps to term 4
        n.step(grant_from(2, 4));
        n.step(grant_from(3, 4));
        assert!(matches!(n.role, Role::Leader { .. }));
        assert_eq!(n.current_term, 4);

        // Peers 2 and 3 confirm they hold idx1 → a MAJORITY holds it. But it's
        // a term-2 entry and we're term 4: counting it would be the Figure 8
        // bug (a term-3 rival could still overwrite it after our crash).
        n.step(ack(2, 4, 1));
        let effects = n.step(ack(3, 4, 1));
        assert_eq!(n.commit_index, 0, "old-term entry must NOT commit by count");
        assert!(applied(&effects).is_empty());

        // Now replicate one OWN-term entry to the same majority...
        n.step(Event::Propose(b"new".to_vec())); // idx2, term 4
        n.step(ack(2, 4, 2));
        let effects = n.step(ack(3, 4, 2));
        // ...and idx2 commits, dragging idx1 along TRANSITIVELY.
        assert_eq!(n.commit_index, 2);
        assert_eq!(applied(&effects), vec![1, 2], "old entry applies in order, after all");
    }

    #[test]
    fn heartbeat_retransmits_to_lagging_peers_only() {
        let mut n = make_leader();
        n.step(Event::Propose(b"x".to_vec()));
        n.step(ack(2, 1, 1)); // peer 2 caught up; peer 3 never acked

        let effects = tick_n(&mut n, HEARTBEAT_TICKS);
        let sends = ae_sends(&effects);
        let to_2 = sends.iter().find(|(to, _)| *to == 2).unwrap();
        let to_3 = sends.iter().find(|(to, _)| *to == 3).unwrap();
        assert!(to_2.1.entries.is_empty(), "caught-up peer gets pure heartbeat");
        assert_eq!(to_3.1.entries.len(), 1, "lagging peer gets the entry again");
    }
}
