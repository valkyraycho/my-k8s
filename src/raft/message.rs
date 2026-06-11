//! The two RPCs of Raft (Figure 2) + a Message envelope so one transport
//! channel carries everything. Replies are standalone messages — Raft is
//! fire-and-forget, never blocking on a response.

use serde::{Deserialize, Serialize};

use crate::raft::log::{LogEntry, LogIndex, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RequestVoteReq {
    pub term: Term,
    pub candidate_id: NodeId,
    // Candidate's (last_index, last_term): voters grant only if this is at
    // least as up-to-date as their own log (§5.4.1).
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AppendEntriesReq {
    pub term: Term,
    pub leader_id: NodeId,
    // The Log Matching consistency check: follower must hold an entry at
    // (prev_log_index, prev_log_term) or it rejects.
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    /// Empty = heartbeat.
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    // "I now match through this index" — makes stale/reordered replies
    // harmless (leader just takes max) vs correlating replies to requests.
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Message {
    RequestVote(RequestVoteReq),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntriesReq),
    AppendEntriesResp(AppendEntriesResp),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every message variant must survive a JSON round-trip — this is the wire
    /// format the HTTP transport (step 8) will carry.
    #[test]
    fn all_message_variants_roundtrip() {
        let messages = vec![
            Message::RequestVote(RequestVoteReq {
                term: 3,
                candidate_id: 1,
                last_log_index: 7,
                last_log_term: 2,
            }),
            Message::RequestVoteResp(RequestVoteResp {
                term: 3,
                vote_granted: true,
            }),
            Message::AppendEntries(AppendEntriesReq {
                term: 3,
                leader_id: 2,
                prev_log_index: 7,
                prev_log_term: 2,
                entries: vec![LogEntry {
                    term: 3,
                    index: 8,
                    command: b"x=5".to_vec(),
                }],
                leader_commit: 7,
            }),
            Message::AppendEntriesResp(AppendEntriesResp {
                term: 3,
                success: true,
                match_index: 8,
            }),
        ];
        for msg in messages {
            let json = serde_json::to_string(&msg).unwrap();
            let parsed: Message = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, msg, "round-trip failed for {json}");
        }
    }

    /// A heartbeat is just AppendEntries with empty entries — make sure the
    /// empty Vec round-trips (not dropped/nulled).
    #[test]
    fn heartbeat_has_empty_entries() {
        let hb = Message::AppendEntries(AppendEntriesReq {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        });
        let json = serde_json::to_string(&hb).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();
        match parsed {
            Message::AppendEntries(req) => assert!(req.entries.is_empty()),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
