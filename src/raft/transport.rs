//! How Raft messages leave the process. Fire-and-forget by design: Raft never
//! blocks on a reply (replies come back later as ordinary inbox messages), so
//! `send` returns immediately; implementations spawn their own I/O.

use std::collections::HashMap;

use crate::raft::log::NodeId;
use crate::raft::message::Message;

pub trait Transport: Send + Sync + 'static {
    /// Best-effort delivery. Losing a message is FINE — Raft's retries
    /// (heartbeat retransmission, election re-runs) are the recovery story.
    fn send(&self, to: NodeId, msg: Message);
}

/// HTTP transport: POST the message to the peer's /raft/message endpoint.
/// Spawns per send — fire-and-forget; a lost POST is just a lost packet.
pub struct HttpTransport {
    self_id: NodeId,
    peers: HashMap<NodeId, String>, // id -> base URL, e.g. "http://127.0.0.1:7002"
    http: reqwest::Client,
}

impl HttpTransport {
    pub fn new(self_id: NodeId, peers: HashMap<NodeId, String>) -> Self {
        Self {
            self_id,
            peers,
            http: reqwest::Client::new(),
        }
    }
}

impl Transport for HttpTransport {
    fn send(&self, to: NodeId, msg: Message) {
        let Some(base) = self.peers.get(&to) else {
            return;
        };
        let url = format!("{base}/raft/message?from={}", self.self_id);
        let http = self.http.clone();
        tokio::spawn(async move {
            let _ = http.post(url).json(&msg).send().await; // best-effort
        });
    }
}
