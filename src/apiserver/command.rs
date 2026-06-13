//! The replicated store's command language. Every mutation becomes ONE
//! StoreCommand serialized into a Raft log entry; every replica applies the
//! same commands in the same order → identical stores. Payloads are raw JSON
//! (`serde_json::Value`) — the applier deserializes per `kind`, keeping this
//! file ignorant of the five resource types.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct StoreCommand {
    /// Correlates a proposer's HTTP request with its apply outcome (the
    /// pending-oneshot map key). Replicas that didn't propose ignore it.
    pub id: Uuid,
    /// Which typed store applies this — a KIND_PREFIX ("pods/", "nodes/", …).
    pub kind: String,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Op {
    /// Object FULLY pre-stamped by the leader (uid, creationTimestamp) — apply
    /// must never generate randomness or read clocks.
    Create { obj: Value },
    ReplaceSpec { name: String, obj: Value },
    ReplaceStatus { name: String, rv: String, status: Value },
    Delete { name: String, rv: String },
}

/// The deterministic verdict every replica reaches independently. Mirrors
/// StoreError minus the sled internals, so the proposing handler maps it to
/// HTTP (200/404/409). No serde: it crosses an in-process oneshot, not the wire.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    /// The resulting object as JSON (the HTTP response body).
    Ok(Value),
    NotFound(String),
    AlreadyExists(String),
    Conflict { current: String, provided: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every command shape must survive the JSON round-trip — this IS the Raft
    /// log entry's payload format.
    #[test]
    fn store_command_variants_roundtrip() {
        let cmds = vec![
            StoreCommand {
                id: Uuid::nil(),
                kind: "pods/".into(),
                op: Op::Create {
                    // A realistically pre-stamped object (uid + ts present).
                    obj: json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {
                            "name": "web",
                            "uid": "550e8400-e29b-41d4-a716-446655440000",
                            "creationTimestamp": "2026-06-12T10:00:00Z"
                        },
                        "spec": { "containers": [] }
                    }),
                },
            },
            StoreCommand {
                id: Uuid::nil(),
                kind: "replicasets/".into(),
                op: Op::ReplaceSpec {
                    name: "web".into(),
                    obj: json!({"spec": {"replicas": 3}}),
                },
            },
            StoreCommand {
                id: Uuid::nil(),
                kind: "pods/".into(),
                op: Op::ReplaceStatus {
                    name: "web".into(),
                    rv: "7".into(),
                    status: json!({"phase": "Running"}),
                },
            },
            StoreCommand {
                id: Uuid::nil(),
                kind: "services/".into(),
                op: Op::Delete {
                    name: "web".into(),
                    rv: "12".into(),
                },
            },
        ];
        for cmd in cmds {
            let bytes = serde_json::to_vec(&cmd).unwrap();
            let parsed: StoreCommand = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(parsed, cmd);
        }
    }

    /// The id survives — it's the key the apply loop uses to resolve the
    /// proposer's pending oneshot.
    #[test]
    fn command_id_roundtrips() {
        let id = Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        let cmd = StoreCommand {
            id,
            kind: "nodes/".into(),
            op: Op::Delete {
                name: "n1".into(),
                rv: "1".into(),
            },
        };
        let parsed: StoreCommand =
            serde_json::from_slice(&serde_json::to_vec(&cmd).unwrap()).unwrap();
        assert_eq!(parsed.id, id);
    }
}
